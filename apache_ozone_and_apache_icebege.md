# Apache Iceberg 1.10.1 完整教學 — 以 PySpark 3.5.x 實作基礎與進階操作

> 本文檔以 Apache Iceberg 1.10.1、PySpark 3.5.x 為核心，涵蓋 ACID 交易、Schema Evolution、Time Travel、Merge Into、檔案結構等所有核心觀念的理論與實作。第一堂課以本機檔案系統為主，後續進階章節將介紹如何與 Apache Ozone S3 Gateway 整合。

---

## 目錄

1. [概述與核心概念](#1-概述與核心概念)
2. [環境建置（本機基礎版）](#2-環境建置本機基礎版)
3. [Apache Iceberg 檔案結構解析](#3-apache-iceberg-檔案結構解析)
4. [PySpark 完整範例程式](#4-pyspark-完整範例程式)
5. [ACID 交易操作](#5-acid-交易操作)
6. [Schema Evolution](#6-schema-evolution)
7. [Time Travel 時間旅行](#7-time-travel-時間旅行)
8. [Merge Into 合併更新](#8-merge-into-合併更新)
9. [刪除操作與刪除檔案](#9-刪除操作與刪除檔案)
10. [Partition 與 Partition Evolution](#10-partition-與-partition-evolution)
11. [Tags 與 Branches](#11-tags-與-branches)
12. [Incremental Read 增量讀取](#12-incremental-read-增量讀取)
13. [Metadata Tables 檢索](#13-metadata-tables-檢索)
14. [進階：Apache Ozone 整合配置](#14-進階apache-ozone-整合配置)
15. [檔案結構實際觀察](#15-檔案結構實際觀察)
16. [進階主題](#16-進階主題)
17. [最佳實踐與注意事項](#17-最佳實踐與注意事項)
18. [參考資源](#18-參考資源)

---

## 1. 概述與核心概念

### 1.1 什麼是 Apache Iceberg？

Apache Iceberg 是一個為大規模分析資料集設計的 **開放式表格格式（Open Table Format）**。它在資料湖儲存層之上提供「表格語意」，帶來了以下核心能力：

| 核心能力 | 說明 |
|---------|------|
| **ACID 交易** | 保證資料寫入的原子性、一致性、隔離性與持久性 |
| **Schema Evolution** | 支援無損的結構變更（新增、刪除、修改、重新排列欄位） |
| **Time Travel** | 可查詢歷史快照，回溯任意時間點的資料狀態 |
| **隱藏式 Partition（Hidden Partitioning）** | 查詢時不需指定 partition 條件，Iceberg 自動過濾 |
| **Merge / Upsert** | 支援行級別的更新與刪除操作 |
| **Branches & Tags** | 支援分支開發與標籤化管理 |

### 1.2 Iceberg 的三層核心架構

```text
查詢引擎層
Spark / Flink / Trino / Dremio / Hive
        │
        ▼
Iceberg Catalog 層
HadoopCatalog / HiveCatalog / REST Catalog / JDBC Catalog / Nessie
        │
        ▼
Iceberg Table Metadata
metadata json / snapshots / manifest list / manifests
        │
        ▼
資料檔案層
Parquet / ORC / Avro / Delete Files
        │
        ▼
Storage
HDFS / S3 / Apache Ozone / Azure Blob / GCS / MinIO
```

iceberg 本身不負責執行 SQL，它負責定義：

表格目前有哪些資料檔案。
表格目前的 Schema。
表格目前的 Partition 規則。
每一次 Commit 的 Snapshot。
哪些檔案屬於哪個 Snapshot。
如何安全地做新增、刪除、更新與版本切換。


### 1.3 Iceberg 的核心概念


#### 1.3.1 Table Format，不是 File Format

Iceberg 不是 Parquet 的替代品。

比較正確的關係是：

```text
Iceberg Table
  ├── Metadata files
  ├── Manifest files
  ├── Snapshot metadata
  └── Data files
        ├── Parquet
        ├── ORC
        └── Avro
```

Parquet、ORC、Avro 是底層資料檔格式。

Iceberg 是管理這些檔案的**表格格式**。

#### 1.3.2 Catalog


> 某一張 Iceberg Table 目前最新的 metadata.json 在哪裡。

常見 Catalog 類型：

| Catalog                  | 說明                                 | 適合場景              |
| ------------------------ | ---------------------------------- | ----------------- |
| Hive Catalog             | 使用 Hive Metastore 管理 Iceberg Table | Hadoop / Hive 生態系 |
| Hadoop Catalog           | 直接用檔案路徑管理表格                        | 測試、簡單環境           |
| REST Catalog             | 透過 REST API 管理表格                   | 跨引擎、雲端、企業平台       |
| AWS Glue Catalog         | AWS 環境常用                           | S3 + AWS 生態       |
| Nessie Catalog           | 支援 Git-like branch/tag             | 多環境版本控管           |
| Polaris / Dremio Catalog | 開放式 Iceberg Catalog 實作             | Lakehouse 平台化     |

Iceberg 的 Spark 設定可以包裝 Spark 原本的 Hive Catalog，使同一個 Hive Metastore 同時管理 Iceberg 與非 Iceberg 表。([Apache Iceberg][2])

#### 1.3.3 Metadata File

每一次 Iceberg Table 有變更時，會產生新的 metadata file。

metadata file 會記錄：

| 項目                     | 說明               |
| ---------------------- | ---------------- |
| Table schema           | 欄位定義、型別、欄位 ID    |
| Partition spec         | 分區規則             |
| Sort order             | 排序規則             |
| Snapshot list          | 歷史快照             |
| Current snapshot       | 目前生效的快照          |
| Manifest list location | 指向 manifest list |
| Table properties       | 表格設定             |

這是 Iceberg 能做到 Time Travel、Schema Evolution、Partition Evolution 的關鍵。

#### 1.3.4 Snapshot

Snapshot 代表某一個時間點的表格狀態。

每一次寫入、更新、刪除、覆蓋，都會產生新的 Snapshot。

例如：

```text
Snapshot 1001：表格建立
Snapshot 1002：新增 10 個 Parquet 檔案
Snapshot 1003：刪除部分資料
Snapshot 1004：新增欄位 customer_level
```

查詢引擎查表時，不是直接掃目錄，而是讀取目前 Snapshot 對應的資料檔案清單。

Iceberg 採用 snapshot-based querying model，資料檔案會透過 manifest 與 metadata file 對應到特定 snapshot。([docs.snowflake.cn][3])


#### 1.3.5 Manifest List 與 Manifest File

Iceberg 不會靠「掃描整個資料目錄」來知道有哪些檔案。

它會透過 metadata 管理資料檔案清單。

```text
metadata.json
   |
   v
snapshot
   |
   v
manifest list
   |
   v
manifest file
   |
   v
data files
```

Manifest file 通常會記錄：

| 資訊                | 用途                       |
| ----------------- | ------------------------ |
| Data file path    | 資料檔案位置                   |
| Partition values  | 分區值                      |
| Record count      | 筆數                       |
| Column statistics | min / max / null count 等 |
| File size         | 檔案大小                     |
| Content type      | data file / delete file  |

這讓查詢引擎可以在真正讀資料前，先用 metadata 排除不需要掃描的檔案。

#### 1.3.6 ACID Transaction

Iceberg 支援 ACID Transaction，讓多個 Job 或多個引擎同時讀寫時，可以維持一致性。

這裡的 ACID 可理解為：

| 特性          | Iceberg 中的意義                     |
| ----------- | -------------------------------- |
| Atomicity   | Commit 成功才切換到新 Snapshot，失敗不影響舊版本 |
| Consistency | Table metadata 維持一致狀態            |
| Isolation   | 查詢看到的是某個穩定 Snapshot              |
| Durability  | Metadata 與 Data File 寫入儲存後可持久化   |

實務上，Iceberg 常用 **optimistic concurrency control**。
也就是寫入者先寫檔案與 metadata，最後嘗試 Commit。如果期間表格已被其他人修改，就需要重試。

---

#### 1.3.7 Time Travel

Iceberg 可以查詢過去某個 Snapshot 或時間點的資料。

概念範例：

```sql
SELECT *
FROM catalog.db.orders
VERSION AS OF 123456789;
```

或：

```sql
SELECT *
FROM catalog.db.orders
TIMESTAMP AS OF '2026-04-01 00:00:00';
```

實際語法會依 Spark、Trino、Flink、Dremio 等引擎略有不同。

Time Travel 的用途：

| 用途   | 說明             |
| ---- | -------------- |
| 資料回溯 | 查詢某個時間點的資料狀態   |
| 問題排查 | 比對資料異常前後版本     |
| 稽核   | 保留歷史變更軌跡       |
| 還原   | 回復到特定 Snapshot |
| 重跑報表 | 使用當時版本重新產生結果   |

---

#### 1.3.8 Schema Evolution

Iceberg 支援 Schema 演進。

常見操作：

| 操作             | 說明                   |
| -------------- | -------------------- |
| Add column     | 新增欄位                 |
| Rename column  | 重新命名欄位               |
| Drop column    | 移除欄位                 |
| Reorder column | 調整欄位順序               |
| Type promotion | 部分型別升級，例如 int → long |

Iceberg 的 Schema Evolution 重要特點是：
**欄位識別主要依靠欄位 ID，不是只靠欄位名稱或欄位順序。**

這比傳統 Hive Table 或單純 Parquet 更安全，因為欄位改名、欄位順序改變時，不容易誤讀資料。

官方規格也明確提到 Iceberg 支援 schema evolution 與底層檔案格式的一致演進規則。([Apache Iceberg][4])

---

#### 1.3.9 Partition Evolution

傳統 Hive Table 的 Partition 很常綁定目錄結構，例如：

```text
/orders/dt=2026-04-25/country=TW/
```

如果一開始分區設計錯了，後續要改通常很麻煩。

Iceberg 支援 Partition Evolution，也就是：

> 舊資料保留舊分區規則，新資料可以使用新的分區規則。

例如一開始用天分區：

```sql
PARTITIONED BY days(order_time)
```

後來資料量變大，改成小時分區：

```sql
PARTITIONED BY hours(order_time)
```

Iceberg 可以同時理解舊資料與新資料的不同 Partition Spec。

---

#### 1.3.10 Hidden Partitioning

Hidden Partitioning 是 Iceberg 很重要的特性。

使用者查詢時不需要知道資料實際怎麼分區。

例如表格用 `days(event_time)` 分區，使用者仍然可以寫：

```sql
SELECT *
FROM events
WHERE event_time >= TIMESTAMP '2026-04-25 00:00:00'
  AND event_time <  TIMESTAMP '2026-04-26 00:00:00';
```

查詢引擎會透過 Iceberg metadata 自動做 partition pruning。

這比傳統 Hive 常見寫法更安全：

```sql
WHERE dt = '2026-04-25'
```

因為使用者不用記住額外的分區欄位，也比較不容易漏加條件。

Iceberg 的規格中也強調，查詢規劃是根據資料值的 predicates，而不是要求使用者直接操作 partition values。([Apache Iceberg][4])

#### 1.3.11 Iceberg 的資料寫入流程

以 Spark 寫入 Iceberg 為例，概念流程如下：

```text
1. Spark Job 產生新的 data files
2. Iceberg 建立新的 manifest file
3. Iceberg 建立新的 manifest list
4. Iceberg 建立新的 metadata.json
5. Catalog 嘗試把 current metadata pointer 指向新的 metadata.json
6. Commit 成功後，新 Snapshot 生效
```

重點是：

> Iceberg 通常不會直接修改舊資料檔，而是新增檔案與新 metadata，最後切換 metadata pointer。

這也是它可以支援 Time Travel 與 Snapshot Isolation 的原因。

#### 1.3.12 Iceberg 的查詢流程

查詢 Iceberg Table 時，流程大致如下：

```text
1. 查詢引擎向 Catalog 取得目前 metadata.json 位置
2. 讀取 table metadata
3. 找到目前 snapshot
4. 讀取 manifest list
5. 根據查詢條件過濾 manifest / data files
6. 只讀取需要的 Parquet / ORC / Avro 檔案
7. 回傳查詢結果
```

與傳統 Hive Table 最大差異：

| 傳統 Hive Table         | Iceberg Table               |
| --------------------- | --------------------------- |
| 常依賴目錄掃描與 partition 目錄 | 依靠 metadata / manifest      |
| Partition 與目錄結構強綁定    | Partition spec 可演進          |
| 改欄位風險較高               | 欄位 ID 管理較安全                 |
| 多引擎寫入一致性較弱            | Snapshot / Commit 機制較完整     |
| 歷史版本管理較弱              | 原生支援 Snapshot / Time Travel |

---

#### 1.3.13 適合使用 Iceberg 的場景
##### 1.3.13.1 Data Lakehouse

適合把 HDFS、S3、Ozone 上的檔案資料變成可被 SQL 查詢與治理的資料表。

```text
Raw Data → Bronze Iceberg → Silver Iceberg → Gold Iceberg
```

---

##### 1.3.13.2 Batch + Streaming 混合

例如：

```text
Flink 寫入即時事件
Spark 做批次清洗
Trino / Dremio 給使用者查詢
```

Iceberg 讓多個引擎共用同一張表的 metadata。

---

##### 1.3.13.3 大型事實表

例如：

| 表格               | 特性                    |
| ---------------- | --------------------- |
| click_event      | 大量寫入、大量查詢             |
| transaction_log  | 需要歷史版本與稽核             |
| customer_profile | Schema 常演進            |
| risk_event       | Streaming + Batch 都會用 |
| iot_sensor_data  | 時序資料、大量分區             |

---

##### 1.3.13.4 需要資料回溯與稽核

Iceberg 的 Snapshot 適合處理：

| 需求         | 說明               |
| ---------- | ---------------- |
| 查詢昨天版本     | Time Travel      |
| 找出哪次寫入造成異常 | Snapshot history |
| 還原到前一版     | Rollback         |
| 保留資料版本     | Metadata history |
| 比對改版前後資料   | Snapshot diff    |

#### 1.3.14 不適合使用 Iceberg 的場景

Iceberg 很強，但不是所有情境都該用。

| 場景                              | 原因                                 |
| ------------------------------- | ---------------------------------- |
| 小型 OLTP 系統                      | Iceberg 不是交易型資料庫                   |
| 毫秒級單筆查詢                         | Iceberg 偏分析型，不是 Redis / PostgreSQL |
| 高頻單筆 update/delete              | 需要謹慎設計，否則 delete file 與小檔案會變多      |
| 資料量很小                           | 管理成本可能大於收益                         |
| 團隊沒有維護 metadata / compaction 能力 | 長期可能效能劣化                           |
| 只需要簡單存檔                         | 直接 Parquet 可能就足夠         


#### 1.3.15 Iceberg 與你們常見架構的整合方式

以你目前常見的 Hadoop / Spark / Hive Metastore / Dremio / Ranger 架構，可以這樣定位：

```text
                    使用者 / BI / API
                          |
              +-----------+-----------+
              |                       |
            JDBC                    JDBC
              |                       |
           Dremio                HiveServer2
              |                       |
      Dremio VDS 控制          Ranger Row/Column Policy
              |                       |
              +-----------+-----------+
                          |
                    Hive Metastore
                          |
                    Iceberg Metadata
                          |
                 HDFS / Apache Ozone / S3
                          |
                 Parquet / ORC / Avro
```

## 重點說明

| 元件                | 在 Iceberg 架構中的角色                            |
| ----------------- | ------------------------------------------- |
| Spark             | 建表、寫入、ETL、Compaction、Rewrite                |
| Hive Metastore    | Iceberg Catalog，用來記錄 table metadata pointer |
| HiveServer2       | SQL 查詢入口，可搭配 Ranger 做權限控制                   |
| Ranger            | 對 HiveServer2 做 Row / Column 級權限            |
| Dremio            | SQL 查詢入口，可用 VDS 做資料視圖與欄位控制                  |
| HDFS / Ozone / S3 | 實際儲存資料檔與 metadata                           |
| Parquet           | 常見底層資料檔格式                                   |
| Kerberos          | Hadoop / Hive / HDFS 身分驗證                   |
| Trino             | 可選的互動式 SQL 查詢引擎                             |
| Flink             | 可選的串流寫入引擎                                   |

## 2. 環境建置（本機基礎版）

為了避免一開始混入儲存後端與網路設定的干擾，第一堂課我們將使用本機的檔案系統（`file:///tmp/iceberg-warehouse`）作為 Warehouse。

### 2.1 依賴元件版本建議

| 元件 | 版本 | 用途 |
|------|------|------|
| Apache Iceberg | 1.10.1 | 表格格式 |
| PySpark / Spark | 3.5.x | 大資料處理引擎（需對齊 iceberg-spark-runtime） |
| Scala ABI | 2.12 | 搭配 Spark 3.5 使用 |
| Python | 3.10+ | 建議統一環境，以利未來升級相容 |
| Java | 11 或 17 | Spark 執行環境 |

### 2.2 建立虛擬環境與安裝 PySpark

```bash
# 1. 建立 Python 3.10 的虛擬環境 (預設名稱就會是 .venv)
uv venv --python 3.10

# 2. 啟動虛擬環境 (與原本相同)
## mac or linux
source .venv/bin/activate
## windows
.\.venv\Scripts\activate.ps1 

# 3. 使用 uv pip 安裝套件
uv pip install pyspark==3.5.4
```

### 2.3 PySpark 基本連線設定

在啟動 SparkSession 時，透過 `spark.jars.packages` 引入對應版本的 Iceberg Runtime JAR，並設定本地 Catalog：

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Iceberg Basic Tutorial")
    .master("local[*]")
    .config(
        "spark.jars.packages",
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.1"
    )
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    )
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", "file:///tmp/iceberg-warehouse")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
```

---

## 3. Apache Iceberg 檔案結構解析

**注意**：Iceberg 的實際目錄結構與檔案命名會依據使用的 Catalog、FileIO、引擎版本及表格屬性而有差異。以下為概念性常見目錄，請勿將其視為絕對固定的格式。

```text
file:///tmp/iceberg-warehouse/
└── demo/                             ← 資料庫目錄 (Namespace)
    └── sales/                        ← 表格目錄
        ├── metadata/                   ← 元資料目錄
        |   ├── version-hint.text
        │   ├── v1.metadata.json       ← v1 版本的元資料
        │   ├── v2.metadata.json       ← v2 版本的元資料
        │   ├── 1234567890123456789.avro    ← Manifest List (Snapshots)
        |   ├── manifest-aaa.avro
        │   └── manifest-list-123456789.avro ← Manifest Files
        └── data/                       ← 實際資料檔案 (由 FileIO 決定路徑)
            ├── 00001-abc123.parquet
            └── ...
```
常見檔案說明：

| 檔案                     | 說明                         |
| ---------------------- | -------------------------- |
| `vN.metadata.json`     | Table metadata             |
| `version-hint.text`    | 提示目前 metadata version      |
| `snap-*.avro`          | Snapshot 相關資訊              |
| `manifest-list-*.avro` | Manifest 清單                |
| `manifest-*.avro`      | Data file / delete file 清單 |
| `*.parquet`            | 實際資料檔案                     |


---

## 4. PySpark 完整範例程式

以下是一個完整的端到端範例，涵蓋 Iceberg 核心功能的實際操作：

```python
#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, expr
import time

WAREHOUSE_PATH = "file:///tmp/iceberg-warehouse"

def create_spark_session():
    return (
        SparkSession.builder
        .appName("Iceberg Local Tutorial")
        .master("local[*]")
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.1")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", WAREHOUSE_PATH)
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )

def print_section(title):
    print("\n" + "=" * 70)
    print(f"  📌 {title}")
    print("=" * 70)

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        # ===================================================================
        # 第一章：建立資料庫與表格
        # ===================================================================
        print_section("第一章：建立資料庫與 Iceberg 表格")
        spark.sql("CREATE DATABASE IF NOT EXISTS local.demo")
        spark.sql("DROP TABLE IF EXISTS local.demo.sales")

        spark.sql("""
            CREATE TABLE local.demo.sales (
                order_id      BIGINT,
                customer_name STRING,
                amount        DOUBLE,
                order_date    DATE,
                category      STRING,
                region        STRING
            ) USING iceberg
        """)
        print("✅ 建立 Iceberg 表格: local.demo.sales")

        # ===================================================================
        # 第二章：資料插入（確保 Date 轉型）
        # ===================================================================
        print_section("第二章：資料插入")
        sample_data = [
            (1, "Alice Chen",   1500.50, "2024-04-20", "Electronics", "North"),
            (2, "Bob Wang",     2300.75, "2024-04-20", "Electronics", "South"),
            (3, "Charlie Liu",   800.00, "2024-04-21", "Clothing",  "East"),
            (4, "Diana Chang",  3200.00, "2024-04-21", "Furniture", "West"),
            (5, "Edward Hsu",   1100.25, "2024-04-22", "Electronics", "North"),
        ]

        df_sample = spark.createDataFrame(
            sample_data,
            ["order_id", "customer_name", "amount", "order_date", "category", "region"]
        ).withColumn("order_date", to_date(col("order_date")))

        df_sample.writeTo("local.demo.sales").append()
        print("✅ 插入初始資料")
        spark.sql("SELECT * FROM local.demo.sales ORDER BY order_id").show(truncate=False)

        # ===================================================================
        # 第三章：觀察 Snapshot 與歷史
        # ===================================================================
        print_section("第三章：觀察 Snapshot")
        spark.sql("SELECT * FROM local.demo.sales.snapshots").show(truncate=False)
        spark.sql("SELECT * FROM local.demo.sales.history").show(truncate=False)

        # ===================================================================
        # 第四章：ACID 交易 — UPDATE & DELETE
        # ===================================================================
        print_section("第四章：ACID 交易")
        spark.sql("UPDATE local.demo.sales SET amount = 1800.50 WHERE order_id = 1")
        spark.sql("DELETE FROM local.demo.sales WHERE order_id = 5")
        spark.sql("SELECT * FROM local.demo.sales ORDER BY order_id").show(truncate=False)

        # ===================================================================
        # 第五章：Schema Evolution
        # ===================================================================
        print_section("第五章：Schema Evolution")
        spark.sql("ALTER TABLE local.demo.sales ADD COLUMN payment_method STRING")
        spark.sql("ALTER TABLE local.demo.sales ADD COLUMN shipping_region STRING")
        spark.sql("""
            UPDATE local.demo.sales
            SET payment_method = 'Credit Card', shipping_region = region
            WHERE payment_method IS NULL
        """)
        spark.sql("SELECT * FROM local.demo.sales ORDER BY order_id").show(truncate=False)

        # ===================================================================
        # 第六章：Time Travel
        # ===================================================================
        print_section("第六章：Time Travel")
        first_snapshot = spark.sql("""
            SELECT snapshot_id FROM local.demo.sales.snapshots
            ORDER BY committed_at ASC LIMIT 1
        """).collect()[0]["snapshot_id"]

        print(f"回到快照 {first_snapshot}:")
        spark.sql(f"SELECT * FROM local.demo.sales VERSION AS OF {first_snapshot} ORDER BY order_id").show(truncate=False)

        # ===================================================================
        # 第七章：MERGE INTO
        # ===================================================================
        print_section("第七章：MERGE INTO")
        spark.sql("DROP TABLE IF EXISTS local.demo.sales_staging")
        spark.sql("""
            CREATE TABLE local.demo.sales_staging (
                order_id BIGINT, customer_name STRING, amount DOUBLE,
                order_date DATE, category STRING, region STRING,
                payment_method STRING, shipping_region STRING
            ) USING iceberg
        """)

        staging_data = [
            (1, "Alice Chen", 1650.50, "2024-04-20", "Electronics", "North", "Line Pay", "North"),
            (6, "Fiona Lin", 950.80, "2024-04-23", "Clothing", "South", "Credit Card", "South")
        ]
        df_staging = spark.createDataFrame(
            staging_data,
            ["order_id", "customer_name", "amount", "order_date", "category", "region", "payment_method", "shipping_region"]
        ).withColumn("order_date", to_date(col("order_date")))
        df_staging.writeTo("local.demo.sales_staging").append()

        spark.sql("""
            MERGE INTO local.demo.sales AS t
            USING local.demo.sales_staging AS s
            ON t.order_id = s.order_id
            WHEN MATCHED THEN UPDATE SET
                t.amount = s.amount, t.payment_method = s.payment_method
            WHEN NOT MATCHED THEN INSERT *
        """)
        spark.sql("SELECT * FROM local.demo.sales ORDER BY order_id").show(truncate=False)

    finally:
        spark.stop()

if __name__ == "__main__":
    main()
```

---

## 5. ACID 交易操作

當執行 `UPDATE`、`DELETE` 或 `MERGE` 時，Iceberg 保證 ACID 特性。舊的 Snapshot 依然存在，提供讀取隔離，直到您執行清理。

---

## 6. Schema Evolution

Iceberg 依賴欄位 ID 進行 Schema 管理，因此增刪欄位不會影響既有的 Parquet 檔案：
```sql
ALTER TABLE local.demo.sales ADD COLUMN discount DOUBLE;
ALTER TABLE local.demo.sales RENAME COLUMN region TO shipping_region;
```

---

## 7. Time Travel 時間旅行

除了使用 Snapshot ID，也可以建立 Tag 來鎖定狀態：
```sql
ALTER TABLE local.demo.sales CREATE TAG stable_snapshot;
SELECT * FROM local.demo.sales VERSION AS OF 'stable_snapshot';
```

---

## 8. Merge Into 合併更新

`MERGE INTO` 適合 Upsert 語意，它會重寫受影響的 Data Files 並建立新的 Snapshot，較傳統的 `INSERT OVERWRITE` 效能更好且具備原子性。

---

## 9. 刪除操作與刪除檔案

 Iceberg 支援兩種刪除模式，實際產生的是位元刪除檔案：
- **Position Delete**：記錄特定檔案與行號的刪除。
- **Equality Delete**：記錄等值條件的刪除。

---

## 10. Partition 與 Partition Evolution

 Iceberg 的隱藏式 Partition 讓使用者無需知道底層切割邏輯：
```sql
CREATE TABLE local.demo.sales_by_day (
    order_id BIGINT, amount DOUBLE, order_date DATE
) USING iceberg
PARTITIONED BY (days(order_date));
```

---

## 11. Tags 與 Branches

Branches 是進階的快照參照，允許開發測試而不影響主線資料：
```sql
ALTER TABLE local.demo.sales CREATE BRANCH experimental;
```
寫入 Branch 時使用識別字寫法：
```python
df.writeTo("local.demo.sales").option("branch", "experimental").append()
```

---

## 12. Incremental Read 增量讀取

增量讀取適合追蹤 Append 操作，在 Spark 中應使用 `start-snapshot-id` 配置：
```python
snapshots = spark.sql("SELECT snapshot_id FROM local.demo.sales.snapshots ORDER BY committed_at").collect()
before_snapshot = snapshots[0]["snapshot_id"]
after_snapshot  = snapshots[-1]["snapshot_id"]

incremental_df = (
    spark.read
    .format("iceberg")
    .option("start-snapshot-id", before_snapshot)
    .option("end-snapshot-id", after_snapshot)
    .load("local.demo.sales")
)
```

---

## 13. Metadata Tables 檢索

 Iceberg 內建豐富的 Metadata 檢索功能：
```sql
SELECT * FROM local.demo.sales.history;
SELECT * FROM local.demo.sales.snapshots;
SELECT * FROM local.demo.sales.files;
SELECT * FROM local.demo.sales.partitions;
```

### CTAS 與 RTAS
為了避免重複建表錯誤，請使用 `DROP TABLE IF EXISTS` 或 `REPLACE TABLE`：
```sql
DROP TABLE IF EXISTS local.demo.high_value_sales;
CREATE TABLE local.demo.high_value_sales USING iceberg
AS SELECT * FROM local.demo.sales WHERE amount >= 2000;
```

---

## 14. 進階：Apache Ozone 整合配置

當學員掌握本地基礎後，第三堂課可以將 Warehouse 移轉至物件儲存。
**注意：Ozone 採用 Ozone Manager (OM) 與 Storage Container Manager (SCM) 架構，請勿與 HDFS NameNode 混淆。**

### 14.1 Ozone Docker Compose 骨架

```yaml
version: '3.8'
services:
  om:
    image: apache/ozone:2.1.0
    environment:
      - OZONE_NODE_TYPE=om
    command: ["sh", "-c", "ozone om"]
  scm:
    image: apache/ozone:2.1.0
    environment:
      - OZONE_NODE_TYPE=scm
    command: ["sh", "-c", "ozone scm"]
  datanode:
    image: apache/ozone:2.1.0
    depends_on: [om, scm]
    environment:
      - OZONE_NODE_TYPE=datanode
    command: ["sh", "-c", "ozone datanode"]
  s3gateway:
    image: apache/ozone:2.1.0
    ports:
      - "9878:9878"
    depends_on: [om, scm, datanode]
    environment:
      - OZONE_NODE_TYPE=s3g
    command: ["sh", "-c", "ozone s3g"]
```

### 14.2 Spark 連接 S3 Gateway 骨架

```python
spark = (
    SparkSession.builder
    .appName("Iceberg Ozone Tutorial")
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.1,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.367")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.ozone", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.ozone.type", "hadoop")
    .config("spark.sql.catalog.ozone.warehouse", "s3a://iceberg-warehouse/warehouse")
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9878")
    .config("spark.hadoop.fs.s3a.access.key", "ozone")
    .config("spark.hadoop.fs.s3a.secret.key", "ozone")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)
```

---

## 15. 檔案結構實際觀察

可透過 AWS CLI 觀察 Ozone S3 Gateway 內的實際生成檔案：
```bash
aws s3 ls --endpoint http://localhost:9878/ iceberg-warehouse/warehouse/demo/sales/metadata/
```

---

## 16. 進階主題

### Vacuum 清理孤兒檔案與過期快照
教學期間建議保留快照以便觀察 Time Travel，實務上可定期清除：
```sql
CALL local.system.expire_snapshots('local.demo.sales', TIMESTAMP '...');
CALL local.system.remove_orphan_files('local.demo.sales');
```

---

## 17. 最佳實踐與注意事項

| 類別 | 建議 |
|---|---|
| 版本 | Spark、Scala ABI、Iceberg runtime 要對齊 |
| Schema | 避免頻繁 rename / drop，先評估下游相容性 |
| Partition | 使用 hidden partition transform，例如 `days(ts)` |
| 儲存整合 | 先用 AWS CLI 驗證 Ozone S3 Gateway，再接入 Spark |

---

## 18. 參考資源
- [Apache Iceberg Releases](https://iceberg.apache.org/releases/)
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Apache Ozone S3 Gateway](https://ozone.apache.org/docs/user-guide/client-interfaces/s3/s3-api)
