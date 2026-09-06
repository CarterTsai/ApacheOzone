# Apache Ozone 2.2.1 Docker Compose

這份設定是單一 Docker host 的 staging / POC 基線，包含：

- Ozone Manager（OM）
- Storage Container Manager（SCM）
- 3 個固定 DataNode
- Recon
- S3 Gateway（S3G）

它不是跨主機的 production HA 叢集。Apache 官方 Docker Compose 文件定位為開發、測試與評估用途；正式環境請使用 bare metal 或 Kubernetes，並依環境建立 HA 拓撲。

## 需求

- Docker Engine 24+ 或 Docker Desktop
- Docker Compose v2（使用 `docker compose`，不是舊版 `docker-compose`）
- 至少 8 GB RAM；壓測或大量資料請提高 CPU、RAM 與磁碟容量

## 快速啟動

第一次啟動前，複製環境檔並下載 image：

```powershell
Copy-Item .env.example .env
docker compose pull
```

啟動完整 stack：

```powershell
docker compose up -d
```

Compose 會依下列順序處理服務：

1. `scm-init` 檢查並初始化 SCM metadata。
2. `scm` 通過 healthcheck 後，三個 DataNode 開始註冊 SCM。
3. `om-init` 等待 SCM healthy，檢查並初始化 OM metadata。
4. `om` 啟動並通過 healthcheck。
5. `recon` 與 `s3g` 等待 OM healthy 後啟動。

`scm-init` 與 `om-init` 是一次性服務；正常完成後顯示 `Exited (0)`，不是錯誤。

## 正確啟動流程

### 日常啟動

已有 initialized volumes 時，每次啟動只使用 base Compose：

```powershell
docker compose up -d
docker compose ps
```

不要在日常啟動使用 `docker-compose.upgrade.yaml`。該檔案只在 Ozone 版本升級的第一次 OM 啟動時使用。

### 啟動完成判斷

容器全部啟動後，確認長駐服務為 `healthy`：

```powershell
docker compose ps
docker compose exec -T scm ozone admin datanode list
docker compose logs --tail=100 scm
```

正常結果應符合：

- `scm`、`om`、`recon`、`s3g`、`datanode1`、`datanode2`、`datanode3` 都是 `healthy`。
- SCM 顯示 3 個 DataNode，狀態為 `Operational State: IN_SERVICE`、`Health State: HEALTHY`。
- SCM log 顯示 `registered datanodes (=3)`，並顯示 `state=OUT_OF_SAFE_MODE`。

剛啟動時 DataNode 尚未註冊，SCM 暫時在 safemode 是正常現象；等待 1 至 2 分鐘後再檢查。若持續沒有 3 個 DataNode，查看：

```powershell
docker compose logs --tail=200 datanode1 datanode2 datanode3 scm
```

### 重啟與停止

只重啟服務時，建議讓 Compose 重新套用依賴條件：

```powershell
docker compose up -d --force-recreate
```

暫停服務但保留 containers 與 volumes：

```powershell
docker compose stop
```

移除 containers 與 networks，但保留所有資料 volumes：

```powershell
docker compose down
```

不要使用 `docker compose down -v`，除非這是可丟棄的測試環境且確認不需要任何資料。

查看狀態：

```powershell
docker compose ps
docker compose logs -f scm om datanode1 datanode2 datanode3 recon s3g
```

`scm-init` 與 `om-init` 正常完成後顯示 `Exited (0)` 是預期結果。長駐服務應為 `Up`，且 health 欄位應為 `healthy`。

## 管理介面

預設只綁定本機：

- OM：<http://127.0.0.1:9874>
- SCM：<http://127.0.0.1:9876>
- Recon：<http://127.0.0.1:9888>
- S3G：<http://127.0.0.1:9878>

DataNode 管理埠刻意不發布到 host；DataNode 只在 `ozone-backend` 網路提供服務。需要臨時查看時，可從容器內檢查：

```powershell
docker compose exec datanode1 bash -lc "bash -c '</dev/tcp/127.0.0.1/9882'"
```

若必須讓其他主機存取管理介面，先在防火牆或反向代理限制來源，再在 `.env` 設定 `ADMIN_BIND_ADDRESS`。不要直接把所有管理埠公開到 Internet。

## Ozone 健康檢查

```powershell
docker compose ps
docker compose exec -T om ozone admin datanode list
docker compose exec -T om ozone admin safemode status
docker compose exec -T om ozone sh volume list /
```

預期有 3 個 DataNode，並且 SCM 最終離開 safemode。若剛啟動時仍在 safemode，等待 DataNode 完成註冊後再檢查。

## S3 Client

S3G endpoint 是 `http://127.0.0.1:9878`。先建立 volume 與 bucket：

```powershell
docker compose exec -T om ozone sh volume create /s3v
docker compose exec -T om ozone sh bucket create /s3v/demo
```

使用 AWS CLI：

```powershell
$env:AWS_ACCESS_KEY_ID = "demo"
$env:AWS_SECRET_ACCESS_KEY = "demo-secret"

aws --endpoint-url http://127.0.0.1:9878 s3api list-buckets
"hello ozone" | Set-Content -NoNewline sample.txt
aws --endpoint-url http://127.0.0.1:9878 s3 cp sample.txt s3://s3v/demo/sample.txt
aws --endpoint-url http://127.0.0.1:9878 s3 cp s3://s3v/demo/sample.txt sample-download.txt
```

實際環境請使用 IAM / Ranger、TLS，以及外部 secret manager 管理憑證，不要把 access key 寫進 Compose。

## 壓測

Freon 適合先驗證叢集功能與粗略效能。以下範例使用 3 副本設定：

```powershell
docker compose exec -T om ozone freon randomkeys --numOfVolumes 1 --numOfBuckets 1 --numOfKeys 1000 --keySize 1024 --valueSize 4096 --replication=THREE
```

大規模壓測前，請先確認磁碟、CPU、JVM heap、網路頻寬與 replication policy，並使用獨立的測試 volume。測試後先列出 volume，再只刪除已確認的測試 volume：

```powershell
docker compose exec -T om ozone sh volume list /
docker compose exec -T om ozone sh volume delete /s3v
```

上例的 `/s3v` 只適用於你確定要刪除該測試 volume 的情況；正式資料禁止直接刪除。

S3 相容性與應用程式吞吐量，請使用 AWS CLI、s5cmd、Warp 或實際 SDK 從 S3G 測試；不要只用 Freon 結果代表 S3 效能。

## Volume 與資料保留

目前使用具名 Docker volumes。為了相容既有 2.1.x 叢集，SCM、OM、Recon 的 RocksDB 與 Ratis 暫時保留在各自的 metadata volume 內；每個 DataNode 則使用獨立的 metadata、Ratis 與資料 volume。這些 volumes 通常仍位於同一台 Docker host，不能取代實體磁碟、跨主機複本或備份。

查看 volume：

```powershell
docker volume ls --filter label=com.docker.compose.project=ozone
```

升級既有資料時只執行 `docker compose down`，不要使用 `down -v`。本 Compose 已恢復 2.1.x 使用的 control-plane metadata 路徑，讓 2.2.1 可以讀取原 cluster state。

```powershell
docker compose down -v
docker compose up -d
```

`down -v` 會刪除 Ozone 資料，正式環境禁止直接執行。要保留資料，請先做停機備份與目錄遷移，再啟動新設定。

## 企業環境還需要補上的項目

這份 Compose 已處理單機 staging 常見的啟動順序、healthcheck、資料目錄、管理面隔離與 log rotation；以下項目不能靠通用單機 Compose 安全地代替：

1. 3 個 OM 與 3 個 SCM，啟用 OM/SCM HA Ratis，並放在不同故障域。
2. 至少 3 個跨主機、跨 rack / AZ 的 DataNode；目前 3 個 DataNode 都在同一台 Docker host。
3. 2 個以上 S3G，放在 TLS reverse proxy 或 load balancer 後方。
4. 每個 DataNode 使用 direct-attached HDD / JBOD；metadata 與 Ratis 使用獨立 SSD / NVMe。避免 NAS / SAN 作為核心資料層。
5. 使用自建、固定 digest、非 root 的 Ozone image；建立 image 掃描與升級回滾流程。
6. Kerberos、TLS、Ranger / IAM、secret manager、audit log 與網路 ACL。
7. Prometheus / Grafana、集中式 logs、告警、備份、restore 演練與 disaster recovery runbook。
8. rack / topology 設定、容量規劃、JVM heap、ulimit、磁碟與網路監控。

### 建議的正式部署方向

- 單機開發或整合測試：使用本檔案。
- 企業 production：使用 Ozone 官方 production 拓撲，在 bare metal 或 Kubernetes 建立 3 OM、3 SCM、3+ DataNode、2+ S3G，並以外部 LB、TLS、Kerberos、監控與備份系統整合。

## 常見問題

### `Unknown option: '-init'`

正確語法是 `ozone scm --init` 與 `ozone om --init`。本檔案已由 `scm-init` / `om-init` 使用正確語法，不要執行 `ozone scm -init`。

### `Failed to set directory permissions ... Operation not permitted`

這通常是 bind mount 權限或容器使用者不匹配。此 staging 設定預設使用 `OZONE_RUNTIME_USER=0:0` 以配合 upstream image；production 應改用自建非 root image，並在 host 先建立目錄、設定正確 UID/GID 與 mount 權限。

### OM `Storage is not initialized yet`

若這是在 2.1.x 升級到 2.2.1 後出現，通常是 OM 仍以一般模式啟動。不要重跑 `om-init`，也不要刪除 `ozone_om-metadata`；升級啟動必須帶 `--upgrade`。本專案提供一次性 override：

```powershell
docker compose down
docker compose -f docker-compose.yaml -f docker-compose.upgrade.yaml up -d scm datanode1 datanode2 datanode3
docker compose -f docker-compose.yaml -f docker-compose.upgrade.yaml up -d --force-recreate om
docker compose logs --tail=200 om
```

確認 OM 已進入 pre-finalized 狀態後，改回一般啟動設定：

```powershell
docker compose up -d --force-recreate om recon s3g
```

不要在日常重啟時持續使用 `docker-compose.upgrade.yaml`。`om-init` 顯示 `OM is already initialized` 只代表找到既有 `VERSION`，不代表已完成版本升級。

### DataNode 反覆連線 Recon `9891`

DataNode 不應以 Recon 作為啟動依賴。此設定已移除 DataNode 對 Recon 的依賴；DataNode 只等待 SCM，Recon 自己等待 OM 與 SCM。若仍看到舊訊息，請確認使用的是目前 `docker-compose.yaml` 並重新建立容器：

```powershell
docker compose up -d --force-recreate
```

### S3G 無法連線

```powershell
docker compose ps s3g om
docker compose logs --tail=200 s3g om
Test-NetConnection 127.0.0.1 -Port 9878
```

確認 OM 為 `healthy`、S3G 為 `healthy`，並確認 `.env` 的 `S3G_BIND_ADDRESS` 沒有綁到錯誤的 host interface。

## 從 2.1.0 升級到 2.2.1

2.2.1 是 Apache Ozone 的 maintenance release。現有資料升級請採 non-rolling 流程，不要直接刪除 volumes；官方流程會先進入 pre-finalized 狀態，確認穩定後才 finalize。Finalize 後將不能回復到舊版本。

先確認目前叢集健康並完成備份：

```powershell
docker compose ps
docker compose exec -T scm ozone admin datanode list
docker compose exec -T scm ozone admin scm finalizationstatus
```

確認 `.env` 的 `OZONE_IMAGE` 已改為 `apache/ozone:2.2.1-all-in-one`，然後停止所有元件但保留 volumes：

```powershell
docker compose down
docker compose pull
```

本次先前的錯誤啟動曾建立新的 SCM/DataNode cluster ID。停止服務後，若要保留原本的 cluster，請先確認三個新 DataNode volumes 都沒有需要保留的資料，再清除它們，讓 DataNode 重新向原 SCM 註冊。由於 ClusterID 也寫在 `/data/hdds/hdds/VERSION`，data volume 不能漏掉：

```powershell
docker volume rm ozone_datanode1-metadata ozone_datanode1-data ozone_datanode1-ratis
docker volume rm ozone_datanode2-metadata ozone_datanode2-data ozone_datanode2-ratis
docker volume rm ozone_datanode3-metadata ozone_datanode3-data ozone_datanode3-ratis
```

不要刪除 `ozone_scm-metadata`、`ozone_om-metadata` 或 `ozone_recon-metadata`。若任一 DataNode data volume 不是空的，先備份並確認資料歸屬後再決定是否清理。

以一次性 override 的 `--upgrade` 啟動 OM：

```powershell
docker compose -f docker-compose.yaml -f docker-compose.upgrade.yaml up -d --force-recreate
docker compose ps
```

確認 OM、SCM 與 DataNode 正常、cluster ID 一致後，先觀察一段時間。確定不需要 rollback，再依官方流程 finalize SCM 與 OM。若是本檔案的單 OM、非 HA 叢集，初始化時沒有設定 OM service ID，不要事後新增 `ozone.om.service.ids`；應先維持 pre-finalized 狀態，或依正式 HA migration 流程處理：

```powershell
docker compose exec -T om ozone admin scm finalizeupgrade
docker compose exec -T om ozone admin om finalizeupgrade -id=<om-service-id>
docker compose up -d --force-recreate om
```

`<om-service-id>` 必須替換成既有環境中已設定的 OM service ID。若升級後仍在 pre-finalized 狀態，維持該狀態即可保留 rollback 選項。參考：[Ozone 2.2.1 release notes](https://ozone.apache.org/release-notes/2.2.1/) 與 [Upgrade and Downgrade](https://ozone.apache.org/docs/administrator-guide/operations/upgrade-and-downgrade/)。
