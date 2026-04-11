# Apache Ozone 2.1.0 Docker Compose 使用說明

這個目錄提供一份 Apache Ozone 2.1.0 的單機 Docker Compose 環境，包含以下服務：

- Ozone Manager: `om`
- Storage Container Manager: `scm`
- DataNode: `datanode`
- Recon: `recon`

這份 compose 是單 DataNode 測試環境，因此已設定 `hdds.scm.safemode.min.datanode=1`。如果改成多 DataNode 或正式環境，請依實際節點數調整 safe mode 設定。

## 環境需求

- Docker
- Docker Compose v2

確認 Docker Compose 可用：

```powershell
docker compose version
```

## 啟動服務

在本目錄執行：

```powershell
docker compose up -d
```

第一次啟動時，`scm` 和 `om` 會自動執行初始化：

- `ozone scm --init`
- `ozone om --init`

初始化完成後會接著啟動對應服務。後續重啟時，如果 volume 中已經存在初始化資料，就不會再次初始化。

## 查看服務狀態

```powershell
docker compose ps
```

查看所有服務 log：

```powershell
docker compose logs -f
```

只查看單一服務 log：

```powershell
docker compose logs -f scm
docker compose logs -f om
docker compose logs -f datanode
docker compose logs -f recon
```

## Web UI

啟動後可開啟以下頁面：

- Ozone Manager: http://localhost:9874
- DataNode: http://localhost:9864
- Recon: http://localhost:9888

SCM 和 Recon RPC 服務對外開放在：

- SCM: `localhost:9876`
- Recon RPC: `localhost:9891`

## 基本驗證

確認 Ozone CLI 可連到服務：

```powershell
docker compose exec om ozone sh volume list /
docker compose exec om ozone admin datanode list
```

查看容器內 Ozone 設定：

```powershell
docker compose exec om ozone getconf -confKey ozone.om.address
docker compose exec om ozone getconf -confKey ozone.scm.names
```

建立測試 bucket 前，先建立 volume 和 bucket：

```powershell
docker compose exec om ozone sh volume create /testvol
docker compose exec om ozone sh bucket create /testvol/testbucket
docker compose exec om ozone sh volume list /
docker compose exec om ozone sh bucket list /testvol
```

## Freon 壓測

Apache Ozone 內建 Freon 作為 load generator / tester。可先查看可用的壓測命令：

```powershell
docker compose exec om ozone freon --help
```

查看 `randomkeys` 參數：

```powershell
docker compose exec om ozone freon randomkeys --help
```

### 寫入 key 壓測

以下範例會建立 1 個 volume、1 個 bucket，並寫入 1000 個 10KB key：

```powershell
docker compose exec om ozone freon randomkeys `
  --num-of-volumes=1 `
  --num-of-buckets=1 `
  --num-of-keys=1000 `
  --key-size=10KB `
  --num-of-threads=10 `
  --type=RATIS `
  --replication=ONE
```

這份 compose 是單 DataNode 環境，請使用 `--replication=ONE`。不要使用 Freon 預設的 replication factor `THREE`，否則會因為 DataNode 數量不足導致壓測失敗或卡住。

### 小量測試

第一次測試可以先用較小的 key 數量確認流程：

```powershell
docker compose exec om ozone freon randomkeys `
  --num-of-volumes=1 `
  --num-of-buckets=1 `
  --num-of-keys=10 `
  --key-size=1KB `
  --num-of-threads=2 `
  --type=RATIS `
  --replication=ONE
```

### 驗證寫入

壓測時加上 `--validate-writes` 可以在寫入後驗證 key：

```powershell
docker compose exec om ozone freon randomkeys `
  --num-of-volumes=1 `
  --num-of-buckets=1 `
  --num-of-keys=100 `
  --key-size=10KB `
  --num-of-threads=5 `
  --type=RATIS `
  --replication=ONE `
  --validate-writes
```

### 清理測試資料

`randomkeys` 可加上 `--clean-objects` 清理 Freon 隨機建立的 volume、bucket 和 key：

```powershell
docker compose exec om ozone freon randomkeys `
  --num-of-volumes=1 `
  --num-of-buckets=1 `
  --num-of-keys=100 `
  --key-size=10KB `
  --num-of-threads=5 `
  --type=RATIS `
  --replication=ONE `
  --clean-objects
```

### 常用 Freon 子命令

- `randomkeys` / `rk`: 建立 volume、bucket，並寫入隨機 key
- `ockg`: 使用 Ozone client 建立 key
- `ockv`: 驗證 key
- `ockr`: 刪除 key
- `om-echo`: 測試 OM RPC
- `dn-echo`: 測試 DataNode RPC
- `scm-throughput-benchmark`: 測試 SCM throughput
- `s3kg`: 透過 S3 interface 建立 key；需要先部署 S3 Gateway 並設定 AWS credentials

## 停止服務

停止容器，但保留資料 volume：

```powershell
docker compose down
```

重新啟動：

```powershell
docker compose up -d
```

## 重建容器

如果修改了 `docker-compose.yaml`，可重建容器：

```powershell
docker compose up -d --force-recreate
```

## 清除所有資料並重新初始化

如果初始化失敗，或想要完整重建一個乾淨的 Ozone 環境，可以刪除容器和 volume：

```powershell
docker compose down -v
docker compose up -d
```

注意：`docker compose down -v` 會刪除 Ozone metadata 和 DataNode 資料，不能復原。

## 常見問題

### SCM 顯示 `Unknown option: '-init'`

Ozone 2.1.0 的 SCM 初始化參數是 `--init`，不是 `-init`。本 compose 檔已經在 `scm` command 中明確使用：

```bash
ozone scm --init
```

如果仍看到舊錯誤，請確認使用的是目前這份 `docker-compose.yaml`，並重建容器：

```powershell
docker compose up -d --force-recreate
```

如果之前已有半初始化的 volume，可清除資料後重新啟動：

```powershell
docker compose down -v
docker compose up -d
```

### Docker 顯示 `Access is denied`

如果看到類似訊息：

```text
Error loading config file: open C:\Users\...\ .docker\config.json: Access is denied
```

請確認目前使用者有權限讀取 Docker 設定，並確認 Docker Desktop 已啟動。若服務仍能正常啟動，這通常不是 `docker-compose.yaml` 語法錯誤。

### SCM 顯示 `Failed to set directory permissions for /data/metadata`

如果 SCM log 出現：

```text
Failed to set directory permissions for /data/metadata: /data/metadata: Operation not permitted
```

這通常是 Docker volume 權限和 Ozone container 使用者不一致造成的。本 compose 檔已設定服務用 `root` 使用者啟動，讓 Ozone 可以調整 `/data/metadata` 的 POSIX 權限。

修改 compose 後請重建容器：

```powershell
docker compose up -d --force-recreate
```

如果之前的 volume 已經留下錯誤權限或半初始化資料，請清除 volume 後重新啟動：

```powershell
docker compose down -v
docker compose up -d
```

注意：`docker compose down -v` 會刪除 Ozone metadata 和 DataNode 資料，不能復原。

### DataNode 顯示 `Retrying connect to server: recon/...:9891`

DataNode 會連到 Recon 的 RPC port `9891` 回報狀態。如果 Recon 還在啟動中，DataNode log 可能短暫出現：

```text
Retrying connect to server: recon/...:9891
```

本 compose 檔已設定 DataNode 等待 `scm:9876`、`om:9874`、`recon:9891` 後再啟動，並將 Recon RPC port `9891` 對外開放。

修改 compose 後請重建容器：

```powershell
docker compose up -d --force-recreate
```

如果仍持續出現，請先看 Recon 是否啟動成功：

```powershell
docker compose logs -f recon
docker compose ps
```

### S3

啟動後，用 AWS CLI 連：

```powershell
$env:AWS_ACCESS_KEY_ID="test"
$env:AWS_SECRET_ACCESS_KEY="test"
aws configure set default.s3.addressing_style path
aws --endpoint-url http://localhost:9878 s3api list-buckets
```

你這個 compose 沒有開 Ozone security，所以 access key / secret 可以先用任意值，例如 `test/test`。

建立 bucket、上傳、下載：

```powershell
aws --endpoint-url http://localhost:9878 s3api create-bucket --bucket bucket1

aws --endpoint-url http://localhost:9878 s3 cp README.md s3://bucket1/README.md

aws --endpoint-url http://localhost:9878 s3 ls s3://bucket1/

aws --endpoint-url http://localhost:9878 s3 cp s3://bucket1/README.md .\README.download.md
```

Ozone S3 bucket 會對應到 Ozone 內部的 `/s3v` volume 底下。官方文件也說明：S3 Gateway 是額外服務，S3 buckets 會存在 `/s3v`，未啟用 security 時可以用任意 `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`。參考：[Ozone S3 API docs](https://ozone.apache.org/docs/user-guide/client-interfaces/s3/s3-api)。