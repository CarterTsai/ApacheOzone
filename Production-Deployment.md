# Apache Ozone 2.2.1 Production Deployment

評估日期：2026-09-13  
適用範圍：本專案的 Apache Ozone 2.2.1 Docker Compose 環境。

## 1. 評估結論

Apache Ozone 2.2.1 可以作為企業部署的候選版本，但目前 Compose 尚不適合直接承載正式業務。需要完成多主機高可用性、安全控制、容量規劃、備份還原與營運驗收。

本文件依據專案設定與官方文件整理，是部署規劃與驗收依據；不代表已完成線上壓測、故障切換、漏洞掃描或正式上線認證。

2.2.1 修正了 SCM 啟動與狀態處理、EC 資料恢復、平行讀取與監控等問題。採用修補版本有合理依據，但版本升級不能取代部署驗證。參考 [2.2.1 Release Notes](https://ozone.apache.org/release-notes/2.2.1/)。

## 2. 目前架構

| 元件 | 數量 | 現況 |
|---|---:|---|
| Ozone Manager (OM) | 1 | 單點故障，管理 namespace 與 metadata |
| Storage Container Manager (SCM) | 1 | 單點故障，管理 DataNode、container 與 pipeline |
| DataNode | 3 | 同一 Docker 主機上的三個實例，預設 RATIS 三副本 |
| Recon | 1 | 提供監控與管理資訊 |
| S3 Gateway (S3G) | 1 | 單點故障，對外提供 S3 API |
| SCM/OM 初始化服務 | 各 1 | 與常駐程序分離，依 VERSION 檔案判斷是否初始化 |

已有的基礎控制：

- 管理埠與 S3G published port 預設綁定主機的 localhost。
- backend network 設定為 internal，另有 admin 與 edge network。
- 各服務配置持久化 named volume。
- SCM/OM 初始化程序與常駐服務分離。
- 配置日誌輪替、restart policy、停止寬限時間與 TCP healthcheck。

主要限制：

- 三個 DataNode 共用同一主機故障域，三副本不能抵抗整台主機或共用儲存故障。
- 不同 named volume 不保證位於不同實體磁碟。
- Compose 未配置完整 Kerberos、授權、傳輸加密及企業 secret 整合。
- 預設使用 root 與 apache/ozone:2.2.1-all-in-one 映像。
- TCP 埠可連線不能證明資料可以寫入、讀回或副本完整。
- 本文件尚未驗證執行環境是否另有 Compose 以外的防火牆、備份或監控措施。

## 3. 上線缺口與優先級

「上線必要」以需要持續服務的正式業務為前提。允許停機的低重要性內部服務，應另外記錄可接受風險、責任人與恢復方式。

| 優先級 | 缺口 | 必要工作 | 驗收證據 |
|---|---|---|---|
| 上線必要 | 單一 OM、SCM | 建置 3 OM + 3 SCM，跨主機部署 | leader 切換、quorum 與 client failover 測試 |
| 上線必要 | DataNode 共用主機 | 分散至不同主機／故障域，預留副本修復資源 | 主機故障後的讀寫、重新複寫及容量紀錄 |
| 上線必要 | 單一 S3G | 至少 2 S3G，搭配具備備援的 LB、DNS、HTTPS | gateway 與 LB 故障測試 |
| 上線必要 | 身份與授權不足 | Kerberos、服務憑證、Native ACL 或 Ranger | 未授權與跨租戶存取被拒絕 |
| 上線必要 | 傳輸保護不足 | HTTPS、Hadoop RPC 保護、gRPC/Ratis 適用加密配置 | 各通訊路徑的身份與加密驗證 |
| 上線必要 | root 與測試映像 | 非 root 部署、固定 digest、SBOM、漏洞掃描 | UID/GID、掃描報告、核准的 artifact |
| 上線必要 | 備份還原未驗證 | 一致性備份、異地副本、金鑰備份 | 獨立環境還原達成 RPO/RTO |
| 上線必要 | healthcheck 僅驗證 TCP | 加入叢集狀態及端到端 S3 驗證 | safe mode、副本、checksum 與延遲結果 |
| 上線必要 | 容量與資源配置不足 | CPU、heap、native memory、IOPS、quota 規劃 | 正常與故障情境的壓測報告 |
| 營運必要 | 僅有本機日誌 | 集中 audit log、metrics、告警與值班流程 | 告警送達與實際事件處理紀錄 |
| 上線必要 | 升級與回退界線不清 | finalization 查核、停機窗口與還原方案 | 升級演練及狀態紀錄 |
| 上線必要 | 初始化缺少部署情境辨識 | 區分首次建置與既有叢集啟動 | metadata 掛載異常時停止並告警 |

目前 [security-todo.md](security-todo.md) 將 HA 列為 P3。若目標是持續提供服務的正式業務，應依本文件將 HA 提升為上線必要項目。

## 4. 建議部署拓撲

以下是設計起點，最終規模仍須依工作負載、故障容忍度與預算決定：

| 層級 | 建議配置 | 設計重點 |
|---|---|---|
| Metadata | 3 OM + 3 SCM | 跨 3 台主機；資源足夠時可各放 1 OM + 1 SCM |
| Data | RATIS 三副本至少分散至 3 台 DataNode 主機 | 預留額外節點與容量，避免故障後沒有足夠目標恢復三副本 |
| S3 存取 | 至少 2 S3G + 備援 LB | 固定 DNS、HTTPS、健康路由；保留 S3 簽章所需的 Host/path |
| 管理 | 至少 1 Recon | 僅允許管理網段與授權身份存取 |
| 依賴服務 | KDC、CA/KMS、Secret Manager、選用 Ranger | 各自有可用性、備份與憑證輪替策略 |
| 營運 | 集中日誌、metrics、告警、異地備份 | 確保叢集故障時仍能取得紀錄與備份 |

官方生產部署指南要求 OM/SCM HA，並建議多個 S3G。增加同一主機上的 container 數量不能消除主機故障風險。參考 [Production Deployment](https://ozone.apache.org/docs/administrator-guide/configuration/performance/placeholder/)。

DataNode 數量需依編碼方式區分。官方硬體指南針對 RS-6-3 建議至少 10 個 DataNode，RS-10-4 至少 15 個；不能直接把這些數字當成所有 RATIS 部署的最低要求。參考 [Hardware and Sizing](https://ozone.apache.org/docs/administrator-guide/installation/hardware-and-sizing/)。

跨機房部署還需評估 quorum 延遲、頻寬及網路分區行為，不應僅為分散節點而把同一 quorum 放到延遲過高的網路。

## 5. 部署平台與映像

目前使用的 all-in-one image 適合本專案的本機驗證。官方發布指南將 Ozone Docker image 定位為測試用途；企業部署應選擇經驗證的 Linux 安裝或自行維護、硬化的映像。參考 [Ozone Release Guideline](https://cwiki.apache.org/confluence/display/OZONE/Ozone%20Release%20Guideline)。

上線前應完成：

- [ ] 從核准來源取得 Ozone artifact，驗證 checksum／signature，保存版本與來源。
- [ ] 若採容器部署，固定 image digest，建立 SBOM、漏洞門檻及修補流程。
- [ ] 使用專用 UID/GID，事先配置 volume ownership 與最小權限。
- [ ] 限制 capabilities，配置 no-new-privileges 與適用的系統安全政策。
- [ ] 設定 CPU、memory、PID 與檔案描述元限制，並預留 JVM 以外的記憶體。
- [ ] 驗證停止寬限時間足以完成服務正常關閉。
- [ ] 區分開發與正式部署設定，納入版本控制及 code review。

改用 Kubernetes 不會自動完成 HA、磁碟隔離或安全配置；仍需驗證排程、故障域、儲存掛載與叢集生命週期。

## 6. 儲存與容量規劃

目前 metadata、data、Ratis 使用不同 named volume，但仍可能共用 Docker 主機的同一顆磁碟。

正式部署需確認實體位置與 I/O 路徑：

- OM/SCM RocksDB 與 Ratis metadata 使用合適的 SSD/NVMe。
- DataNode 資料與 Ratis log 依吞吐量、延遲及磁碟故障策略分配。
- 記錄每個 volume 對應的主機、磁碟、檔案系統、掛載點與 owner。
- 預留 RocksDB compaction、資料成長、節點維護及副本重建空間。
- 監控磁碟故障、I/O latency、容量與 inode 等資源。
- 設定租戶／volume／bucket quota，避免單一工作負載耗盡空間。

RATIS 三副本的資料空間粗估為邏輯資料量的三倍，還需加上 metadata、log、快照保留與修復預留；不能把總 raw capacity 直接當成可用容量。

硬體規格應以物件數量、物件大小、metadata 操作頻率、峰值吞吐及修復時間估算，再以實際壓測調整。參考 [Hardware and Sizing](https://ozone.apache.org/docs/administrator-guide/installation/hardware-and-sizing/)。

## 7. 安全與權限

詳細待辦見 [security-todo.md](security-todo.md)。部署時至少完成下列項目：

- [ ] 啟用 Ozone secure mode 與 Kerberos，配置適用的服務 principal、keytab、DNS 及時間同步。
- [ ] 管理 UI/API 配置身份驗證與來源限制。
- [ ] 選定 Native ACL 或 Ranger，定義管理員、應用程式、租戶與群組權限。
- [ ] 驗證未授權、跨租戶、過期／撤銷 credentials 的拒絕行為。
- [ ] 分別驗證 HTTPS、Hadoop RPC 保護及 gRPC/Ratis 適用加密設定；不把單一 TLS 開關視為全部通訊加密。
- [ ] S3 credentials、keytab、TLS private key 與備份解密金鑰納入 secret 管理及輪替。
- [ ] 保存 audit log，限制存取並避免寫入敏感憑證。

Ozone secure mode 中的 DataNode 可使用憑證身份；不應假設每個 DataNode 都必須採用傳統 Kerberos keytab。參考 [Configuring Kerberos](https://ozone.apache.org/docs/administrator-guide/configuration/security/kerberos/)。

只在 LB 終止 HTTPS，仍需評估 LB 到 S3G 及叢集內部的明文路徑。

## 8. 初始化、升級與回退

### 初始化保護

目前 SCM/OM 初始化程序在找不到 VERSION 檔案時會執行初始化。正式環境應分開處理：

| 情境 | 預期行為 |
|---|---|
| 明確的首次建置 | 確認目標 volume 為新建、配置正確，再執行初始化 |
| 既有叢集正常啟動 | 驗證 metadata、cluster ID、node ID 與掛載後啟動 |
| 既有叢集缺少 VERSION 或掛載異常 | 停止並告警，先核對掛載與備份 |
| 從備份還原 | 按還原 runbook 恢復一致資料並驗證身份，不以初始化取代還原 |

這可避免掛錯或遺失 volume 時意外建立另一個叢集。

### 升級與 finalization

2.2.1 官方流程是停機升級。升級後需區分 pre-finalized 與 finalized，不能只依 image tag 或 container healthy 判斷升級完成。

- [ ] 升級前保存版本、artifact、設定、cluster ID、volume 對照及可用備份。
- [ ] 事先定義停機窗口、成功條件、停止條件及回退方式。
- [ ] 查核並保存 OM、SCM 的 finalization 狀態與 DataNode 健康狀態。
- [ ] 先完成相容性與資料讀寫驗證，再依核准流程 finalize。
- [ ] 完成 finalize 後，不將更換舊 image 當成降版方案；須依適用的備份還原／遷移流程處理。
- [ ] 單 OM 轉 HA 前先完成 service ID、node ID、metadata 與客戶端設定的遷移設計，不能只增加 container。

官方文件指出 finalize 後不能直接降版。參考 [Upgrade and Downgrade](https://ozone.apache.org/docs/administrator-guide/operations/upgrade-and-downgrade/)。

## 9. 備份與災難復原

三副本與 HA 不等於備份，無法單獨防止誤刪、錯誤操作或整體故障域遺失。

- [ ] 定義 RPO（可接受資料損失時間）與 RTO（可接受恢復時間）。
- [ ] 規劃 OM/SCM metadata、資料、設定、ACL/policy、憑證與金鑰的備份。
- [ ] 確認備份一致性；不把執行中資料目錄的任意複製當成已驗證備份。
- [ ] 保留位於不同故障域、具備適當存取保護的備份。
- [ ] 在獨立環境實際還原，驗證物件內容、metadata、權限與應用程式。
- [ ] 演練主機遺失、磁碟遺失、metadata 故障與誤刪。
- [ ] 記錄還原耗時、資料損失範圍、執行者與結果。

正式資料的 volume 清除、重建或 docker compose down -v，必須納入既定變更與備份確認程序。

## 10. 監控與健康驗證

目前 healthcheck 只檢查 TCP listener。正式環境應把程序存活、服務可用與資料完整性分開驗證，避免把短暫依賴故障直接轉成連鎖重啟。

| 類型 | 檢查項目 |
|---|---|
| Metadata | OM/SCM leader、quorum、RPC 錯誤、RocksDB 與 Ratis 狀態 |
| DataNode | IN_SERVICE／HEALTHY、volume failure、STALE／DEAD |
| 資料保護 | 缺失、低副本、待修復 container 與修復進度 |
| 寫入能力 | SCM safe mode、可用 pipeline、端到端 S3 寫入 |
| 讀取能力 | S3 讀回、checksum、錯誤率與 P95/P99 延遲 |
| 資源 | heap、native memory、GC、CPU、磁碟容量與 I/O latency |
| 安全 | 登入失敗、權限變更、secret／憑證異動與到期 |
| 營運 | 備份失敗、告警送達、監控本身失效 |

端到端探測使用專用測試 bucket 與最小權限身份，定義頻率和清理流程，避免污染業務資料。

Recon 可提供叢集資訊，但仍需集中 metrics、audit log、告警通知、值班責任與事件處理 runbook。

## 11. S3 與應用程式相容性

S3-compatible 不代表具備全部 AWS S3 功能。必須以 2.2.1 的實際支援與企業應用驗證結果為準。

| 測試範圍 | 驗證內容 |
|---|---|
| AWS SDK／CLI | 簽章、endpoint、path-style、TLS、錯誤處理 |
| 物件操作 | PUT/GET/HEAD/LIST/DELETE、分頁、checksum |
| 大檔案 | multipart upload、完成、取消、重試與清理 |
| 故障 | timeout、斷線、gateway failover、重試與併發 |
| 資料平台 | Spark、Trino、Iceberg 的讀寫、提交、重試與清理 |
| 權限 | 跨租戶隔離、credential 撤銷、管理與資料身份分離 |
| 進階需求 | STS、Object Lock、版本控制、Lifecycle、SSE-KMS 等逐項核對 |

表中的進階功能是待確認需求，不代表 2.2.1 已全部支援。若業務依賴某功能，應取得版本支援證據並完成正負向測試後才上線。參考 [S3 API 文件](https://ozone.apache.org/docs/user-guide/client-interfaces/s3/)。

## 12. 上線執行順序

1. 確定可用容量、物件數量／大小、尖峰吞吐、SLO、RPO/RTO、主機數與預算。
2. 決定部署平台、故障域、OM/SCM HA、DataNode 數量與 S3G/LB 拓撲。
3. 配置磁碟、DNS、時間同步、網路及核准的 artifact／映像。
4. 在驗證環境建立 HA、安全配置及可重複的部署程序。
5. 完成既有資料遷移、初始化保護、備份、升級與回退 runbook。
6. 接入 metrics、audit log、告警與值班處理。
7. 執行應用相容性、容量壓測、故障切換與還原演練。
8. 依驗收證據確認上線條件，安排切換與觀察窗口。
9. 上線後持續檢查容量、修補、憑證輪替、備份與恢復能力。

## 13. 上線驗收表

所有結果都需附上測試環境、版本、設定、時間及可重現證據。

| 驗收項目 | 通過條件 | Owner | 證據 |
|---|---|---|---|
| OM/SCM HA | 單節點失效後依 SLO 恢復服務，quorum 正常 | 待指定 | 待補 |
| DataNode 故障 | 資料可讀，寫入行為符合預期，副本依目標完成修復 | 待指定 | 待補 |
| S3G/LB 備援 | 單一 gateway／LB 故障後 client 可恢復請求 | 待指定 | 待補 |
| 安全 | 未授權與跨租戶請求被拒絕，加密路徑完成驗證 | 待指定 | 待補 |
| 效能 | 正常及故障情境符合吞吐、錯誤率、P95/P99 目標 | 待指定 | 待補 |
| 備份還原 | 獨立環境恢復成功，符合 RPO/RTO | 待指定 | 待補 |
| 版本升級 | finalization 狀態明確，回退界線與程序經演練 | 待指定 | 待補 |
| 相容性 | 實際應用與必要 S3 功能全部通過 | 待指定 | 待補 |
| 監控告警 | 故障可被偵測，通知可送達且有處理責任人 | 待指定 | 待補 |
| Artifact | 版本與 digest 可追溯，漏洞符合企業門檻 | 待指定 | 待補 |

驗收表不取代企業自身的上線審查。尚未完成的項目需記錄影響、補救方式、期限與風險接受責任人。

## 14. 規模確認表

完成下表後，才能把參考拓撲轉成具體硬體數量與正式部署設定。

| 規劃項目 | 需求 |
|---|---|
| 初期／一年後可用容量 | 待確認 |
| 物件總數與成長率 | 待確認 |
| 物件大小分布、小檔比例 | 待確認 |
| 尖峰讀寫吞吐與請求數 | 待確認 |
| 同時連線與應用程式清單 | 待確認 |
| P95/P99 延遲與可用性 SLO | 待確認 |
| 可接受維護停機時間 | 待確認 |
| RPO / RTO | 待確認 |
| 可用主機、磁碟、網路與機房 | 待確認 |
| 資料保留、加密、租戶及稽核需求 | 待確認 |
| 維運團隊、值班與支援責任 | 待確認 |

