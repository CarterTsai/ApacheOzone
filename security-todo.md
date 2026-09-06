# Apache Ozone Security TODO

本文件是目前 Apache Ozone 2.2.1 Docker Compose 環境的安全強化清單。現有環境適合開發、整合測試與單機驗證；在本文件中的 P0/P1 項目完成前，不應直接承載正式資料，也不應把管理介面暴露到公網。

目前 Compose 架構包含單一 SCM、單一 OM、三個 DataNode、Recon 與 S3 Gateway。這是可用性測試拓撲，不是跨主機、跨機房的正式 HA 架構。

## 優先級

- **P0**：在存放真實資料或開放網路前必須完成。
- **P1**：正式環境上線前必須完成。
- **P2**：上線後的持續改善與營運控制。
- **P3**：長期架構改善。

## 官方參考

- [Ozone Security Configuration](https://ozone.apache.org/docs/administrator-guide/configuration/security/)
- [Kerberos Authentication](https://ozone.apache.org/docs/administrator-guide/configuration/security/kerberos/)
- [Ranger ACLs](https://ozone.apache.org/docs/administrator-guide/configuration/security/ranger/)
- [Native ACLs](https://ozone.apache.org/docs/core-concepts/security/acls/native-acls/)
- [Ozone Administrators](https://ozone.apache.org/docs/administrator-guide/configuration/security/administrators/)
- [gRPC Network Encryption](https://ozone.apache.org/docs/administrator-guide/configuration/security/encryption/network-encryption/grpc/)

## 目前安全基線

以下是目前 Compose 設定的已知狀態，不代表企業安全控制已完成：

| 項目 | 目前狀態 | 風險 |
|---|---|---|
| 管理介面綁定位址 | SCM、OM、Recon、S3G 預設綁定 127.0.0.1 | 若日後改成 0.0.0.0，管理介面會被直接暴露 |
| DataNode 管理埠 | 未發布到 Docker host | 仍須以 Docker network 與主機防火牆限制東西向流量 |
| 身份驗證 | 尚未啟用 Kerberos 或其他企業身份整合 | 預設 simple authentication 不適合正式環境 |
| 傳輸加密 | 尚未啟用 TLS/mTLS | 帳密、管理流量與資料流量可能以明文傳輸 |
| 授權 | 尚未完成 Native ACL 或 Ranger policy 設計 | 需要明確的租戶、群組與最小權限模型 |
| Container 使用者 | .env.example 預設 OZONE_RUNTIME_USER=0:0，主要用於本機 volume 相容性 | Container root 權限過大 |
| Secret | 不可把 S3 secret、Kerberos keytab 或 TLS private key 放進 Git、.env 或 Compose | 可能造成長期憑證外洩 |
| HA | 單一 SCM、單一 OM、單一主機 Compose | 任一核心服務或主機故障都可能中斷服務 |
| 稽核 | 尚未接入集中式、不可竄改的 audit log | 無法可靠追查登入、授權與管理操作 |

## P0 上線前阻擋項目

### 網路暴露

- [ ] 只透過受控的 Load Balancer、反向代理或內部網路提供 S3 Gateway。
- [ ] SCM 9876、OM 9874、Recon 9888 僅允許管理網段存取，不對使用者網路或公網開放。
- [ ] DataNode RPC、Ratis、HTTP 管理埠只允許 Ozone backend network 與必要的管理來源。
- [ ] 主機防火牆設定 allowlist，並確認雲端 Security Group、網路 ACL 與 Docker published ports 一致。
- [ ] 禁止使用 0.0.0.0 綁定管理介面，除非前方已有驗證、TLS 與來源限制。
- [ ] 限制 container 對外連線，只允許 DNS、KDC、Ranger、Vault、監控、套件更新等必要目的地。
- [ ] 將 edge、backend、admin 網路分離；S3G 不應與不必要的管理服務共用對外網段。

### Secret 與憑證

- [ ] 移除所有範例 S3 credentials，並立即輪替曾經出現在 README、shell history、CI log 或 Git history 的 secret。
- [ ] 使用企業 Secret Manager、HashiCorp Vault、Docker secrets 或 Kubernetes Secret 管理 S3 secret、Kerberos keytab、TLS private key 與 KMS 憑證。
- [ ] .env 只能保存非敏感設定；確認它已被 .gitignore 排除，並掃描 Git history 是否曾提交 secret。
- [ ] 為每個服務、租戶、CI/CD pipeline 使用不同身份，不共用 root/admin credential。
- [ ] 建立憑證輪替、撤銷、遺失與緊急切換程序，並記錄 owner、有效期限與最後驗證時間。
- [ ] 備份中的 OM metadata、ACL、keytab、private key 與 S3 credential 必須加密，備份解密金鑰不可與備份放在同一位置。

### Container 與映像檔

- [ ] 將 OZONE_IMAGE 從 mutable tag 改成已核准的 immutable image digest，並保留可追溯的 SBOM。
- [ ] 每次升級前執行 CVE scan、SBOM 產生、來源驗證與回滾測試。
- [ ] 建立不需要 root 的 Ozone image；完成 volume ownership 後將 OZONE_RUNTIME_USER 改為專用 UID/GID。
- [ ] 套用 no-new-privileges、最小 Linux capabilities、seccomp 與 AppArmor/SELinux profile。
- [ ] 不掛載 Docker socket，不掛載 host 的敏感目錄，不使用 privileged container。
- [ ] 對每個服務設定 CPU、memory、PID 與檔案描述元限制，避免單一服務耗盡主機資源。
- [ ] 對 metadata 與 data volume 設定正確 owner、mode 與磁碟加密；不要用 chmod 777 解決權限問題。

## P1 身份驗證與授權

### Kerberos

- [ ] 先建置並測試企業 KDC、DNS/FQDN 與 NTP/時間同步。
- [ ] 為 OM、SCM、S3G、Recon 與必要的 HTTP/RPC endpoint 建立專用 principal 與 keytab；禁止共用服務帳號。
- [ ] 將 krb5.conf 與 keytab 以唯讀方式注入，keytab 不得寫入 image layer、Git 或公開 volume。
- [ ] 啟用並驗證：

~~~xml
<property>
  <name>ozone.security.enabled</name>
  <value>true</value>
</property>
<property>
  <name>hadoop.security.authentication</name>
  <value>kerberos</value>
</property>
~~~

- [ ] 分別測試服務啟動、kinit、票證更新、keytab 輪替、票證過期與 KDC 暫時不可用時的行為。
- [ ] 確認所有主機時鐘偏差符合 KDC 要求，並監控 DNS、NTP 與 KDC 可用性。
- [ ] 為管理員、應用程式、批次工作與監控建立不同的 Kerberos identity，禁止使用無限制的管理身份。

### 管理員與最小權限

- [ ] 明確設定 ozone.administrators、ozone.administrators.groups 與必要的 read-only administrators。
- [ ] S3 管理操作與 Recon 管理操作使用獨立的管理群組，避免所有平台管理員都能讀寫使用者資料。
- [ ] 管理群組採 allowlist，不使用 * 或所有人群組。
- [ ] 建立離職、轉調、break-glass account 與定期 access review 流程。
- [ ] 將管理操作納入 ticket/change approval，並保留操作者、原因、時間與結果。

### ACL 或 Ranger

- [ ] 在 Native ACL 與 Apache Ranger 之間做出正式決策，不能只同時開啟而沒有權威來源。
- [ ] 若選 Native ACL，啟用並驗證：

~~~xml
<property>
  <name>ozone.acl.enabled</name>
  <value>true</value>
</property>
<property>
  <name>ozone.acl.authorizer.class</name>
  <value>org.apache.hadoop.ozone.security.acl.OzoneNativeAuthorizer</value>
</property>
~~~

- [ ] 若選 Ranger，部署相容版本的 Ranger Ozone plugin，設定：

~~~xml
<property>
  <name>ozone.acl.enabled</name>
  <value>true</value>
</property>
<property>
  <name>ozone.acl.authorizer.class</name>
  <value>org.apache.ranger.authorization.ozone.authorizer.RangerOzoneAuthorizer</value>
</property>
~~~

- [ ] Ranger policy 以租戶、bucket、prefix、服務帳號與群組拆分，預設 deny，逐項加入必要的 read/write/list/delete 權限。
- [ ] 驗證 Ranger admin/plugin 之間的 TLS、Kerberos、policy cache、Ranger 暫時不可用與 fail-closed 行為。
- [ ] 建立至少一組正向測試與負向測試：可讀、不可讀、可寫、不可刪除、跨租戶不可列舉。
- [ ] 釐清 ACL、S3 bucket policy、Ranger policy 與 superuser 權限的優先順序，並把結果寫入操作手冊。

## P1 傳輸與資料保護

### TLS / mTLS

- [ ] 為 S3G、OM、SCM、Recon 的 HTTP endpoint 啟用 HTTPS，使用企業 CA 簽發憑證。
- [ ] 為 Ozone 服務間 RPC、Ratis 與 gRPC 啟用適用的 TLS/mTLS 設定；核心服務可依官方設定啟用 hdds.grpc.tls.enabled=true。
- [ ] 憑證 SAN 必須包含實際 DNS service name，例如 om、scm、recon 與負載平衡器名稱；不要用 IP 或 wildcard 取代完整命名規則。
- [ ] 所有 client 必須驗證 CA、hostname、有效期限與撤銷狀態；禁止 insecureSkipVerify 類型設定。
- [ ] 使用現代 TLS 版本與核准 cipher，關閉不必要的明文 listener。
- [ ] 建立 CA、server certificate、client certificate 的輪替與失效演練。
- [ ] 用 openssl s_client、實際 S3 client 與 Ozone CLI 驗證成功和失敗案例。

### 儲存加密與備份

- [ ] 啟用主機、磁碟或雲端 block storage encryption，並將 KMS 管理權限與 Ozone 管理權限分離。
- [ ] 評估應用層或 S3 SSE/KMS 需求，確認 key rotation、key deletion 與 restore 行為。
- [ ] OM metadata、SCM metadata、Ratis metadata 與 DataNode container data 都要有備份策略；備份前先確認一致性與 retention。
- [ ] 備份至少保留一份在不同故障域，並定期執行完整 restore drill，不只驗證檔案存在。
- [ ] 定義 RPO/RTO，並為 OM/SCM 故障、DataNode 遺失、主機遺失、誤刪 bucket 與勒索情境建立 runbook。
- [ ] 任何 docker compose down -v、volume 清除或重建操作都必須先通過變更核准與備份確認。

## P1 稽核、監控與告警

- [ ] 將 OM、SCM、S3G、Recon、DataNode 與 container runtime log 傳送到集中式 log platform。
- [ ] Audit log 必須包含時間、服務、principal、來源、資源、操作、授權結果、request ID 與錯誤原因。
- [ ] 禁止把 access key、secret key、keytab 內容、JWT、Kerberos ticket 或 TLS private key 寫入 log。
- [ ] Log retention、存取權限、加密、備份與不可竄改性符合企業稽核要求。
- [ ] 建立告警：反覆 authentication failure、ACL/Ranger policy 變更、administrator 變更、S3 secret 變更、非預期 listener、DataNode volume 異常、safe mode、container restart 與 disk 使用率。
- [ ] Recon、SCM、OM 的 metrics、JMX 與 debug endpoint 僅允許監控網段，不能直接對外提供。
- [ ] 建立每週 security review 與每月 access review，保留證據與處理結果。

## P2 Compose 與平台硬化

- [ ] 將正式環境設定與本機開發設定分離，禁止以 production secret 啟動 docker-compose.yaml。
- [ ] 為正式環境建立獨立的 compose.prod.yaml 或改用 Kubernetes/Operator，並在 CI 驗證設定差異。
- [ ] 為每個服務明確設定 restart、healthcheck、timeout、dependency 與 graceful shutdown；不要把 container 啟動成功當成服務可用。
- [ ] Healthcheck 不應洩漏敏感資訊，且應驗證實際依賴，例如 OM 是否能連 SCM、S3G 是否能連 OM。
- [ ] 管理埠只在管理介面或 SSH tunnel 可達；不要因為除錯方便而長期 publish DataNode port。
- [ ] Compose 檔案中的 image、port、volume、network、environment change 都要經過 code review。
- [ ] 在 CI 執行 YAML lint、docker compose config --quiet、secret scan、image scan、container permission scan 與基本 smoke test。
- [ ] 定期更新 Apache Ozone、JDK、OS base image、Docker Engine 與所有外部 plugin，並驗證相容性與回滾。

## P3 正式 HA 架構

- [ ] 將 SCM、OM、DataNode 分散到不同 host 或 failure domain，不以單一 Docker host 作為正式 HA。
- [ ] 規劃多 OM、多 SCM、足夠 DataNode、Ratis quorum、metadata 磁碟與網路頻寬，並測試任一節點故障時的行為。
- [ ] 使用企業 KDC、Ranger、Vault/KMS、集中式 log、監控與備份服務，不把這些依賴與 Ozone 共用同一個故障域。
- [ ] 在新集群初始化時固定 OM/SCM service ID、node ID、DNS 命名與憑證命名規則。
- [ ] 不要直接對已初始化的目前單 OM 集群事後加入新的 OM service ID；先建立 migration/rebuild runbook、備份與回滾方案，再變更叢集拓撲。
- [ ] 以正式容量模型規劃 CPU、RAM、metadata IOPS、網路、容量成長、replication、container 數量與 rebalancing 預算。
- [ ] 進行故障演練：OM、SCM、Ratis leader、DataNode、網路分區、KDC、Ranger、Vault、DNS、NTP 與 storage failure。
- [ ] 上線前完成獨立 penetration test、權限 review、災難復原演練與正式風險接受紀錄。

## 驗證命令

以下命令是在管理主機執行的基礎檢查；正式環境需替換 project name、credentials 與 endpoint。

~~~powershell
# 1. Compose 設定與服務狀態
docker compose config --quiet
docker compose ps

# 2. 確認 published port 沒有意外綁到所有介面
docker compose port scm 9876
docker compose port om 9874
docker compose port recon 9888
docker compose port s3g 9878

# 3. Ozone 基本健康狀態
docker compose exec scm ozone admin datanode list
docker compose exec scm ozone admin safemode get

# 4. Container 權限與安全設定
docker inspect ozone-om-1 --format '{{.Config.User}} {{.HostConfig.Privileged}} {{.HostConfig.NoNewPrivileges}}'
docker inspect ozone-datanode1-1 --format '{{.Config.User}} {{.HostConfig.Privileged}} {{.HostConfig.NoNewPrivileges}}'

# 5. 在 workspace 掃描疑似 secret，確認沒有把值提交到 Git
rg -n --hidden -g '!\\.git' -g '!security-todo.md' '(AWS_SECRET|AWS_ACCESS_KEY|PASSWORD|SECRET|BEGIN .* PRIVATE KEY|keytab)'
git grep -n -I -E '(AWS_SECRET|AWS_ACCESS_KEY|PASSWORD|SECRET|BEGIN .* PRIVATE KEY|keytab)' -- ':!security-todo.md'
~~~

完成 TLS/Kerberos 後，再加入：

~~~bash
kinit -kt /etc/security/keytabs/<service>.keytab <service-principal>
klist
openssl s_client -connect <endpoint>:<tls-port> -servername <dns-name> -verify_return_error
~~~

驗證結果必須保存 command output、時間、執行者、環境、憑證版本與失敗案例，不只在終端機手動確認。

## 上線順序

1. 匯出並驗證 OM/SCM/DataNode 備份，記錄目前版本、image digest、volume 與叢集 ID。
2. 封閉所有不必要的 published ports，完成主機防火牆與網路 allowlist。
3. 建立 immutable image、SBOM、CVE gate、非 root container 與 volume permission runbook。
4. 接入 KDC，啟用 Kerberos，完成服務 principal、keytab 輪替與失敗演練。
5. 啟用 TLS/mTLS，先在內部 endpoint 驗證，再讓 S3 client 切換至 HTTPS。
6. 選定 Native ACL 或 Ranger，建立 deny-by-default policy 與正負向測試。
7. 將 secret、audit log、metrics、backup 與告警接到企業平台。
8. 完成 HA、故障演練、restore drill、penetration test 與風險簽核。
9. 只公開必要的 S3 Gateway endpoint，並以監控與定期 review 維持控制。

## 禁止事項

- [ ] 不在正式環境執行未經核准的 docker compose down -v。
- [ ] 不使用 chmod 777、privileged container 或 Docker socket 解決 Ozone 問題。
- [ ] 不把 OZONE_RUNTIME_USER=0:0 視為正式安全設定。
- [ ] 不把 S3 secret、Kerberos keytab、TLS private key 或 KMS credential 放入 Git、image、.env 或公開 log。
- [ ] 不用 wildcard admin、共用 root account 或永久有效的 access key。
- [ ] 不在未啟用 TLS 與身份驗證時把 OM、SCM、Recon 或 DataNode 管理埠暴露到公網。
- [ ] 不直接刪除目前仍可能包含資料的 Docker volume；先核對 backup、container、volume 與 restore 結果。

## 安全決策紀錄

每個 P0/P1 項目完成後填寫證據，避免只留下口頭確認：

| 項目 | Owner | 完成日期 | 證據/連結 | Review 結果 |
|---|---|---|---|---|
| Kerberos |  |  |  |  |
| TLS/mTLS |  |  |  |  |
| ACL/Ranger |  |  |  |  |
| Secret rotation |  |  |  |  |
| Backup restore |  |  |  |  |
| Network review |  |  |  |  |
| Image/SBOM/CVE |  |  |  |  |
| HA/failure drill |  |  |  |  |
| Penetration test |  |  |  |  |

## 完成定義

只有在以下條件全部成立後，才能把此 Compose 架構視為通過正式環境安全審查：

- P0/P1 checkbox 全部完成並有可重現證據。
- 未授權 client 無法存取 S3、OM、SCM、Recon 或 DataNode 管理介面。
- 所有服務身份、TLS 憑證、S3 secret 與管理權限都有 owner、輪替期限與撤銷流程。
- 備份可以在獨立環境完成 restore，且 RPO/RTO 符合需求。
- 服務、映像檔、OS、Docker 與 plugin 都通過版本與漏洞門檻。
- HA、故障、網路分區、身份服務故障與災難復原演練已完成並由系統 owner 簽核。
