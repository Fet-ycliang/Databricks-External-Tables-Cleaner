# 使用案例與最佳實務

本文件提供 Databricks External Tables Cleaner 的實際使用案例與最佳實務建議，幫助您在不同場景下正確且安全地使用此工具。

## 目錄

1. [基本使用案例](#基本使用案例)
2. [進階安全使用案例](#進階安全使用案例)
3. [自動化與排程案例](#自動化與排程案例)
4. [疑難排解案例](#疑難排解案例)
5. [最佳實務建議](#最佳實務建議)
6. [常見錯誤與解決方案](#常見錯誤與解決方案)

---

## 基本使用案例

### 案例 1：首次使用 - 探索性清理

**場景描述：**
您第一次使用此工具，想了解 `default` schema 中有多少孤兒表定義。

**執行步驟：**

1. 開啟 `notebooks/clean_tables_with_dryrun.py` Notebook
2. 設定 Widgets 參數：
   ```
   store: hive_metastore
   schema: default
   debug: True
   dry_run: True
   require_confirmation: False
   whitelist: (留空)
   blacklist: (留空)
   ```
3. 執行整個 Notebook
4. 查看輸出結果，了解有哪些表將被清理

**預期結果：**
```
[DRY-RUN] 預計刪除表數量：5
因白名單跳過：0
因黑名單跳過：0
因資料存在跳過：15
總計檢查表數量：20
```

**下一步：**
檢查候選表清單，確認這些表確實可以刪除。

---

### 案例 2：清理測試環境的臨時表

**場景描述：**
開發團隊在 `dev` schema 中建立了許多測試表，現在需要清理不再使用的表。

**執行步驟：**

1. **第一步：Dry-run 預覽**
   ```python
   # 設定配置
   config = CleanupConfig(
       dry_run=True,
       whitelist_patterns=['dev.important_*'],  # 保護重要的測試表
       max_last_access_age_days=30  # 只刪除 30 天未存取的表
   )
   
   # 執行掃描
   tables = get_tables(spark, 'hive_metastore', 'dev', False)
   tabledetailsDF = get_tables_details(spark, 'hive_metastore', 'dev', tables)
   
   # Dry-run
   deleted, candidates = drop_table_definition_without_storage_safe(
       spark, tabledetailsDF, logger, config
   )
   
   # 檢查結果
   print(f"將刪除 {deleted} 個表")
   for c in candidates:
       if c['action'] == 'dry_run_candidate':
           print(f"  - {c['table_name']}: {c['reason']}")
   ```

2. **第二步：確認後實際刪除**
   ```python
   # 確認無誤後，設定 dry_run=False
   config.dry_run = False
   
   # 實際執行刪除
   deleted, candidates = drop_table_definition_without_storage_safe(
       spark, tabledetailsDF, logger, config
   )
   
   print(f"✓ 成功刪除 {deleted} 個表")
   ```

**預期結果：**
```
[DRY-RUN] 預計刪除表數量：12
✓ 成功刪除 12 個表
```

---

### 案例 3：清理特定格式的表

**場景描述：**
只想清理 Parquet 格式的孤兒表，保留所有 Delta 表。

**執行步驟：**

```python
# 取得表詳細資訊
tabledetailsDF = get_tables_details(spark, 'main', 'staging', tables)

# 過濾：只保留 Parquet 表
parquet_tables = tabledetailsDF.filter(col('Provider') == 'parquet')

# 執行清理
config = CleanupConfig(dry_run=True)
deleted, candidates = drop_table_definition_without_storage_safe(
    spark, parquet_tables, logger, config
)
```

**預期結果：**
```
找到 8 個 Parquet 表需要檢查
[DRY-RUN] 預計刪除表數量：3
```

---

## 進階安全使用案例

### 案例 4：生產環境清理（高安全要求）

**場景描述：**
在生產環境中清理 `production` schema 的孤兒表，需要多層安全控制。

**執行步驟：**

```python
from datetime import date

# 建立嚴格的安全配置
config = CleanupConfig(
    dry_run=True,  # 先 Dry-run
    whitelist_patterns=[
        'production.critical_*',  # 關鍵表永不刪除
        'production.*_important',
        '*.customer_*',  # 所有客戶相關表
        '*.financial_*'  # 所有財務相關表
    ],
    blacklist_patterns=[
        'production.temp_*',  # 雖然是臨時表，但先不刪除
        'production.backup_*'  # 備份表不刪除
    ],
    min_create_date=date(2023, 1, 1),  # 只刪除 2023 年前建立的表
    max_last_access_age_days=180,  # 只刪除 180 天未存取的表
    require_confirmation=True,  # 需要互動確認
    estimate_storage_size=True  # 估算可釋放的空間
)

# 執行掃描
tables = get_tables(spark, 'main', 'production', False)
tabledetailsDF = get_tables_details(spark, 'main', 'production', tables)

# 第一步：Dry-run
logger.trace('步驟 1：執行 Dry-run 預覽')
deleted, candidates = drop_table_definition_without_storage_safe(
    spark, tabledetailsDF, logger, config
)

# 第二步：人工審核候選表
logger.trace('步驟 2：人工審核候選表清單')
candidates_df = spark.createDataFrame(candidates)
display(candidates_df.filter(col('action') == 'dry_run_candidate'))

# 第三步：互動確認
if config.require_confirmation:
    confirmed = confirm_deletion_interactive(candidates, config.dry_run)
    if not confirmed:
        logger.trace('使用者取消操作')
        dbutils.notebook.exit('已取消')

# 第四步：實際刪除（需要再次手動設定）
# config.dry_run = False
# deleted, candidates = drop_table_definition_without_storage_safe(
#     spark, tabledetailsDF, logger, config
# )
```

**安全檢查清單：**
- [x] 使用白名單保護關鍵表
- [x] 設定時間條件（建立日期、最後存取時間）
- [x] 啟用互動確認
- [x] 先執行 Dry-run
- [x] 人工審核候選表清單
- [x] 在非高峰時段執行
- [x] 備份重要的 metadata

---

### 案例 5：段階式清理大型 Schema

**場景描述：**
`data_warehouse` schema 有超過 1000 個表，需要分批清理。

**執行步驟：**

```python
# 方法 1：按表名稱前綴分批
prefixes = ['tmp_', 'test_', 'staging_', 'dev_']

for prefix in prefixes:
    logger.trace(f'處理前綴：{prefix}')
    
    # 過濾特定前綴的表
    filtered_df = tabledetailsDF.filter(col('Table').startswith(prefix))
    
    # 建立配置
    config = CleanupConfig(
        dry_run=False,
        max_last_access_age_days=60
    )
    
    # 執行清理
    deleted, candidates = drop_table_definition_without_storage_safe(
        spark, filtered_df, logger, config
    )
    
    logger.trace(f'完成前綴 {prefix}，刪除 {deleted} 個表')

# 方法 2：按建立時間分批
from datetime import date, timedelta

time_ranges = [
    (date(2020, 1, 1), date(2021, 1, 1)),
    (date(2021, 1, 1), date(2022, 1, 1)),
    (date(2022, 1, 1), date(2023, 1, 1)),
]

for start_date, end_date in time_ranges:
    logger.trace(f'處理時間範圍：{start_date} ~ {end_date}')
    
    # 根據時間範圍過濾（需要額外查詢建立時間）
    # ... 過濾邏輯 ...
    
    # 執行清理
    deleted, candidates = drop_table_definition_without_storage_safe(
        spark, filtered_df, logger, config
    )
```

**預期結果：**
```
處理前綴：tmp_
完成前綴 tmp_，刪除 45 個表

處理前綴：test_
完成前綴 test_，刪除 67 個表

...

總計刪除 234 個表（分 4 批完成）
```

---

## 自動化與排程案例

### 案例 6：每日自動清理測試環境

**場景描述：**
建立 Databricks Job，每日自動清理測試環境的孤兒表。

**Job 設定：**

```json
{
  "name": "Daily Cleanup - Test Environment",
  "tasks": [
    {
      "task_key": "cleanup_dev_schema",
      "notebook_task": {
        "notebook_path": "/Repos/your-repo/notebooks/clean_tables_with_dryrun",
        "base_parameters": {
          "store": "hive_metastore",
          "schema": "dev",
          "debug": "False",
          "dry_run": "False",
          "require_confirmation": "False",
          "whitelist": "dev.important_*, dev.baseline_*",
          "blacklist": "",
          "max_last_access_age_days": "30"
        }
      },
      "job_cluster_key": "cleanup_cluster"
    }
  ],
  "job_clusters": [
    {
      "job_cluster_key": "cleanup_cluster",
      "new_cluster": {
        "spark_version": "10.4.x-scala2.12",
        "node_type_id": "Standard_DS3_v2",
        "num_workers": 1,
        "auto_termination_minutes": 15
      }
    }
  ],
  "schedule": {
    "quartz_cron_expression": "0 0 2 * * ?",
    "timezone_id": "Asia/Taipei"
  },
  "email_notifications": {
    "on_failure": ["data-team@company.com"]
  }
}
```

**監控與告警：**

```python
# 在 Notebook 最後加入通知邏輯
if deleted > 50:
    # 如果刪除超過 50 個表，發送通知
    send_notification(
        channel='slack',
        message=f'警告：今日清理了 {deleted} 個表，超過正常範圍'
    )

if any(c['action'] == 'failed' for c in candidates):
    # 如果有刪除失敗，發送告警
    failed_tables = [c for c in candidates if c['action'] == 'failed']
    send_alert(
        level='error',
        message=f'有 {len(failed_tables)} 個表刪除失敗'
    )
```

---

### 案例 7：每週清理生產環境（含審核流程）

**場景描述：**
每週執行一次生產環境清理，但需要人工審核批准。

**執行步驟：**

**Job 1：掃描與報告（每週日執行）**
```python
# 步驟 1：執行 Dry-run 掃描
config = CleanupConfig(
    dry_run=True,
    whitelist_patterns=PRODUCTION_WHITELIST,
    max_last_access_age_days=180
)

deleted, candidates = drop_table_definition_without_storage_safe(
    spark, tabledetailsDF, logger, config
)

# 步驟 2：產生報告
report_df = spark.createDataFrame(candidates)
report_df.write.format('delta').mode('overwrite').saveAsTable('monitoring.cleanup_candidates')

# 步驟 3：發送審核通知
send_approval_request(
    approvers=['data-lead@company.com', 'dba@company.com'],
    report_link=f'https://databricks-workspace.com/sql/dashboards/cleanup-report',
    deadline='3 days'
)
```

**Job 2：執行清理（手動觸發）**
```python
# 由審核者在確認後手動觸發
config = CleanupConfig(
    dry_run=False,
    whitelist_patterns=PRODUCTION_WHITELIST,
    max_last_access_age_days=180
)

# 讀取已審核的候選表
approved_tables = spark.read.table('monitoring.cleanup_candidates_approved')

# 執行清理
deleted, candidates = drop_table_definition_without_storage_safe(
    spark, approved_tables, logger, config
)

# 記錄結果
log_cleanup_result(deleted, candidates, approved_by=dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get())
```

---

## 疑難排解案例

### 案例 8：權限不足

**錯誤訊息：**
```
Error: User does not have DROP privilege on table production.example_table
```

**解決方案：**

1. **檢查使用者權限：**
   ```sql
   SHOW GRANT ON TABLE production.example_table;
   ```

2. **授予必要權限：**
   ```sql
   -- Unity Catalog
   GRANT USE CATALOG ON CATALOG main TO `user@company.com`;
   GRANT USE SCHEMA ON SCHEMA main.production TO `user@company.com`;
   GRANT MODIFY ON SCHEMA main.production TO `user@company.com`;
   
   -- Hive Metastore
   GRANT ALL PRIVILEGES ON DATABASE production TO `user@company.com`;
   ```

3. **或使用具有足夠權限的服務帳號：**
   - 建立服務主體（Service Principal）
   - 授予適當的權限
   - 在 Job 中使用服務帳號執行

---

### 案例 9：儲存路徑存取失敗

**錯誤訊息：**
```
AccessDeniedException: Access denied to path abfss://container@storage.dfs.core.windows.net/data/
```

**解決方案：**

1. **檢查儲存帳號的存取權限：**
   ```python
   # 測試存取
   try:
       dbutils.fs.ls('abfss://container@storage.dfs.core.windows.net/')
       print('✓ 存取成功')
   except Exception as e:
       print(f'✗ 存取失敗：{str(e)}')
   ```

2. **設定儲存帳號認證：**
   ```python
   # 方法 1：使用 Service Principal
   spark.conf.set(
       f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net",
       "OAuth"
   )
   spark.conf.set(
       f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net",
       "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
   )
   spark.conf.set(
       f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net",
       dbutils.secrets.get(scope="azure", key="client-id")
   )
   # ... 其他設定 ...
   
   # 方法 2：使用 SAS Token
   spark.conf.set(
       f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net",
       "SAS"
   )
   spark.conf.set(
       f"fs.azure.sas.token.provider.type.{storage_account}.dfs.core.windows.net",
       "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider"
   )
   ```

3. **或在 Cluster 層級設定：**
   - 在 Cluster Configuration 中設定 Spark Config
   - 或掛載儲存帳號到 DBFS

---

### 案例 10：大量表導致執行時間過長

**問題：**
Schema 中有 2000+ 個表，執行時間超過 1 小時。

**優化方案：**

1. **使用 information_schema 批次查詢：**（參見 [docs/optimization-notes.md](optimization-notes.md)）

2. **分批處理：**
   ```python
   # 按表名稱首字母分批
   import string
   
   for letter in string.ascii_lowercase:
       logger.trace(f'處理表名稱以 {letter} 開頭的表')
       
       filtered_df = tabledetailsDF.filter(
           col('Table').startswith(letter)
       )
       
       if filtered_df.count() > 0:
           deleted, candidates = drop_table_definition_without_storage_safe(
               spark, filtered_df, logger, config
           )
           logger.trace(f'完成 {letter}，刪除 {deleted} 個表')
   ```

3. **只掃描特定條件的表：**
   ```python
   # 使用 information_schema 預先過濾
   query = f"""
   SELECT table_name
   FROM system.information_schema.tables
   WHERE table_catalog = 'main'
     AND table_schema = 'large_schema'
     AND table_type = 'EXTERNAL'
     AND data_source_format IN ('DELTA', 'PARQUET')
     AND created < '2023-01-01'  -- 只處理舊表
   """
   
   old_tables = spark.sql(query)
   # 只對這些舊表執行詳細檢查
   ```

---

## 最佳實務建議

### 1. 執行前的準備工作

- [ ] **備份重要 Metadata**
  ```sql
  -- 匯出表清單和定義
  CREATE TABLE backup.schema_tables_backup AS
  SELECT * FROM system.information_schema.tables
  WHERE table_schema = 'target_schema';
  ```

- [ ] **建立測試資料集**
  ```python
  # 在測試環境中建立幾個測試表
  spark.sql("CREATE TABLE test.orphan_table (id INT) LOCATION 'abfss://test@storage/orphan/'")
  # 刪除儲存資料但保留表定義
  dbutils.fs.rm('abfss://test@storage/orphan/', True)
  # 驗證工具能正確識別並清理
  ```

- [ ] **設定監控與告警**
  - 設定 Job 執行失敗告警
  - 監控刪除數量異常
  - 記錄執行歷史

### 2. 執行時的安全措施

- [ ] **永遠先執行 Dry-run**
  ```python
  # 不要跳過這一步！
  config.dry_run = True
  deleted, candidates = drop_table_definition_without_storage_safe(...)
  # 檢查結果無誤後，再設定 dry_run=False
  ```

- [ ] **使用白名單保護重要的表**
  ```python
  CRITICAL_TABLES = [
      'prod.*',
      'production.*',
      '*.customer_*',
      '*.financial_*',
      '*.legal_*',
      'critical_*'
  ]
  
  config = CleanupConfig(whitelist_patterns=CRITICAL_TABLES)
  ```

- [ ] **在非高峰時段執行**
  - 避免與 ETL pipeline 衝突
  - 減少對使用者的影響
  - 降低 metastore 負載

### 3. 執行後的驗證

- [ ] **檢查刪除結果**
  ```python
  # 檢查是否有失敗的操作
  failed = [c for c in candidates if c['action'] == 'failed']
  if failed:
      print(f'警告：有 {len(failed)} 個表刪除失敗')
      for f in failed:
          print(f"  - {f['table_name']}: {f['reason']}")
  ```

- [ ] **記錄執行歷史**
  ```python
  # 將結果寫入 Delta table
  log_entry = {
      'execution_time': datetime.now(),
      'schema': schema,
      'deleted_count': deleted,
      'skipped_count': len(candidates) - deleted,
      'executed_by': current_user
  }
  
  spark.createDataFrame([log_entry]).write \
      .format('delta').mode('append') \
      .saveAsTable('monitoring.cleanup_history')
  ```

- [ ] **驗證 Schema 狀態**
  ```sql
  -- 檢查 Schema 中剩餘的表
  SELECT COUNT(*) as remaining_tables
  FROM system.information_schema.tables
  WHERE table_schema = 'cleaned_schema';
  ```

---

## 常見錯誤與解決方案

### 錯誤 1：誤刪生產表

**預防措施：**
1. 永遠使用白名單保護生產表
2. 永遠先執行 Dry-run
3. 要求人工審核與確認
4. 在生產環境限制執行權限

**補救措施：**
```sql
-- 如果表定義被刪除但資料仍在
CREATE EXTERNAL TABLE recovered_table
LOCATION 'abfss://original-location/'
USING delta;  -- 或 parquet

-- 從備份恢復表定義
CREATE TABLE schema.table_name AS
SELECT * FROM backup.table_definition
WHERE table_name = 'deleted_table';
```

---

### 錯誤 2：配置錯誤導致大量表被刪除

**預防措施：**
```python
# 設定安全閾值
MAX_ALLOWED_DELETIONS = 50

deleted, candidates = drop_table_definition_without_storage_safe(...)

if deleted > MAX_ALLOWED_DELETIONS:
    raise Exception(f'刪除數量 ({deleted}) 超過安全閾值 ({MAX_ALLOWED_DELETIONS})！')
```

---

### 錯誤 3：並行執行導致衝突

**問題：**
多個 Job 同時清理同一個 Schema。

**解決方案：**
```python
# 使用 Delta table 實作鎖機制
def acquire_lock(schema_name, timeout=300):
    """取得清理鎖"""
    lock_table = 'monitoring.cleanup_locks'
    
    try:
        spark.sql(f"""
        INSERT INTO {lock_table}
        VALUES ('{schema_name}', current_timestamp(), '{current_user}')
        """)
        return True
    except:
        # 鎖已被其他程序持有
        return False

def release_lock(schema_name):
    """釋放清理鎖"""
    spark.sql(f"""
    DELETE FROM monitoring.cleanup_locks
    WHERE schema_name = '{schema_name}'
    """)

# 在清理前取得鎖
if not acquire_lock(schema):
    raise Exception(f'Schema {schema} 正在被其他程序清理')

try:
    # 執行清理
    deleted, candidates = drop_table_definition_without_storage_safe(...)
finally:
    # 確保鎖被釋放
    release_lock(schema)
```

---

## 參考資源

### 內部文件
- [README.md](../README.md) - 完整使用說明
- [docs/system-design.md](system-design.md) - 系統架構說明
- [docs/optimization-notes.md](optimization-notes.md) - 效能優化建議
- [docs/config-examples.md](config-examples.md) - 配置範例
- [docs/developer-guide.md](developer-guide.md) - 開發者指南

### 相關主題
- [Databricks Jobs 文件](https://docs.databricks.com/workflows/jobs/jobs.html)
- [Unity Catalog 權限管理](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/index.html)
- [Delta Lake 最佳實務](https://docs.delta.io/latest/best-practices.html)

---

**版本資訊**
- 文件版本：1.0
- 最後更新：2026-02-12
- 維護者：專案團隊
