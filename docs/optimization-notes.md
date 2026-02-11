# Databricks 執行效能優化建議

本文件針對 Databricks External Tables Cleaner 提出具體的效能與成本優化建議，目標是縮短清理 external tables 的處理時間、減少不必要的運算與 IO 操作。

## 優化建議總覽

| # | 優化項目 | 預期效益 | 實作難度 | 實作狀態 |
|---|---------|---------|---------|---------|
| 1 | 使用 information_schema 批次查詢 | 大幅減少 metastore 查詢次數 | 中 | 未實作 |
| 2 | 智慧化過濾條件 | 減少需要檢查的表數量 | 中 | 未實作 |
| 3 | 併行處理表檢查與刪除 | 縮短整體執行時間 | 高 | 未實作 |
| 4 | 實作 Dry-run 模式 | 避免不必要的重複執行 | 低 | 未實作 |
| 5 | 快取與增量處理 | 避免重複掃描同樣的表 | 中 | 未實作 |
| 6 | 優化 Cluster 配置 | 降低運算成本 | 低 | 部分實作 |
| 7 | 批次 DROP TABLE 操作 | 減少 metastore 交互次數 | 中 | 未實作 |
| 8 | 路徑驗證優化 | 減少不必要的儲存 IO | 中 | 未實作 |

---

## 1. 減少不必要的掃描與操作

### 1.1 使用 information_schema 進行批次查詢

**目前問題：**
- `get_tables_details()` 函式對每個表執行獨立的 `SHOW TABLE EXTENDED` 命令
- 如果 schema 中有 1000 個表，就會執行 1000 次 metastore 查詢
- 每次查詢都有網路往返的延遲

**優化建議：**

使用 Unity Catalog 的 `information_schema.tables` 一次性取得所有表的 metadata：

```python
def get_tables_details_optimized(
        spark: SparkSession,
        store: str,
        schema: str
   ) -> DataFrame:
    """
    使用 information_schema 批次取得表的詳細資訊（優化版本）

    相比原版本，此方法只執行一次查詢，大幅減少 metastore 負擔
    """

    query = f"""
    SELECT
        table_catalog,
        table_schema as database,
        table_name as table,
        table_type as type,
        data_source_format as provider,
        storage_path as location
    FROM system.information_schema.tables
    WHERE table_catalog = '{store}'
      AND table_schema = '{schema}'
      AND table_type = 'EXTERNAL'
      AND data_source_format IN ('DELTA', 'PARQUET')
    """

    tableDetailsDF = spark.sql(query)

    return tableDetailsDF
```

**預期效益：**
- 從 O(n) 次查詢降低到 O(1) 次查詢（n = 表數量）
- 對於 1000 個表的 schema，可節省 99.9% 的 metastore 查詢時間
- 更穩定，不會因網路抖動而累積延遲

**實作狀態：** ❌ 未實作

**注意事項：**
- `information_schema` 需要 Unity Catalog
- 對於傳統 Hive Metastore，需要使用不同的查詢方式

---

### 1.2 智慧化過濾條件：依 Table Metadata 判斷

**目前問題：**
- 程式會檢查 schema 中的所有 external tables
- 沒有考慮表的建立時間、最後存取時間等資訊
- 可能檢查許多不需要清理的表

**優化建議：**

加入多種過濾條件，只檢查真正可能需要清理的表：

```python
def get_tables_to_clean(
        spark: SparkSession,
        store: str,
        schema: str,
        filters: dict = None
   ) -> DataFrame:
    """
    根據多種條件過濾需要清理的表

    Parameters
    ----------
    filters: dict
        過濾條件字典，可包含：
        - created_before: 只檢查在此日期之前建立的表（格式：'YYYY-MM-DD'）
        - name_pattern: 表名稱需符合的 regex pattern（例如：'tmp_.*' 只檢查臨時表）
        - comment_contains: 表的 comment 欄位包含特定關鍵字（例如：'deprecated'）
        - not_accessed_days: 超過 N 天未被存取的表
    """

    base_query = f"""
    SELECT
        table_schema as database,
        table_name as table,
        data_source_format as provider,
        table_type as type,
        storage_path as location,
        created,
        last_altered
    FROM system.information_schema.tables
    WHERE table_catalog = '{store}'
      AND table_schema = '{schema}'
      AND table_type = 'EXTERNAL'
      AND data_source_format IN ('DELTA', 'PARQUET')
    """

    # 加入過濾條件
    conditions = []

    if filters:
        if 'created_before' in filters:
            conditions.append(f"created < '{filters['created_before']}'")

        if 'name_pattern' in filters:
            conditions.append(f"table_name RLIKE '{filters['name_pattern']}'")

        if 'not_accessed_days' in filters:
            # 需要額外查詢 table access logs（如果有啟用）
            days = filters['not_accessed_days']
            conditions.append(f"last_altered < date_sub(current_date(), {days})")

    if conditions:
        base_query += " AND " + " AND ".join(conditions)

    return spark.sql(base_query)
```

**使用範例：**

```python
# 只清理符合以下條件的表：
# 1. 建立日期超過 90 天
# 2. 表名稱以 'tmp_' 或 'test_' 開頭
# 3. 超過 30 天未被修改

filters = {
    'created_before': '2024-11-01',
    'name_pattern': '^(tmp_|test_).*',
    'not_accessed_days': 30
}

tables_to_check = get_tables_to_clean(spark, store, schema, filters)
```

**預期效益：**
- 可將需要檢查的表數量減少 50-90%（視實際情況而定）
- 大幅縮短執行時間
- 降低對生產環境表的影響風險

**實作狀態：** ❌ 未實作

---

### 1.3 支援白名單與黑名單機制

**優化建議：**

```python
def apply_whitelist_blacklist(
        df: DataFrame,
        whitelist: list = None,
        blacklist: list = None
   ) -> DataFrame:
    """
    根據白名單和黑名單過濾表

    Parameters
    ----------
    whitelist: list
        如果提供，只處理名單中的表（支援萬用字元 *）
    blacklist: list
        如果提供，排除名單中的表（支援萬用字元 *）
    """

    if blacklist:
        # 排除黑名單中的表
        for pattern in blacklist:
            df = df.filter(~col('table').rlike(pattern))

    if whitelist:
        # 只保留白名單中的表
        conditions = [col('table').rlike(pattern) for pattern in whitelist]
        df = df.filter(reduce(lambda a, b: a | b, conditions))

    return df
```

**使用範例：**

```python
# 排除所有生產環境的表
blacklist = ['prod_.*', 'production_.*', '.*_prd']

# 只處理測試相關的表
whitelist = ['test_.*', 'tmp_.*', 'dev_.*']

filtered_df = apply_whitelist_blacklist(tableDetailsDF, whitelist, blacklist)
```

**實作狀態：** ❌ 未實作

---

## 2. 批次與併行處理

### 2.1 Spark DataFrame 操作實現併行檢查

**目前問題：**
- `drop_table_definition_without_storage()` 使用 `df.collect()` 將所有資料收集到 driver
- 使用 Python for loop 逐一處理，完全是單執行緒執行
- 無法利用 Spark 的分散式運算能力

**優化建議：**

使用 Spark UDF 和 DataFrame 操作實現併行處理：

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType, StringType, StructType, StructField

def create_check_storage_udf(spark):
    """
    建立 UDF 用於檢查儲存是否存在
    """

    def check_storage_exists(provider: str, location: str) -> dict:
        """
        檢查指定位置的儲存是否存在

        Returns
        -------
        dict: {'exists': bool, 'error': str}
        """
        try:
            if provider.lower() == 'delta':
                from delta.tables import DeltaTable
                exists = DeltaTable.isDeltaTable(spark, location)
            else:  # parquet
                from pyspark.dbutils import DBUtils
                dbutils = DBUtils(spark)
                dbutils.fs.ls(location)
                exists = True
        except Exception as e:
            exists = False

        return {'exists': exists, 'should_delete': not exists}

    schema = StructType([
        StructField('exists', BooleanType(), True),
        StructField('should_delete', BooleanType(), True)
    ])

    return udf(check_storage_exists, schema)

def drop_tables_parallel(
        spark: SparkSession,
        df: DataFrame,
        log: logs,
        max_parallelism: int = 10
   ) -> int:
    """
    使用 Spark 併行處理表的檢查與刪除（優化版本）

    Parameters
    ----------
    max_parallelism: int
        最大併行度，避免對 metastore 造成過大壓力
    """

    # 步驟 1：併行檢查所有表的儲存狀態
    check_udf = create_check_storage_udf(spark)

    df_with_check = df.withColumn(
        'check_result',
        check_udf(col('Provider'), col('Location'))
    ).withColumn(
        'should_delete',
        col('check_result.should_delete')
    )

    # 步驟 2：只保留需要刪除的表
    tables_to_delete = df_with_check.filter(col('should_delete') == True)

    # 步驟 3：將結果收集到 driver（數量應該大幅減少）
    delete_list = tables_to_delete.select('Database', 'Table').collect()

    log.trace(f'找到 {len(delete_list)} 個需要刪除的表')

    # 步驟 4：批次刪除表
    deleted = 0
    batch_size = max_parallelism

    for i in range(0, len(delete_list), batch_size):
        batch = delete_list[i:i+batch_size]

        for row in batch:
            try:
                spark.sql(f'DROP TABLE IF EXISTS {row.Database}.{row.Table}')
                log.trace(f'已刪除表: {row.Database}.{row.Table}')
                deleted += 1
            except Exception as e:
                log.trace(f'刪除表 {row.Database}.{row.Table} 失敗: {str(e)}')

    return deleted
```

**預期效益：**
- 儲存檢查階段可以完全併行執行
- 對於 1000 個表，可從數小時縮短到數分鐘
- 更好地利用 Databricks cluster 的運算資源

**實作狀態：** ❌ 未實作

**注意事項：**
- UDF 中的操作可能無法存取 driver 端的變數
- 需要注意 metastore 的併行限制（避免過多同時請求）

---

### 2.2 批次 DROP TABLE 操作

**優化建議：**

```python
def batch_drop_tables(
        spark: SparkSession,
        tables_to_drop: list,
        batch_size: int = 50
   ) -> dict:
    """
    批次執行 DROP TABLE 操作

    Parameters
    ----------
    batch_size: int
        每批次處理的表數量，建議 20-100 之間

    Returns
    -------
    dict: {'success': int, 'failed': list}
    """

    success_count = 0
    failed_tables = []

    for i in range(0, len(tables_to_drop), batch_size):
        batch = tables_to_drop[i:i+batch_size]

        # 建立批次 SQL 語句
        drop_statements = []
        for table in batch:
            drop_statements.append(f"DROP TABLE IF EXISTS {table['database']}.{table['table']}")

        # 執行批次操作
        try:
            for stmt in drop_statements:
                spark.sql(stmt)
                success_count += 1
        except Exception as e:
            # 記錄失敗的表
            failed_tables.extend(batch)
            print(f"批次刪除失敗: {str(e)}")

    return {
        'success': success_count,
        'failed': failed_tables
    }
```

**實作狀態：** ❌ 未實作

---

## 3. 減少 IO 與儲存操作風險

### 3.1 實作 Dry-run 模式

**目前問題：**
- 每次執行都會實際刪除表
- 無法預覽將被刪除的表清單
- 如果設定錯誤，可能造成不可逆的損失

**優化建議：**

```python
def drop_table_definition_without_storage_v2(
        spark: SparkSession,
        df: DataFrame,
        log: logs,
        dry_run: bool = True,
        output_path: str = None
   ) -> dict:
    """
    清理表定義（支援 dry-run 模式）

    Parameters
    ----------
    dry_run: bool
        True: 只列出將被刪除的表，不實際刪除
        False: 實際執行刪除操作
    output_path: str
        將清理報告儲存到指定的 Delta table 路徑

    Returns
    -------
    dict: {
        'total_checked': int,
        'to_delete': int,
        'deleted': int,
        'skipped': int,
        'failed': int,
        'tables_to_delete': list
    }
    """

    result = {
        'total_checked': 0,
        'to_delete': 0,
        'deleted': 0,
        'skipped': 0,
        'failed': 0,
        'tables_to_delete': []
    }

    tables_info = []

    for row in df.collect():
        result['total_checked'] += 1

        should_delete = False
        reason = ""

        # 檢查邏輯（與原版相同）
        if row.Provider.lower() == 'delta':
            if not DeltaTable.isDeltaTable(spark, row.Location):
                should_delete = True
                reason = "Delta table location 不存在或無效"
        else:  # parquet
            if not file_exists(spark, row.Location):
                should_delete = True
                reason = "Parquet 檔案路徑不存在"

        table_info = {
            'database': row.Database,
            'table': row.Table,
            'provider': row.Provider,
            'location': row.Location,
            'should_delete': should_delete,
            'reason': reason
        }

        if should_delete:
            result['to_delete'] += 1
            result['tables_to_delete'].append(table_info)

            if dry_run:
                log.trace(f'[DRY-RUN] 將刪除表: {row.Database}.{row.Table} - 原因: {reason}')
            else:
                try:
                    spark.sql(f'DROP TABLE {row.Database}.{row.Table}')
                    log.trace(f'已刪除表: {row.Database}.{row.Table}')
                    result['deleted'] += 1
                    table_info['deleted'] = True
                except Exception as e:
                    log.trace(f'刪除失敗: {row.Database}.{row.Table} - 錯誤: {str(e)}')
                    result['failed'] += 1
                    table_info['deleted'] = False
                    table_info['error'] = str(e)
        else:
            result['skipped'] += 1

        tables_info.append(table_info)

    # 儲存清理報告
    if output_path:
        report_df = spark.createDataFrame(tables_info)
        report_df.write.format('delta').mode('append').save(output_path)
        log.trace(f'清理報告已儲存至: {output_path}')

    return result
```

**使用範例：**

```python
# 第一次執行：dry-run 模式，預覽將被刪除的表
result = drop_table_definition_without_storage_v2(
    spark,
    tabledetailsDF,
    logger,
    dry_run=True,
    output_path='/mnt/logs/table_cleanup_reports'
)

print(f"將刪除 {result['to_delete']} 個表")
print(f"詳細清單: {result['tables_to_delete']}")

# 確認無誤後，執行實際刪除
result = drop_table_definition_without_storage_v2(
    spark,
    tabledetailsDF,
    logger,
    dry_run=False,
    output_path='/mnt/logs/table_cleanup_reports'
)
```

**預期效益：**
- 避免誤刪重要的表
- 可以事先審核刪除清單
- 減少不必要的重複執行

**實作狀態：** ❌ 未實作

---

### 3.2 路徑驗證與安全檢查

**優化建議：**

```python
def validate_location_path(location: str, allowed_prefixes: list) -> bool:
    """
    驗證 location 路徑是否安全

    Parameters
    ----------
    allowed_prefixes: list
        允許的路徑前綴，例如：
        ['abfss://external@', 's3://my-external-bucket/']

    Returns
    -------
    bool: 路徑是否安全
    """

    # 檢查是否為外部路徑
    if not location:
        return False

    # 檢查是否符合允許的前綴
    for prefix in allowed_prefixes:
        if location.startswith(prefix):
            return True

    return False

def enhanced_safety_check(row: Row, config: dict) -> dict:
    """
    增強的安全檢查

    Returns
    -------
    dict: {'safe_to_delete': bool, 'warnings': list}
    """

    warnings = []
    safe = True

    # 1. 確認是 EXTERNAL 表
    if row.Type.upper() != 'EXTERNAL':
        warnings.append(f"表類型為 {row.Type}，不是 EXTERNAL")
        safe = False

    # 2. 驗證 location 路徑
    if not validate_location_path(row.Location, config.get('allowed_prefixes', [])):
        warnings.append(f"Location 路徑 {row.Location} 不在允許的範圍內")
        safe = False

    # 3. 檢查表名稱是否在保護清單中
    protected_patterns = config.get('protected_patterns', [])
    for pattern in protected_patterns:
        if re.match(pattern, row.Table):
            warnings.append(f"表名稱符合保護模式 {pattern}")
            safe = False

    return {
        'safe_to_delete': safe,
        'warnings': warnings
    }
```

**實作狀態：** ❌ 未實作

---

## 4. 利用 Databricks 平台特性

### 4.1 整合 Table Access Logs

**優化建議：**

如果 Databricks workspace 有啟用 Unity Catalog 的 audit logs，可以利用存取記錄來判斷表是否仍在使用：

```python
def get_table_last_access_time(
        spark: SparkSession,
        catalog: str,
        schema: str,
        table: str,
        days_to_check: int = 90
   ) -> datetime:
    """
    從 audit logs 查詢表的最後存取時間

    Returns
    -------
    datetime: 最後存取時間，如果從未被存取則回傳 None
    """

    query = f"""
    SELECT MAX(event_time) as last_access
    FROM system.access.audit
    WHERE action_name IN ('READ', 'SELECT', 'DESCRIBE')
      AND request_params.table_full_name = '{catalog}.{schema}.{table}'
      AND event_date >= date_sub(current_date(), {days_to_check})
    """

    result = spark.sql(query).collect()

    if result and result[0].last_access:
        return result[0].last_access
    else:
        return None

def filter_by_last_access(
        spark: SparkSession,
        df: DataFrame,
        days_threshold: int = 90
   ) -> DataFrame:
    """
    只保留超過指定天數未被存取的表
    """

    # 此處需要使用 UDF 或將 access log 查詢結果 join 到表清單
    # 實作細節略

    pass
```

**預期效益：**
- 更精準地識別不再使用的表
- 避免刪除仍在使用但儲存路徑有問題的表

**實作狀態：** ❌ 未實作

**前提條件：**
- 需要 Unity Catalog
- 需要啟用 audit logging
- 需要對 `system.access.audit` 有查詢權限

---

### 4.2 利用 Table Properties 和 Tags

**優化建議：**

在建立表時加入自訂的 properties 或 tags，用於清理策略：

```python
# 建立表時加入 metadata
spark.sql("""
CREATE TABLE IF NOT EXISTS my_catalog.my_schema.temp_table (id INT)
LOCATION 'abfss://...'
TBLPROPERTIES (
  'retention_days' = '30',
  'cleanup_policy' = 'auto',
  'owner' = 'data-team'
)
""")

# 清理時讀取這些 properties
def get_tables_with_cleanup_policy(spark, catalog, schema):
    query = f"""
    SELECT
        table_name,
        tbl_properties['retention_days'] as retention_days,
        tbl_properties['cleanup_policy'] as policy,
        created
    FROM system.information_schema.tables
    WHERE table_catalog = '{catalog}'
      AND table_schema = '{schema}'
      AND tbl_properties['cleanup_policy'] = 'auto'
    """

    return spark.sql(query)
```

**實作狀態：** ❌ 未實作

---

## 5. 運算與成本優化

### 5.1 快取與增量處理

**優化建議：**

```python
def incremental_cleanup(
        spark: SparkSession,
        catalog: str,
        schema: str,
        checkpoint_path: str,
        log: logs
   ):
    """
    增量清理：只處理自上次執行後新增或變更的表

    Parameters
    ----------
    checkpoint_path: str
        儲存檢查點資料的 Delta table 路徑
    """

    # 1. 取得當前所有表的清單
    current_tables = get_tables_details_optimized(spark, catalog, schema)

    # 2. 讀取上次執行的檢查點
    try:
        last_checkpoint = spark.read.format('delta').load(checkpoint_path)

        # 3. 找出新增的表（差集）
        new_tables = current_tables.join(
            last_checkpoint,
            on=['database', 'table'],
            how='left_anti'
        )

        log.trace(f'找到 {new_tables.count()} 個新表需要檢查')

        # 4. 只處理新表
        result = drop_table_definition_without_storage(spark, new_tables, log)

    except Exception as e:
        # 第一次執行，沒有檢查點
        log.trace('找不到檢查點，執行完整掃描')
        result = drop_table_definition_without_storage(spark, current_tables, log)

    # 5. 更新檢查點
    current_tables.write.format('delta').mode('overwrite').save(checkpoint_path)

    return result
```

**預期效益：**
- 大幅減少重複掃描
- 適合定期排程執行（例如每日或每週）

**實作狀態：** ❌ 未實作

---

### 5.2 建議的 Cluster 配置

**小型環境（< 100 個表）：**
```
- Worker 類型: Standard_DS3_v2 (AWS: m5.xlarge)
- Workers 數量: 1
- Driver 類型: 與 worker 相同
- Databricks Runtime: 10.4 LTS 或更高
- Auto-termination: 15 分鐘
- 預估成本: < $1 per run
```

**中型環境（100-1000 個表）：**
```
- Worker 類型: Standard_DS4_v2 (AWS: m5.2xlarge)
- Workers 數量: 2-4
- Driver 類型: 與 worker 相同
- Databricks Runtime: 10.4 LTS 或更高
- Auto-termination: 30 分鐘
- 預估成本: $2-5 per run
```

**大型環境（> 1000 個表）：**
```
- Worker 類型: Standard_DS5_v2 (AWS: m5.4xlarge)
- Workers 數量: 4-8
- Driver 類型: 與 worker 相同或更大
- Databricks Runtime: 10.4 LTS 或更高
- Auto-termination: 60 分鐘
- 啟用 Autoscaling: min 2, max 8
- 預估成本: $5-15 per run
```

**成本優化建議：**
1. 使用 Spot/Preemptible instances（可節省 60-80% 成本）
2. 在非高峰時段執行（部分雲端供應商有離峰優惠）
3. 使用 Job cluster 而非 All-purpose cluster（更便宜）
4. 設定合理的 auto-termination 時間

**實作狀態：** ⚠️ 部分建議（需要在 Job 設定中手動配置）

---

### 5.3 執行排程建議

**建議排程頻率：**

| 環境類型 | 建議頻率 | 說明 |
|---------|---------|------|
| 開發環境 (Dev) | 每日 | 開發環境變化快，表的建立和刪除頻繁 |
| 測試環境 (QA/Staging) | 每週 | 測試完成後通常會留下許多臨時表 |
| 生產環境 (Production) | 每月 | 生產環境應該更謹慎，且表的變化較少 |

**最佳執行時間：**
- 選擇業務低峰時段（例如：週末凌晨）
- 避免與 ETL pipeline 或重要報表執行時間衝突
- 考慮 metastore 的維護時間窗口

**實作狀態：** ⚠️ 部分建議（需要在 Job scheduling 中手動設定）

---

## 6. 監控與可觀測性

### 6.1 建立清理儀表板

**優化建議：**

將清理結果記錄到 Delta table，並建立 Databricks SQL 儀表板：

```python
def log_cleanup_metrics(
        spark: SparkSession,
        result: dict,
        log_table: str = 'monitoring.table_cleanup_logs'
   ):
    """
    記錄清理作業的指標
    """

    from datetime import datetime
    import json

    log_entry = {
        'execution_time': datetime.now(),
        'catalog': result.get('catalog'),
        'schema': result.get('schema'),
        'total_checked': result.get('total_checked'),
        'deleted': result.get('deleted'),
        'skipped': result.get('skipped'),
        'failed': result.get('failed'),
        'execution_duration_seconds': result.get('duration'),
        'tables_deleted': json.dumps(result.get('tables_to_delete', []))
    }

    log_df = spark.createDataFrame([log_entry])
    log_df.write.format('delta').mode('append').saveAsTable(log_table)
```

**儀表板 SQL 範例：**

```sql
-- 每日清理趨勢
SELECT
    date(execution_time) as date,
    SUM(deleted) as total_deleted,
    SUM(total_checked) as total_checked,
    AVG(execution_duration_seconds) as avg_duration
FROM monitoring.table_cleanup_logs
WHERE execution_time >= date_sub(current_date(), 30)
GROUP BY date(execution_time)
ORDER BY date DESC;

-- 各 Schema 的清理統計
SELECT
    schema,
    COUNT(*) as executions,
    SUM(deleted) as total_deleted,
    SUM(failed) as total_failed
FROM monitoring.table_cleanup_logs
WHERE execution_time >= date_sub(current_date(), 90)
GROUP BY schema
ORDER BY total_deleted DESC;
```

**實作狀態：** ❌ 未實作

---

## 7. 總結與優先級建議

### 高優先級（立即實作）

1. **實作 Dry-run 模式** (優化項目 #4)
   - 風險最低
   - 實作簡單
   - 立即提升安全性

2. **使用 information_schema 批次查詢** (優化項目 #1)
   - 效益最大
   - 適用於所有規模的環境

3. **加入基本的智慧過濾** (優化項目 #2)
   - 例如：依建立日期、表名稱模式過濾
   - 可大幅減少不必要的掃描

### 中優先級（近期規劃）

4. **實作錯誤處理與重試機制**
   - 提升穩定性
   - 避免單一失敗中斷整個流程

5. **加入結構化日誌與監控** (優化項目 #6)
   - 便於追蹤和審計
   - 建立 Delta table 記錄執行歷史

6. **優化 Cluster 配置**
   - 根據實際負載調整
   - 使用 Spot instances 降低成本

### 低優先級（長期優化）

7. **併行處理實作** (優化項目 #3)
   - 實作複雜度高
   - 需要充分測試
   - 適合處理大量表的場景

8. **整合 Table Access Logs** (優化項目 #4.1)
   - 需要 Unity Catalog 和特定權限
   - 提供更智慧的清理決策

---

## 附錄：效能測試基準

建議在實作優化後進行效能測試，記錄以下指標：

| 指標 | 優化前 | 優化後 | 改善幅度 |
|-----|--------|--------|---------|
| 處理 100 個表的時間 | ___ 分鐘 | ___ 分鐘 | ___% |
| 處理 1000 個表的時間 | ___ 分鐘 | ___ 分鐘 | ___% |
| Metastore 查詢次數 | ___ 次 | ___ 次 | ___% |
| Cluster 運算成本 | $___ | $___ | ___% |
| 儲存 IO 操作次數 | ___ 次 | ___ 次 | ___% |

透過持續測量和優化，可以找到最適合您環境的配置。
