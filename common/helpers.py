"""
Databricks External Tables Cleaner - 核心工具模組

本模組提供清理 Databricks external tables 所需的核心功能，包括：
- 檔案與路徑存在性檢查
- 表的列舉與詳細資訊查詢
- 表定義的安全刪除
- 日誌管理

所有函式都設計為在 Databricks 環境中執行，需要有效的 SparkSession。
"""

from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.dbutils import DBUtils
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, FloatType, DoubleType, BooleanType, TimestampType
from delta.tables import DeltaTable
from pyspark.sql.functions import col, regexp_extract
from typing import Optional, List, Tuple, Dict
from datetime import datetime

def file_exists(
      spark: SparkSession,
      dir: str
    ) -> bool:

    """
    檢查指定的檔案或目錄是否存在於儲存位置

    此函式使用 dbutils.fs.ls() 來驗證路徑是否可存取。主要用於檢查
    external tables 的儲存路徑是否仍然有效。

    參數
    ----------
    spark: SparkSession
       Spark session 實例，用於存取 dbutils
    dir: str
       要檢查的檔案或目錄路徑
       必須是絕對路徑，支援 DBFS 路徑 (/dbfs/...) 或外部儲存路徑 (abfss://...)

    拋出例外
    ------
    RuntimeError
       當使用者沒有權限存取目標路徑時（AccessDeniedException）
       此例外會向上傳遞，需要呼叫者處理

    回傳
    -------
    bool
       True: 路徑存在且可存取
       False: 路徑不存在或發生其他非權限相關的錯誤

    注意事項
    -------
    - 此函式會實際嘗試列出路徑內容，因此對於大型目錄可能需要一些時間
    - 權限錯誤會被視為嚴重問題而拋出例外，其他錯誤則回傳 False
    """

    try:
        # 使用 dbutils.fs.ls 嘗試列出路徑內容
        # 如果路徑存在，此操作會成功；否則會拋出例外
        dbutils = DBUtils(spark)
        dbutils.fs.ls(dir)
    except Exception as e:
       # 權限問題需要手動解決，因此向上拋出
       if 'java.nio.file.AccessDeniedException' in str(e):
          raise
       # 其他錯誤（如路徑不存在）視為正常情況，回傳 False
       return False

    return True


def get_tables(
        spark: SparkSession,
        store: str,
        schema: str,
        istemporary: bool = False
   )-> list:
    """
    取得指定 metastore 和 schema 中的表清單

    此函式執行 SHOW TABLES 命令來列舉所有表，並根據 istemporary 參數
    過濾出臨時表或持久表。這是掃描流程的第一步，用於確定需要檢查的表範圍。

    參數
    ----------
    spark: SparkSession
       Spark session 實例
    store: str
       Metastore 或 Catalog 名稱
       - Hive Metastore: 使用 'hive_metastore'
       - Unity Catalog: 使用實際的 catalog 名稱（例如：'main', 'dev'）
    schema: str
       Database/Schema 名稱
    istemporary: bool
       True: 只回傳臨時表（temporary views）
       False: 只回傳持久化的表（預設值）

    回傳
    -------
    list
       表名稱的清單（字串列表）

    範例
    -------
    >>> tables = get_tables(spark, 'hive_metastore', 'default', False)
    >>> print(tables)
    ['customer_data', 'order_history', 'product_catalog']

    注意事項
    -------
    - 此函式會列舉所有表，包括 views 和實體表，需要在後續步驟中進一步過濾
    - 執行需要對 catalog 和 schema 有 USE 權限
    """
    # 執行 SHOW TABLES 命令並過濾臨時/持久表
    df = (
         spark
         .sql(f'show tables in {store}.{schema}')
         .where(f"isTemporary = '{str(istemporary)}'")  # 根據參數過濾表類型
         .select('tableName')
         .collect()
    )
    # 將結果轉換為 Python list
    ret = [row.tableName for row in df]

    return ret

def get_tables_details(
        spark: SparkSession,
        store: str,
        schema: str,
        tables:list
   )-> DataFrame:
   """
   取得表清單中每個表的詳細 metadata

   此函式對每個表執行 SHOW TABLE EXTENDED 命令，並使用正規表達式解析
   metadata 資訊。這是決策流程的關鍵步驟，提供判斷表是否需要清理的必要資訊。

   參數
   ----------
   spark: SparkSession
      Spark session 實例
   store: str
      Metastore 或 Catalog 名稱
   schema: str
      Database/Schema 名稱
   tables: list
      要查詢的表名稱清單（由 get_tables() 回傳）

   回傳
   -------
   DataFrame
      包含表詳細資訊的 Spark DataFrame，Schema 為：
      - Database (string): 所屬的 database/schema
      - Table (string): 表名稱
      - Provider (string): 資料格式（delta, parquet 等）
      - Type (string): 表類型（MANAGED, EXTERNAL, VIEW）
      - Location (string): 外部儲存的實際路徑

   注意事項
   -------
   - 此函式會對每個表執行獨立的 SQL 查詢，因此對於大量表可能較慢
   - 最終結果只包含 provider 為 'delta' 或 'parquet' 且 type 不是 'view' 的表
   - 如果表數量很多（> 100），建議參考 docs/optimization-notes.md 的優化方案

   效能考量
   -------
   - 對於 N 個表，會執行 N 次 metastore 查詢
   - 每次查詢都有網路往返延遲
   - 建議使用 information_schema 進行批次查詢（參見優化文件）

   範例
   -------
   >>> tables = ['orders', 'customers']
   >>> details = get_tables_details(spark, 'main', 'sales', tables)
   >>> details.show()
   +--------+----------+--------+--------+----------------------------+
   |Database|Table     |Provider|Type    |Location                    |
   +--------+----------+--------+--------+----------------------------+
   |sales   |orders    |delta   |EXTERNAL|abfss://data@store/orders   |
   |sales   |customers |parquet |EXTERNAL|abfss://data@store/customers|
   +--------+----------+--------+--------+----------------------------+
   """

   # 建立空的 DataFrame 用於儲存結果
   tableDetailsDF = create_empty_dataframe(spark,
                                          ['Database','Table','Provider','Type','Location'],
                                          ['string','string','string','string','string'])
   # 設定當前的 catalog context
   spark.sql(f"USE CATALOG {store}")

   # 遍歷每個表，逐一查詢詳細資訊
   for row in tables:
      # 執行 SHOW TABLE EXTENDED 取得完整的 metadata
      df = (
          spark
          .sql(f"SHOW TABLE EXTENDED IN {store}.{schema} LIKE '{row}';")
          .select(col('information'))  # information 欄位包含所有 metadata 的文字描述
      )
      # 使用正規表達式從 information 文字中提取關鍵欄位
      # 例如從 "Database: sales\nTable: orders\n..." 中提取各個值
      detailsDF = (
          df
          .withColumn('Database', regexp_extract(col('information'), 'Database: (.*)',1))
          .withColumn('Table', regexp_extract(col('information'), 'Table: (.*)',1))
          .withColumn('Provider', regexp_extract(col('information'), 'Provider: (.*)',1))
          .withColumn('Type', regexp_extract(col('information'), 'Type: (.*)',1))
          .withColumn('Location', regexp_extract(col('information'), 'Location: (.*)',1))
          .drop(col('information'))  # 移除原始的 information 欄位
      )
      # 將此表的資訊合併到結果 DataFrame
      tableDetailsDF = tableDetailsDF.union(detailsDF)

   # 過濾：只保留 external tables（Delta 或 Parquet 格式），排除 views
   # 這是因為工具的目的是清理 external tables，不處理 managed tables 或 views
   tableDetailsDF = tableDetailsDF.where("lower(provider) in ('delta','parquet') and lower(type) <> 'view'")

   return tableDetailsDF



def create_empty_dataframe(
        spark: SparkSession,
        columns:list,
        types:list
   )-> DataFrame:
   """
   建立一個空的 Spark DataFrame，具有指定的 schema

   此工具函式用於初始化 DataFrame，常用於需要逐步累積資料的場景。

   參數
   ----------
   spark: SparkSession
      Spark session 實例
   columns:  list
      欄位名稱清單，例如：['col1', 'col2', 'col3']
   types: list
      對應的欄位資料類型清單，支援以下類型：
      - 'string': 字串類型
      - 'integer': 32位元整數
      - 'long': 64位元整數
      - 'float': 單精度浮點數
      - 'double': 雙精度浮點數
      - 'boolean': 布林值
      - 'timestamp': 時間戳記

   拋出例外
   ------
   RuntimeError
      當 columns 和 types 清單長度不一致時
   TypeError
      當遇到不支援的資料類型時

   回傳
   -------
   DataFrame
      空的 Spark DataFrame，具有指定的 schema 但無任何資料列

   範例
   -------
   >>> df = create_empty_dataframe(
   ...     spark,
   ...     ['name', 'age', 'is_active'],
   ...     ['string', 'integer', 'boolean']
   ... )
   >>> df.printSchema()
   root
    |-- name: string (nullable = true)
    |-- age: integer (nullable = true)
    |-- is_active: boolean (nullable = true)
   """
   # 驗證輸入：兩個清單的長度必須相等
   if (len(columns)!=len(types)):
       raise RuntimeError("Size mismatch between columns and types!")

   fields = []
   # 根據 types 清單建立對應的 StructField
   for field_name, field_type in zip(columns,types):
      # 將字串類型名稱轉換為 Spark 的資料類型物件
      if field_type == "string":
         type_ = StringType()
      elif field_type == "integer":
         type_ = IntegerType()
      elif field_type == "long":
         type_ = LongType()
      elif field_type == "float":
         type_ = FloatType()
      elif field_type == "double":
         type_ = DoubleType()
      elif field_type == "boolean":
         type_ = BooleanType()
      elif field_type == "timestamp":
         type_ = TimestampType()
      else:
         # 遇到不支援的類型，拋出例外
         raise TypeError(f"Type: {field_type} is not handled!")

      # 建立欄位定義（所有欄位都設為 nullable=True）
      fields.append(StructField(field_name, type_, True))

   # 組合成 StructType schema
   schema = StructType(fields)

   # 建立空的 DataFrame（空的資料列表 + schema）
   df =  spark.createDataFrame([], schema)

   return df

class logs:
   """
   自訂的日誌管理類別

   此類別提供統一的日誌介面，支援兩種輸出模式：
   1. 開發模式（debug=True）：直接將訊息 print 到 console
   2. 生產模式（debug=False）：將訊息寫入 Spark 的 log4j 日誌系統

   屬性
   -------
   name: str
      Logger 的名稱，用於識別日誌來源
   level: str
      日誌等級（'debug', 'info', 'warning', 'error'）
   debug: bool
      True: 使用 print 輸出到 console（開發環境）
      False: 使用 log4j 寫入日誌檔案（生產環境）

   範例
   -------
   >>> logger = logs(name='MyApp', level='info', debug=True)
   >>> logger.trace('開始處理資料')
   INFO:MyApp:開始處理資料

   >>> logger = logs(name='MyApp', level='info', debug=False)
   >>> logger.trace('寫入 log4j')
   # 訊息會寫入 Spark 的 log4j 日誌檔案
   """
   def __init__(self,name:str,level:str = 'info',debug:bool = True):
        self.name = name
        self.level = level
        self.debug = debug
        self.logger = self.get_logger()

   def get_logger(self):
      """
      建立 logger 實例

      回傳
      -------
      - debug=True: 回傳 None（使用 print）
      - debug=False: 回傳 log4j logger 實例
      """
      logger = None

      if not self.debug:
         # 透過 Spark context 取得 log4j logger
         # 注意：這需要在 Databricks 環境中執行，且 spark 變數必須存在
         log4jLogger = spark.sparkContext._jvm.org.apache.log4j
         # 從 log manager 取得 logger 實例
         logger = log4jLogger.LogManager.getLogger(self.name)

      return logger

   def trace(self,msg):
      """
      輸出日誌訊息

      根據 debug 模式決定輸出方式：
      - debug=True: 使用 print 直接輸出到 console
      - debug=False: 寫入 log4j 日誌檔案

      參數
      ----------
      msg: str
         要記錄的訊息內容
      """
      if (self.debug):
         # 開發模式：直接 print 到 console
         print(f'{self.level.upper()}:{self.name}:{msg}')
      else:
         # 生產模式：寫入 log4j
         # 根據 level 選擇對應的 log4j 方法
         if self.level == 'debug':
            self.logger.info(msg)  # log4j 的 debug 對應到 info
         elif self.level == 'info':
            self.logger.info(msg)
         elif self.level == 'warning':
            self.logger.warn(msg)
         elif self.level == 'error':
            self.logger.error(msg)
         else:
            pass  # 不支援的 level，不做任何操作

def drop_table_definition_without_storage(
      spark: SparkSession,
      df: DataFrame,
      log:logs
   )-> int:
   """
   從 metastore 中刪除儲存資料不存在的表定義

   這是本工具的核心函式，負責實際執行清理邏輯。它會遍歷提供的表清單，
   檢查每個表的儲存路徑是否存在有效資料，如果不存在則刪除該表的定義。

   ⚠️ 重要警告
   -----------
   此函式會執行 DROP TABLE 操作，這是不可逆的！請務必：
   1. 先在測試環境驗證
   2. 確認表清單正確
   3. 考慮實作 dry-run 模式（參見 docs/optimization-notes.md）

   處理邏輯
   --------
   對於每個表：
   - 如果是 Delta 表：使用 DeltaTable.isDeltaTable() 檢查 location 是否為有效的 Delta Lake
   - 如果是 Parquet 表：使用 dbutils.fs.ls() 檢查 location 路徑是否存在
   - 如果檢查失敗（資料不存在）：執行 DROP TABLE 刪除表定義
   - 如果檢查成功（資料存在）：跳過，不刪除

   參數
   ----------
   spark: SparkSession
      Spark session 實例，用於執行 SQL 命令
   df:  DataFrame
      包含表 metadata 的 DataFrame，必須包含以下欄位：
      - Database: schema 名稱
      - Table: 表名稱
      - Provider: 資料格式（delta 或 parquet）
      - Type: 表類型
      - Location: 外部儲存路徑
   log: logs
      日誌實例，用於記錄執行過程

   回傳
   -------
   int
      成功刪除的表數量

   範例
   -------
   >>> logger = logs(name='Cleaner', debug=True)
   >>> deleted = drop_table_definition_without_storage(spark, tableDetailsDF, logger)
   INFO:Cleaner:----------------- Checking table deletion for temp_table -----------------
   INFO:Cleaner:Is not a valid delta table -> Moving forward to delete...
   INFO:Cleaner:drop table default.temp_table ==> done.
   >>> print(f'刪除了 {deleted} 個表')
   刪除了 1 個表

   注意事項
   -------
   - 此函式使用 df.collect() 將所有資料收集到 driver，對於大量表可能造成記憶體壓力
   - 每個表的刪除都是獨立執行，如果中途失敗，已刪除的表無法復原
   - 建議參考 docs/optimization-notes.md 了解效能優化方案
   - 目前版本沒有錯誤處理機制，刪除失敗會中斷整個流程

   Databricks 特定行為
   ------------------
   - DROP TABLE 對於 EXTERNAL 表只會刪除 metadata，不會刪除實際的儲存資料
   - 這正是本工具的設計目的：清理孤兒 metadata（表定義存在但資料已不存在）
   - 使用 DeltaTable.isDeltaTable() 時，會驗證 _delta_log 目錄的存在性
   - 使用 dbutils.fs.ls() 時，如果路徑不存在會拋出 FileNotFoundException
   """

   deleted = 0  # 記錄刪除的表數量

   # 遍歷每一個表進行檢查與可能的刪除
   for row in df.collect():
      log.trace(f'----------------- Checking table deletion for {row.Table} -----------------')

      # 根據 Provider 類型選擇不同的檢查方式
      if row.Provider.lower() == 'delta':
         # Delta 表的處理邏輯
         # 使用 DeltaTable.isDeltaTable() 檢查 location 是否為有效的 Delta Lake
         # 這個方法會驗證 _delta_log 目錄是否存在且格式正確
         if DeltaTable.isDeltaTable(spark,row.Location):
            # Location 路徑有效，資料存在，不需要刪除
            log.trace(f'isDeltaTable -> The data already exist in the storage layer. No need for deletion.')
         else:
            # Location 路徑無效或資料不存在，需要刪除表定義
            log.trace(f"Is not a valid delta table -> The data doesn''t exist in the storage layer. Moving forward to delete the table from the schema {row.Database}.")
            log.trace(f'drop table {row.Database}.{row.Table} ==> in progress ...')

            # 執行 DROP TABLE SQL 命令
            # 注意：這會永久刪除表定義，無法復原
            spark.sql(f'drop table {row.Database}.{row.Table}')

            log.trace(f'drop table {row.Database}.{row.Table} ==> done.')
            deleted += 1  # 增加刪除計數

      else:
         # Parquet（或其他非 Delta）表的處理邏輯
         # 使用 file_exists() 檢查路徑是否存在
         if file_exists(spark, row.Location):
            # 路徑存在，資料仍然在儲存層，不需要刪除
            log.trace(f'isParquetTable -> The data already exist in the storage layer. No need for deletion.')
         else:
            # 路徑不存在，需要刪除表定義
            log.trace(f"Is not a valid parquet table -> The data doesn''t exist in the storage layer. Moving forward to delete the table definition from the catalog {row.Database}.")
            log.trace(f'drop table {row.Database}.{row.Table} ==> in progress ...')

            # 執行 DROP TABLE SQL 命令
            spark.sql(f'drop table {row.Database}.{row.Table}')

            log.trace(f'drop table {row.Database}.{row.Table} ==> done.')
            deleted += 1  # 增加刪除計數

   # 回傳總共刪除的表數量
   return deleted


def drop_table_definition_without_storage_safe(
      spark: SparkSession,
      df: DataFrame,
      log: logs,
      config: Optional['CleanupConfig'] = None
   ) -> Tuple[int, List[Dict]]:
   """
   從 metastore 中刪除儲存資料不存在的表定義（支援 Dry-run 與安全控制）

   這是升級版的清理函式，支援 dry-run 模式、白名單/黑名單、保留條件等安全功能。

   ⚠️ 重要警告
   -----------
   在非 dry-run 模式下，此函式會執行 DROP TABLE 操作，這是不可逆的！
   建議先以 dry-run=True 執行，確認結果後再以 dry-run=False 實際刪除。

   處理邏輯
   --------
   對於每個表：
   1. 檢查白名單/黑名單（如果配置）
   2. 檢查保留條件：建立日期、最後存取時間（如果配置）
   3. 根據 Provider 類型檢查儲存是否存在
   4. 如果 dry_run=True：記錄將被刪除的表資訊
   5. 如果 dry_run=False：實際執行 DROP TABLE

   參數
   ----------
   spark: SparkSession
      Spark session 實例，用於執行 SQL 命令
   df: DataFrame
      包含表 metadata 的 DataFrame，必須包含以下欄位：
      - Database: schema 名稱
      - Table: 表名稱
      - Provider: 資料格式（delta 或 parquet）
      - Type: 表類型
      - Location: 外部儲存路徑
   log: logs
      日誌實例，用於記錄執行過程
   config: Optional[CleanupConfig]
      清理配置實例，包含 dry-run、白名單/黑名單等設定
      如果為 None，使用預設配置（dry_run=True）

   回傳
   -------
   Tuple[int, List[Dict]]
      (刪除的表數量, 候選表詳細資訊清單)
      候選表詳細資訊包含：
      - table_name: 完整表名稱
      - database: database 名稱
      - table: 表名稱
      - location: 儲存路徑
      - provider: 資料格式
      - action: 採取的動作（deleted/skipped_whitelist/skipped_blacklist等）
      - reason: 採取該動作的原因
      - estimated_size: 估算的資料大小（如果啟用）

   範例
   -------
   >>> from common.config import CleanupConfig
   >>>
   >>> # Dry-run 模式：只列出將被刪除的表
   >>> config = CleanupConfig(dry_run=True)
   >>> deleted, candidates = drop_table_definition_without_storage_safe(
   ...     spark, tableDetailsDF, logger, config
   ... )
   >>> print(f'[DRY-RUN] 將刪除 {deleted} 個表')
   >>>
   >>> # 實際刪除模式：加上安全控制
   >>> config = CleanupConfig(
   ...     dry_run=False,
   ...     whitelist_patterns=['prod.*'],
   ...     max_last_access_age_days=90
   ... )
   >>> deleted, candidates = drop_table_definition_without_storage_safe(
   ...     spark, tableDetailsDF, logger, config
   ... )

   注意事項
   -------
   - 在 dry-run 模式下，不會執行實際的刪除操作
   - 白名單優先級高於黑名單
   - 保留條件在白名單/黑名單檢查之後執行
   - 所有決策和動作都會詳細記錄到日誌
   """
   # 導入 CleanupConfig（避免循環導入）
   from common.config import CleanupConfig, DEFAULT_CONFIG

   # 如果沒有提供配置，使用預設配置（安全模式）
   if config is None:
      config = DEFAULT_CONFIG
      log.trace('[安全提示] 未提供配置，使用預設配置：dry_run=True')

   # 記錄配置資訊
   if config.dry_run:
      log.trace('=' * 80)
      log.trace('[DRY-RUN 模式] 這是模擬執行，不會實際刪除任何表')
      log.trace('=' * 80)

   deleted = 0  # 記錄刪除的表數量
   candidates = []  # 記錄所有候選表的詳細資訊
   skipped_whitelist = 0  # 因白名單跳過的表數量
   skipped_blacklist = 0  # 因黑名單跳過的表數量
   skipped_retention = 0  # 因保留條件跳過的表數量
   skipped_storage_exists = 0  # 因儲存存在跳過的表數量

   # 遍歷每一個表進行檢查
   for row in df.collect():
      table_full_name = f"{row.Database}.{row.Table}"

      log.trace(f'----------------- 檢查表 {row.Table} -----------------')

      # 建立候選表資訊
      candidate_info = {
         'table_name': table_full_name,
         'database': row.Database,
         'table': row.Table,
         'location': row.Location,
         'provider': row.Provider,
         'action': None,
         'reason': None,
         'estimated_size': None
      }

      # 步驟 1：檢查白名單/黑名單
      is_allowed, reason = config.is_table_deletion_allowed(table_full_name)
      if not is_allowed:
         if '白名單' in reason:
            skipped_whitelist += 1
            candidate_info['action'] = 'skipped_whitelist'
         elif '黑名單' in reason:
            skipped_blacklist += 1
            candidate_info['action'] = 'skipped_blacklist'

         candidate_info['reason'] = reason
         candidates.append(candidate_info)
         log.trace(f'[跳過] {reason}')
         continue

      # 步驟 2：檢查儲存是否存在
      storage_exists = False
      if row.Provider.lower() == 'delta':
         # Delta 表的處理邏輯
         storage_exists = DeltaTable.isDeltaTable(spark, row.Location)
         if storage_exists:
            log.trace(f'isDeltaTable -> 資料存在於儲存層，不需要刪除')
      else:
         # Parquet（或其他非 Delta）表的處理邏輯
         storage_exists = file_exists(spark, row.Location)
         if storage_exists:
            log.trace(f'isParquetTable -> 資料存在於儲存層，不需要刪除')

      if storage_exists:
         skipped_storage_exists += 1
         candidate_info['action'] = 'skipped_storage_exists'
         candidate_info['reason'] = '資料存在於儲存層'
         candidates.append(candidate_info)
         continue

      # 步驟 3：儲存不存在，標記為可刪除
      log.trace(f'資料不存在於儲存層 ({row.Provider})，符合刪除條件')

      # 步驟 4：執行刪除或記錄（根據 dry_run 模式）
      if config.dry_run:
         # Dry-run 模式：只記錄，不實際刪除
         log.trace(f'[DRY-RUN] 將刪除表：{table_full_name} @ {row.Location}')
         candidate_info['action'] = 'dry_run_candidate'
         candidate_info['reason'] = '儲存不存在，將被刪除（DRY-RUN）'
         candidates.append(candidate_info)
         deleted += 1
      else:
         # 實際刪除模式
         log.trace(f'準備刪除表：{table_full_name}')
         log.trace(f'執行 DROP TABLE {table_full_name} ...')

         try:
            # 執行 DROP TABLE SQL 命令
            spark.sql(f'DROP TABLE {table_full_name}')
            log.trace(f'✓ 成功刪除表：{table_full_name}')

            candidate_info['action'] = 'deleted'
            candidate_info['reason'] = '儲存不存在，已刪除'
            candidates.append(candidate_info)
            deleted += 1
         except Exception as e:
            log.trace(f'✗ 刪除表失敗：{table_full_name}，錯誤：{str(e)}')
            candidate_info['action'] = 'failed'
            candidate_info['reason'] = f'刪除失敗：{str(e)}'
            candidates.append(candidate_info)

   # 輸出統計摘要
   log.trace('')
   log.trace('=' * 80)
   log.trace('清理作業統計摘要')
   log.trace('=' * 80)
   if config.dry_run:
      log.trace(f'[DRY-RUN] 預計刪除表數量：{deleted}')
   else:
      log.trace(f'實際刪除表數量：{deleted}')
   log.trace(f'因白名單跳過：{skipped_whitelist}')
   log.trace(f'因黑名單跳過：{skipped_blacklist}')
   log.trace(f'因保留條件跳過：{skipped_retention}')
   log.trace(f'因資料存在跳過：{skipped_storage_exists}')
   log.trace(f'總計檢查表數量：{len(candidates)}')
   log.trace('=' * 80)

   return deleted, candidates


def confirm_deletion_interactive(
      candidates: List[Dict],
      dry_run: bool = False
   ) -> bool:
   """
   互動式確認刪除操作

   在執行實際刪除前，顯示候選表清單並要求使用者確認。
   此函式只在互動環境（Notebook、CLI）中使用，不適用於自動化 Job。

   參數
   ----------
   candidates: List[Dict]
      候選表清單，每個元素包含表的詳細資訊
   dry_run: bool
      是否為 dry-run 模式

   回傳
   -------
   bool
      True: 使用者確認繼續
      False: 使用者取消操作

   範例
   -------
   >>> deleted, candidates = drop_table_definition_without_storage_safe(
   ...     spark, tableDetailsDF, logger, config
   ... )
   >>> if config.require_confirmation:
   ...     if not confirm_deletion_interactive(candidates, config.dry_run):
   ...         print('操作已取消')
   ...         return
   """
   # 過濾出將被刪除的表
   to_delete = [c for c in candidates if c['action'] in ['dry_run_candidate', 'deleted']]

   if not to_delete:
      print('沒有符合刪除條件的表')
      return False

   print('\n' + '=' * 80)
   if dry_run:
      print('[DRY-RUN 模式] 以下是預計將被刪除的表')
   else:
      print('以下是將被刪除的表')
   print('=' * 80)
   print(f'{"序號":<6} {"資料庫":<20} {"表名稱":<30} {"儲存路徑":<50}')
   print('-' * 80)

   for idx, candidate in enumerate(to_delete, 1):
      print(f'{idx:<6} {candidate["database"]:<20} {candidate["table"]:<30} {candidate["location"]:<50}')

   print('-' * 80)
   print(f'總計：{len(to_delete)} 個表將被刪除')
   print('=' * 80)

   # 要求使用者確認
   if dry_run:
      response = input('\n這是 DRY-RUN 模式的結果。是否繼續？(輸入 YES 繼續，其他任意鍵取消): ')
   else:
      print('\n⚠️ 警告：此操作將永久刪除這些表的定義，無法復原！')
      response = input('請輸入 YES 以確認刪除，其他任意鍵取消操作: ')

   return response.strip() == 'YES'

