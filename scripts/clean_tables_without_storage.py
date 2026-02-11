"""
Databricks External Tables Cleaner - 主要執行腳本

此腳本是清理工具的主要執行入口點，設計為在 Databricks 環境中執行。
它會使用 dbutils.widgets 接收參數，然後呼叫 common.helpers 中的函式來執行清理流程。

使用方式：
1. 在 Databricks 中執行此腳本
2. 透過 widgets 設定參數：
   - store: metastore/catalog 名稱
   - schema: 要清理的 schema 名稱
   - debug: True（開發）或 False（生產）
3. 腳本會自動掃描並清理符合條件的表

注意：此腳本會實際刪除表定義，請謹慎使用！
"""

from context import (logs, get_tables, get_tables_details,
                     drop_table_definition_without_storage)

# 建立 Databricks widgets 用於接收參數
# 這些 widgets 會在 Databricks UI 中顯示，讓使用者可以輸入參數
dbutils.widgets.text('store', 'hive_metastore')  # Metastore/Catalog 名稱
dbutils.widgets.text('schema', 'default')  # Schema/Database 名稱
dbutils.widgets.dropdown('debug', 'True', ['True','False'])  # Debug 模式選擇

# 從 widgets 讀取使用者輸入的參數值
store = dbutils.widgets.get("store")
schema = dbutils.widgets.get("schema")
debug = dbutils.widgets.get("debug")

# 初始化 logger
# - name: Logger 名稱，用於識別日誌來源
# - level: 日誌等級（info）
# - debug: eval(f'{debug}') 將字串 'True'/'False' 轉換為布林值
logger = logs(name="CleanTableLogger", level='info', debug=eval(f'{debug}'))

# 記錄開始執行的資訊
logger.trace(f"Get the list of tables and their details store={store} schema={schema}")

# 步驟 1：取得指定 schema 中的所有持久化表清單
# istemporary=False 表示只取得持久化的表，不包含臨時表
tables = get_tables(spark,store=f'{store}',schema=f'{schema}',istemporary=False)

# 步驟 2：取得每個表的詳細 metadata（包含 Location 等關鍵資訊）
tabledetailsDF = get_tables_details(spark,store=f'{store}',schema=f'{schema}',tables=tables)

# 計算需要檢查的表總數
tocheck = tabledetailsDF.count()
logger.trace(f'Found {tocheck} tables to be inspected')

# 步驟 3：執行清理操作
# 此函式會檢查每個表的儲存路徑，並刪除資料不存在的表定義
deleted = drop_table_definition_without_storage(spark,tabledetailsDF,logger)

# 記錄最終結果
logger.trace(f'Cleaning tables without data from the store {store} and the catalog {schema} ==> {deleted} out of {tocheck}')
 