# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks External Tables Cleaner - Notebook 版本
# MAGIC
# MAGIC ## 功能說明
# MAGIC 此 Notebook 用於自動清理 Databricks 中不再使用的 external tables。
# MAGIC
# MAGIC ## 清理邏輯
# MAGIC 1. 掃描指定 schema 中的所有 external tables
# MAGIC 2. 檢查每個表的儲存路徑是否仍然存在資料
# MAGIC 3. 如果資料不存在，則刪除該表的定義（metadata）
# MAGIC
# MAGIC ## ⚠️ 重要警告
# MAGIC - 此 Notebook 會執行 DROP TABLE 操作，這是不可逆的
# MAGIC - 請務必先在測試環境中驗證
# MAGIC - 建議開啟 debug 模式以查看詳細日誌
# MAGIC
# MAGIC ## 使用方式
# MAGIC 1. 設定下方的 Widget 參數
# MAGIC 2. 執行所有 cells
# MAGIC 3. 檢查輸出日誌確認執行結果

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup Phase
# MAGIC ###### 匯入函式庫、初始化參數與建立 logger 實例

# COMMAND ----------
from context import (logs, get_tables, get_tables_details,
                     drop_table_definition_without_storage)
 # COMMAND ----------

# 建立 Databricks widgets 用於接收參數
# 這些 widgets 會在 Notebook UI 頂部顯示，供使用者輸入
dbutils.widgets.text('store', 'hive_metastore')  # Metastore/Catalog 名稱，預設為 hive_metastore
dbutils.widgets.text('schema', 'default')  # Schema/Database 名稱，預設為 default
dbutils.widgets.dropdown('debug', 'True', ['True','False'])  # Debug 模式，預設為 True（顯示詳細日誌）

# 從 widgets 讀取使用者設定的參數值
store = dbutils.widgets.get("store")
schema = dbutils.widgets.get("schema")
debug = dbutils.widgets.get("debug")
 # COMMAND ----------

# 初始化 logger 實例
# - name: 'CleanTableLogger' - Logger 名稱
# - level: 'info' - 日誌等級
# - debug: 將字串 'True'/'False' 轉換為布林值
#   - True: 日誌會直接輸出到 Notebook console
#   - False: 日誌會寫入 Spark 的 log4j 系統
logger = logs(name='CleanTableLogger',level='info', debug=eval(f'{debug}'))
# COMMAND ----------

# MAGIC %md
# MAGIC ### Main code
# MAGIC ###### 主要執行邏輯

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Get tables details
# MAGIC ###### 取得表的詳細資訊

# COMMAND ----------
# 記錄開始執行，顯示當前處理的 store 和 schema
logger.trace(f"Get the list of tables and their details store={store} schema={schema}")

# 步驟 1：取得指定 schema 中的所有持久化表清單
# - istemporary=False: 只取得持久化的表，排除臨時表/views
tables = get_tables(spark,store=f'{store}',schema=f'{schema}',istemporary=False)

# 步驟 2：取得每個表的詳細 metadata
# 這會對每個表執行 SHOW TABLE EXTENDED 並解析結果
# 回傳的 DataFrame 包含：Database, Table, Provider, Type, Location
tabledetailsDF = get_tables_details(spark,store=f'{store}',schema=f'{schema}',tables=tables)

# COMMAND ----------
# MAGIC %md
# MAGIC ###### Drop tables without storage
# MAGIC ###### 刪除儲存資料不存在的表

# COMMAND ----------
# 計算需要檢查的表總數
tocheck = tabledetailsDF.count()
logger.trace(f'Found {tocheck} tables to be inspected')

# 步驟 3：執行清理邏輯
# 此函式會：
# 1. 遍歷每個表
# 2. 檢查其 Location 路徑是否有資料
# 3. 如果沒有資料，執行 DROP TABLE
# 回傳值：成功刪除的表數量
deleted = drop_table_definition_without_storage(spark,tabledetailsDF,logger)

# 記錄最終執行結果
logger.trace(f'Cleaning tables without data from the store {store} and the catalog {schema} ==> {deleted} out of {tocheck}')
 
