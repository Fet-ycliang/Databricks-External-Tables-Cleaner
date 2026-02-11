# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks External Tables Cleaner - 進階安全模式
# MAGIC
# MAGIC ## 功能說明
# MAGIC 此 Notebook 展示如何使用進階的安全功能來清理 Databricks 中的 external tables：
# MAGIC - **Dry-run 模式**：先預覽將被刪除的表，不實際執行刪除
# MAGIC - **白名單/黑名單**：保護特定的表不被誤刪
# MAGIC - **互動式確認**：在刪除前要求使用者確認
# MAGIC
# MAGIC ## 使用流程
# MAGIC 1. **第一步（Dry-run）**：設定 `dry_run=True`，執行查看將被刪除的表
# MAGIC 2. **第二步（確認）**：檢查 Dry-run 的結果，確認無誤
# MAGIC 3. **第三步（執行）**：設定 `dry_run=False`，實際執行刪除
# MAGIC
# MAGIC ## ⚠️ 安全提示
# MAGIC - 預設使用 Dry-run 模式，確保安全
# MAGIC - 建議先在測試環境中驗證
# MAGIC - 可設定白名單保護重要的表
# MAGIC - 支援互動式確認，防止誤操作

# COMMAND ----------

# MAGIC %md
# MAGIC ### 步驟 1：匯入模組與初始化

# COMMAND ----------

from context import (logs, get_tables, get_tables_details,
                     drop_table_definition_without_storage_safe,
                     confirm_deletion_interactive)
from common.config import CleanupConfig

# COMMAND ----------

# MAGIC %md
# MAGIC ### 步驟 2：設定參數
# MAGIC
# MAGIC 使用 Databricks Widgets 接收參數，方便在 UI 中調整設定

# COMMAND ----------

# 建立 Databricks widgets
dbutils.widgets.text('store', 'hive_metastore', 'Metastore/Catalog 名稱')
dbutils.widgets.text('schema', 'default', 'Schema/Database 名稱')
dbutils.widgets.dropdown('debug', 'True', ['True','False'], 'Debug 模式')
dbutils.widgets.dropdown('dry_run', 'True', ['True','False'], 'Dry-run 模式')
dbutils.widgets.dropdown('require_confirmation', 'True', ['True','False'], '需要互動確認')
dbutils.widgets.text('whitelist', '', '白名單（逗號分隔，支援萬用字元）')
dbutils.widgets.text('blacklist', '', '黑名單（逗號分隔，支援萬用字元）')

# 讀取參數
store = dbutils.widgets.get("store")
schema = dbutils.widgets.get("schema")
debug = eval(dbutils.widgets.get("debug"))
dry_run = eval(dbutils.widgets.get("dry_run"))
require_confirmation = eval(dbutils.widgets.get("require_confirmation"))

# 處理白名單/黑名單（如果有提供）
whitelist_str = dbutils.widgets.get("whitelist")
blacklist_str = dbutils.widgets.get("blacklist")

whitelist_patterns = [p.strip() for p in whitelist_str.split(',') if p.strip()] if whitelist_str else []
blacklist_patterns = [p.strip() for p in blacklist_str.split(',') if p.strip()] if blacklist_str else []

# COMMAND ----------

# MAGIC %md
# MAGIC ### 步驟 3：建立配置與 Logger

# COMMAND ----------

# 建立清理配置
config = CleanupConfig(
    dry_run=dry_run,
    whitelist_patterns=whitelist_patterns,
    blacklist_patterns=blacklist_patterns,
    require_confirmation=require_confirmation,
    estimate_storage_size=False  # 可根據需要啟用
)

# 初始化 logger
logger = logs(name='CleanTableLogger', level='info', debug=debug)

# 顯示當前配置
logger.trace('=' * 80)
logger.trace('清理工具配置')
logger.trace('=' * 80)
logger.trace(f'目標 Store: {store}')
logger.trace(f'目標 Schema: {schema}')
logger.trace(f'Dry-run 模式: {config.dry_run}')
logger.trace(f'需要確認: {config.require_confirmation}')
logger.trace(f'白名單模式: {config.whitelist_patterns}')
logger.trace(f'黑名單模式: {config.blacklist_patterns}')
logger.trace('=' * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 步驟 4：掃描表並取得詳細資訊

# COMMAND ----------

logger.trace(f"開始掃描表：store={store}, schema={schema}")

# 取得指定 schema 中的所有持久化表清單
tables = get_tables(spark, store=store, schema=schema, istemporary=False)

# 取得每個表的詳細 metadata
tabledetailsDF = get_tables_details(spark, store=store, schema=schema, tables=tables)

# 顯示掃描結果
tocheck = tabledetailsDF.count()
logger.trace(f'找到 {tocheck} 個表需要檢查')

# COMMAND ----------

# MAGIC %md
# MAGIC ### 步驟 5：執行清理（使用安全模式）
# MAGIC
# MAGIC 此步驟會根據配置執行 Dry-run 或實際刪除：
# MAGIC - **Dry-run 模式**：只列出將被刪除的表
# MAGIC - **實際刪除模式**：執行 DROP TABLE 操作

# COMMAND ----------

# 執行清理作業
deleted, candidates = drop_table_definition_without_storage_safe(
    spark=spark,
    df=tabledetailsDF,
    log=logger,
    config=config
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 步驟 6：查看候選表詳細資訊
# MAGIC
# MAGIC 將結果轉換為 DataFrame，方便查看和分析

# COMMAND ----------

if candidates:
    # 將候選表資訊轉換為 Spark DataFrame
    candidates_df = spark.createDataFrame(candidates)

    # 顯示所有候選表
    display(candidates_df)

    # 統計各種動作的數量
    action_counts = candidates_df.groupBy('action').count().orderBy('count', ascending=False)
    display(action_counts)
else:
    print('沒有找到符合條件的候選表')

# COMMAND ----------

# MAGIC %md
# MAGIC ### 步驟 7：互動式確認（可選）
# MAGIC
# MAGIC 如果啟用了互動式確認，此步驟會顯示將被刪除的表並要求使用者確認

# COMMAND ----------

if config.require_confirmation and not config.dry_run:
    # 只在非 Dry-run 模式且需要確認時執行
    if candidates:
        confirmed = confirm_deletion_interactive(candidates, config.dry_run)
        if confirmed:
            logger.trace('✓ 使用者已確認，繼續執行刪除')
            # 在實際場景中，這裡會重新執行刪除（config.dry_run=False）
        else:
            logger.trace('✗ 使用者取消操作')
    else:
        logger.trace('沒有需要確認的表')

# COMMAND ----------

# MAGIC %md
# MAGIC ### 步驟 8：輸出最終統計
# MAGIC
# MAGIC 顯示清理作業的完整統計資訊

# COMMAND ----------

logger.trace('')
logger.trace('=' * 80)
logger.trace('清理作業完成')
logger.trace('=' * 80)

if config.dry_run:
    logger.trace(f'[DRY-RUN] 預計刪除表數量：{deleted} / {tocheck}')
    logger.trace('')
    logger.trace('💡 提示：')
    logger.trace('  1. 檢查上方的候選表清單，確認無誤')
    logger.trace('  2. 如需實際刪除，請設定 dry_run=False 並重新執行')
    logger.trace('  3. 可透過白名單保護重要的表')
else:
    logger.trace(f'✓ 實際刪除表數量：{deleted} / {tocheck}')
    logger.trace('')
    logger.trace('作業已完成，已從 metastore 中移除孤兒表定義')

logger.trace('=' * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 使用範例
# MAGIC
# MAGIC ### 範例 1：Dry-run 模式（預覽）
# MAGIC ```
# MAGIC Widgets 設定：
# MAGIC - dry_run: True
# MAGIC - require_confirmation: False
# MAGIC - whitelist: prod.*, critical_*
# MAGIC ```
# MAGIC
# MAGIC ### 範例 2：實際刪除 + 互動確認
# MAGIC ```
# MAGIC Widgets 設定：
# MAGIC - dry_run: False
# MAGIC - require_confirmation: True
# MAGIC - whitelist: prod.*, important.*
# MAGIC - blacklist: test.*, temp_*
# MAGIC ```
# MAGIC
# MAGIC ### 範例 3：自動化 Job（無確認）
# MAGIC ```
# MAGIC Widgets 設定：
# MAGIC - dry_run: False
# MAGIC - require_confirmation: False
# MAGIC - whitelist: prod.*, production.*, critical_*
# MAGIC - max_last_access_age_days: 180
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 安全建議
# MAGIC
# MAGIC 1. **首次使用**：
# MAGIC    - 先在測試環境執行
# MAGIC    - 使用 Dry-run 模式確認結果
# MAGIC    - 逐步擴大清理範圍
# MAGIC
# MAGIC 2. **生產環境**：
# MAGIC    - 務必設定白名單保護重要的表
# MAGIC    - 建議啟用互動式確認
# MAGIC    - 定期檢查清理日誌
# MAGIC
# MAGIC 3. **自動化 Job**：
# MAGIC    - 使用嚴格的白名單規則
# MAGIC    - 設定適當的保留條件（如最後存取時間）
# MAGIC    - 監控清理作業的執行結果
# MAGIC
# MAGIC 4. **錯誤處理**：
# MAGIC    - 保存每次執行的日誌
# MAGIC    - 定期檢查 candidates DataFrame 的 action='failed' 記錄
# MAGIC    - 建立告警機制通知異常情況

# COMMAND ----------
