# Databricks notebook source
# MAGIC %md
# MAGIC # 孤兒目錄掃描工具
# MAGIC
# MAGIC ## 功能說明
# MAGIC
# MAGIC 此 Notebook 用於偵測 Storage 中未被 Unity Catalog 引用的「孤兒目錄」。
# MAGIC
# MAGIC ### 掃描邏輯
# MAGIC
# MAGIC 1. **收集 Unity Catalog 引用的路徑**
# MAGIC    - External Tables 的 LOCATION
# MAGIC    - External Volumes 的 storage_location
# MAGIC    - External Locations 的 url
# MAGIC
# MAGIC 2. **掃描 Storage 實際目錄結構**
# MAGIC    - 從指定的基礎路徑開始
# MAGIC    - 遞迴掃描到指定深度
# MAGIC
# MAGIC 3. **比對並找出孤兒目錄**
# MAGIC    - 檢查每個 Storage 目錄是否被 UC 引用
# MAGIC    - 列出未被引用的目錄
# MAGIC
# MAGIC ### ⚠️ 重要說明
# MAGIC
# MAGIC - **此工具僅用於偵測和報告，不會刪除任何資料**
# MAGIC - 建議在執行前先在測試環境驗證
# MAGIC - 需要對 UC catalogs 和 storage 路徑有讀取權限
# MAGIC
# MAGIC ### 權限需求
# MAGIC
# MAGIC - Unity Catalog: `USE CATALOG`、`USE SCHEMA`、`SELECT` 權限
# MAGIC - Storage: 讀取權限（list 和 read）
# MAGIC - External Locations: `SELECT` 權限（如要掃描 external locations）

# COMMAND ----------

# MAGIC %md
# MAGIC ## 步驟 1: 設定參數

# COMMAND ----------

# 建立 Widgets 用於參數輸入
dbutils.widgets.text("base_paths", "", "1. 掃描的基礎路徑 (逗號分隔)")
dbutils.widgets.text("catalogs", "", "2. 要掃描的 Catalogs (逗號分隔，空白=全部)")
dbutils.widgets.dropdown("max_depth", "2", ["1", "2", "3", "4", "5"], "3. 掃描深度")
dbutils.widgets.dropdown("output_format", "notebook", ["notebook", "log", "delta_table"], "4. 輸出格式")
dbutils.widgets.text("output_table", "", "5. 輸出 Table (如選 delta_table)")
dbutils.widgets.dropdown("debug", "True", ["True", "False"], "6. Debug 模式")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 步驟 2: 載入模組與配置

# COMMAND ----------

# 設定模組路徑
import sys
import os

# 取得 notebook 的父目錄（repo 根目錄）
notebook_path = os.path.dirname(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get())
repo_root = os.path.dirname(notebook_path)

# 將 common 目錄加入 Python path
common_path = os.path.join(repo_root, 'common')
if common_path not in sys.path:
    sys.path.insert(0, common_path)

# COMMAND ----------

# 載入必要模組
from common.helpers import logs
from common.orphan_paths_scanner import scan_orphan_paths
from common.config import OrphanScanConfig

# COMMAND ----------

# MAGIC %md
# MAGIC ## 步驟 3: 解析參數並建立配置

# COMMAND ----------

# 取得參數
base_paths_str = dbutils.widgets.get("base_paths")
catalogs_str = dbutils.widgets.get("catalogs")
max_depth = int(dbutils.widgets.get("max_depth"))
output_format = dbutils.widgets.get("output_format")
output_table = dbutils.widgets.get("output_table")
debug = dbutils.widgets.get("debug") == "True"

# 解析路徑清單
if not base_paths_str:
    raise ValueError("必須至少指定一個 base_path！")

base_paths = [p.strip() for p in base_paths_str.split(',') if p.strip()]

# 解析 catalogs 清單
catalogs = None
if catalogs_str:
    catalogs = [c.strip() for c in catalogs_str.split(',') if c.strip()]

# 建立配置
config = OrphanScanConfig(
    enabled=True,
    base_paths=base_paths,
    catalogs=catalogs,
    max_depth=max_depth,
    dry_run=True,  # 孤兒掃描永遠是 dry-run
    output_format=output_format,
    output_table=output_table if output_table else None
)

# 驗證配置
is_valid, message = config.validate()
if not is_valid:
    raise ValueError(f"配置錯誤: {message}")

print("=" * 80)
print("孤兒目錄掃描配置")
print("=" * 80)
print(f"基礎路徑: {base_paths}")
print(f"Catalogs: {catalogs if catalogs else '全部'}")
print(f"掃描深度: {max_depth}")
print(f"輸出格式: {output_format}")
if output_table:
    print(f"輸出 Table: {output_table}")
print(f"Debug 模式: {debug}")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 步驟 4: 執行孤兒目錄掃描

# COMMAND ----------

# 初始化日誌
logger = logs(name='OrphanPathsScanner', level='info', debug=debug)

logger.trace('')
logger.trace('=' * 80)
logger.trace('開始執行孤兒目錄掃描')
logger.trace('=' * 80)

# 執行掃描
orphan_directories, report_df = scan_orphan_paths(
    spark=spark,
    log=logger,
    base_paths=config.base_paths,
    catalogs=config.catalogs,
    max_depth=config.max_depth
)

logger.trace('')
logger.trace('=' * 80)
logger.trace('掃描完成')
logger.trace('=' * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 步驟 5: 顯示掃描結果

# COMMAND ----------

if len(orphan_directories) == 0:
    print("✓ 沒有發現孤兒目錄！")
    print("所有掃描到的目錄都被 Unity Catalog 引用。")
else:
    print(f"⚠️ 發現 {len(orphan_directories)} 個孤兒目錄")
    print("")

    # 顯示 DataFrame
    if config.output_format == 'notebook':
        print("孤兒目錄清單：")
        print("=" * 80)
        display(report_df.orderBy("path"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 步驟 6: 統計摘要

# COMMAND ----------

if len(orphan_directories) > 0:
    # 計算總大小
    total_size_bytes = sum(d.get('size', 0) for d in orphan_directories)

    def format_size(size_bytes):
        """格式化檔案大小"""
        if size_bytes < 1024:
            return f"{size_bytes} B"
        elif size_bytes < 1024 * 1024:
            return f"{size_bytes / 1024:.2f} KB"
        elif size_bytes < 1024 * 1024 * 1024:
            return f"{size_bytes / (1024 * 1024):.2f} MB"
        else:
            return f"{size_bytes / (1024 * 1024 * 1024):.2f} GB"

    print("=" * 80)
    print("統計摘要")
    print("=" * 80)
    print(f"孤兒目錄數量: {len(orphan_directories)}")
    print(f"孤兒目錄總大小: {format_size(total_size_bytes)}")
    print("=" * 80)

    # 按大小排序，顯示前 10 大
    sorted_orphans = sorted(orphan_directories, key=lambda x: x.get('size', 0), reverse=True)
    print("")
    print("前 10 大孤兒目錄:")
    print("-" * 80)
    print(f"{'排名':<6} {'大小':<15} {'路徑':<60}")
    print("-" * 80)

    for idx, orphan in enumerate(sorted_orphans[:10], 1):
        size_str = format_size(orphan.get('size', 0))
        path = orphan['path']
        # 截斷過長的路徑
        if len(path) > 60:
            path = path[:57] + '...'
        print(f"{idx:<6} {size_str:<15} {path:<60}")

    print("-" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 步驟 7: （可選）儲存報表到 Delta Table

# COMMAND ----------

if config.output_format == 'delta_table' and config.output_table and len(orphan_directories) > 0:
    logger.trace(f'正在將報表寫入 Delta table: {config.output_table}')

    # 加入掃描時間戳記
    from pyspark.sql.functions import lit, current_timestamp

    report_with_timestamp = report_df.withColumn("scan_timestamp", current_timestamp())

    # 寫入 Delta table (append 模式，保留歷史記錄)
    report_with_timestamp.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(config.output_table)

    logger.trace(f'✓ 報表已寫入: {config.output_table}')
    print(f"✓ 報表已儲存到 Delta table: {config.output_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 使用範例
# MAGIC
# MAGIC ### 範例 1: 掃描單一路徑
# MAGIC
# MAGIC ```
# MAGIC base_paths: abfss://data@myacct.dfs.core.windows.net/raw/
# MAGIC catalogs: main
# MAGIC max_depth: 2
# MAGIC output_format: notebook
# MAGIC ```
# MAGIC
# MAGIC ### 範例 2: 掃描多個路徑
# MAGIC
# MAGIC ```
# MAGIC base_paths: abfss://data@myacct.dfs.core.windows.net/raw/, abfss://data@myacct.dfs.core.windows.net/curated/
# MAGIC catalogs: main, dev
# MAGIC max_depth: 3
# MAGIC output_format: notebook
# MAGIC ```
# MAGIC
# MAGIC ### 範例 3: 輸出到 Delta Table
# MAGIC
# MAGIC ```
# MAGIC base_paths: abfss://data@myacct.dfs.core.windows.net/raw/
# MAGIC catalogs: main
# MAGIC max_depth: 2
# MAGIC output_format: delta_table
# MAGIC output_table: main.audit.orphan_paths_report
# MAGIC ```
# MAGIC
# MAGIC ## 下一步建議
# MAGIC
# MAGIC 1. **檢視孤兒目錄清單**：確認這些目錄是否真的不再需要
# MAGIC 2. **人工驗證**：對於重要路徑，建議人工確認後再處理
# MAGIC 3. **規劃清理**：如需刪除，建議使用其他工具或手動處理
# MAGIC 4. **定期掃描**：可以將此 Notebook 設定為定期執行的 Job
# MAGIC
# MAGIC ## 注意事項
# MAGIC
# MAGIC - 此工具不會刪除任何資料，僅用於偵測和報告
# MAGIC - 孤兒目錄的刪除需要另外的工具或手動處理
# MAGIC - 建議先在測試環境驗證掃描結果
# MAGIC - 注意 Unity Catalog 的路徑重疊限制
