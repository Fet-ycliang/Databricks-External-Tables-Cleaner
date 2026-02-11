"""
孤兒目錄掃描腳本

此腳本用於從 Storage 角度反向檢查 Unity Catalog，偵測未被引用的「孤兒目錄」。

使用方式:
    在 Databricks 環境中執行:
    python scripts/scan_orphan_paths.py

功能:
    - 從 Unity Catalog 收集所有已引用的路徑
    - 掃描指定 Storage 區域的實際目錄結構
    - 比對並找出未被 Unity Catalog 引用的目錄
    - 產生孤兒目錄報表

注意:
    - 此腳本僅用於偵測和報告，不會刪除任何資料
    - 需要在 Databricks 環境中執行（需要 SparkSession 和 dbutils）
    - 需要對 Unity Catalog 和 Storage 有適當的讀取權限
"""

import sys
import os

# 設定模組路徑
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)

from common.helpers import logs
from common.orphan_paths_scanner import scan_orphan_paths
from common.config import OrphanScanConfig


def main():
    """
    主程式入口點
    """
    # 建立 Widgets 用於參數輸入（如果在 Notebook 環境）
    try:
        # 嘗試建立 widgets（Notebook 環境）
        dbutils.widgets.text("base_paths", "", "1. 掃描的基礎路徑 (逗號分隔)")
        dbutils.widgets.text("catalogs", "", "2. 要掃描的 Catalogs (逗號分隔，空白=全部)")
        dbutils.widgets.dropdown("max_depth", "2", ["1", "2", "3", "4", "5"], "3. 掃描深度")
        dbutils.widgets.dropdown("output_format", "log", ["notebook", "log", "delta_table"], "4. 輸出格式")
        dbutils.widgets.text("output_table", "", "5. 輸出 Table (如選 delta_table)")
        dbutils.widgets.dropdown("debug", "True", ["True", "False"], "6. Debug 模式")

        # 從 widgets 取得參數
        base_paths_str = dbutils.widgets.get("base_paths")
        catalogs_str = dbutils.widgets.get("catalogs")
        max_depth = int(dbutils.widgets.get("max_depth"))
        output_format = dbutils.widgets.get("output_format")
        output_table = dbutils.widgets.get("output_table")
        debug = dbutils.widgets.get("debug") == "True"

    except NameError:
        # 非 Notebook 環境，使用預設值或環境變數
        print("⚠️ 非 Notebook 環境，請修改腳本中的參數設定")
        print("或在 Notebook 中執行此腳本")
        return

    # 驗證必要參數
    if not base_paths_str:
        raise ValueError("必須至少指定一個 base_path！")

    # 解析路徑清單
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

    # 初始化日誌
    logger = logs(name='OrphanPathsScanner', level='info', debug=debug)

    # 顯示配置資訊
    logger.trace('')
    logger.trace('=' * 80)
    logger.trace('孤兒目錄掃描配置')
    logger.trace('=' * 80)
    logger.trace(f'基礎路徑: {base_paths}')
    logger.trace(f'Catalogs: {catalogs if catalogs else "全部"}')
    logger.trace(f'掃描深度: {max_depth}')
    logger.trace(f'輸出格式: {output_format}')
    if output_table:
        logger.trace(f'輸出 Table: {output_table}')
    logger.trace(f'Debug 模式: {debug}')
    logger.trace('=' * 80)

    # 執行掃描
    logger.trace('')
    logger.trace('=' * 80)
    logger.trace('開始執行孤兒目錄掃描')
    logger.trace('=' * 80)

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

    # 顯示結果摘要
    if len(orphan_directories) == 0:
        logger.trace('✓ 沒有發現孤兒目錄！')
        logger.trace('所有掃描到的目錄都被 Unity Catalog 引用。')
    else:
        logger.trace(f'⚠️ 發現 {len(orphan_directories)} 個孤兒目錄')

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

        logger.trace('')
        logger.trace('=' * 80)
        logger.trace('統計摘要')
        logger.trace('=' * 80)
        logger.trace(f'孤兒目錄數量: {len(orphan_directories)}')
        logger.trace(f'孤兒目錄總大小: {format_size(total_size_bytes)}')
        logger.trace('=' * 80)

        # 顯示 DataFrame（如果在 notebook 環境）
        if output_format == 'notebook':
            logger.trace('')
            logger.trace('孤兒目錄清單：')
            report_df.show(truncate=False)

    # 儲存報表到 Delta Table（如果配置）
    if config.output_format == 'delta_table' and config.output_table and len(orphan_directories) > 0:
        logger.trace('')
        logger.trace(f'正在將報表寫入 Delta table: {config.output_table}')

        # 加入掃描時間戳記
        from pyspark.sql.functions import current_timestamp

        report_with_timestamp = report_df.withColumn("scan_timestamp", current_timestamp())

        # 寫入 Delta table (append 模式，保留歷史記錄)
        report_with_timestamp.write \
            .format("delta") \
            .mode("append") \
            .saveAsTable(config.output_table)

        logger.trace(f'✓ 報表已寫入: {config.output_table}')

    logger.trace('')
    logger.trace('=' * 80)
    logger.trace('孤兒目錄掃描完成')
    logger.trace('=' * 80)


if __name__ == "__main__":
    main()
