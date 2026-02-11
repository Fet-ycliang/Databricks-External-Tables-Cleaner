# Databricks External Tables Cleaner

## 專案簡介

Databricks External Tables Cleaner 是一個自動化工具，用於清理 Databricks 環境中不再使用的 external tables（外部表）。

在 Databricks 中，當您刪除一個 external table 時，只會移除 metastore 中的表定義，但實際儲存在 ADLS 或 S3 等外部儲存系統中的資料檔案仍然會保留。這可能導致：
- 儲存空間浪費
- 管理複雜度增加
- 難以追蹤哪些資料仍在使用中

本工具的核心功能是：
1. **自動偵測孤兒表定義**：掃描 Databricks metastore 中的 external tables，找出那些定義存在但實際儲存路徑已無資料的表
2. **安全清理**：自動從 metastore 中移除這些孤兒表定義，避免混淆
3. **批次處理**：支援對整個 schema 進行批次掃描與清理
4. **詳細日誌**：提供完整的執行日誌，追蹤每個表的處理狀態

**注意**：本工具只刪除「表定義存在但儲存資料不存在」的表，不會刪除實際的儲存資料。

## 系統需求與環境

### Python 版本
- Python 3.8 或更高版本

### Databricks 環境要求
- Databricks Runtime 10.4 LTS 或更高版本
- 支援 Unity Catalog 和傳統 Hive Metastore
- 需要對目標 catalog 和 schema 具有 `USE CATALOG`、`USE SCHEMA` 和 `DROP TABLE` 權限
- 需要對外部儲存路徑具有讀取權限（用於驗證資料是否存在）

### 外部表類型支援
本工具支援以下格式的 external tables：
- **Delta Tables**：使用 Delta Lake 格式的外部表
- **Parquet Tables**：使用 Parquet 格式的外部表

### 必要套件
所有必要的 Python 套件列於 `requirements.txt` 中：
```
pytest==7.4.0
databricks-connect
```

## 安裝與部署方式

### 方法一：透過 Databricks Repos（推薦）

1. 在 Databricks workspace 中，導航至 **Repos** 區域
2. 點擊 **Add Repo**
3. 輸入此 Git repository 的 URL：`https://github.com/Fet-ycliang/Databricks-External-Tables-Cleaner`
4. 點擊 **Create Repo**
5. Databricks 會自動同步程式碼，您可以直接在 workspace 中執行

### 方法二：直接上傳檔案

1. 將專案檔案下載到本地
2. 在 Databricks workspace 中建立對應的目錄結構
3. 上傳以下檔案：
   - `common/helpers.py`
   - `scripts/clean_tables_without_storage.py`
   - `scripts/context.py`
4. 上傳 notebook：`notebooks/clean_tables_without_storage.py`

### 方法三：打包為 Wheel（適用於生產環境）

1. 建立 `setup.py` 檔案（未來支援項目）
2. 使用 `python setup.py bdist_wheel` 打包
3. 將 wheel 檔案上傳至 Databricks
4. 在 cluster 設定中安裝此 wheel

### 設定 Cluster 或 Job

#### Cluster 設定
建議使用以下 cluster 配置：
- **Databricks Runtime**：10.4 LTS 或更高版本
- **Worker 類型**：根據要清理的表數量選擇，一般情況下 Standard_DS3_v2（或同等規格）即可
- **Workers 數量**：1-2 個即可（清理作業不需要大量運算資源）
- **Auto-termination**：建議設定為 15-30 分鐘

#### Job 設定範例
建立一個 Databricks Job 以定期執行清理：
1. 導航至 **Workflows** → **Jobs**
2. 點擊 **Create Job**
3. 設定以下參數：
   - **Task type**：Notebook
   - **Notebook path**：選擇 `notebooks/clean_tables_without_storage.py`
   - **Cluster**：選擇或建立一個符合上述規格的 cluster
   - **Schedule**：建議每週執行一次（可根據需求調整）
   - **Parameters**：
     - `store`: 例如 `hive_metastore` 或 Unity Catalog 名稱
     - `schema`: 要清理的 schema 名稱
     - `debug`: `True`（開發測試時）或 `False`（生產環境）

## 使用方式

### 透過 Notebook 執行

1. 開啟 `notebooks/clean_tables_without_storage.py`
2. 設定 Widget 參數：
   - **store**：metastore 名稱（例如：`hive_metastore` 或 Unity Catalog 名稱如 `main`）
   - **schema**：要清理的 schema/database 名稱（例如：`default`）
   - **debug**：選擇 `True`（顯示詳細日誌到 console）或 `False`（寫入 log4j）
3. 執行整個 notebook

### 透過腳本執行

```python
from common.helpers import (logs, get_tables, get_tables_details,
                            drop_table_definition_without_storage)

# 初始化 logger
logger = logs(name="CleanTableLogger", level='info', debug=True)

# 設定參數
store = 'hive_metastore'
schema = 'my_schema'

# 取得表列表
tables = get_tables(spark, store=store, schema=schema, istemporary=False)
tabledetailsDF = get_tables_details(spark, store=store, schema=schema, tables=tables)

# 執行清理
deleted = drop_table_definition_without_storage(spark, tabledetailsDF, logger)
logger.trace(f'清理完成：刪除了 {deleted} 個表')
```

### 參數說明

- **store**：Metastore 或 Catalog 名稱
  - Hive Metastore：使用 `hive_metastore`
  - Unity Catalog：使用您的 catalog 名稱（例如：`main`、`dev`）

- **schema**：要清理的 database/schema 名稱
  - 例如：`default`、`staging`、`production`

- **debug**：日誌模式
  - `True`：將日誌輸出到 console（適合開發和測試）
  - `False`：將日誌寫入 log4j 檔案（適合生產環境）

### Dry-run 模式（建議未來支援項目）

目前版本會直接執行刪除操作。建議未來版本加入以下功能：
- **Dry-run 模式**：僅列出將被刪除的表，不實際執行刪除
- **白名單/黑名單**：支援設定不應被刪除的表名稱模式
- **保留條件**：依據建立日期、最後存取時間等條件篩選
- **互動式確認**：在刪除前要求使用者確認

## 專案目錄結構

```
Databricks-External-Tables-Cleaner/
├── common/                      # 共用工具模組
│   └── helpers.py              # 核心功能函式（表掃描、檢查、刪除等）
├── scripts/                     # 可執行腳本
│   ├── context.py              # 模組路徑設定
│   └── clean_tables_without_storage.py  # 主要清理腳本
├── notebooks/                   # Databricks Notebook
│   ├── context.py              # 模組路徑設定
│   └── clean_tables_without_storage.py  # Notebook 版本的清理工具
├── tests/                       # 單元測試
│   ├── context.py              # 測試環境設定
│   └── test_clean_tables_without_storage.py  # 測試案例
├── docs/                        # 文件目錄
│   ├── system-design.md        # 系統架構與設計說明
│   └── optimization-notes.md   # 效能優化建議
├── requirements.txt             # Python 套件依賴
├── .gitignore                  # Git 忽略規則
└── README.md                   # 專案說明文件（本檔案）
```

### 各目錄說明

- **common/**：包含可重複使用的核心函式庫
  - 表列舉與詳細資訊查詢
  - 檔案存在性檢查
  - 表定義刪除邏輯
  - 日誌管理

- **scripts/**：獨立的 Python 腳本，可在 Databricks 或本地執行

- **notebooks/**：Databricks Notebook 格式，包含 Magic Commands 和 Widget 設定

- **tests/**：使用 pytest 的單元測試，可透過 databricks-connect 在遠端 cluster 上執行

- **docs/**：詳細的技術文件和架構說明

## 注意事項與安全性

### ⚠️ 重要警告

1. **實際刪除操作**：本工具會從 Databricks metastore 中永久刪除表定義，此操作**無法復原**
2. **測試環境驗證**：強烈建議先在測試環境中執行，確認行為符合預期後再用於生產環境
3. **權限管理**：執行此工具的使用者需要適當的權限，確保不會誤刪重要的表

### 安全性建議

1. **備份重要 Metadata**：
   - 在執行清理前，建議先匯出目標 schema 的表列表和定義
   - 可使用 `SHOW TABLES` 和 `DESCRIBE TABLE EXTENDED` 儲存 metadata

2. **段階式執行**：
   - 首次使用時，建議對單一小型 schema 進行測試
   - 確認結果無誤後，再逐步擴大清理範圍

3. **監控與日誌**：
   - 開啟 debug 模式（`debug=True`），完整記錄每個刪除操作
   - 保存執行日誌以便日後審計

4. **存取控制**：
   - 限制只有特定人員或服務帳號可執行此工具
   - 使用 Databricks Secrets 管理敏感資訊

5. **通知機制**（建議未來實作）：
   - 在刪除表後發送通知給相關團隊
   - 整合到組織的變更管理流程中

### 已知限制

- 本工具目前不支援刪除實際的儲存資料，只刪除表定義
- 不支援 View（視圖），僅處理實體表
- 單執行緒逐一處理表，對於大量表的 schema 可能需要較長時間
- 無法處理有外鍵約束或依賴關係的表

## 疑難排解

### 常見問題

**Q: 執行時出現權限錯誤**
```
Error: User does not have DROP privilege on table
```
A: 確認執行使用者具有目標 catalog/schema 的 DROP TABLE 權限

**Q: 無法偵測到儲存路徑**
```
AccessDeniedException: Access denied to path
```
A: 確認 cluster 具有存取外部儲存（ADLS/S3）的權限，需要正確設定 Service Principal 或 IAM Role

**Q: 表數量很多但處理速度很慢**
A: 本工具採用序列處理，建議參考 `docs/optimization-notes.md` 中的效能優化建議

## 貢獻與支援

本專案為 fork 自 [CommanderWahid/Databricks-External-Tables-Cleaner](https://github.com/CommanderWahid/Databricks-External-Tables-Cleaner)。

如有問題或建議，歡迎開啟 Issue 或提交 Pull Request。

## 授權

請參考原始專案的授權條款。

## 相關資源

- [Databricks External Tables 文件](https://learn.microsoft.com/en-us/azure/databricks/tables/external)
- [Unity Catalog 最佳實務](https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/best-practices)
- [Databricks 效能優化指南](https://learn.microsoft.com/en-us/azure/databricks/lakehouse-architecture/performance-efficiency/best-practices)
