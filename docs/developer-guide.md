# 開發者指南

本文件提供 Databricks External Tables Cleaner 的開發者資訊，包括開發環境設定、程式碼結構、擴展指南與最佳實務。

## 目錄

1. [開發環境設定](#開發環境設定)
2. [程式碼結構說明](#程式碼結構說明)
3. [設計原則與模式](#設計原則與模式)
4. [擴展與客製化](#擴展與客製化)
5. [測試指南](#測試指南)
6. [程式碼風格指南](#程式碼風格指南)
7. [貢獻流程](#貢獻流程)
8. [常見開發場景](#常見開發場景)

---

## 開發環境設定

### 必要條件

- **Python 版本**：3.8 或更高版本
- **Databricks 環境**：Databricks Runtime 10.4 LTS 或更高版本
- **開發工具**：
  - Visual Studio Code 或 PyCharm（建議）
  - Git
  - databricks-connect（用於本地開發）

### 本地開發環境設定

1. **Clone 專案**
   ```bash
   git clone https://github.com/Fet-ycliang/Databricks-External-Tables-Cleaner.git
   cd Databricks-External-Tables-Cleaner
   ```

2. **建立虛擬環境**
   ```bash
   python -m venv venv
   source venv/bin/activate  # Linux/Mac
   # 或
   venv\Scripts\activate  # Windows
   ```

3. **安裝依賴套件**
   ```bash
   pip install -r requirements.txt
   ```

4. **設定 databricks-connect**（可選，用於本地開發）
   ```bash
   databricks-connect configure
   ```
   按照提示輸入：
   - Databricks workspace URL
   - Personal Access Token
   - Cluster ID

5. **執行測試**
   ```bash
   pytest tests/
   ```

### Databricks 環境開發

1. **使用 Databricks Repos**（推薦）
   - 在 Databricks workspace 中，導航至 Repos
   - 點擊 "Add Repo" 並連接此 Git repository
   - 可直接在 Databricks 中編輯和執行

2. **直接上傳檔案**
   - 適合小型修改
   - 上傳修改後的檔案到 workspace

---

## 程式碼結構說明

### 核心模組：common/helpers.py

包含所有核心功能函式，職責清楚分離：

```python
# 檔案操作
file_exists(spark, dir)  # 檢查路徑是否存在

# 表掃描與查詢
get_tables(spark, store, schema, istemporary)  # 列舉表清單
get_tables_details(spark, store, schema, tables)  # 取得表詳細資訊

# 工具函式
create_empty_dataframe(spark, columns, types)  # 建立空 DataFrame

# 日誌管理
logs(name, level, debug)  # Logger 類別

# 清理邏輯
drop_table_definition_without_storage(spark, df, log)  # 基本版清理
drop_table_definition_without_storage_safe(spark, df, log, config)  # 安全版清理
confirm_deletion_interactive(candidates, dry_run)  # 互動確認
```

**設計原則：**
- 每個函式職責單一且明確
- 使用型別提示（Type Hints）
- 完整的繁體中文文檔字串
- 參數驗證與錯誤處理

### 配置模組：common/config.py

封裝所有配置相關的邏輯：

```python
class CleanupConfig:
    """清理配置類別"""
    
    # 核心屬性
    dry_run: bool  # Dry-run 模式
    whitelist_patterns: List[str]  # 白名單
    blacklist_patterns: List[str]  # 黑名單
    min_create_date: Optional[date]  # 最小建立日期
    max_last_access_age_days: Optional[int]  # 最後存取天數
    require_confirmation: bool  # 互動確認
    
    # 核心方法
    def is_table_deletion_allowed(self, table_name: str) -> tuple[bool, str]
    def check_retention_conditions(self, create_time, last_access_time) -> tuple[bool, str]
    def to_dict(self) -> dict
    @classmethod
    def from_dict(cls, config_dict: dict) -> 'CleanupConfig'
```

**設計原則：**
- 使用 dataclass 風格的類別設計
- 提供序列化/反序列化支援
- 包含預設配置範本
- 清楚的模式匹配邏輯

### 應用層：scripts 和 notebooks

**scripts/**
- 純 Python 腳本，可在 Databricks 或本地執行
- 使用 `dbutils.widgets` 接收參數
- 適合 Databricks Jobs

**notebooks/**
- Databricks Notebook 格式
- 使用 Magic Commands (`# MAGIC %md`)
- 支援互動式執行
- 適合手動操作和開發

---

## 設計原則與模式

### 1. 關注點分離（Separation of Concerns）

**決策邏輯與執行動作分離**

✅ **好的設計：**
```python
# 步驟 1：收集所有需要刪除的表（決策）
tables_to_delete = []
for row in df.collect():
    if should_delete(row):
        tables_to_delete.append(row)

# 步驟 2：批次執行刪除（動作）
for table in tables_to_delete:
    execute_drop(table)
```

❌ **應避免：**
```python
# 決策與執行混在一起
for row in df.collect():
    if should_delete(row):
        execute_drop(row)  # 立即執行，難以測試和回滾
```

### 2. 防禦性編程（Defensive Programming）

**多層安全檢查**

```python
def drop_table_safe(table_name, config):
    # 第一層：白名單檢查
    if in_whitelist(table_name):
        return False, "在白名單中"
    
    # 第二層：黑名單檢查
    if in_blacklist(table_name):
        return False, "在黑名單中"
    
    # 第三層：儲存檢查
    if storage_exists(table_name):
        return False, "資料存在"
    
    # 第四層：Dry-run 檢查
    if config.dry_run:
        log.info(f"[DRY-RUN] 將刪除：{table_name}")
        return True, "Dry-run"
    
    # 最後才執行實際刪除
    drop_table(table_name)
    return True, "已刪除"
```

### 3. 明確的狀態管理

**使用明確的回傳值和狀態碼**

```python
# 使用 tuple 回傳多個值
def check_table(table_name) -> Tuple[bool, str, Optional[Dict]]:
    """
    回傳
    -------
    (是否可刪除, 原因說明, 額外資訊)
    """
    pass

# 使用字典記錄詳細狀態
candidate_info = {
    'table_name': full_name,
    'action': 'deleted',  # 明確的動作狀態
    'reason': '儲存不存在',
    'timestamp': datetime.now()
}
```

### 4. 文件優先（Documentation First）

**完整的繁體中文文檔**

```python
def function_name(param1: Type1, param2: Type2) -> ReturnType:
    """
    函式的簡短描述（一行）
    
    詳細說明函式的功能、使用場景和注意事項。
    
    參數
    ----------
    param1: Type1
        參數 1 的說明
    param2: Type2
        參數 2 的說明
    
    回傳
    -------
    ReturnType
        回傳值的說明
    
    範例
    -------
    >>> result = function_name(arg1, arg2)
    >>> print(result)
    
    注意事項
    -------
    - 重要的使用限制
    - 效能考量
    - 安全性提醒
    """
    pass
```

---

## 擴展與客製化

### 場景 1：新增其他表格式支援（例如：Avro、ORC）

**步驟 1：** 在 `get_tables_details()` 修改過濾條件

```python
# 在 common/helpers.py 中
tableDetailsDF = tableDetailsDF.where(
    "lower(provider) in ('delta','parquet','avro','orc') and lower(type) <> 'view'"
)
```

**步驟 2：** 在 `drop_table_definition_without_storage_safe()` 新增檢查邏輯

```python
# 在清理邏輯中新增分支
if row.Provider.lower() == 'avro':
    # 實作 Avro 表的儲存檢查邏輯
    storage_exists = check_avro_storage(spark, row.Location)
elif row.Provider.lower() == 'orc':
    # 實作 ORC 表的儲存檢查邏輯
    storage_exists = check_orc_storage(spark, row.Location)
```

**步驟 3：** 實作對應的檢查函式

```python
def check_avro_storage(spark: SparkSession, location: str) -> bool:
    """
    檢查 Avro 表的儲存是否存在
    
    參數
    ----------
    spark: SparkSession
        Spark session 實例
    location: str
        Avro 表的儲存路徑
    
    回傳
    -------
    bool
        True 表示儲存存在，False 表示不存在
    """
    try:
        # 檢查路徑是否存在且包含 .avro 檔案
        files = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark._jsc.hadoopConfiguration()
        ).listStatus(spark._jvm.org.apache.hadoop.fs.Path(location))
        
        for file in files:
            if file.getPath().getName().endswith('.avro'):
                return True
        return False
    except:
        return False
```

### 場景 2：整合外部通知系統（Email、Slack）

**步驟 1：** 建立通知模組 `common/notifications.py`

```python
"""
通知系統整合模組

支援通知清理結果到外部系統
"""

from typing import List, Dict
import requests

def send_slack_notification(
    webhook_url: str,
    deleted: int,
    skipped: int,
    candidates: List[Dict]
):
    """
    發送 Slack 通知
    
    參數
    ----------
    webhook_url: str
        Slack Webhook URL
    deleted: int
        刪除的表數量
    skipped: int
        跳過的表數量
    candidates: List[Dict]
        候選表清單
    """
    message = {
        "text": f"Databricks 清理作業完成",
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*清理統計*\n✓ 刪除：{deleted} 個表\n⊘ 跳過：{skipped} 個表"
                }
            }
        ]
    }
    
    response = requests.post(webhook_url, json=message)
    return response.status_code == 200
```

**步驟 2：** 在清理完成後呼叫

```python
from common.notifications import send_slack_notification

# 執行清理
deleted, candidates = drop_table_definition_without_storage_safe(
    spark, tableDetailsDF, logger, config
)

# 發送通知
webhook_url = dbutils.secrets.get(scope="notifications", key="slack-webhook")
send_slack_notification(webhook_url, deleted, len(candidates) - deleted, candidates)
```

### 場景 3：新增自訂過濾條件

**步驟 1：** 擴展 `CleanupConfig` 類別

```python
# 在 common/config.py 中
class CleanupConfig:
    def __init__(
        self,
        # ... 現有參數 ...
        custom_filter_fn: Optional[Callable[[str, str, str], bool]] = None
    ):
        # ... 現有初始化 ...
        self.custom_filter_fn = custom_filter_fn
    
    def apply_custom_filter(self, database: str, table: str, location: str) -> bool:
        """
        應用自訂過濾條件
        
        參數
        ----------
        database: str
            資料庫名稱
        table: str
            表名稱
        location: str
            儲存位置
        
        回傳
        -------
        bool
            True 表示通過過濾，False 表示應該跳過
        """
        if self.custom_filter_fn:
            return self.custom_filter_fn(database, table, location)
        return True
```

**步驟 2：** 使用自訂過濾

```python
# 定義自訂過濾邏輯
def my_custom_filter(database: str, table: str, location: str) -> bool:
    # 只處理特定路徑的表
    if '/staging/' in location:
        return True
    # 排除包含特定關鍵字的表
    if 'important' in table.lower():
        return False
    return True

# 建立配置
config = CleanupConfig(
    dry_run=True,
    custom_filter_fn=my_custom_filter
)
```

---

## 測試指南

### 測試架構

```
tests/
├── context.py               # 測試環境設定
├── test_config.py          # 配置模組測試
└── test_clean_tables_without_storage.py  # 核心功能測試
```

### 執行測試

**在 Databricks 環境中：**
```python
# 在 Notebook 中執行
%run tests/test_config.py
%run tests/test_clean_tables_without_storage.py
```

**在本地環境中（需要 databricks-connect）：**
```bash
pytest tests/ -v
pytest tests/test_config.py -v  # 只執行特定測試
```

### 編寫測試的最佳實務

**1. 測試命名規範**
```python
def test_<功能>_<場景>_<預期結果>():
    """測試 <具體功能> 在 <特定場景> 下 <應該有的行為>"""
    pass

# 範例
def test_whitelist_patterns_should_match_wildcard():
    """測試白名單模式應該支援萬用字元匹配"""
    pass
```

**2. 使用 Arrange-Act-Assert 模式**
```python
def test_config_whitelist():
    # Arrange: 準備測試資料
    config = CleanupConfig(whitelist_patterns=['prod.*'])
    
    # Act: 執行測試動作
    is_allowed, reason = config.is_table_deletion_allowed('prod.sales.orders')
    
    # Assert: 驗證結果
    assert is_allowed == False
    assert '白名單' in reason
```

**3. 測試邊界條件**
```python
def test_empty_whitelist():
    """測試空白名單的行為"""
    config = CleanupConfig(whitelist_patterns=[])
    is_allowed, _ = config.is_table_deletion_allowed('any.table')
    assert is_allowed == True

def test_none_whitelist():
    """測試 None 白名單的行為"""
    config = CleanupConfig(whitelist_patterns=None)
    is_allowed, _ = config.is_table_deletion_allowed('any.table')
    assert is_allowed == True
```

---

## 程式碼風格指南

### Python 程式碼風格

遵循 PEP 8 標準，並加上專案特定規範：

**1. 命名規範**
```python
# 函式和變數：小寫 + 底線
def get_table_details():
    table_name = "example"

# 類別：大駝峰
class CleanupConfig:
    pass

# 常數：大寫 + 底線
DEFAULT_CONFIG = CleanupConfig()
MAX_RETRY_COUNT = 3
```

**2. 型別提示**
```python
from typing import List, Dict, Optional, Tuple

def process_tables(
    tables: List[str],
    config: Optional[CleanupConfig] = None
) -> Tuple[int, List[Dict]]:
    """明確的型別提示提升程式碼可讀性"""
    pass
```

**3. 文檔字串**
- 使用 NumPy 風格的文檔字串
- 必須使用繁體中文
- 包含參數、回傳值、範例、注意事項

```python
def example_function(param1: str, param2: int) -> bool:
    """
    簡短的一行描述
    
    詳細的多行描述，說明函式的功能、使用場景等。
    
    參數
    ----------
    param1: str
        參數說明
    param2: int
        參數說明
    
    回傳
    -------
    bool
        回傳值說明
    
    範例
    -------
    >>> result = example_function("test", 123)
    >>> print(result)
    True
    
    注意事項
    -------
    - 重要的使用限制
    - 效能考量
    """
    pass
```

**4. 註解風格**
```python
# 單行註解使用 # 開頭，空一格後開始內容

# 對於複雜邏輯，使用多行註解說明
# 第一行說明整體目的
# 第二行說明具體步驟
# 第三行說明預期結果

"""
對於非常複雜的邏輯區塊，使用文檔字串格式
來提供更詳細的說明
"""
```

### Git Commit 訊息規範

使用繁體中文，遵循以下格式：

```
<類型>: <簡短描述>

<詳細說明>（可選）

<相關 Issue>（可選）
```

**類型標籤：**
- `新增`：新功能
- `修正`：Bug 修復
- `文件`：文件更新
- `重構`：程式碼重構
- `測試`：測試相關
- `優化`：效能優化

**範例：**
```
新增: 支援 Avro 表格式的儲存檢查

實作 check_avro_storage() 函式來檢查 Avro 表的儲存是否存在。
此功能擴展了清理工具對不同表格式的支援。

相關 Issue: #123
```

---

## 貢獻流程

### 1. 建立分支

```bash
git checkout -b feature/your-feature-name
# 或
git checkout -b bugfix/issue-description
```

### 2. 開發與測試

- 編寫程式碼
- 新增單元測試
- 確保所有測試通過
- 更新相關文件

### 3. 提交變更

```bash
git add .
git commit -m "新增: 功能描述"
git push origin feature/your-feature-name
```

### 4. 建立 Pull Request

- 在 GitHub 上建立 Pull Request
- 填寫詳細的變更說明
- 等待 Code Review

### 5. Code Review 檢查清單

- [ ] 程式碼遵循專案風格指南
- [ ] 有完整的繁體中文文檔和註解
- [ ] 包含適當的單元測試
- [ ] 所有測試都通過
- [ ] 更新了相關文件（README、設計文件等）
- [ ] 沒有引入新的安全風險
- [ ] 向後相容（或明確說明破壞性變更）

---

## 常見開發場景

### 場景 1：如何除錯清理邏輯

**步驟 1：** 開啟 Debug 模式
```python
logger = logs(name='Cleaner', level='debug', debug=True)
```

**步驟 2：** 使用 Dry-run 模式
```python
config = CleanupConfig(dry_run=True)
deleted, candidates = drop_table_definition_without_storage_safe(
    spark, df, logger, config
)

# 檢查 candidates 了解決策過程
for c in candidates:
    print(f"{c['table_name']}: {c['action']} - {c['reason']}")
```

**步驟 3：** 針對單一表測試
```python
# 只測試一個表
test_df = tableDetailsDF.filter(col('Table') == 'specific_table_name')
deleted, candidates = drop_table_definition_without_storage_safe(
    spark, test_df, logger, config
)
```

### 場景 2：如何新增自訂日誌輸出

**方法 1：** 擴展 logs 類別
```python
class CustomLogger(logs):
    def __init__(self, name, level='info', debug=True, log_file=None):
        super().__init__(name, level, debug)
        self.log_file = log_file
    
    def trace(self, msg):
        super().trace(msg)
        if self.log_file:
            with open(self.log_file, 'a') as f:
                f.write(f"{datetime.now()}: {msg}\n")
```

**方法 2：** 將日誌寫入 Delta Table
```python
def log_to_delta(spark, log_table, message, level='INFO'):
    """將日誌寫入 Delta Table"""
    log_entry = [(datetime.now(), level, message)]
    log_df = spark.createDataFrame(log_entry, ['timestamp', 'level', 'message'])
    log_df.write.format('delta').mode('append').saveAsTable(log_table)
```

### 場景 3：如何進行效能優化

**1. 使用 information_schema 批次查詢**（參見 optimization-notes.md）

**2. 平行化處理**
```python
from concurrent.futures import ThreadPoolExecutor

def check_table_parallel(tables, max_workers=10):
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = list(executor.map(check_single_table, tables))
    return results
```

**3. 快取中間結果**
```python
# 快取表詳細資訊 DataFrame
tableDetailsDF.cache()

# 使用完畢後釋放快取
tableDetailsDF.unpersist()
```

---

## 參考資源

### 內部文件
- [系統架構說明](system-design.md)
- [效能優化建議](optimization-notes.md)
- [配置範例](config-examples.md)

### 外部資源
- [Databricks External Tables 文件](https://learn.microsoft.com/en-us/azure/databricks/tables/external)
- [Unity Catalog 最佳實務](https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/best-practices)
- [Delta Lake 文件](https://docs.delta.io/latest/index.html)
- [PEP 8 Python 程式碼風格指南](https://peps.python.org/pep-0008/)

### 聯絡與支援
如有開發相關問題，歡迎透過以下方式聯繫：
- 開啟 GitHub Issue
- 提交 Pull Request
- 參與 Code Review

---

**版本資訊**
- 文件版本：1.0
- 最後更新：2026-02-12
- 維護者：專案團隊
