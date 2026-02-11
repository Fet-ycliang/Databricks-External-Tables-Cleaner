# Databricks External Tables Cleaner - 配置範例

此檔案提供不同場景下的配置範例，可根據實際需求調整。

## 配置參數說明

### 基本參數
- `dry_run`: Dry-run 模式（預設：True）
  - True: 只列出將被刪除的表，不實際執行刪除
  - False: 實際執行刪除操作

- `require_confirmation`: 是否需要互動式確認（預設：False）
  - True: 在刪除前需要使用者輸入 YES 確認
  - False: 不需要確認，直接執行（適合自動化 Job）

### 白名單/黑名單
- `whitelist_patterns`: 白名單模式清單（優先級最高）
  - 列入白名單的表永不刪除
  - 支援萬用字元：`*`（任意字元）、`?`（單一字元）
  - 範例：`['prod.*', 'critical_*', 'main.important.*']`

- `blacklist_patterns`: 黑名單模式清單
  - 列入黑名單的表禁止刪除
  - 範例：`['test.*', 'temp_*', 'staging.tmp_*']`

### 保留條件（未來功能）
- `min_create_date`: 最小建立日期
  - 只有在此日期之前建立的表才會被清除
  - 格式：`date(2023, 1, 1)`

- `max_last_access_age_days`: 最後存取時間的最大天數
  - 只有超過此天數未被存取的表才會被清除
  - 範例：`90`（90 天未存取）

### 進階參數
- `estimate_storage_size`: 是否估算儲存空間大小（預設：False）
  - True: 嘗試計算將被刪除的資料大小（較慢）
  - False: 不計算（較快）

---

## 配置範例

### 範例 1：預設安全配置（推薦首次使用）

```python
from common.config import CleanupConfig

config = CleanupConfig(
    dry_run=True,                    # 安全模式：只預覽不刪除
    require_confirmation=True,       # 需要使用者確認
    whitelist_patterns=[],           # 無白名單
    blacklist_patterns=[],           # 無黑名單
    estimate_storage_size=False      # 不估算大小（較快）
)
```

**適用場景**：
- 首次使用工具
- 探索將被刪除的表
- 測試環境初步驗證

---

### 範例 2：生產環境安全配置（推薦）

```python
from common.config import CleanupConfig

config = CleanupConfig(
    dry_run=True,                    # 先執行 Dry-run
    require_confirmation=True,       # 需要確認
    whitelist_patterns=[
        'prod.*',                    # 保護所有 prod catalog 的表
        'production.*',              # 保護所有 production catalog 的表
        'critical_*',                # 保護所有 critical_ 開頭的表
        '*.important_*'              # 保護所有 schema 中 important_ 開頭的表
    ],
    blacklist_patterns=[
        'system.*',                  # 禁止刪除 system catalog
        'metadata.*',                # 禁止刪除 metadata catalog
        '*.audit_*'                  # 禁止刪除所有 audit_ 開頭的表
    ],
    max_last_access_age_days=180,   # 只刪除 180 天未存取的表
    estimate_storage_size=True       # 估算釋放的空間
)
```

**適用場景**：
- 生產環境執行
- 需要嚴格的安全控制
- 有明確的保護規則

**執行流程**：
1. 先以 `dry_run=True` 執行，檢查結果
2. 確認無誤後，改為 `dry_run=False` 實際刪除
3. 保存執行日誌供審計

---

### 範例 3：測試環境配置（寬鬆）

```python
from common.config import CleanupConfig

config = CleanupConfig(
    dry_run=False,                   # 直接刪除
    require_confirmation=False,      # 不需要確認（適合自動化）
    whitelist_patterns=[
        'important_*'                # 只保護重要的表
    ],
    blacklist_patterns=[],           # 無黑名單
    max_last_access_age_days=30,    # 30 天未存取即可刪除
    estimate_storage_size=False
)
```

**適用場景**：
- 測試環境
- 開發環境
- 定期自動清理

**注意事項**：
- 僅適用於非關鍵環境
- 建議定期檢查清理日誌
- 確保白名單涵蓋重要的測試資料

---

### 範例 4：自動化 Job 配置

```python
from common.config import CleanupConfig
from datetime import date

config = CleanupConfig(
    dry_run=False,                   # 實際執行刪除
    require_confirmation=False,      # Job 無法互動，不需確認
    whitelist_patterns=[
        'prod.*',
        'production.*',
        'critical_*',
        'main.sensitive.*',
        '*.customer_*',              # 保護所有客戶相關的表
        '*.financial_*'              # 保護所有財務相關的表
    ],
    blacklist_patterns=[
        'system.*',
        'metadata.*',
        'admin.*'
    ],
    max_last_access_age_days=365,   # 一年未存取才刪除（保守）
    estimate_storage_size=True
)
```

**適用場景**：
- Databricks Job 定期執行
- 無人值守的自動化清理
- 需要嚴格的安全規則

**Job 設定建議**：
- 排程：每週或每月執行一次
- 通知：設定執行結果通知（Email/Slack）
- 權限：使用專用服務帳號，權限最小化
- 日誌：啟用詳細日誌，定期檢查

---

### 範例 5：只針對特定 Schema 的配置

```python
from common.config import CleanupConfig

# 假設只清理 'staging' schema
config = CleanupConfig(
    dry_run=True,
    require_confirmation=True,
    whitelist_patterns=[
        'staging.keep_*',            # 保護 staging 中 keep_ 開頭的表
        'staging.reference_*'        # 保護參考資料表
    ],
    blacklist_patterns=[
        'staging.active_*'           # 黑名單：仍在使用中的表
    ],
    max_last_access_age_days=60,
    estimate_storage_size=True
)
```

**適用場景**：
- 清理特定的 staging 環境
- 定期整理臨時表
- 釋放測試資料空間

---

## 在 Notebook 中使用配置

### 方法 1：直接在 Notebook 中建立配置

```python
# 在 Notebook cell 中
from common.config import CleanupConfig

config = CleanupConfig(
    dry_run=True,
    whitelist_patterns=['prod.*', 'critical_*'],
    require_confirmation=True
)
```

### 方法 2：使用 Databricks Widgets 動態設定

```python
# 建立 widgets
dbutils.widgets.dropdown('dry_run', 'True', ['True','False'])
dbutils.widgets.text('whitelist', 'prod.*,critical_*')

# 讀取並建立配置
dry_run = eval(dbutils.widgets.get("dry_run"))
whitelist_str = dbutils.widgets.get("whitelist")
whitelist_patterns = [p.strip() for p in whitelist_str.split(',') if p.strip()]

config = CleanupConfig(
    dry_run=dry_run,
    whitelist_patterns=whitelist_patterns
)
```

### 方法 3：從 JSON 檔案載入配置

```python
import json
from common.config import CleanupConfig

# 從 DBFS 讀取配置檔
with open('/dbfs/configs/cleanup_config.json', 'r') as f:
    config_dict = json.load(f)

config = CleanupConfig.from_dict(config_dict)
```

---

## 配置檔案範例（JSON 格式）

```json
{
  "dry_run": true,
  "whitelist_patterns": [
    "prod.*",
    "production.*",
    "critical_*"
  ],
  "blacklist_patterns": [
    "system.*",
    "metadata.*"
  ],
  "min_create_date": "2023-01-01",
  "max_last_access_age_days": 180,
  "require_confirmation": true,
  "estimate_storage_size": true
}
```

將此 JSON 儲存為檔案，可透過 `CleanupConfig.from_dict()` 載入。

---

## 最佳實務建議

1. **漸進式執行**：
   - 第一次：dry_run=True，檢查結果
   - 第二次：dry_run=False，小範圍測試
   - 第三次：正式環境執行

2. **白名單優先**：
   - 明確列出需要保護的表
   - 使用嚴格的模式（避免過於寬泛）
   - 定期檢查白名單是否仍然適用

3. **日誌與審計**：
   - 開啟 debug 模式記錄詳細日誌
   - 保存執行結果供後續審計
   - 定期檢查 failed 的操作

4. **權限控制**：
   - 限制只有特定人員可執行
   - 使用專用服務帳號執行 Job
   - 避免在生產環境給予過大權限

5. **監控與通知**：
   - 設定執行結果通知
   - 監控刪除的表數量是否異常
   - 建立異常告警機制

---

## 疑難排解

### Q: 如何測試白名單是否生效？

```python
from common.config import CleanupConfig

config = CleanupConfig(
    whitelist_patterns=['prod.*', 'critical_*']
)

# 測試表名是否被白名單保護
is_allowed, reason = config.is_table_deletion_allowed('prod.sales.orders')
print(f"允許刪除: {is_allowed}, 原因: {reason}")
# 輸出：允許刪除: False, 原因: 在白名單中，永不刪除
```

### Q: 如何查看配置的詳細資訊？

```python
config_dict = config.to_dict()
print(json.dumps(config_dict, indent=2, ensure_ascii=False))
```

### Q: 如何在 Dry-run 後切換到實際刪除？

```python
# 第一次執行：Dry-run
config.dry_run = True
deleted, candidates = drop_table_definition_without_storage_safe(spark, df, logger, config)

# 檢查結果，確認無誤後
config.dry_run = False
deleted, candidates = drop_table_definition_without_storage_safe(spark, df, logger, config)
```

---

更多資訊請參考：
- [README.md](../README.md) - 完整的使用說明
- [docs/system-design.md](../docs/system-design.md) - 系統架構設計
- [notebooks/clean_tables_with_dryrun.py](../notebooks/clean_tables_with_dryrun.py) - 範例 Notebook
