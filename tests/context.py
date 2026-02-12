"""
Context 設定模組 - 測試環境版本

此模組的作用與 scripts/context.py 相同，但用於測試環境。
它設定 Python 模組的搜尋路徑，讓 tests 目錄中的測試檔案
可以正確匯入 common 目錄中的工具函式。

工作原理：
1. 使用 os.path 取得當前檔案的父目錄的父目錄（專案根目錄）
2. 將該路徑加入 sys.path
3. 從 common.helpers 匯入所有必要的函式和類別

在測試環境中：
- 此檔案會在測試檔案的開頭被匯入（from context import *）
- 確保測試可以存取 common 模組中的所有工具函式
- 讓測試能夠使用與生產環境相同的程式碼
"""

#region Path to the modules
import os
import sys

# 取得專案根目錄的絕對路徑
# __file__: 當前檔案的路徑
# os.path.dirname(__file__): tests 目錄
# os.path.dirname(os.path.dirname(__file__)): 專案根目錄
base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# 將專案根目錄加入 Python 模組搜尋路徑
# 這樣就可以使用 "from common.helpers import ..." 的方式匯入
sys.path.append(base_path)
#endregion

# 從 common.helpers 模組匯入所有核心函式和類別
# 包括：
# - file_exists: 檢查檔案/目錄是否存在
# - get_tables: 取得表清單
# - get_tables_details: 取得表詳細資訊
# - create_empty_dataframe: 建立空 DataFrame
# - logs: 日誌管理類別
# - drop_table_definition_without_storage: 基本版清理函式
# - drop_table_definition_without_storage_safe: 安全版清理函式
# - confirm_deletion_interactive: 互動確認函式
from common.helpers import *

