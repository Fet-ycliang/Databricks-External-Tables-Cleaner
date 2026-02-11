"""
Context 設定模組 - Notebook 版本

此模組的作用與 scripts/context.py 相同，但用於 Databricks Notebooks。
它設定 Python 模組的搜尋路徑，讓 notebooks 目錄中的 notebook
可以正確匯入 common 目錄中的工具函式。

工作原理：
1. 使用 os.path 取得當前檔案的父目錄的父目錄（專案根目錄）
2. 將該路徑加入 sys.path
3. 從 common.helpers 匯入所有必要的函式和類別

在 Databricks Notebooks 中：
- 此檔案通常會在 notebook 的第一個 cell 中被匯入
- 確保 notebook 可以存取 common 模組中的所有工具函式
"""

#region Path to the modules
import os
import sys

# 取得專案根目錄的絕對路徑
# __file__: 當前檔案的路徑
# os.path.dirname(__file__): notebooks 目錄
# os.path.dirname(os.path.dirname(__file__)): 專案根目錄
base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# 將專案根目錄加入 Python 模組搜尋路徑
sys.path.append(base_path)
#endregion

# 從 common.helpers 模組匯入所有核心函式
# 包括：file_exists, get_tables, get_tables_details, create_empty_dataframe,
#       logs, drop_table_definition_without_storage
from common.helpers import *


