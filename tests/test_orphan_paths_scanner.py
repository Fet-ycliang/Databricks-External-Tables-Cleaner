"""
測試孤兒目錄掃描功能

注意：這些測試需要在 Databricks 環境中執行，因為需要 SparkSession 和 dbutils。
"""

import pytest
from common.config import OrphanScanConfig


class TestOrphanScanConfig:
    """測試孤兒掃描配置類別"""

    def test_default_config(self):
        """測試預設配置"""
        config = OrphanScanConfig()

        assert config.enabled is True
        assert config.base_paths == []
        assert config.catalogs is None
        assert config.max_depth == 2
        assert config.dry_run is True
        assert config.output_format == 'notebook'
        assert config.output_table is None

    def test_custom_config(self):
        """測試自訂配置"""
        config = OrphanScanConfig(
            enabled=True,
            base_paths=['abfss://data@myacct.dfs.core.windows.net/raw/'],
            catalogs=['main', 'dev'],
            max_depth=3,
            output_format='delta_table',
            output_table='main.audit.orphan_paths'
        )

        assert config.enabled is True
        assert len(config.base_paths) == 1
        assert config.catalogs == ['main', 'dev']
        assert config.max_depth == 3
        assert config.output_format == 'delta_table'
        assert config.output_table == 'main.audit.orphan_paths'

    def test_validate_no_base_paths(self):
        """測試驗證：沒有 base_paths"""
        config = OrphanScanConfig(enabled=True, base_paths=[])

        is_valid, message = config.validate()

        assert is_valid is False
        assert '至少指定一個 base_path' in message

    def test_validate_invalid_max_depth(self):
        """測試驗證：無效的 max_depth"""
        config = OrphanScanConfig(
            base_paths=['abfss://data@myacct.dfs.core.windows.net/raw/'],
            max_depth=0
        )

        is_valid, message = config.validate()

        assert is_valid is False
        assert 'max_depth' in message

    def test_validate_invalid_max_depth_too_large(self):
        """測試驗證：max_depth 過大"""
        config = OrphanScanConfig(
            base_paths=['abfss://data@myacct.dfs.core.windows.net/raw/'],
            max_depth=11
        )

        is_valid, message = config.validate()

        assert is_valid is False
        assert 'max_depth' in message

    def test_validate_invalid_output_format(self):
        """測試驗證：無效的輸出格式"""
        config = OrphanScanConfig(
            base_paths=['abfss://data@myacct.dfs.core.windows.net/raw/'],
            output_format='invalid'
        )

        is_valid, message = config.validate()

        assert is_valid is False
        assert 'output_format' in message

    def test_validate_delta_table_without_table_name(self):
        """測試驗證：使用 delta_table 但未指定 table 名稱"""
        config = OrphanScanConfig(
            base_paths=['abfss://data@myacct.dfs.core.windows.net/raw/'],
            output_format='delta_table',
            output_table=None
        )

        is_valid, message = config.validate()

        assert is_valid is False
        assert 'output_table' in message

    def test_validate_valid_config(self):
        """測試驗證：有效配置"""
        config = OrphanScanConfig(
            base_paths=['abfss://data@myacct.dfs.core.windows.net/raw/'],
            catalogs=['main'],
            max_depth=2,
            output_format='notebook'
        )

        is_valid, message = config.validate()

        assert is_valid is True
        assert '有效' in message

    def test_validate_disabled_config(self):
        """測試驗證：停用的配置"""
        config = OrphanScanConfig(enabled=False)

        is_valid, message = config.validate()

        assert is_valid is True
        assert '停用' in message

    def test_to_dict(self):
        """測試配置轉換為字典"""
        config = OrphanScanConfig(
            enabled=True,
            base_paths=['abfss://data@myacct.dfs.core.windows.net/raw/'],
            catalogs=['main'],
            max_depth=3,
            output_format='log'
        )

        config_dict = config.to_dict()

        assert config_dict['enabled'] is True
        assert config_dict['base_paths'] == ['abfss://data@myacct.dfs.core.windows.net/raw/']
        assert config_dict['catalogs'] == ['main']
        assert config_dict['max_depth'] == 3
        assert config_dict['output_format'] == 'log'

    def test_from_dict(self):
        """測試從字典建立配置"""
        config_dict = {
            'enabled': True,
            'base_paths': ['abfss://data@myacct.dfs.core.windows.net/raw/'],
            'catalogs': ['main', 'dev'],
            'max_depth': 3,
            'dry_run': True,
            'output_format': 'delta_table',
            'output_table': 'main.audit.orphan_paths'
        }

        config = OrphanScanConfig.from_dict(config_dict)

        assert config.enabled is True
        assert config.base_paths == ['abfss://data@myacct.dfs.core.windows.net/raw/']
        assert config.catalogs == ['main', 'dev']
        assert config.max_depth == 3
        assert config.dry_run is True
        assert config.output_format == 'delta_table'
        assert config.output_table == 'main.audit.orphan_paths'

    def test_serialization_roundtrip(self):
        """測試序列化往返"""
        original = OrphanScanConfig(
            enabled=True,
            base_paths=['abfss://data@myacct.dfs.core.windows.net/raw/'],
            catalogs=['main'],
            max_depth=2,
            output_format='notebook'
        )

        # 轉換為字典再轉回配置
        config_dict = original.to_dict()
        restored = OrphanScanConfig.from_dict(config_dict)

        # 驗證所有屬性相同
        assert restored.enabled == original.enabled
        assert restored.base_paths == original.base_paths
        assert restored.catalogs == original.catalogs
        assert restored.max_depth == original.max_depth
        assert restored.dry_run == original.dry_run
        assert restored.output_format == original.output_format
        assert restored.output_table == original.output_table


class TestOrphanPathsScanner:
    """
    測試孤兒目錄掃描器

    注意：這些測試需要實際的 Spark 環境和儲存存取權限。
    以下是骨架測試，需要在 Databricks 環境中執行完整測試。
    """

    def test_normalize_path_abfss(self):
        """測試 ABFSS 路徑正規化"""
        from common.orphan_paths_scanner import OrphanPathsScanner

        # 這個測試需要 mock SparkSession 和 log
        # 在實際環境中，這會是完整的整合測試

        # 骨架測試：驗證路徑正規化邏輯
        test_cases = [
            ('abfss://data@myacct.dfs.core.windows.net/raw/', 'abfss://data@myacct.dfs.core.windows.net/raw'),
            ('abfss://data@myacct.dfs.core.windows.net/raw', 'abfss://data@myacct.dfs.core.windows.net/raw'),
            ('wasbs://data@myacct.blob.core.windows.net/raw/', 'abfss://data@myacct.blob.core.windows.net/raw'),
        ]

        # 實際測試需要在 Databricks 環境中運行
        # 這裡只是文件化預期行為

    def test_path_comparison_logic(self):
        """測試路徑比對邏輯"""
        # 測試案例：
        # 1. 完全匹配
        # 2. 子路徑匹配
        # 3. 父路徑匹配
        # 4. 不匹配（孤兒目錄）

        # 實際測試需要在 Databricks 環境中運行
        pass


# 執行測試的說明
"""
在 Databricks 環境中執行測試：

1. 確保已安裝 pytest
2. 使用 databricks-connect 連接到 cluster
3. 執行測試：
   pytest tests/test_orphan_paths_scanner.py -v

注意：完整的整合測試需要：
- 有效的 SparkSession
- Unity Catalog 存取權限
- Storage 存取權限
- 測試用的 external tables/volumes/locations
"""
