"""
Databricks External Tables Cleaner - 配置與安全功能測試

本測試模組涵蓋：
- CleanupConfig 類別的各種功能
- 白名單/黑名單模式匹配
- 保留條件檢查
- Dry-run 模式
"""

import pytest
from datetime import date, datetime, timedelta
from common.config import CleanupConfig, DEFAULT_CONFIG


class TestCleanupConfig:
    """測試 CleanupConfig 類別的基本功能"""

    def test_default_config_should_be_safe(self):
        """預設配置應該是安全的（dry_run=True）"""
        # ARRANGE & ACT
        config = CleanupConfig()

        # ASSERT
        assert config.dry_run == True
        assert config.require_confirmation == False
        assert config.whitelist_patterns == []
        assert config.blacklist_patterns == []

    def test_custom_config_should_set_all_parameters(self):
        """自訂配置應該正確設定所有參數"""
        # ARRANGE & ACT
        config = CleanupConfig(
            dry_run=False,
            whitelist_patterns=['prod.*', 'critical_*'],
            blacklist_patterns=['test.*'],
            max_last_access_age_days=90,
            require_confirmation=True
        )

        # ASSERT
        assert config.dry_run == False
        assert config.whitelist_patterns == ['prod.*', 'critical_*']
        assert config.blacklist_patterns == ['test.*']
        assert config.max_last_access_age_days == 90
        assert config.require_confirmation == True

    def test_to_dict_should_serialize_config(self):
        """to_dict 應該正確序列化配置"""
        # ARRANGE
        config = CleanupConfig(
            dry_run=True,
            whitelist_patterns=['prod.*'],
            min_create_date=date(2023, 1, 1)
        )

        # ACT
        config_dict = config.to_dict()

        # ASSERT
        assert config_dict['dry_run'] == True
        assert config_dict['whitelist_patterns'] == ['prod.*']
        assert config_dict['min_create_date'] == '2023-01-01'

    def test_from_dict_should_deserialize_config(self):
        """from_dict 應該正確反序列化配置"""
        # ARRANGE
        config_dict = {
            'dry_run': False,
            'whitelist_patterns': ['prod.*', 'critical_*'],
            'blacklist_patterns': ['test.*'],
            'min_create_date': '2023-01-01',
            'max_last_access_age_days': 90,
            'require_confirmation': True,
            'estimate_storage_size': False
        }

        # ACT
        config = CleanupConfig.from_dict(config_dict)

        # ASSERT
        assert config.dry_run == False
        assert config.whitelist_patterns == ['prod.*', 'critical_*']
        assert config.blacklist_patterns == ['test.*']
        assert config.min_create_date == date(2023, 1, 1)
        assert config.max_last_access_age_days == 90
        assert config.require_confirmation == True


class TestWhitelistBlacklist:
    """測試白名單和黑名單功能"""

    def test_table_in_whitelist_should_not_be_deleted(self):
        """白名單中的表不應被刪除"""
        # ARRANGE
        config = CleanupConfig(
            whitelist_patterns=['prod.*', 'critical_*']
        )

        # ACT
        is_allowed, reason = config.is_table_deletion_allowed('prod.sales.orders')

        # ASSERT
        assert is_allowed == False
        assert '白名單' in reason

    def test_table_in_blacklist_should_not_be_deleted(self):
        """黑名單中的表不應被刪除"""
        # ARRANGE
        config = CleanupConfig(
            blacklist_patterns=['test.*', 'temp_*']
        )

        # ACT
        is_allowed, reason = config.is_table_deletion_allowed('test.staging.data')

        # ASSERT
        assert is_allowed == False
        assert '黑名單' in reason

    def test_table_not_in_any_list_should_be_allowed(self):
        """不在任何名單中的表應該允許刪除"""
        # ARRANGE
        config = CleanupConfig(
            whitelist_patterns=['prod.*'],
            blacklist_patterns=['test.*']
        )

        # ACT
        is_allowed, reason = config.is_table_deletion_allowed('dev.staging.table1')

        # ASSERT
        assert is_allowed == True
        assert '允許刪除' in reason

    def test_whitelist_should_have_higher_priority_than_blacklist(self):
        """白名單優先級應該高於黑名單"""
        # ARRANGE
        config = CleanupConfig(
            whitelist_patterns=['critical_*'],
            blacklist_patterns=['critical_test']
        )

        # ACT
        is_allowed, reason = config.is_table_deletion_allowed('hive_metastore.default.critical_test')

        # ASSERT
        assert is_allowed == False
        assert '白名單' in reason

    def test_wildcard_star_should_match_multiple_chars(self):
        """萬用字元 * 應該匹配多個字元"""
        # ARRANGE
        config = CleanupConfig(
            whitelist_patterns=['prod.*']
        )

        # ACT & ASSERT
        is_allowed1, _ = config.is_table_deletion_allowed('prod.sales.orders')
        is_allowed2, _ = config.is_table_deletion_allowed('prod.analytics.reports')
        is_allowed3, _ = config.is_table_deletion_allowed('production.sales.orders')

        assert is_allowed1 == False  # 匹配
        assert is_allowed2 == False  # 匹配
        assert is_allowed3 == True   # 不匹配

    def test_wildcard_question_mark_should_match_single_char(self):
        """萬用字元 ? 應該匹配單一字元"""
        # ARRANGE
        config = CleanupConfig(
            whitelist_patterns=['test_?']
        )

        # ACT & ASSERT
        is_allowed1, _ = config.is_table_deletion_allowed('hive_metastore.default.test_1')
        is_allowed2, _ = config.is_table_deletion_allowed('hive_metastore.default.test_12')

        assert is_allowed1 == False  # 匹配 test_?
        assert is_allowed2 == True   # 不匹配（有兩個字元）

    def test_pattern_should_match_full_table_name(self):
        """模式應該匹配完整的表名稱"""
        # ARRANGE
        config = CleanupConfig(
            whitelist_patterns=['*.important_*']
        )

        # ACT & ASSERT
        is_allowed1, _ = config.is_table_deletion_allowed('prod.sales.important_data')
        is_allowed2, _ = config.is_table_deletion_allowed('dev.staging.important_table')
        is_allowed3, _ = config.is_table_deletion_allowed('prod.sales.regular_data')

        assert is_allowed1 == False  # 匹配
        assert is_allowed2 == False  # 匹配
        assert is_allowed3 == True   # 不匹配

    def test_multiple_patterns_should_work_correctly(self):
        """多個模式應該正確運作"""
        # ARRANGE
        config = CleanupConfig(
            whitelist_patterns=['prod.*', 'critical_*', '*.important_*']
        )

        # ACT & ASSERT
        test_cases = [
            ('prod.sales.orders', False),           # 匹配 prod.*
            ('hive_metastore.default.critical_data', False),  # 匹配 critical_*
            ('dev.staging.important_table', False), # 匹配 *.important_*
            ('dev.staging.regular_table', True)     # 不匹配任何模式
        ]

        for table_name, expected_allowed in test_cases:
            is_allowed, _ = config.is_table_deletion_allowed(table_name)
            assert is_allowed == expected_allowed, f"Failed for {table_name}"


class TestRetentionConditions:
    """測試保留條件功能"""

    def test_table_created_after_min_date_should_be_kept(self):
        """建立時間晚於最小日期的表應該被保留"""
        # ARRANGE
        config = CleanupConfig(
            min_create_date=date(2023, 1, 1)
        )
        create_time = datetime(2024, 1, 1)  # 2024 年建立，晚於 2023

        # ACT
        is_allowed, reason = config.check_retention_conditions(
            create_time=create_time
        )

        # ASSERT
        assert is_allowed == False
        assert '建立時間太新' in reason

    def test_table_created_before_min_date_should_be_deleted(self):
        """建立時間早於最小日期的表可以被刪除"""
        # ARRANGE
        config = CleanupConfig(
            min_create_date=date(2023, 1, 1)
        )
        create_time = datetime(2022, 1, 1)  # 2022 年建立，早於 2023

        # ACT
        is_allowed, reason = config.check_retention_conditions(
            create_time=create_time
        )

        # ASSERT
        assert is_allowed == True
        assert '符合刪除條件' in reason

    def test_table_accessed_recently_should_be_kept(self):
        """最近有存取的表應該被保留"""
        # ARRANGE
        config = CleanupConfig(
            max_last_access_age_days=90
        )
        # 30 天前存取，小於 90 天
        last_access_time = datetime.now() - timedelta(days=30)

        # ACT
        is_allowed, reason = config.check_retention_conditions(
            last_access_time=last_access_time
        )

        # ASSERT
        assert is_allowed == False
        assert '最近有存取' in reason

    def test_table_not_accessed_for_long_time_should_be_deleted(self):
        """長時間未存取的表可以被刪除"""
        # ARRANGE
        config = CleanupConfig(
            max_last_access_age_days=90
        )
        # 180 天前存取，大於 90 天
        last_access_time = datetime.now() - timedelta(days=180)

        # ACT
        is_allowed, reason = config.check_retention_conditions(
            last_access_time=last_access_time
        )

        # ASSERT
        assert is_allowed == True
        assert '符合刪除條件' in reason

    def test_missing_time_info_should_warn_user(self):
        """缺少時間資訊應該警告使用者"""
        # ARRANGE
        config = CleanupConfig(
            max_last_access_age_days=90
        )

        # ACT
        is_allowed, reason = config.check_retention_conditions(
            last_access_time=None  # 缺少時間資訊
        )

        # ASSERT
        assert is_allowed == True
        assert '缺少時間資訊' in reason

    def test_multiple_retention_conditions_should_all_be_checked(self):
        """多個保留條件應該都被檢查"""
        # ARRANGE
        config = CleanupConfig(
            min_create_date=date(2023, 1, 1),
            max_last_access_age_days=90
        )

        # 測試案例 1：建立時間太新（應該被保留）
        create_time_new = datetime(2024, 1, 1)
        last_access_old = datetime.now() - timedelta(days=180)

        is_allowed1, reason1 = config.check_retention_conditions(
            create_time=create_time_new,
            last_access_time=last_access_old
        )

        assert is_allowed1 == False
        assert '建立時間太新' in reason1

        # 測試案例 2：建立時間符合但最近有存取（應該被保留）
        create_time_old = datetime(2022, 1, 1)
        last_access_recent = datetime.now() - timedelta(days=30)

        is_allowed2, reason2 = config.check_retention_conditions(
            create_time=create_time_old,
            last_access_time=last_access_recent
        )

        assert is_allowed2 == False
        assert '最近有存取' in reason2

        # 測試案例 3：兩個條件都符合（可以刪除）
        create_time_old = datetime(2022, 1, 1)
        last_access_old = datetime.now() - timedelta(days=180)

        is_allowed3, reason3 = config.check_retention_conditions(
            create_time=create_time_old,
            last_access_time=last_access_old
        )

        assert is_allowed3 == True
        assert '符合刪除條件' in reason3


class TestConfigIntegration:
    """測試配置的整合場景"""

    def test_production_safe_config_example(self):
        """測試生產環境安全配置範例"""
        # ARRANGE
        config = CleanupConfig(
            dry_run=True,
            whitelist_patterns=['prod.*', 'production.*', 'critical_*'],
            blacklist_patterns=['system.*', 'metadata.*'],
            max_last_access_age_days=180,
            require_confirmation=True
        )

        # ACT & ASSERT
        # 白名單中的表
        is_allowed1, _ = config.is_table_deletion_allowed('prod.sales.orders')
        assert is_allowed1 == False

        # 黑名單中的表
        is_allowed2, _ = config.is_table_deletion_allowed('system.internal.config')
        assert is_allowed2 == False

        # 符合條件的表
        is_allowed3, _ = config.is_table_deletion_allowed('dev.staging.temp_table')
        assert is_allowed3 == True

        # 檢查配置參數
        assert config.dry_run == True
        assert config.require_confirmation == True

    def test_test_environment_config_example(self):
        """測試環境配置範例"""
        # ARRANGE
        config = CleanupConfig(
            dry_run=False,
            whitelist_patterns=['important_*'],
            max_last_access_age_days=30,
            require_confirmation=False
        )

        # ACT & ASSERT
        assert config.dry_run == False
        assert config.require_confirmation == False
        assert config.max_last_access_age_days == 30

        # 保護重要的表
        is_allowed, _ = config.is_table_deletion_allowed('test.staging.important_data')
        assert is_allowed == False

    def test_empty_patterns_should_allow_all_tables(self):
        """空的模式清單應該允許刪除所有表"""
        # ARRANGE
        config = CleanupConfig(
            whitelist_patterns=[],
            blacklist_patterns=[]
        )

        # ACT
        is_allowed, reason = config.is_table_deletion_allowed('any.schema.table')

        # ASSERT
        assert is_allowed == True
        assert '允許刪除' in reason
