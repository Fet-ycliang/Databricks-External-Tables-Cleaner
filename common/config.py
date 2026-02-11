"""
Databricks External Tables Cleaner - 配置模組

本模組提供清理工具的配置管理，包括：
- 白名單/黑名單模式匹配
- 保留條件（建立日期、最後存取時間）
- Dry-run 模式設定

所有設定都透過 CleanupConfig 類別進行管理。
"""

from typing import List, Optional
from datetime import date, datetime, timedelta
import fnmatch


class CleanupConfig:
    """
    清理工具的配置類別

    此類別封裝所有清理相關的配置參數，包括安全控制選項。

    屬性
    -------
    dry_run: bool
        Dry-run 模式，預設為 True
        - True: 只列出將被刪除的表，不實際執行刪除
        - False: 實際執行刪除操作

    whitelist_patterns: List[str]
        白名單模式清單，支援萬用字元（*, ?）
        列入白名單的表永不刪除，優先級最高
        範例: ['prod.*', 'critical_*', 'main.important.table_*']

    blacklist_patterns: List[str]
        黑名單模式清單，支援萬用字元（*, ?）
        列入黑名單的表禁止刪除
        範例: ['test.*', 'temp_*']

    min_create_date: Optional[date]
        最小建立日期，只有在此日期之前建立的表才會被清除
        None 表示不限制

    max_last_access_age_days: Optional[int]
        最後存取時間的最大天數
        只有超過此天數未被存取的表才會被清除
        None 表示不限制

    require_confirmation: bool
        是否需要互動式確認，預設為 False
        - True: 在刪除前需要使用者輸入 YES 確認
        - False: 不需要確認，直接執行（適合自動化 Job）

    estimate_storage_size: bool
        是否估算儲存空間大小，預設為 False
        - True: 嘗試計算將被刪除的資料大小
        - False: 不計算（較快）

    範例
    -------
    >>> # 建立預設配置（安全模式：dry-run + 需確認）
    >>> config = CleanupConfig()
    >>>
    >>> # 建立自訂配置
    >>> config = CleanupConfig(
    ...     dry_run=False,
    ...     whitelist_patterns=['prod.*', 'critical_*'],
    ...     blacklist_patterns=['test.*'],
    ...     max_last_access_age_days=90,
    ...     require_confirmation=True
    ... )
    """

    def __init__(
        self,
        dry_run: bool = True,
        whitelist_patterns: Optional[List[str]] = None,
        blacklist_patterns: Optional[List[str]] = None,
        min_create_date: Optional[date] = None,
        max_last_access_age_days: Optional[int] = None,
        require_confirmation: bool = False,
        estimate_storage_size: bool = False
    ):
        self.dry_run = dry_run
        self.whitelist_patterns = whitelist_patterns or []
        self.blacklist_patterns = blacklist_patterns or []
        self.min_create_date = min_create_date
        self.max_last_access_age_days = max_last_access_age_days
        self.require_confirmation = require_confirmation
        self.estimate_storage_size = estimate_storage_size

    def is_table_deletion_allowed(self, table_full_name: str) -> tuple[bool, str]:
        """
        檢查指定的表是否允許刪除

        此函式根據白名單和黑名單規則判斷表是否可以被刪除。
        白名單的優先級最高，黑名單次之。

        參數
        ----------
        table_full_name: str
            完整的表名稱，格式為 'catalog.schema.table' 或 'schema.table'

        回傳
        -------
        tuple[bool, str]
            (是否允許刪除, 原因說明)
            - (False, "在白名單中"): 表在白名單中，不允許刪除
            - (False, "在黑名單中"): 表在黑名單中，不允許刪除
            - (True, "允許刪除"): 表不在白名單或黑名單中，允許刪除

        範例
        -------
        >>> config = CleanupConfig(
        ...     whitelist_patterns=['prod.*', 'critical_*'],
        ...     blacklist_patterns=['test.*']
        ... )
        >>> config.is_table_deletion_allowed('prod.sales.orders')
        (False, '在白名單中，永不刪除')
        >>> config.is_table_deletion_allowed('test.temp.data')
        (False, '在黑名單中，禁止刪除')
        >>> config.is_table_deletion_allowed('dev.staging.table1')
        (True, '允許刪除')
        """
        # 檢查白名單（優先級最高）
        if self._matches_any_pattern(table_full_name, self.whitelist_patterns):
            return False, "在白名單中，永不刪除"

        # 檢查黑名單
        if self._matches_any_pattern(table_full_name, self.blacklist_patterns):
            return False, "在黑名單中，禁止刪除"

        # 不在任何名單中，允許刪除
        return True, "允許刪除"

    def _matches_any_pattern(self, table_name: str, patterns: List[str]) -> bool:
        """
        檢查表名是否匹配任何一個模式

        支援萬用字元：
        - * : 匹配任意數量的任意字元
        - ? : 匹配單一字元

        參數
        ----------
        table_name: str
            要檢查的表名稱
        patterns: List[str]
            模式清單

        回傳
        -------
        bool
            True: 匹配至少一個模式
            False: 不匹配任何模式
        """
        for pattern in patterns:
            if fnmatch.fnmatch(table_name, pattern):
                return True
        return False

    def check_retention_conditions(
        self,
        create_time: Optional[datetime] = None,
        last_access_time: Optional[datetime] = None
    ) -> tuple[bool, str]:
        """
        檢查表是否符合保留條件

        根據建立時間和最後存取時間判斷表是否應該被保留。

        參數
        ----------
        create_time: Optional[datetime]
            表的建立時間
        last_access_time: Optional[datetime]
            表的最後存取時間

        回傳
        -------
        tuple[bool, str]
            (是否允許刪除, 原因說明)
            - (False, "建立時間太新"): 表建立時間晚於最小建立日期
            - (False, "最近有存取"): 表最後存取時間在指定天數內
            - (True, "符合刪除條件"): 表符合所有刪除條件
            - (True, "缺少時間資訊，請人工確認"): 無法取得時間資訊

        範例
        -------
        >>> config = CleanupConfig(
        ...     min_create_date=date(2023, 1, 1),
        ...     max_last_access_age_days=90
        ... )
        >>> # 表建立於 2024 年，太新
        >>> config.check_retention_conditions(
        ...     create_time=datetime(2024, 1, 1),
        ...     last_access_time=datetime(2023, 1, 1)
        ... )
        (False, '建立時間太新，不符合刪除條件')
        """
        # 檢查建立日期
        if self.min_create_date and create_time:
            if create_time.date() > self.min_create_date:
                return False, f"建立時間太新（{create_time.date()}），不符合刪除條件"

        # 檢查最後存取時間
        if self.max_last_access_age_days and last_access_time:
            age_days = (datetime.now() - last_access_time).days
            if age_days < self.max_last_access_age_days:
                return False, f"最近有存取（{age_days} 天前），不符合刪除條件"

        # 如果設定了條件但無法取得時間資訊
        if (self.min_create_date and not create_time) or \
           (self.max_last_access_age_days and not last_access_time):
            return True, "缺少時間資訊，請人工確認"

        # 符合所有條件
        return True, "符合刪除條件"

    def to_dict(self) -> dict:
        """
        將配置轉換為字典格式

        回傳
        -------
        dict
            包含所有配置參數的字典
        """
        return {
            'dry_run': self.dry_run,
            'whitelist_patterns': self.whitelist_patterns,
            'blacklist_patterns': self.blacklist_patterns,
            'min_create_date': self.min_create_date.isoformat() if self.min_create_date else None,
            'max_last_access_age_days': self.max_last_access_age_days,
            'require_confirmation': self.require_confirmation,
            'estimate_storage_size': self.estimate_storage_size
        }

    @classmethod
    def from_dict(cls, config_dict: dict) -> 'CleanupConfig':
        """
        從字典建立配置實例

        參數
        ----------
        config_dict: dict
            包含配置參數的字典

        回傳
        -------
        CleanupConfig
            配置實例
        """
        # 處理日期格式
        min_create_date = None
        if config_dict.get('min_create_date'):
            min_create_date = date.fromisoformat(config_dict['min_create_date'])

        return cls(
            dry_run=config_dict.get('dry_run', True),
            whitelist_patterns=config_dict.get('whitelist_patterns', []),
            blacklist_patterns=config_dict.get('blacklist_patterns', []),
            min_create_date=min_create_date,
            max_last_access_age_days=config_dict.get('max_last_access_age_days'),
            require_confirmation=config_dict.get('require_confirmation', False),
            estimate_storage_size=config_dict.get('estimate_storage_size', False)
        )


# 預設配置範例
DEFAULT_CONFIG = CleanupConfig(
    dry_run=True,
    require_confirmation=True
)

# 生產環境安全配置範例
PRODUCTION_SAFE_CONFIG = CleanupConfig(
    dry_run=True,
    whitelist_patterns=['prod.*', 'production.*', 'critical_*'],
    blacklist_patterns=['system.*', 'metadata.*'],
    max_last_access_age_days=180,  # 只刪除 180 天未存取的表
    require_confirmation=True,
    estimate_storage_size=True
)

# 測試環境配置範例
TEST_CONFIG = CleanupConfig(
    dry_run=False,
    whitelist_patterns=['important_*'],
    max_last_access_age_days=30,  # 測試環境：30 天未存取即可刪除
    require_confirmation=False  # 測試環境可以不需要確認
)
