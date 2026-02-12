"""
Databricks External Tables Cleaner - 孤兒目錄掃描模組

本模組提供從 Storage 角度反向檢查 Unity Catalog 的功能，用於偵測未被引用的「孤兒目錄」。

主要功能：
- 從 Unity Catalog 收集所有已引用的路徑（external tables、volumes、external locations）
- 掃描指定 Storage 區域的實際目錄結構
- 比對並找出未被 Unity Catalog 引用的目錄
- 產生孤兒目錄報表（Dry-run 模式，不執行刪除）

注意：此模組僅用於偵測和報告，不提供任何刪除功能。
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.dbutils import DBUtils
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
from typing import List, Dict, Set, Optional, Tuple
from datetime import datetime
from urllib.parse import urlparse
import re


class OrphanPathsScanner:
    """
    孤兒目錄掃描器

    此類別負責從 Storage 角度反向檢查 Unity Catalog，找出未被引用的目錄。

    屬性
    -------
    spark: SparkSession
        Spark session 實例
    log: logs
        日誌管理實例
    max_depth: int
        掃描目錄的最大深度，預設為 2
    """

    def __init__(self, spark: SparkSession, log, max_depth: int = 2):
        """
        初始化孤兒目錄掃描器

        參數
        ----------
        spark: SparkSession
            Spark session 實例
        log: logs
            日誌管理實例
        max_depth: int
            掃描目錄的最大深度，預設為 2
        """
        self.spark = spark
        self.log = log
        self.max_depth = max_depth
        self.dbutils = DBUtils(spark)

    def normalize_path(self, path: str) -> str:
        """
        將路徑正規化為統一格式

        支援多種路徑格式：
        - abfss://container@account.dfs.core.windows.net/path/to/data
        - s3://bucket/path/to/data
        - gs://bucket/path/to/data
        - dbfs:/path/to/data

        參數
        ----------
        path: str
            原始路徑

        回傳
        -------
        str
            正規化後的路徑（去除尾部斜線，統一格式）

        範例
        -------
        >>> scanner.normalize_path('abfss://data@myacct.dfs.core.windows.net/raw/')
        'abfss://data@myacct.dfs.core.windows.net/raw'
        """
        if not path:
            return ""

        # 去除尾部斜線
        normalized = path.rstrip('/')

        # 統一將 wasbs 轉換為 abfss (Azure storage)
        normalized = normalized.replace('wasbs://', 'abfss://')
        normalized = normalized.replace('wasb://', 'abfss://')

        return normalized

    def get_external_table_locations(self, catalogs: Optional[List[str]] = None) -> Set[str]:
        """
        取得所有 external tables 的儲存位置

        透過 INFORMATION_SCHEMA 或系統目錄查詢所有 external tables 的 LOCATION。

        參數
        ----------
        catalogs: Optional[List[str]]
            要掃描的 catalog 清單，如果為 None 則掃描所有可存取的 catalogs

        回傳
        -------
        Set[str]
            所有 external tables 的正規化路徑集合

        範例
        -------
        >>> locations = scanner.get_external_table_locations(['main'])
        >>> print(locations)
        {'abfss://data@myacct.dfs.core.windows.net/tables/orders', ...}
        """
        self.log.trace('開始收集 external tables 的路徑...')
        locations = set()

        try:
            # 如果未指定 catalogs，取得所有可存取的 catalogs
            if catalogs is None:
                catalogs_df = self.spark.sql("SHOW CATALOGS")
                catalogs = [row.catalog for row in catalogs_df.collect()]
                self.log.trace(f'找到 {len(catalogs)} 個 catalogs')

            # 遍歷每個 catalog
            for catalog in catalogs:
                try:
                    self.log.trace(f'掃描 catalog: {catalog}')

                    # 使用 INFORMATION_SCHEMA 查詢所有 external tables
                    # 注意：只處理有 LOCATION 的 external tables
                    query = f"""
                    SELECT
                        table_catalog,
                        table_schema,
                        table_name,
                        table_type
                    FROM {catalog}.information_schema.tables
                    WHERE table_type = 'EXTERNAL'
                    """

                    tables_df = self.spark.sql(query)

                    # 對每個 external table 取得詳細資訊
                    for row in tables_df.collect():
                        table_full_name = f"{row.table_catalog}.{row.table_schema}.{row.table_name}"

                        try:
                            # 使用 DESCRIBE EXTENDED 取得 LOCATION
                            desc_df = self.spark.sql(f"DESCRIBE EXTENDED {table_full_name}")
                            location_rows = desc_df.filter("col_name = 'Location'").collect()

                            if location_rows:
                                location = location_rows[0].data_type
                                if location and location != 'null':
                                    normalized_location = self.normalize_path(location)
                                    locations.add(normalized_location)
                                    self.log.trace(f'  找到表 {table_full_name} 的路徑: {normalized_location}')
                        except Exception as e:
                            self.log.trace(f'  ⚠️ 無法取得表 {table_full_name} 的詳細資訊: {str(e)}')
                            continue

                except Exception as e:
                    self.log.trace(f'⚠️ 掃描 catalog {catalog} 時發生錯誤: {str(e)}')
                    continue

            self.log.trace(f'✓ 收集到 {len(locations)} 個 external table 路徑')

        except Exception as e:
            self.log.trace(f'✗ 收集 external tables 時發生錯誤: {str(e)}')

        return locations

    def get_external_volume_locations(self, catalogs: Optional[List[str]] = None) -> Set[str]:
        """
        取得所有 external volumes 的儲存位置

        參數
        ----------
        catalogs: Optional[List[str]]
            要掃描的 catalog 清單

        回傳
        -------
        Set[str]
            所有 external volumes 的正規化路徑集合
        """
        self.log.trace('開始收集 external volumes 的路徑...')
        locations = set()

        try:
            # 如果未指定 catalogs，取得所有可存取的 catalogs
            if catalogs is None:
                catalogs_df = self.spark.sql("SHOW CATALOGS")
                catalogs = [row.catalog for row in catalogs_df.collect()]

            # 遍歷每個 catalog
            for catalog in catalogs:
                try:
                    self.log.trace(f'掃描 catalog: {catalog}')

                    # 查詢所有 schemas
                    schemas_df = self.spark.sql(f"SHOW SCHEMAS IN {catalog}")

                    for schema_row in schemas_df.collect():
                        schema = schema_row.databaseName

                        try:
                            # 查詢每個 schema 中的 volumes
                            volumes_df = self.spark.sql(f"SHOW VOLUMES IN {catalog}.{schema}")

                            for volume_row in volumes_df.collect():
                                volume_name = volume_row.volume_name
                                volume_type = volume_row.volume_type if hasattr(volume_row, 'volume_type') else 'UNKNOWN'

                                # 只處理 EXTERNAL volumes
                                if volume_type.upper() == 'EXTERNAL':
                                    volume_full_name = f"{catalog}.{schema}.{volume_name}"

                                    try:
                                        # 使用 DESCRIBE VOLUME 取得 storage_location
                                        desc_df = self.spark.sql(f"DESCRIBE VOLUME {volume_full_name}")
                                        location_rows = desc_df.filter("info_name = 'StorageLocation'").collect()

                                        if location_rows:
                                            location = location_rows[0].info_value
                                            if location and location != 'null':
                                                normalized_location = self.normalize_path(location)
                                                locations.add(normalized_location)
                                                self.log.trace(f'  找到 volume {volume_full_name} 的路徑: {normalized_location}')
                                    except Exception as e:
                                        self.log.trace(f'  ⚠️ 無法取得 volume {volume_full_name} 的詳細資訊: {str(e)}')
                                        continue

                        except Exception as e:
                            # SHOW VOLUMES 可能在某些環境不支援，靜默跳過
                            continue

                except Exception as e:
                    self.log.trace(f'⚠️ 掃描 catalog {catalog} 的 volumes 時發生錯誤: {str(e)}')
                    continue

            self.log.trace(f'✓ 收集到 {len(locations)} 個 external volume 路徑')

        except Exception as e:
            self.log.trace(f'⚠️ 收集 external volumes 時發生錯誤: {str(e)}')

        return locations

    def get_external_location_paths(self) -> Set[str]:
        """
        取得所有 external locations 的路徑

        回傳
        -------
        Set[str]
            所有 external locations 的正規化路徑集合
        """
        self.log.trace('開始收集 external locations 的路徑...')
        locations = set()

        try:
            # 使用 SHOW EXTERNAL LOCATIONS 查詢
            locations_df = self.spark.sql("SHOW EXTERNAL LOCATIONS")

            for row in locations_df.collect():
                location_name = row.name
                url = row.url

                if url:
                    normalized_url = self.normalize_path(url)
                    locations.add(normalized_url)
                    self.log.trace(f'  找到 external location {location_name}: {normalized_url}')

            self.log.trace(f'✓ 收集到 {len(locations)} 個 external location 路徑')

        except Exception as e:
            self.log.trace(f'⚠️ 收集 external locations 時發生錯誤: {str(e)}')
            self.log.trace('  (可能因權限不足或環境不支援 Unity Catalog)')

        return locations

    def collect_all_uc_references(self, catalogs: Optional[List[str]] = None) -> Set[str]:
        """
        收集所有 Unity Catalog 引用的路徑

        此函式會整合：
        1. External tables 的 LOCATION
        2. External volumes 的 storage_location
        3. External locations 的 url

        參數
        ----------
        catalogs: Optional[List[str]]
            要掃描的 catalog 清單

        回傳
        -------
        Set[str]
            所有被 UC 引用的正規化路徑集合

        範例
        -------
        >>> all_paths = scanner.collect_all_uc_references(['main', 'dev'])
        >>> print(f'共找到 {len(all_paths)} 個被 UC 引用的路徑')
        """
        self.log.trace('=' * 80)
        self.log.trace('開始收集所有 Unity Catalog 引用的路徑')
        self.log.trace('=' * 80)

        all_paths = set()

        # 收集 external tables
        table_paths = self.get_external_table_locations(catalogs)
        all_paths.update(table_paths)

        # 收集 external volumes
        volume_paths = self.get_external_volume_locations(catalogs)
        all_paths.update(volume_paths)

        # 收集 external locations
        location_paths = self.get_external_location_paths()
        all_paths.update(location_paths)

        self.log.trace('')
        self.log.trace('=' * 80)
        self.log.trace('Unity Catalog 路徑收集摘要')
        self.log.trace('=' * 80)
        self.log.trace(f'External Tables: {len(table_paths)} 個路徑')
        self.log.trace(f'External Volumes: {len(volume_paths)} 個路徑')
        self.log.trace(f'External Locations: {len(location_paths)} 個路徑')
        self.log.trace(f'總計（去重後）: {len(all_paths)} 個路徑')
        self.log.trace('=' * 80)

        return all_paths

    def scan_storage_directories(
        self,
        base_path: str,
        current_depth: int = 0
    ) -> List[Dict[str, any]]:
        """
        遞迴掃描儲存路徑的目錄結構

        參數
        ----------
        base_path: str
            要掃描的基礎路徑
        current_depth: int
            當前遞迴深度

        回傳
        -------
        List[Dict[str, any]]
            目錄資訊清單，每個元素包含：
            - path: 目錄路徑
            - size: 估算大小（bytes）
            - modified_time: 最後修改時間

        範例
        -------
        >>> dirs = scanner.scan_storage_directories(
        ...     'abfss://data@myacct.dfs.core.windows.net/raw/'
        ... )
        """
        directories = []

        # 檢查深度限制
        if current_depth > self.max_depth:
            return directories

        try:
            # 正規化路徑
            normalized_base = self.normalize_path(base_path)

            if current_depth == 0:
                self.log.trace(f'開始掃描路徑: {normalized_base} (最大深度: {self.max_depth})')

            # 列出當前路徑下的內容
            file_infos = self.dbutils.fs.ls(normalized_base)

            for file_info in file_infos:
                # 只處理目錄
                if file_info.isDir():
                    dir_path = self.normalize_path(file_info.path)

                    dir_info = {
                        'path': dir_path,
                        'size': file_info.size,
                        'modified_time': datetime.fromtimestamp(file_info.modificationTime / 1000.0)
                    }

                    directories.append(dir_info)

                    # 遞迴掃描子目錄
                    if current_depth < self.max_depth:
                        subdirs = self.scan_storage_directories(dir_path, current_depth + 1)
                        directories.extend(subdirs)

            if current_depth == 0:
                self.log.trace(f'✓ 掃描完成，找到 {len(directories)} 個目錄')

        except Exception as e:
            self.log.trace(f'⚠️ 掃描路徑 {base_path} 時發生錯誤: {str(e)}')

        return directories

    def is_path_referenced(self, dir_path: str, uc_paths: Set[str]) -> Tuple[bool, Optional[str]]:
        """
        檢查指定路徑是否被 Unity Catalog 引用

        判斷邏輯：
        1. 如果 dir_path 完全匹配任何 UC 路徑 -> 被引用
        2. 如果 dir_path 是任何 UC 路徑的前綴（父目錄）-> 被引用
        3. 如果 dir_path 是任何 UC 路徑的子路徑 -> 被引用
        4. 否則 -> 未被引用（孤兒目錄）

        參數
        ----------
        dir_path: str
            要檢查的目錄路徑
        uc_paths: Set[str]
            所有 UC 引用的路徑集合

        回傳
        -------
        Tuple[bool, Optional[str]]
            (是否被引用, 匹配的 UC 路徑)

        範例
        -------
        >>> is_ref, matched = scanner.is_path_referenced(
        ...     'abfss://data@myacct.dfs.core.windows.net/raw/temp',
        ...     uc_paths
        ... )
        >>> if not is_ref:
        ...     print(f'孤兒目錄: {dir_path}')
        """
        normalized_dir = self.normalize_path(dir_path)

        # 檢查完全匹配
        if normalized_dir in uc_paths:
            return True, normalized_dir

        # 檢查是否為 UC 路徑的父目錄或子目錄
        for uc_path in uc_paths:
            # 檢查 dir_path 是否為 uc_path 的父目錄
            # 例如: dir_path = /data/raw, uc_path = /data/raw/tables
            if uc_path.startswith(normalized_dir + '/'):
                return True, uc_path

            # 檢查 dir_path 是否為 uc_path 的子目錄
            # 例如: dir_path = /data/raw/temp, uc_path = /data/raw
            if normalized_dir.startswith(uc_path + '/'):
                return True, uc_path

        # 未找到任何匹配
        return False, None

    def find_orphan_directories(
        self,
        base_paths: List[str],
        catalogs: Optional[List[str]] = None
    ) -> List[Dict[str, any]]:
        """
        找出所有孤兒目錄

        這是主要的掃描函式，整合了所有步驟：
        1. 收集 UC 引用的路徑
        2. 掃描儲存目錄
        3. 比對並找出孤兒目錄

        參數
        ----------
        base_paths: List[str]
            要掃描的基礎路徑清單
        catalogs: Optional[List[str]]
            要掃描的 catalog 清單

        回傳
        -------
        List[Dict[str, any]]
            孤兒目錄清單，每個元素包含：
            - path: 目錄路徑
            - size: 估算大小
            - modified_time: 最後修改時間
            - reason: 判定為孤兒目錄的原因

        範例
        -------
        >>> orphans = scanner.find_orphan_directories(
        ...     base_paths=['abfss://data@myacct.dfs.core.windows.net/raw/'],
        ...     catalogs=['main']
        ... )
        >>> print(f'找到 {len(orphans)} 個孤兒目錄')
        """
        self.log.trace('')
        self.log.trace('=' * 80)
        self.log.trace('開始孤兒目錄掃描')
        self.log.trace('=' * 80)

        # 步驟 1: 收集所有 UC 引用的路徑
        uc_paths = self.collect_all_uc_references(catalogs)

        if not uc_paths:
            self.log.trace('⚠️ 警告：未找到任何 UC 引用的路徑，請檢查權限設定')

        # 步驟 2: 掃描所有指定的 storage 路徑
        self.log.trace('')
        self.log.trace('=' * 80)
        self.log.trace('開始掃描 Storage 目錄')
        self.log.trace('=' * 80)

        all_directories = []
        for base_path in base_paths:
            self.log.trace(f'掃描基礎路徑: {base_path}')
            dirs = self.scan_storage_directories(base_path)
            all_directories.extend(dirs)

        self.log.trace(f'✓ 共掃描到 {len(all_directories)} 個目錄')

        # 步驟 3: 比對並找出孤兒目錄
        self.log.trace('')
        self.log.trace('=' * 80)
        self.log.trace('開始比對並找出孤兒目錄')
        self.log.trace('=' * 80)

        orphan_directories = []

        for dir_info in all_directories:
            dir_path = dir_info['path']
            is_referenced, matched_path = self.is_path_referenced(dir_path, uc_paths)

            if not is_referenced:
                # 這是孤兒目錄
                orphan_info = dir_info.copy()
                orphan_info['reason'] = 'not referenced by any UC table/volume/external_location'
                orphan_directories.append(orphan_info)
                self.log.trace(f'  [孤兒] {dir_path}')
            else:
                self.log.trace(f'  [引用] {dir_path} <- 被 {matched_path} 引用')

        # 輸出統計摘要
        self.log.trace('')
        self.log.trace('=' * 80)
        self.log.trace('孤兒目錄掃描結果摘要')
        self.log.trace('=' * 80)
        self.log.trace(f'掃描的基礎路徑數量: {len(base_paths)}')
        self.log.trace(f'找到的總目錄數量: {len(all_directories)}')
        self.log.trace(f'被 UC 引用的目錄: {len(all_directories) - len(orphan_directories)}')
        self.log.trace(f'孤兒目錄數量: {len(orphan_directories)}')

        if orphan_directories:
            total_size = sum(d.get('size', 0) for d in orphan_directories)
            self.log.trace(f'孤兒目錄總大小: {self._format_size(total_size)}')

        self.log.trace('=' * 80)

        return orphan_directories

    def _format_size(self, size_bytes: int) -> str:
        """
        格式化檔案大小顯示

        參數
        ----------
        size_bytes: int
            大小（bytes）

        回傳
        -------
        str
            格式化後的大小字串
        """
        if size_bytes < 1024:
            return f"{size_bytes} B"
        elif size_bytes < 1024 * 1024:
            return f"{size_bytes / 1024:.2f} KB"
        elif size_bytes < 1024 * 1024 * 1024:
            return f"{size_bytes / (1024 * 1024):.2f} MB"
        else:
            return f"{size_bytes / (1024 * 1024 * 1024):.2f} GB"

    def generate_report_dataframe(self, orphan_directories: List[Dict[str, any]]) -> DataFrame:
        """
        將孤兒目錄清單轉換為 Spark DataFrame

        參數
        ----------
        orphan_directories: List[Dict[str, any]]
            孤兒目錄清單

        回傳
        -------
        DataFrame
            包含孤兒目錄資訊的 DataFrame
        """
        if not orphan_directories:
            # 回傳空的 DataFrame
            schema = StructType([
                StructField("path", StringType(), True),
                StructField("size_bytes", LongType(), True),
                StructField("size_formatted", StringType(), True),
                StructField("last_modified", TimestampType(), True),
                StructField("reason", StringType(), True)
            ])
            return self.spark.createDataFrame([], schema)

        # 準備資料
        data = []
        for orphan in orphan_directories:
            data.append((
                orphan['path'],
                orphan.get('size', 0),
                self._format_size(orphan.get('size', 0)),
                orphan.get('modified_time'),
                orphan['reason']
            ))

        # 建立 DataFrame
        df = self.spark.createDataFrame(data, [
            "path",
            "size_bytes",
            "size_formatted",
            "last_modified",
            "reason"
        ])

        return df


def scan_orphan_paths(
    spark: SparkSession,
    log,
    base_paths: List[str],
    catalogs: Optional[List[str]] = None,
    max_depth: int = 2
) -> Tuple[List[Dict[str, any]], DataFrame]:
    """
    掃描孤兒目錄的便利函式

    這是一個高階 API，簡化了孤兒目錄掃描的使用。

    參數
    ----------
    spark: SparkSession
        Spark session 實例
    log: logs
        日誌管理實例
    base_paths: List[str]
        要掃描的基礎路徑清單
    catalogs: Optional[List[str]]
        要掃描的 catalog 清單
    max_depth: int
        掃描目錄的最大深度

    回傳
    -------
    Tuple[List[Dict[str, any]], DataFrame]
        (孤兒目錄清單, 孤兒目錄 DataFrame)

    範例
    -------
    >>> from common.helpers import logs
    >>> from common.orphan_paths_scanner import scan_orphan_paths
    >>>
    >>> logger = logs(name='OrphanScanner', debug=True)
    >>> orphans, df = scan_orphan_paths(
    ...     spark=spark,
    ...     log=logger,
    ...     base_paths=['abfss://data@myacct.dfs.core.windows.net/raw/'],
    ...     catalogs=['main'],
    ...     max_depth=2
    ... )
    >>>
    >>> # 顯示結果
    >>> df.show(truncate=False)
    >>> print(f'找到 {len(orphans)} 個孤兒目錄')
    """
    scanner = OrphanPathsScanner(spark, log, max_depth)
    orphan_directories = scanner.find_orphan_directories(base_paths, catalogs)
    report_df = scanner.generate_report_dataframe(orphan_directories)

    return orphan_directories, report_df
