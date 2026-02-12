"""
Databricks External Tables Cleaner - 核心功能測試

本測試模組涵蓋：
- file_exists 函式：檔案/目錄存在性檢查
- get_tables 函式：表清單查詢
- get_tables_details 函式：表詳細資訊查詢
- create_empty_dataframe 函式：空 DataFrame 建立
- drop_table_definition_without_storage 函式：清理邏輯

所有測試都需要連接到實際的 Databricks cluster 才能執行。
"""

import pytest
from context import *
from databricks.connect import DatabricksSession
import pandas as pd


@pytest.fixture(scope="session")
def spark() -> DatabricksSession:
    """
    建立 Databricks Spark Session fixture
    
    此 fixture 會在整個測試 session 中只建立一次，
    所有測試函式共用同一個 spark session。
    
    回傳
    -------
    DatabricksSession
        連接到遠端 Databricks cluster 的 Spark session
    
    注意事項
    -------
    需要事先設定 databricks-connect 並連接到有效的 cluster
    """
    # 建立 DatabricksSession（連接到遠端 Databricks workspace 的 cluster）
    # 單元測試預設無法存取這個 session，因此需要透過 fixture 提供
    return DatabricksSession.builder.getOrCreate()


# ============================================================================
# file_exists 函式測試
# ============================================================================

def test_file_exists_should_work_when_path_is_invalid(spark):
    """測試 file_exists 應該在路徑格式無效時回傳 False"""
    # ARRANGE：準備一個格式無效的路徑
    path = '\mnt'  # 缺少前導斜線的無效路徑
    
    # ACT：執行檢查
    output = file_exists(spark, path)
    expected = False

    # ASSERT：驗證結果
    assert (output == expected), f"預期 {expected}，但得到 {output}"


def test_file_should_return_true_when_target_folder_exists(spark):
    """測試 file_exists 應該在目標目錄存在時回傳 True"""
    # ARRANGE：準備一個存在的路徑（/mnt 是 Databricks 中常見的掛載點）
    path = '/mnt'

    # ACT：執行檢查
    output = file_exists(spark, path)
    expected = True

    # ASSERT：驗證結果
    assert (output == expected), f"預期 {expected}，但得到 {output}"


def test_file_should_return_false_when_target_folder_doesnt_exist(spark):
    """測試 file_exists 應該在目標目錄不存在時回傳 False"""
    # ARRANGE：準備一個不存在的路徑
    path = '/mnt/hello'  # 假設這個路徑不存在

    # ACT：執行檢查
    output = file_exists(spark, path)
    expected = False

    # ASSERT：驗證結果
    assert (output == expected), f"預期 {expected}，但得到 {output}"


# ============================================================================
# get_tables 函式測試
# ============================================================================

def test_get_tables_should_return_just_temporary_tables(spark):
    # ARRANGE
    store = 'hive_metastore'
    schema = 'dbunittest_1'
    spark.sql(f'create schema if not exists {store}.{schema};')
    spark.sql('create or replace temporary view mytemp as select 1 as id;')
    
    # ACT
    output = get_tables(spark,store,schema,True)
    expected = ['mytemp']
    
    # ASSERT
    assert(output==expected) 

    # CLEAN
    spark.sql(f'drop schema if exists {store}.{schema} cascade;')


def test_get_tables_should_return_none_temporary_tables(spark):
    # ARRANGE
    store = 'hive_metastore'
    schema = 'dbunittest_2'
    spark.sql(f'create schema if not exists {store}.{schema};')
    spark.sql(f'create view if not exists {store}.{schema}.myview as select 1 as id;')
    spark.sql(f'create table if not exists {store}.{schema}.mytable (id int);')
    
    # ACT
    output = get_tables(spark,store,schema,False)
    expected = ['mytable','myview']

    #ASSERT
    assert(output==expected)

    # CLEAN
    spark.sql(f'drop schema if exists {store}.{schema} cascade;')


def test_get_table_details_should_not_return_type_as_view_and_provider_notin_parquet_delta(spark):
    # ARRANGE
    store = 'hive_metastore'
    schema = 'dbunittest_3'
    tables = ['mytable_1','mytable_2','myview']
    
    spark.sql(f'create schema if not exists {store}.{schema};')
    spark.sql(f'create view if not exists {store}.{schema}.myview as select 1 as id;')
    spark.sql(f'create table if not exists {store}.{schema}.mytable_1 (id int);')
    spark.sql(f'create table if not exists {store}.{schema}.mytable_2 (id int);')

    # ACT
    output_DF = get_tables_details(spark,store,schema,tables)
    output_Without_Views_DF = output_DF.where("lower(Type) = 'view'")
    output_Without_Not_Parquet_Delta_DF = output_DF.where("lower(Provider) not in ('parquet','delta')")
    
    # ASSERT
    assert (output_Without_Views_DF.isEmpty() == True)
    assert (output_Without_Not_Parquet_Delta_DF.isEmpty() == True)

    # CLEAN
    spark.sql(f'drop schema if exists {store}.{schema} cascade;')

 
def test_create_empty_dataframe(spark):
    # ARRANGE
    columns = ['col1','col2','col3']
    dtypes =    ['str','int32','bool']
    types = ['string','integer','boolean']
 
    expected_pdf = pd.DataFrame(columns=columns, index=None, dtype=str)
    for col, dtype in zip(columns,dtypes):
        expected_pdf[col] = expected_pdf[col].astype(dtype)

    # ACT
    output = create_empty_dataframe(spark,columns,types)
    output_pdf = output.toPandas()

    # ASSERT
    assert(output_pdf.equals(expected_pdf))


def test_drop_table_definition_without_storage_return_zero_when_empty_dataframe_as_argument(spark):
    # ARRANGE
    columns = ['Database','Table','Provider','Type','Location']
    types = ['string','string','string','string','string']

    empty_df = create_empty_dataframe(spark,columns,types)
    log = logs(name='unit_test')
    expected = 0

    # ACT
    output = drop_table_definition_without_storage(spark,empty_df,log)

    # ASSERT 
    assert (output == expected)

