import pytest
from mapper.fuzzyMatch import  map_columns, map_df_columns
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("local-tests")
        .config("spark.executor.cores", "1")
        .config("spark.executor.instances", "1")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
    yield spark
    spark.stop()

@pytest.mark.parametrize('source_columns, target_columns, source_columns_result, final_result, final_map_result',
                         [
                             ( ['a','b'], ['aa','bb'], ['a','b'], ['aa','bb'], {'aa': 67, 'bb': 67})
                         ]
)
def test_map_columns(source_columns, target_columns, source_columns_result, final_result, final_map_result):
    source_columns, final, final_map = map_columns(source_columns, target_columns)
    assert source_columns == source_columns_result
    assert final == final_result
    assert final_map == final_map_result


def test_map_df_columns(spark):
    source_data = [("James", "kadam"),
             ("Michael", "Rose")
             ]
    source_schema = StructType([ \
        StructField("firstname", StringType(), True), \
        StructField("lastname", StringType(), True), \
        ])
    source_df = spark.createDataFrame(data=source_data, schema=source_schema)

    target_schema = StructType([ \
        StructField("first_name", StringType(), True), \
        StructField("last_name", StringType(), True), \
        ])
    target_df = spark.createDataFrame(data=source_data, schema=target_schema)

    source_columns, final, final_map = map_df_columns(spark, source_df, target_df)
    assert source_columns == ['firstname','lastname']
    assert final == ['first_name','last_name']
    assert final_map == {'first_name': 95, 'last_name': 94}