from util.sparkUtils import check_supported_pairs, get_spark_session, get_df_columns, get_df_columns_list, \
    convert_data_type, get_datatype_converted_column, change_df_column_name, convert_sourcedf_to_targetdf
import pytest
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType


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




@pytest.mark.parametrize('jars, env',
                         [ ("C:\\Users\\Amar Kadam\\Desktop\\DE_Capability\\Data-Mapper\\jar_files\\mysql-connector-java-8.0.22.jar", "local"),
                           (None, "local")
                         ])
def test_get_spark_session(jars, env):
    spark = get_spark_session(jars, env)
    assert spark.sparkContext.appName == "DataMapper"


@pytest.mark.parametrize('sdata_type, tdata_type, result_flag',
                         [
                             ("IntegerType", "StringType", True),
                             ("StringType", "BooleanType", True),
                             ("StringType", "long", False),
                             ("StringType", "DateType", True)
                         ])
def test_check_supported_pairs(sdata_type, tdata_type, result_flag):
    flag = check_supported_pairs(sdata_type, tdata_type)
    assert flag == result_flag


def test_get_df_columns(spark):
    source_data = [("James", "kadam"),
                   ("Michael", "Rose")
                   ]
    source_schema = StructType([ \
        StructField("firstname", StringType(), True), \
        StructField("lastname", StringType(), True), \
        ])
    source_df = spark.createDataFrame(data=source_data, schema=source_schema)
    schema = get_df_columns(spark, source_df)
    assert  schema == [i for i in source_df.schema]


def test_get_df_columns_list(spark):
    source_data = [("James", "kadam"),
                   ("Michael", "Rose")
                   ]
    source_schema = StructType([ \
        StructField("firstname", StringType(), True), \
        StructField("lastname", StringType(), True), \
        ])
    source_df = spark.createDataFrame(data=source_data, schema=source_schema)
    c_list = get_df_columns_list(source_df.schema)
    assert c_list == ["firstname","lastname"]


def test_get_datatype_converted_column(spark):
    source_data = [("James", 59),
                   ("Michael", 80)
                   ]
    source_schema = StructType([ \
        StructField("firstname", StringType(), True), \
        StructField("marks", IntegerType(), True), \
        ])
    source_df = spark.createDataFrame(data=source_data, schema=source_schema)

    target_df = source_df.withColumn("new_column", source_df["marks"].cast(StringType()))
    df = get_datatype_converted_column(source_df, "marks", "IntegerType", "StringType", "new_column")
    assert df.schema == target_df.schema


def test_convert_data_type(spark):
    source_data = [("James", 59),
                   ("Michael", 80)
                   ]
    source_schema = StructType([ \
        StructField("firstname", StringType(), True), \
        StructField("marks", IntegerType(), True), \
        ])
    source_df = spark.createDataFrame(data=source_data, schema=source_schema)

    target_schema = StructType([ \
        StructField("firstname", StringType(), True), \
        StructField("marks", StringType(), True), \
        ])
    target_df = spark.createDataFrame(data=source_data, schema=target_schema)

    df = convert_data_type(source_df, target_df)
    assert df.schema == target_df.schema

def test_change_df_column_name(spark):
    source_data = [("James", 59),
                       ("Michael", 80)
                       ]
    source_schema = StructType([ \
            StructField("firstname", StringType(), True), \
            StructField("marks", IntegerType(), True), \
            ])
    source_df = spark.createDataFrame(data=source_data, schema=source_schema)
    Final =["first_name","mark"]
    df = change_df_column_name(Final, source_df)

    expected_schema = StructType([ \
        StructField("first_name", StringType(), True), \
        StructField("mark", LongType(), True), \
        ])
    expected_df = spark.createDataFrame(data=source_data, schema=expected_schema)

    assert  df.schema == expected_df.schema



@pytest.mark.parametrize('column_percentage, job_type, final, final_map',
                         [
                             (90, "auto",["first_name", "mark"],{"firstname": 95, "marks": 96})
                         ])
def test_convert_sourcedf_to_targetdf(spark, column_percentage, job_type, final, final_map):
    source_data = [("James", 59),
                   ("Michael", 80)
                   ]
    source_schema = StructType([ \
        StructField("firstname", StringType(), True), \
        StructField("marks", IntegerType(), True), \
        ])
    source_df = spark.createDataFrame(data=source_data, schema=source_schema)
    df = convert_sourcedf_to_targetdf(source_df, column_percentage, job_type, final, final_map)

    expected_schema = StructType([ \
        StructField("first_name", StringType(), True), \
        StructField("mark", LongType(), True), \
        ])
    expected_df = spark.createDataFrame(data=source_data, schema=expected_schema)

    assert df.schema == expected_df.schema