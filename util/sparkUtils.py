import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, BinaryType, BooleanType, DateType, \
    MapType, NullType, NumericType, StringType, StructType, TimestampType, IntegerType


def get_spark_session(jars):
    if jars is None:
        return SparkSession \
            .master("local") \
            .appName("DataMapper") \
            .getOrCreate()
    else:
        return SparkSession \
            .builder.config("spark.jars", jars) \
            .master("local") \
            .appName("DataMapper") \
            .getOrCreate()


def get_df_columns(spark, df):
    schema = [i for i in df.schema]
    return schema


def get_df_columns_list(schema):
    column_list = []
    for field in schema:
        column_list.append(field.name)
    return column_list


def get_datatype_converted_column(df, scolumn, sdata_type, tdata_type, new_column):
    supported_pairs = [
        ["IntegerType", "StringType"], ["StringType", "DateType"], ["StringType", "BooleanType"]
    ]
    flag = False
    for pair in supported_pairs:
        if pair[0] == str(sdata_type) and pair[1] == str(tdata_type):
            flag = True
        else:
            pass
    if flag:
        try:
            if str(tdata_type) == "StringType":
                return df.withColumn(new_column, df[scolumn].cast(StringType()))
            elif str(tdata_type) == "DateType":
                return df.withColumn(new_column, df[scolumn].cast(DateType()))
            elif str(tdata_type) == "BooleanType":
                return df.withColumn(new_column, df[scolumn].cast(BooleanType()))
            elif str(tdata_type) == "IntegerType":
                return df.withColumn(new_column, df[scolumn].cast(IntegerType()))
            else:
                logging.warning(f'This datatype is not supported please add this data type and try again')
                return df.withColumn(new_column, df[scolumn])
        except Exception as e:
            logging.error(f'Data type can not be casted. going ahead with existing data type')

            return df.withColumn(new_column, df[scolumn])
    else:
        return df.withColumn(new_column, df[scolumn])


def convert_data_type(source_df, target_df):
    source_schema = [i for i in source_df.schema]
    target_schema = [i for i in target_df.schema]

    for sfield in source_schema:
        for tfield in target_schema:
            if sfield.name == tfield.name:
                if sfield.dataType == tfield.dataType:
                    pass
                else:
                    logging.info(f'Found different datatype in target. Converting source datatype to target datatype. '
                                 f'Source:{sfield.dataType} Target: {tfield.dataType}  Column Name: {sfield.name}')

                    new_column = "new_column"
                    try:
                        source_df1 = get_datatype_converted_column(source_df, sfield.name, sfield.dataType,
                                                                   tfield.dataType, new_column)
                        source_df2 = source_df1.drop(sfield.name).withColumnRenamed(new_column, sfield.name)
                        source_df = source_df2
                    except Exception as e:
                        logging.warning(f'Exception while converting data type:{e}')
    return source_df

def change_df_column_name(Final, source_df):
    df = source_df.rdd.toDF(Final)
    return df
