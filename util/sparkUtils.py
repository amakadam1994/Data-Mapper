from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, BinaryType, BooleanType, DateType, \
    MapType, NullType, NumericType, StringType, StructType, TimestampType, IntegerType


def get_spark_session(jars):
    print("Jars:", jars)
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
    supported_pairs= [
        ["IntegerType","StringType"], ["StringType","DateType"], ["StringType","BooleanType"]
    ]
    flag= False
    for pair in supported_pairs:
        if pair[0] == str(sdata_type) and pair[1] == str(tdata_type):
            flag=True
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
                print("This datatype is not supported please add this data type and try again")
                return df.withColumn(new_column, df[scolumn])
        except Exception as e:
            print("Data type can not be casted. going ahead with existing data type")
            return df.withColumn(new_column, df[scolumn])


def convert_data_type(sourceDF, targetDF):
    source_schema = [i for i in sourceDF.schema]
    target_schema = [i for i in targetDF.schema]

    for sfield in source_schema:
        for tfield in target_schema:
            if sfield.name == tfield.name:
                print(sfield.name, sfield.dataType, tfield.name, tfield.dataType)
                if sfield.dataType == tfield.dataType:
                    pass
                else:
                    print("Found different datatype in target. Converting source datatype to target datatype")
                    new_column = "new_column"
                    try:
                        sourceDF1 = get_datatype_converted_column(sourceDF, sfield.name,sfield.dataType, tfield.dataType, new_column)
                        print("Before Converting data type")
                        sourceDF1.printSchema()
                        sourceDF2 = sourceDF1.drop(sfield.name).withColumnRenamed(new_column, sfield.name)
                        print("After Converting data type")
                        sourceDF2.printSchema()
                        sourceDF2.show()
                    except Exception as e:
                        print("Exception while converting data type:",e)

def change_df_column_name(Final, source_df):
    print("Final and source_df",Final)
    source_df.show()
    # source_df = source_df.select(*([col(i[0]).alias(i[1]) for i in Final] ))
    df1 = source_df.rdd.toDF(Final)
    return df1