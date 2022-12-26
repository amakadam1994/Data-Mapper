from pyspark.sql import SparkSession

def getSparkSession(jars):
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

def getDfColumns(spark, df):
    schema = [i for i in df.schema]
    return schema

def getDfColumnList(schema):
    column_list = []
    for field in schema:
        column_list.append(field.name)
    return column_list
