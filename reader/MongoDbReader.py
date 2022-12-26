import pymongo as pymongo
from pandas import DataFrame
from pyspark.sql.types import StructType

class MongoDbReader:
    def read(spark, DbName, TblName, db_conf):
        from urllib.parse import quote_plus
        username = quote_plus(db_conf['DB_USER'])
        password = quote_plus(db_conf['DB_PASS'])
        client = pymongo.MongoClient(
            "mongodb+srv://" + username + ":" + password + "@ihubcluster.rxkoa.mongodb.net/?retryWrites=true&w=majority")
        db = client[DbName]
        collections = db.list_collection_names()
        for collection in collections:
            if collection == TblName:
                try:
                    df = DataFrame(list(db[TblName].find({})))
                    df_spark = spark.createDataFrame(df)
                    return df_spark
                except IndexError as e:
                    print("There is no data in table so not able to infer the schema")
                    columns = StructType([])
                    return spark.createDataFrame(data=[], schema=columns)