import pymongo as pymongo
from pandas import DataFrame
from pyspark.sql.types import StructType
import logging
from util.utils import get_decrypted_password


class MongoDbReader:
    def read(spark, db_name, tbl_name, db_conf):
        password = get_decrypted_password('MongoDB', db_conf)
        from urllib.parse import quote_plus
        username = quote_plus(db_conf['DB_USER'])
        password = quote_plus(password)
        client = pymongo.MongoClient(
            "mongodb+srv://" + username + ":" + password + "@ihubcluster.rxkoa.mongodb.net/?retryWrites=true&w=majority")
        db = client[db_name]
        collections = db.list_collection_names()
        for collection in collections:
            if collection == tbl_name:
                try:
                    df = DataFrame(list(db[tbl_name].find({})))
                    df_spark = spark.createDataFrame(df)
                    return df_spark
                except IndexError as e:
                    logging.info(f'There is no data in table so not able to infer the schema')
                    columns = StructType([])
                    return spark.createDataFrame(data=[], schema=columns)
