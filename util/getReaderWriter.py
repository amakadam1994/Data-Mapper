import logging

from reader.MongoDbReader import MongoDbReader
from writer.MongoDbWriter import MongoDbWriter
from reader.MySqlReader import MySqlReader
from writer.MySqlWriter import MySqlWriter

def read_df(spark, DBConnector, databaseName, tableName, config):
    if DBConnector == "MYSQL":
        return MySqlReader.read(spark, databaseName, tableName, config[DBConnector], config['COMMON'])
    elif DBConnector == "MONGODB":
        return MongoDbReader.read(spark, databaseName, tableName, config[DBConnector], config['COMMON'])
    else:
        logging.info(f'Does not find reader!! Please create reader for this connector!')

def write_df(spark, DBConnector, databaseName, tableName, config, df, id_column):
    if DBConnector == "MYSQL":
        return MySqlWriter.write(spark, databaseName, tableName, config[DBConnector], df, config['COMMON'])
    elif DBConnector == "MONGODB":
        return MongoDbWriter.write(spark, databaseName, tableName, config[DBConnector], df, id_column, config['COMMON'])
    else:
        logging.info(f'Does not find writer!! Please create writer for this connector!')