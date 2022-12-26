import os
import argparse
import configparser
from util.utils import getCommonJars
from mapper.fuzzyMatch import mapColumns
from reader.MySqlReader import MySqlReader
from writer.MySqlWriter import MySqlWriter
from util.sparkUtils import getSparkSession
from reader.MongoDbReader import MongoDbReader
from writer.MongoDbWriter import MongoDbWriter

def getDF(spark, DBConnector, databaseName, tableName, config):
    if DBConnector == "MySql":
        return MySqlReader.read(spark, databaseName, tableName, config[DBConnector])
    elif DBConnector == "MongoDB":
        return MongoDbReader.read(spark, databaseName, tableName, config[DBConnector])
    else:
        print("Does not find reader!! Please create reader for this connector!")

def writeDF(spark, DBConnector, databaseName, tableName, config, df, id_column):
    if DBConnector == "MySql":
        return MySqlWriter.write(spark, databaseName, tableName, config[DBConnector], df)
    elif DBConnector == "MongoDB":
        return MongoDbWriter.write(spark, databaseName, tableName, config[DBConnector], df, id_column)
    else:
        print("Does not find writer!! Please create writer for this connector!")

# MongoDB, MySql
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", help="some useful description.")
    parser.add_argument("--target", help="some useful description.")
    parser.add_argument("--source_db", help="some useful description.")
    parser.add_argument("--target_db", help="some useful description.")
    parser.add_argument("--source_table", help="some useful description.")
    parser.add_argument("--target_table", help="some useful description.")
    parser.add_argument("--id_column", help="some useful description.")
    args = parser.parse_args()

    source = args.source
    target = args.target
    source_db = args.source_db
    target_db = args.target_db
    source_table = args.source_table
    target_table = args.target_table
    id_column = args.id_column
    print(source, " ", source_db, " ", source_table, " ", target, " ", target_db, " ", target_table)

    parent_path = os.path.abspath('')
    print("path:", parent_path)

    file = parent_path + '\config\config.ini'
    config = configparser.ConfigParser()
    config.read(file)

    jars_string = getCommonJars(parent_path, source, target, config)
    spark = getSparkSession(jars_string)

    sourceDF = getDF(spark, source, source_db, source_table, config)

    # For testing only writes to terget
    # MongoDbWriter.write(spark, target_db, target_table, config[target], sourceDF, id_column)

    targetDF = getDF(spark, target, target_db, target_table, config)

    if targetDF.schema:
        df = mapColumns(spark, sourceDF, targetDF)
        writeDF(spark, target, target_db, target_table, config, df, id_column)
    else:
        print("Target table doesn't have schema!! Please try with different table or create new table!")