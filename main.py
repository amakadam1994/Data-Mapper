import os
import argparse
import logging
import configparser
from util.utils import get_common_jars, send_email
from mapper.fuzzyMatch import map_columns, convert_sourcedf_to_targetdf
from reader.MySqlReader import MySqlReader
from writer.MySqlWriter import MySqlWriter
from util.sparkUtils import get_spark_session, convert_data_type
from reader.MongoDbReader import MongoDbReader
from writer.MongoDbWriter import MongoDbWriter


def get_df(spark, DBConnector, databaseName, tableName, config):
    if DBConnector == "MySql":
        return MySqlReader.read(spark, databaseName, tableName, config[DBConnector])
    elif DBConnector == "MongoDB":
        return MongoDbReader.read(spark, databaseName, tableName, config[DBConnector])
    else:
        logging.info(f'Does not find reader!! Please create reader for this connector!')

def write_df(spark, DBConnector, databaseName, tableName, config, df, id_column):
    if DBConnector == "MySql":
        return MySqlWriter.write(spark, databaseName, tableName, config[DBConnector], df)
    elif DBConnector == "MongoDB":
        return MongoDbWriter.write(spark, databaseName, tableName, config[DBConnector], df, id_column)
    else:
        logging.info(f'Does not find writer!! Please create writer for this connector!')

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
    parser.add_argument("--column_percentage", help="some useful description.")
    parser.add_argument("--jobtype", help="manual or auto.")



    logging.getLogger().setLevel(logging.INFO)

    args = parser.parse_args()

    source = args.source
    target = args.target
    source_db = args.source_db
    target_db = args.target_db
    source_table = args.source_table.split(',')
    target_table = args.target_table.split(',')
    id_column = args.id_column
    column_percentage = args.column_percentage
    job_type = args.jobtype

    logging.info(f' {source} {source_db}  {source_table} {target}  {target_db} {target_table} {column_percentage} {job_type}')

    parent_path = os.path.abspath('')
    file = parent_path + '\config\config.ini'
    config = configparser.ConfigParser()
    config.read(file)
    env = config['Common']['env']
    send_email_flag = config['Common']['send_email']
    load_data = config['Common']['load_data']

    jars_string = get_common_jars(parent_path, source, target, config)
    spark = get_spark_session(jars_string)

    for i in range(len(source_table)):
        source_df = get_df(spark, source, source_db, source_table[i], config)
        target_df = get_df(spark, target, target_db, target_table[i], config)

        if target_df.schema:
            source_columns, final, final_map = map_columns(spark, source_df, target_df)

            if send_email_flag.lower() == 'true':
                send_email(source_columns, final, source_db, source_table[i], target_db, target_table[i], env, config['EMAIL'])

            if load_data.lower() == 'true':
                map_df = convert_sourcedf_to_targetdf(source_df, column_percentage, job_type, final, final_map)

                if "Not Identified" in map_df.columns:
                    map_df = map_df.drop("Not Identified")
                dtype_df = convert_data_type(map_df, target_df)
                write_df(spark, target, target_db, target_table[i], config, map_df, id_column)
        else:
            logging.info(f'Target table does not have schema!! Please try with different table or create new table!')
