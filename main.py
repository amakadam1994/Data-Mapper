import os
import argparse
import logging
import configparser
from util.getReaderWriter import read_df, write_df
from util.utils import get_common_jars, send_email
from mapper.fuzzyMatch import map_df_columns
from util.sparkUtils import get_spark_session, convert_data_type, convert_sourcedf_to_targetdf


# MONGODB, MYSQL
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--param_file", help="some useful description.")
    logging.getLogger().setLevel(logging.INFO)

    args = parser.parse_args()

    parent_path = os.path.abspath('')
    param_file = args.param_file
    param_config = configparser.ConfigParser()
    param_config.read(parent_path+param_file)

    param_config= param_config['PARAMETERS']
    source = param_config["source"].upper()
    target = param_config["target"].upper()
    source_db = param_config["source_db"]
    target_db = param_config["target_db"]
    source_table = param_config["source_table"].split(',')
    target_table = param_config["target_table"].split(',')
    id_column = param_config["id_column"]
    column_percentage = param_config["column_percentage"]
    job_type = param_config["job_type"]
    logging.info(f' {source} {source_db}  {source_table} {target}  {target_db} {target_table} {column_percentage} {job_type}')


    file = parent_path + '\config\config.ini'
    config = configparser.ConfigParser()
    config.read(file)
    env = config['COMMON']['ENV']
    send_email_flag = config['COMMON']['SEND_EMAIL']
    load_data = config['COMMON']['LOAD_DATA']
    read_key_from = config['COMMON']['READ_KEY_FROM']


    jars_string = get_common_jars(parent_path, source, target, config)
    spark = get_spark_session(jars_string, env)

    for i in range(len(source_table)):
        source_df = read_df(spark, source, source_db, source_table[i], config)
        target_df = read_df(spark, target, target_db, target_table[i], config)

        if target_df.schema:
            source_columns, final, final_map = map_df_columns(spark, source_df, target_df)

            if send_email_flag.lower() == 'true':
                send_email(source_columns, final, source_db, source_table[i], target_db, target_table[i], env, config)

            if load_data.lower() == 'true':
                map_df = convert_sourcedf_to_targetdf(source_df, column_percentage, job_type, final, final_map)

                if "Not Identified" in map_df.columns:
                    map_df = map_df.drop("Not Identified")

                dtype_df = convert_data_type(map_df, target_df)

                map_df.show(5)
                write_df(spark, target, target_db, target_table[i], config, map_df, id_column)
        else:
            logging.info(f'Target table does not have schema!! Please try with different table or create new table!')
