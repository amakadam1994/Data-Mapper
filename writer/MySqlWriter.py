import logging
from util.utils import get_decrypted_password


class MySqlWriter:
    def write(spark, db_name, tbl_name, db_conf, df, common_config):
        password = get_decrypted_password('MYSQL', db_conf['DB_USER'], common_config)
        df.write.mode("append") \
            .format("jdbc") \
            .option("driver", db_conf['DB_DRIVER']) \
            .option("url", db_conf['URL'] + db_name) \
            .option("dbtable", tbl_name) \
            .option("user", db_conf['DB_USER']) \
            .option("password", password) \
            .save()
        logging.info(f'Data loaded into MySQL target table:{db_name}.{tbl_name}')
