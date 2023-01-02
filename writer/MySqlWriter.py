import logging


class MySqlWriter:
    def write(spark, db_name, tbl_name, db_conf, df):
        df.write.mode("append") \
            .format("jdbc") \
            .option("driver", db_conf['DB_DRIVER']) \
            .option("url", db_conf['URL'] + db_name) \
            .option("dbtable", tbl_name) \
            .option("user", db_conf['DB_USER']) \
            .option("password", db_conf['DB_PASS']) \
            .save()
        logging.info("Data loaded into MySQL target table:" + db_name + "." + tbl_name)
        print("Data loaded into MySQL target table:" + db_name + "." + tbl_name)
