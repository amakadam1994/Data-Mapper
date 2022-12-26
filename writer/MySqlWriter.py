class MySqlWriter:
    def write(spark, DbName, TblName, db_conf, df):
        df.write.mode("append") \
            .format("jdbc") \
            .option("driver", db_conf['DB_DRIVER']) \
            .option("url", db_conf['URL'] + DbName) \
            .option("dbtable", TblName) \
            .option("user", db_conf['DB_USER']) \
            .option("password", db_conf['DB_PASS']) \
            .save()
        print("Data loaded into MySQL target table:" + DbName + "." + TblName)