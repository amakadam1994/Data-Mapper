class MySqlReader:
    def read(spark, DbName, TblName, db_conf):
        data_frame = spark.read. \
            format("jdbc"). \
            option("url", db_conf['URL'] + DbName). \
            option("driver", db_conf['DB_DRIVER']). \
            option("dbtable", TblName). \
            option("user", db_conf['DB_USER']). \
            option("password", db_conf['DB_PASS']). \
            load()
        return data_frame