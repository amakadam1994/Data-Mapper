class MySqlReader:
    def read(spark, db_name, tbl_name, db_conf):

        data_frame = spark.read. \
            format("jdbc"). \
            option("url", db_conf['URL'] + db_name). \
            option("driver", db_conf['DB_DRIVER']). \
            option("dbtable", tbl_name). \
            option("user", db_conf['DB_USER']). \
            option("password", db_conf['DB_PASS']). \
            load()
        return data_frame
