class MySqlReader:
    def read(spark, db_name, tbl_name, db_conf):
        tbl_lst = tbl_name.split('&')
        join_cond = False
        tbls_dict = {}
        if len(tbl_lst) > 1:
            join_cond = True
            tbls_dict = { i.split(':')[0]:i.split(':')[1] for i in tbl_lst }

        if join_cond:
            data_frame = ''
            for tbl_name, join_col in tbls_dict.items():
                if not data_frame:
                    data_frame = spark.read. \
                        format("jdbc"). \
                        option("url", db_conf['URL'] + db_name). \
                        option("driver", db_conf['DB_DRIVER']). \
                        option("dbtable", tbl_name). \
                        option("user", db_conf['DB_USER']). \
                        option("password", db_conf['DB_PASS']). \
                        load()
                else:
                    df2 = spark.read. \
                        format("jdbc"). \
                        option("url", db_conf['URL'] + db_name). \
                        option("driver", db_conf['DB_DRIVER']). \
                        option("dbtable", tbl_name). \
                        option("user", db_conf['DB_USER']). \
                        option("password", db_conf['DB_PASS']). \
                        load()

                    data_frame = data_frame.join(df2,[join_col], how='inner')

            print('data_frame after join')

            return data_frame


        else:

            data_frame = spark.read. \
                format("jdbc"). \
                option("url", db_conf['URL'] + db_name). \
                option("driver", db_conf['DB_DRIVER']). \
                option("dbtable", tbl_name). \
                option("user", db_conf['DB_USER']). \
                option("password", db_conf['DB_PASS']). \
                load()
            return data_frame