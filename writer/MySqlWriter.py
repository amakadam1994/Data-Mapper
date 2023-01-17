import logging

from cryptography.fernet import Fernet


class MySqlWriter:
    def write(spark, db_name, tbl_name, db_conf, df):

        password = None
        try:
            key = db_conf['KEY']  # put your key here
            cipher_suite = Fernet(bytes(key, "UTF-8"))
            ciphered_text = db_conf['DB_PASS']  # put your encrypted password here

            password = cipher_suite.decrypt(bytes(ciphered_text, "UTF-8"))
            password = password.decode()
        except Exception as e:
            print(e)


        df.write.mode("append") \
            .format("jdbc") \
            .option("driver", db_conf['DB_DRIVER']) \
            .option("url", db_conf['URL'] + db_name) \
            .option("dbtable", tbl_name) \
            .option("user", db_conf['DB_USER']) \
            .option("password", password) \
            .save()
        logging.info("Data loaded into MySQL target table:" + db_name + "." + tbl_name)
        print("Data loaded into MySQL target table:" + db_name + "." + tbl_name)
