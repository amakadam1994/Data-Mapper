import configparser
import logging
import os

from cryptography.fernet import Fernet


def get_key(source):
    parent_path = os.path.abspath('')
    file = parent_path + '\config\decryption_keys.ini'
    print("get_key file path:",file)
    config = configparser.ConfigParser()
    config.read(file)
    return config[source][source+"_KEY"]


def get_decrypted_password(source, db_conf):
    try:
        key = get_key(source)  # put your key here
        cipher_suite = Fernet(bytes(key, "UTF-8"))
        ciphered_text = db_conf['PASSWORD']  # put your encrypted password here

        password = cipher_suite.decrypt(bytes(ciphered_text, "UTF-8"))
        password = password.decode()
        return password
    except Exception as e:
        print(e)

def get_common_jars(parent_path, source, target, config):
    jars = []
    source_jars_comma = config[source]['jars']
    logging.info("source_jars_comma:", source_jars_comma)
    print("source_jars_comma:", source_jars_comma)
    if source_jars_comma == None or source_jars_comma == "None":
        pass
    else:
        source_jars_list = source_jars_comma.split(",")
        for jar in source_jars_list:
            jar_path = parent_path + jar
            if jars.__contains__(jar_path):
                pass
            else:
                jars.append(jar_path)

    target_jars_comma = config[target]['jars']
    if target_jars_comma == None or target_jars_comma == "None":
        pass
    else:
        target_jars_list = target_jars_comma.split(",")
        for jar in target_jars_list:
            jar_path = parent_path + jar
            if jars.__contains__(jar_path):
                pass
            else:
                jars.append(jar_path)

    return ','.join(map(str, jars))
