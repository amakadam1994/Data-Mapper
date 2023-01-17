import configparser
import json
import logging
import os
from cryptography.fernet import Fernet


def get_key(source, user_name, common_config):
    read_key_from = common_config['READ_KEY_FROM']
    api_url = common_config['KEYMAKER_API']

    if read_key_from.lower() == 'api':
        import requests
        source_user_name = source + '_' + user_name
        url = api_url + source_user_name
        payload = ""
        headers = {}
        response = requests.request("GET", url, headers=headers, data=payload)
        _json = json.loads(response.text)
        return _json['key_value'], _json['password_value']
    else:
        parent_path = os.path.abspath('')
        file = parent_path + '\config\decryption_keys.ini'
        config = configparser.ConfigParser()
        config.read(file)
        return config[source][source + "_KEY"], config[source]["PASSWORD"]


def get_decrypted_password(source, user_name, common_config):
    try:
        key, enc_pass = get_key(source, user_name, common_config)
        cipher_suite = Fernet(bytes(key, "UTF-8"))
        # ciphered_text = db_conf['PASSWORD']
        password = cipher_suite.decrypt(bytes(enc_pass, "UTF-8"))
        return password.decode()
    except Exception as e:
        logging.error(e)


def get_common_jars(parent_path, source, target, config):
    jars = []
    source_jars_comma = config[source]['JARS']
    logging.info(f'source_jars_comma:{source_jars_comma}')
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

    target_jars_comma = config[target]['JARS']
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

def send_email(source_columns, final, source_db, source_table, target_db, target_table, env, config):

    subject = 'Data-Mapper mapping for :{} vs {}'.format(source_table, target_table)
    content = 'The mapping for ' + source_table + ' from ' + source_db + ' vs ' + target_table + ' from ' + target_db + " \n\n"
    file_content= ''
    for i in range(len(source_columns)):
        logging.info(f'{source_columns[i]} : {final[i]}')
        content = content + source_columns[i] + ":" + final[i] + "\n"
        file_content = file_content + source_columns[i] + ":" + final[i] + "\n"

    if env == 'local':
        import smtplib
        from email.message import EmailMessage
        password = get_decrypted_password('EMAIL', config['EMAIL']['EMAIL_FROM'], config['COMMON'])
        msg = EmailMessage()
        msg['Subject'] = subject
        msg['From'] = config['EMAIL']['EMAIL_FROM']
        msg['To'] = config['EMAIL']['EMAIL_TO_LIST']
        msg.set_content(content)
        server = smtplib.SMTP(host='smtp.gmail.com', port=587)
        server.ehlo()
        server.starttls()
        server.set_debuglevel(1)
        server.login(config['EMAIL']['EMAIL_FROM'], password)
        server.send_message(msg)
        server.quit()
        logging.info(f'successfully sent the mail.')
    else:
        pass
        # Write code for unix mail sender
