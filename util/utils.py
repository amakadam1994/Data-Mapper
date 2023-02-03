import configparser
import json
import logging
import os
from cryptography.fernet import Fernet


def get_key_from_file(source):
    parent_path = os.path.abspath('')
    file = parent_path + '\config\decryption_keys.ini'
    print(file)
    config = configparser.ConfigParser()
    config.read(file)
    return config[source][source + "_KEY"], config[source]["PASSWORD"]

def get_key_from_api(source, user_name, common_config):
    api_url = common_config['KEYMAKER_API']
    import requests
    source_user_name = source + '_' + user_name
    url = api_url + source_user_name
    payload = ""
    headers = {}
    response = requests.request("GET", url, headers=headers, data=payload)
    _json = json.loads(response.text)
    return _json['key_value'], _json['password_value']

def get_key(source, user_name, common_config):
    read_key_from = common_config['READ_KEY_FROM']
    if read_key_from.lower() == 'api':
        return get_key_from_api(source, user_name, common_config)
    else:
        return get_key_from_file(source)

def get_decrypted_password(source, user_name, common_config):
    try:
        key, enc_pass = get_key(source, user_name, common_config)
        cipher_suite = Fernet(bytes(key, "UTF-8"))
        # ciphered_text = db_conf['PASSWORD']
        password = cipher_suite.decrypt(bytes(enc_pass, "UTF-8"))
        return password.decode()
    except Exception as e:
        logging.error(e)

def add_jars_in_set(jars_comma_separated, parent_path, jars_set):
    if jars_comma_separated == None or jars_comma_separated == "None":
        pass
    else:
        source_jars_list = jars_comma_separated.split(",")
        for jar in source_jars_list:
            jar_path = parent_path + jar
            jars_set.add(jar_path)

    return jars_set


def get_common_jars(parent_path, source, target, config):
    jars_set = set()
    source_jars_comma_separated = config[source]['JARS']
    jars_set = add_jars_in_set(source_jars_comma_separated, parent_path, jars_set)
    target_jars_comma_separated = config[target]['JARS']
    jars_set = add_jars_in_set(target_jars_comma_separated, parent_path, jars_set)
    return ','.join(map(str, jars_set))

def send_email(source_columns, final, source_db, source_table, target_db, target_table, config):

    subject = 'Data-Mapper mapping for :{} vs {}'.format(source_table, target_table)
    content = 'The mapping for ' + source_table + ' from ' + source_db + ' vs ' + target_table + ' from ' + target_db + " \n\n"
    file_content= ''
    for i in range(len(source_columns)):
        logging.info(f'{source_columns[i]} : {final[i]}')
        content = content + source_columns[i] + ":" + final[i] + "\n"
        file_content = file_content + source_columns[i] + ":" + final[i] + "\n"

    if config['COMMON']['ENV'] == 'local':
        smtp_mail(config, subject, content)
    else:
        unix_mail()


def smtp_mail(config, subject, content):
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

def unix_mail():
    pass
    # Write code for unix mail sender
