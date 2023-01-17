import configparser
import logging
import os
from cryptography.fernet import Fernet


def get_key(source):
    parent_path = os.path.abspath('')
    file = parent_path + '\config\decryption_keys.ini'
    config = configparser.ConfigParser()
    config.read(file)
    return config[source][source + "_KEY"], config[source]["PASSWORD"]

def get_decrypted_password(source, db_conf):
    try:
        key, enc_pass = get_key(source)
        cipher_suite = Fernet(bytes(key, "UTF-8"))
        # ciphered_text = db_conf['PASSWORD']
        password = cipher_suite.decrypt(bytes(enc_pass, "UTF-8"))
        return password.decode()
    except Exception as e:
        logging.error(e)


def get_common_jars(parent_path, source, target, config):
    jars = []
    source_jars_comma = config[source]['jars']
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

def send_email(source_columns, final, source_db, source_table, target_db, target_table, env, email_list):
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
        password = get_decrypted_password('EMAIL', email_list)
        msg = EmailMessage()
        msg['Subject'] = subject
        msg['From'] = email_list['EMAIL_FROM']
        msg['To'] = email_list['EMAIL_TO_LIST']
        msg.set_content(content)
        server = smtplib.SMTP(host='smtp.gmail.com', port=587)
        server.ehlo()
        server.starttls()
        server.set_debuglevel(1)
        server.login(email_list['EMAIL_FROM'], password)
        server.send_message(msg)
        server.quit()
        logging.info(f'successfully sent the mail.')
    else:
        pass
        # Write code for unix mail sender
