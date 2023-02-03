import os
import pytest
from main.util.utils import send_email
from main.util.utils import get_common_jars
from main.util.utils import get_decrypted_password

config = {
        'MYSQL': {
            'JARS': '\jar_files\mysql-connector-java-8.0.22.jar',
            'URL': 'jdbc:mysql://localhost:3306/',
            'DB_DRIVER': 'com.mysql.cj.jdbc.Driver',
            'DB_USER': 'root'
        },
        'MONGODB': {
            'JARS': 'None',
            'URL': 'jdbc:mysql://localhost:3306/',
            'DB_USER': 'ihub'
        },
        'EMAIL': {
            'EMAIL_FROM':'amar.kadam@clairvoyantsoft.com', 'EMAIL_TO_LIST':'amar.kadam@clairvoyantsoft.com'
        },
        'COMMON': {
            'KEYMAKER_API':'http://127.0.0.1:5000/key/',
            'ENV':'local',
            'SEND_EMAIL':'true',
            'LOAD_DATA':'true',
            'READ_KEY_FROM':'api',
            'CREATE_TABLE_IF_NOT_EXIST':'true'
        }
     }



@pytest.mark.parametrize('source_columns, final, source_db, source_table, target_db, target_table, config',
                         [
                             ( ['a','b'], ['A','B'], 'source_db', 'source_table', 'target_db', 'target_table', config)
                         ]
)
def test_send_email(source_columns, final, source_db, source_table, target_db, target_table, config):
    send_email(source_columns, final, source_db, source_table, target_db, target_table, config)



@pytest.mark.parametrize('source, target, config, result',
                         [
                             ( 'MYSQL', 'MONGODB', config, 'C:\\Users\\Amar Kadam\\Desktop\\DE_Capability\\Data-Mapper\\jar_files\\mysql-connector-java-8.0.22.jar')
                         ]
)
def test_get_common_jars( source, target, config, result):
    jar_string = get_common_jars(os.path.abspath('../../'), source, target, config)
    assert jar_string == result



@pytest.mark.parametrize('source, user_name, common_config, result',
                         [
                             ( 'MYSQL', 'root', config['COMMON'], 'MySql@123')
                         ]
)
def test_get_decrypted_password(source, user_name, common_config, result):
    password = get_decrypted_password(source, user_name, common_config)
    assert password == result






