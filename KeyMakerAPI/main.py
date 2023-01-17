import json

import pymysql
from app import app
from config import mysql
from flask import jsonify
from flask import flash, request

@app.route('/create', methods=['POST'])
def create_key():
    try:
        _json = json.loads(request.get_data(as_text="json"))
        _source = _json["source"]
        _user_name = _json["user_name"]
        _key_value = _json["key_value"]
        _password_value = _json["password_value"]
        _source_user_name = _json["source"]+'_'+_json["user_name"]
        if _source and _user_name and _key_value and _password_value and str(request.method) == "POST":
            conn = mysql.connect()
            cursor = conn.cursor(pymysql.cursors.DictCursor)
            sqlQuery = "INSERT INTO rest_api.keys(source_user_name, source, user_name, key_value, password_value) VALUES(%s, %s, %s, %s, %s)"
            bindData = (_source_user_name, _source, _user_name, _key_value, _password_value)
            cursor.execute(sqlQuery, bindData)
            conn.commit()
            respone = jsonify('Key added successfully!')
            respone.status_code = 200
            return respone
        else:
            return showMessage()
    except Exception as e:
        print(e)
    finally:
        cursor.close()
        conn.close()


@app.route('/key')
def keys():
    try:
        conn = mysql.connect()
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        cursor.execute("SELECT source_user_name,source,user_name,key_value,password_value FROM rest_api.keys")
        empRows = cursor.fetchall()
        respone = jsonify(empRows)
        respone.status_code = 200
        return respone
    except Exception as e:
        print(e)
    finally:
        cursor.close()
        conn.close()


@app.route('/key/<_source_user_name>')
def get_key(_source_user_name):
    try:
        # _source_user_name='MySql_root'
        conn = mysql.connect()
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        cursor.execute("SELECT key_value, password_value FROM rest_api.keys WHERE source_user_name =%s", _source_user_name)
        empRow = cursor.fetchone()
        respone = jsonify(empRow)
        respone.status_code = 200
        return respone
    except Exception as e:
        print(e)
    finally:
        cursor.close()
        conn.close()


@app.route('/update', methods=['PUT'])
def update_key():
    try:
        _json = json.loads(request.get_data(as_text="json"))
        _source = _json['source']
        _user_name = _json['user_name']
        _key_value = _json['key_value']
        _password_value = _json['password_value']
        _source_user_name = _json['source']+'_'+_json['user_name']
        if _source and _user_name and _key_value and _password_value and request.method == 'PUT':
            sqlQuery = "UPDATE rest_api.keys SET key_value=%s, password_value=%s WHERE user_name=%s and source=%s and source_user_name=%s"
            bindData = (_key_value, _password_value, _user_name, _source, _source_user_name)
            conn = mysql.connect()
            cursor = conn.cursor()
            cursor.execute(sqlQuery, bindData)
            conn.commit()
            respone = jsonify('Key updated successfully!')
            respone.status_code = 200
            return respone
        else:
            return showMessage()
    except Exception as e:
        print(e)
    finally:
        cursor.close()
        conn.close()


@app.route('/delete/<_source_user_name>', methods=['DELETE'])
def delete_key(_source_user_name):
    try:
        conn = mysql.connect()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM rest_api.keys WHERE source_user_name =%s", (_source_user_name,))
        conn.commit()
        respone = jsonify('Key deleted successfully!')
        respone.status_code = 200
        return respone
    except Exception as e:
        print(e)
    finally:
        cursor.close()
        conn.close()


@app.errorhandler(404)
def showMessage(error=None):
    message = {
        'status': 404,
        'message': 'Record not found: ' + request.url,
    }
    respone = jsonify(message)
    respone.status_code = 404
    return respone


if __name__ == "__main__":
    app.run()