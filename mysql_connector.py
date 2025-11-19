import mysql.connector

def get_db():
    return mysql.connector.connect(user='root', password='root', host='localhost', database='medstar')
