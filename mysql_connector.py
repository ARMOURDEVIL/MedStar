import mysql.connector

def get_db():
    return mysql.connector.connect(user='root', password='1234', host='localhost', database='medstar')
