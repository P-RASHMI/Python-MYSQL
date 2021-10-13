'''
@Author: Rashmi
@Date: 2021-10-11 23:30
@Last Modified by: Rashmi
@Last Modified time: 2021-10-11 23:44
@Title :Write a Python program for mysql connection and creating Database employeedb,
showing databses'''

import mysql.connector
import os
from dotenv import load_dotenv
load_dotenv()
db_connection = mysql.connector.connect(
  host=os.getenv("Host"),
  user=os.getenv("User"),
  passwd=os.getenv("Passwd")
)
#print(db_connection) #to check db connection
db_cursor = db_connection.cursor()
db_cursor.execute("CREATE DATABASE employeedb")
db_cursor.execute("SHOW DATABASES")
print("the databases are:")
for db in db_cursor:
  print(db)

