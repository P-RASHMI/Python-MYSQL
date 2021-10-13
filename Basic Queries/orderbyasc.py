'''
@Author: Rashmi
@Date: 2021-10-12 2:30
@Last Modified by: Rashmi
@Last Modified time: 2021-10-12 2:45
@Title :Write a Python program for mysql connection and get ascending,descending
order of names using ORDER BY'''

import mysql.connector
import os
from dotenv import load_dotenv
load_dotenv()
db_connection = mysql.connector.connect(
  host=os.getenv("Host"),
  user=os.getenv("User"),
  passwd=os.getenv("Passwd"),
  database = "employeedb"
)
db_cursor = db_connection.cursor()
insert_query = "SELECT * FROM employee ORDER BY empid"
#insert_query = "SELECT * FROM employee ORDER BY name DESC"
db_cursor.execute(insert_query)
# db_connection.commit()
result = db_cursor.fetchall()
for emp in result:
    print(emp)