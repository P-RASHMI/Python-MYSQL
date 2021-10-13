'''
@Author: Rashmi
@Date: 2021-10-11 23:50
@Last Modified by: Rashmi
@Last Modified time: 2021-10-12 00:13
@Title :Write a Python program for mysql connection and creating Table employee,show tables
Describe tables'''

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
db_cursor.execute("CREATE TABLE employee(empid INT, name VARCHAR(20),dept VARCHAR(20),phone INT,email VARCHAR(20))")
# db_cursor.execute("SHOW TABLES")
db_cursor.execute("DESCRIBE employee")
for table in db_cursor:
	print(table)


