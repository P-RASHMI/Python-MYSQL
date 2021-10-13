'''
@Author: Rashmi
@Date: 2021-10-12 1:20
@Last Modified by: Rashmi
@Last Modified time: 2021-10-12 2:30
@Title :Write a Python program for mysql connection and delete rows using where condition'''

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
insert_query = "DELETE FROM employee WHERE empid = 3"
db_cursor.execute(insert_query)
db_connection.commit() #commit to make changes persistent in db;
print(db_cursor.rowcount, "Record deleted")
db_cursor.execute("SELECT * FROM employee")
for emp in db_cursor:
    print(emp)