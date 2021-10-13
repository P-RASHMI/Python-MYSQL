'''
@Author: Rashmi
@Date: 2021-10-12 00:15
@Last Modified by: Rashmi
@Last Modified time: 2021-10-12 1:10
@Title :Write a Python program for mysql connection and insert single values into table ,
multiple values at a time into table ,display table values'''

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
# db_cursor.execute("INSERT INTO employee VALUES(01,'Isha','Accounts')") #for single insertion
insert_query = "INSERT INTO employee (empid,name,dept,phone,email) VALUES (%s,%s,%s,%s,%s)"
insert_values = [(2,'Raj','HR',354156334,'raj@gd.com'),(3,'midhun','IT',2144324,'rssa@g.com'),(4,'virinchi','Admin',12277812,'midhu@gmai.com'),(5,'Mehak','Sports',982313,'mehak@g.com')]
db_cursor.executemany(insert_query,insert_values)
db_connection.commit() #commit to make changes persistent in db;
print(db_cursor.rowcount, "Record Inserted")
db_cursor.execute("SELECT * FROM employee")
for emp in db_cursor:
    print(emp)

