'''
@Author: Rashmi
@Date: 2021-10-12 15:35
@Last Modified by: Rashmi
@Last Modified time: 2021-10-13 18:08
@Title :Write a Python program for mysql connection and execute queries like create Database,
tables,insert,select,delete,update,order,limit,where,distinct,like,having,group by,
aggregate functions like count,avg,sum,min,max'''

import os
import mysql.connector
from LoggerHandler1 import logger 
from dotenv import load_dotenv
load_dotenv()

class mysql_operations():
    '''
    Description : Different methods performing various mysql operations
    '''
    def __init__(self):
        '''
        Description : get mysql connection,get host,user,password from .env file
        '''
        self.db_connection = mysql.connector.connect(
            host=os.getenv("Host"),
            user=os.getenv("User"),
            passwd=os.getenv("Passwd"),
        )
        self.db_cursor = self.db_connection.cursor()
        
    def connection_establish(self):
        '''
        Description : To print connection establishment
        parameter : self
        '''    
        try:
            print(self.db_connection)
            logger.info("connection established")
        except Exception as e:
            logger.error(e)

    def create_db(self):
        '''
        Description : To create database 
        parameter : self
        '''   
        try:
            self.db_cursor.execute("CREATE DATABASE departdb")
            self.db_cursor.execute("CREATE DATABASE student")
            logger.info("created database")
            self.db_cursor.execute("SHOW DATABASES")
            print("the databases are:")
            for db in self.db_cursor:
                print(db)
        except Exception as e:
            logger.error(e)

    def create_table(self):
        '''
        Description : To create table
        parameter : self 
        '''
        try:
            self.db_cursor.execute("USE departdb")
            self.db_cursor.execute("CREATE TABLE depart(id INT,empname VARCHAR(20),name VARCHAR(20),branch VARCHAR(20),employ INT,sal INT)")
            logger.info("created table")
            print("created table")
            #self.db_cursor.execute("SHOW TABLES")
            self.db_cursor.execute("DESCRIBE employee")
            for table in self.db_cursor:
	            print(table)
        except Exception as e:
            logger.error(e)

    def drop_db(self):
        '''
        Description : To drop the databases
        parameter : self 
        '''
        try:
            self.db_cursor.execute("DROP DATABASE student")
            logger.info("droped student database")
            print("droped student database")
            self.db_cursor.execute("SHOW TABLES")
            for table in self.db_cursor:
	            print(table)
        except Exception as e:
            logger.error(e)

    def insert_value(self):
        '''
        Description : To insert values to the table depart
        parameter : self 
        '''
        try:
            self.db_cursor.execute("INSERT INTO depart VALUES(1,'Harsh','Finance depart','Accounts',25,30000)")
            print("one value inserted into depart table")
            logger.info("one value inserted into depart table")
            self.db_connection.commit() #commit to make changes persistent in db;
            print(self.db_cursor.rowcount, "Record Inserted")
            self.db_cursor.execute("SELECT * FROM depart")
            for emp in self.db_cursor:
                print(emp)
        except Exception as e:
            logger.error(e)

    def insert_manyvalues(self):
        '''
        Description : To insert multiple values to the table depart
        parameter : self 
        '''
        try:
            insert_query = "INSERT INTO depart (id,empname,name,branch,employ,sal) VALUES (%s,%s,%s,%s,%s,%s)"
            insert_values = [
                                (2,'vithika','Admin depart','registration',15,40000),
                                (3,'Chintu','Humanities depart','langs',22,100000),
                                (4,'Riddi','Human resources','HR',12,20000),
                                (5,'Navi','Cultural','sports',45,50000)
                            ]
            self.db_cursor.executemany(insert_query,insert_values)
            self.db_connection.commit() #commit to make changes persistent in db;
            logger.info("inserted values")
            print(self.db_cursor.rowcount, "Record Inserted")
        except Exception as e:
            logger.error(e)
        
    def select(self):
        '''
        Description : To display table depart
        parameter : self 
        '''
        try:
            self.db_cursor.execute("SELECT * FROM depart")
            result = self.db_cursor.fetchall()
            logger.info("displays table")
            print("display table ")
            for emp in result:
                print(emp)
        except Exception as e:
            logger.error(e)

    def where(self):
        '''
        Description : gives row by filtering the conditions used by where
        parameter : self 
        '''
        try:
            self.db_cursor.execute("SELECT * FROM depart WHERE name = 'Admin depart'")
            result = self.db_cursor.fetchall()
            logger.info("prints selected row")
            print("the row is:")
            for emp in result:
                print(emp)
        except Exception as e:
            logger.error(e)
    
    def order(self):
        '''
        Description : gives ascending,descending of rows by using order by
        parameter : self 
        '''
        try:
            self.db_cursor.execute("SELECT * FROM depart ORDER BY id")
            result = self.db_cursor.fetchall()
            logger.info("prints depart row ascen order id")
            print("prints depart row ascen order id:")
            for emp in result:
                print(emp)
            self.db_cursor.execute("SELECT * FROM depart ORDER BY employ DESC")
            result = self.db_cursor.fetchall()
            logger.info("prints depart row desc order employ")
            print("prints depart row desc order employ:")
            for emp in result:
                print(emp)
        except Exception as e:
            logger.error(e)

    def delet(self):
        '''
        Description : deleted row containig id as 5 from depart table
        parameter : self 
        '''
        try:
            self.db_cursor.execute("DELETE FROM depart WHERE id = 5")
            result = self.db_cursor.fetchall()
            logger.info("deleted id of 5")
            print("deleted id of 5")
            self.db_connection.commit() #commit to make changes persistent in db;
            print(self.db_cursor.rowcount, "Record deleted")
            for emp in result:
                print(emp)
        except Exception as e:
            logger.error(e)

    def update_val(self):
        '''
        Description : update row value of depart table where initial employe
                        of 12 to 32 of id 4
        parameter : self 
        '''
        try:
            self.db_cursor.execute("UPDATE depart SET employ = '32' WHERE employ = '12'")
            result = self.db_cursor.fetchall()
            logger.info("updated value to 32")
            print("updated value to 32")
            self.db_connection.commit() #commit to make changes persistent in db;
            print(self.db_cursor.rowcount, "Record updated")
            for emp in result:
                print(emp)
        except Exception as e:
            logger.error(e)

    def limit(self):
        '''
        Description : to restrict the no.of records ,here restricts to 6,
                        use offset 2 from (3RD ROW) where it starts rows printing 
        parameter : self 
        '''
        try:
            self.db_cursor.execute("SELECT * FROM depart LIMIT 6")
            result = self.db_cursor.fetchall()
            logger.info("limit of 6 records")
            print("limit of 6 records")
            for emp in result:
                print(emp)
            self.db_cursor.execute("SELECT * FROM depart LIMIT 6 OFFSET 2")
            result = self.db_cursor.fetchall()
            logger.info("limit of 6 records FROM 3RD ROW")
            print("limit of 6 records from 3rd row")
            for emp in result:
                print(emp)
        except Exception as e:
            logger.error(e)
    
    def distinct(self):
        '''
        Description : gives distinct values from the column
        parameter : self 
        '''
        try:
            self.db_cursor.execute("SELECT DISTINCT name FROM depart")
            result = self.db_cursor.fetchall()
            logger.info("distinct values")
            print("distinct values of name from depart ")
            for emp in result:
                print(emp)
        except Exception as e:
            logger.error(e)

    def like(self):
        '''
        Description : gives specified pattern values from the column
        parameter : self 
        '''
        try:
            self.db_cursor.execute("SELECT * FROM depart WHERE name LIKE '%Human%'")
            result = self.db_cursor.fetchall()
            logger.info("human pattern values ")
            print("human pattern values ")
            for emp in result:
                print(emp)
        except Exception as e:
            logger.error(e)

    def groupby(self):
        '''
        Description : identical values are arranged into groups
        parameter : self 
        '''
        try:
            self.db_cursor.execute("SELECT sal,name,empname FROM depart WHERE sal>40000  GROUP BY sal,name,empname")
            result = self.db_cursor.fetchall()
            logger.info("sal,name,empname fetched with sal >40000 ")
            print("sal,name,empname fetched with sal >40000 ")
            for emp in result:
                print(emp)

            self.db_cursor.execute("SELECT name,SUM(sal) FROM depart GROUP BY name")
            result = self.db_cursor.fetchall()
            logger.info("gives departname with sum of the salaries of each dept")
            print("gives departname with sum of the salaries of each dept ")
            for emp in result:
                print(emp)

        except Exception as e:
            logger.error(e)

    def having(self):
        '''
        Description :gives rows with having conditions 
        parameter : self 
        '''
        try:
            self.db_cursor.execute("SELECT name,SUM(sal) FROM depart GROUP BY name HAVING SUM(sal)>20000")
            result = self.db_cursor.fetchall()
            logger.info("sal,name fetched with sum(sal) >20000 ")
            print("sal,name fetched with sum(sal) >20000 ")
            for emp in result:
                print(emp)

            self.db_cursor.execute("SELECT sal,name,empname FROM depart GROUP BY sal,name,empname HAVING sal>40000")
            result = self.db_cursor.fetchall()
            logger.info("sal,name,empname fetched from depart with sal >40000 ")
            print("sal,name,empname fetched from depart with sal >40000")
            for emp in result:
                print(emp)

        except Exception as e:
            logger.info(e)

    def aggregate_func(self):
        '''
        Description : performs aggregate functions like sum,max,min,count,avg
        Parameter : self
        '''
        try:
            self.db_cursor.execute("SELECT SUM(sal) FROM depart")
            result = self.db_cursor.fetchall()
            logger.info("sum of col")
            for emp in result:
                print("sum",emp)
            
            self.db_cursor.execute("SELECT MAX(sal) FROM depart")
            result = self.db_cursor.fetchall()
            logger.info("max of col")
            for emp in result:
                print("max",emp)

            self.db_cursor.execute("SELECT MIN(sal) FROM depart")
            result = self.db_cursor.fetchall()
            logger.info("min of col")
            for emp in result:
                print("min",emp)

            self.db_cursor.execute("SELECT AVG(Sal) FROM depart")
            result = self.db_cursor.fetchall()
            logger.info("avg of col")
            for emp in result:
                print("avg",emp)

            self.db_cursor.execute("SELECT COUNT(sal) FROM depart")
            result = self.db_cursor.fetchall()
            logger.info("count of col")
            for emp in result:
                print("count",emp)
        except Exception as e:
            logger.info(e)

if __name__ == '__main__':
    op_obj = mysql_operations()
    op_obj.connection_establish()
    op_obj.create_db()
    op_obj.create_table()
    op_obj.drop_db()
    op_obj.insert_value()
    op_obj.insert_manyvalues()
    op_obj.select()
    op_obj.where()
    op_obj.order()
    op_obj.delet()
    op_obj.update_val()
    op_obj.limit()
    op_obj.distinct()
    op_obj.like()
    op_obj.groupby()
    op_obj.having()
    op_obj.aggregate_func()
