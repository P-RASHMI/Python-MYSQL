'''
@Author: Rashmi
@Date: 2021-10-13 22:54
@Last Modified by: Rashmi
@Last Modified time: 2021-10-14  03:35
@Title :Write a Python program for mysql connection and create (simple,unique)indexes,
drop index,display index'''

import os
import mysql.connector
from LoggerHandler import logger 
from dotenv import load_dotenv
load_dotenv()

class index_operations():
    '''
    Description : Different methods performing various index operations
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

    def create_simple_index(self):
        '''
        Description: creates index 'ind_1' on table 'employee' to the column 'name'
        Parameter : self
        '''
        try:
            self.db_cursor.execute("USE employeedb")
            self.db_cursor.execute("CREATE INDEX ind_1 ON employee(name)")
            print("index created")
            logger.info("index created")
        except Exception as e:
            logger.error(e)

    def create_unique_index(self):
        '''
        Description: creates index 'uniq_1' on table 'employee' to the column 'empid',doesn't 
                        allow duplicates
        Parameter : self
        '''
        try:
            self.db_cursor.execute("CREATE UNIQUE INDEX uniq_1 ON employee(empid)")
            print("unique index created")
            logger.info("unique index created")
        except Exception as e:
            logger.error(e)

    def display_indexes(self):
        '''
        Description: To display the indexes from the table
        Parameter : self
        '''
        try:
            self.db_cursor.execute("SHOW INDEXES FROM employee")
            print("indexes are")
            logger.info("indexes are")
            result = self.db_cursor.fetchall()
            for indx in result:
                print(indx)                
        except Exception as e:
            logger.error(e)

    def explain_internal_working(self):
        '''
        Description: To explain internal working of indexing using index on dept column
                        before indexing and after indexing 
        Parameter : self
        '''
        try:
            # before indexing
            self.db_cursor.execute("EXPLAIN SELECT empid,name,dept FROM employee WHERE dept='HR'")
            print("before indexing dept")
            logger.info("before indexing are")
            result = self.db_cursor.fetchall()
            for indx in result:
                print(indx) 

            #creating index dept_ind and after indexing 
            self.db_cursor.execute("CREATE INDEX dept_ind ON employee(dept)")
            self.db_cursor.execute("EXPLAIN SELECT empid,name,dept FROM employee WHERE dept='HR'")
            print("after indexing dept")
            logger.info("after indexing are")
            result = self.db_cursor.fetchall()
            for indx in result:
                print(indx) 
        except Exception as e:
            logger.error(e)

    def select_explain(self):  
        '''
        Description: To explain internal working of indexing using index on phone column
                        before indexing and after indexing with select *
        Parameter : self
        '''  
        try:  
            # before indexing
            self.db_cursor.execute("EXPLAIN SELECT * FROM employee WHERE phone LIKE '%4'")
            print("before indexing phone")
            logger.info("before indexing are")
            result = self.db_cursor.fetchall()
            for indx in result:
                print(indx) 

            #creating index ind_2 and after indexing 
            self.db_cursor.execute("CREATE INDEX ind_2 ON employee(phone)") 
            self.db_cursor.execute("EXPLAIN SELECT * FROM employee WHERE phone LIKE '%4'")
            print("after indexing phone")
            logger.info("after indexing are")
            result = self.db_cursor.fetchall()
            for indx in result:
                print(indx) 
        except Exception as e:
            logger.error(e)  

    def drop_index(self):
        '''
        Description: To drop the index ind_1 from the table employee
        Parameter : self
        '''
        try:
            self.db_cursor.execute("DROP INDEX ind_1 ON employee")
            self.db_cursor.execute("DROP INDEX uniq_1 ON employee")
            self.db_cursor.execute("DROP INDEX ind_2 ON employee")
            self.db_cursor.execute("DROP INDEX dept_ind ON employee")
            print("dropped index")
            logger.info("dropped index are")
            self.db_cursor.execute("SHOW INDEXES FROM employee")
            result = self.db_cursor.fetchall()
            for indx in result:
                print(indx)                
        except Exception as e:
            logger.error(e)

if __name__ == '__main__':
    op_obj = index_operations()
    op_obj.connection_establish()
    op_obj.create_simple_index()
    op_obj.create_unique_index()
    op_obj.display_indexes()
    op_obj.explain_internal_working()
    op_obj.select_explain()
    op_obj.drop_index()
