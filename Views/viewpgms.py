'''
@Author: Rashmi
@Date: 2021-10-14 14:15
@Last Modified by: Rashmi
@Last Modified time: 2021-10-14  14:52
@Title :Write a Python program for mysql connection with details
reading from .env file and create views view1,update,display
drop view and use loggers'''

import os
import mysql.connector
from Loggerview import logger 
from dotenv import load_dotenv
load_dotenv()

class view_operations():
    '''
    Description : Different methods for performing various view operations
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

    def create_view(self):
        '''
        Description : to create view called view1 from table depart with columns of
                        name,branch
        Parameter : self
        '''
        try:
            self.db_cursor.execute("USE departdb")
            self.db_cursor.execute("CREATE VIEW view1 AS SELECT name,branch FROM depart")
            logger.info("created view1 view")
            print("created view1 view")
        except Exception as e:
            logger.error(e)  

    def display_view(self):
        '''
        Description : to display view1 table
        Parameter : self
        '''
        try:
            self.db_cursor.execute("SELECT * FROM view1")
            logger.info("DISPLAY OF VIEW1")
            print("DISPLAY OF VIEW1")
            result = self.db_cursor.fetchall()
            for val in result:
                print(val)
        except Exception as e:
            logger.error(e)  

    def update_view(self):
        '''
        Description : to update view1 table with empname using alter
        Parameter : self
        '''
        try:
            self.db_cursor.execute("ALTER VIEW view1 AS SELECT empname,name,branch FROM depart")
            logger.info("UPDATION OF VIEW1")
            print("UPDATION OF VIEW1")
            # to view the updated view1
            self.db_cursor.execute("SELECT * FROM view1")
            result = self.db_cursor.fetchall()
            for val in result:
                print(val)
        except Exception as e:
            logger.error(e) 

    def drop_view(self):
        '''
        Description : to drop the view named view1 
        Parameter : self
        '''
        try:
            self.db_cursor.execute("drop view view1")
            logger.info("dropped VIEW1")
            print("dropped VIEW1")
            # to view the view1 ,shows error as no view1 exists
            self.db_cursor.execute("SELECT * FROM view1")
            result = self.db_cursor.fetchall()
            for val in result:
                print(val)
        except Exception as e:
            logger.error(e) 

if __name__ == '__main__':
    op_obj = view_operations()
    op_obj.create_view()
    op_obj.display_view()
    op_obj.update_view()
    op_obj.drop_view()
        