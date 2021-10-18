'''
@Author: Rashmi
@Date: 2021-10-16 16:00
@Last Modified by: Rashmi
@Last Modified time: 2021-10-17  16:35
@Title :Write a Python program for mysql connection with details
reading from .env file and perform import ,export of databases
'''

import os
import mysql.connector
from Loggerimportexport import logger 
from dotenv import load_dotenv
load_dotenv()

class db_impoexpo():
    '''
    Description : performing import and export of databases
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

    def expo_db(self):
        '''
        Description : To export database(departdb) to file (newdbfile.sql)
        parameter : self
        ''' 
        try:
            os.system("mysqldump -u root -p departdb > newdbfile.sql")
            logger.info("exported db")
        except Exception as e:
            logger.error(e)

    def impo_db(self):
        '''
        Description : To import file (newdbfile.sql) data into new database(newimportdepart) 
        parameter : self
        ''' 
        try:
            self.db_cursor.execute("CREATE DATABASE newimportdepart")
            logger.info("db created")
            os.system("mysql -u root -p newimportdepart < newdbfile.sql")
            self.db_cursor.execute("SHOW DATABASES")
            result = self.db_cursor.fetchall()
            for emp in result:
                print(emp)
        except Exception as e:
            logger.error(e)

if __name__ == '__main__':
    op_obj = db_impoexpo()
    try:   
        while(True):
            print("1.connection establish")
            print("2.export database")
            print("3.import database") 
            print("4.Exit")
            choice = int(input())            
            if choice == 1:
                op_obj.connection_establish()
            elif choice == 2:
                op_obj.expo_db()
            elif choice == 3:
                op_obj.impo_db()
            else:
                break
    except Exception as e:
        logger.error(e)