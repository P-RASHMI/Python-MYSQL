'''
@Author: Rashmi
@Date: 2021-10-17 17:35
@Last Modified by: Rashmi
@Last Modified time: 2021-10-17  20:35
@Title :Write a Python program for mysql connection with details
reading from .env file and perform window function using aggregate,ranking,
'''

import os
import mysql.connector
from Loggerwindow import logger 
from dotenv import load_dotenv
load_dotenv()

class window_fun():
    '''
    Description : performing queries with window function
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

    def aggregate_fun(self):
        '''
        Description : To perform window function using partition by where 
                      sum(),MIN() aggregation function used;returns sum(sal),MIN(sal) as total sales 
                      for each branch(partition),for each name(partition)
        parameter : self
        '''  
        try:
            self.db_cursor.execute("USE departdb")
            self.db_cursor.execute("SELECT empname,name,branch,SUM(sal) OVER(PARTITION BY branch) AS total_sales FROM depart")
            logger.info("partition by branch done")
            result = self.db_cursor.fetchall()
            for emp in result:
                print(emp)
            # finds min sal by column name wise and applies to all the name columns same value
            self.db_cursor.execute("SELECT empname,name,branch,MIN(sal) OVER(PARTITION BY name) AS total_sales FROM depart")
            logger.info("partition by name done")
            print("partition by name")
            result = self.db_cursor.fetchall()
            for emp in result:
                print(emp)    
        except Exception as e:
            logger.error(e)
 
    def ranking_fun(self):
        '''
        Description : To perform window function for ranking functions(rank(),dense_rank() )
                      using partition by branch,order by no.of employee;
                      returns rank()--->with gaps calculates rank + no of duplicates for next rank
                      returns dense_rank()-->no gaps increments by 1
        parameter : self
        '''  
        try:
            self.db_cursor.execute("USE departdb")
            self.db_cursor.execute("SELECT id,empname,name,branch,employ,sal,RANK() OVER(PARTITION BY branch ORDER BY employ) AS employ_RANK FROM depart")
            logger.info("rank() with gaps INCLUDES DUPLICATE VALUES")
            result = self.db_cursor.fetchall()
            for emp in result:
                print(emp)
            # finds min sal by column name wise and applies to all the name columns same value
            self.db_cursor.execute("SELECT id,empname,name,branch,employ,sal,DENSE_RANK() OVER(PARTITION BY branch ORDER BY employ) AS employ_RANK FROM depart")
            logger.info("DENSE_RANK()")
            print("DENSE_RANK()")
            result = self.db_cursor.fetchall()
            for emp in result:
                print(emp)    
        except Exception as e:
            logger.error(e)

    def analytical_fun(self):
        '''
        Description : To perform window function for analytical functions(NTILE(),LEAD() )
                      using order by name;
                      returns NTILE(4)--->Divides total records by four-->gives 5,4 
                      records with numbering 1-4
                      returns LEAD()--->access sal of  next row to current row in 
                      leading_one column
        parameter : self
        '''  
        try:
            self.db_cursor.execute("USE departdb")
            self.db_cursor.execute("SELECT id,empname,name,branch,employ,sal, NTILE(4) OVER() AS SALE FROM depart")
            logger.info("NTILE(4)") #DIVIDES RECORDS BY 4
            result = self.db_cursor.fetchall()
            for emp in result:
                print(emp)
            # lead to get next row data value to current row
            self.db_cursor.execute("SELECT id,empname,name,branch,employ,sal, LEAD(sal,1) OVER(ORDER BY name) AS leading_one FROM depart")
            logger.info("Lead()")
            print("LEAD SAL ")
            result = self.db_cursor.fetchall()
            for emp in result:
                print(emp)    
        except Exception as e:
            logger.error(e)
    
if __name__ == '__main__':
    op_obj = window_fun()
    try:   
        while(True):
            print("1.connection establish")
            print("2.aggregate function")
            print("3.ranking functions") 
            print("4.analytical function") 
            print("5.Exit")
            choice = int(input())            
            if choice == 1:
                op_obj.connection_establish()
            elif choice == 2:
                op_obj.aggregate_fun()
            elif choice == 3:
                op_obj.ranking_fun()
            elif choice == 4:
                op_obj.analytical_fun()
            else:
                break
    except ValueError as e:
            print("Invalid Input",e)
