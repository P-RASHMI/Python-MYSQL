'''
@Author: Rashmi
@Date: 2021-10-14 16:31
@Last Modified by: Rashmi
@Last Modified time: 2021-10-15  16:54
@Title :Write a Python program for mysql connection with details
reading from .env file and perform procedures --
create procedures(no parameter,IN parameter,OUT,INOUT),calling procedure,drop'''

import os
import mysql.connector
from Loggerprocedure import logger 
from dotenv import load_dotenv
load_dotenv()

class procedure_op():
    '''
    Description : Different methods for performing various types of joins
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

    def procedure_create_noparameter(self):
        '''
        Description : To create procedure without any parameter
        parameter : self
        '''    
        try:
            self.db_cursor.execute("USE employeedb")
            self.db_cursor.execute("CREATE PROCEDURE All_Employees() BEGIN  SELECT * FROM employee;END")
            logger.info("all employees")
        except Exception as e:
            logger.error(e)

    def calling_procedure_noparameter(self):
        '''
        Description : To call created procedure(without any parameter)
        parameter : self
        '''    
        try:
            self.db_cursor.execute("USE employeedb")
            self.db_cursor.execute("CALL ALL_Employees()")
            logger.info("calling procedure")
            for emp in self.db_cursor:
                print(emp)
        except Exception as e :
            logger.error(e)

    def procedure_create_IN_parameter(self):
        '''
        Description : To create procedure with IN parameter,takes user value as parameter
        parameter : self
        '''    
        try:
            self.db_cursor.execute("USE employeedb")
            self.db_cursor.execute("CREATE PROCEDURE get_Of_Employees(IN var1 INT) BEGIN  SELECT * FROM employee LIMIT var1;END")
            logger.info("limited employees")
        except Exception as e:
            logger.error(e)

    def calling_procedure_IN_parameter(self):
        '''
        Description : To call created procedure(IN parameter) to get limited employees
        parameter : self
        '''    
        try:
            self.db_cursor.execute("USE employeedb")
            self.db_cursor.execute("CALL get_Of_Employees(2)")
            logger.info("calling procedure of IN parameter")
            for emp in self.db_cursor:
                print(emp)
        except Exception as e :
            logger.error(e)

    def procedure_create_OUT_parameter(self):
        '''
        Description : To create procedure with out parameter,An OUT parameter is used to 
                        pass a parameter as output or display like the select operator, but 
                        implicitly (through a set value). 
        parameter : self
        '''    
        try:
            self.db_cursor.execute("USE employeedb")
            self.db_cursor.execute("CREATE PROCEDURE display_count_phone (OUT var2 INT)   BEGIN  SELECT COUNT(phone) INTO var2 FROM employee;END")
            logger.info("count of phones")
        except Exception as e:
            logger.error(e)

    def calling_procedure_OUT_parameter(self):
        '''
        Description : To call created procedure(OUT parameter) to get COUNT OF PHONES IN  employee table
        parameter : self
        '''    
        try:
            self.db_cursor.execute("USE employeedb")
            self.db_cursor.execute("CALL display_count_phone(@M)")
            self.db_cursor.execute("SELECT @M")
            logger.info("calling procedure of OUT parameter")
            for emp in self.db_cursor:
                print(emp)
        except Exception as e :
            logger.error(e)

    def procedure_create_INOUT_parameter(self):
        '''
        Description : To create procedure with INout parameter,
        parameter : self
        '''    
        try:
            self.db_cursor.execute("USE employeedb")
            self.db_cursor.execute("CREATE PROCEDURE display_name_from_depart(INOUT var3 VARCHAR(20))  BEGIN  SELECT name INTO var3 FROM employee WHERE dept = var3;END")
            logger.info("INOUT PARAMETER CREATED")
        except Exception as e:
            logger.error(e)

    def calling_procedure_INOUT_parameter(self):
        '''
        Description : To call created procedure(INOUT parameter) to take depart name as input 
                        and get the employee name as output
        parameter : self
        '''    
        try:
            self.db_cursor.execute("USE employeedb")
            self.db_cursor.execute("SET @M = 'HR'")
            self.db_cursor.execute("CALL display_name_from_depart(@M)")
            self.db_cursor.execute("SELECT @M")
            result = self.db_cursor.fetchall()
            logger.info("calling procedure of INOUT parameter")
            for emp in result:
                print(emp)
        except Exception as e :
            logger.error(e)

    def show_procedures_list(self):
        '''
        Description :To get list of procedures stored in database employeedb
        parameter : self
        ''' 
        try: 
            self.db_cursor.execute("USE employeedb")
            self.db_cursor.execute("SHOW PROCEDURE STATUS WHERE db = 'employeedb'")
            result = self.db_cursor.fetchall()
            logger.info("list of procedures")
            for emp in result:
                print(emp)
        except Exception as e :
            logger.error(e)

    def drop_procedures(self):
        '''
        Description :To drop list of procedures stored in database employeedb
        parameter : self
        ''' 
        try: 
            self.db_cursor.execute("USE employeedb")
            self.db_cursor.execute("DROP PROCEDURE ALL_Employees")
            self.db_cursor.execute("DROP PROCEDURE get_Of_Employees")
            self.db_cursor.execute("DROP PROCEDURE display_count_phone")
            self.db_cursor.execute("DROP PROCEDURE display_name_from_depart")
            result = self.db_cursor.fetchall()
            logger.info("dropped procedure ALL_Employees")
            for emp in result:
                print(emp)
        except Exception as e :
            logger.error(e)

if __name__ == '__main__':
    op_obj = procedure_op()
    try:   
        while(True):
            print("1.connection establish")
            print("2.procedure create with no parameter")
            print("3.calling procedure") 
            print("4.procedure create with IN parameter,limited employees") 
            print("5.calling procedure of IN parameter,limited employees of 2")
            print("6.procedure create with OUT parameter,COUNT OF PHONES") 
            print("7.CALLING procedure OF OUT parameter,COUNT OF PHONES") 
            print("8.procedure create with INOUT parameter,getting name from departname of employee") 
            print("9.Calling procedure of INOUT parameter,getting name from departname of employee") 
            print("10.list of stored procedures") 
            print("11.drop the procedures")
            print("12.Exit")
            choice = int(input())            
            if choice == 1:
                op_obj.connection_establish()
            elif choice == 2:
                op_obj.procedure_create_noparameter()
            elif choice == 3:
                op_obj.calling_procedure_noparameter()
            elif choice == 4:
                op_obj.procedure_create_IN_parameter()
            elif choice == 5:
                op_obj.calling_procedure_IN_parameter()
            elif choice == 6:
                op_obj.procedure_create_OUT_parameter()
            elif choice == 7:
                op_obj.calling_procedure_OUT_parameter()
            elif choice == 8:
                op_obj.procedure_create_INOUT_parameter()
            elif choice == 9:
                op_obj.calling_procedure_INOUT_parameter()
            elif choice == 10:
                op_obj.show_procedures_list()
            elif choice == 11:
                op_obj.drop_procedures()
            else:
                break
    except ValueError as e:
            print("Invalid Input",e)

    