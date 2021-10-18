'''
@Author: Rashmi
@Date: 2021-10-14 16:31
@Last Modified by: Rashmi
@Last Modified time: 2021-10-14  23:30
@Title :Write a Python program for mysql connection with details
reading from .env file and perform joins like inner join, left join,right join,full join, 
cross join,self join'''

import os
import mysql.connector
from Loggerjoin import logger 
from dotenv import load_dotenv
load_dotenv()

class joins_type():
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

    def create_db(self):
        '''
        Description : To create database 
        parameter : self
        '''   
        try:
            self.db_cursor.execute("CREATE DATABASE joindb")
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
            self.db_cursor.execute("USE joindb")
            self.db_cursor.execute("CREATE TABLE student(id INT AUTO_INCREMENT,name VARCHAR(20),city VARCHAR(20),PRIMARY KEY(id))")
            self.db_cursor.execute("CREATE TABLE contact(con_id INT,mobile BIGINT,CONSTRAINT fk_id FOREIGN KEY(con_id) REFERENCES student(id))")
            logger.info("created table")
            print("created table")
            self.db_cursor.execute("SHOW TABLES")
            for table in self.db_cursor:
	            print(table)
        except Exception as e:
            logger.error(e)
        
    def describe_tables(self):
        '''
        Description : To describe tables student,contact     
        parameter : self 
        '''
        try:
            self.db_cursor.execute("USE joindb")
            self.db_cursor.execute("DESCRIBE student")
            logger.info("student table")
            print("student table describe")
            for table in self.db_cursor:
	            print(table)
            
            self.db_cursor.execute("DESCRIBE contact")
            logger.info("contact table")
            print("contact table describe")
            for table in self.db_cursor:
	            print(table)
        except Exception as e:
            logger.error(e)

    def insert_values(self):
        '''
        Description : To insert multiple values to the table student,contact
        parameter : self 
        '''
        try:
            self.db_cursor.execute("USE joindb")
            insert_query = "INSERT INTO student(id,name,city) VALUES (%s,%s,%s)"
            insert_values = [
                                (1,'vithika','HYD'),
                                (2,'Chintu','Banglore'),
                                (3,'Riddi','Chennai'),   
                            ]
            self.db_cursor.executemany(insert_query,insert_values)
            self.db_connection.commit() #commit to make changes persistent in db;
            logger.info("inserted values")
            print(self.db_cursor.rowcount, "Record Inserted")

            insert_query1 = "INSERT INTO contact(con_id,mobile) VALUES (%s,%s)"
            insert_values1 = [
                                (1,98425242553),
                                (2,842351222),
                             ]
            self.db_cursor.executemany(insert_query1,insert_values1)
            self.db_connection.commit() #commit to make changes persistent in db;
            logger.info("inserted values")
            print(self.db_cursor.rowcount, "Record Inserted")
        except Exception as e:
            logger.error(e)

    def inner_join(self):
        '''
        Description : To perform inner join of the table student,contact which returns columns 
                        as specified for the common values
        parameter : self 
        '''
        try:
            self.db_cursor.execute("USE joindb")
            self.db_cursor.execute("SELECT student.name,student.city,contact.mobile FROM student INNER JOIN contact ON student.id = contact.con_id")
            logger.info("inner join")
            print("inner join")
            result = self.db_cursor.fetchall()
            for val in result:
                print(val)
        except Exception as e:
            logger.error(e)

    def left_join(self):
        '''
        Description : To perform left join of the table student,contact which returns all 
                        rows from left(student table) and these values not matched right table 
                        gives null
        parameter : self 
        '''
        try:
            self.db_cursor.execute("USE joindb")
            self.db_cursor.execute("SELECT student.*,contact.* FROM student LEFT JOIN contact ON student.id = contact.con_id")
            logger.info("LEFT join")
            print("LEFT join")
            result = self.db_cursor.fetchall()
            for val in result:
                print(val)
        except Exception as e:
            logger.error(e)

    def right_join(self):
        '''
        Description : To perform right join of the table student,contact which returns all 
                        rows from right(contact table) and these values not matched left table 
                        gives null
        parameter : self 
        '''
        try:
            self.db_cursor.execute("USE joindb")
            self.db_cursor.execute("SELECT student.*,contact.* FROM student RIGHT JOIN contact ON student.id = contact.con_id")
            logger.info("right join")
            print("right join")
            result = self.db_cursor.fetchall()
            for val in result:
                print(val)
        except Exception as e:
            logger.error(e)

    # def full_join(self):
    #     '''
    #     Description : To perform right join of the table student,contact which returns all 
    #                     rows from right(contact table) and these values not matched left table 
    #                     gives null
    #     parameter : self 
    #     '''
    #     try:
    #         self.db_cursor.execute("USE joindb")
    #         self.db_cursor.execute("SELECT student.* FROM student UNION SELECT contact.* FROM contact")
    #         logger.info("full join")
    #         print("full join")
    #         result = self.db_cursor.fetchall()
    #         for val in result:
    #             print(val)
    #     except Exception as e:
    #         logger.error(e)

    def cross_join(self):
        '''
        Description : To perform cross join(cartesian product) of the table student,contact which returns all 
                     records from student and contact, and each row is the combination of rows of both tables.
        parameter : self 
        '''
        try:
            self.db_cursor.execute("USE joindb")
            self.db_cursor.execute("SELECT student.*,contact.* FROM student CROSS JOIN contact")
            logger.info("cross join")
            print("cross join")
            result = self.db_cursor.fetchall()
            for val in result:
                print(val)
        except Exception as e:
            logger.error(e)

    def self_join(self):
        '''
        Description : performs self join that is used to join a table with itself
        parameter : self 
        '''
        try:
            self.db_cursor.execute("USE joindb")
            self.db_cursor.execute("SELECT A.id,B.name FROM student A,student B WHERE A.city = B.city")
            logger.info("self join")
            print("self join")
            result = self.db_cursor.fetchall()
            for val in result:
                print(val)
        except Exception as e:
            logger.error(e)    

if __name__ == '__main__':
    op_obj = joins_type()
    try:   
        while(True):
            print("1.connection establish")
            print("2.create database")
            print("3.create tables") 
            print("4.Describe tables") 
            print("5.insert values")
            print("6.inner join") 
            print("7.left join") 
            print("8.right join") 
            # print("9.full join") 
            print("10.cross join") 
            print("11.self join")
            print("12.Exit")
            choice = int(input())            
            if choice == 1:
                op_obj.connection_establish()
            elif choice == 2:
                op_obj.create_db()
            elif choice == 3:
                op_obj.create_table()
            elif choice == 4:
                op_obj.describe_tables()
            elif choice == 5:
                op_obj.insert_values()
            elif choice == 6:
                op_obj.inner_join()
            elif choice == 7:
                op_obj.left_join()
            elif choice == 8:
                op_obj.right_join()
            # elif choice == 9:
            #     op_obj.full_join()
            elif choice == 10:
                op_obj.cross_join()
            elif choice == 11:
                op_obj.self_join()
            else:
                break
    except ValueError as e:
            print("Invalid Input",e)

    
    
    
    
    
    