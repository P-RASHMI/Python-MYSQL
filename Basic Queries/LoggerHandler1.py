import logging

logger = logging

logger.basicConfig(filename='/home/lenovo/Desktop/Python_work/mysqlcon/Basic Queries/mysqloper.log',
                    level=logging.INFO, format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')

logger.basicConfig(filename='/home/lenovo/Desktop/Python_work/mysqlcon/Basic Queries/mysqloper.log',
                    level=logging.ERROR, format='%(asctime)s:%(funcName)s:%(levelname)s:%(lineo)d')