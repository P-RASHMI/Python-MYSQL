import logging

logger = logging

logger.basicConfig(filename='/home/lenovo/Desktop/Python_work/mysqlcon/window functn/windowfun.log',
                    level=logging.INFO, format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')

logger.basicConfig(filename='/home/lenovo/Desktop/Python_work/mysqlcon/window functn/windowfun.log',
                    level=logging.ERROR, format='%(asctime)s:%(funcName)s:%(levelname)s:%(lineo)d')