import pymysql
import re
import traceback


class Data_deal(object):
    def __init__(self, user, password, database=None, host='localhost', port=3306):
        self.host = host
        self.user = user
        self.password = password
        self.port = port
        self.charset = 'utf8'
        self.__database = database

    def connect_sql(self, database_name=None):
        '''连接数据库'''
        self.__database = database_name
        self.__connect = pymysql.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            port=self.port,
            charset=self.charset,
            database=self.__database
        )
        self.__cursor = self.__connect.cursor()
        connect, cursor = self.__connect, self.__cursor
        return connect, cursor

    def close(self):
        '''关闭数据库连接'''
        self.__connect.commit()
        self.__cursor.close()
        self.__connect.close()

    def database_exist(self, database_name):
        '''是否存在数据库'''
        self.bools = 1
        try:
            self.connect_sql()
            sql = "show databases"
            self.__cursor.execute(sql)
            databases = [self.__cursor.fetchall()]
            databases_li = re.findall("\'(.*?)\'", str(databases))
            if database_name not in databases_li:
                self.bools = 0
        except Exception as e:
            print(e)
            traceback.print_exc()
        return self.bools

    def table_exist(self, table_name, database_name=None):
        '''判断是否存在表'''
        bools = 1
        if self.__database is not None:
            database_name = self.__database
        if database_name is None:
            err = 'Missing database'
            return err
        try:
            self.connect_sql(database_name)
            sql = "show tables"
            self.__cursor.execute(sql)
            tables = [self.__cursor.fetchall()]
            table_li = re.findall("\'(.*?)\'", str(tables))
            if table_name not in table_li:
                bools = 0
        except Exception as e:
            print(e)
            traceback.print_exc()
        return bools

    def create_database(self, database_name):
        '''创建数据库'''
        try:
            # self.connect_sql()
            if self.database_exit(database_name) != 1:
                sql = "create database %s charset utf8mb4" % database_name
                self.__cursor.execute(sql)
        except Exception as e:
            traceback.print_exc()

    def create_table(self, table_name, cretae_table_str):
        '''创建表'''
        try:
            if self.table_exit(table_name) != 1:
                self.__cursor.execute(cretae_table_str)
        except Exception as e:
            traceback.print_exc()

    def del_table(self, table_name):
        '''删除表'''
        try:
            if self.table_exist(table_name):
                self.__cursor.execute("drop table if exists %s" % table_name)
            else:
                return 1
        except Exception as e:
            traceback.print_exc()

    def del_database(self, database_name):
        '''删除数据库'''
        try:
            if self.database_exist(database_name):
                self.__cursor.execute("drop database if exists %s " % database_name)
            else:
                return 1
        except Exception as e:
            traceback.print_exc()
