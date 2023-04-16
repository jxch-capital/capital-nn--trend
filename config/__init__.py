import psycopg2
import pymysql
import pandas as pd
from sqlalchemy import create_engine
from concurrent.futures import ThreadPoolExecutor


class Config(object):
    encoding = 'utf8'
    db_engine = None
    db_connection = None
    db_host = None
    db_port = None
    db_database = 'capital'
    db_schema = 'nn'
    db_user = 'capital'
    db_password = 'capital'
    thread_pool_size = 33
    __thread_pool = None
    __pd2db_conn = None
    __db_conn = None

    def db_conn(self):
        raise BaseException('请创建数据库链接')

    def pd2db_conn(self):
        if self.__pd2db_conn is None:
            self.__pd2db_conn = create_engine(
                f'{self.db_engine}://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_database}')
        return self.__pd2db_conn

    def get_thread_pool(self):
        if self.__thread_pool is None:
            self.__thread_pool = ThreadPoolExecutor(max_workers=self.thread_pool_size)
        return self.__thread_pool


class DevConfigWinLocal(Config):
    db_engine = 'postgresql+psycopg2'
    db_host = '127.0.0.1'
    db_port = '5432'
    __db_conn = None

    def db_conn(self):
        if self.__db_conn is None:
            self.__db_conn = psycopg2.connect(database=self.db_database, user=self.db_user, password=self.db_password,
                                              host=self.db_host, port=self.db_port)
        return self.__db_conn


config = DevConfigWinLocal()
