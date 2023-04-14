import psycopg2
import pymysql
import pandas as pd
from sqlalchemy import create_engine


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

    def db_conn(self):
        raise BaseException('请创建数据库链接')

    def pd2db_conn(self):
        return create_engine(
            f'{self.db_engine}://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_database}')


class DevConfigWinLocal(Config):
    db_engine = 'postgresql+psycopg2'
    db_host = '127.0.0.1'
    db_port = '5432'

    def db_conn(self):
        return psycopg2.connect(database=self.db_database, user=self.db_user, password=self.db_password,
                                host=self.db_host, port=self.db_port)


config = DevConfigWinLocal()
