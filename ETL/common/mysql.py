'''Connector and methods accessing S3'''
import logging
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.sql import text


class MysqlConnector():
    ''' 
    Class for interacting with Mysql Buckets
    '''
    def __init__(self, host: str, user: str, password: str, database: str):
        ''' 
        Constructor for MysqlConnector

        : param access_key: access key for accessing S3
        : param secret_key: secret key for accessing S3
        : param endpoint_url: endpoint url to S3
        : param bucket: S3 bucket name 
        '''
        self._logger = logging.getLogger(__name__)
        self.engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:3306/{database}", \
                                    encoding='utf-8')

    def execute_sql_queries(self, query):
        '''
        listing all files with prefix on the S3 bucket

        :param query: sql queries to read id from database
        '''
        self._logger.info('Reading sql query')
        df = pd.read_sql(text(query), con=self.engine)
        return df

    def extract_id(self, query):
        self._logger.info('Matching stock id')
        data = self.execute_sql_queries(query)
        dic = {'深A': 'SZ' ,'新三板': 'NQ', '沪A': 'SH'}
        newid = {}
        for i in range(len(data)):
            if data.iloc[i,1] in dic.keys(): id = dic[data.iloc[i,1]] + data.iloc[i,0]
            else: id = data.iloc[i,0]
            newid[id] = data.iloc[i,0]
        return newid

    def load_to_db(self, target_table_name: str, data):
        '''
        listing all files with prefix on the S3 bucket

        :param prefix: prefix on the S3 bucket that should be filtered with

        returns:
            files: list of all file names containing the prefix in the key
        '''
        data.to_sql(target_table_name, con = self.engine, if_exists='replace', index=False)
        return True
    