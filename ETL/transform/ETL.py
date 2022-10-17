import requests
import pandas as pd
from datetime import datetime, timedelta
from io import BytesIO
import time
import json
from typing import NamedTuple
from ETL.common.mysql import MysqlConnector
import logging

class DBSourceConfig(NamedTuple):
    """
    Class for source configuration data

    src_first_extract_date: determines the date for extracting the source
    """
    host: str
    user: str
    password: str
    database: str
    trg_table: str
    id_sql: str


class WebConfig(NamedTuple):
    """
    Class for source configuration data

    src_first_extract_date: determines the date for extracting the source
    """
    headers: str
    income_url: str
    balance_url: str
    cash_url: str
    # Type can be{'Q4': yearly, 'all': all data, 'Q1'} 
    type: str
    id_detail: str
    count: int
    report_period: str


class FinanceETL():
    def __init__(self,  mysqlconnector: MysqlConnector, src_web_args: WebConfig, db_args:DBSourceConfig):
        self._logger = logging.getLogger(__name__)
        self.mysql = mysqlconnector
        self.src_web_arg = src_web_args
        self.db_args = db_args
        self.id_list = self.mysql.extract_id(self.db_args.id_sql)

    @staticmethod
    def needed_data(list_data):
        def split_to_two(data):
            a,b = [],[]
            for i in data:
                a.append(i[0])
                b.append(i[1])
            return a,b

        num = list_data.shape[1]
        for name in list_data.columns:
            if isinstance(list_data[name][0],str):
                continue
            try:
                len(list_data[name][0])
                a,b = split_to_two(list_data[name])
                use_name = f'{name}_rate'
                df = pd.DataFrame({name:a,use_name:b})
                list_data = pd.concat([list_data,df],axis=1)
            except TypeError:
                continue
        new = list_data.iloc[:,num:]
        new = pd.concat([list_data.iloc[:,1],new],axis=1)
        return new


    def extract(self, url, id):
        """
        Extracting data from website

        :param: id -> the stock id
        """
        timestamp = int(time.time()*1000)

        param = {
        'symbol': id,
        'type': self.src_web_arg.type,
        'is_detail': 'true',
        'count': self.src_web_arg.count,
        'timestamp': timestamp
        }
        r = requests.get(url, headers = self.src_web_arg.headers, params = param)
        self._logger.info(f'Extracting status code is {r.status_code}')
        return {id: json.loads(r.text)}



    def update_current(self, r, id):
        if r['id']['last_report_name']:
            pass

    def transform_json(self, dic, id):
        json_data = pd.DataFrame.from_dict(dic['data'])
        list_data = pd.DataFrame.from_dict(dic['data']['list'])
        data = self.needed_data(list_data)
        table = pd.concat([json_data.iloc[:,[0,1,5]], data],axis=1)
        table['stock_id'] = id
        return table
    

    def income_transform(self, dic):
        self._logger.info('Transforming data')
        data = pd.concat([self.transform_json(dic[id], self.id_list[id]) for id in self.id_list], axis = 0)
        data['year'] = data.report_name.str[:4]
        data['type'] = data.report_name.str[4:]
        data.drop('report_name',axis=1, inplace=True)
        return data

    
    def load(self, data):
        self._logger.info('Loading data to database')
        self.mysql.load_to_db(self.db_args.trg_table, data)
        print('已存入')
    

    def income_ETL(self):
        self._logger.info('Starting ETL process')
        dic = {}
        self._logger.info('Extracting income data from website')
        for id in self.id_list.keys():
            dicnew = self.extract(self.src_web_arg.income_url, id)
            dic = dict(dicnew, **dic)

        self._logger.info('Saving data to local file')
        # f = open('data.json', 'wb')

        json_object = json.dumps(dic, indent=4)

        # Writing to sample.json
        with open("data.json", "w") as outfile:
            outfile.write(json_object)

        table = self.income_transform(dic)
        self.load(table)
        return True