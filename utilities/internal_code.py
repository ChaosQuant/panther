#! /usr/bin/env python
# -*- coding: utf-8 -*-

"""
@version: ??
@author: zzh
@file: sqlengine.py
@time: 2019-08-29 16:22
"""

import datetime
import sys

sys.path.append('../..')

import config
import sqlalchemy as sa
import numpy as np
import pandas as pd
from sqlalchemy.orm import sessionmaker


class InternalCode(object):
    def __init__(self):
        source_db = '''mysql+mysqlconnector://{0}:{1}@{2}:{3}/{4}'''.format(config.rl_db_user,
                                                                            config.rl_db_pwd,
                                                                            config.rl_db_host,
                                                                            config.rl_db_port,
                                                                            config.rl_db_database)
        # 源数据库
        self.source = sa.create_engine(source_db)
        # 数据库Session
        self.session = sessionmaker(bind=self.source, autocommit=False, autoflush=True)
        self.internal_code_table = 'gl_internal_code'

    def get_Ashare_internal_code(self, trade_date=None):
        if trade_date == None:
            trade_date = datetime.date.today()
        sql = """select security_code,symbol,exchange,company_id from `{0}`
                    where exchange in (001002,001003) and security_type=101
                     and ((begin_date<='{1}' and end_date>='{1}') 
                     or (begin_date<='{1}' and end_date='19000101'));""".format(
            self.internal_code_table, trade_date)
        result_list = pd.read_sql(sql, self.source)
        if not result_list.empty:
            result_list['symbol'] = np.where(result_list['exchange'] == '001002',
                                             result_list['symbol'] + '.XSHG',
                                             result_list['symbol'] + '.XSHE')
            result_list.drop(['exchange'], axis=1, inplace=True)
        return result_list

    def get_Ashare_internal_code_list(self, trade_dates=[]):
        if len(trade_dates) == 0:
            trade_dates = trade_dates.append(datetime.date.today())
        trade_dates.sort()
        sql = """select security_code,symbol,exchange,company_id,begin_date,end_date from `{0}`
                    where exchange in (001002,001003) and security_type=101
                     and ((begin_date<='{1}' and end_date>'{2}') 
                     or (begin_date<='{1}' and end_date='19000101'));""".format(
            self.internal_code_table, trade_dates[0], trade_dates[-1])
        result_list = pd.read_sql(sql, self.source)
        df = pd.DataFrame()
        if not result_list.empty:
            result_list['symbol'] = np.where(result_list['exchange'] == '001002',
                                             result_list['symbol'] + '.XSHG',
                                             result_list['symbol'] + '.XSHE')
            for trade_date in trade_dates:
                if not isinstance(trade_date, datetime.date):
                    trade_date = datetime.datetime.strptime(str(trade_date), '%Y%m%d').date()
                trade_date_code = result_list[
                    ((result_list['begin_date'] <= trade_date) & (result_list['end_date'] > trade_date)) | (
                            (result_list['begin_date'] <= trade_date) & (
                            result_list['end_date'] == datetime.datetime.strptime('19000101', '%Y%m%d').date()))]
                trade_date_code['trade_date'] = trade_date
                df = df.append(trade_date_code)
        df.drop(columns=['begin_date', 'end_date', 'exchange'], axis=1, inplace=True)
        df.reset_index(drop=True, inplace=True)
        return df

    def join_internal_code(self, df, left=[], right=[]):
        if not df.empty:
            if ('trade_date' in left):
                trade_dates = list(set(df['trade_date']))
                trade_dates.sort()
                internal_code_sets = self.get_Ashare_internal_code_list(trade_dates)
            else:
                internal_code_sets = self.get_Ashare_internal_code()
            df = pd.merge(df, internal_code_sets, left_on=left, right_on=right)
        return df


if __name__ == '__main__':
    internal = InternalCode()
    # sets = internal.get_Ashare_internal_code('20190108')
    # print(sets)
    sets = internal.get_Ashare_internal_code_list(['20190108', '20180101'])
    print(sets)
