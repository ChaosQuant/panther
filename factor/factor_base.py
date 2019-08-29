#!/usr/bin/env python
# coding=utf-8
import os
import sys
import numpy as np
import pandas as pd
import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker

sys.path.append('..')
from factor.utillities.trade_date import TradeDate
from factor import factor_config


class FactorBase(object):
    def __init__(self, name):
        destination_db = '''mysql+mysqlconnector://{0}:{1}@{2}:{3}/{4}'''.format(factor_config.destination_db_user,
                                                                                 factor_config.destination_db_pwd,
                                                                                 factor_config.destination_db_host,
                                                                                 factor_config.destination_db_port,
                                                                                 factor_config.destination_db_database)
        self._name = name
        self._destination = sa.create_engine(destination_db)
        self._dest_session = sessionmaker(bind=self._destination, autocommit=False, autoflush=True)
        self._trade_date = TradeDate()
        self._dir = factor_config.RECORD_BASE_DIR + 'factor/' + str(self._name)

    def __del__(self):
        session = self._dest_session()
        if session:
            session.close()

    def _create_index(self):
        session = self._dest_session()
        indexs = [
            '''CREATE INDEX {0}_trade_date_symbol_index ON `{0}` (trade_date, symbol);'''.format(self._name)
        ]
        for sql in indexs:
            session.execute(sql)
        session.commit()
        session.close()

    def _create_tables(self, create_sql, drop_sql):
        session = self._dest_session()
        if drop_sql is not None:
            session.execute(drop_sql)
        session.execute(create_sql)
        session.commit()
        session.close()
        self._create_index()

    def _storage_data(self, data_flow, trade_date):

        data_flow = data_flow.where(pd.notnull(data_flow), None)
        data_flow = data_flow.replace([-np.inf, np.inf], 0).fillna(value=0)
        # 保存本地
        if not os.path.exists(self._dir):
            os.makedirs(self._dir)
        file_name = self._dir + '/' + str(trade_date) + '.csv'
        if os.path.exists(str(file_name)):
            os.remove(str(file_name))
        data_flow.to_csv(file_name, encoding='UTF-8')
        try:
            self.delete_trade_data(trade_date)
            data_flow.to_sql(name=self._name, con=self._destination, if_exists='append', index=False)
        except Exception as e:
            # print(e.orig.msg)
            print(e)
            self.insert_or_update(data_flow)

    def delete_trade_data(self, trade_date):
        session = self._dest_session()
        session.execute('''delete from `{0}` where trade_date={1}'''.format(self._name, trade_date))
        session.commit()

    def insert_or_update(self, datas):
        session = self._dest_session()
        for i in range(datas.shape[0]):
            data = datas.iloc[i]
            values = ''
            update = ''
            title = ''
            for j in range(len(data)):
                index = data.index[j]
                value = str(data[j]).replace("'", "\\'")
                title += """`{0}`,""".format(index)
                values += """'{0}',""".format(value)
                update += """`{0}`='{1}',""".format(index, value)

            sql = '''insert into {0} ({1}) values({2}) ON DUPLICATE KEY UPDATE {3}'''.format(self._name,
                                                                                             title[0:-1],
                                                                                             values[0:-1],
                                                                                             update[0:-1]
                                                                                             )
            sql = sql.replace("'nan'", 'Null').replace("'None'", 'Null')
            session.execute(sql)
        session.commit()
        session.close()
