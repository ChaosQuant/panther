# -*- coding: utf-8 -*-
import sys
from sqlalchemy import MetaData, create_engine, and_, or_, select
from sqlalchemy.ext.automap import automap_base
from PyFin.api import makeSchedule, BizDayConventions
import pandas as pd

sys.path.append('..')
import config


class FetchRLFactorEngine(object):
    def __init__(self, table_name):
        self._db_url = '''mysql+mysqlconnector://{0}:{1}@{2}:{3}/{4}'''.format(config.rl_db_user,
                                                                               config.rl_db_pwd,
                                                                               config.rl_db_host,
                                                                               config.rl_db_port,
                                                                               config.rl_db_database)
        self._engine = create_engine(self._db_url)
        self._table = self._base_prepare(table_name)

    def _base_prepare(self, table_name):
        Base = automap_base()
        Base.prepare(self._engine, reflect=True)
        table = eval('Base.classes.' + table_name)
        return table

    def fetch_factors(self, begin_date, end_date, freq=None):
        if freq is None:
            query = select([self._table]).where(
                and_(self._table.trade_date >= begin_date,
                     self._table.trade_date <= end_date,
                     ))
        else:
            rebalance_dates = makeSchedule(begin_date, end_date, freq, 'china.sse', BizDayConventions.Preceding)
            query = select([self._table]).where(
                and_(self._table.trade_date.in_(rebalance_dates), ))

        data = pd.read_sql(query, self._engine)

        for col in ['id', 'creat_time', 'update_time']:
            if col in data.columns:
                data = data.drop([col], axis=1)

        return data


if __name__ == "__main__":
    import datetime as dt

    begin = dt.datetime(2018, 8, 1)
    end = dt.datetime(2018, 8, 10)
    tn = 'factor_reversal'

    process = FetchRLFactorEngine(tn)
    data = process.fetch_factors(begin, end, '2b')
    print(data.head())
