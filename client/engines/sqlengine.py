#! /usr/bin/env python
# -*- coding: utf-8 -*-

"""
@version: ??
@author: li
@file: sqlengine.py
@time: 2019-08-28 16:22
"""

from typing import Iterable
from typing import List
from typing import Dict
from typing import Tuple
from typing import Union
import pandas as pd
import sqlalchemy as sa
import sqlalchemy.orm as orm
from sqlalchemy import select, and_, outerjoin, join, column
from sqlalchemy.ext.declarative import declarative_base

from client.dbmodel.model import BalanceMRQ, BalanceTTM, IndicatorReport


class sqlEngine(object):
    def __init__(self, db_url: str = None):
        self.engine = sa.create_engine(db_url)
        self.session = self.create_session()

    def __del__(self):
        if self.session:
            self.session.close()

    def create_session(self):
        db_session = orm.sessionmaker(bind=self.engine)
        return db_session()

    def query_dict(self, entities, filters, name=None):
        query_sets = {'entities': entities,
                      'filter': filters,
                      'name': name}
        return query_sets

    def fetch_fundamentals(self,
                           db_name: declarative_base(),
                           db_filters: List = None,
                           start_date=None, end_date=None,
                           dates: Iterable[str] = None):
        query = select(db_filters).where(

            db_name.PUBLISHDATE.in_(dates) if dates else db_name.PUBLISHDATE.between(start_date, end_date)
        )
        return pd.read_sql(query, self.session.bind)


if __name__ == '__main__':
    db_url = 'mysql+mysqlconnector://factor_edit:factor_edit_2019@db1.irongliang.com:3306/pre_data'
    engine = sqlEngine(db_url)

    a = engine.fetch_fundamentals(IndicatorReport, [IndicatorReport.COMPCODE,
                                                    IndicatorReport.PUBLISHDATE,
                                                    IndicatorReport.DIVCOVER,
                                                    IndicatorReport.ROE],
                                  dates=['20190822'])






