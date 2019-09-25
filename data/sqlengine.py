#! /usr/bin/env python
# -*- coding: utf-8 -*-

"""
@version: ??
@author: zzh
@file: sqlengine.py
@time: 2019-08-29 16:22
"""
from datetime import datetime
from typing import Iterable
from typing import List
import pandas as pd
import sqlalchemy as sa
import sqlalchemy.orm as orm
from sqlalchemy.ext.declarative import declarative_base
import sys

sys.path.append('../')
sys.path.append('../../')
sys.path.append('../../../')
from data.model import BalanceMRQ, BalanceTTM, IndicatorReport, IndicatorMRQ
from utilities.internal_code import InternalCode
from utilities.sync_util import SyncUtil

import config


class sqlEngine(object):
    def __init__(self):
        db = '''mysql+mysqlconnector://{0}:{1}@{2}:{3}/{4}'''.format(config.pre_db_user,
                                                                     config.pre_db_pwd,
                                                                     config.pre_db_host,
                                                                     config.pre_db_port,
                                                                     config.pre_db_database)
        self.engine = sa.create_engine(db)
        self.session = self.create_session()
        self._sync_util = SyncUtil()
        self._internal = InternalCode()

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

    def deal_fundamentals_old(self, dates, fundamentals_sets):
        deal_df = pd.DataFrame()
        for trade_date in dates:
            report_date = self._sync_util.get_before_report_date(trade_date, 2)
            trade_date = datetime.strptime(str(trade_date), '%Y%m%d').date()
            report_date = datetime.strptime(str(report_date), '%Y%m%d').date()
            trades_date_fundamentals = fundamentals_sets[
                (fundamentals_sets['ENDDATE'] >= report_date) & (
                        fundamentals_sets['PUBLISHDATE'] <= trade_date)]
            trades_date_fundamentals.sort_values(by='ENDDATE', ascending=False, inplace=True)
            trades_date_fundamentals.drop_duplicates(subset=['COMPCODE'], keep='first', inplace=True)
            trades_date_fundamentals['trade_date'] = trade_date
            trades_date_fundamentals = trades_date_fundamentals.dropna(how='all')
            deal_df = deal_df.append(trades_date_fundamentals)
        deal_df.reset_index(drop=True, inplace=True)
        return deal_df

    def deal_fundamentals(self, dates, fundamentals_sets, db_name):
        pub_date_str = db_name.__pit_column__['pub_date'].name
        filter_date_str = db_name.__pit_column__['filter_date'].name
        index_str = db_name.__pit_column__['index'].name
        deal_df = pd.DataFrame()
        for trade_date in dates:
            report_date = self._sync_util.get_before_report_date(trade_date, 2)
            trade_date = datetime.strptime(str(trade_date), '%Y%m%d').date()
            filter_date = datetime.strptime(str(report_date), '%Y%m%d').date()
            trades_date_fundamentals = fundamentals_sets[
                (fundamentals_sets[filter_date_str] >= filter_date) & (
                        fundamentals_sets[pub_date_str] <= trade_date)]
            trades_date_fundamentals.sort_values(by=filter_date_str, ascending=False, inplace=True)
            trades_date_fundamentals.drop_duplicates(subset=[index_str], keep='first', inplace=True)
            trades_date_fundamentals['trade_date'] = trade_date
            trades_date_fundamentals = trades_date_fundamentals.dropna(how='all')
            deal_df = deal_df.append(trades_date_fundamentals)
        deal_df.reset_index(drop=True, inplace=True)
        return deal_df

    def fetch_fundamentals(self,
                           db_name: declarative_base(),
                           db_entities: List = None,
                           db_filters: List = None):
        query = self.session.query(*db_entities, db_name.PUBLISHDATE, db_name.ENDDATE).filter(db_name.REPORTTYPE == 1)
        if db_filters is not None:
            query = query.filter(*db_filters)
        result_list = pd.read_sql(query.statement, self.session.bind)
        return result_list

    def fetch_fundamentals_pit_old(self,
                                   db_name: declarative_base(),
                                   db_entities: List = None,
                                   db_filters: List = None,
                                   dates: Iterable[str] = None):
        dates = list(set(dates))
        dates.sort()
        report_date = self._sync_util.get_before_report_date(dates[0], 2)
        query = self.session.query(*db_entities, db_name.PUBLISHDATE, db_name.ENDDATE).filter(
            db_name.PUBLISHDATE <= dates[-1], db_name.ENDDATE >= report_date, db_name.REPORTTYPE == 1
        )
        if db_filters is not None:
            query = query.filter(*db_filters)
        df = pd.DataFrame()
        result_list = pd.read_sql(query.statement, self.session.bind)
        if not result_list.empty:
            df = self.deal_fundamentals_old(dates, result_list)
        return df

    def fetch_fundamentals_pit(self,
                               db_name: declarative_base(),
                               db_entities: List = None,
                               db_filters: List = None,
                               dates: Iterable[str] = None):
        dates = list(set(dates))
        dates.sort()
        report_date = self._sync_util.get_before_report_date(dates[0], 2)
        query = self.session.query(*db_entities, db_name.__pit_column__['pub_date'],
                                   db_name.__pit_column__['filter_date']).filter(
            db_name.__pit_column__['pub_date'] <= dates[-1], db_name.__pit_column__['filter_date'] >= report_date,
            db_name.REPORTTYPE == 1
        )
        if db_filters is not None:
            query = query.filter(*db_filters)
        df = pd.DataFrame()
        result_list = pd.read_sql(query.statement, self.session.bind)
        if not result_list.empty:
            df = self.deal_fundamentals(dates, result_list, db_name)
        return df

    def fetch_fundamentals_pit_extend_company_id(self, db_name: declarative_base(),
                                                 db_entities: List = None,
                                                 db_filters: List = None,
                                                 dates: Iterable[str] = None):
        db_entities.append(db_name.COMPCODE)
        df = self.fetch_fundamentals_pit(db_name, db_entities, db_filters, dates)
        df = self._internal.join_internal_code(df, left=['trade_date', 'COMPCODE'], right=['trade_date', 'company_id'])
        return df


if __name__ == '__main__':
    internal = InternalCode()
    engine = sqlEngine()
    a = engine.fetch_fundamentals_pit(BalanceMRQ, [BalanceMRQ.COMPCODE,
                                                   BalanceMRQ.PUBLISHDATE,
                                                   BalanceMRQ.ENDDATE],
                                      # [IndicatorReport.PUBLISHDATE <= '20190801'],
                                      dates=['20190822', '20190818'])
    print(a)
    # 内码转换
    df = internal.join_internal_code(a, left=['trade_date', 'COMPCODE'], right=['trade_date', 'company_id'])
    print(df)
    df = engine.fetch_fundamentals_pit_extend_company_id(IndicatorMRQ, [IndicatorMRQ.COMPCODE,
                                                                        IndicatorMRQ.PUBLISHDATE,
                                                                        IndicatorMRQ.DIVCOVER,
                                                                        IndicatorMRQ.ROE,
                                                                        IndicatorMRQ.ENDDATE],
                                                         # [IndicatorReport.PUBLISHDATE <= '20190801'],
                                                         dates=['20190822', '20190818'])
    print(df)
