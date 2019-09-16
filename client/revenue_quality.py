#! /usr/bin/env python
# -*- coding: utf-8 -*-

"""
@version: ??
@author: li
@file: revenue_quality.py
@time: 2019-09-04 14:36
"""


import sys
sys.path.append('../')
sys.path.append('../../')
sys.path.append('../../../')
import time
import collections
import argparse
import pandas as pd
from datetime import datetime, timedelta
from factor import factor_revenue_quality
from client.dbmodel.model import BalanceMRQ, BalanceTTM, BalanceReport
from client.dbmodel.model import CashFlowMRQ, CashFlowTTM, CashFlowReport
from client.dbmodel.model import IndicatorReport, IndicatorMRQ, IndicatorTTM
from client.dbmodel.model import IncomeMRQ, IncomeReport, IncomeTTM
from vision.vision.file_unit.valuation import Valuation
from vision.vision.db.signletion_engine import *

from client.engines.sqlengine import sqlEngine
from client.utillities.sync_util import SyncUtil
# from ultron.cluster.invoke.cache_data import cache_data
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


def get_trade_date(trade_date, n):
    """
    获取当前时间前n年的时间点，且为交易日，如果非交易日，则往前提取最近的一天。
    :param trade_date: 当前交易日
    :param n:
    :return:
    """
    syn_util = SyncUtil()
    trade_date_sets = syn_util.get_all_trades('001002', '19900101', trade_date)
    trade_date_sets = trade_date_sets['TRADEDATE'].values

    time_array = datetime.strptime(str(trade_date), "%Y%m%d")
    time_array = time_array - timedelta(days=365) * n
    date_time = int(datetime.strftime(time_array, "%Y%m%d"))
    if str(date_time) < min(trade_date_sets):
        # print('date_time %s is out of trade_date_sets' % date_time)
        return str(date_time)
    else:
        while str(date_time) not in trade_date_sets:
            date_time = date_time - 1
        # print('trade_date pre %s year %s' % (n, date_time))
        return str(date_time)


def get_basic_data(trade_date):
    # 读取目前涉及到的因子
    maplist = {
        # cash flow
        'BIZNETCFLOW': 'net_operate_cash_flow',  # 经营活动产生的现金流量净额
        # income
        'TOTPROFIT': 'total_profit',  # 利润总额
        'NONOREVE': 'non_operating_revenue',  # 营业外收入
        'NONOEXPE': 'non_operating_expense',  # 营业外支出
        'BIZTOTCOST': 'total_operating_cost',  # 营业总成本
        'BIZTOTINCO': 'total_operating_revenue',  # 营业总收入
        'PERPROFIT': 'operating_profit', # 营业利润
        'NETPROFIT': 'net_profit',  # 净利润
        'BIZINCO': 'operating_revenue',  # 营业收入
        # balance
        'TOTALCURRLIAB': 'total_current_liability',  # 流动负债合计
        }

    engine = sqlEngine()
    columns = ['COMPCODE', 'PUBLISHDATE', 'ENDDATE', 'symbol', 'company_id', 'trade_date']

    # Report Data
    cash_flow_sets = engine.fetch_fundamentals_pit_extend_company_id(CashFlowReport,
                                                                     [CashFlowReport.BIZNETCFLOW,
                                                                      ],
                                                                     dates=[trade_date]).drop(columns, axis=1)
    cash_flow_sets = cash_flow_sets.rename(columns={'BIZNETCFLOW': 'net_operate_cash_flow',  # 经营活动产生的现金流量净额
                                                    })

    income_sets = engine.fetch_fundamentals_pit_extend_company_id(IncomeReport,
                                                                  [IncomeReport.TOTPROFIT,
                                                                   IncomeReport.NONOREVE,
                                                                   IncomeReport.NONOEXPE,
                                                                   IncomeReport.BIZTOTCOST,
                                                                   IncomeReport.BIZTOTINCO,
                                                                   ],
                                                                  dates=[trade_date]).drop(columns, axis=1)
    income_sets = income_sets.rename(columns={'TOTPROFIT': 'total_profit',  # 利润总额
                                              'NONOREVE': 'non_operating_revenue',  # 营业外收入
                                              'NONOEXPE': 'non_operating_expense',  # 营业外支出
                                              'BIZTOTCOST': 'total_operating_cost',  # 营业总成本
                                              'BIZTOTINCO': 'total_operating_revenue',  # 营业总收入
                                               })

    balance_sets = engine.fetch_fundamentals_pit_extend_company_id(BalanceReport,
                                                                   [BalanceReport.TOTALCURRLIAB
                                                                    ],
                                                                   dates=[trade_date]).drop(columns, axis=1)
    balance_sets = balance_sets.rename(columns={'TOTALCURRLIAB': 'total_current_liability',  # 流动负债合计
                                                })

    trade_date_pre_year = get_trade_date(trade_date, 1)
    trade_date_pre_year_2 = get_trade_date(trade_date, 2)
    trade_date_pre_year_3 = get_trade_date(trade_date, 3)
    trade_date_pre_year_4 = get_trade_date(trade_date, 4)

    income_con_sets = engine.fetch_fundamentals_pit_extend_company_id(IncomeReport,
                                                                      [IncomeReport.NETPROFIT,
                                                                       ],
                                                                         dates=[trade_date,
                                                                                trade_date_pre_year,
                                                                                trade_date_pre_year_2,
                                                                                trade_date_pre_year_3,
                                                                                trade_date_pre_year_4,
                                                                                ]).drop(columns, axis=1)
    income_con_sets = income_con_sets.groupby(['security_code'])
    income_con_sets = income_con_sets.sum()
    income_con_sets = income_con_sets.rename(columns={'NETPROFIT': 'net_profit_5'})

    tp_revenue_quanlity = pd.merge(cash_flow_sets, income_sets, on='security_code')
    tp_revenue_quanlity = pd.merge(balance_sets, tp_revenue_quanlity, on='security_code')
    tp_revenue_quanlity = pd.merge(income_con_sets, tp_revenue_quanlity, on='security_code')

    # TTM Data
    cash_flow_ttm_sets = engine.fetch_fundamentals_pit_extend_company_id(CashFlowTTM,
                                                                         [CashFlowTTM.BIZNETCFLOW,
                                                                          ],
                                                                         dates=[trade_date]).drop(columns, axis=1)
    cash_flow_ttm_sets = cash_flow_ttm_sets.rename(
        columns={'BIZNETCFLOW': 'net_operate_cash_flow',  # 经营活动产生的现金流量净额
                 })

    income_ttm_sets = engine.fetch_fundamentals_pit_extend_company_id(IncomeTTM,
                                                                      [IncomeTTM.TOTPROFIT,
                                                                       IncomeTTM.NONOREVE,
                                                                       IncomeTTM.NONOEXPE,
                                                                       IncomeTTM.BIZTOTCOST,
                                                                       IncomeTTM.BIZTOTINCO,
                                                                       IncomeTTM.PERPROFIT,
                                                                       IncomeTTM.NETPROFIT,
                                                                       IncomeTTM.BIZINCO,
                                                                       ], dates=[trade_date]).drop(columns, axis=1)
    income_ttm_sets = income_ttm_sets.rename(
        columns={'TOTPROFIT': 'total_profit',  # 利润总额
                 'NONOREVE': 'non_operating_revenue',  # 营业外收入
                 'NONOEXPE': 'non_operating_expense',  # 营业外支出
                 'BIZTOTCOST': 'total_operating_cost',  # 营业总成本
                 'BIZTOTINCO': 'total_operating_revenue',  # 营业总收入
                 'PERPROFIT': 'operating_profit',  # 营业利润
                 'NETPROFIT': 'net_profit',  # 净利润
                 'BIZINCO': 'operating_revenue',  # 营业收入
                 })

    balance_ttm_sets = engine.fetch_fundamentals_pit_extend_company_id(BalanceTTM,
                                                                       [BalanceTTM.TOTALCURRLIAB
                                                                        ],
                                                                       dates=[trade_date]).drop(columns, axis=1)
    balance_ttm_sets = balance_ttm_sets.rename(
        columns={'TOTALCURRLIAB': 'total_current_liability',  # 流动负债合计
                 })

    column = ['trade_date']
    valuation_sets = get_fundamentals(query(Valuation.security_code,
                                            Valuation.trade_date,
                                            Valuation.market_cap,)
                                      .filter(Valuation.trade_date.in_([trade_date]))).drop(column, axis=1)

    indicator_ttm_sets = engine.fetch_fundamentals_pit_extend_company_id(IndicatorTTM,
                                                                         [IndicatorTTM.NVALCHGITOTP,
                                                                          ], dates=[trade_date]).drop(columns, axis=1)

    ttm_revenue_quanlity = pd.merge(cash_flow_ttm_sets, income_ttm_sets, on='security_code')
    ttm_revenue_quanlity = pd.merge(balance_ttm_sets, ttm_revenue_quanlity, on='security_code')
    ttm_revenue_quanlity = pd.merge(valuation_sets, ttm_revenue_quanlity, on='security_code')
    ttm_revenue_quanlity = pd.merge(indicator_ttm_sets, ttm_revenue_quanlity, on='security_code')

    valuation_con_sets = get_fundamentals(query(Valuation.security_code,
                                                Valuation.trade_date,
                                                Valuation.market_cap,
                                                Valuation.circulating_market_cap,
                                                )
                                          .filter(Valuation.trade_date.in_([trade_date,
                                                                        trade_date_pre_year,
                                                                        trade_date_pre_year_2,
                                                                        trade_date_pre_year_3,
                                                                        trade_date_pre_year_4]))).drop(column, axis=1)

    valuation_con_sets = valuation_con_sets.groupby(['security_code'])
    valuation_con_sets = valuation_con_sets.sum()
    valuation_con_sets = valuation_con_sets.rename(columns={'market_cap': 'market_cap_5',
                                                            'circulating_market_cap': 'circulating_market_cap_5'
                                                            })
    tp_revenue_quanlity = pd.merge(valuation_con_sets, tp_revenue_quanlity, on='security_code')

    return tp_revenue_quanlity, ttm_revenue_quanlity


def prepare_calculate_local(trade_date, factor_name):
    # local
    tic = time.time()
    tp_revenue_quanlity, ttm_revenue_quanlity = get_basic_data(trade_date)
    if len(tp_revenue_quanlity) <= 0 or len(ttm_revenue_quanlity) <= 0:
        print("%s has no data" % trade_date)
        return
    else:
        factor_revenue_quality.calculate(trade_date, tp_revenue_quanlity, ttm_revenue_quanlity, factor_name)
    time6 = time.time()
    print('earning_cal_time:{}'.format(time6 - tic))


def prepare_calculate_remote(trade_date):
    # remote
    tp_revenue_quanlity, ttm_revenue_quanlity = get_basic_data(trade_date)
    if len(tp_revenue_quanlity) <= 0 or len(ttm_revenue_quanlity) <= 0:
        print("%s has no data" % trade_date)
        return
    else:
        tic = time.time()
        session = str(int(time.time() * 1000000 + datetime.now().microsecond))
        cache_data.set_cache(session + str(trade_date) + "1", trade_date, tp_revenue_quanlity.to_json(orient='records'))
        cache_data.set_cache(session + str(trade_date) + "2", trade_date, ttm_revenue_quanlity.to_json(orient='records'))
        factor_revenue_quality.factor_calculate.delay(date_index=trade_date, session=session)
        time6 = time.time()
        print('earning_cal_time:{}'.format(time6 - tic))


def do_update(start_date, end_date, count, factor_name):
    # 读取本地交易日
    syn_util = SyncUtil()
    trade_date_sets = syn_util.get_trades_ago('001002', start_date, end_date, count, order='DESC')
    trade_date_sets = trade_date_sets['TRADEDATE'].values
    for trade_date in trade_date_sets:
        print('因子计算日期： %s' % trade_date)
        prepare_calculate_local(trade_date, factor_name)
    print('----->')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--start_date', type=int, default=20070101)
    parser.add_argument('--end_date', type=int, default=0)
    parser.add_argument('--count', type=int, default=1)
    parser.add_argument('--rebuild', type=bool, default=False)
    parser.add_argument('--update', type=bool, default=False)
    parser.add_argument('--schedule', type=bool, default=False)

    factor_name = 'factor_revenue'

    args = parser.parse_args()
    if args.end_date == 0:
        end_date = int(datetime.now().date().strftime('%Y%m%d'))
    else:
        end_date = args.end_date
    if args.rebuild:
        processor = factor_revenue_quality.RevenueQuality(factor_name)
        processor.create_dest_tables()
        do_update(args.start_date, end_date, args.count, factor_name)
    if args.update:
        do_update(args.start_date, end_date, args.count, factor_name)
    # do_update('20190819', '20190823', 10)