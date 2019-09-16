#! /usr/bin/env python
# -*- coding: utf-8 -*-

"""
@version: ??
@author: li
@file: pre_share_indicators.py
@time: 2019-09-03 22:45
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
from factor import factor_per_share_indicators
from client.engines.sqlengine import sqlEngine


from client.dbmodel.model import BalanceMRQ, BalanceTTM, BalanceReport
from client.dbmodel.model import CashFlowMRQ, CashFlowTTM, CashFlowReport
from client.dbmodel.model import IndicatorReport, IndicatorMRQ, IndicatorTTM
from client.dbmodel.model import IncomeMRQ, IncomeReport, IncomeTTM
from vision.vision.db.signletion_engine import *
from vision.vision.table.valuation import Valuation

from client.utillities.sync_util import SyncUtil
from ultron.cluster.invoke.cache_data import cache_data
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
    """
    获取基础数据
    按天获取当天交易日所有股票的基础数据
    :param trade_date: 交易日
    :return:
    """
    engine = sqlEngine()
    maplist = {'MANANETR': 'net_operate_cash_flow',  # 经营活动现金流量净额
               'FINALCASHBALA': 'cash_and_equivalents_at_end',  # 期末现金及现金等价物余额
               'CASHNETI': 'cash_equivalent_increase',  # 现金及现金等价物净增加额

               'DILUTEDEPS': 'diluted_eps',   # 稀释每股收益
               'NETPROFIT': 'net_profit',  # 净利润1
               'BIZINCO': 'operating_revenue',  # 营业收入
               'PERPROFIT':'operating_profit',  # 营业利润
               'BIZTOTINCO': 'total_operating_revenue',  # 营业总收入
               'PARENETP': 'np_parent_company_owners',  # 归属于母公司所有者的净利润

               'CAPISURP': 'capital_reserve_fund',  # 资本公积
               'RESE': 'surplus_reserve_fund',  # 盈余公积
               'UNDIPROF': 'retained_profit',  # 未分配利润
               'PARESHARRIGH': 'total_owner_equities',  # 归属于母公司的所有者权益

               'FCFE': 'shareholder_fcfps',   # 股东自由现金流量
               'FCFF': 'enterprise_fcfps',   # 企业自由现金流量
               'EPSBASIC': 'basic_eps',  # 基本每股收益
               'DPS': 'dividend_receivable',  # 每股股利（税前）  每股普通股股利

               'capitalization':'capitalization', # 总股本
               }
    columns = ['COMPCODE', 'PUBLISHDATE', 'ENDDATE', 'symbol', 'company_id', 'trade_date']
    # Report data
    cash_flow_sets = engine.fetch_fundamentals_pit_extend_company_id(CashFlowReport,
                                                                     [CashFlowReport.FINALCASHBALA,  # 期末现金及现金等价物余额
                                                                      ],
                                                                     dates=[trade_date]).drop(columns, axis=1)

    cash_flow_sets = cash_flow_sets.rename(columns={'FINALCASHBALA': 'cash_and_equivalents_at_end',  # 期末现金及现金等价物余额
                                                    })

    income_sets = engine.fetch_fundamentals_pit_extend_company_id(IncomeReport,
                                                                  [IncomeReport.BIZINCO,     # 营业收入
                                                                   IncomeReport.BIZTOTINCO,  # 营业总收入
                                                                   IncomeReport.PERPROFIT,   # 营业利润
                                                                   IncomeReport.DILUTEDEPS,  # 稀释每股收益
                                                                   ],
                                                                  dates=[trade_date]).drop(columns, axis=1)

    income_sets = income_sets.rename(columns={'BIZINCO': 'operating_revenue',  # 营业收入
                                              'BIZTOTINCO': 'total_operating_revenue',  # 营业总收入
                                              'PERPROFIT': 'operating_profit',  # 营业利润
                                              'DILUTEDEPS': 'diluted_eps',      # 稀释每股收益
                                              })

    balance_sets = engine.fetch_fundamentals_pit_extend_company_id(BalanceReport,
                                                                   [BalanceReport.PARESHARRIGH,  # 归属于母公司的所有者权益
                                                                    BalanceReport.CAPISURP,
                                                                    BalanceReport.RESE,
                                                                    BalanceReport.UNDIPROF,
                                                                    ],
                                                                   dates=[trade_date]).drop(columns, axis=1)
    balance_sets = balance_sets.rename(columns={'PARESHARRIGH': 'total_owner_equities',  # 归属于母公司的所有者权益
                                                'CAPISURP': 'capital_reserve_fund',  # 资本公积
                                                'RESE': 'surplus_reserve_fund',  # 盈余公积
                                                'UNDIPROF': 'retained_profit',  # 未分配利润
                                                })

    indicator_sets = engine.fetch_fundamentals_pit_extend_company_id(IndicatorReport,
                                                                     [IndicatorReport.FCFE,  # 股东自由现金流量
                                                                      IndicatorReport.FCFF,  # 企业自由现金流量
                                                                      IndicatorReport.EPSBASIC,  # 基本每股收益
                                                                      IndicatorReport.DPS,  # 每股股利（税前）
                                                                      ],
                                                                     dates=[trade_date]).drop(columns, axis=1)
    indicator_sets = indicator_sets.rename(columns={'FCFE': 'shareholder_fcfps',   # 股东自由现金流量
                                                    'FCFF': 'enterprise_fcfps',   # 企业自由现金流量
                                                    'EPSBASIC': 'basic_eps',  # 基本每股收益
                                                    'DPS': 'dividend_receivable',  # 每股股利（税前）
                                                    })

    # TTM data
    cash_flow_ttm_sets = engine.fetch_fundamentals_pit_extend_company_id(CashFlowTTM,
                                                                         [CashFlowTTM.CASHNETI,  # 现金及现金等价物净增加额
                                                                          CashFlowTTM.MANANETR,  # 经营活动现金流量净额
                                                                          ],
                                                                         dates=[trade_date]).drop(columns, axis=1)

    cash_flow_ttm_sets = cash_flow_ttm_sets.rename(columns={'CASHNETI': 'cash_equivalent_increase_ttm',  # 现金及现金等价物净增加额
                                                            'MANANETR': 'net_operate_cash_flow_ttm',  # 经营活动现金流量净额
                                                            })

    income_ttm_sets = engine.fetch_fundamentals_pit_extend_company_id(IncomeTTM,
                                                                      [IncomeTTM.PARENETP,    # 归属于母公司所有者的净利润
                                                                       IncomeTTM.PERPROFIT,   # 营业利润
                                                                       IncomeTTM.BIZINCO,     # 营业收入
                                                                       IncomeTTM.BIZTOTINCO,  # 营业总收入
                                                                       ],
                                                                      dates=[trade_date]).drop(columns, axis=1)

    income_ttm_sets = income_ttm_sets.rename(columns={'PARENETP': 'np_parent_company_owners_ttm',  # 归属于母公司所有者的净利润
                                                      'PERPROFIT': 'operating_profit_ttm',  # 营业利润
                                                      'BIZINCO': 'operating_revenue_ttm',  # 营业收入
                                                      'BIZTOTINCO': 'total_operating_revenue_ttm',  # 营业总收入
                                                      })

    column = ['trade_date']
    valuation_data = get_fundamentals(query(Valuation.security_code,
                                            Valuation.trade_date,
                                            Valuation.capitalization,).filter(Valuation.trade_date.in_([trade_date]))).drop(column, axis=1)

    valuation_sets = pd.merge(cash_flow_sets, income_sets, on='security_code').reindex()
    valuation_sets = pd.merge(balance_sets, valuation_sets, on='security_code').reindex()
    valuation_sets = pd.merge(indicator_sets, valuation_sets, on='security_code').reindex()
    valuation_sets = pd.merge(cash_flow_ttm_sets, valuation_sets, on='security_code').reindex()
    valuation_sets = pd.merge(income_ttm_sets, valuation_sets, on='security_code').reindex()
    valuation_sets = pd.merge(valuation_data, valuation_sets, on='security_code').reindex()

    return valuation_sets


def prepare_calculate_local(trade_date):
    # local
    tic = time.time()
    valuation_sets = get_basic_data(trade_date)
    print('len_of_valation: %s' % len(valuation_sets))
    print(valuation_sets.head())
    if len(valuation_sets) <= 0:
        print("%s has no data" % trade_date)
        return
    else:
        factor_per_share_indicators.calculate(trade_date, valuation_sets)
    time3 = time.time()
    print('per_share_cal_time:{}'.format(time3 - tic))


def prepare_calculate_remote(trade_date):
    # remote
    valuation_sets = get_basic_data(trade_date)
    if len(valuation_sets) <= 0:
        print("%s has no data" % trade_date)
        return
    else:
        tic = time.time()
        session = str(int(time.time() * 1000000 + datetime.now().microsecond))
        cache_data.set_cache(session + str(trade_date), trade_date, valuation_sets.to_json(orient='records'))
        factor_per_share_indicators.factor_calculate.delay(date_index=trade_date, session=session)
        time3 = time.time()
        print('per_share_cal_time:{}'.format(time3 - tic))


def do_update(start_date, end_date, count):
    # 读取本地交易日
    syn_util = SyncUtil()
    trade_date_sets = syn_util.get_trades_ago('001002', start_date, end_date, count, order='DESC')
    trade_date_sets = trade_date_sets['TRADEDATE'].values
    for trade_date in trade_date_sets:
        print('因子计算日期： %s' % trade_date)
        prepare_calculate_local(trade_date)
    print('----->')


if __name__ == '__main__':
    # parser = argparse.ArgumentParser()
    # parser.add_argument('--start_date', type=int, default=20070101)
    # parser.add_argument('--end_date', type=int, default=0)
    # parser.add_argument('--count', type=int, default=1)
    # parser.add_argument('--rebuild', type=bool, default=False)
    # parser.add_argument('--update', type=bool, default=False)
    # parser.add_argument('--schedule', type=bool, default=False)
    #
    # args = parser.parse_args()
    # if args.end_date == 0:
    #     end_date = int(datetime.now().date().strftime('%Y%m%d'))
    # else:
    #     end_date = args.end_date
    # if args.rebuild:
    #     processor = factor_per_share_indicators.PerShareIndicators('factor_per_share')
    #     processor.create_dest_tables()
    #     do_update(args.start_date, end_date, args.count)
    # if args.update:
    #     do_update(args.start_date, end_date, args.count)
    do_update('20190819', '20190823', 10)

