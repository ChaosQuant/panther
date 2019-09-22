#! /usr/bin/env python
# -*- coding: utf-8 -*-

"""
@version: ??
@author: li
@file: historical_growth.py
@time: 2019-09-02 13:30
"""
import gc
import sys
sys.path.append('../')
sys.path.append('../../')
sys.path.append('../../../')
import time
import argparse
import pandas as pd
from datetime import datetime, timedelta
from financial import factor_history_growth
from data.model import BalanceReport
from data.model import CashFlowTTM
from data.model import IncomeTTM

from data.sqlengine import sqlEngine
from ultron.cluster.invoke.cache_data import cache_data
from utilities.sync_util import SyncUtil


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
    trade_date_pre_year = get_trade_date(trade_date, 1)
    trade_date_pre_year_2 = get_trade_date(trade_date, 2)
    trade_date_pre_year_3 = get_trade_date(trade_date, 3)
    trade_date_pre_year_4 = get_trade_date(trade_date, 4)
    trade_date_pre_year_5 = get_trade_date(trade_date, 5)
    # print('trade_date %s' % trade_date)
    # print('trade_date_pre_year %s' % trade_date_pre_year)
    # print('trade_date_pre_year_2 %s' % trade_date_pre_year_2)
    # print('trade_date_pre_year_3 %s' % trade_date_pre_year_3)
    # print('trade_date_pre_year_4 %s' % trade_date_pre_year_4)
    # print('trade_date_pre_year_5 %s' % trade_date_pre_year_5)

    engine = sqlEngine()
    maplist = {'MANANETR': 'net_operate_cash_flow',  # 经营活动现金流量净额
               'FINNETCFLOW':'net_finance_cash_flow',    # 筹资活动产生的现金流量净额
               'INVNETCASHFLOW':'net_invest_cash_flow',  # 投资活动产生的现金流量净额

               'BIZINCO': 'operating_revenue',  # 营业收入
               'PERPROFIT': 'operating_profit',  # 营业利润
               'TOTPROFIT': 'total_profit',  # 利润总额
               'NETPROFIT': 'net_profit',  # 净利润
               'BIZCOST': 'operating_cost',   # 营业成本
               'PARENETP': 'np_parent_company_owners',  # 归属于母公司所有者的净利润

               'TOTASSET':'total_assets',      # 资产总计
               'RIGHAGGR':'total_owner_equities',   # 股东权益合计
               }

    columns = ['COMPCODE', 'PUBLISHDATE', 'ENDDATE', 'symbol', 'company_id', 'trade_date']

    # report data
    balance_sets = engine.fetch_fundamentals_pit_extend_company_id(BalanceReport,
                                                                   [BalanceReport.TOTASSET,  # 总资产（资产合计）
                                                                    BalanceReport.RIGHAGGR,  # 股东权益合计
                                                                    ],
                                                                   dates=[trade_date]).drop(columns, axis=1)
    balance_sets = balance_sets.rename(columns={'TOTASSET':'total_assets',      # 资产总计
                                                'RIGHAGGR':'total_owner_equities',   # 股东权益合计
                                                })

    balance_sets_pre_year = engine.fetch_fundamentals_pit_extend_company_id(BalanceReport,
                                                                            [BalanceReport.TOTASSET,  # 总资产（资产合计）
                                                                             BalanceReport.RIGHAGGR,  # 股东权益合计
                                                                             ],
                                                                            dates=[trade_date_pre_year]).drop(columns, axis=1)
    balance_sets_pre_year = balance_sets_pre_year.rename(columns={"TOTASSET": "total_assets_pre_year",
                                                                  "RIGHAGGR": "total_owner_equities_pre_year"})

    balance_sets = pd.merge(balance_sets, balance_sets_pre_year, on='security_code')
    print('get_balabce_sets')

    # ttm 计算
    ttm_factor_sets = engine.fetch_fundamentals_pit_extend_company_id(IncomeTTM,
                                                                      [IncomeTTM.BIZINCO,  # 营业收入
                                                                       IncomeTTM.PERPROFIT,  # 营业利润
                                                                       IncomeTTM.TOTPROFIT,  # 利润总额
                                                                       IncomeTTM.NETPROFIT,  # 净利润
                                                                       IncomeTTM.BIZCOST,  # 营业成本
                                                                       IncomeTTM.PARENETP],  # 归属于母公司所有者的净利润
                                                                      dates=[trade_date]).drop(columns, axis=1)

    ttm_cash_flow_sets = engine.fetch_fundamentals_pit_extend_company_id(CashFlowTTM,
                                                                         [CashFlowTTM.FINNETCFLOW,  # 筹资活动产生的现金流量净额
                                                                          CashFlowTTM.MANANETR,  # 经营活动产生的现金流量净额
                                                                          CashFlowTTM.INVNETCASHFLOW,  # 投资活动产生的现金流量净额
                                                                          CashFlowTTM.CASHNETI,  # 现金及现金等价物的净增加额
                                                                          ],
                                                                         dates=[trade_date]).drop(columns, axis=1)

    field_key = ttm_cash_flow_sets.keys()
    for i in field_key:
        ttm_factor_sets[i] = ttm_cash_flow_sets[i]
    # ttm_factor_sets = pd.merge(ttm_factor_sets, ttm_cash_flow_sets, on='security_code')

    ttm_factor_sets = ttm_factor_sets.rename(
        columns={"BIZINCO": "operating_revenue",
                 "PERPROFIT": "operating_profit",
                 "TOTPROFIT": "total_profit",
                 "NETPROFIT": "net_profit",
                 "BIZCOST": "operating_cost",
                 "PARENETP": "np_parent_company_owners",
                 "FINNETCFLOW": "net_finance_cash_flow",
                 "MANANETR": "net_operate_cash_flow",
                 "INVNETCASHFLOW": "net_invest_cash_flow",
                 'CASHNETI': 'n_change_in_cash'
                 })

    ttm_income_sets_pre = engine.fetch_fundamentals_pit_extend_company_id(IncomeTTM,
                                                                          [IncomeTTM.BIZINCO,  # 营业收入
                                                                           IncomeTTM.PERPROFIT,  # 营业利润
                                                                           IncomeTTM.TOTPROFIT,  # 利润总额
                                                                           IncomeTTM.NETPROFIT,  # 净利润
                                                                           IncomeTTM.BIZCOST,  # 营业成本
                                                                           IncomeTTM.PARENETP  # 归属于母公司所有者的净利润
                                                                           ],
                                                                          dates=[trade_date_pre_year]).drop(columns, axis=1)

    ttm_factor_sets_pre = ttm_income_sets_pre.rename(
        columns={"BIZINCO": "operating_revenue_pre_year",
                 "PERPROFIT": "operating_profit_pre_year",
                 "TOTPROFIT": "total_profit_pre_year",
                 "NETPROFIT": "net_profit_pre_year",
                 "BIZCOST": "operating_cost_pre_year",
                 "PARENETP": "np_parent_company_owners_pre_year",
                 })

    field_key = ttm_factor_sets_pre.keys()
    for i in field_key:
        ttm_factor_sets[i] = ttm_factor_sets_pre[i]

    ttm_cash_flow_sets_pre = engine.fetch_fundamentals_pit_extend_company_id(CashFlowTTM,
                                                                             [CashFlowTTM.FINNETCFLOW,  # 筹资活动产生的现金流量净额
                                                                              CashFlowTTM.MANANETR,  # 经营活动产生的现金流量净额
                                                                              CashFlowTTM.INVNETCASHFLOW,  # 投资活动产生的现金流量净额
                                                                              CashFlowTTM.CASHNETI,  # 现金及现金等价物的净增加额
                                                                              ],
                                                                             dates=[trade_date_pre_year]).drop(columns, axis=1)

    ttm_cash_flow_sets_pre = ttm_cash_flow_sets_pre.rename(
        columns={"FINNETCFLOW": "net_finance_cash_flow_pre_year",
                 "MANANETR": "net_operate_cash_flow_pre_year",
                 "INVNETCASHFLOW": "net_invest_cash_flow_pre_year",
                 'CASHNETI': 'n_change_in_cash_pre_year',
                 })

    field_key = ttm_cash_flow_sets_pre.keys()
    for i in field_key:
        ttm_factor_sets[i] = ttm_cash_flow_sets_pre[i]
    # ttm_factor_sets = pd.merge(ttm_factor_sets, ttm_cash_flow_sets, on='security_code')
    print('get_ttm_factor_sets_pre')

    # ttm 连续
    ttm_factor_sets_pre_year_2 = engine.fetch_fundamentals_pit_extend_company_id(IncomeTTM,
                                                                                 [IncomeTTM.NETPROFIT,
                                                                                  IncomeTTM.BIZINCO,
                                                                                  IncomeTTM.BIZCOST,
                                                                                  ],
                                                                                 dates=[trade_date_pre_year_2]).drop(columns, axis=1)
    ttm_factor_sets_pre_year_2 = ttm_factor_sets_pre_year_2.rename(
        columns={"BIZINCO": "operating_revenue_pre_year_2",
                 "BIZCOST": "operating_cost_pre_year_2",
                 "NETPROFIT": "net_profit_pre_year_2",
                 })
    # ttm_factor_sets = pd.merge(ttm_factor_sets, ttm_factor_sets_pre_year_2, on="security_code")
    field_key = ttm_factor_sets_pre_year_2.keys()
    for i in field_key:
        ttm_factor_sets[i] = ttm_factor_sets_pre_year_2[i]
    print('get_ttm_factor_sets_2')

    ttm_factor_sets_pre_year_3 = engine.fetch_fundamentals_pit_extend_company_id(IncomeTTM,
                                                                                 [IncomeTTM.NETPROFIT,
                                                                                  IncomeTTM.BIZINCO,
                                                                                  IncomeTTM.BIZCOST,
                                                                                  ],
                                                                                 dates=[trade_date_pre_year_3]).drop(columns, axis=1)
    ttm_factor_sets_pre_year_3 = ttm_factor_sets_pre_year_3.rename(
        columns={"BIZINCO": "operating_revenue_pre_year_3",
                 "BIZCOST": "operating_cost_pre_year_3",
                 "NETPROFIT": "net_profit_pre_year_3",
                 })
    # ttm_factor_sets = pd.merge(ttm_factor_sets, ttm_factor_sets_pre_year_3, on="security_code")
    field_key = ttm_factor_sets_pre_year_3.keys()
    for i in field_key:
        ttm_factor_sets[i] = ttm_factor_sets_pre_year_3[i]

    print('get_ttm_factor_sets_3')

    ttm_factor_sets_pre_year_4 = engine.fetch_fundamentals_pit_extend_company_id(IncomeTTM,
                                                                                 [IncomeTTM.NETPROFIT,
                                                                                  IncomeTTM.BIZINCO,
                                                                                  IncomeTTM.BIZCOST,
                                                                                  ],
                                                                                 dates=[trade_date_pre_year_4]).drop(columns, axis=1)
    ttm_factor_sets_pre_year_4 = ttm_factor_sets_pre_year_4.rename(
        columns={"BIZINCO": "operating_revenue_pre_year_4",
                 "BIZCOST": "operating_cost_pre_year_4",
                 "NETPROFIT": "net_profit_pre_year_4",
                 })
    # ttm_factor_sets = pd.merge(ttm_factor_sets, ttm_factor_sets_pre_year_4, on="security_code")
    field_key = ttm_factor_sets_pre_year_4.keys()
    for i in field_key:
        ttm_factor_sets[i] = ttm_factor_sets_pre_year_4[i]
    print('get_ttm_factor_sets_4')

    ttm_factor_sets_pre_year_5 = engine.fetch_fundamentals_pit_extend_company_id(IncomeTTM,
                                                                                 [IncomeTTM.NETPROFIT,
                                                                                  IncomeTTM.BIZINCO,
                                                                                  IncomeTTM.BIZCOST,
                                                                                  ],
                                                                                 dates=[trade_date_pre_year_5]).drop(columns, axis=1)

    ttm_factor_sets_pre_year_5 = ttm_factor_sets_pre_year_5.rename(
        columns={"BIZINCO": "operating_revenue_pre_year_5",
                 "BIZCOST": "operating_cost_pre_year_5",
                 "NETPROFIT": "net_profit_pre_year_5",
                 })
    # ttm_factor_sets = pd.merge(ttm_factor_sets, ttm_factor_sets_pre_year_5, on="security_code")
    field_key = ttm_factor_sets_pre_year_5.keys()
    for i in field_key:
        ttm_factor_sets[i] = ttm_factor_sets_pre_year_5[i]
    print('get_ttm_factor_sets_5')

    return ttm_factor_sets, balance_sets


def prepare_calculate_local(trade_date, factor_name):
    # growth
    tic = time.time()
    print('trade_date %s' % trade_date)
    ttm_factor_sets, balance_sets = get_basic_data(trade_date)
    growth_sets = pd.merge(ttm_factor_sets, balance_sets, on='security_code')
    print('len_of_total_growth: %s' % len(growth_sets))
    print(growth_sets.head())
    if len(growth_sets) <= 0:
        print("%s has no data" % trade_date)
        return
    else:
        factor_history_growth.calculate(trade_date, growth_sets, factor_name)
    time1 = time.time()
    print('growth_cal_time:{}'.format(time1 - tic))
    del ttm_factor_sets, balance_sets
    gc.collect()


def prepare_calculate_remote(trade_date):
    # growth
    tic = time.time()
    print('trade_date %s' % trade_date)
    ttm_factor_sets, balance_sets = get_basic_data(trade_date)
    growth_sets = pd.merge(ttm_factor_sets, balance_sets, on='security_code')
    print('len_of_total_growth: %s' % len(growth_sets))
    print(growth_sets.head())
    if len(growth_sets) <= 0:
        print("%s has no data" % trade_date)
        return
    else:
        session = str(int(time.time() * 1000000 + datetime.now().microsecond))
        cache_data.set_cache(session, "growth" + str(trade_date), growth_sets.to_json(orient='records'))
        factor_history_growth.factor_calculate.delay(date_index=trade_date, session=session)
    time1 = time.time()
    print('growth_cal_time:{}'.format(time1 - tic))


def do_update(start_date, end_date, count, factor_name):
    # 读取交易日
    syn_util = SyncUtil()
    trade_date_sets = syn_util.get_trades_ago('001002', start_date, end_date, count, order='DESC')
    trade_date_sets = trade_date_sets['TRADEDATE'].values
    # print('交易日：%s' % trade_date_sets)
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

    factor_name = 'factor_growth'
    args = parser.parse_args()
    if args.end_date == 0:
        end_date = int(datetime.now().date().strftime('%Y%m%d'))
    else:
        end_date = args.end_date
    if args.rebuild:
        processor = factor_history_growth.Growth(factor_name)
        processor.create_dest_tables()
        do_update(args.start_date, end_date, args.count, factor_name)
    if args.update:
        do_update(args.start_date, end_date, args.count, factor_name)
    # do_update('20190819', '20190823', 10)


