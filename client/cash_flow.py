#! /usr/bin/env python
# -*- coding: utf-8 -*-

"""
@version: ??
@author: li
@file: cash_flow.py
@time: 2019-09-02 10:51
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
from factor import factor_cash_flow
from client.engines.sqlengine import sqlEngine
from client.utillities.sync_util import SyncUtil

from client.dbmodel.model import BalanceMRQ, BalanceTTM, BalanceReport
from client.dbmodel.model import CashFlowMRQ, CashFlowTTM, CashFlowReport
from client.dbmodel.model import IndicatorReport, IndicatorMRQ, IndicatorTTM
from client.dbmodel.model import IncomeMRQ, IncomeReport, IncomeTTM

from vision.vision.db.signletion_engine import *
# from ultron.cluster.invoke.cache_data import cache_data
pd.set_option('display.max_columns', None)


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
    获取cash flow所需要的因子
    :param trade_date:
    :return:
    """
    maplist = {'MANANETR': 'net_operate_cash_flow',  # 经营活动现金流量净额
               'LABORGETCASH': 'goods_sale_and_service_render_cash',   # 销售商品、提供劳务收到的现金

               'BIZINCO': 'operating_revenue',  # 营业收入
               'BIZTOTINCO': 'total_operating_revenue',    # 营业总收入
               'BIZTOTCOST': 'total_operating_cost',    # 营业总成本
               'NETPROFIT':'net_profit',   # 净利润
               'PARENETP':'np_parent_company_owners',    # 归属于母公司所有者的净利润

               'TOTLIAB': 'total_liability',  # 负债合计
               'SHORTTERMBORR':'shortterm_loan',  # 短期借款
               'LONGBORR':'longterm_loan',  # 长期借款
               'TOTALCURRLIAB':'total_current_liability',   # 流动负债合计
               'net_liability':'',  # 净负债
               'TOTCURRASSET':'total_current_assets',  # 流动资产合计
               'TOTASSET':'total_assets',      # 资产总计
               'FINALCASHBALA':'cash_and_equivalents_at_end',  # 期末现金及现金等价物余额
               }

    columns = ['COMPCODE', 'PUBLISHDATE', 'ENDDATE', 'symbol', 'company_id', 'trade_date']
    # report data
    engine = sqlEngine()
    cash_flow_sets = engine.fetch_fundamentals_pit_extend_company_id(CashFlowReport,
                                                                     [CashFlowReport.MANANETR,  # 经营活动现金流量净额
                                                                      CashFlowReport.LABORGETCASH,  # 销售商品、提供劳务收到的现金
                                                                      ], dates=[trade_date]).drop(columns, axis=1)

    income_sets = engine.fetch_fundamentals_pit_extend_company_id(IncomeReport,
                                                                  [IncomeReport.BIZINCO,  # 营业收入
                                                                   IncomeReport.BIZTOTCOST,  # 营业总成本
                                                                   IncomeReport.BIZTOTINCO,  # 营业总收入
                                                                   ], dates=[trade_date]).drop(columns, axis=1)

    tp_cash_flow = pd.merge(cash_flow_sets, income_sets, on="security_code")

    tp_cash_flow = tp_cash_flow.rename(columns={'MANANETR': 'net_operate_cash_flow',  # 经营活动现金流量净额
                                                'LABORGETCASH': 'goods_sale_and_service_render_cash',   # 销售商品、提供劳务收到的现金
                                                'BIZINCO': 'operating_revenue',  # 营业收入
                                                'BIZTOTINCO': 'total_operating_revenue',    # 营业总收入
                                                'BIZTOTCOST': 'total_operating_cost',    # 营业总成本
                                                })

    # ttm data
    balance_ttm_sets = engine.fetch_fundamentals_pit_extend_company_id(BalanceTTM,
                                                                       [BalanceTTM.TOTLIAB,        # 负债合计
                                                                        BalanceTTM.SHORTTERMBORR,  # 短期借款
                                                                        BalanceTTM.LONGBORR,       # 长期借款
                                                                        BalanceTTM.TOTALCURRLIAB,  # 流动负债合计
                                                                        BalanceTTM.TOTCURRASSET,   # 流动资产合计
                                                                        BalanceTTM.TOTASSET,       # 资产总计
                                                                        ],
                                                                       dates=[trade_date]).drop(columns, axis=1)

    cash_flow_ttm_sets = engine.fetch_fundamentals_pit_extend_company_id(CashFlowTTM,
                                                                         [CashFlowTTM.MANANETR,       # 经营活动现金流量净额
                                                                          CashFlowTTM.FINALCASHBALA,  # 期末现金及现金等价物余额
                                                                          CashFlowTTM.LABORGETCASH,
                                                                          ],
                                                                         dates=[trade_date]).drop(columns, axis=1)

    income_ttm_sets = engine.fetch_fundamentals_pit_extend_company_id(IncomeTTM,
                                                                      [IncomeTTM.BIZTOTCOST,  # 营业总成本
                                                                       IncomeTTM.BIZINCO,     # 营业收入
                                                                       IncomeTTM.BIZTOTINCO,  # 营业总收入
                                                                       IncomeTTM.NETPROFIT,   # 净利润
                                                                       IncomeTTM.PARENETP,    # 归属于母公司所有者的净利润
                                                                       ],
                                                                      dates=[trade_date]).drop(columns, axis=1)

    ttm_cash_flow = pd.merge(balance_ttm_sets, cash_flow_ttm_sets, on="security_code")
    ttm_cash_flow = pd.merge(income_ttm_sets, ttm_cash_flow, on="security_code")
    ttm_cash_flow = ttm_cash_flow.rename(columns={'MANANETR': 'net_operate_cash_flow',  # 经营活动现金流量净额
                                                  'BIZINCO': 'operating_revenue',  # 营业收入
                                                  'BIZTOTINCO': 'total_operating_revenue',    # 营业总收入
                                                  'BIZTOTCOST': 'total_operating_cost',    # 营业总成本
                                                  'NETPROFIT':'net_profit',   # 净利润
                                                  'PARENETP':'np_parent_company_owners',    # 归属于母公司所有者的净利润
                                                  'TOTLIAB': 'total_liability',  # 负债合计
                                                  'SHORTTERMBORR': 'shortterm_loan',  # 短期借款
                                                  'LONGBORR': 'longterm_loan',  # 长期借款
                                                  'TOTALCURRLIAB': 'total_current_liability',   # 流动负债合计
                                                  'LABORGETCASH': 'goods_sale_and_service_render_cash', # 销售商品、提供劳务收到的现金
                                                  # 'NDEBT':'net_liability',  # 净负债
                                                  'TOTCURRASSET': 'total_current_assets',  # 流动资产合计
                                                  'TOTASSET': 'total_assets',      # 资产总计
                                                  'FINALCASHBALA': 'cash_and_equivalents_at_end',  # 期末现金及现金等价物余额
                                                  })

    column = ['trade_date']
    valuation_sets = get_fundamentals(query(Valuation.security_code,
                                            Valuation.trade_date,
                                            Valuation.market_cap,)
                                      .filter(Valuation.trade_date.in_([trade_date]))).drop(column, axis=1)

    ttm_cash_flow = pd.merge(ttm_cash_flow, valuation_sets, how='outer', on='security_code')
    tp_cash_flow = pd.merge(tp_cash_flow, valuation_sets, how='outer', on='security_code')

    return tp_cash_flow, ttm_cash_flow


def prepare_calculate_local(trade_date):
    # 本地计算
    tic = time.time()
    tp_cash_flow, ttm_cash_flow_sets = get_basic_data(trade_date)
    print('len_tp_cash_flow: %s' % len(tp_cash_flow))
    print('len_ttm_cash_flow: %s' % len(ttm_cash_flow_sets))
    print('tp_cash_flow: \n%s' % tp_cash_flow.head())
    print('ttm_cash_flow: \n%s' % ttm_cash_flow_sets.head())

    if len(tp_cash_flow) <= 0 or len(ttm_cash_flow_sets) <= 0:
        print("%s has no data" % trade_date)
        return
    else:
        factor_cash_flow.calculate(trade_date, tp_cash_flow, ttm_cash_flow_sets)
    end = time.time()
    print('cash_flow_cal_time:{}'.format(end - tic))


def prepare_calculate_remote(trade_date):
    # 远程计算
    tp_cash_flow, ttm_cash_flow_sets = get_basic_data(trade_date)
    print('len_tp_cash_flow: %s' % len(tp_cash_flow))
    print('len_ttm_cash_flow: %s' % len(ttm_cash_flow_sets))
    print('tp_cash_flow: \n%s' % tp_cash_flow.head())
    print('ttm_cash_flow: \n%s' % ttm_cash_flow_sets.head())

    if len(tp_cash_flow) <= 0 or len(ttm_cash_flow_sets) <= 0:
        print("%s has no data" % trade_date)
        return
    else:
        tic = time.time()
        session = str(int(time.time() * 1000000 + datetime.now().microsecond))
        cache_data.set_cache(session + str(trade_date) + "1", trade_date, tp_cash_flow.to_json(orient='records'))
        cache_data.set_cache(session + str(trade_date) + "2", trade_date, ttm_cash_flow_sets.to_json(orient='records'))
        factor_cash_flow.factor_calculate(date_index=trade_date, session=session)
        time4 = time.time()
        print('cash_flow_cal_time:{}'.format(time4 - tic))


def do_update(start_date, end_date, count):
    # 读取交易日
    syn_util = SyncUtil()
    trade_date_sets = syn_util.get_trades_ago('001002', start_date, end_date, count, order='DESC')
    trade_date_sets = trade_date_sets['TRADEDATE'].values
    print('交易日：%s' % trade_date_sets)
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
    #     processor = factor_cash_flow.FactorCashFlow('factor_cash_flow')
    #     processor.create_dest_tables()
    #     do_update(args.start_date, end_date, args.count)
    # if args.update:
    #     do_update(args.start_date, end_date, args.count)
    do_update('20190819', '20190823', 10)
