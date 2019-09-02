#! /usr/bin/env python
# -*- coding: utf-8 -*-

"""
@version: ??
@author: li
@file: cash_flow.py
@time: 2019-07-16 17:31
"""
import sys
sys.path.append('../')
sys.path.append('../../')
sys.path.append('../../../')
import time
import collections
import argparse
from datetime import datetime, timedelta
from factor import factor_cash_flow
import pandas as pd
from client.engines.sqlengine import sqlEngine
from client.utillities.trade_date import TradeDate

from client.dbmodel.model import BalanceMRQ, BalanceTTM, BalanceReport
from client.dbmodel.model import CashFlowMRQ, CashFlowTTM,CashFlowReport
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
    _trade_date = TradeDate()
    trade_date_sets = collections.OrderedDict(
        sorted(_trade_date._trade_date_sets.items(), key=lambda t: t[0], reverse=False))

    time_array = datetime.strptime(str(trade_date), "%Y%m%d")
    time_array = time_array - timedelta(days=365) * n
    date_time = int(datetime.strftime(time_array, "%Y%m%d"))
    if date_time < min(trade_date_sets.keys()):
        # print('date_time %s is outof trade_date_sets' % date_time)
        return date_time
    else:
        while date_time not in trade_date_sets:
            date_time = date_time - 1
        # print('trade_date pre %s year %s' % (n, date_time))
        return date_time


def get_basic_cash_flow(trade_date):
    """
    获取cash flow所需要的因子
    :param trade_date:
    :return:
    """
    engine = sqlEngine()
    columns = ['COMPCODE', 'PUBLISHDATE', 'ENDDATE', 'symbol', 'company_id']
    # report data
    cash_flow_sets = engine.fetch_fundamentals_extend_company_id(CashFlowReport,
                                                                 [CashFlowReport.MANANETR,  # 经营活动现金流量净额
                                                                  CashFlowReport.LABORGETCASH,  # 销售商品、提供劳务收到的现金
                                                                  ],
                                                                 dates=[trade_date]).drop(columns, axis=1)

    income_sets = engine.fetch_fundamentals_extend_company_id(IncomeReport,
                                                              [IncomeReport.BIZINCO,  # 营业收入
                                                               IncomeReport.BIZTOTCOST,  # 营业总成本
                                                               IncomeReport.BIZTOTINCO,  # 营业总收入
                                                               ],
                                                              dates=[trade_date]).drop(columns, axis=1)
    print(cash_flow_sets.head())
    print(income_sets.head())

    tp_cash_flow = pd.merge(cash_flow_sets, income_sets, on="security_code")
    print(tp_cash_flow.head())

    # ttm data
    balance_ttm_sets = engine.fetch_fundamentals_extend_company_id(BalanceTTM,
                                                                   [BalanceTTM.TOTLIAB,  # 负债合计
                                                                    BalanceTTM.SHORTTERMBORR,  # 短期借款
                                                                    BalanceTTM.LONGBORR,  # 长期借款
                                                                    BalanceTTM.TOTALCURRLIAB,  # 流动负债合计
                                                                    BalanceTTM.TOTCURRASSET,   # 流动资产合计
                                                                    BalanceTTM.TOTASSET,  # 资产总计
                                                                    ],
                                                                   dates=[trade_date])

    cash_flow_ttm_sets = engine.fetch_fundamentals_extend_company_id(CashFlowTTM,
                                                                     [CashFlowTTM.MANANETR,  # 经营活动现金流量净额
                                                                      CashFlowTTM.FINALCASHBALA,  # 期末现金及现金等价物余额
                                                                      ],
                                                                     dates=[trade_date])

    income_ttm_sets = engine.fetch_fundamentals_extend_company_id(IncomeTTM,
                                                                  [IncomeTTM.BIZTOTCOST,  # 营业总成本
                                                                   IncomeTTM.BIZTOTINCO,  # 营业总收入
                                                                   IncomeTTM.NETPROFIT,  # 净利润
                                                                   IncomeTTM.PARENETP,  # 归属于母公司所有者的净利润
                                                                   ],
                                                                  dates=[trade_date])


    valuation_sets = get_fundamentals(add_filter_trade(query(Valuation.__name__,
                                                             [Valuation.symbol,
                                                              Valuation.market_cap,  # 总市值
                                                              Valuation.circulating_market_cap]), [trade_date])) # 流通市值

    # 合并
    # tp_cash_flow = pd.merge(cash_flow_sets, income_sets, on="symbol")
    # tp_cash_flow = tp_cash_flow[-tp_cash_flow.duplicated()]


    # ttm_factors = {Balance.__name__: [Balance.symbol,
    #                                   Balance.total_liability,  # 负债合计
    #                                   Balance.shortterm_loan,  # 短期借款
    #                                   Balance.longterm_loan,  # 长期借款
    #                                   Balance.total_current_liability,  # 流动负债合计
    #                                   Balance.net_liability,  # 缺 净负债
    #                                   Balance.total_current_assets,  # 流动资产合计
    #                                   Balance.interest_bearing_liability,  # 带息负债， 缺
    #                                   Balance.total_assets],  # 资产总计
    #
    #                CashFlow.__name__: [CashFlow.symbol,
    #                                    CashFlow.net_operate_cash_flow,
    #                                    CashFlow.goods_sale_and_service_render_cash,  # 缺 销售商品、提供劳务收到的现金
    #                                    CashFlow.cash_and_equivalents_at_end],  # 期末现金及现金等价物余额
    #                Income.__name__: [Income.symbol,
    #                                  Income.operating_revenue,  # 营业收入 缺
    #                                  Income.total_operating_revenue,  # 营业总收入
    #                                  Income.total_operating_cost,  # 营业总成本
    #                                  Income.net_profit,  # 净利润
    #                                  Income.np_parent_company_owners] # 归属于母公司所有者的净利润
    #                }
    # ttm_cash_flow_sets = get_ttm_fundamental([], ttm_factors, trade_date).reset_index()
    # ttm_cash_flow_sets = ttm_cash_flow_sets[-ttm_cash_flow_sets.duplicated()]
    # # 合并
    # ttm_cash_flow_sets = pd.merge(ttm_cash_flow_sets, valuation_sets, on="symbol")

    return tp_cash_flow


def prepare_calculate(trade_date):
    # cash flow
    tp_cash_flow, ttm_cash_flow_sets = get_basic_cash_flow(trade_date)
    print('len_tp_cash_flow: %s' % len(tp_cash_flow))
    print('len_ttm_cash_flow: %s' % len(ttm_cash_flow_sets))
    print('tp_cash_flow: %s' % tp_cash_flow.head())
    print('ttm_cash_flow: %s' % ttm_cash_flow_sets.head())

    if len(tp_cash_flow) <= 0 or len(ttm_cash_flow_sets) <= 0:
        print("%s has no data" % trade_date)
        return
    else:
        tic = time.time()
        session = str(int(time.time() * 1000000 + datetime.now().microsecond))
        cache_data.set_cache(session + str(trade_date) + "1", trade_date, tp_cash_flow.to_json(orient='records'))
        cache_data.set_cache(session + str(trade_date) + "2", trade_date, ttm_cash_flow_sets.to_json(orient='records'))
        factor_cash_flow.factor_calculate.delay(date_index=trade_date, session=session)
        time4 = time.time()
        print('cash_flow_cal_time:{}'.format(time4 - tic))


def do_update(start_date, end_date, count):
    # 读取本地交易日
    _trade_date = TradeDate()
    trade_date_sets = _trade_date.trade_date_sets_ago(start_date, end_date, count)
    for trade_date in trade_date_sets:
        print('因子计算日期： %s' % trade_date)
        prepare_calculate(trade_date)
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

    get_basic_cash_flow('20190822')