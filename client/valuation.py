#! /usr/bin/env python
# -*- coding: utf-8 -*-

"""
@version: ??
@author: li
@file: valuation.py
@time: 2019-09-05 11:05
"""

#! /usr/bin/env python
# -*- coding: utf-8 -*-

"""
@version: ??
@author: li
@file: factor_historical_value.py
@time: 2019-07-16 19:48
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
from factor import factor_valuation

from client.dbmodel.model import BalanceMRQ, BalanceTTM, BalanceReport
from client.dbmodel.model import CashFlowMRQ, CashFlowTTM, CashFlowReport
from client.dbmodel.model import IndicatorReport, IndicatorMRQ, IndicatorTTM
from client.dbmodel.model import IncomeMRQ, IncomeReport, IncomeTTM

from vision.vision.db.signletion_engine import *
from vision.vision.table.valuation import Valuation
from vision.vision.table.industry import Industry
from client.engines.sqlengine import sqlEngine
from client.utillities.sync_util import SyncUtil
from ultron.cluster.invoke.cache_data import cache_data


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


def get_basic_history_value_data(trade_date):
    """
    获取基础数据
    按天获取当天交易日所有股票的基础数据
    :param trade_date: 交易日
    :return:
    """
    maplist = {
        # cash_flow
        'LABORGETCASH': 'goods_sale_and_service_render_cash',  # 销售商品、提供劳务收到的现金
        'MANANETR': 'net_operate_cash_flow',  # 经营活动现金流量净额
        'CASHNETR':'cash_equivalent_increase', # 现金及现金等价物净增加额

        'FINALCASHBALA': 'cash_and_equivalents_at_end',  # 期末现金及现金等价物余额

        # income
        'NETPROFIT': 'net_profit',  # 净利润
        'PARENETP': 'np_parent_company_owners',  # 归属于母公司所有者的净利润
        'BIZTOTINCO': 'total_operating_revenue',  # 营业总收入
        'BIZINCO': 'operating_revenue',  # 营业收入

        'BIZTOTCOST': 'total_operating_cost',    # 营业总成本

        # balance
        'TOTASSET': 'total_assets',  # 资产总计
        'SHORTTERMBORR': 'shortterm_loan',  # 短期借款
        'LONGBORR': 'longterm_loan',  # 长期借款

        'TOTLIAB': 'total_liability',  # 负债合计
        'TOTALCURRLIAB':'total_current_liability',   # 流动负债合计
        'net_liability':'',  # 净负债
        'TOTCURRASSET':'total_current_assets',  # 流动资产合计

        # indicator
        'FCFF':'enterprise_fcfps', # 企业自由现金流
        'NETPROFITCUT': 'net_profit_cut',  # 扣除非经常性损益的净利润



        # valuation
        '':'pe',
        '':'pb',
        '':'ps',
        '':'pcf',
        '':'market_cap',
        '':'circulating_market_cap'

        }

    columns = ['COMPCODE', 'PUBLISHDATE', 'ENDDATE', 'symbol', 'company_id', 'trade_date']
    engine = sqlEngine()
    trade_date_1y = get_trade_date(trade_date, 1)
    trade_date_2y = get_trade_date(trade_date, 2)
    trade_date_3y = get_trade_date(trade_date, 3)
    trade_date_4y = get_trade_date(trade_date, 4)
    trade_date_5y = get_trade_date(trade_date, 5)
    # report data
    cash_flow_sets = engine.fetch_fundamentals_pit_extend_company_id(CashFlowReport,
                                                                     [CashFlowReport.LABORGETCASH,
                                                                      CashFlowReport.CASHNETR,
                                                                      ], dates=[trade_date]).drop(columns, axis=1)
    cash_flow_sets = cash_flow_sets.rename(columns={
        'CASHNETR': 'cash_equivalent_increase',  # 现金及现金等价物净增加额
        'LABORGETCASH': 'goods_sale_and_service_render_cash',  # 销售商品、提供劳务收到的现金
    })

    income_sets = engine.fetch_fundamentals_pit_extend_company_id(IncomeReport,
                                                                  [IncomeReport.NETPROFIT,
                                                                   IncomeTTM.BIZINCO,
                                                                   ], dates=[trade_date]).drop(columns, axis=1)
    income_sets = income_sets.rename(columns={
        'NETPROFIT': 'net_profit',  # 净利润
        'BIZINCO': 'operating_revenue',  # 营业收入
    })

    indicator_sets = engine.fetch_fundamentals_pit_extend_company_id(IndicatorReport,
                                                                     [IndicatorReport.FCFF,
                                                                      ], dates=[trade_date]).drop(columns, axis=1)

    indicator_sets = indicator_sets.rename(columns={
        'FCFF': 'enterprise_fcfps',  # 企业自由现金流
    })

    balance_sets = engine.fetch_fundamentals_pit_extend_company_id(BalanceReport,
                                                                   [BalanceReport.LONGBORR,
                                                                    BalanceReport.SHORTTERMBORR,
                                                                    ], dates=[trade_date]).drop(columns, axis=1)

    balance_sets = balance_sets.rename(columns={
        'SHORTTERMBORR': 'shortterm_loan',  # 短期借款
        'LONGBORR': 'longterm_loan',  # 长期借款
    })

    valuation_sets = pd.merge(cash_flow_sets, income_sets, on='security_code')
    valuation_sets = pd.merge(valuation_sets, indicator_sets, on='security_code')
    valuation_sets = pd.merge(valuation_sets, balance_sets, on='security_code')


    # MRQ data
    cash_flow_mrq = engine.fetch_fundamentals_pit_extend_company_id(CashFlowMRQ,
                                                                    [CashFlowMRQ.CASHNETR,
                                                                     ], dates=[trade_date]).drop(columns, axis=1)
    cash_flow_mrq = cash_flow_mrq.rename(columns={
        'CASHNETR': 'cash_equivalent_increase',  # 现金及现金等价物净增加额
    })

    balance_mrq = engine.fetch_fundamentals_pit_extend_company_id(BalanceMRQ,
                                                                  [BalanceMRQ.LONGBORR,
                                                                   BalanceMRQ.TOTASSET,
                                                                   BalanceMRQ.SHORTTERMBORR,
                                                                   ], dates=[trade_date]).drop(columns, axis=1)

    balance_mrq = balance_mrq.rename(columns={
        'SHORTTERMBORR': 'shortterm_loan',  # 短期借款
        'LONGBORR': 'longterm_loan',  # 长期借款
        'TOTASSET': 'total_assets',  # 资产总计
    })

    valuation_mrq = pd.merge(cash_flow_mrq, balance_mrq, on='security_code')



    # TTM data
    # 总市值徐奥合并到TTM数据中，
    cash_flow_ttm_sets = engine.fetch_fundamentals_pit_extend_company_id(CashFlowTTM,
                                                                         [CashFlowTTM.MANANETR,
                                                                          ], dates=[trade_date]).drop(columns, axis=1)
    cash_flow_ttm_sets = cash_flow_ttm_sets.rename(columns={
        'MANANETR': 'net_operate_cash_flow',  # 经营活动现金流量净额
    })

    indicator_ttm_sets = engine.fetch_fundamentals_pit_extend_company_id(IndicatorTTM,
                                                                         [IndicatorTTM.NETPROFITCUT,
                                                                          ], dates=[trade_date_1y]).drop(columns, axis=1)
    indicator_ttm_sets = indicator_ttm_sets.rename(columns={
        'NETPROFITCUT': 'net_profit_cut_pre',  # 扣除非经常性损益的净利润
    })

    income_ttm_sets = engine.fetch_fundamentals_pit_extend_company_id(IncomeTTM,
                                                                      [IncomeTTM.NETPROFIT,
                                                                       IncomeTTM.PARENETP,
                                                                       ], dates=[trade_date]).drop(columns, axis=1)
    income_ttm_sets = income_ttm_sets.rename(columns={
        'NETPROFIT': 'net_profit',  # 净利润
        'PARENETP': 'np_parent_company_owners',  # 归属于母公司所有者的净利润
    })

    income_ttm_sets_3 = engine.fetch_fundamentals_pit_extend_company_id(IncomeTTM,
                                                                        [IncomeTTM.NETPROFIT,
                                                                         IncomeTTM.PARENETP,
                                                                         ], dates=[trade_date_3y]).drop(columns, axis=1)
    income_ttm_sets_3 = income_ttm_sets_3.rename(columns={
        'PARENETP': 'np_parent_company_owners_3',  # 归属于母公司所有者的净利润
    })

    income_ttm_sets_5 = engine.fetch_fundamentals_pit_extend_company_id(IncomeTTM,
                                                                        [IncomeTTM.NETPROFIT,
                                                                         IncomeTTM.PARENETP,
                                                                         ], dates=[trade_date_5y]).drop(columns, axis=1)
    income_ttm_sets_5 = income_ttm_sets_5.rename(columns={
        'NETPROFIT': 'net_profit_5',  # 净利润
        'PARENETP': 'np_parent_company_owners_5',  # 归属于母公司所有者的净利润
    })

    valuation_ttm_sets = pd.merge(cash_flow_ttm_sets, income_ttm_sets, on='security_code')
    valuation_ttm_sets = pd.merge(valuation_ttm_sets, indicator_ttm_sets, on='security_code')

    valuation_ttm_sets = pd.merge(valuation_ttm_sets, income_ttm_sets_3, on='security_code')
    valuation_ttm_sets = pd.merge(valuation_ttm_sets, income_ttm_sets_5, on='security_code')


    # PS, PE, PB, PCF
    valuation_sets = get_fundamentals_pit_extend_internal(query(Valuation.security_code,
                                                                Valuation.trade_date,
                                                                Valuation.pe,
                                                                Valuation.ps,
                                                                Valuation.pb,
                                                                Valuation.pcf,
                                                                Valuation.market_cap,
                                                                Valuation.circulating_market_cap)
                                                          .filter(Valuation.trade_date.in_([trade_date])))

    industry_set = ['801010', '801020', '801030', '801040', '801050', '801080', '801110', '801120', '801130',
                    '801140', '801150', '801160', '801170', '801180', '801200', '801210', '801230', '801710',
                    '801720', '801730', '801740', '801750', '801760', '801770', '801780', '801790', '801880',
                    '801890']
    sw_industry = get_fundamentals_extend_internal(query(Industry.trade_date,
                                                         Industry.symbol,
                                                         Industry.isymbol)
                                                   .filter(Industry.trade_date.in_(['20180905'])),
                                                   internal_type='symbol')




    # TTM计算
    ttm_factors = {Income._name_: [Income.symbol,
                                   Income.np_parent_company_owners],
                   CashFlow._name_:[CashFlow.symbol,
                                    CashFlow.net_operate_cash_flow]
                   }

    ttm_factors_sum_list = {Income._name_:[Income.symbol,
                                           Income.net_profit,  # 净利润
                                        ],}

    trade_date_2y = get_trade_date(trade_date, 2)
    trade_date_3y = get_trade_date(trade_date, 3)
    trade_date_4y = get_trade_date(trade_date, 4)
    trade_date_5y = get_trade_date(trade_date, 5)
    # print(trade_date_2y, trade_date_3y, trade_date_4y, trade_date_5y)

    ttm_factor_sets = get_ttm_fundamental([], ttm_factors, trade_date).reset_index()
    ttm_factor_sets_3 = get_ttm_fundamental([], ttm_factors, trade_date_3y).reset_index()
    ttm_factor_sets_5 = get_ttm_fundamental([], ttm_factors, trade_date_5y).reset_index()
    # ttm 周期内计算需要优化
    # ttm_factor_sets_sum = get_ttm_fundamental([], ttm_factors_sum_list, trade_date, 5).reset_index()

    factor_sets_sum = get_fundamentals(add_filter_trade(query(Valuation._name_,
                                                              [Valuation.symbol,
                                                               Valuation.market_cap,
                                                               Valuation.circulating_market_cap,
                                                               Valuation.trade_date]),
                                                        [trade_date_2y, trade_date_3y, trade_date_4y, trade_date_5y]))

    factor_sets_sum_1 = factor_sets_sum.groupby('symbol')['market_cap'].sum().reset_index().rename(columns={"market_cap": "market_cap_sum",})
    factor_sets_sum_2 = factor_sets_sum.groupby('symbol')['circulating_market_cap'].sum().reset_index().rename(columns={"circulating_market_cap": "circulating_market_cap_sum",})

    # print(factor_sets_sum_1)
    # 根据申万一级代码筛选
    sw_industry = sw_industry[sw_industry['isymbol'].isin(industry_set)]

    # 合并价值数据和申万一级行业
    valuation_sets = pd.merge(valuation_sets, sw_industry, on='symbol')
    # valuation_sets = pd.merge(valuation_sets, sw_industry, on='symbol', how="outer")

    ttm_factor_sets = ttm_factor_sets.drop(columns={"trade_date"})
    ttm_factor_sets_3 = ttm_factor_sets_3.rename(columns={"np_parent_company_owners": "np_parent_company_owners_3"})
    ttm_factor_sets_3 = ttm_factor_sets_3.drop(columns={"trade_date"})

    ttm_factor_sets_5 = ttm_factor_sets_5.rename(columns={"np_parent_company_owners": "np_parent_company_owners_5"})
    ttm_factor_sets_5 = ttm_factor_sets_5.drop(columns={"trade_date"})

    # ttm_factor_sets_sum = ttm_factor_sets_sum.rename(columns={"net_profit": "net_profit_5"})
    ttm_factor_sets = pd.merge(ttm_factor_sets, ttm_factor_sets_3, on='symbol')
    ttm_factor_sets = pd.merge(ttm_factor_sets, ttm_factor_sets_5, on='symbol')
    # ttm_factor_sets = pd.merge(ttm_factor_sets, ttm_factor_sets_sum, on='symbol')
    ttm_factor_sets = pd.merge(ttm_factor_sets, factor_sets_sum_1, on='symbol')
    ttm_factor_sets = pd.merge(ttm_factor_sets, factor_sets_sum_2, on='symbol')
    # ttm_factor_sets = pd.merge(ttm_factor_sets, ttm_factor_sets_3, on='symbol', how='outer')
    # ttm_factor_sets = pd.merge(ttm_factor_sets, ttm_factor_sets_5, on='symbol', how='outer')

    return valuation_sets, ttm_factor_sets, cash_flow_sets, income_sets


def prepare_calculate_remote(trade_date):
    # historical_value
    valuation_sets, ttm_factor_sets, cash_flow_sets, income_sets = get_basic_history_value_data(trade_date)
    valuation_sets = pd.merge(valuation_sets, income_sets, on='symbol')
    valuation_sets = pd.merge(valuation_sets, ttm_factor_sets, on='symbol')
    valuation_sets = pd.merge(valuation_sets, cash_flow_sets, on='symbol')
    if len(valuation_sets) <= 0:
        print("%s has no data" % trade_date)
        return
    else:
        tic = time.time()
        session = str(int(time.time() * 1000000 + datetime.now().microsecond))
        cache_data.set_cache(session + str(trade_date), trade_date, valuation_sets.to_json(orient='records'))
        factor_valuation.factor_calculate.delay(date_index=trade_date, session=session)
        time2 = time.time()
        print('history_cal_time:{}'.format(time2 - tic))


def prepare_calculate_local(trade_date):
    # historical_value
    tic = time.time()
    valuation_sets, ttm_factor_sets, cash_flow_sets, income_sets = get_basic_history_value_data(trade_date)
    valuation_sets = pd.merge(valuation_sets, income_sets, on='symbol')
    valuation_sets = pd.merge(valuation_sets, ttm_factor_sets, on='symbol')
    valuation_sets = pd.merge(valuation_sets, cash_flow_sets, on='symbol')
    if len(valuation_sets) <= 0:
        print("%s has no data" % trade_date)
        return
    else:
        factor_valuation.factor_calculate.delay(date_index=trade_date, session=session)
    time2 = time.time()
    print('history_cal_time:{}'.format(time2 - tic))


def do_update(start_date, end_date, count):
    # 读取本地交易日
    syn_util = SyncUtil()
    trade_date_sets = syn_util.get_trades_ago('001002', start_date, end_date, count, order='DESC')
    trade_date_sets = trade_date_sets['TRADEDATE'].values
    print('交易日：%s' % trade_date_sets)
    for trade_date in trade_date_sets:
        print('因子计算日期： %s' % trade_date)
        prepare_calculate_local(trade_date)
    print('----->')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--start_date', type=int, default=20070101)
    parser.add_argument('--end_date', type=int, default=0)
    parser.add_argument('--count', type=int, default=1)
    parser.add_argument('--rebuild', type=bool, default=False)
    parser.add_argument('--update', type=bool, default=False)
    parser.add_argument('--schedule', type=bool, default=False)

    args = parser.parse_args()
    if args.end_date == 0:
        end_date = int(datetime.now().date().strftime('%Y%m%d'))
    else:
        end_date = args.end_date
    if args.rebuild:
        processor = factor_valuation.Valuation('factor_historical_value')
        processor.create_dest_tables()
        do_update(args.start_date, end_date, args.count)
    if args.update:
        do_update(args.start_date, end_date, args.count)
