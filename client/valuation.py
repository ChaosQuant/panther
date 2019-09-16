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
# from ultron.cluster.invoke.cache_data import cache_data
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


def get_trade_date(trade_date, n, days=365):
    """
    获取当前时间前n年的时间点，且为交易日，如果非交易日，则往前提取最近的一天。
    :param days:
    :param trade_date: 当前交易日
    :param n:
    :return:
    """
    syn_util = SyncUtil()
    trade_date_sets = syn_util.get_all_trades('001002', '19900101', trade_date)
    trade_date_sets = trade_date_sets['TRADEDATE'].values

    time_array = datetime.strptime(str(trade_date), "%Y%m%d")
    time_array = time_array - timedelta(days=days) * n
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
        'MANANETR': 'net_operate_cash_flow',  # 经营活动现金流量净额, ttm
        'FINALCASHBALA': 'cash_and_equivalents_at_end',  # 期末现金及现金等价物余额 , mrq

        # income
        'NETPROFIT': 'net_profit',  # 净利润 ttm
        'PARENETP': 'np_parent_company_owners',  # 归属于母公司所有者的净利润 ttm
        'BIZTOTINCO': 'total_operating_revenue',  # 营业总收入 ttm
        'BIZINCO': 'operating_revenue',  # 营业收入 ttm
        'TOTPROFIT': 'total_profit',  # 利润总额 ttm

        # balance
        'TOTASSET': 'total_assets',  # 资产总计 mrq
        'SHORTTERMBORR': 'shortterm_loan',  # 短期借款 mrq
        'LONGBORR': 'longterm_loan',  # 长期借款 mrq
        'PARESHARRIGH':'equities_parent_company_owners', # 归属于母公司股东权益合计 mrq

        # indicator
        'FCFF': 'enterprise_fcfps',  # 企业自由现金流 report
        'NETPROFITCUT': 'net_profit_cut',  # 扣除非经常性损益的净利润 ttm

        # valuation
        'pe': 'pe',
        'pb': 'pb',
        'ps': 'ps',
        'pcf': 'pcf',
        'market_cap': 'market_cap',  # 总市值
        'circulating_market_cap': 'circulating_market_cap'
        }

    columns = ['COMPCODE', 'PUBLISHDATE', 'ENDDATE', 'symbol', 'company_id', 'trade_date']
    engine = sqlEngine()
    trade_date_1y = get_trade_date(trade_date, 1)
    trade_date_3y = get_trade_date(trade_date, 3)
    trade_date_4y = get_trade_date(trade_date, 4)
    trade_date_5y = get_trade_date(trade_date, 5)

    # report data
    indicator_sets = engine.fetch_fundamentals_pit_extend_company_id(IndicatorReport,
                                                                     [IndicatorReport.FCFF,
                                                                      ], dates=[trade_date]).drop(columns, axis=1)
    indicator_sets = indicator_sets.rename(columns={
        'FCFF': 'enterprise_fcfps',  # 企业自由现金流
    })

    balance_sets = engine.fetch_fundamentals_pit_extend_company_id(BalanceReport,
                                                                   [BalanceReport.TOTASSET,
                                                                    ], dates=[trade_date]).drop(columns, axis=1)
    balance_sets = balance_sets.rename(columns={
        'TOTASSET': 'total_assets_report',  # 资产总计
    })
    valuation_report_sets = pd.merge(indicator_sets, balance_sets, how='outer', on='security_code')
    print('valuation_report_sets')

    # MRQ data
    cash_flow_mrq = engine.fetch_fundamentals_pit_extend_company_id(CashFlowMRQ,
                                                                    [CashFlowMRQ.FINALCASHBALA,
                                                                     ], dates=[trade_date]).drop(columns, axis=1)
    cash_flow_mrq = cash_flow_mrq.rename(columns={
        'FINALCASHBALA': 'cash_and_equivalents_at_end',  # 期末现金及现金等价物余额

    })
    balance_mrq = engine.fetch_fundamentals_pit_extend_company_id(BalanceMRQ,
                                                                  [BalanceMRQ.LONGBORR,
                                                                   BalanceMRQ.TOTASSET,
                                                                   BalanceMRQ.SHORTTERMBORR,
                                                                   BalanceMRQ.PARESHARRIGH,
                                                                   ], dates=[trade_date]).drop(columns, axis=1)
    balance_mrq = balance_mrq.rename(columns={
        'SHORTTERMBORR': 'shortterm_loan',  # 短期借款
        'LONGBORR': 'longterm_loan',  # 长期借款
        'TOTASSET': 'total_assets',  # 资产总计
        'PARESHARRIGH': 'equities_parent_company_owners',  # 归属于母公司股东权益合计
    })
    valuation_mrq = pd.merge(cash_flow_mrq, balance_mrq, on='security_code')
    print('valuation_mrq')

    # TTM data
    # 总市值合并到TTM数据中，
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
                                                                       IncomeTTM.BIZTOTINCO,
                                                                       IncomeTTM.BIZINCO,
                                                                       IncomeTTM.TOTPROFIT,
                                                                       ], dates=[trade_date]).drop(columns, axis=1)
    income_ttm_sets = income_ttm_sets.rename(columns={
        'TOTPROFIT': 'total_profit',  # 利润总额 ttm
        'NETPROFIT': 'net_profit',  # 净利润
        'PARENETP': 'np_parent_company_owners',  # 归属于母公司所有者的净利润
        'BIZTOTINCO': 'total_operating_revenue',  # 营业总收入
        'BIZINCO': 'operating_revenue',  # 营业收入
    })

    income_ttm_sets_3 = engine.fetch_fundamentals_pit_extend_company_id(IncomeTTM,
                                                                        [IncomeTTM.PARENETP,
                                                                         ], dates=[trade_date_3y]).drop(columns, axis=1)
    income_ttm_sets_3 = income_ttm_sets_3.rename(columns={
        'PARENETP': 'np_parent_company_owners_3',  # 归属于母公司所有者的净利润
    })

    income_ttm_sets_5 = engine.fetch_fundamentals_pit_extend_company_id(IncomeTTM,
                                                                        [IncomeTTM.PARENETP,
                                                                         ], dates=[trade_date_5y]).drop(columns, axis=1)
    income_ttm_sets_5 = income_ttm_sets_5.rename(columns={
        'PARENETP': 'np_parent_company_owners_5',  # 归属于母公司所有者的净利润
    })

    valuation_ttm_sets = pd.merge(cash_flow_ttm_sets, income_ttm_sets, how='outer', on='security_code')
    valuation_ttm_sets = pd.merge(valuation_ttm_sets, indicator_ttm_sets, how='outer', on='security_code')
    valuation_ttm_sets = pd.merge(valuation_ttm_sets, income_ttm_sets_3, how='outer', on='security_code')
    valuation_ttm_sets = pd.merge(valuation_ttm_sets, income_ttm_sets_5, how='outer', on='security_code')

    # PS, PE, PB, PCF
    column = ['trade_date']
    valuation_sets = get_fundamentals(query(Valuation.security_code,
                                            Valuation.trade_date,
                                            Valuation.pe,
                                            Valuation.ps,
                                            Valuation.pb,
                                            Valuation.pcf,
                                            Valuation.market_cap,
                                            Valuation.circulating_market_cap)
                                      .filter(Valuation.trade_date.in_([trade_date]))).drop(column, axis=1)

    trade_date_6m = get_trade_date(trade_date, 1, 180)
    trade_date_3m = get_trade_date(trade_date, 1, 90)
    trade_date_2m = get_trade_date(trade_date, 1, 60)

    pe_set = get_fundamentals(query(Valuation.security_code,
                                    Valuation.trade_date,
                                    Valuation.pe,).filter(Valuation.trade_date.in_([trade_date]))).drop(column, axis=1)

    pe_sets_6m = get_fundamentals(query(Valuation.security_code,
                                        Valuation.trade_date,
                                        Valuation.pe)
                                  .filter(Valuation.trade_date.between(trade_date_6m, trade_date))).drop(column, axis=1)
    pe_sets_6m = pe_sets_6m.groupby('security_code').mean().rename(columns={'pe': 'pe_mean_6m'})

    pe_sets_3m = get_fundamentals(query(Valuation.security_code,
                                        Valuation.trade_date,
                                        Valuation.pe)
                                  .filter(Valuation.trade_date.between(trade_date_3m, trade_date))).drop(column, axis=1)
    pe_sets_3m = pe_sets_3m.groupby('security_code').mean().rename(columns={'pe': 'pe_mean_3m'})

    pe_sets_2m = get_fundamentals(query(Valuation.security_code,
                                        Valuation.trade_date,
                                        Valuation.pe)
                                  .filter(Valuation.trade_date.between(trade_date_2m, trade_date))).drop(column, axis=1)
    pe_sets_2m = pe_sets_2m.groupby('security_code').mean().rename(columns={'pe': 'pe_mean_2m'})

    pe_sets_1y = get_fundamentals(query(Valuation.security_code,
                                        Valuation.trade_date,
                                        Valuation.pe)
                                  .filter(Valuation.trade_date.between(trade_date_1y, trade_date))).drop(column, axis=1)
    pe_sets_1y = pe_sets_1y.groupby('security_code').mean().rename(columns={'pe': 'pe_mean_1y'})

    pe_sets = pd.merge(pe_sets_6m, pe_sets_3m, how='outer', on='security_code')
    pe_sets = pd.merge(pe_sets, pe_sets_2m, how='outer', on='security_code')
    pe_sets = pd.merge(pe_sets, pe_sets_1y, how='outer', on='security_code')
    pe_sets = pd.merge(pe_sets, pe_set, how='outer', on='security_code')

    industry_set = ['801010', '801020', '801030', '801040', '801050', '801080', '801110', '801120', '801130',
                    '801140', '801150', '801160', '801170', '801180', '801200', '801210', '801230', '801710',
                    '801720', '801730', '801740', '801750', '801760', '801770', '801780', '801790', '801880',
                    '801890']
    column_sw = ['trade_date', 'symbol', 'company_id']
    sw_indu = get_fundamentals_extend_internal(query(Industry.trade_date,
                                                     Industry.symbol,
                                                     Industry.isymbol)
                                               .filter(Industry.trade_date.in_([trade_date])),
                                               internal_type='symbol').drop(column_sw, axis=1)
    print('sw_indu')
    sw_indu = sw_indu[sw_indu['isymbol'].isin(industry_set)]

    valuation_sets = pd.merge(valuation_sets, valuation_report_sets, how='outer', on='security_code')
    valuation_sets = pd.merge(valuation_sets, valuation_mrq, how='outer', on='security_code')
    valuation_sets = pd.merge(valuation_sets, valuation_ttm_sets, how='outer', on='security_code')
    print('valuation_sets')
    # valuation_sets = valuation_sets.drop('trade_date', axis=1)
    # pe_sets = pe_sets.drop('trade_date', axis=1)

    return valuation_sets, sw_indu, pe_sets


def prepare_calculate_remote(trade_date):
    # historical_value
    valuation_sets, sw_indu, pe_sets = get_basic_history_value_data(trade_date)
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


def prepare_calculate_local(trade_date, factor_name):
    # historical_value
    tic = time.time()
    valuation_sets, sw_indu, pe_sets = get_basic_history_value_data(trade_date)
    print('data_read_time: %s' % (time.time()-tic))
    print('len_of_valuation_sets: %s' % len(valuation_sets))
    if len(valuation_sets) <= 0:
        print("%s has no data" % trade_date)
        return
    else:
        factor_valuation.calculate(trade_date, valuation_sets, sw_indu, pe_sets, factor_name)
    time2 = time.time()
    print('history_cal_time:{}'.format(time2 - tic))


def do_update(start_date, end_date, count, factor_name):
    # 读取本地交易日
    syn_util = SyncUtil()
    trade_date_sets = syn_util.get_trades_ago('001002', start_date, end_date, count, order='DESC')
    trade_date_sets = trade_date_sets['TRADEDATE'].values
    # print('交易日：%s' % trade_date_sets)
    for trade_date in trade_date_sets:
        print('因子计算日期： %s' % trade_date)
        prepare_calculate_local(trade_date, factor_name)
    print('----->')


if __name__ == '__main__':
    # parser = argparse.ArgumentParser()
    # parser.add_argument('--start_date', type=int, default=20070101)
    # parser.add_argument('--end_date', type=int, default=0)
    # parser.add_argument('--count', type=int, default=1)
    # parser.add_argument('--rebuild', type=bool, default=False)
    # parser.add_argument('--update', type=bool, default=False)
    # parser.add_argument('--schedule', type=bool, default=False)
    # factor_name = 'factor_valuation'
    #
    # args = parser.parse_args()
    # if args.end_date == 0:
    #     end_date = int(datetime.now().date().strftime('%Y%m%d'))
    # else:
    #     end_date = args.end_date
    # if args.rebuild:
    #     processor = factor_valuation.Valuation(factor_name)
    #     processor.create_dest_tables()
    #     do_update(args.start_date, end_date, args.count, factor_name)
    # if args.update:
    #     do_update(args.start_date, end_date, args.count, factor_name)
    do_update('20160819', '20190101', 10, 'factor_valuation')

