#! /usr/bin/env python
# -*- coding: utf-8 -*-

"""
@version: ??
@author: li
@file: earning.py
@time: 2019-09-03 14:46
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
from factor import factor_earning
from client.dbmodel.model import BalanceMRQ, BalanceTTM, BalanceReport
from client.dbmodel.model import CashFlowMRQ, CashFlowTTM, CashFlowReport
from client.dbmodel.model import IndicatorReport, IndicatorMRQ, IndicatorTTM
from client.dbmodel.model import IncomeMRQ, IncomeReport, IncomeTTM
# from vision.file_unit.valuation import Valuation

from client.engines.sqlengine import sqlEngine
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
    # 读取目前涉及到的因子
    maplist = {
        # cash flow
        'LABORGETCASH': 'goods_sale_and_service_render_cash',  # 销售商品、提供劳务收到的现金
        'FINALCASHBALA': 'cash_and_equivalents_at_end',  # 期末现金及现金等价物余额
        'FINNETCFLOW': 'net_finance_cash_flow', # 筹资活动产生的现金流量净

        # income
        'BIZTOTINCO': 'total_operating_revenue',  # 营业总收入
        'BIZTOTCOST': 'total_operating_cost',  # 营业总成本
        'ASSOINVEPROF':'invest_income_associates',  # 对联营企业和合营企业的投资收益
        'NONOREVE':'non_operating_revenue',  # 营业外收入1
        'NONOEXPE':'non_operating_expense',  # 营业外支出1
        'TOTPROFIT':'total_profit',  # 利润总额
        'NETPROFIT':'net_profit',   # 净利润
        'PARENETP': 'np_parent_company_owners',  # 归属于母公司所有者的净利润
        'BIZINCO': 'operating_revenue',  # 营业收入
        'INTEINCO':'interest_income',  # 利息收入
        'BIZCOST':'operating_cost',  # 营业成本
        'FINEXPE':'financial_expense',  # 财务费用
        'PERPROFIT':'operating_profit',  # 营业利润
        'MANAEXPE':'administration_expense',  # 管理费用
        'SALESEXPE':'sale_expense',  # 销售费用
        'BIZTAX':'operating_tax_surcharges',  # 营业税金及附加1
        'ASSEIMPALOSS':'asset_impairment_loss',  # 资产减值损失

        # balance
        'PARESHARRIGH': 'equities_parent_company_owners',  # 归属于母公司股东权益合计
        'RIGHAGGR':'total_owner_equities',  # 所有者权益（或股东权益）合计
        'TOTASSET': 'total_assets',  # 资产总计
        'LONGBORR': 'longterm_loan',  # 长期借款

        # indicator
        'NETPROFITCUT':'adjusted_profit',  # 扣除非经常损益后的净利润1

        # valuation
        '':'circulating_market_cap'  # 流通市值1
        }

    trade_date_pre_year = get_trade_date(trade_date, 1)
    trade_date_pre_year_2 = get_trade_date(trade_date, 2)
    trade_date_pre_year_3 = get_trade_date(trade_date, 3)
    trade_date_pre_year_4 = get_trade_date(trade_date, 4)
    trade_date_pre_year_5 = get_trade_date(trade_date, 5)

    engine = sqlEngine()
    columns = ['COMPCODE', 'PUBLISHDATE', 'ENDDATE', 'symbol', 'company_id', 'trade_date']

    # Report Data
    cash_flow_sets = engine.fetch_fundamentals_pit_extend_company_id(CashFlowReport,
                                                                     [CashFlowReport.LABORGETCASH,
                                                                      CashFlowReport.FINALCASHBALA,
                                                                      ], dates=[trade_date]).drop(columns, axis=1)
    cash_flow_sets = cash_flow_sets.rename(columns={'LABORGETCASH': 'goods_sale_and_service_render_cash',   # 销售商品、提供劳务收到的现金
                                                    'FINALCASHBALA': 'cash_and_equivalents_at_end',  # 期末现金及现金等价物余额
                                                    })

    income_sets = engine.fetch_fundamentals_pit_extend_company_id(IncomeReport,
                                                                  [IncomeReport.BIZTOTINCO,
                                                                   IncomeReport.BIZINCO,
                                                                   IncomeReport.PERPROFIT,
                                                                   IncomeReport.PARENETP,
                                                                   IncomeReport.NETPROFIT,
                                                                   ], dates=[trade_date]).drop(columns, axis=1)
    income_sets = income_sets.rename(columns={'NETPROFIT': 'net_profit',   # 净利润
                                              'BIZTOTINCO': 'total_operating_revenue',  # 营业总收入
                                              'BIZINCO': 'operating_revenue',  # 营业收入
                                              'PERPROFIT': 'operating_profit',  # 营业利润
                                              'PARENETP': 'np_parent_company_owners',  # 归属于母公司所有者的净利润
                                              })

    indicator_sets = engine.fetch_fundamentals_pit_extend_company_id(IndicatorReport,
                                                                     [
                                                                      IndicatorReport.NETPROFITCUT,  # 扣除非经常损益后的净利润
                                                                      IndicatorReport.MGTEXPRT
                                                                      ], dates=[trade_date]).drop(columns, axis=1)
    indicator_sets = indicator_sets.rename(columns={'NETPROFITCUT': 'adjusted_profit',  # 扣除非经常损益后的净利润
                                                    })

    balance_sets = engine.fetch_fundamentals_pit_extend_company_id(BalanceReport,
                                                                   [BalanceReport.PARESHARRIGH,
                                                                    ], dates=[trade_date]).drop(columns, axis=1)
    balance_sets = balance_sets.rename(columns={'PARESHARRIGH': 'equities_parent_company_owners',   # 归属于母公司股东权益合计
                                                })

    income_sets_pre_year_1 = engine.fetch_fundamentals_pit_extend_company_id(IncomeReport,
                                                                             [IncomeReport.BIZINCO,   # 营业收入
                                                                              IncomeReport.NETPROFIT,  # 净利润
                                                                              ], dates=[trade_date_pre_year]).drop(columns, axis=1)
    income_sets_pre_year_1 = income_sets_pre_year_1.rename(columns={'NETPROFIT': 'net_profit_pre_year_1',   # 净利润
                                                                    'BIZINCO': 'operating_revenue_pre_year_1',  # 营业收入
                                                                    })

    income_sets_pre_year_2 = engine.fetch_fundamentals_pit_extend_company_id(IncomeReport,
                                                                             [IncomeReport.BIZINCO,
                                                                              IncomeReport.NETPROFIT,
                                                                              ], dates=[trade_date_pre_year_2]).drop(columns, axis=1)
    income_sets_pre_year_2 = income_sets_pre_year_2.rename(columns={'NETPROFIT': 'net_profit_pre_year_2',   # 净利润
                                                                    'BIZINCO': 'operating_revenue_pre_year_2',  # 营业收入
                                                                    })

    income_sets_pre_year_3 = engine.fetch_fundamentals_pit_extend_company_id(IncomeReport,
                                                                             [IncomeReport.BIZINCO,
                                                                              IncomeReport.NETPROFIT,
                                                                              ], dates=[trade_date_pre_year_3]).drop(columns, axis=1)
    income_sets_pre_year_3 = income_sets_pre_year_3.rename(columns={'NETPROFIT': 'net_profit_pre_year_3',   # 净利润
                                                                    'BIZINCO': 'operating_revenue_pre_year_3',  # 营业收入
                                                                    })

    income_sets_pre_year_4 = engine.fetch_fundamentals_pit_extend_company_id(IncomeReport,
                                                                             [IncomeReport.BIZINCO,
                                                                              IncomeReport.NETPROFIT,
                                                                              ], dates=[trade_date_pre_year_4]).drop(columns, axis=1)
    income_sets_pre_year_4 = income_sets_pre_year_4.rename(columns={'NETPROFIT': 'net_profit_pre_year_4',   # 净利润
                                                                    'BIZINCO': 'operating_revenue_pre_year_4',  # 营业收入
                                                                    })

    tp_earning = pd.merge(cash_flow_sets, income_sets, how='outer',  on='security_code')
    tp_earning = pd.merge(indicator_sets, tp_earning, how='outer', on='security_code')
    tp_earning = pd.merge(balance_sets, tp_earning, how='outer', on='security_code')
    tp_earning = pd.merge(income_sets_pre_year_1, tp_earning, how='outer', on='security_code')
    tp_earning = pd.merge(income_sets_pre_year_2, tp_earning, how='outer', on='security_code')
    tp_earning = pd.merge(income_sets_pre_year_3, tp_earning, how='outer', on='security_code')
    tp_earning = pd.merge(income_sets_pre_year_4, tp_earning, how='outer', on='security_code')

    # MRQ
    balance_mrq_sets = engine.fetch_fundamentals_pit_extend_company_id(BalanceMRQ,
                                                                       [BalanceMRQ.TOTASSET,  # 资产总计
                                                                        BalanceMRQ.PARESHARRIGH,  # 归属于母公司股东权益合计
                                                                        BalanceMRQ.RIGHAGGR,  # 所有者权益（或股东权益）合计
                                                                        BalanceMRQ.LONGBORR,  # 长期借款
                                                                        ], dates=[trade_date]).drop(columns, axis=1)
    balance_mrq_sets = balance_mrq_sets.rename(columns={'TOTASSET': 'total_assets_mrq',
                                                        'PARESHARRIGH': 'equities_parent_company_owners_mrq',  # 归属于母公司股东权益合计
                                                        'RIGHAGGR': 'total_owner_equities_mrq',  # 所有者权益（或股东权益）合计
                                                        'LONGBORR': 'longterm_loan_mrq',  # 长期借款
                                                        })

    balance_mrq_sets_pre = engine.fetch_fundamentals_pit_extend_company_id(BalanceMRQ,
                                                                           [BalanceMRQ.TOTASSET,  # 资产总计
                                                                            BalanceMRQ.RIGHAGGR,  # 所有者权益(或股东权益)合计
                                                                            BalanceMRQ.LONGBORR,  # 长期借款
                                                                            ], dates=[trade_date]).drop(columns, axis=1)
    balance_mrq_sets_pre = balance_mrq_sets_pre.rename(columns={'TOTASSET': 'total_assets_mrq_pre',
                                                                'RIGHAGGR': 'total_owner_equities_mrq_pre',  # 所有者权益（或股东权益）合计
                                                                'LONGBORR': 'longterm_loan_mrq_pre',  # 长期借款
                                                                })

    # TTM Data
    cash_flow_ttm_sets = engine.fetch_fundamentals_pit_extend_company_id(CashFlowTTM,
                                                                         [CashFlowTTM.FINNETCFLOW,
                                                                          ], dates=[trade_date]).drop(columns, axis=1)
    cash_flow_ttm_sets = cash_flow_ttm_sets.rename(columns={'FINNETCFLOW': 'net_finance_cash_flow'})

    income_ttm_sets = engine.fetch_fundamentals_pit_extend_company_id(IncomeTTM,
                                                                      [IncomeTTM.BIZINCO,    # 营业收入
                                                                       IncomeTTM.NETPROFIT,   # 净利润
                                                                       IncomeTTM.MANAEXPE,  # 管理费用
                                                                       IncomeTTM.BIZTOTINCO,   # 营业总收入
                                                                       IncomeTTM.TOTPROFIT,   # 利润总额
                                                                       IncomeTTM.FINEXPE,  # 财务费用
                                                                       IncomeTTM.INTEINCO,  # 利息收入
                                                                       IncomeTTM.SALESEXPE,   # 销售费用
                                                                       IncomeTTM.BIZTOTCOST,   # 营业总成本
                                                                       IncomeTTM.PERPROFIT,   # 营业利润
                                                                       IncomeTTM.PARENETP,   # 归属于母公司所有者的净利润
                                                                       IncomeTTM.BIZCOST,     # 营业成本
                                                                       IncomeTTM.ASSOINVEPROF,  # 对联营企业和合营企业的投资收益
                                                                       IncomeTTM.BIZTAX,  # 营业税金及附加
                                                                       IncomeTTM.ASSEIMPALOSS,  # 资产减值损失
                                                                       ], dates=[trade_date]).drop(columns, axis=1)

    income_ttm_sets = income_ttm_sets.rename(columns={'BIZINCO': 'operating_revenue',  # 营业收入
                                                      'NETPROFIT': 'net_profit',  # 净利润
                                                      'MANAEXPE': 'administration_expense',  # 管理费用
                                                      'BIZTOTINCO': 'total_operating_revenue',  # 营业总收入
                                                      'TOTPROFIT': 'total_profit',  # 利润总额
                                                      'FINEXPE': 'financial_expense',  # 财务费用
                                                      'INTEINCO': 'interest_income',  # 利息收入
                                                      'SALESEXPE': 'sale_expense',  # 销售费用
                                                      'BIZTOTCOST': 'total_operating_cost',  # 营业总成本
                                                      'PERPROFIT': 'operating_profit',  # 营业利润
                                                      'PARENETP': 'np_parent_company_owners',  # 归属于母公司所有者的净利润
                                                      'BIZCOST': 'operating_cost',  # 营业成本
                                                      'ASSOINVEPROF': 'invest_income_associates',  # 对联营企业和合营企业的投资收益
                                                      'BIZTAX': 'operating_tax_surcharges',  # 营业税金及附加
                                                      'ASSEIMPALOSS': 'asset_impairment_loss',  # 资产减值损失
                                                      })

    balance_ttm_sets = engine.fetch_fundamentals_pit_extend_company_id(BalanceTTM,
                                                                       [BalanceTTM.TOTASSET,  # 资产总计
                                                                        BalanceTTM.RIGHAGGR,  # 所有者权益（或股东权益）合计
                                                                        BalanceTTM.PARESHARRIGH,   # 归属于母公司股东权益合计
                                                                        ], dates=[trade_date]).drop(columns, axis=1)
    balance_ttm_sets = balance_ttm_sets.rename(columns={'PARESHARRIGH': 'equities_parent_company_owners',   # 归属于母公司股东权益合计
                                                        'RIGHAGGR':'total_owner_equities',  # 所有者权益（或股东权益）合计
                                                        'TOTASSET': 'total_assets',  # 资产总计
                                                        })

    income_ttm_sets_pre_year_1 = engine.fetch_fundamentals_pit_extend_company_id(IncomeTTM,
                                                                                 [IncomeTTM.BIZINCO,
                                                                                  IncomeTTM.NETPROFIT,
                                                                                  ], dates=[trade_date_pre_year]).drop(columns, axis=1)
    income_ttm_sets_pre_year_1 = income_ttm_sets_pre_year_1.rename(columns={'BIZINCO': 'operating_revenue_pre_year_1',  # 营业收入
                                                                            'NETPROFIT': 'net_profit_pre_year_1',  # 净利润
                                                                            })

    income_ttm_sets_pre_year_2 = engine.fetch_fundamentals_pit_extend_company_id(IncomeTTM,
                                                                                 [IncomeTTM.BIZINCO,
                                                                                  IncomeTTM.NETPROFIT,
                                                                                  ], dates=[trade_date_pre_year_2]).drop(columns, axis=1)
    income_ttm_sets_pre_year_2 = income_ttm_sets_pre_year_2.rename(columns={'BIZINCO': 'operating_revenue_pre_year_2',  # 营业收入
                                                                            'NETPROFIT': 'net_profit_pre_year_2',  # 净利润
                                                                            })

    income_ttm_sets_pre_year_3 = engine.fetch_fundamentals_pit_extend_company_id(IncomeTTM,
                                                                                 [IncomeTTM.BIZINCO,
                                                                                  IncomeTTM.NETPROFIT,
                                                                                  ], dates=[trade_date_pre_year_3]).drop(columns, axis=1)
    income_ttm_sets_pre_year_3 = income_ttm_sets_pre_year_3.rename(columns={'BIZINCO': 'operating_revenue_pre_year_3',  # 营业收入
                                                                            'NETPROFIT': 'net_profit_pre_year_3',  # 净利润
                                                                            })

    income_ttm_sets_pre_year_4 = engine.fetch_fundamentals_pit_extend_company_id(IncomeTTM,
                                                                                 [IncomeTTM.BIZINCO,
                                                                                  IncomeTTM.NETPROFIT,
                                                                                  ], dates=[trade_date_pre_year_4]).drop(columns, axis=1)
    income_ttm_sets_pre_year_4 = income_ttm_sets_pre_year_4.rename(columns={'BIZINCO': 'operating_revenue_pre_year_4',  # 营业收入
                                                                            'NETPROFIT': 'net_profit_pre_year_4',  # 净利润
                                                                            })

    # indicator_ttm_sets = engine.fetch_fundamentals_pit_extend_company_id(IndicatorTTM,
    #                                                                      [IndicatorTTM.ROIC,   # 投入资本回报率
    #                                                                       ], dates=[trade_date]).drop(columns, axis=1)
    #
    # indicator_ttm_sets = indicator_ttm_sets.rename(columns={'ROIC': '',
    #                                                         })

    ttm_earning = pd.merge(income_ttm_sets, balance_ttm_sets, how='outer', on='security_code')
    ttm_earning = pd.merge(ttm_earning, cash_flow_ttm_sets, how='outer', on='security_code')
    ttm_earning = pd.merge(ttm_earning, income_ttm_sets_pre_year_1, how='outer', on='security_code')
    ttm_earning = pd.merge(ttm_earning, income_ttm_sets_pre_year_2, how='outer', on='security_code')
    ttm_earning = pd.merge(ttm_earning, income_ttm_sets_pre_year_3, how='outer', on='security_code')
    ttm_earning = pd.merge(ttm_earning, income_ttm_sets_pre_year_4, how='outer', on='security_code')
    ttm_earning = pd.merge(ttm_earning, balance_mrq_sets, how='outer', on='security_code')
    ttm_earning = pd.merge(ttm_earning, balance_mrq_sets_pre, how='outer', on='security_code')

    balance_con_sets = engine.fetch_fundamentals_pit_extend_company_id(BalanceTTM,
                                                                       [BalanceTTM.TOTASSET,  # 资产总计
                                                                        BalanceTTM.RIGHAGGR,  # 所有者权益（或股东权益）合计
                                                                        ],
                                                                       dates=[trade_date,
                                                                              trade_date_pre_year,
                                                                              trade_date_pre_year_2,
                                                                              trade_date_pre_year_3,
                                                                              trade_date_pre_year_4,
                                                                              ]).drop(columns, axis=1)
    balance_con_sets = balance_con_sets.groupby(['security_code'])
    balance_con_sets = balance_con_sets.sum()
    balance_con_sets = balance_con_sets.rename(columns={'TOTASSET':'total_assets',
                                                        'RIGHAGGR':'total_owner_equities'})

    # cash_flow_con_sets = engine.fetch_fundamentals_pit_extend_company_id(CashFlowReport,
    #                                                                      [CashFlowReport.FINALCASHBALA,
    #                                                                   ],
    #                                                                  dates=[trade_date,
    #                                                                         trade_date_pre_year,
    #                                                                         trade_date_pre_year_2,
    #                                                                         trade_date_pre_year_3,
    #                                                                         trade_date_pre_year_4,
    #                                                                         trade_date_pre_year_5,
    #                                                                         ]).drop(columns, axis=1)
    # cash_flow_con_sets = cash_flow_con_sets.groupby(['security_code'])
    # cash_flow_con_sets = cash_flow_con_sets.sum()
    # cash_flow_con_sets = cash_flow_con_sets.rename(columns={'FINALCASHBALA':'cash_and_equivalents_at_end'})

    income_con_sets = engine.fetch_fundamentals_pit_extend_company_id(IncomeReport,
                                                                      [IncomeReport.NETPROFIT,
                                                                       ],
                                                                      dates=[trade_date,
                                                                             trade_date_pre_year,
                                                                             trade_date_pre_year_2,
                                                                             trade_date_pre_year_3,
                                                                             trade_date_pre_year_4,
                                                                             trade_date_pre_year_5,
                                                                             ]).drop(columns, axis=1)
    income_con_sets = income_con_sets.groupby(['security_code'])
    income_con_sets = income_con_sets.sum()
    income_con_sets = income_con_sets.rename(columns={'NETPROFIT':'net_profit'}).reset_index()
    ttm_earning_5y = pd.merge(balance_con_sets, income_con_sets, how='outer', on='security_code')

    return tp_earning, ttm_earning, ttm_earning_5y


def prepare_calculate_local(trade_date):
    # local
    tic = time.time()
    tp_earning, ttm_earning, ttm_earning_5y = get_basic_data(trade_date)
    if len(tp_earning) <= 0 or len(ttm_earning_5y) <= 0 or len(ttm_earning) <= 0:
        print("%s has no data" % trade_date)
        return
    else:
        factor_earning.calculate(trade_date, tp_earning, ttm_earning, ttm_earning_5y)
    time6 = time.time()
    print('earning_cal_time:{}'.format(time6 - tic))


def prepare_calculate_remote(trade_date):
    # remote
    tp_earning, ttm_earning, ttm_earning_5y = get_basic_data(trade_date)
    if len(tp_earning) <= 0 or len(ttm_earning_5y) <= 0 or len(ttm_earning) <= 0:
        print("%s has no data" % trade_date)
        return
    else:
        tic = time.time()
        session = str(int(time.time() * 1000000 + datetime.now().microsecond))
        cache_data.set_cache(session + str(trade_date) + "1", trade_date, tp_earning.to_json(orient='records'))
        cache_data.set_cache(session + str(trade_date) + "2", trade_date, ttm_earning_5y.to_json(orient='records'))
        cache_data.set_cache(session + str(trade_date) + "3", trade_date, ttm_earning.to_json(orient='records'))
        factor_earning.factor_calculate.delay(date_index=trade_date, session=session)
        time6 = time.time()
        print('earning_cal_time:{}'.format(time6 - tic))


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
    #     processor = factor_earning.FactorEarning('factor_earning')
    #     processor.create_dest_tables()
    #     do_update(args.start_date, end_date, args.count)
    # if args.update:
    #     do_update(args.start_date, end_date, args.count)
    do_update('20190819', '20190823', 10)

