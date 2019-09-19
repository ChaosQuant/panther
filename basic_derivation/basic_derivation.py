#! /usr/bin/env python
# -*- coding: utf-8 -*-

"""
@version: ??
@author: li
@file: capital_structure.py
@time: 2019-09-04 13:06
"""
import gc
import sys
sys.path.append('../')
sys.path.append('../../')
sys.path.append('../../../')
import time
import argparse
from datetime import datetime
from basic_derivation import factor_basic_derivation
import pandas as pd

from data.model import BalanceMRQ
from data.model import CashFlowMRQ, CashFlowTTM
from data.model import IndicatorMRQ, IndicatorTTM
from data.model import IncomeMRQ, IncomeTTM

# from vision.file_unit.valuation import Valuation
from data.sqlengine import sqlEngine
from utilities.sync_util import SyncUtil
# from client.utillities.sync_util import SyncUtilfrom
# ultron.cluster.invoke.cache_data import cache_data
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


def get_basic_data(trade_date):
    engine = sqlEngine()
    maplist = {
        # cashflow
        'FINALCASHBALA':'',
        'MANANETR':'',
        'LABORGETCASH':'',
        'FINNETCFLOW':'',
        'CASHNETI':'',

        # income
        'BIZTOTINCO':'',
        'BIZTOTCOST':'',
        'BIZINCO':'',
        'SALESEXPE':'',
        'MANAEXPE':'',
        'FINEXPE':'',
        'INTEEXPE':'',
        'ASSEIMPALOSS':'',
        'PERPROFIT':'',
        'TOTPROFIT':'',
        'NETPROFIT':'',
        'PARENETP':'',
        'BIZTAX':'',

        # balance
        'SHORTTERMBORR': 'shortterm_loan',  # 短期借款
        'DUENONCLIAB': 'non_current_liability_in_one_year',  # 一年内到期的非流动负债
        'LONGBORR': 'longterm_loan',  # 长期借款
        'BDSPAYA': 'bonds_payable',  # 应付债券
        'INTEPAYA': 'interest_payable',  # 应付利息
        'PARESHARRIGH':'',
        'TOTASSET': 'total_assets',        # 资产总计
        'FIXEDASSECLEATOT':'',
        'TOTLIAB':'',
        'RIGHAGGR':'',

        # indicator
        'FCFF':'',
        'FCFE':'',
        'NEGAL':'',
        'NOPI':'',
        'WORKCAP':'',
        'RETAINEDEAR':'',
        'NDEBT':'',
        'NONINTCURLIABS':'',
        'NONINTNONCURLIAB':'',
        'CURDEPANDAMOR':'',
        'TOTIC':'',
        'OPGPMARGIN':'',
        'NPCUT':'',
        }
    # 读取目前涉及到的因子
    columns = ['COMPCODE', 'PUBLISHDATE', 'ENDDATE', 'symbol', 'company_id', 'trade_date']

    cash_flow_sets = engine.fetch_fundamentals_pit_extend_company_id(CashFlowMRQ,
                                                                     [CashFlowMRQ.FINALCASHBALA,
                                                                      ], dates=[trade_date]).drop(columns, axis=1)

    balance_sets = engine.fetch_fundamentals_pit_extend_company_id(BalanceMRQ,
                                                                   [BalanceMRQ.SHORTTERMBORR,
                                                                    BalanceMRQ.DUENONCLIAB,
                                                                    BalanceMRQ.LONGBORR,
                                                                    BalanceMRQ.BDSPAYA,
                                                                    BalanceMRQ.INTEPAYA,
                                                                    BalanceMRQ.PARESHARRIGH,
                                                                    BalanceMRQ.TOTASSET,
                                                                    BalanceMRQ.FIXEDASSECLEATOT,
                                                                    BalanceMRQ.TOTLIAB,
                                                                    BalanceMRQ.RIGHAGGR,
                                                                    BalanceMRQ.INTAASSET,
                                                                    BalanceMRQ.DEVEEXPE,
                                                                    BalanceMRQ.GOODWILL,
                                                                    BalanceMRQ.LOGPREPEXPE,
                                                                    BalanceMRQ.DEFETAXASSET,
                                                                    BalanceMRQ.MINYSHARRIGH,
                                                                    ], dates=[trade_date]).drop(columns, axis=1)

    balance_sets = balance_sets.rename(columns={
        'SHORTTERMBORR': 'shortterm_loan',  # 短期借款
        'DUENONCLIAB': 'non_current_liability_in_one_year',  # 一年内到期的非流动负债
        'LONGBORR': 'longterm_loan',  # 长期借款
        'BDSPAYA': 'bonds_payable',  # 应付债券
        'INTEPAYA': 'interest_payable',  # 应付利息
        })

    indicator_sets = engine.fetch_fundamentals_pit_extend_company_id(IndicatorMRQ,
                                                                     [IndicatorMRQ.FCFF,
                                                                      IndicatorMRQ.FCFE,
                                                                      IndicatorMRQ.NEGAL,
                                                                      IndicatorMRQ.NOPI,
                                                                      IndicatorMRQ.WORKCAP,
                                                                      IndicatorMRQ.RETAINEDEAR,
                                                                      IndicatorMRQ.NDEBT,
                                                                      IndicatorMRQ.NONINTCURLIABS,
                                                                      IndicatorMRQ.NONINTNONCURLIAB,
                                                                      IndicatorMRQ.CURDEPANDAMOR,
                                                                      IndicatorMRQ.TOTIC,
                                                                      IndicatorMRQ.EBIT,
                                                                      ], dates=[trade_date]).drop(columns, axis=1)

    income_sets = engine.fetch_fundamentals_pit_extend_company_id(IncomeMRQ,
                                                                  [IncomeMRQ.INCOTAXEXPE,
                                                                   ], dates=[trade_date]).drop(columns, axis=1)

    tp_detivation = pd.merge(cash_flow_sets, balance_sets, how='outer', on='security_code')
    tp_detivation = pd.merge(indicator_sets, tp_detivation, how='outer', on='security_code')
    tp_detivation = pd.merge(income_sets, tp_detivation, how='outer', on='security_code')

    # balance_ttm_sets = engine.fetch_fundamentals_pit_extend_company_id(BalanceTTM,
    #                                                                    [BalanceTTM.MINYSHARRIGH,
    #                                                                    ], dates=[trade_date]).drop(columns, axis=1)

    income_ttm_sets = engine.fetch_fundamentals_pit_extend_company_id(IncomeTTM,
                                                                      [IncomeTTM.BIZTOTINCO,
                                                                       IncomeTTM.BIZTOTCOST,
                                                                       IncomeTTM.BIZINCO,
                                                                       IncomeTTM.SALESEXPE,
                                                                       IncomeTTM.MANAEXPE,
                                                                       IncomeTTM.FINEXPE,
                                                                       IncomeTTM.INTEEXPE,
                                                                       IncomeTTM.ASSEIMPALOSS,
                                                                       IncomeTTM.PERPROFIT,
                                                                       IncomeTTM.TOTPROFIT,
                                                                       IncomeTTM.NETPROFIT,
                                                                       IncomeTTM.PARENETP,
                                                                       IncomeTTM.BIZTAX,
                                                                       IncomeTTM.NONOREVE,
                                                                       IncomeTTM.NONOEXPE,
                                                                       IncomeTTM.MINYSHARRIGH,
                                                                       IncomeTTM.INCOTAXEXPE,
                                                                       ], dates=[trade_date]).drop(columns, axis=1)
    income_ttm_sets = income_ttm_sets.rename(columns={'MINYSHARRIGH':'minority_profit'})

    cash_flow_ttm_sets = engine.fetch_fundamentals_pit_extend_company_id(CashFlowTTM,
                                                                         [CashFlowTTM.MANANETR,
                                                                          CashFlowTTM.LABORGETCASH,
                                                                          CashFlowTTM.INVNETCASHFLOW,
                                                                          CashFlowTTM.FINNETCFLOW,
                                                                          CashFlowTTM.CASHNETI,
                                                                          ], dates=[trade_date]).drop(columns, axis=1)

    indicator_ttm_sets = engine.fetch_fundamentals_pit_extend_company_id(IndicatorTTM,
                                                                         [IndicatorTTM.OPGPMARGIN,
                                                                          IndicatorTTM.NPCUT,
                                                                          IndicatorTTM.NVALCHGIT,
                                                                          IndicatorTTM.EBITDA,
                                                                          IndicatorTTM.EBIT,
                                                                          IndicatorTTM.EBITFORP,
                                                                          ], dates=[trade_date]).drop(columns, axis=1)

    ttm_derivation = pd.merge(income_ttm_sets, cash_flow_ttm_sets, how='outer', on='security_code')
    ttm_derivation = pd.merge(indicator_ttm_sets, ttm_derivation, how='outer', on='security_code')

    return tp_detivation, ttm_derivation


def prepare_calculate_local(trade_date, factor_name):
    tic = time.time()
    tp_detivation, ttm_derivation = get_basic_data(trade_date)
    print('tp_derivation')
    print(tp_detivation.head())
    print('ttm_derivation')
    print(ttm_derivation.head())
    if len(tp_detivation) <= 0 or len(ttm_derivation) <= 0:
        print("%s has no data" % trade_date)
        return
    else:
        factor_basic_derivation.calculate(trade_date, tp_detivation, ttm_derivation, factor_name)
    time4 = time.time()
    print('basic_derivation_cal_time:{}'.format(time4 - tic))
    del tp_detivation, ttm_derivation
    gc.collect()


def prepare_calculate_remote(trade_date):
    tp_detivation, ttm_derivation = get_basic_data(trade_date)
    if len(tp_detivation) <= 0 or len(ttm_derivation) <= 0:
        print("%s has no data" % trade_date)
        return
    else:
        tic = time.time()
        session = str(int(time.time() * 1000000 + datetime.now().microsecond))
        cache_data.set_cache(session + str(trade_date) + "1", trade_date, tp_management.to_json(orient='records'))
        factor_basic_derivation.factor_calculate(date_index=trade_date, session=session)
        time4 = time.time()
        print('management_cal_time:{}'.format(time4 - tic))


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
    parser = argparse.ArgumentParser()
    parser.add_argument('--start_date', type=int, default=20070101)
    parser.add_argument('--end_date', type=int, default=0)
    parser.add_argument('--count', type=int, default=1)
    parser.add_argument('--rebuild', type=bool, default=False)
    parser.add_argument('--update', type=bool, default=False)
    parser.add_argument('--schedule', type=bool, default=False)
    factor_name = 'factor_derivation'

    args = parser.parse_args()
    if args.end_date == 0:
        end_date = int(datetime.now().date().strftime('%Y%m%d'))
    else:
        end_date = args.end_date
    if args.rebuild:
        processor = factor_basic_derivation.Derivation(factor_name)
        processor.create_dest_tables()
        do_update(args.start_date, end_date, args.count, factor_name)
    if args.update:
        do_update(args.start_date, end_date, args.count, factor_name)
    # do_update('20190819', '20190823', 10)

