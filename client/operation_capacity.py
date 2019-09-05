#! /usr/bin/env python
# -*- coding: utf-8 -*-

"""
@version: ??
@author: li
@file: operation_capacity.py
@time: 2019-09-04 22:36
"""

import sys
sys.path.append('../')
sys.path.append('../../')
sys.path.append('../../../')
import time
import collections
import argparse
from datetime import datetime, timedelta
from factor import factor_operation_capacity
import pandas as pd
from client.dbmodel.model import BalanceMRQ, BalanceTTM, BalanceReport
from client.dbmodel.model import CashFlowMRQ, CashFlowTTM, CashFlowReport
from client.dbmodel.model import IndicatorReport, IndicatorMRQ, IndicatorTTM
from client.dbmodel.model import IncomeMRQ, IncomeReport, IncomeTTM
from client.engines.sqlengine import sqlEngine
from client.utillities.sync_util import SyncUtil
from ultron.cluster.invoke.cache_data import cache_data


def get_basic_data(trade_date):
    maplist = {
        # cash flow
        'MANANETR': 'net_operate_cash_flow',  # 经营活动现金流量净额
        'FINALCASHBALA': 'cash_and_equivalents_at_end',  # 期末现金及现金等价物余额

        # income
        'BIZCOST': 'operating_cost',  # 营业成本
        'BIZINCO': 'operating_revenue',  # 营业收入

        # balance
        'ACCORECE':'accounts_payable',  # 应付账款
        'NOTESRECE': 'bill_receivable',  # 应收票据
        '':'advance_payment',  # 预付款项
        'INVE': 'inventories',  # 存货
        'TOTCURRASSET': 'total_current_assets',  # 流动资产合计
        'FIXEDASSENET': 'fixed_assets',  # 固定资产
        'ENGIMATE':'construction_materials',  # 工程物资
        'CONSPROG':'constru_in_process',  # 在建工程
        'TOTASSET': 'total_assets',  # 资产总计
        'ADVAPAYM': 'advance_peceipts',  # 预收款项

    }

    # 读取目前涉及到的因子
    # 当期数据
    engine = sqlEngine()
    columns = ['COMPCODE', 'PUBLISHDATE', 'ENDDATE', 'symbol', 'company_id', 'trade_date']
    ttm_cash_flow = engine.fetch_fundamentals_pit_extend_company_id(CashFlowTTM,
                                                                    [CashFlowTTM.MANANETR,
                                                                     CashFlowTTM.FINALCASHBALA,
                                                                     ], dates=[trade_date]).drop(columns, axis=1)

    ttm_cash_flow = ttm_cash_flow.rename(columns={
        'MANANETR': 'net_operate_cash_flow',  # 经营活动现金流量净额
        'FINALCASHBALA': 'cash_and_equivalents_at_end',  # 期末现金及现金等价物余额
    })

    ttm_income = engine.fetch_fundamentals_pit_extend_company_id(IncomeTTM,
                                                                 [IncomeTTM.BIZCOST,
                                                                  IncomeTTM.BIZINCO,
                                                                  ], dates=[trade_date]).drop(columns, axis=1)
    ttm_income = ttm_income.rename(columns={
        'BIZCOST': 'operating_cost',  # 营业成本
        'BIZINCO': 'operating_revenue',  # 营业收入
    })

    ttm_balance = engine.fetch_fundamentals_pit_extend_company_id(BalanceTTM,
                                                                  [BalanceTTM.ACCORECE,
                                                                   BalanceTTM.NOTESRECE,
                                                                   # BalanceTTM.advance_payment,
                                                                   BalanceTTM.INVE,
                                                                   BalanceTTM.TOTCURRASSET,
                                                                   BalanceTTM.FIXEDASSENET,
                                                                   BalanceTTM.ENGIMATE,
                                                                   BalanceTTM.CONSPROG,
                                                                   BalanceTTM.TOTASSET,
                                                                   BalanceTTM.ADVAPAYM,
                                                                   ], dates=[trade_date]).drop(columns, axis=1)
    ttm_balance = ttm_balance.rename(columns={
        'ACCORECE': 'accounts_payable',  # 应付账款
        'NOTESRECE': 'bill_receivable',  # 应收票据
        # '': 'advance_payment',  # 预付款项
        'INVE': 'inventories',  # 存货
        'TOTCURRASSET': 'total_current_assets',  # 流动资产合计
        'FIXEDASSENET': 'fixed_assets',  # 固定资产
        'ENGIMATE': 'construction_materials',  # 工程物资
        'CONSPROG': 'constru_in_process',  # 在建工程
        'TOTASSET': 'total_assets',  # 资产总计
        'ADVAPAYM': 'advance_peceipts',  # 预收款项
    })

    ttm_operation_capacity = pd.merge(ttm_cash_flow, ttm_income, on='security_code')
    ttm_operation_capacity = pd.merge(ttm_balance, ttm_operation_capacity, on='security_code')
    return ttm_operation_capacity


def prepare_calculate_local(trade_date):
    tic = time.time()
    ttm_operation_capacity = get_basic_data(trade_date)
    if len(ttm_operation_capacity) <= 0:
        print("%s has no data" % trade_date)
        return
    else:
        factor_operation_capacity.calculate(trade_date, ttm_operation_capacity)
    time4 = time.time()
    print('operation_capacity_cal_time:{}'.format(time4 - tic))


def prepare_calculate(trade_date):
    ttm_operation_capacity = get_basic_data(trade_date)
    print('len_ttm_operation_capacity: %s' % len(ttm_operation_capacity))
    if len(ttm_operation_capacity) <= 0:
        print("%s has no data" % trade_date)
        return
    else:
        tic = time.time()
        session = str(int(time.time() * 1000000 + datetime.now().microsecond))
        cache_data.set_cache(session + str(trade_date) + "1", trade_date, ttm_operation_capacity.to_json(orient='records'))
        ttm_operation_capacity.factor_calculate(date_index=trade_date, session=session)
        time4 = time.time()
        print('operation_capacity_cal_time:{}'.format(time4 - tic))


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
    #     processor = factor_operation_capacity.OperationCapacity('operation_capacity')
    #     processor.create_dest_tables()
    #     do_update(args.start_date, end_date, args.count)
    # if args.update:
    #     do_update(args.start_date, end_date, args.count)
    do_update('20190819', '20190823', 10)
