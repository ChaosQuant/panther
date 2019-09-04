#! /usr/bin/env python
# -*- coding: utf-8 -*-

"""
@version: ??
@author: li
@file: capital_structure.py
@time: 2019-09-04 13:06
"""

import sys
sys.path.append('../')
sys.path.append('../../')
sys.path.append('../../../')
import time
import collections
import argparse
from datetime import datetime, timedelta
from factor import factor_capital_structure

from client.dbmodel.model import BalanceMRQ, BalanceTTM, BalanceReport
from client.dbmodel.model import CashFlowMRQ, CashFlowTTM, CashFlowReport
from client.dbmodel.model import IndicatorReport, IndicatorMRQ, IndicatorTTM
from client.dbmodel.model import IncomeMRQ, IncomeReport, IncomeTTM

# from vision.file_unit.valuation import Valuation
from client.engines.sqlengine import sqlEngine
from client.utillities.sync_util import SyncUtil
# from client.utillities.sync_util import SyncUtilfrom
# ultron.cluster.invoke.cache_data import cache_data


def get_management_data(trade_date):
    engine = sqlEngine()
    maplist = {
        # balance
        'TOTALNONCASSETS': 'total_non_current_assets',     # 非流动资产合计
        'TOTASSET': 'total_assets',        # 资产总计
        'TOTALNONCLIAB':' total_non_current_liability',  # 非流动负债合计
        'LONGBORR':' longterm_loan',  # 长期借款
        'INTAASSET':' intangible_assets',  # 无形资产
        'DEVEEXPE': 'development_expenditure',  # 开发支出
        'GOODWILL':'good_will',  # 商誉
        'FIXEDASSENET':'fixed_assets',  # 固定资产
        'ENGIMATE':'construction_materials',  # 工程物资
        'CONSPROG':'constru_in_process',  # 在建工程
        'RIGHAGGR': 'total_owner_equities',  # 股东权益合计
        'TOTCURRASSET': 'total_current_assets',  # 流动资产合计
        }
    # 读取目前涉及到的因子
    columns = ['COMPCODE', 'PUBLISHDATE', 'ENDDATE', 'symbol', 'company_id', 'trade_date']
    balance_sets = engine.fetch_fundamentals_pit_extend_company_id(BalanceMRQ,
                                                                   [BalanceMRQ.TOTALNONCASSETS,
                                                                    BalanceMRQ.TOTASSET,
                                                                    BalanceMRQ.TOTALNONCLIAB,
                                                                    BalanceMRQ.LONGBORR,
                                                                    BalanceMRQ.INTAASSET,
                                                                    BalanceMRQ.DEVEEXPE,
                                                                    BalanceMRQ.GOODWILL,
                                                                    BalanceMRQ.FIXEDASSENET,
                                                                    BalanceMRQ.ENGIMATE,
                                                                    BalanceMRQ.CONSPROG,
                                                                    BalanceMRQ.RIGHAGGR,
                                                                    BalanceMRQ.TOTCURRASSET,
                                                                    ],
                                                                   dates=[trade_date]).drop(columns, axis=1)

    balance_sets = balance_sets.rename(columns={
        'TOTALNONCASSETS': 'total_non_current_assets',     # 非流动资产合计
        'TOTASSET': 'total_assets',        # 资产总计
        'TOTALNONCLIAB':' total_non_current_liability',  # 非流动负债合计
        'LONGBORR':' longterm_loan',  # 长期借款
        'INTAASSET':' intangible_assets',  # 无形资产
        'DEVEEXPE': 'development_expenditure',  # 开发支出
        'GOODWILL':'good_will',  # 商誉
        'FIXEDASSENET':'fixed_assets',  # 固定资产
        'ENGIMATE':'construction_materials',  # 工程物资
        'CONSPROG':'constru_in_process',  # 在建工程
        'RIGHAGGR': 'total_owner_equities',  # 股东权益合计
        'TOTCURRASSET': 'total_current_assets',  # 流动资产合计
        })

    return balance_sets


def prepare_calculate_local(trade_date):
    tic = time.time()
    tp_management = get_management_data(trade_date)
    print(tp_management.head())
    if len(tp_management) <= 0:
        print("%s has no data" % trade_date)
        return
    else:
        factor_capital_structure.calculate(trade_date, tp_management)
    time4 = time.time()
    print('management_cal_time:{}'.format(time4 - tic))


def prepare_calculate_remote(trade_date):
    ttm_management, tp_management = get_management_data(trade_date)
    # print(ttm_management.head())
    # print(tp_management.head())
    if len(ttm_management) <= 0 or len(tp_management) <= 0:
        print("%s has no data" % trade_date)
        return
    else:
        tic = time.time()
        session = str(int(time.time() * 1000000 + datetime.now().microsecond))
        cache_data.set_cache(session + str(trade_date) + "1", trade_date, tp_management.to_json(orient='records'))
        cache_data.set_cache(session + str(trade_date) + "2", trade_date, ttm_management.to_json(orient='records'))
        factor_capital_structure.factor_calculate(date_index=trade_date, session=session)
        time4 = time.time()
        print('management_cal_time:{}'.format(time4 - tic))


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
    #     processor = factor_capital_structure.CapitalStructure('factor_management')
    #     processor.create_dest_tables()
    #     do_update(args.start_date, end_date, args.count)
    # if args.update:
    #     do_update(args.start_date, end_date, args.count)
    do_update('20190819', '20190823', 10)

