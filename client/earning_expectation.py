#! /usr/bin/env python
# -*- coding: utf-8 -*-

"""
@version: ??
@author: zzh
@file: earning_expectation.py
@time: 2019-09-10 14:46
"""
import argparse
import sys

from dateutil.relativedelta import relativedelta
import time

sys.path.append('..')
from factor import factor_earning_expectation
from vision.table.stk_consensus_expectation import StkConsensusExpectation
from vision.table.stk_consensus_rating import StkConsensusRating
from vision.db.signletion_engine import *

from client.utillities.sync_util import SyncUtil


# from ultron.cluster.invoke.cache_data import cache_data


def get_trade_date(trade_date, delta):
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
    time_array = time_array - delta
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
    trade_date_pre_weak = get_trade_date(trade_date, relativedelta(weeks=+1))
    trade_date_pre_month_1 = get_trade_date(trade_date, relativedelta(months=+1))
    trade_date_pre_month_3 = get_trade_date(trade_date, relativedelta(months=+3))
    trade_date_pre_month_6 = get_trade_date(trade_date, relativedelta(months=+6))

    # Report Data
    q1 = query(StkConsensusExpectation.publish_date, StkConsensusExpectation.security_code,
               StkConsensusExpectation.symbol,
               StkConsensusExpectation.net_profit_fy1,
               StkConsensusExpectation.net_profit_fy2,
               StkConsensusExpectation.eps_fy1,
               StkConsensusExpectation.eps_fy2,
               StkConsensusExpectation.operating_revenue_fy1,
               StkConsensusExpectation.operating_revenue_fy2,
               StkConsensusExpectation.pe_fy1,
               StkConsensusExpectation.pe_fy2,
               StkConsensusExpectation.pb_fy1,
               StkConsensusExpectation.pb_fy2,
               StkConsensusExpectation.peg_fy1,
               StkConsensusExpectation.peg_fy2).filter(StkConsensusExpectation.publish_date.in_(
        [trade_date, trade_date_pre_weak, trade_date_pre_month_1, trade_date_pre_month_3, trade_date_pre_month_6]))
    consensus_expectation_sets = get_fundamentals(q1)

    q2 = query(StkConsensusRating.publish_date, StkConsensusRating.symbol,
               StkConsensusRating.rating2).filter(StkConsensusRating.publish_date.in_(
        [trade_date, trade_date_pre_weak, trade_date_pre_month_1, trade_date_pre_month_3, trade_date_pre_month_6]),
        StkConsensusRating.date_type == 5)
    consensus_rating_sets = get_fundamentals(q2)

    tp_earning = pd.merge(consensus_expectation_sets, consensus_rating_sets, on=['publish_date', 'symbol'], how='left')
    tp_earning['publish_date'] = pd.to_datetime(tp_earning['publish_date'], format='%Y-%m-%d')
    return tp_earning


def prepare_calculate_local(trade_date):
    # local
    tic = time.time()
    tp_earning = get_basic_data(trade_date)
    if len(tp_earning) <= 0:
        print("%s has no data" % trade_date)
        return
    else:
        factor_earning_expectation.calculate(trade_date, tp_earning)
    time6 = time.time()
    print('earning_cal_time:{}'.format(time6 - tic))


def prepare_calculate_remote(trade_date):
    # remote
    tp_earning = get_basic_data(trade_date)
    if len(tp_earning) <= 0:
        print("%s has no data" % trade_date)
        return
    else:
        tic = time.time()
        session = str(int(time.time() * 1000000 + datetime.now().microsecond))
        cache_data.set_cache(session + str(trade_date), trade_date, tp_earning.to_json(orient='records'))
        factor_earning_expectation.factor_calculate.delay(date_index=trade_date, session=session)
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
        # prepare_calculate_remote(trade_date)
    print('----->')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--start_date', type=int, default=20120801)
    parser.add_argument('--end_date', type=int, default=0)
    parser.add_argument('--count', type=int, default=-1)
    parser.add_argument('--rebuild', type=bool, default=True)
    parser.add_argument('--update', type=bool, default=False)
    parser.add_argument('--schedule', type=bool, default=False)

    args = parser.parse_args()
    if args.end_date == 0:
        end_date = int(datetime.now().date().strftime('%Y%m%d'))
    else:
        end_date = args.end_date
    if args.rebuild:
        processor = factor_earning_expectation.FactorEarningExpectation('factor_earning_expectation')
        processor.create_dest_tables()
        do_update(args.start_date, end_date, args.count)
    if args.update:
        do_update(args.start_date, end_date, args.count)
