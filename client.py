# -*- coding: utf-8 -*-
import argparse
import importlib

# from performance.calc_engine import CalcEngine

from data.rebuild import Rebuild
from PyFin.api import *
import time
from datetime import datetime
import warnings
import config
import sqlalchemy as sa
import pandas as pd

warnings.filterwarnings("ignore")
db_url = '''mysql+mysqlconnector://{0}:{1}@{2}:{3}/{4}'''.format(config.rl_db_user,
                                                                 config.rl_db_pwd,
                                                                 config.rl_db_host,
                                                                 config.rl_db_port,
                                                                 config.rl_db_database)


def change_date(date):
    date = str(date)
    date = date[0:4] + '-' + date[4:6] + '-' + date[6:8]
    return date


def get_start_date(factor_name, type):
    destination = sa.create_engine(db_url)
    if type == 'after':
        td = 'max(trade_date)'
        date = 20120101
    else:
        td = 'min(trade_date)'
        date = int(datetime.now().date().strftime('%Y%m%d'))
        # date = 20190101
    sql = """select {0} as trade_date from `{1}`;""".format(td, factor_name)
    trades_sets = pd.read_sql(sql, destination)
    if not trades_sets.empty:
        ts = trades_sets['trade_date'][0]
        if ts is not None:
            date = str(ts).replace('-', '')
    return date


def do_schedule(factor_name, calc_engine, type):
    if type == 'pre':
        start_date = 20120101
        end_date = get_start_date(factor_name, type)
    else:
        start_date = get_start_date(factor_name, type)
        end_date = int(datetime.now().date().strftime('%Y%m%d'))
        # date = 20190101
    do_update(start_date, end_date, calc_engine, type)


def do_update(start_date, end_date, calc_engine, type='after'):
    start_date = change_date(start_date)
    end_date = change_date(end_date)
    freq = '1b'
    rebalance_dates = makeSchedule(start_date, end_date, freq, 'china.sse', BizDayConventions.Preceding,
                                   DateGeneration.Backward)
    if type == 'pre':
        rebalance_dates.reverse()
    for date in rebalance_dates:
        start_time = time.time()
        print('开始计算：', date.strftime('%Y-%m-%d'))
        calc_engine.local_run(date.strftime('%Y-%m-%d'))
        print(date, time.time() - start_time)


if __name__ == "__main__":
    print('in client exe')
    parser = argparse.ArgumentParser()
    parser.add_argument('--start_date', type=int, default=20120101)
    parser.add_argument('--end_date', type=int, default=0)
    parser.add_argument('--packet_name', type=str, default='earning_expectation.factor_earning_expectation')
    parser.add_argument('--class_name', type=str, default='FactorEarningExpectation')
    parser.add_argument('--rebuild', type=bool, default=False)
    parser.add_argument('--update', type=bool, default=False)
    parser.add_argument('--schedule', type=bool, default=False)
    parser.add_argument('--type', type=str, default='after')
    args = parser.parse_args()
    factor_type = args.packet_name.split('.')[0]
    factor_name = args.packet_name.split('.')[1]
    class_method = importlib.import_module(factor_type + '.calc_engine').__getattribute__('CalcEngine')
    calc_engine = class_method('rl', db_url, methods=[{'packet': args.packet_name, 'class': args.class_name}])
    rebuild = Rebuild(db_url)
    if args.end_date == 0:
        end_date = int(datetime.now().date().strftime('%Y%m%d'))
    else:
        end_date = args.end_date
    if args.rebuild:
        rebuild.rebuild_table(args.packet_name, args.class_name)
        # do_update(args.start_date, end_date, calc_engine)
    if args.update:
        do_update(args.start_date, end_date, calc_engine, args.type)
    if args.schedule:
        do_schedule(factor_name, calc_engine, args.type)

