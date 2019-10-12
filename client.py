# -*- coding: utf-8 -*-
import argparse

from performance.calc_engine import CalcEngine
from datetime import datetime
import warnings

warnings.filterwarnings("ignore")

if __name__ == "__main__":

    calc_engine = CalcEngine('rl',
                             'mysql+mysqlconnector://factor_edit:factor_edit_2019@39.98.3.182/vision?charset=utf8')
    # begin_date = '2018-08-23'
    # factor_table = 'factor_reversal'  # 因子表名
    # calc_engine.local_run(begin_date, factor_table)
    parser = argparse.ArgumentParser()
    parser.add_argument('--start_date', type=int, default=20120101)
    # parser.add_argument('--start_date', type=int, default=20180701)
    parser.add_argument('--end_date', type=int, default=20190101)
    parser.add_argument('--factor_name', type=str, default='factor_cash_flow')  # factor_earning
    args = parser.parse_args()
    if args.end_date == 0:
        end_date = int(datetime.now().date().strftime('%Y%m%d'))
    else:
        end_date = args.end_date
    calc_engine.local_run(args.start_date, end_date, args.factor_name)
