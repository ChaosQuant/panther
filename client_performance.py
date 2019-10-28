# -*- coding: utf-8 -*-
import argparse

from performance.calc_engine import CalcEngine
from datetime import datetime
import warnings
import config

warnings.filterwarnings("ignore")

if __name__ == "__main__":
    db_url = '''mysql+mysqlconnector://{0}:{1}@{2}:{3}/{4}'''.format(config.rl_db_user,
                                                                     config.rl_db_pwd,
                                                                     config.rl_db_host,
                                                                     config.rl_db_port,
                                                                     config.rl_db_database)
    calc_engine = CalcEngine('rl', db_url)
    # begin_date = '2018-08-23'
    # factor_table = 'factor_reversal'  # 因子表名
    # calc_engine.local_run(begin_date, factor_table)
    parser = argparse.ArgumentParser()
    parser.add_argument('--start_date', type=int, default=20150101)
    # parser.add_argument('--start_date', type=int, default=20180701)
    parser.add_argument('--end_date', type=int, default=0)
    parser.add_argument('--factor_name', type=str, default='factor_capital_structure')  # factor_earning
    args = parser.parse_args()
    if args.end_date == 0:
        end_date = int(datetime.now().date().strftime('%Y%m%d'))
    else:
        end_date = args.end_date
    calc_engine.local_run(args.start_date, end_date, args.factor_name)
