# -*- coding: utf-8 -*-
import argparse
import pandas as pd
import sqlalchemy as sa
from integrated.calc_engine import CalcEngine
from datetime import datetime
import warnings
import config

warnings.filterwarnings("ignore")
db_url = '''mysql+mysqlconnector://{0}:{1}@{2}:{3}/{4}'''.format(config.rl_db_user,
                                                                 config.rl_db_pwd,
                                                                 config.rl_db_host,
                                                                 config.rl_db_port,
                                                                 config.rl_db_database)


def get_start_date(factor_name):
    destination = sa.create_engine(db_url)
    date = 20140101
    sql = """select max(trade_date) as trade_date from factor_integrated_basic where factor_type='{0}';""".format(
        factor_name)
    trades_sets = pd.read_sql(sql, destination)
    if not trades_sets.empty:
        ts = trades_sets['trade_date'][0]
        if ts is not None:
            date = str(ts).replace('-', '')
    return date


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('--start_date', type=int, default=20140101)
    parser.add_argument('--end_date', type=int, default=0)
    parser.add_argument('--factor_name', type=str, default='factor_sentiment')  # factor_earning
    parser.add_argument('--update', type=bool, default=False)
    parser.add_argument('--schedule', type=bool, default=False)
    args = parser.parse_args()
    if args.end_date == 0:
        end_date = datetime.now().date().strftime('%Y%m%d')
    else:
        end_date = args.end_date
    calc_engine = CalcEngine('rl', db_url, args.factor_name)
    if args.update:
        calc_engine.local_run(args.start_date, end_date, args.factor_name)
    if args.schedule:
        start_date = get_start_date(args.factor_name)
        calc_engine.local_run(start_date, end_date, args.factor_name)
