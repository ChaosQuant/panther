# -*- coding: utf-8 -*-
import argparse
import importlib

from data.rebuild import Rebuild
from PyFin.api import *
import time
from datetime import datetime
import warnings
import config

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


def do_update(start_date, end_date, calc_engine):
    start_date = change_date(start_date)
    end_date = change_date(end_date)
    freq = '1b'
    rebalance_dates = makeSchedule(start_date, end_date, freq, 'china.sse', BizDayConventions.Preceding,
                                   DateGeneration.Backward)
    rebalance_dates.reverse()
    for date in rebalance_dates:
        start_time = time.time()
        calc_engine.local_run(date.strftime('%Y-%m-%d'))
        print(date, time.time() - start_time)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--start_date', type=int, default=20070101)
    parser.add_argument('--end_date', type=int, default=0)
    parser.add_argument('--factor_name', type=str, default='earning_expectation')
    # parser.add_argument('--packet_name', type=str, default='earning_expectation.factor_earning_expectation')
    # parser.add_argument('--class_name', type=str, default='FactorEarningExpectation')
    parser.add_argument('--rebuild', type=bool, default=False)
    parser.add_argument('--update', type=bool, default=False)
    parser.add_argument('--schedule', type=bool, default=False)

    engine_path_dic = {'capacity': 'financial.calc_engine.capacity_calc_engine',
                       'cash_flow': 'financial.calc_engine.cash_flow_calc_engine',
                       'earning': 'financial.calc_engine.earning_calc_engine',
                       'growth': 'financial.calc_engine.growth_calc_engine',
                       'pre_share': 'financial.calc_engine.pre_share_calc_engine',
                       'revenue_quality': 'financial.calc_engine.quality_calc_engine',
                       'solvency': 'financial.calc_engine.solvency_calc_engine',
                       'capital_structure':'financial.calc_engine.structure_calc_engine',
                       'valuation': 'valuation_estimation.calc_engine',
                       'basic_derivation': 'basic_derivation.calc_engine',
                       'earning_expectation': 'earning_expectation.calc_engine',
                       'momentum': 'technical.calc_engine',
                       'power_volume': 'technical.calc_engine',
                       'reversal': 'technical.calc_engine',
                       'sentiment': 'technical.calc_engine',
                       }

    packet_name_dic = {'capacity': 'financial.factor_operation_capacity',
                       'cash_flow': 'financial.factor_cash_flow',
                       'earning': 'financial.factor_earning',
                       'growth': 'financial.factor_history_growth',
                       'pre_share': 'financial.factor_per_share_indicators',
                       'revenue_quality': 'financial.factor_revenue_quality',
                       'solvency': 'financial.factor_solvency',
                       'capital_structure': 'financial.factor_capital_structure',
                       'valuation': 'valuation_estimation.factor_valuation',
                       'basic_derivation': 'basic_derivation.factor_basic_derivation',
                       'earning_expectation': 'earning_expectation.factor_earning_expectation',
                       'momentum': 'technical.momentum',
                       'power_volume': 'technical.power_volume',
                       'reversal': 'technical.reversal',
                       'sentiment': 'technical.sentiment',
                       }

    class_name_dic = {'capacity': 'CapitalStructure',
                      'cash_flow': 'FactorCashFlow',
                      'earning': 'FactorEarning',
                      'growth': 'Growth',
                      'pre_share': 'PerShareIndicators',
                      'revenue_quality': 'RevenueQuality',
                      'solvency': 'Solvency',
                      'capital_structure': 'CapitalStructure',
                      'valuation': 'ValuationEstimation',
                      'basic_derivation': 'Derivation',
                      'earning_expectation': 'FactorEarningExpectation',
                      'momentum': 'Momentum',
                      'power_volume': 'PowerVolume',
                      'reversal': 'Reversal',
                      'sentiment': 'Sentiment',
                      }

    args = parser.parse_args()
    # factor_type = args.packet_name.split('.')[0]
    # class_method = importlib.import_module(factor_type + '.calc_engine').__getattribute__('CalcEngine')

    factor_name = args.factor_name

    engine = engine_path_dic[factor_name]
    packet_name = packet_name_dic[factor_name]
    class_name = class_name_dic[factor_name]

    class_method = importlib.import_module(engine).__getattribute__('CalcEngine')
    calc_engine = class_method('rl', db_url)
    rebuild = Rebuild(db_url)
    if args.end_date == 0:
        end_date = int(datetime.now().date().strftime('%Y%m%d'))
    else:
        end_date = args.end_date
    if args.rebuild:
        # rebuild.rebuild_table(args.packet_name, args.class_name)
        rebuild.rebuild_table(packet_name, class_name)
        do_update(args.start_date, end_date, calc_engine)
    if args.update:
        do_update(args.start_date, end_date, calc_engine)

    # rebuild.rebuild_table('technical.momentum','Momentum')
    # rebuild.rebuild_table('technical.power_volume','PowerVolume')
    # rebuild.rebuild_table('technical.reversal','Reversal')
    # rebuild.rebuild_table('technical.sentiment','Sentiment')
