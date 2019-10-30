# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import multiprocessing
import pdb, importlib, inspect, time, datetime, json
from sqlalchemy import create_engine, select, and_, or_
from data.polymerize import DBPolymerize
from data.storage_engine import IntegratedStorageEngine, IntegratedSubStorageEngine
from utilities.factor_se import *
from data.fetch_factor import FetchRLFactorEngine
from data.rl_model import Integrated
import calendar
from datetime import datetime
from PyFin.api import *


class CalcEngine(object):
    def __init__(self, name, url, table,
                 methods=[{'packet': 'integrated.integrated_return', 'class': 'IntegratedReturn'}]):
        self._name = name
        self._url = url
        self._methods = {}
        self._factor_columns = []
        self._neutralized_styles = ['SIZE', 'Bank', 'RealEstate', 'Health', 'Transportation',
                                    'Mining', 'NonFerMetal', 'HouseApp', 'LeiService', 'MachiEquip',
                                    'BuildDeco', 'CommeTrade', 'CONMAT', 'Auto', 'Textile', 'FoodBever',
                                    'Electronics', 'Computer', 'LightIndus', 'Utilities', 'Telecom',
                                    'AgriForest', 'CHEM', 'Media', 'IronSteel', 'NonBankFinan', 'ELECEQP',
                                    'AERODEF', 'Conglomerates']
        self._factor_type = table
        for method in methods:
            name = str(method['packet'].split('.')[-1])
            self._methods[name] = method

    def _stock_return(self, market_data):
        market_data = market_data.set_index(['trade_date', 'security_code'])
        market_data['close'] = market_data['close'] * market_data['lat_factor']
        market_data = market_data['close'].unstack()
        market_data = market_data.sort_index()
        market_data = market_data.apply(lambda x: np.log(x / x.shift(1)))
        mkt_se = market_data.stack()
        mkt_se.name = 'returns'
        return mkt_se.dropna().reset_index()

    def preprocessing(self, benchmark_data, market_data, factor_data, exposure_data):
        self._factor_columns = [i for i in factor_data.columns if i not in ['id', 'trade_date', 'security_code']]
        mkt_se = self._stock_return(market_data)
        mkt_se['returns'] = mkt_se['returns'].replace([np.inf, -np.inf], np.nan)
        total_data = pd.merge(factor_data, exposure_data, on=['trade_date', 'security_code'])
        total_data = pd.merge(total_data, benchmark_data, on=['trade_date', 'security_code'])

        def date_shift(data):
            data['trade_date'] = data['trade_date'].shift(-1)
            return data.dropna(subset=['trade_date'])

        total_data = total_data.groupby(['security_code']).apply(date_shift).reset_index(drop=True)

        mkt_se['trade_date'] = mkt_se['trade_date'].apply(lambda x: x.to_pydatetime().date())
        total_data = pd.merge(total_data, mkt_se, on=['trade_date', 'security_code'], how='left')
        return total_data

    def _factor_preprocess(self, data, neu=1):
        '''
        :param data:
        :param neu: 是否对因子进行中性化处理
        :return:
        '''
        for factor in self._factor_columns:
            data[factor] = se_winsorize(data[factor], method='med')
            if neu:
                data[factor] = se_neutralize(data[factor], data.loc[:, self._neutralized_styles])
            data[factor] = se_standardize(data[factor])
        return data.drop(['index_name', 'sname', 'industry'], axis=1)

    def loadon_data(self, begin_date, end_date, benchmark_code_dict, table):

        db_polymerize = DBPolymerize(self._name)
        db_factor = FetchRLFactorEngine(table)
        factor_data = db_factor.fetch_factors(begin_date=begin_date, end_date=end_date)
        benchmark_data, market_data, exposure_data = db_polymerize.fetch_integrated_data(begin_date, end_date)

        # 针对不同的基准
        total_data_dict = {}
        benchmark_industry_weights_dict = {}

        for key, value in benchmark_code_dict.items():
            total_data = self.preprocessing(benchmark_data[benchmark_data.index_code == key], market_data, factor_data,
                                            exposure_data)
            # 因子标准化
            total_data = total_data.sort_values(['trade_date', 'security_code'])

            # 因子中性化
            total_data = total_data.groupby(['trade_date']).apply(self._factor_preprocess)
            total_data.loc[:, self._factor_columns] = total_data.loc[:, self._factor_columns].fillna(0)

            benchmark_industry_weights = benchmark_data[benchmark_data.index_code == key].groupby(
                ['trade_date', 'industry_code']).apply(lambda x: x['weighing'].sum())
            benchmark_industry_weights = benchmark_industry_weights.unstack().fillna(0)

            total_data_dict[value] = total_data
            benchmark_industry_weights_dict[value] = benchmark_industry_weights

        return total_data_dict, benchmark_industry_weights_dict

    def integrated_basic(self, engine, benchmark, factor_name, total_data, benchmark_weights):
        # 非中性化
        # 分组收益
        groups = engine.calc_group(total_data, factor_name)
        total_data = pd.merge(total_data, groups, on=['trade_date', 'security_code'])
        group_rets_df_non_neu = engine.calc_group_rets(total_data, 5)
        group_rets_df_non_neu = group_rets_df_non_neu.rename(
            columns={'q' + str(i): 'ret_q' + str(i) for i in range(1, 6)})
        group_rets_df_non_neu['spread'] = group_rets_df_non_neu['ret_q1'] - group_rets_df_non_neu['ret_q5']
        group_rets_df_non_neu['benchmark'] = benchmark
        group_rets_df_non_neu['factor_name'] = factor_name
        group_rets_df_non_neu['factor_neu'] = 1
        group_rets_df_non_neu['group_neu'] = 0

        # 中性化
        ## 预处理 基准行业权重
        benchmark_weights_dict = benchmark_weights.T.to_dict()

        if 'group' in total_data.columns:
            total_data = total_data.drop(['group'], axis=1)
        groups = engine.calc_group(total_data, factor_name, industry=True)
        total_data = pd.merge(total_data, groups, on=['trade_date', 'security_code'])
        total_data['trade_date'] = total_data['trade_date'].apply(lambda x: x.date())
        group_rets_df_neu = engine.calc_group_rets(total_data, 5, benchmark_weights=benchmark_weights_dict,
                                                   industry=True)
        group_rets_df_neu = group_rets_df_neu.rename(columns={'q' + str(i): 'ret_q' + str(i) for i in range(1, 6)})
        group_rets_df_neu['spread'] = group_rets_df_neu['ret_q1'] - group_rets_df_neu['ret_q5']
        group_rets_df_neu['benchmark'] = benchmark
        group_rets_df_neu['factor_name'] = factor_name
        group_rets_df_neu['factor_neu'] = 1
        group_rets_df_neu['group_neu'] = 1

        group_rets = pd.concat([group_rets_df_non_neu, group_rets_df_neu], axis=0)
        group_rets.reset_index(inplace=True)

        return group_rets.loc[:,
               ['benchmark', 'trade_date', 'factor_name', 'factor_neu', 'group_neu',
                'ret_q1', 'ret_q2', 'ret_q3', 'ret_q4', 'ret_q5', 'spread']]

    def calc_daily_return(self, benchmark_code_dict, total_data_dict, benchmark_industry_weights_dict):
        if 'integrated_return' not in self._methods:
            return

        method = self._methods['integrated_return']
        integrated_return = importlib.import_module(method['packet']).__getattribute__(method['class'])()
        storage_engine = IntegratedStorageEngine(self._url)

        for factor_name in self._factor_columns:
            factor_name = str(factor_name)

            # 判断因子是否为空
            if total_data_dict['2070000187'][factor_name].isnull().all() or total_data_dict['2070000060'][
                factor_name].isnull().all():
                print('The factor {0} is null!'.format(factor_name))
                continue

            group_rets_list = []

            for key, value in benchmark_code_dict.items():
                total_data = total_data_dict[value]
                benchmark_industry_weights = benchmark_industry_weights_dict[value]
                start_time = time.time()
                print('------------------------------------------------')
                print('The factor {} is calculated with benchmark {}!'.format(factor_name, key))
                group_rets_df = self.integrated_basic(engine=integrated_return,
                                                      benchmark=key,
                                                      factor_name=factor_name,
                                                      total_data=total_data,
                                                      benchmark_weights=benchmark_industry_weights)
                group_rets_list.append(group_rets_df)

            integrated_rets_df = pd.concat(group_rets_list, axis=0)
            dates = list(set(integrated_rets_df.trade_date))
            dates.sort()
            start_date = dates[0].strftime('%Y-%m-%d')
            stop_date = dates[-1].strftime('%Y-%m-%d')
            integrated_rets_df['factor_type'] = self._factor_type
            storage_engine.update_destdb('factor_integrated_basic', start_date, stop_date, factor_name,
                                         integrated_rets_df)
            print(integrated_rets_df.head())

    def calc_interval_rets(self, trade_date):
        begin_date = advanceDateByCalendar('china.sse', trade_date, '-3y')

        table = Integrated
        engine = create_engine(self._url)
        storage_engine = IntegratedSubStorageEngine(self._url)

        factor_list = [str(factor) for factor in self._factor_columns]

        query = select([table]).where(and_(
            and_(table.trade_date >= begin_date, table.trade_date <= trade_date),
            table.factor_name.in_(factor_list)
        ))
        daily_rets_df = pd.read_sql(query, engine)

        # 取中性化做分析
        daily_rets_df = daily_rets_df[(daily_rets_df.factor_neu == 1) & (daily_rets_df.group_neu == 1)]

        grouped = daily_rets_df.groupby(['factor_name', 'benchmark'])

        for para, g in grouped:
            factor_name = para[0]
            benchmark = para[1]

            ret_dict = {}
            ret_dict['factor_name'] = factor_name
            ret_dict['benchmark'] = benchmark
            ret_dict['factor_neu'] = 1
            ret_dict['group_neu'] = 1
            ret_dict['trade_date'] = trade_date

            g = g.sort_values('trade_date')
            ret_dict['week_ret'] = g.spread.iloc[-5:].sum()
            ret_dict['month_ret'] = g.spread.iloc[-21:].sum()
            ret_dict['year_ret'] = g.spread.iloc[-252:].sum()
            ret_dict['three_ann_ret'] = g.spread.iloc[-252 * 3:].sum() / len(g.spread.iloc[-252 * 3:]) * 252

            interval_rets_df = pd.DataFrame([ret_dict])
            interval_rets_df['factor_type'] = self._factor_type
            storage_engine.update_destdb('factor_integrated_sub', trade_date, factor_name, benchmark, interval_rets_df)
            print(interval_rets_df.head())

    def get_last_trade_date_month(self, year, month):
        d = calendar.monthrange(year, month)
        start_date = (datetime(year, month, 1)).strftime("%Y-%m-%d")
        end_date = (datetime(year, month, d[1])).strftime("%Y-%m-%d")
        freq = '1b'
        rebalance_dates = makeSchedule(start_date, end_date, freq, 'china.sse', BizDayConventions.Preceding,
                                       DateGeneration.Forward)
        return rebalance_dates[-1].strftime('%Y%m%d')

    def split_date(self, begin_date, end_date):
        date_arr = []
        begin_year = int(begin_date[0:4])
        begin_month = int(begin_date[4:6])
        if begin_date == '20140101':
            date_arr.append('20140102')
        else:
            if begin_month == 1:
                month = 12
                year = begin_year - 1
            else:
                month = begin_month - 1
                year = begin_year
            date_arr.append(self.get_last_trade_date_month(year, month))
        while (True):
            t_end_date = self.get_last_trade_date_month(begin_year, begin_month)
            if t_end_date >= end_date:
                date_arr.append(end_date)
                break
            date_arr.append(t_end_date)
            if begin_month == 12:
                begin_year += 1
                begin_month = 1
            else:
                begin_month += 1
        if date_arr[1] < begin_date:
            date_arr.pop(0)
        return date_arr

    def local_run(self, begin_date, end_date, table):
        benchmark_code_dict = {
            '000905.XSHG': '2070000187',
            '000300.XSHG': '2070000060',
        }
        date_arr = self.split_date(str(begin_date), str(end_date))
        for i in range(len(date_arr) - 1):
            print('计算区间：', date_arr[i], '-', date_arr[i + 1])
            # 计算每日收益
            total_data_dict, benchmark_industry_weights_dict = self.loadon_data(date_arr[i],
                                                                                date_arr[i + 1],
                                                                                benchmark_code_dict,
                                                                                table)

            # 计算区间收益
            self.calc_daily_return(benchmark_code_dict, total_data_dict, benchmark_industry_weights_dict)
        last_date = date_arr[-1]
        self.calc_interval_rets(last_date[0:4] + '-' + last_date[4:6] + '-' + last_date[6:8])


if __name__ == '__main__':
    pass
