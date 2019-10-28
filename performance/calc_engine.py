# 动态加载因子计算指标，可将因子性能指标分布式
import pdb, importlib, time
import numpy as np
import pandas as pd
from scipy import stats
from PyFin.api import *
from utilities.factor_se import *
from data.polymerize import DBPolymerize
from data.storage_engine import PerformanceStorageEngine, BenchmarkStorageEngine
from data.fetch_factor import FetchRLFactorEngine


class CalcEngine(object):

    def __init__(self, name, url, methods=[{'packet': 'performance.basic_return', 'class': 'BasicReturn'},
                                           {'packet': 'performance.icir', 'class': 'ICIR'},
                                           {'packet': 'performance.other', 'class': 'Other'}]):
        self._name = name
        self._methods = methods
        self._url = url
        self._methods = {}
        self._factor_columns = []
        self._neutralized_styles = ['SIZE', 'Bank', 'RealEstate', 'Health', 'Transportation',
                                    'Mining', 'NonFerMetal', 'HouseApp', 'LeiService', 'MachiEquip',
                                    'BuildDeco', 'CommeTrade', 'CONMAT', 'Auto', 'Textile', 'FoodBever',
                                    'Electronics', 'Computer', 'LightIndus', 'Utilities', 'Telecom',
                                    'AgriForest', 'CHEM', 'Media', 'IronSteel', 'NonBankFinan', 'ELECEQP',
                                    'AERODEF', 'Conglomerates']
        for method in methods:
            name = str(method['packet'].split('.')[-1])
            self._methods[name] = method

    def _stock_return(self, market_data):
        market_data = market_data.set_index(['trade_date', 'security_code'])
        market_data['close'] = market_data['close'] * market_data['lat_factor']
        market_data = market_data['close'].unstack()
        market_data = market_data.sort_index()
        market_data = market_data.apply(lambda x: np.log(x.shift(-1) / x))
        mkt_se = market_data.stack()
        mkt_se.name = 'returns'
        return mkt_se.dropna().reset_index()

    def _index_return(self, index_data):
        index_data = index_data.set_index(['trade_date'])
        index_data = index_data.sort_index()
        index_data['returns'] = np.log(index_data['close'].shift(-1) / index_data['close'])
        return index_data.loc[:, ['returns']].dropna().reset_index()

    def performance_preprocessing(self, benchmark_data, index_data, market_data, factor_data, exposure_data):
        # 修改 index_se_dict, 直接删除基准df
        # index_se_dict = {}
        self._factor_columns = [i for i in factor_data.columns if i not in ['id', 'trade_date', 'security_code']]

        # security_code_sets = index_data.security_code.unique()
        # for security_code in security_code_sets:
        #     index_se = index_data.set_index('security_code').loc[security_code].reset_index()
        #     index_se_dict[security_code] = self._index_return(index_se)
        index_rets = self._index_return(index_data)

        mkt_se = self._stock_return(market_data)
        mkt_se['returns'] = mkt_se['returns'].replace([np.inf, -np.inf], np.nan)
        total_data = pd.merge(factor_data, exposure_data, on=['trade_date', 'security_code'])
        total_data = pd.merge(total_data, benchmark_data, on=['trade_date', 'security_code'])
        mkt_se['trade_date'] = mkt_se['trade_date'].apply(lambda x: x.to_pydatetime().date())
        total_data = pd.merge(total_data, mkt_se, on=['trade_date', 'security_code'], how='left')
        # return total_data.dropna(), index_rets
        return total_data, index_rets

    def _factor_preprocess(self, data):
        for factor in self._factor_columns:
            data[factor] = se_winsorize(data[factor], method='med')
            data[factor] = se_neutralize(data[factor], data.loc[:, self._neutralized_styles])
            data[factor] = se_standardize(data[factor])
        return data.drop(['index_name', 'sname', 'industry'], axis=1)

    def loadon_data(self, begin_date, trade_date, table):
        # 若两个指数有重合，需要重构该函数；已改
        # 确定获取的日期范围
        freq = '1m'
        # begin_date = advanceDate(trade_date, '-3y')
        benchmark_code_dict = {'000905.XSHG': '2070000187', '000300.XSHG': '2070000060'}

        db_factor = FetchRLFactorEngine(table)
        factor_data = db_factor.fetch_factors(begin_date=begin_date, end_date=trade_date, freq=freq)

        db_polymerize = DBPolymerize(self._name)
        benchmark_data, index_data, market_data, exposure_data = db_polymerize.fetch_performance_data(
            benchmark_code_dict.keys(), begin_date, trade_date, freq)

        # 针对不同的基准
        total_data_dict = {}
        index_rets_dict = {}
        benchmark_industry_weights_dict = {}

        for key, value in benchmark_code_dict.items():
            total_data, index_rets = self.performance_preprocessing(benchmark_data[benchmark_data.index_code == key],
                                                                    index_data[index_data.security_code == value],
                                                                    market_data,
                                                                    factor_data, exposure_data)
            # 中性化处理
            total_data = total_data.sort_values(['trade_date', 'security_code'])
            total_data = total_data.groupby(['trade_date']).apply(self._factor_preprocess)
            total_data.loc[:, self._factor_columns] = total_data.loc[:, self._factor_columns].fillna(0)

            benchmark_industry_weights = benchmark_data[benchmark_data.index_code == key].groupby(
                ['trade_date', 'industry_code']).apply(lambda x: x['weighing'].sum())
            benchmark_industry_weights = benchmark_industry_weights.unstack().fillna(0)

            total_data_dict[value] = total_data
            index_rets_dict[value] = index_rets
            benchmark_industry_weights_dict[value] = benchmark_industry_weights

        return total_data_dict, benchmark_industry_weights_dict, index_rets_dict

    def change_date(self, date):
        date = str(date)
        date = date[0:4] + '-' + date[4:6] + '-' + date[6:8]
        return date

    def local_run(self, start_date, trade_date, factor_table):
        start_date = self.change_date(start_date)
        trade_date = self.change_date(trade_date)
        benchmark_code_dict = {'000905.XSHG': '2070000187', '000300.XSHG': '2070000060'}

        total_data_dict, benchmark_industry_weights_dict, index_rets_dict = self.loadon_data(start_date, trade_date,
                                                                                             factor_table)

        # # benchmark
        # hs300_rets = index_rets_dict[benchmark_code_dict['000300.XSHG']].rename(columns={'returns': 'hs300'})
        # zz500_rets = index_rets_dict[benchmark_code_dict['000905.XSHG']].rename(columns={'returns': 'zz500'})
        # benchmark_rets_df = pd.merge(hs300_rets, zz500_rets, on=['trade_date'])
        # benchmark_rets_df = benchmark_rets_df.loc[:, ['trade_date', 'hs300', 'zz500']]
        #
        # # 存基准数据
        # benchmark_storage_engine = BenchmarkStorageEngine(self._url)
        # benchmark_storage_engine.update_destdb('factor_performance_benchmark', benchmark_rets_df)

        # 数据层放在层，因子放里层
        # # 收益相关
        for factor_name in self._factor_columns:

            factor_name = str(factor_name)

            # 判断因子是否为空
            if total_data_dict['2070000187'][factor_name].isnull().all() or total_data_dict['2070000060'][factor_name].isnull().all():
                print('The factor {0} is null!'.format(factor_name))
                continue

            return_basic_list, return_sub_list = [], []
            ic_basic_list, ic_sub_list, group_ic_list, group_ic_sub_list, industry_ic_list = [], [], [], [], []
            other_basic_list, other_sub_list = [], []

            for key, value in benchmark_code_dict.items():
                total_data = total_data_dict[value]
                benchmark_industry_weights = benchmark_industry_weights_dict[value]
                index_rets = index_rets_dict[value]

                start_time = time.time()
                print('------------------------------------------------')
                print('The factor {} is calculated with universe {} and benchmark {}!'.format(factor_name, key, key))
                group_rets_df, return_sub_df = self.calc_return(benchmark=key,
                                                                universe=key,
                                                                trade_date=trade_date,
                                                                factor_name=factor_name,
                                                                total_data=total_data,
                                                                benchmark_weights=benchmark_industry_weights,
                                                                index_rets=index_rets)
                return_basic_list.append(group_rets_df)
                return_sub_list.append(return_sub_df)
                print(group_rets_df)
                print(return_sub_df)

                ic_df, ic_sub_df, group_ic_df, group_ic_sub_df, industry_ic_df = self.calc_icir(benchmark=key,
                                                                                                universe=key,
                                                                                                trade_date=trade_date,
                                                                                                factor_name=factor_name,
                                                                                                total_data=total_data)
                ic_basic_list.append(ic_df)
                ic_sub_list.append(ic_sub_df)
                group_ic_list.append(group_ic_df)
                group_ic_sub_list.append(group_ic_sub_df)
                industry_ic_list.append(industry_ic_df)
                print(ic_df)
                print(ic_sub_df)
                print(group_ic_df)
                print(group_ic_sub_df)
                print(industry_ic_df)

                other_basic_df, other_sub_df = self.calc_other(benchmark=key,
                                                               universe=key,
                                                               trade_date=trade_date,
                                                               factor_name=factor_name,
                                                               total_data=total_data,
                                                               benchmark_weights=benchmark_industry_weights)
                other_basic_list.append(other_basic_df)
                other_sub_list.append(other_sub_df)
                print(other_basic_df)
                print(other_sub_df)
                print(time.time() - start_time)

            # 存储层
            return_basic_df = pd.concat(return_basic_list, axis=0)
            return_sub_df = pd.concat(return_sub_list, axis=0)
            ic_df = pd.concat(ic_basic_list, axis=0).reset_index(drop=True)
            ic_df.drop_duplicates(inplace=True)
            ic_sub_df = pd.concat(ic_sub_list, axis=0).reset_index(drop=True)
            ic_sub_df.drop_duplicates(inplace=True)
            group_ic_df = pd.concat(group_ic_list, axis=0)
            group_ic_sub_df = pd.concat(group_ic_sub_list, axis=0)
            industry_ic_df = pd.concat(industry_ic_list, axis=0)
            other_basic_df = pd.concat(other_basic_list, axis=0)
            other_sub_df = pd.concat(other_sub_list, axis=0)

            storage_engine = PerformanceStorageEngine(self._url)
            storage_engine.update_destdb('factor_performance_return_basic', factor_name, return_basic_df)
            storage_engine.update_destdb('factor_performance_return_sub', factor_name, return_sub_df)

            storage_engine.update_destdb('factor_performance_ic_ir_basic', factor_name, ic_df)
            storage_engine.update_destdb('factor_performance_ic_ir_sub', factor_name, ic_sub_df)
            storage_engine.update_destdb('factor_performance_ic_ir_group', factor_name, group_ic_df)
            storage_engine.update_destdb('factor_performance_ic_ir_group_sub', factor_name, group_ic_sub_df)
            storage_engine.update_destdb('factor_performance_ic_industry', factor_name, industry_ic_df)

            storage_engine.update_destdb('factor_performance_other_basic', factor_name, other_basic_df)
            storage_engine.update_destdb('factor_performance_other_sub', factor_name, other_sub_df)

    def calc_other(self, benchmark, universe, trade_date, factor_name, total_data, benchmark_weights):
        if 'other' not in self._methods:
            return

        # pdb.set_trace()
        method = self._methods['other']
        other = importlib.import_module(method['packet']).__getattribute__(method['class'])()
        other_basic_df = self.other_basic(other, benchmark, universe, trade_date, factor_name, total_data,
                                          benchmark_weights)
        other_sub_df = self.other_sub(other, benchmark, universe, trade_date, factor_name, other_basic_df)
        return other_basic_df, other_sub_df

    def other_basic(self, engine, benchmark, universe, trade_date, factor_name, total_data, benchmark_weights):

        other_basic_list = []

        for neu in [0, 1]:
            # 0 非中性; 1 中性
            if neu == 0:
                if 'group' in total_data.columns:
                    total_data = total_data.drop(['group'], axis=1)
                groups = engine.calc_group(total_data, factor_name)
                total_data = pd.merge(total_data, groups, on=['trade_date', 'security_code'])
            else:
                if 'group' in total_data.columns:
                    total_data = total_data.drop(['group'], axis=1)
                groups = engine.calc_group(total_data, factor_name, industry=True)
                total_data = pd.merge(total_data, groups, on=['trade_date', 'security_code'])

            # 计算换手率
            # if neu == 0:
            #     turnover = engine.calc_turnover(total_data, 5)
            # else:
            #     benchmark_weights_dict = benchmark_weights.T.to_dict()
            #     weights = engine.calc_weight(total_data, benchmark_weights_dict)
            #
            #     if 'weight' in total_data.columns:
            #         total_data = total_data.drop(['weight'], axis=1)
            #
            #     total_data = pd.merge(total_data, weights, on=['trade_date', 'security_code'])
            #     turnover = engine.calc_turnover2(total_data, 5)

            turnover = engine.calc_turnover(total_data, 5)
            turnover = turnover.rename(columns={i: 'turnover_q' + str(i) for i in range(1, 6)})

            # 计算覆盖率
            coverage = total_data.groupby(['trade_date', 'group']).apply(len)
            coverage = coverage.unstack()
            coverage = coverage.rename(columns={i: 'coverage_q' + str(i) for i in range(1, 6)})

            other_basic_df = pd.merge(turnover, coverage, on=['trade_date'])
            other_basic_df['benchmark'] = benchmark
            other_basic_df['universe'] = universe
            other_basic_df['factor_name'] = factor_name
            other_basic_df['neutralization'] = neu

            other_basic_list.append(other_basic_df)

        result = pd.concat(other_basic_list, axis=0)
        result = result.reset_index()
        return result.loc[:, ['factor_name', 'universe', 'benchmark', 'neutralization', 'trade_date', 'turnover_q1',
                              'turnover_q2', 'turnover_q3', 'turnover_q4', 'turnover_q5', 'coverage_q1', 'coverage_q2',
                              'coverage_q3', 'coverage_q4', 'coverage_q5']]

    def other_sub(self, engine, benchmark, universe, trade_date, factor_name, other_df):
        result_list = []
        for neu in [0, 1]:
            other_df_slt = other_df[other_df['neutralization'] == neu]
            other_df_slt = other_df_slt.loc[:,
                           ['turnover_q' + str(i) for i in range(1, 6)] + ['coverage_q' + str(i) for i in range(1, 6)]]

            year_list = [3, 5, 10]

            for year in year_list:
                other_sub_dict = {}
                other_sub_dict['benchmark'] = benchmark
                other_sub_dict['universe'] = universe
                other_sub_dict['trade_date'] = trade_date
                other_sub_dict['factor_name'] = factor_name
                other_sub_dict['neutralization'] = neu
                other_sub_dict['time_type'] = year

                # 平均ic
                other_avg = other_df_slt.iloc[-(year * 12):, :].mean()
                # other_avg = other_avg.rename({'turnover_q'+str(i):'turnover_avg_q'+str(i) for i in range(1,6)})
                # other_avg = other_avg.rename({'coverage_q'+str(i):'coverage_avg_q'+str(i) for i in range(1,6)})
                other_sub_dict.update(other_avg.to_dict())
                other_sub_dict['turnover_q_avg'] = other_avg.loc[['turnover_q' + str(i) for i in range(1, 6)]].mean()
                other_sub_dict['coverage_q_avg'] = other_avg.loc[['coverage_q' + str(i) for i in range(1, 6)]].mean()
                result_list.append(other_sub_dict)
        result = pd.DataFrame(result_list, columns=['factor_name', 'universe', 'benchmark', 'neutralization',
                                                    'time_type', 'trade_date', 'turnover_q1', 'turnover_q2',
                                                    'turnover_q3', 'turnover_q4', 'turnover_q5', 'turnover_q_avg',
                                                    'coverage_q1', 'coverage_q2', 'coverage_q3', 'coverage_q4',
                                                    'coverage_q5', 'coverage_q_avg'])
        return result

        ## ic_ir IC IR相关计算

    def calc_icir(self, benchmark, universe, trade_date, factor_name, total_data):
        if 'icir' not in self._methods:
            return

        method = self._methods['icir']
        icir = importlib.import_module(method['packet']).__getattribute__(method['class'])()
        ic_df = self.ic_ir_basic(icir, universe, factor_name, total_data)
        ic_sub_df = self.ic_ir_sub(icir, universe, trade_date, factor_name, ic_df)
        group_ic_df = self.ic_ir_group(icir, benchmark, universe, factor_name, total_data)
        group_ic_sub_df = self.ic_ir_group_sub(icir, benchmark, universe, trade_date, factor_name, group_ic_df)
        industry_ic_df = self.ic_industry(icir, universe, trade_date, factor_name, total_data)
        return ic_df, ic_sub_df, group_ic_df, group_ic_sub_df, industry_ic_df

    def ic_ir_basic(self, engine, universe, factor_name, total_data):
        ic_df = total_data.groupby(['trade_date']).apply(lambda x: engine.calc_se_ic(x[factor_name],
                                                                                     x['returns'])).to_frame(name='ic')
        ic_df['universe'] = universe
        ic_df['factor_name'] = factor_name
        # ic_df = ic_df.shift(1).dropna()
        ic_df = ic_df.reset_index()
        return ic_df.loc[:, ['factor_name', 'universe', 'trade_date', 'ic']]

    def ic_ir_group(self, engne, benchmark, universe, factor_name, total_data):
        group_ic_list = []
        for neu in [0, 1]:
            # 0 非中性; 1 中性
            if neu == 0:
                if 'group' in total_data.columns:
                    total_data = total_data.drop(['group'], axis=1)
                groups = engne.calc_group(total_data, factor_name)
                total_data = pd.merge(total_data, groups, on=['trade_date', 'security_code'])
            else:
                if 'group' in total_data.columns:
                    total_data = total_data.drop(['group'], axis=1)
                groups = engne.calc_group(total_data, factor_name, industry=True)
                total_data = pd.merge(total_data, groups, on=['trade_date', 'security_code'])

            group_ic = total_data.groupby(['trade_date', 'group']).apply(lambda x: engne.calc_se_ic(x[factor_name],
                                                                                                    x['returns']))
            group_ic = group_ic.unstack()
            group_ic = group_ic.rename(columns={i: 'ic_q' + str(i) for i in range(1, 6)})
            group_ic['universe'] = universe
            group_ic['benchmark'] = benchmark
            group_ic['factor_name'] = factor_name
            group_ic['neutralization'] = neu
            # group_ic = group_ic.shift(1).dropna()
            group_ic_list.append(group_ic)

        group_ic_df = pd.concat(group_ic_list, axis=0)
        group_ic_df = group_ic_df.reset_index()

        return group_ic_df.loc[:, ['factor_name', 'universe', 'benchmark', 'neutralization', 'trade_date', 'ic_q1',
                                   'ic_q2', 'ic_q3', 'ic_q4', 'ic_q5']]

    def ic_ir_sub(self, engine, universe, trade_date, factor_name, ic_df):
        ic_df = ic_df.copy().set_index('trade_date')
        result_list = []
        year_list = [3, 5, 10]
        for year in year_list:
            ic_sub_dict = {}
            ic_sub_dict['universe'] = universe
            ic_sub_dict['factor_name'] = factor_name
            ic_sub_dict['time_type'] = year
            ic_sub_dict['trade_date'] = trade_date

            # 平均IC
            ic_avg = ic_df.iloc[-(year * 12):, :]['ic'].mean()
            ic_sub_dict['ic_avg'] = ic_avg

            # ir
            ir = ic_df.iloc[-(year * 12):, :]['ic'].mean() / ic_df.iloc[-(year * 12):, :]['ic'].std()
            ic_sub_dict['ir'] = ir

            # best_ic
            best_ic = ic_df.iloc[-(year * 12):, :]['ic'].max()
            ic_sub_dict['best_ic'] = best_ic

            # worst_ic, 指负相关，并不特指差
            worst_ic = ic_df.iloc[-(year * 12):, :]['ic'].min()
            ic_sub_dict['worst_ic'] = worst_ic

            # positive_ic_rate
            positive_rate = sum(ic_df.iloc[-(year * 12):, :]['ic'] > 0) / len(ic_df.iloc[-(year * 12):, :])
            ic_sub_dict['positive_ic_rate'] = positive_rate

            # ic_std
            ic_std = ic_df.iloc[-(year * 12):, :]['ic'].std()
            ic_sub_dict['ic_std'] = ic_std

            # ic_t_stat
            ic_t_stat, _ = stats.ttest_1samp(ic_df.iloc[-(year * 12):, :]['ic'].values, 0)
            ic_sub_dict['ic_t_stat'] = ic_t_stat
            result_list.append(ic_sub_dict)

        result = pd.DataFrame(result_list, columns=['factor_name', 'universe', 'time_type', 'trade_date', 'ic_avg',
                                                    'ir', 'best_ic', 'worst_ic', 'positive_ic_rate', 'ic_std',
                                                    'ic_t_stat'])
        return result

    def ic_ir_group_sub(self, engine, benchmark, universe, trade_date, factor_name, group_ic_df):
        group_ic_df = group_ic_df.copy()
        group_ic_df = group_ic_df.set_index('trade_date')

        result_list = []
        for neu in [0, 1]:
            group_ic_df_slt = group_ic_df[group_ic_df['neutralization'] == neu]
            group_ic_df_slt = group_ic_df_slt.loc[:, ['ic_q' + str(i) for i in range(1, 6)]]

            year_list = [3, 5, 10]

            for year in year_list:
                group_ic_sub_dict = {}
                group_ic_sub_dict['benchmark'] = benchmark
                group_ic_sub_dict['universe'] = universe
                group_ic_sub_dict['factor_name'] = factor_name
                group_ic_sub_dict['neutralization'] = neu
                group_ic_sub_dict['time_type'] = year
                group_ic_sub_dict['trade_date'] = trade_date

                # 平均ic
                ic_avg = group_ic_df_slt.iloc[-(year * 12):, :].mean().rename(
                    {'ic_q' + str(i): 'ic_q' + str(i) + '_avg' for i in range(1, 6)})
                group_ic_sub_dict.update(ic_avg.to_dict())
                group_ic_sub_dict['ic_q_avg'] = ic_avg.mean()

                # ir
                ir = group_ic_df_slt.iloc[-(year * 12):, :].mean() / group_ic_df_slt.iloc[-(year * 12):, :].std()
                ir = ir.rename({'ic_q' + str(i): 'ir_q' + str(i) for i in range(1, 6)})
                group_ic_sub_dict.update(ir.to_dict())
                group_ic_sub_dict['ir_q_avg'] = ir.mean()
                result_list.append(group_ic_sub_dict)

        result = pd.DataFrame(result_list, columns=['factor_name', 'universe', 'benchmark', 'neutralization',
                                                    'time_type', 'trade_date', 'ic_q1_avg', 'ic_q2_avg', 'ic_q3_avg',
                                                    'ic_q4_avg', 'ic_q5_avg', 'ic_q_avg', 'ir_q1', 'ir_q2', 'ir_q3',
                                                    'ir_q4', 'ir_q5', 'ir_q_avg'])
        return result

    def ic_industry(self, engine, universe, trade_date, factor_name, total_data):
        total_se = total_data.copy()
        total_se['trade_date'] = total_se['trade_date'].apply(lambda x: pd.Timestamp(x))
        industry_ic = total_se.groupby(['trade_date', 'industry_code']).apply(
            lambda x: engine.calc_se_ic(x[factor_name], x['returns']))
        industry_ic = industry_ic.unstack()
        # industry_ic['universe'] = universe
        # industry_ic['factor_name'] = factor_name
        # industry_ic = industry_ic.shift(1).dropna(how='all')
        # industry_ic = industry_ic.dropna(how='all')
        this_year = industry_ic.index[-1].year
        industry_ic_last_time = industry_ic.iloc[-1, :]
        industry_ic_this_year_mean = industry_ic.loc[str(this_year):, :].mean()
        industry_ic_ttm = industry_ic.rolling(window=12).mean().iloc[-1, :]

        industry_ic_df = pd.DataFrame({'ic_t1m': industry_ic_last_time, 'ic_ytd': industry_ic_this_year_mean,
                                       'ic_ttm': industry_ic_ttm})
        industry_ic_df['universe'] = universe
        industry_ic_df['factor_name'] = factor_name
        industry_ic_df['trade_date'] = trade_date
        industry_ic_df = industry_ic_df.reset_index()
        industry_ic_df = industry_ic_df.rename(columns={'industry_code': 'industry'})

        return industry_ic_df.loc[:, ['factor_name', 'universe', 'trade_date', 'industry', 'ic_t1m',
                                      'ic_ytd', 'ic_ttm']]

    ## calc_return 收益率相关计算
    def calc_return(self, benchmark, universe, trade_date, factor_name, total_data, benchmark_weights, index_rets):
        if 'basic_return' not in self._methods:
            return

        method = self._methods['basic_return']
        basic_return = importlib.import_module(method['packet']).__getattribute__(method['class'])()

        group_rets_df = self.return_basic(engine=basic_return,
                                          benchmark=benchmark,
                                          universe=universe,
                                          factor_name=factor_name,
                                          total_data=total_data,
                                          benchmark_weights=benchmark_weights)

        benchmark_rets_df = index_rets.set_index('trade_date')
        benchmark_rets_df.index = pd.to_datetime(benchmark_rets_df.index)
        benchmark_rets_df = benchmark_rets_df.loc[benchmark_rets_df.index.isin(list(set(group_rets_df.trade_date))), :]

        return_sub_df = self.return_sub(engine=basic_return,
                                        benchmark=benchmark,
                                        universe=universe,
                                        trade_date=trade_date,
                                        factor_name=factor_name,
                                        group_rets_df=group_rets_df,
                                        benchmark_rets_df=benchmark_rets_df,
                                        total_data=total_data)
        return group_rets_df, return_sub_df

    def return_basic(self, engine, benchmark, universe, factor_name, total_data, benchmark_weights):
        # 非中性化
        # 分组收益
        groups = engine.calc_group(total_data, factor_name)
        total_data = pd.merge(total_data, groups, on=['trade_date', 'security_code'])
        equal_weighted_rets = total_data.groupby(['trade_date']).apply(lambda x: x['returns'].mean())
        group_rets_df_non_neu = engine.calc_group_rets(total_data, 5)
        group_rets_df_non_neu = group_rets_df_non_neu.rename(
            columns={'q' + str(i): 'ret_q' + str(i) for i in range(1, 6)})
        for i in range(1, 6):
            group_rets_df_non_neu['relative_ret_q'+str(i)] = group_rets_df_non_neu['ret_q'+str(i)] - equal_weighted_rets
        group_rets_df_non_neu['spread'] = group_rets_df_non_neu['ret_q5'] - group_rets_df_non_neu['ret_q1']
        group_rets_df_non_neu['benchmark'] = benchmark
        group_rets_df_non_neu['universe'] = universe
        group_rets_df_non_neu['factor_name'] = factor_name
        group_rets_df_non_neu['neutralization'] = 0
        group_rets_df_non_neu['weight_scheme'] = 0

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
        for i in range(1, 6):
            group_rets_df_neu['relative_ret_q'+str(i)] = group_rets_df_neu['ret_q'+str(i)] - equal_weighted_rets
        group_rets_df_neu['spread'] = group_rets_df_neu['ret_q5'] - group_rets_df_neu['ret_q1']
        group_rets_df_neu['benchmark'] = benchmark
        group_rets_df_neu['universe'] = universe
        group_rets_df_neu['factor_name'] = factor_name
        group_rets_df_neu['neutralization'] = 1
        group_rets_df_neu['weight_scheme'] = 0

        group_rets = pd.concat([group_rets_df_non_neu, group_rets_df_neu], axis=0)
        group_rets.reset_index(inplace=True)

        return group_rets.loc[:,
               ['factor_name', 'universe', 'benchmark', 'neutralization', 'trade_date', 'weight_scheme',
                'ret_q1', 'ret_q2', 'ret_q3', 'ret_q4', 'ret_q5', 'relative_ret_q1', 'relative_ret_q2',
                'relative_ret_q3', 'relative_ret_q4', 'relative_ret_q5', 'spread']]

    def return_sub(self, engine, benchmark, universe, trade_date, factor_name, total_data, group_rets_df,
                   benchmark_rets_df):
        group_rets_df = group_rets_df.copy()
        group_rets_df = group_rets_df.set_index('trade_date')

        result_list = []
        for neu in [0, 1]:
            # 0 非中性; 1 中性
            if neu == 0:
                groups = engine.calc_group(total_data, factor_name)
                total_data = pd.merge(total_data, groups, on=['trade_date', 'security_code'])
            else:
                if 'group' in total_data.columns:
                    total_data = total_data.drop(['group'], axis=1)
                groups = engine.calc_group(total_data, factor_name, industry=True)
                total_data = pd.merge(total_data, groups, on=['trade_date', 'security_code'])

            group_rets_df_slt = group_rets_df[group_rets_df['neutralization'] == neu]
            group_rets_df_slt = group_rets_df_slt.loc[:, ['ret_q' + str(i) for i in range(1, 6)]]

            # 过去3年、5年、10年的统计量
            year_list = [3, 5, 10]
            for year in year_list:
                ret_sub_dict = {}
                ret_sub_dict['benchmark'] = benchmark
                ret_sub_dict['universe'] = universe
                ret_sub_dict['factor_name'] = factor_name
                ret_sub_dict['neutralization'] = neu
                ret_sub_dict['time_type'] = year
                ret_sub_dict['trade_date'] = trade_date
                ret_sub_dict['weight_scheme'] = 0

                # 分组平均收益
                group_avg_ret = group_rets_df_slt.iloc[-(year * 12):, :].mean().rename(
                    {'ret_q' + str(i): 'ret_avg_q' + str(i) for i in range(1, 6)})
                group_avg_ret_dict = group_avg_ret.to_dict()
                group_avg_ret_dict['spread_avg'] = group_avg_ret_dict['ret_avg_q1'] - group_avg_ret_dict['ret_avg_q5']
                ret_sub_dict.update(group_avg_ret_dict)

                # 分组最优、最差收益收益
                group_best_ret = group_rets_df_slt.iloc[-(year * 12):, :].max().rename(
                    {'ret_q' + str(i): 'ret_best_q' + str(i) for i in range(1, 6)})
                group_best_ret_dict = group_best_ret.to_dict()
                ret_sub_dict.update(group_best_ret_dict)

                group_worst_ret = group_rets_df_slt.iloc[-(year * 12):, :].min().rename(
                    {'ret_q' + str(i): 'ret_worst_q' + str(i) for i in range(1, 6)})
                group_worst_ret_dict = group_worst_ret.to_dict()
                ret_sub_dict.update(group_worst_ret_dict)

                # 分组最优、最差主动收益
                group_active_ret = group_rets_df_slt.sub(benchmark_rets_df.loc[group_rets_df_slt.index, 'returns'],
                                                         axis=0)

                group_best_active_ret = group_active_ret.iloc[-(year * 12):, :].max().rename(
                    {'ret_q' + str(i): 'active_ret_best_q' + str(i) for i in range(1, 6)})
                group_best_active_ret_dict = group_best_active_ret.to_dict()
                ret_sub_dict.update(group_best_active_ret_dict)

                group_worst_active_ret = group_active_ret.iloc[-(year * 12):, :].min().rename(
                    {'ret_q' + str(i): 'active_ret_worst_q' + str(i) for i in range(1, 6)})
                group_worst_active_ret_dict = group_worst_active_ret.to_dict()
                ret_sub_dict.update(group_worst_active_ret_dict)

                # 胜率
                # 数据周期不够
                group_hitratio_dict = engine.calc_group_hitratio(group_rets_df_slt.iloc[-(year * 12):, :],
                                                                 benchmark_rets_df.iloc[-(year * 12):, :]['returns'])
                ret_sub_dict.update(group_hitratio_dict)

                # 截面平均胜率
                # 此处股票收益率放在T-1期，需要调整benchmark收益率

                # benchmark_rets_se_forward = benchmark_rets_df.shift(-1).dropna()
                benchmark_rets_se_slt = benchmark_rets_df.iloc[-(12 * year):]['returns']
                total_data_slt = total_data[total_data.trade_date.isin(list(set(benchmark_rets_se_slt.index)))]
                group_cs_hitratio_dict = engine.calc_cs_group_hitratio(total_data_slt,
                                                                       benchmark_rets_se_slt)
                ret_sub_dict.update(group_cs_hitratio_dict)

                result_list.append(ret_sub_dict)

        result = pd.DataFrame(result_list, columns=['factor_name', 'universe', 'benchmark', 'neutralization',
                                                    'time_type', 'trade_date', 'weight_scheme', 'ret_avg_q1',
                                                    'ret_avg_q2', 'ret_avg_q3', 'ret_avg_q4',
                                                    'ret_avg_q5', 'spread_avg',
                                                    'ret_best_q1', 'ret_best_q2', 'ret_best_q3', 'ret_best_q4',
                                                    'ret_best_q5', 'ret_worst_q1', 'ret_worst_q2', 'ret_worst_q3',
                                                    'ret_worst_q4', 'ret_worst_q5', 'active_ret_best_q1',
                                                    'active_ret_best_q2', 'active_ret_best_q3', 'active_ret_best_q4',
                                                    'active_ret_best_q5', 'active_ret_worst_q1', 'active_ret_worst_q2',
                                                    'active_ret_worst_q3', 'active_ret_worst_q4', 'active_ret_worst_q5',
                                                    'hr_q1', 'hr_q2', 'hr_q3', 'hr_q4', 'hr_q5', 'ba_q1', 'ba_q2',
                                                    'ba_q3', 'ba_q4', 'ba_q5'])
        return result
