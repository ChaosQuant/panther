# -*- coding: utf-8 -*-
import six,pdb,math
from scipy import stats
import numpy as np
import pandas as pd
from utilities.singleton import Singleton

@six.add_metaclass(Singleton)
class BasciReturn(object):
    def __init__(self):
        __str__ = 'basic_return'
        self.name = 'BasciReturn'
    
    
    def _se_group(self, se, n_bins):
        """
        根据变量值分组
        :param x:
        :param n_bins:
        :return:
        """
        length = len(se)
        group = se.rank(method='first')
        group.dropna(inplace=True)
        group = group.map(lambda x: int(math.ceil(x / length * n_bins)))
        return group

    def calc_group(self, factor_df, factor_name, n_bins=5, industry=False):
        if industry:
            grouped = factor_df.groupby(['trade_date', 'industry'])
        else:
            grouped = factor_df.groupby(['trade_date'])

        group_list = []

        #此处gevent提高性能计算
        for k, g in grouped:
            group_df = pd.DataFrame(columns=['trade_date', 'code', 'group'])
            group_df['group'] = self._se_group(g[factor_name], n_bins)
            group_df['code'] = g['code']
            group_df['trade_date'] = g['trade_date']
            group_list.append(group_df)

        total_group_df = pd.concat(group_list, axis=0)
        total_group_df.sort_values(['trade_date', 'code'], inplace=True)
        total_group_df.reset_index(drop=True, inplace=True)

        return total_group_df
    
    
    def calc_group_rets(self, group_df, n_bins, benchmark_weights=None, industry=False, nargout=1):
    # 中性化分组收益， 非中心化分组收益
        if not industry:
            group_rets = group_df.groupby(['trade_date', 'group']).apply(lambda x: x['dx'].mean())
            group_rets = group_rets.unstack()
            group_rets = group_rets.shift(1).dropna()
            group_rets = group_rets.rename(columns={i:'q'+str(i) for i in range(1,6)})
            return group_rets
        else:
            grouped = group_df.groupby(['trade_date', 'industry', 'group'])
            total_list = []
            for k, g in grouped:
                group_weight = pd.DataFrame(columns=['trade_date', 'code', 'group', 'dx', 'weight'])
                industry_weight = benchmark_weights[k[0]][k[1]] if (k[1] in benchmark_weights[k[0]]) else 0
                group_weight['trade_date'] = g['trade_date']
                group_weight['code'] = g['code']
                group_weight['group'] = g['group']
                group_weight['dx'] = g['dx']
                group_weight['weight'] = industry_weight / len(g) if len(g) > 0 else 0
                group_weight['weight'] = group_weight['weight'].fillna(0)
                total_list.append(group_weight)
    
            group_weights = pd.concat(total_list, axis=0)
            group_weights.sort_values(['trade_date', 'code'], inplace=True)
            group_weights.reset_index(drop=True, inplace=True)
    
            group_rets = group_weights.groupby(['trade_date', 'group']).apply(
                lambda x: x.dropna()['dx'].dot(x.dropna()['weight']))
            group_rets = group_rets.unstack()
            group_rets = group_rets.shift(1).dropna()
            group_rets = group_rets.rename(columns={i:'q'+str(i) for i in range(1,6)})
    
            if nargout == 1:
                return group_rets
            else:
                return group_rets, group_weights
        
        
    def calc_relative_rets(self, group_rets_df, benchmark_rets_se):
        rlt_rets = {}
        for col in group_rets_df.columns:
            rlt_rets[col] = group_rets_df[col] - benchmark_rets_se
    
        relative_rets_df = pd.DataFrame(rlt_rets, index=group_rets_df.index)
    
        return relative_rets_df
    
    def calc_group_hitratio(self, group_rets_df, benchmark_rets_se):
        # 返回字典
        group_hitratio = {}
        group_rets_df = group_rets_df.copy(deep=True)
    
        for col in ['ret_q'+str(i) for i in range(1,6)]:
            se = group_rets_df[col] - benchmark_rets_se
            se = se.dropna()
            group_hitratio['hr_'+col] = sum(se>0) / len(se)
    
        return group_hitratio

    
    def calc_cs_group_hitratio(self, stock_rets_df, benchmark_rets_se):
        stock_rets_df = stock_rets_df.copy(deep=True)

        stock_rets_df['hit'] = stock_rets_df['dx'] - benchmark_rets_se
        cs_hit = stock_rets_df.groupby(['trade_date', 'group']).apply(lambda x: sum(x['hit'] > 0) / len(x['hit']) * 100)
        cs_hit = cs_hit.unstack()
        cs_hit.rename(columns={col: 'ba_q' + str(col) for col in cs_hit.columns}, inplace=True)
        group_cs_hitratio = cs_hit.mean().to_dict()
        return group_cs_hitratio
    
    # 非中性化 分组收益
    def group_rets_df_non_neu(self, total_data, factor_name, benchmark, universe):
        groups = self.calc_group(total_data, factor_name)
        total_data = pd.merge(total_data, groups, on=['trade_date', 'code'])
        group_rets_df_non_neu = self.calc_group_rets(total_data, 5)
        group_rets_df_non_neu = group_rets_df_non_neu.rename(columns={'q'+str(i):'ret_q'+str(i) for i in range(1,6)})
        group_rets_df_non_neu['spread'] = group_rets_df_non_neu['ret_q1'] - group_rets_df_non_neu['ret_q5']
        group_rets_df_non_neu['benchmark'] = benchmark
        group_rets_df_non_neu['universe'] = universe
        group_rets_df_non_neu['factor_name'] = factor_name
        group_rets_df_non_neu['neutralization'] = 0
        return group_rets_df_non_neu
    
    # 中性化 分组收益
    def group_rets_df_neu(self, total_data, benchmark_weights, factor_name, benchmark, universe):
        benchmark_weights_dict = benchmark_weights.T.to_dict()

        if 'group' in total_data.columns:
            total_data = total_data.drop(['group'], axis=1)
        groups = self.calc_group(total_data, factor_name, industry=True)
        total_data = pd.merge(total_data, groups, on=['trade_date', 'code'])
        group_rets_df_neu = self.calc_group_rets(total_data, 5, benchmark_weights=benchmark_weights_dict, industry=True)
        group_rets_df_neu = group_rets_df_neu.rename(columns={'q'+str(i):'ret_q'+str(i) for i in range(1,6)})
        group_rets_df_neu['spread'] = group_rets_df_neu['ret_q1'] - group_rets_df_neu['ret_q5']
        group_rets_df_neu['benchmark'] = benchmark
        group_rets_df_neu['universe'] = universe
        group_rets_df_neu['factor_name'] = factor_name
        group_rets_df_neu['neutralization'] = 1
        return group_rets_df_neu
    
    
    def calc_return_sub(self, total_data, group_rets_df, benchmark_rets_se, factor_name, universe, benchmark):
        """
        返回表factor_performance_return_sub信息
        从数据库取过去10年的group_rets_df, benchmark_rets_df, 包括中性或非中性从外部导入，应制定universe和benchmark
        :params group_rets_df: dataframe, q1至q5
        :params benchmark_rets_df: dataframe, 基准收益率，给定基准1-hs300或基准2-zz500
        """
        sub_dict = {}
        for neu in [0,1]:
            # 0 非中性; 1 中性
            if neu == 0:
                groups = self.calc_group(total_data, factor_name)
                total_data = pd.merge(total_data, groups, on=['trade_date', 'code'])
            else:
                if 'group' in total_data.columns:
                    total_data = total_data.drop(['group'], axis=1) 
                groups = self.calc_group(total_data, factor_name, industry=True)
                total_data = pd.merge(total_data, groups, on=['trade_date', 'code'])
            
            group_rets_df_slt = group_rets_df[group_rets_df['neutralization']==neu]
            group_rets_df_slt = group_rets_df_slt.loc[:, ['ret_q'+str(i) for i in range(1,6)]]
    
            # 过去3年、5年、10年的统计量
            year_list = [3, 5, 10]
    
            for year in year_list:
                ret_sub_dict = {}
                ret_sub_dict['benchmark'] = benchmark
                ret_sub_dict['universe'] = universe
                ret_sub_dict['factor_name'] = factor_name
                ret_sub_dict['neutralization'] = neu
                ret_sub_dict['time_type'] = year
    
                # 分组平均收益
                group_avg_ret = group_rets_df_slt.iloc[-(year*12):,:].mean().rename({'ret_q'+str(i):'ret_avg_q'+str(i) for i in range(1,6)})
                group_avg_ret_dict = group_avg_ret.to_dict()
                group_avg_ret_dict['spread_avg'] = group_avg_ret_dict['ret_avg_q1'] - group_avg_ret_dict['ret_avg_q5']
                ret_sub_dict.update(group_avg_ret_dict)
    
                # 分组最优、最差收益收益
                group_best_ret = group_rets_df_slt.iloc[-(year*12):,:].max().rename({'ret_q'+str(i):'ret_best_q'+str(i) for i in range(1,6)})
                group_best_ret_dict = group_best_ret.to_dict()
                ret_sub_dict.update(group_best_ret_dict)
    
                group_worst_ret = group_rets_df_slt.iloc[-(year*12):,:].min().rename({'ret_q'+str(i):'ret_worst_q'+str(i) for i in range(1,6)})
                group_worst_ret_dict = group_worst_ret.to_dict()
                ret_sub_dict.update(group_worst_ret_dict)

                # 分组最优、最差主动收益
                group_active_ret = group_rets_df_slt.sub(benchmark_rets_se, axis=0)
    
                group_best_active_ret = group_active_ret.iloc[-(year*12):,:].max().rename({'ret_q'+str(i):'active_ret_best_q'+str(i) for i in range(1,6)})
                group_best_active_ret_dict = group_best_active_ret.to_dict()
                ret_sub_dict.update(group_best_active_ret_dict)

                group_worst_active_ret = group_rets_df_slt.iloc[-(year*12):,:].min().rename({'ret_q'+str(i):'active_ret_worst_q'+str(i) for i in range(1,6)})
                group_worst_active_ret_dict = group_worst_active_ret.to_dict()
                ret_sub_dict.update(group_worst_active_ret_dict)

                # 胜率
                group_hitratio_dict = self.calc_group_hitratio(group_rets_df_slt.iloc[-(year*12):,:], benchmark_rets_se.iloc[-(year*12):])
                ret_sub_dict.update(group_hitratio_dict)

                # 截面平均胜率
                # 此处股票收益率放在T-1期，需要调整benchmark收益率
                benchmark_rets_se_forward = benchmark_rets_se.shift(-1).dropna()
                benchmark_rets_se_slt = benchmark_rets_se_forward.iloc[-(12*year):]
                total_data_slt = total_data[total_data.trade_date.isin(list(set(benchmark_rets_se_slt.index)))]
                group_cs_hitratio_dict = self.calc_cs_group_hitratio(total_data_slt.set_index('trade_date'), benchmark_rets_se_slt) # 此处计算平均少了一个月，待修正
                ret_sub_dict.update(group_cs_hitratio_dict)
                sub_dict[str(neu) + '_' + str(year)] = ret_sub_dict
                #print(ret_sub_dict)
        return sub_dict