# -*- coding: utf-8 -*-
import six,pdb,math
from scipy import stats
import numpy as np
import pandas as pd
from utilities.singleton import Singleton

@six.add_metaclass(Singleton)
class Other(object):
    def __init__(self):
        __str__ = 'Other'
        self.name = 'Other'
        
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
    
    def calc_turnover(self, group_df, n_bins):
        # 非中性换手率，近似算法
        grouped = group_df.groupby(['trade_date'])
        turnover_dict = {i: [] for i in range(1, n_bins + 1)}
        turnover_dict['trade_date'] = []

        i = 1
        for k, g in grouped:
            if i == 1:
                g_last = g.loc[:, ['code', 'group']].set_index('group')
            else:
                turnover_dict['trade_date'].append(k)
                g = g.loc[:, ['code', 'group']].set_index('group')
                for i in range(1, n_bins + 1):
                    stks = set(g.loc[i, 'code'].tolist())
                    stks_last = set(g_last.loc[i, 'code'].tolist())
                    stks_overlap = stks & stks_last
                    turnover_dict[i].append(
                        (len(stks) + len(stks_last) - 2.0 * len(stks_overlap)) * 2 / (len(stks) + len(stks_last)))
                g_last = g
            i += 1

        turnover = pd.DataFrame(turnover_dict)
        # turnover = turnover.rename(columns={i:'q'+str(i) for i in range(1,6)})
        turnover = turnover.shift(1).dropna(how='all')
        turnover.set_index('trade_date', inplace=True)
    
        return turnover
    
    
    def calc_weight(self, group_df, benchmark_weights):
        grouped = group_df.groupby(['trade_date', 'industry', 'group'])
        total_list = []
        for k, g in grouped:
            group_weight = pd.DataFrame(columns=['trade_date', 'code', 'weight'])
            industry_weight = benchmark_weights[k[0]][k[1]] if (k[1] in benchmark_weights[k[0]]) else 0
            group_weight['trade_date'] = g['trade_date']
            group_weight['code'] = g['code']
            group_weight['weight'] = industry_weight / len(g) if len(g) > 0 else None
            group_weight['weight'] = group_weight['weight'].fillna(0)
            total_list.append(group_weight)
    
        group_weights = pd.concat(total_list, axis=0)
        group_weights.sort_values(['trade_date', 'code'], inplace=True)
        group_weights.reset_index(drop=True, inplace=True)
        return group_weights
    
    def calc_weight_renew(self, weight_se, rets_se):
        weight_new = pd.Series(index=weight_se)
        weight_new = weight_se * (1 + rets_se)
        return weight_new / weight_new.sum()
    
    def calc_turnover2(self, group_df, n_bins):
        # 中性化换手率
        grouped = group_df.groupby(['trade_date'])
        turnover_dict = {i: [] for i in range(1, n_bins + 1)}
        turnover_dict['trade_date'] = []
    
        i = 1
        for k, g in grouped:
            if i == 1:
                g_last = g.loc[:, ['code', 'dx', 'group', 'weight']].set_index('group')
            else:
                turnover_dict['trade_date'].append(k)
                g = g.loc[:, ['code', 'dx', 'group', 'weight']].set_index('group')
                for j in range(1, n_bins + 1):
                    single_g = g.loc[j, :].set_index('code')
                    single_g['weight'] = self.calc_weight_renew(single_g['weight'], single_g['dx'])
                    single_g_last = g_last.loc[j, :].set_index('code')
                    weights_df = pd.merge(single_g, single_g_last, how='outer', left_index=True, right_index=True).fillna(0)
                    turnover_dict[j].append((weights_df['weight_x'] - weights_df['weight_y']).map(abs).sum())
                g_last = g
            i += 1
    
        turnover = pd.DataFrame(turnover_dict)
        turnover = turnover.shift(1).dropna(how='all')
        turnover.set_index('trade_date', inplace=True)
    
        return turnover
    
    
    def other_basic(self, benchmark, universe, factor_name, total_data, benchmark_weights):
        """
        返回factor_performance_other_basic信息
        """

        other_basic_list = [] # 为了后一步计算使用，工程化可以删除

        for neu in [0,1]:
            # 0 非中性; 1 中性
            if neu == 0:
                if 'group' in total_data.columns:
                    total_data = total_data.drop(['group'], axis=1) 
                groups = self.calc_group(total_data, factor_name)
                total_data = pd.merge(total_data, groups, on=['trade_date', 'code'])
            else:
                if 'group' in total_data.columns:
                    total_data = total_data.drop(['group'], axis=1) 
                groups = self.calc_group(total_data, factor_name, industry=True)
                total_data = pd.merge(total_data, groups, on=['trade_date', 'code'])

            # 计算换手率
            if neu == 0:
                turnover = self.calc_turnover(total_data, 5)
            else:
                benchmark_weights_dict = benchmark_weights.T.to_dict()
                weights = self.calc_weight(total_data, benchmark_weights_dict)
    
                if 'weight' in total_data.columns:
                    total_data = total_data.drop(['weight'], axis=1)
    
                total_data = pd.merge(total_data, weights, on=['trade_date', 'code'])
                turnover = self.calc_turnover2(total_data, 5)
        
            turnover = turnover.rename(columns={i:'turnover_q'+str(i) for i in range(1,6)})
    
            # 计算覆盖率
            coverage = total_data.groupby(['trade_date', 'group']).apply(len)
            coverage = coverage.unstack()
            coverage = coverage.rename(columns={i:'coverage_q'+str(i) for i in range(1,6)})
    
            other_basic_df = pd.merge(turnover,coverage,on=['trade_date'])
            other_basic_df['benchmark'] = benchmark
            other_basic_df['universe'] = universe
            other_basic_df['factor_name'] = factor_name
            other_basic_df['neutralization'] = neu

            other_basic_list.append(other_basic_df)
    
        return pd.concat(other_basic_list, axis=0)
    
    
    def other_sub(self, benchmark, universe, factor_name, other_df):
        """
        返回factor_performance_ic_ir_group_sub信息
        """
        sub_dict = {}
        for neu in [0,1]:
            other_df_slt = other_df[other_df['neutralization']==neu]
            other_df_slt = other_df_slt.loc[:, ['turnover_q'+str(i) for i in range(1,6)]+['coverage_q'+str(i) for i in range(1,6)]]

            year_list = [3, 5, 10]

            for year in year_list:
                other_sub_dict = {}
                other_sub_dict['benchmark'] = benchmark
                other_sub_dict['universe'] = universe
                other_sub_dict['factor_name'] = factor_name
                other_sub_dict['neutralization'] = neu
                other_sub_dict['time_type'] = year
    
                # 平均ic
                other_avg = other_df_slt.iloc[-(year*12):,:].mean()
                other_avg = other_avg.rename({'turnover_q'+str(i):'turnover_avg_q'+str(i) for i in range(1,6)})
                other_avg = other_avg.rename({'coverage_q'+str(i):'coverage_avg_q'+str(i) for i in range(1,6)})
                other_sub_dict.update(other_avg.to_dict())
                other_sub_dict['turnover_q_avg'] = other_avg.loc[['turnover_avg_q'+str(i) for i in range(1,6)]].mean()
                other_sub_dict['coverage_q_avg'] = other_avg.loc[['coverage_avg_q'+str(i) for i in range(1,6)]].mean()
                sub_dict[str(neu) + '_' + str(year)] = other_sub_dict
        return sub_dict