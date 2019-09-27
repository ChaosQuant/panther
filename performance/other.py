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
        def calc_grouped(data):
            group_df = pd.DataFrame(columns=['security_code', 'group'])
            group_df['group'] = self._se_group(data[factor_name], n_bins)
            group_df['security_code'] = data['security_code'].values
            return group_df.set_index('security_code')
        
        if industry:
            factor_df['trade_date'] = factor_df['trade_date'].apply(lambda x : pd.Timestamp(x))
            #apply 时间类型只识别Timestamp
            total_group_df = factor_df.groupby(['trade_date', 'industry_code']).apply(calc_grouped)
            #total_group_df = total_group_df.reset_index()[['trade_date','security_code','group']]
            #total_group_df['trade_date'] = total_group_df['trade_date'].apply(lambda x : x.date())
        else:
            factor_df['trade_date'] = factor_df['trade_date'].apply(lambda x: pd.Timestamp(x))
            total_group_df = factor_df.groupby(['trade_date']).apply(calc_grouped)
            
        return total_group_df.reset_index()[['trade_date','security_code','group']]
    
    def calc_turnover(self, group_df, n_bins):
        # 非中性换手率，近似算法
        grouped = group_df.groupby(['trade_date'])
        turnover_dict = {i: [] for i in range(1, n_bins + 1)}
        turnover_dict['trade_date'] = []

        i = 1
        for k, g in grouped:
            if i == 1:
                g_last = g.loc[:, ['security_code', 'group']].set_index('group')
            else:
                turnover_dict['trade_date'].append(k)
                g = g.loc[:, ['security_code', 'group']].set_index('group')
                for i in range(1, n_bins + 1):
                    stks = set(g.loc[i, 'security_code'].tolist())
                    stks_last = set(g_last.loc[i, 'security_code'].tolist())
                    stks_overlap = stks & stks_last
                    turnover_dict[i].append(
                        (len(stks) + len(stks_last) - 2.0 * len(stks_overlap)) * 2 / (len(stks) + len(stks_last)))
                g_last = g
            i += 1

        turnover = pd.DataFrame(turnover_dict)
        # turnover = turnover.rename(columns={i:'q'+str(i) for i in range(1,6)})
        # turnover = turnover.shift(1).dropna(how='all')
        turnover = turnover.dropna(how='all')
        turnover.set_index('trade_date', inplace=True)
    
        return turnover
    
    
    def calc_weight(self, group_df, benchmark_weights):
        def calc_grouped(data):
            trade_date_u = data.trade_date.iloc[0]
            industry_code = data.industry_code.iloc[0]
            group = data.group.iloc[0]
            industry_weight = benchmark_weights[trade_date_u.date()][industry_code] if (
                                industry_code in benchmark_weights[trade_date_u.date()]) else 0
            group_weight = pd.DataFrame(columns=['security_code','returns', 'weight'])
            group_weight['returns'] = data['returns'].values
            group_weight['security_code'] = data['security_code'].values
            group_weight['weight'] = industry_weight / len(data) if len(data) > 0 else 0
            group_weight['weight'] = group_weight['weight'].fillna(0)
            return group_weight.set_index('returns')
        
        group_weights = group_df.groupby(['trade_date', 'industry_code', 'group'], axis=0).apply(calc_grouped)
        '''
        grouped = group_df.groupby(['trade_date', 'industry_code', 'group'])
        total_list = []
        for k, g in grouped:
            group_weight = pd.DataFrame(columns=['trade_date', 'security_code', 'weight'])
            industry_weight = benchmark_weights[k[0]][k[1]] if (k[1] in benchmark_weights[k[0]]) else 0
            group_weight['trade_date'] = g['trade_date']
            group_weight['security_code'] = g['security_code']
            group_weight['weight'] = industry_weight / len(g) if len(g) > 0 else None
            group_weight['weight'] = group_weight['weight'].fillna(0)
            total_list.append(group_weight)
    
        group_weights = pd.concat(total_list, axis=0)
        '''
        group_weights = group_weights.reset_index()
        group_weights.sort_values(['trade_date', 'security_code'], inplace=True)
        return group_weights[['trade_date','security_code','weight']]
    
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
                g_last = g.loc[:, ['security_code', 'returns', 'group', 'weight']].set_index('group')
            else:
                turnover_dict['trade_date'].append(k)
                g = g.loc[:, ['security_code', 'returns', 'group', 'weight']].set_index('group')
                for j in range(1, n_bins + 1):
                    single_g = g.loc[j, :].set_index('security_code')
                    single_g['weight'] = self.calc_weight_renew(single_g['weight'], single_g['returns'])
                    single_g_last = g_last.loc[j, :].set_index('security_code')
                    weights_df = pd.merge(single_g, single_g_last, how='outer', left_index=True, right_index=True).fillna(0)
                    turnover_dict[j].append((weights_df['weight_x'] - weights_df['weight_y']).map(abs).sum())
                g_last = g
            i += 1
    
        turnover = pd.DataFrame(turnover_dict)
        turnover = turnover.shift(1).dropna(how='all')
        turnover.set_index('trade_date', inplace=True)
    
        return turnover
    
    
    
    
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