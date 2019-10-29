# -*- coding: utf-8 -*-
import six, pdb, math
from scipy import stats
import numpy as np
import pandas as pd
from utilities.singleton import Singleton


@six.add_metaclass(Singleton)
class IntegratedReturn(object):
    def __init__(self):
        __str__ = 'integrated_return'
        self.name = 'IntegratedReturn'

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
            factor_df['trade_date'] = factor_df['trade_date'].apply(lambda x: pd.Timestamp(x))
            # apply 时间类型只识别Timestamp
            total_group_df = factor_df.groupby(['trade_date', 'industry_code']).apply(calc_grouped)
            # total_group_df = total_group_df.reset_index()[['trade_date','security_code','group']]
            # total_group_df['trade_date'] = total_group_df['trade_date'].apply(lambda x : x.date())
        else:
            factor_df['trade_date'] = factor_df['trade_date'].apply(lambda x: pd.Timestamp(x))
            total_group_df = factor_df.groupby(['trade_date']).apply(calc_grouped)

        return total_group_df.reset_index()[['trade_date', 'security_code', 'group']]
        '''
        if industry:
            grouped = factor_df.groupby(['trade_date', 'industry'])
        else:
            grouped = factor_df.groupby(['trade_date'])

        group_list = []


        for k, g in grouped:
            group_df = pd.DataFrame(columns=['trade_date', 'security_code', 'group'])
            group_df['group'] = self._se_group(g[factor_name], n_bins)
            group_df['security_code'] = g['security_code']
            group_df['trade_date'] = g['trade_date']
            group_list.append(group_df)


        total_group_df = pd.concat(group_list, axis=0)

        total_group_df.sort_values(['trade_date', 'security_code'], inplace=True)
        total_group_df.reset_index(drop=True, inplace=True)
        '''

    def calc_group_rets(self, group_df, n_bins, benchmark_weights=None, industry=False):
        def calc_groupd(data):
            trade_date_u = data.trade_date.iloc[0]
            industry_code = data.industry_code.iloc[0]
            group = data.group.iloc[0]
            industry_weight = benchmark_weights[trade_date_u.date()][industry_code] if (
                    industry_code in benchmark_weights[trade_date_u.date()]) else 0
            group_weight = pd.DataFrame(columns=['security_code', 'returns', 'weight'])
            group_weight['returns'] = data['returns'].values
            group_weight['security_code'] = data['security_code'].values
            group_weight['weight'] = industry_weight / len(data) if len(data) > 0 else 0
            group_weight['weight'] = group_weight['weight'].fillna(0) / 100
            return group_weight.set_index('returns')

        # 中性化分组收益， 非中心化分组收益
        if not industry:
            group_rets = group_df.groupby(['trade_date', 'group']).apply(lambda x: x['returns'].mean())
            group_rets = group_rets.unstack()
            # group_rets = group_rets.shift(1).dropna()
            group_rets = group_rets.rename(columns={i: 'q' + str(i) for i in range(1, 6)})
            group_rets = group_rets.reset_index()
            group_rets['trade_date'] = group_rets['trade_date'].apply(lambda x: pd.Timestamp(x))
            group_rets = group_rets.set_index('trade_date')
            return group_rets
        else:
            group_df['trade_date'] = group_df['trade_date'].apply(lambda x: pd.Timestamp(x))
            group_weights = group_df.groupby(['trade_date', 'industry_code', 'group'], axis=0).apply(calc_groupd)
            '''
            grouped = group_df.groupby(['trade_date', 'industry_code', 'group'])
            total_list = []
            for k, g in grouped:
                group_weight = pd.DataFrame(columns=['trade_date', 'security_code', 'group', 'returns', 'weight'])
                industry_weight = benchmark_weights[k[0]][k[1]] if (k[1] in benchmark_weights[k[0]]) else 0
                group_weight['trade_date'] = g['trade_date']
                group_weight['security_code'] = g['security_code']
                group_weight['group'] = g['group']
                group_weight['returns'] = g['returns']
                group_weight['weight'] = industry_weight / len(g) if len(g) > 0 else 0
                group_weight['weight'] = group_weight['weight'].fillna(0)
                total_list.append(group_weight)

            group_weights = pd.concat(total_list, axis=0)
            '''
            group_weights = group_weights.reset_index()
            group_weights.sort_values(['trade_date', 'security_code'], inplace=True)
            group_rets = group_weights.groupby(['trade_date', 'group']).apply(
                lambda x: x.dropna()['returns'].dot(x.dropna()['weight']))
            group_rets = group_rets.unstack()
            # group_rets = group_rets.shift(1).dropna()
            group_rets = group_rets.rename(columns={i: 'q' + str(i) for i in range(1, 6)})

            return group_rets

    def calc_relative_rets(self, group_rets_df, benchmark_rets_se):
        rlt_rets = {}
        for col in group_rets_df.columns:
            rlt_rets[col] = group_rets_df[col] - benchmark_rets_se

        relative_rets_df = pd.DataFrame(rlt_rets, index=group_rets_df.index)

        return relative_rets_df

    # 非中性化 分组收益
    def group_rets_df_non_neu(self, total_data, factor_name, benchmark, universe):
        groups = self.calc_group(total_data, factor_name)
        total_data = pd.merge(total_data, groups, on=['trade_date', 'security_code'])
        group_rets_df_non_neu = self.calc_group_rets(total_data, 5)
        group_rets_df_non_neu = group_rets_df_non_neu.rename(
            columns={'q' + str(i): 'ret_q' + str(i) for i in range(1, 6)})
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
        total_data = pd.merge(total_data, groups, on=['trade_date', 'security_code'])
        group_rets_df_neu = self.calc_group_rets(total_data, 5, benchmark_weights=benchmark_weights_dict, industry=True)
        group_rets_df_neu = group_rets_df_neu.rename(columns={'q' + str(i): 'ret_q' + str(i) for i in range(1, 6)})
        group_rets_df_neu['spread'] = group_rets_df_neu['ret_q1'] - group_rets_df_neu['ret_q5']
        group_rets_df_neu['benchmark'] = benchmark
        group_rets_df_neu['universe'] = universe
        group_rets_df_neu['factor_name'] = factor_name
        group_rets_df_neu['neutralization'] = 1
        return group_rets_df_neu