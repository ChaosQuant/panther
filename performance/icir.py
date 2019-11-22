# -*- coding: utf-8 -*-
import six,pdb,math
from scipy import stats
import numpy as np
import pandas as pd
from utilities.singleton import Singleton

@six.add_metaclass(Singleton)
class ICIR(object):
    def __init__(self):
        __str__ = 'ic'
        self.name = 'ic'
        
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
    
    
    def calc_se_ic(self, se1, se2):
        '''
        计算IC值，主要为了处理NaN的情况
        '''
        #df = pd.concat([se1, se2], axis=1, join='inner')
        #df = df.replace([np.inf, -np.inf], np.nan)
        se1 = se1.copy().replace([np.inf, -np.inf], np.nan).fillna(0)
        se2 = se2.copy().replace([np.inf, -np.inf], np.nan).fillna(0)
        return np.corrcoef(se1.fillna(0).values,se2.fillna(0).values)[0, 1]
    
    
    def ic_ir_basic(self, universe, factor_name, total_data):
        ic_df = total_data.groupby(['trade_date']).apply(lambda x: self.calc_se_ic(x[factor_name], x['dx'])).to_frame(name='ic')
        ic_df['universe'] = universe
        ic_df['factor_name'] = factor_name
        ic_df = ic_df.shift(1).dropna()
        return ic_df
    
    
    def ic_ir_sub(self,universe,factor_name,ic_df):
        year_list = [3, 5, 10]
        
        sub_dict = {}
        for year in year_list:
            ic_sub_dict = {}
            ic_sub_dict['universe'] = universe
            ic_sub_dict['factor_name'] = factor_name
            ic_sub_dict['time_type'] = year
    
            # 平均IC
            ic_avg = ic_df.iloc[-(year*12):,:]['ic'].mean()
            ic_sub_dict['ic_avg'] = ic_avg
    
            # ir
            ir = ic_df.iloc[-(year*12):,:]['ic'].mean()/ic_df.iloc[-(year*12):,:]['ic'].std()
            ic_sub_dict['ir'] = ir
        
            # best_ic
            best_ic = ic_df.iloc[-(year*12):,:]['ic'].max()
            ic_sub_dict['best_ic'] = best_ic
    
            # worst_ic, 指负相关，并不特指差
            worst_ic = ic_df.iloc[-(year*12):,:]['ic'].min()
            ic_sub_dict['worst_ic'] = worst_ic
    
            # positive_ic_rate
            positive_rate = sum(ic_df.iloc[-(year*12):,:]['ic']>0)/len(ic_df.iloc[-(year*12):,:])
            ic_sub_dict['positive_ic_rate'] = positive_rate
    
            # ic_std
            ic_std = ic_df.iloc[-(year*12):,:]['ic'].std()
            ic_sub_dict['ic_std'] = ic_std

            # ic_t_stat
            ic_t_stat, _ = stats.ttest_1samp(ic_df.iloc[-(year*12):,:]['ic'].values, 0)
            ic_sub_dict['ic_t_stat'] = ic_t_stat
            
            sub_dict[year] = ic_sub_dict
        return sub_dict
    
    
    def ic_ir_group(self, benchmark, universe, factor_name, total_data):
        """
        返回factor_performance_ic_ir_group信息
        """

        group_ic_list = [] # 工程环境可删除

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
    
            group_ic = total_data.groupby(['trade_date', 'group']).apply(lambda x: self.calc_se_ic(x[factor_name], x['dx']))
            group_ic = group_ic.unstack()
            group_ic = group_ic.rename(columns={i:'ic_q'+str(i) for i in range(1,6)})
            group_ic['universe'] = universe
            group_ic['benchmark'] = benchmark
            group_ic['factor_name'] = factor_name
            group_ic['neutralization'] = neu
            group_ic = group_ic.shift(1).dropna()
            group_ic_list.append(group_ic)
        return pd.concat(group_ic_list, axis=0)

    def ic_ir_group_sub(self, benchmark, universe, factor_name, group_ic_df):
        group_ic_sub_list = []
        for neu in [0,1]:
            group_ic_df_slt = group_ic_df[group_ic_df['neutralization']==neu]
            group_ic_df_slt = group_ic_df_slt.loc[:, ['ic_q'+str(i) for i in range(1,6)]]
    
            year_list = [3, 5, 10]
    
            for year in year_list:
                group_ic_sub_dict = {}
                group_ic_sub_dict['benchmark'] = benchmark
                group_ic_sub_dict['universe'] = universe
                group_ic_sub_dict['factor_name'] = factor_name
                group_ic_sub_dict['neutralization'] = neu
                group_ic_sub_dict['time_type'] = year
    
                # 平均ic
                ic_avg = group_ic_df_slt.iloc[-(year*12):,:].mean().rename({'ic_q'+str(i):'ic_avg_q'+str(i) for i in range(1,6)})
                group_ic_sub_dict.update(ic_avg.to_dict())
                group_ic_sub_dict['ic_q_avg'] = ic_avg.mean()
    
                # ir
                ir = group_ic_df_slt.iloc[-(year*12):,:].mean()/group_ic_df_slt.iloc[-(year*12):,:].std()
                ir = ir.rename({'ic_q'+str(i):'ir_q'+str(i) for i in range(1,6)})
                group_ic_sub_dict.update(ir.to_dict())
                group_ic_sub_dict['ir_q_avg'] = ir.mean()
    
                group_ic_sub_list.append(group_ic_sub_dict) 
        return group_ic_sub_list

            
    def ic_industry(self, universe, factor_name, total_data):
        industry_ic = total_data.groupby(['trade_date', 'industry']).apply(lambda x: self.calc_se_ic(x[factor_name], x['dx']))
        industry_ic = industry_ic.unstack()
        industry_ic['universe'] = universe
        industry_ic['factor_name'] = factor_name
        industry_ic = industry_ic.shift(1).dropna(how='all')
        # 最好从数据库读取字段再更新,怎么存，增加字段 industry?
        this_year = industry_ic.index[-1].year
        industry_ic_last_time = industry_ic.iloc[-1, :]
        industry_ic_this_year_mean = industry_ic.loc[str(this_year):, :].mean()
        industry_ic_ttm = industry_ic.drop(['universe','factor_name'], axis=1).rolling(window=12).mean().dropna(how='all')
        return industry_ic_last_time, industry_ic_this_year_mean, industry_ic_ttm
            
         