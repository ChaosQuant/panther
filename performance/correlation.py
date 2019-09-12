# -*- coding: utf-8 -*-
import six,pdb,math
from scipy import stats
import numpy as np
import pandas as pd
from utilities.singleton import Singleton

@six.add_metaclass(Singleton)
class Correlation(object):
    def __init__(self):
        __str__ = 'correlation'
        self.name = 'correlation'
        
    def calc_correlation(self, factor_df, factor, factor_compare):
        grouped = factor_df.groupby(['trade_date'])
        correlation_dict = {i: [] for i in factor_compare if i != factor}
        trade_dates = []
    
        for k, g in grouped:
            trade_dates.append(k)
            for factor_cmp in correlation_dict.keys():
                cor = calc_se_ic(g[factor], g[factor_cmp])
                correlation_dict[factor_cmp].append(cor)

        correlation_dict['trade_date'] = trade_dates
        correlation = pd.DataFrame(correlation_dict)
        correlation = correlation.set_index('trade_date')
    
        return correlation
    
    
    def calc_correlation_info(total_data, factor_name, factor_list, period):
        trade_dates = list(set(total_data['trade_date']))
        if len(trade_dates) < period:
            print('The num of dates is less than {}.'.format(period))
        trade_dates.sort()
        trade_date_min = trade_dates[-period]
        total_data = total_data[total_data['trade_date'] > trade_date_min]
    
        correlation = calc_correlation(total_data, factor_name, factor_list)
        correlation_mean = correlation.mean()
    
        ## 收益率差t统计量
        correlation_t_dict = {}
        for col in correlation.columns:
            correlation_t, _ = stats.ttest_1samp(correlation[col].values, 0)
            correlation_t_dict[col] = correlation_t
        correlation_t = pd.Series(correlation_t_dict)
    
        output_dict = {}
        output_dict['correlation'] = correlation
        output_dict['correlation_mean'] = correlation_mean
        output_dict['correlation_t'] = correlation_t
    
        return output_dict
    