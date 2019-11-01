#! /usr/bin/env python
# -*- coding: utf-8 -*-

"""
@version: 0.1
@author: li
@file: factor_historical_growth.py
@time: 2019-02-12 10:03
"""
import gc, six
import json
import pandas as pd
from pandas.io.json import json_normalize
from utilities.singleton import Singleton
# from basic_derivation import app
# from ultron.cluster.invoke.cache_data import cache_data


@six.add_metaclass(Singleton)
class FactorHistoricalGrowth(object):
    """
    历史成长
    """
    def __init__(self):
        __str__ = 'factor_historical_growth'
        self.name = '财务指标'
        self.factor_type1 = '财务指标'
        self.factor_type2 = '历史成长'
        self.description = '财务指标的二级指标-历史成长'

    @staticmethod
    def NetAsset1YChg(tp_historical_growth, factor_historical_growth, dependencies=['total_owner_equities',
                                                                                    'total_owner_equities_pre_year']):
        """
        :name: 净资产增长率
        :desc:（本期归属母公司股东的权益（MRQ）-上年同期归属母公司股东的权益（MRQ））/ABS（上年同期归属母公司股东的权益（MRQ）)
        :unit:
        :view_dimension: 0.01
        """

        historical_growth = tp_historical_growth.loc[:, dependencies]
        if len(historical_growth) <= 0:
            return
        # fun = lambda x: ((x[0] / x[1]) - 1.0 if x[1] and x[1] != 0 and x[0] is not None and x[1] is not None else None)
        fun = lambda x: ((x[0] - x[1]) / abs(x[1]) if x[1] and x[1] != 0 and x[0] is not None and x[1] is not None else None)
        historical_growth['NetAsset1YChg'] = historical_growth[dependencies].apply(fun, axis=1)

        historical_growth = historical_growth.drop(dependencies, axis=1)
        factor_historical_growth = pd.merge(factor_historical_growth, historical_growth, on='security_code')
        return factor_historical_growth

    @staticmethod
    def TotalAsset1YChg(tp_historical_growth, factor_historical_growth, dependencies=['total_assets',
                                                                                      'total_assets_pre_year']):
        """
        :name: 总资产增长率
        :desc:（本期资产总计（MRQ）-上年同期资产总计（MRQ））/ABS（上年同期资产总计（MRQ）)
        :unit:
        :view_dimension: 0.01
        """
        historical_growth = tp_historical_growth.loc[:, dependencies]
        # fun = lambda x: ((x[0] / x[1]) - 1.0 if x[1] and x[1] != 0 and x[0] is not None and x[1] is not None else None)
        fun = lambda x: ((x[0] - x[1]) / abs(x[1]) if x[1] and x[1] != 0 and x[0] is not None and x[1] is not None else None)
        historical_growth['TotalAsset1YChg'] = historical_growth[dependencies].apply(fun, axis=1)
        historical_growth = historical_growth.drop(dependencies, axis=1)
        res = pd.merge(factor_historical_growth, historical_growth, on='security_code')
        return res

    @staticmethod
    def FCF1YChgTTM(tp_historical_growth, factor_historical_growth, dependencies=['net_finance_cash_flow',
                                                                                  'net_finance_cash_flow_pre_year']):
        """
        :name: 筹资活动产生的现金流量净额增长率
        :desc: （本期CFF（TTM）-上年同期CFF（TTM））/ABS（上年同期CFF（TTM））
        :unit:
        :view_dimension: 0.01
        """
        historical_growth = tp_historical_growth.loc[:, dependencies]
        # fun = lambda x: ((x[0] / x[1]) - 1 if x[1] and x[1] != 0 and x[1] is not None and x[0] is not None else None)
        fun = lambda x: ((x[0] - x[1]) / abs(x[1]) if x[1] and x[1] != 0 and x[0] is not None and x[1] is not None else None)
        historical_growth['FCF1YChgTTM'] = historical_growth[dependencies].apply(fun, axis=1)

        historical_growth = historical_growth.drop(dependencies, axis=1)
        factor_historical_growth = pd.merge(factor_historical_growth, historical_growth, how='outer', on='security_code')
        return factor_historical_growth

    @staticmethod
    def GrPft1YChgTTM(tp_historical_growth, factor_historical_growth, dependencies=['total_profit',
                                                                                    'total_profit_pre_year']):
        """
        :name: 利润总额增长率
        :desc: （本期利润总额（TTM）-上年同期利润总额（TTM））/ABS（上年同期利润总额（TTM）
        :unit:
        :view_dimension: 0.01
        """
        historical_growth = tp_historical_growth.loc[:, dependencies]

        # fun = lambda x: ((x[0] / x[1]) - 1 if x[1] and x[1] != 0 and x[0] is not None and x[1] is not None else None)
        fun = lambda x: ((x[0] - x[1]) / abs(x[1]) if x[1] and x[1] != 0 and x[0] is not None and x[1] is not None else None)
        historical_growth['GrPft1YChgTTM'] = historical_growth[dependencies].apply(fun, axis=1)

        historical_growth = historical_growth.drop(dependencies, axis=1)
        factor_historical_growth = pd.merge(factor_historical_growth, historical_growth, how='outer', on='security_code')
        return factor_historical_growth

    @staticmethod
    def ICF1YChgTTM(tp_historical_growth, factor_historical_growth, dependencies=['net_invest_cash_flow',
                                                                                  'net_invest_cash_flow_pre_year']):
        """
        :name: 投资活动产生的现金流量净额增长率
        :desc:（本期CFI（TTM）-上年同期CFI（TTM））/ABS（上年同期CFI（TTM））
        :unit:
        :view_dimension: 0.01
        """
        historical_growth = tp_historical_growth.loc[:, dependencies]

        # fun = lambda x: ((x[0] / x[1]) - 1 if x[1] and x[1] != 0 and x[1] is not None and x[0] is not None else None)
        fun = lambda x: ((x[0] - x[1]) / abs(x[1]) if x[1] and x[1] != 0 and x[0] is not None and x[1] is not None else None)
        historical_growth['ICF1YChgTTM'] = historical_growth[dependencies].apply(fun, axis=1)

        historical_growth = historical_growth.drop(dependencies, axis=1)
        factor_historical_growth = pd.merge(factor_historical_growth, historical_growth, how='outer', on='security_code')
        return factor_historical_growth

    @staticmethod
    def NetCF1YChgTTM(tp_historical_growth, factor_historical_growth, dependencies=['n_change_in_cash',
                                                                                    'n_change_in_cash_pre_year']):
        """
        :name: 净现金流量增长率
        :desc:（本期现金净流量（TTM）-上年同期现金净流量（TTM））/ABS（上年同期现金净流量（TTM））
        :unit:
        :view_dimension: 0.01
        """
        historical_growth = tp_historical_growth.loc[:, dependencies]

        # fun = lambda x: ((x[0] / x[1]) - 1 if x[1] and x[1] != 0 and x[0] is not None and x[1] is not None else None)
        fun = lambda x: ((x[0] - x[1]) / abs(x[1]) if x[1] and x[1] != 0 and x[0] is not None and x[1] is not None else None)
        historical_growth['NetCF1YChgTTM'] = historical_growth[dependencies].apply(fun, axis=1)

        historical_growth = historical_growth.drop(dependencies, axis=1)
        factor_historical_growth = pd.merge(factor_historical_growth, historical_growth, how='outer', on='security_code')
        return factor_historical_growth

    @staticmethod
    def NetPftAP1YChgTTM(tp_historical_growth, factor_historical_growth, dependencies=['np_parent_company_owners',
                                                                                       'np_parent_company_owners_pre_year']):
        """
        :name: 归属母公司股东的净利润增长率
        :desc: （本期归母净利润（TTM）-上年同期归母净利润（TTM）/ABS（上年同期归母净利润（TTM））
        :unit:
        :view_dimension: 0.01
        """
        historical_growth = tp_historical_growth.loc[:, dependencies]

        # fun = lambda x: ((x[0] / x[1]) - 1 if x[1] and x[1] != 0 and x[0] is not None and x[1] is not None else None)
        fun = lambda x: ((x[0] - x[1]) / abs(x[1]) if x[1] and x[1] != 0 and x[0] is not None and x[1] is not None else None)
        historical_growth['NetPftAP1YChgTTM'] = historical_growth[dependencies].apply(fun, axis=1)

        historical_growth = historical_growth.drop(dependencies, axis=1)
        factor_historical_growth = pd.merge(factor_historical_growth, historical_growth, how='outer', on='security_code')
        return factor_historical_growth

    @staticmethod
    def NetPftAPNNRec1YChgTTM(tp_historical_growth, factor_historical_growth, dependencies=['ni_attr_p_cut',
                                                                                            'ni_attr_p_cut_pre']):
        """
        :name: 归属母公司股东的净利润（扣除非经常损益）同比增长
        :desc: 归属母公司股东的净利润(扣除非经常损益)TTM增长
        :unit:
        :view_dimension: 0.01
        """
        historical_growth = tp_historical_growth.loc[:, dependencies]

        # fun = lambda x: ((x[0] / x[1]) - 1 if x[1] and x[1] != 0 and x[0] is not None and x[1] is not None else None)
        fun = lambda x: ((x[0] - x[1]) / abs(x[1]) if x[1] and x[1] != 0 and x[0] is not None and x[1] is not None else None)
        historical_growth['NetPftAPNNRec1YChgTTM'] = historical_growth[dependencies].apply(fun, axis=1)

        historical_growth = historical_growth.drop(dependencies, axis=1)
        factor_historical_growth = pd.merge(factor_historical_growth, historical_growth, how='outer', on='security_code')
        return factor_historical_growth

    @staticmethod
    def NetPft1YChgTTM(tp_historical_growth, factor_historical_growth, dependencies=['net_profit',
                                                                                     'net_profit_pre_year']):
        """
        :name: 净利润增长率
        :desc: （本期净利润（TTM）-上年同期净利润（TTM））/ABS（上年同期净利润（TTM））
        :unit:
        :view_dimension: 0.01
        """
        historical_growth = tp_historical_growth.loc[:, dependencies]
        if len(historical_growth) <= 0:
            return
        # fun = lambda x: ((x[0] / x[1]) - 1 if x[1] and x[1] != 0 and x[0] is not None and x[1] is not None else None)
        fun = lambda x: ((x[0] - x[1]) / abs(x[1]) if x[1] and x[1] != 0 and x[0] is not None and x[1] is not None else None)
        historical_growth['NetPft1YChgTTM'] = historical_growth[dependencies].apply(fun, axis=1)

        historical_growth = historical_growth.drop(dependencies, axis=1)
        factor_historical_growth = pd.merge(factor_historical_growth, historical_growth, how='outer', on='security_code')
        return factor_historical_growth

    @staticmethod
    def OCF1YChgTTM(tp_historical_growth, factor_historical_growth, dependencies=['net_operate_cash_flow',
                                                                                  'net_operate_cash_flow_pre_year']):
        """
        :name: 经营活动产生的现金流量净额
        :desc: （本期CFO（TTM）-上年同期CFO（TTM））/ABS（上年同期CFO（TTM））
        :unit: 元
        :view_dimension: 10000
        """
        historical_growth = tp_historical_growth.loc[:, dependencies]
        if len(historical_growth) <= 0:
            return
        # fun = lambda x: ((x[0] / x[1]) - 1 if x[1] and x[1] != 0 and x[1] is not None and x[0] is not None else None)
        fun = lambda x: ((x[0] - x[1]) / abs(x[1]) if x[1] and x[1] != 0 and x[0] is not None and x[1] is not None else None)
        historical_growth['OCF1YChgTTM'] = historical_growth[dependencies].apply(fun, axis=1)

        historical_growth = historical_growth.drop(dependencies, axis=1)
        factor_historical_growth = pd.merge(factor_historical_growth, historical_growth, how='outer', on='security_code')
        return factor_historical_growth

    @staticmethod
    def OPft1YChgTTM(tp_historical_growth, factor_historical_growth, dependencies=['operating_profit',
                                                                                   'operating_profit_pre_year']):
        """
        :name: 营业利润增长率
        :desc:（本期CFI（TTM）-上年同期CFI（TTM）]/ABS（上年同期CFI（TTM））
        :unit:
        :view_dimension: 0.01
        """
        historical_growth = tp_historical_growth.loc[:, dependencies]
        if len(historical_growth) <= 0:
            return
        # fun = lambda x: ((x[0] / x[1]) - 1.0 if x[1] and x[1] != 0 and x[0] is not None and x[1] is not None else None)
        fun = lambda x: ((x[0] - x[1]) / abs(x[1]) if x[1] and x[1] != 0 and x[0] is not None and x[1] is not None else None)
        historical_growth['OPft1YChgTTM'] = historical_growth[dependencies].apply(fun, axis=1)

        historical_growth = historical_growth.drop(dependencies, axis=1)
        factor_historical_growth = pd.merge(factor_historical_growth, historical_growth, how='outer', on='security_code')
        return factor_historical_growth

    @staticmethod
    def ORev1YChgTTM(tp_historical_growth, factor_historical_growth, dependencies=['operating_revenue',
                                                                                   'operating_revenue_pre_year']):
        """
        :name: 营业收入增长率
        :desc: 营业收入增长率=（本期营业收入（TTM）-上年同期营业收入（TTM））/ABS（上年同期营业收入（TTM））
        :unit:
        :view_dimension: 0.01
        """
        historical_growth = tp_historical_growth.loc[:, dependencies]
        # fun = lambda x: ((x[0] / x[1]) - 1.0 if x[1] and x[1] != 0 and x[0] is not None and x[1] is not None else None)
        fun = lambda x: ((x[0] - x[1]) / abs(x[1]) if x[1] and x[1] != 0 and x[0] is not None and x[1] is not None else None)
        historical_growth['ORev1YChgTTM'] = historical_growth[dependencies].apply(fun, axis=1)

        historical_growth = historical_growth.drop(dependencies, axis=1)
        factor_historical_growth = pd.merge(factor_historical_growth, historical_growth, how='outer', on='security_code')
        return factor_historical_growth

    @staticmethod
    def StdUxpErn1YTTM(tp_historical_growth, factor_historical_growth, dependencies=['net_profit',
                                                                                     'net_profit_pre_year',
                                                                                     'net_profit_pre_year_2',
                                                                                     'net_profit_pre_year_3',
                                                                                     'net_profit_pre_year_4']):
        """
        :name: 未预期盈余
        :desc: (最近一年净利润-除去最近一年的过往净利润均值)/ 除去最近一年的过往净利润标准差
        :unit: 元
        :view_dimension: 10000
        """
        historical_growth = tp_historical_growth.loc[:, dependencies]
        if len(historical_growth) <= 0:
            return
        historical_growth['mean'] = historical_growth[dependencies].fillna(method='ffill').mean(axis=1)
        historical_growth['std'] = historical_growth[dependencies].fillna(method='ffill').std(axis=1)

        fun = lambda x: (x[0] - x[1]) / x[2] if x[2] !=0 and x[1] is not None and x[0] is not None and x[2] is not None else None

        historical_growth['StdUxpErn1YTTM'] = historical_growth[['net_profit', 'mean', 'std']].apply(fun, axis=1)

        # historical_growth = historical_growth.drop(columns=['net_profit', 'std', 'mean'], axis=1)
        historical_growth = historical_growth[['StdUxpErn1YTTM']]
        factor_historical_growth = pd.merge(factor_historical_growth, historical_growth, how='outer', on='security_code')

        return factor_historical_growth

    @staticmethod
    def StdUxpGrPft1YTTM(tp_historical_growth, factor_historical_growth, dependencies=['operating_revenue',
                                                                                       'operating_revenue_pre_year',
                                                                                       'operating_revenue_pre_year_2',
                                                                                       'operating_revenue_pre_year_3',
                                                                                       'operating_revenue_pre_year_4',
                                                                                       'operating_revenue_pre_year_5',
                                                                                       'operating_cost',
                                                                                       'operating_cost_pre_year',
                                                                                       'operating_cost_pre_year_2',
                                                                                       'operating_cost_pre_year_3',
                                                                                       'operating_cost_pre_year_4',
                                                                                       'operating_cost_pre_year_5']):
        """
        :name: 未预期毛利
        :desc: (最近一年毛利-除去最近一年的过往毛利均值)/ 除去最近一年的过往毛利标准差
        :unit: 元
        :view_dimension: 10000
        """
        historical_growth = tp_historical_growth.loc[:, dependencies]
        if len(historical_growth) <= 0:
            return
        historical_growth['gi_1'] = historical_growth['operating_revenue'] - historical_growth['operating_cost']
        historical_growth['gi_2'] = historical_growth['operating_revenue_pre_year_2'] - historical_growth[
            'operating_cost_pre_year_2']
        historical_growth['gi_3'] = historical_growth['operating_revenue_pre_year_3'] - historical_growth[
            'operating_cost_pre_year_3']
        historical_growth['gi_4'] = historical_growth['operating_revenue_pre_year_4'] - historical_growth[
            'operating_cost_pre_year_4']
        historical_growth['gi_5'] = historical_growth['operating_revenue_pre_year_5'] - historical_growth[
            'operating_cost_pre_year_5']

        historical_growth['mean'] = historical_growth[['gi_2', 'gi_3', 'gi_4', 'gi_5']].mean(axis=1)
        historical_growth['std'] = historical_growth[['gi_2', 'gi_3', 'gi_4', 'gi_5']].std(axis=1)

        fun = lambda x: ((x[0] - x[1]) / x[2] if x[2] != 0 and x[1] is not None and x[0] is not None and x[2] is not None else None)
        historical_growth['StdUxpGrPft1YTTM'] = historical_growth[['gi_1', 'mean', 'std']].apply(fun, axis=1)

        historical_growth = historical_growth[['StdUxpGrPft1YTTM']]
        factor_historical_growth = pd.merge(factor_historical_growth, historical_growth, how='outer', on='security_code')

        return factor_historical_growth

    @staticmethod
    def NetPft3YChgTTM(tp_historical_growth, factor_historical_growth, dependencies=['net_profit',
                                                                                     'net_profit_pre_year_3']):
        """
        :name: 净利润3年复合增长率
        :desc: (当期净利润/3年前净利润)^(1/3)-1
        :unit:
        :view_dimension: 0.01
        """
        historical_growth = tp_historical_growth.loc[:, dependencies]

        fun = lambda x: (pow((x[0] / x[1]), 1 / 3.0) - 1 if x[1] and x[1] != 0 and x[0] is not None and x[1] is not None else None)
        historical_growth['NetPft3YChgTTM'] = historical_growth[dependencies].apply(fun, axis=1)

        historical_growth = historical_growth.drop(columns=dependencies, axis=1)
        factor_historical_growth = pd.merge(factor_historical_growth, historical_growth, how='outer', on='security_code')
        return factor_historical_growth

    @staticmethod
    def NetPft5YChgTTM(tp_historical_growth, factor_historical_growth, dependencies=['net_profit',
                                                                                     'net_profit_pre_year_5']):
        """
        :name: 净利润5年复合增长率
        :desc: (当期净利润/5年前净利润)^(1/5)-1
        :unit:
        :view_dimension: 0.01
        """
        historical_growth = tp_historical_growth.loc[:, dependencies]

        if len(historical_growth) <= 0:
            return None
        fun = lambda x: (pow((x[0] / x[1]), 1 / 5.0) - 1 if x[1] and x[1] != 0 and x[0] is not None and x[1] is not None else None)
        historical_growth['NetPft5YChgTTM'] = historical_growth[dependencies].apply(fun, axis=1)

        historical_growth = historical_growth.drop(columns=dependencies, axis=1)
        factor_historical_growth = pd.merge(factor_historical_growth, historical_growth, how='outer', on='security_code')
        return factor_historical_growth

    @staticmethod
    def ORev3YChgTTM(tp_historical_growth, factor_historical_growth, dependencies=['operating_revenue',
                                                                                   'operating_revenue_pre_year_3']):
        """
        :name: 营业收入3年复合增长率
        :desc: (当期营业收入/3年前营业收入)^(1/3)-1
        :unit:
        :view_dimension: 0.01
        """
        historical_growth = tp_historical_growth.loc[:, dependencies]
        if len(historical_growth) <= 0:
            return None
        fun = lambda x: (pow((x[0] / x[1]), 1 / 3.0) - 1 if x[1] and x[1] != 0 and x[0] is not None and x[1] is not None else None)

        historical_growth['ORev3YChgTTM'] = historical_growth[dependencies].apply(fun, axis=1)

        historical_growth = historical_growth.drop(columns=dependencies, axis=1)
        factor_historical_growth = pd.merge(factor_historical_growth, historical_growth, how='outer', on='security_code')
        return factor_historical_growth

    @staticmethod
    def ORev5YChgTTM(tp_historical_growth, factor_historical_growth, dependencies=['operating_revenue',
                                                                                   'operating_revenue_pre_year_3']):
        """
        :name: 营业收入5年复合增长率
        :desc: (当期营业收入/5年前营业收入)^(1/5)-1
        :unit:
        :view_dimension: 0.01
        """
        historical_growth = tp_historical_growth.loc[:, dependencies]
        if len(historical_growth) <= 0:
            return None
        fun = lambda x: (pow((x[0] / x[1]), 1 / 5.0) - 1 if x[1] and x[1] != 0 and x[0] is not None and x[1] is not None else None)

        historical_growth['ORev5YChgTTM'] = historical_growth[dependencies].apply(fun, axis=1)

        historical_growth = historical_growth.drop(columns=dependencies, axis=1)
        factor_historical_growth = pd.merge(factor_historical_growth, historical_growth, how='outer', on='security_code')
        return factor_historical_growth
