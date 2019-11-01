#! /usr/bin/env python
# -*- coding: utf-8 -*-

"""
每股指标
@version: 0.1
@author: li
@file: factor_per_share_indicators.py
@time: 2019-02-12 10:02
"""

import sys, six
sys.path.append("../")
sys.path.append("../../")
sys.path.append("../../../")
import gc
import pandas as pd
import json
from pandas.io.json import json_normalize
from utilities.singleton import Singleton

# from basic_derivation import app
# from ultron.cluster.invoke.cache_data import cache_data


@six.add_metaclass(Singleton)
class FactorPerShareIndicators(object):
    """
    每股因子
    """
    def __init__(self):
        __str__ = 'factor_per_share_indicators'
        self.name = '财务指标'
        self.factor_type1 = '财务指标'
        self.factor_type2 = '每股指标'
        self.description = '财务指标的二级指标-每股因子'

    @staticmethod
    def CapticalSurplusPS(tp_share_indicators, factor_share_indicators, dependencies=['capital_reserve_fund', 'capitalization']):
        """
        :name: 每股资本公积金
        :desc: 资本公积（MRQ)/当日总股本
        :unit: 元
        :view_dimension: 1
        """
        share_indicators = tp_share_indicators.loc[:, dependencies]
        fun = lambda x: (x[0] / x[1] if x[1] and x[1] != 0 else None)
        share_indicators['CapticalSurplusPS'] = share_indicators[dependencies].apply(fun, axis=1)

        share_indicators = share_indicators.drop(columns=dependencies, axis=1)
        factor_share_indicators = pd.merge(factor_share_indicators, share_indicators, how='outer', on='security_code')
        return factor_share_indicators

    @staticmethod
    def CashEquPS(tp_share_indicators, factor_share_indicators, dependencies=['capitalization', 'cash_and_equivalents_at_end']):
        """
        :name: 每股现金及现金等价物余额
        :desc: 现金及现金等价物余额（MRQ）/当日总股本
        :unit: 元
        :view_dimension: 1
        """
        share_indicators = tp_share_indicators.loc[:, dependencies]
        fun = lambda x: (x[1] / x[0] if x[0] and x[0] != 0 else None)
        share_indicators['CashEquPS'] = share_indicators[dependencies].apply(fun, axis=1)

        share_indicators = share_indicators.drop(columns=dependencies, axis=1)
        factor_share_indicators = pd.merge(factor_share_indicators, share_indicators, how='outer', on='security_code')
        return factor_share_indicators

    # @staticmethod
    # def DivPS(tp_share_indicators, factor_share_indicators, dependencies=['dividend_receivable']):
    #     """
    #     缺每股股利
    #     :name: 每股股利(税前)
    #     :desc: 根据分红预案公告日，即中报或年报披露日期，披露的分红教据（每股股利税前（已宣告），若读公司不分红返回为空
    #     """
    #     share_indicators = tp_share_indicators.loc[:, dependencies]
    #     share_indicators = share_indicators.rename(columns={'dividend_receivable': 'DivPS'})
    #     # share_indicators = share_indicators.drop(columns=dependencies, axis=1)
    #     factor_share_indicators = pd.merge(factor_share_indicators, share_indicators, how='outer', on='security_code')
    #     return factor_share_indicators

    @staticmethod
    def EPS(tp_share_indicators, factor_share_indicators, dependencies=['basic_eps']):
        """
        :name: 基本每股收益
        :desc: 基本每股收益, 报表公布值
        :unit: 元
        :view_dimension: 1
        """
        share_indicators = tp_share_indicators.loc[:, dependencies]
        # print(share_indicators.head())
        share_indicators = share_indicators.rename(columns={'basic_eps': 'EPS'})
        # share_indicators = share_indicators.drop(columns=dependencies, axis=1)
        factor_share_indicators = pd.merge(factor_share_indicators, share_indicators,  how='outer', on='security_code')
        return factor_share_indicators

    @staticmethod
    def ShareholderFCFPS(tp_share_indicators, factor_share_indicators, dependencies=['shareholder_fcfps', 'capitalization']):
        """
        :name: 每股股东自由现金流量
        :desc: 股东自由现金流量FCFE（MRQ）/当日总股本
        :unit: 元
        :view_dimension: 1
        """
        share_indicators = tp_share_indicators.loc[:, dependencies]
        fun = lambda x: (x[0] / x[1] if x[1] and x[1] != 0 else None)
        share_indicators['ShareholderFCFPS'] = share_indicators[dependencies].apply(fun, axis=1)

        share_indicators = share_indicators.drop(columns=dependencies, axis=1)
        factor_share_indicators = pd.merge(factor_share_indicators, share_indicators, how='outer', on='security_code')
        # factor_share_indicators['ShareholderFCFPS'] = share_indicators['ShareholderFCFPS']
        return factor_share_indicators

    @staticmethod
    def EnterpriseFCFPS(tp_share_indicators, factor_share_indicators, dependencies=['enterprise_fcfps', 'capitalization']):
        """

        :name: 每股企业自由现金流量
        :desc: 企业自由现金流量（MRQ）/当日总股本
        :unit: 元
        :view_dimension: 1
        """
        share_indicators = tp_share_indicators.loc[:, dependencies]
        fun = lambda x: (x[0] / x[1] if x[1] and x[1] != 0 else None)
        share_indicators['EnterpriseFCFPS'] = share_indicators[dependencies].apply(fun, axis=1)
        share_indicators = share_indicators.drop(columns=dependencies, axis=1)
        factor_share_indicators = pd.merge(factor_share_indicators, share_indicators, how='outer', on='security_code')
        # factor_share_indicators['EnterpriseFCFPS'] = share_indicators['EnterpriseFCFPS']

        return factor_share_indicators

    @staticmethod
    def NetAssetPS(tp_share_indicators, factor_share_indicators, dependencies=['total_owner_equities', 'capitalization']):
        """
        :name: 每股净资产
        :desc: 归属母公司所有者权益合计（MRQ)/当日总股本
        :unit: 元
        :view_dimension: 1
        """
        share_indicators = tp_share_indicators.loc[:, dependencies]
        fun = lambda x: (x[0] / x[1] if x[1] and x[1] != 0 else None)
        share_indicators['NetAssetPS'] = share_indicators[dependencies].apply(fun, axis=1)

        share_indicators = share_indicators.drop(columns=dependencies, axis=1)
        factor_share_indicators = pd.merge(factor_share_indicators, share_indicators, how='outer', on='security_code')
        # factor_share_indicators['NetAssetPS'] = share_indicators['NetAssetPS']

        return factor_share_indicators

    @staticmethod
    def OptRevPS(tp_share_indicators, factor_share_indicators, dependencies=['operating_revenue', 'capitalization']):
        """
        :name: 每股营业收入
        :desc: 营业收入（MRQ)/当日总股本
        :unit: 元
        :view_dimension: 1
        """
        share_indicators = tp_share_indicators.loc[:, dependencies]
        fun = lambda x: (x[0] / x[1] if x[1] and x[1] != 0 else None)
        share_indicators['OptRevPS'] = share_indicators[dependencies].apply(fun, axis=1)

        share_indicators = share_indicators.drop(columns=dependencies, axis=1)
        factor_share_indicators = pd.merge(factor_share_indicators, share_indicators, how='outer', on='security_code')
        # factor_share_indicators['OptRevPS'] = share_indicators['OptRevPS']

        return factor_share_indicators

    @staticmethod
    def SurplusReservePS(tp_share_indicators, factor_share_indicators, dependencies=['surplus_reserve_fund', 'capitalization']):
        """
        :name: 每股盈余公积金
        :desc: 盈余公积金（MRQ)/当日总股本
        :unit: 元
        :view_dimension: 1
        """

        share_indicators = tp_share_indicators.loc[:, dependencies]
        fun = lambda x: (x[0] / x[1] if x[1] and x[1] != 0 else None)
        share_indicators['SurplusReservePS'] = share_indicators[dependencies].apply(fun, axis=1)

        share_indicators = share_indicators.drop(columns=dependencies, axis=1)
        factor_share_indicators = pd.merge(factor_share_indicators, share_indicators, how='outer', on='security_code')
        # factor_share_indicators['SurplusReservePS'] = share_indicators['SurplusReservePS']

        return factor_share_indicators

    @staticmethod
    def OptProfitPS(tp_share_indicators, factor_share_indicators, dependencies=['operating_profit', 'capitalization']):
        """
        :name: 每股营业利润
        :desc: 营业利润（MRQ)/当日总股本
        :unit: 元
        :view_dimension: 1
        """
        share_indicators = tp_share_indicators.loc[:, dependencies]
        fun = lambda x: (x[0] / x[1] if x[1] and x[1] != 0 else None)
        share_indicators['OptProfitPS'] = share_indicators[dependencies].apply(fun, axis=1)

        share_indicators = share_indicators.drop(columns=dependencies, axis=1)
        factor_share_indicators = pd.merge(factor_share_indicators, share_indicators, how='outer', on='security_code')
        # factor_share_indicators['OptProfitPS'] = share_indicators['OptProfitPS']

        return factor_share_indicators

    @staticmethod
    def UndividedProfitPS(tp_share_indicators, factor_share_indicators, dependencies=['retained_profit', 'capitalization']):
        """
        :name: 每股未分配利润
        :desc: 未分配利润（MRQ)/当日总股本
        :unit: 元
        :view_dimension: 1
        """
        share_indicators = tp_share_indicators.loc[:, dependencies]
        fun = lambda x: (x[0] / x[1] if x[1] and x[1] != 0 else None)
        share_indicators['UndividedProfitPS'] = share_indicators[dependencies].apply(fun, axis=1)

        share_indicators = share_indicators.drop(columns=dependencies, axis=1)
        factor_share_indicators = pd.merge(factor_share_indicators, share_indicators, how='outer', on='security_code')
        # factor_share_indicators['UndividedProfitPS'] = share_indicators['UndividedProfitPS']

        return factor_share_indicators

    @staticmethod
    def RetainedEarningsPS(tp_share_indicators, factor_share_indicators, dependencies=['SurplusReservePS', 'UndividedProfitPS']):
        """
        :name: 每股留存收益
        :desc: 留存收益（MRQ)/当日总股本
        :unit: 元
        :view_dimension: 1
        """
        share_indicators = tp_share_indicators.loc[:, dependencies]
        share_indicators['RetainedEarningsPS'] = share_indicators['UndividedProfitPS'] + share_indicators[
            'SurplusReservePS']

        share_indicators = share_indicators.drop(columns=dependencies, axis=1)
        factor_share_indicators = pd.merge(factor_share_indicators, share_indicators, how='outer', on='security_code')
        # factor_share_indicators['RetainedEarningsPS'] = share_indicators['RetainedEarningsPS']

        return factor_share_indicators

    @staticmethod
    def TotalRevPS(tp_share_indicators, factor_share_indicators, dependencies=['total_operating_revenue', 'capitalization']):
        """
        :name: 每股营业收入
        :desc: 营业收入（MRQ)/当日总股本
        :unit: 元
        :view_dimension: 1
        """
        share_indicators = tp_share_indicators.loc[:, dependencies]
        fun = lambda x: (x[0] / x[1] if x[1] and x[1] != 0 else None)
        share_indicators['TotalRevPS'] = share_indicators[dependencies].apply(fun, axis=1)

        share_indicators = share_indicators.drop(columns=dependencies, axis=1)
        factor_share_indicators = pd.merge(factor_share_indicators, share_indicators, how='outer', on='security_code')
        # factor_share_indicators['TotalRevPS'] = share_indicators['TotalRevPS']
        return factor_share_indicators

    @staticmethod
    def CFPSTTM(tp_share_indicators, factor_share_indicators, dependencies=['cash_equivalent_increase_ttm', 'capitalization']):
        """
        :name:每股现金流量净额(TTM)
        :desc: 现金及现金等价物净增加额（TTM）/当日总股本
        :unit: 元
        :view_dimension: 1
        """
        share_indicators = tp_share_indicators.loc[:, dependencies]
        fun = lambda x: (x[0] / x[1] if x[1] and x[1] != 0 else None)
        share_indicators['CFPSTTM'] = share_indicators[dependencies].apply(fun, axis=1)

        share_indicators = share_indicators.drop(columns=dependencies, axis=1)
        factor_share_indicators = pd.merge(factor_share_indicators, share_indicators, how='outer', on='security_code')
        # factor_share_indicators['CFPSTTM'] = share_indicators['CFPSTTM']

        return factor_share_indicators

    @staticmethod
    def DilutedEPSTTM(tp_share_indicators, factor_share_indicators, dependencies=['diluted_eps']):
        """
        :name: 稀释每股收益(TTM)
        :desc: 报表公布值
        :unit: 元
        :view_dimension: 1
        """
        share_indicators = tp_share_indicators.loc[:, dependencies]
        share_indicators = share_indicators.rename(columns={'diluted_eps': 'DilutedEPSTTM'})
        # share_indicators = share_indicators.drop(columns=['diluted_eps'], axis=1)
        # share_indicators = share_indicators[['security_code', 'DilutedEPSTTM']]
        factor_share_indicators = pd.merge(factor_share_indicators, share_indicators, how='outer', on='security_code')
        # factor_share_indicators['DilutedEPSTTM'] = share_indicators['DilutedEPSTTM']

        return factor_share_indicators

    @staticmethod
    def EPSTTM(tp_share_indicators, factor_share_indicators, dependencies=['np_parent_company_owners_ttm', 'capitalization']):
        """
        :name: 每股收益（TTM）
        :desc: 每股收益（TTM）
        :unit: 元
        :view_dimension: 1
        """
        share_indicators = tp_share_indicators.loc[:, dependencies]
        fun = lambda x: (x[0] / x[1] if x[1] and x[1] != 0 else 0)
        share_indicators['EPSTTM'] = share_indicators[dependencies].apply(fun, axis=1)

        share_indicators = share_indicators.drop(columns=dependencies, axis=1)
        factor_share_indicators = pd.merge(factor_share_indicators, share_indicators, how='outer', on='security_code')
        # factor_share_indicators['EPSTTM'] = share_indicators['EPSTTM']

        return factor_share_indicators

    @staticmethod
    def OptCFPSTTM(tp_share_indicators, factor_share_indicators, dependencies=['net_operate_cash_flow_ttm', 'capitalization']):
        """
        :name: 每股经营活动产生的现金流量净额(TTM)
        :desc: "经营活动产生的现金流量净额（TTM）/当日总股本
        :unit: 元
        :view_dimension: 1
        """

        share_indicators = tp_share_indicators.loc[:, dependencies]
        fun = lambda x: (x[0] / x[1] if x[1] and x[1] != 0 else None)
        share_indicators['OptCFPSTTM'] = share_indicators[dependencies].apply(fun, axis=1)

        share_indicators = share_indicators.drop(columns=dependencies, axis=1)
        factor_share_indicators = pd.merge(factor_share_indicators, share_indicators, how='outer', on='security_code')
        # factor_share_indicators['OptCFPSTTM'] = share_indicators['OptCFPSTTM']

        return factor_share_indicators

    @staticmethod
    def OptProfitPSTTM(tp_share_indicators, factor_share_indicators, dependencies=['operating_profit_ttm', 'capitalization']):
        """
        :name: 每股营业利润(TTM)
        :desc:"营业利润（TTM）/当日总股本
        :unit: 元
        :view_dimension: 1
        """

        share_indicators = tp_share_indicators.loc[:, dependencies]
        fun = lambda x: (x[0] / x[1] if x[1] and x[1] != 0 else None)
        share_indicators['OptProfitPSTTM'] = share_indicators[dependencies].apply(fun, axis=1)

        share_indicators = share_indicators.drop(columns=dependencies, axis=1)
        factor_share_indicators = pd.merge(factor_share_indicators, share_indicators, how='outer', on='security_code')
        # factor_share_indicators['OptProfitPSTTM'] = share_indicators['OptProfitPSTTM']

        return factor_share_indicators

    @staticmethod
    def OptRevPSTTM(tp_share_indicators, factor_share_indicators, dependencies=['operating_revenue_ttm', 'capitalization']):
        """
        :name:每股营业收入TTM
        :desc: 营业收入（TTM)/当日总股本
        :unit: 元
        :view_dimension: 1
        """

        share_indicators = tp_share_indicators.loc[:, dependencies]
        fun = lambda x: (x[0] / x[1] if x[1] and x[1] != 0 else None)
        share_indicators['OptRevPSTTM'] = share_indicators[dependencies].apply(fun, axis=1)

        share_indicators = share_indicators.drop(columns=dependencies, axis=1)
        factor_share_indicators = pd.merge(factor_share_indicators, share_indicators, on='security_code')
        # factor_share_indicators['OptRevPSTTM'] = share_indicators['OptRevPSTTM']

        return factor_share_indicators

    @staticmethod
    def TotalRevPSTTM(tp_share_indicators, factor_share_indicators, dependencies=['total_operating_revenue_ttm', 'capitalization']):
        """
        :name: 每股营业总收入(TTM)
        :desc: 营业总收入(TTM)/当日总股本
        :unit: 元
        :view_dimension: 1
        """

        share_indicators = tp_share_indicators.loc[:, dependencies]
        fun = lambda x: (x[0] / x[1] if x[1] and x[1] != 0 else None)
        share_indicators['TotalRevPSTTM'] = share_indicators[dependencies].apply(fun, axis=1)

        share_indicators = share_indicators.drop(columns=dependencies, axis=1)
        factor_share_indicators = pd.merge(factor_share_indicators, share_indicators, on='security_code')
        # factor_share_indicators['TotalRevPSTTM'] = share_indicators['TotalRevPSTTM']

        return factor_share_indicators
