#!/usr/bin/env python
# coding=utf-8
"""
@version: 0.1
@author: li
@file: factor_revenue_quality.py
@time: 2019-01-28 11:33
"""
import gc, six
import sys
sys.path.append("../")
sys.path.append("../../")
sys.path.append("../../../")
import numpy as np
import pandas as pd
import json
from pandas.io.json import json_normalize
from utilities.calc_tools import CalcTools
from utilities.singleton import Singleton

# from basic_derivation import app
# from ultron.cluster.invoke.cache_data import cache_data
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


@six.add_metaclass(Singleton)
class FactorRevenueQuality(object):
    """
    收益质量
    """
    def __init__(self):
        __str__ = 'factor_revenue_quality'
        self.name = '财务指标'
        self.factor_type1 = '财务指标'
        self.factor_type2 = '收益质量'
        self.description = '财务指标的二级指标， 收益质量'

    @staticmethod
    def NetNonOIToTP(tp_revenue_quanlity, revenue_quality, dependencies=['total_profit', 'non_operating_revenue', 'non_operating_expense']):
        """
        :name: 营业外收支净额/利润总额
        :desc: 营业外收支净额/利润总额
        :unit:
        :view_dimension: 0.01
        """
        earning = tp_revenue_quanlity.loc[:, dependencies]
        earning['NetNonOIToTP'] = np.where(
            CalcTools.is_zero(earning.total_profit.values), 0,
            (earning.non_operating_revenue.values +
             earning.non_operating_expense.values)
            / earning.total_profit.values
            )
        earning = earning.drop(dependencies, axis=1)
        revenue_quality = pd.merge(revenue_quality, earning, how='outer', on="security_code")
        return revenue_quality

    @staticmethod
    def NetNonOIToTPTTM(ttm_revenue_quanlity, revenue_quality, dependencies=['total_profit', 'non_operating_revenue', 'non_operating_expense']):
        """
        :name:营业外收支净额(TTM)/利润总额(TTM)
        :desc: 营业外收支净额（TTM）/利润总额（TTM)
        :unit:
        :view_dimension: 0.01
        """
        earning = ttm_revenue_quanlity.loc[:, dependencies]
        earning['NetNonOIToTPTTM'] = np.where(
            CalcTools.is_zero(earning.total_profit.values), 0,
            (earning.non_operating_revenue.values +
             earning.non_operating_expense.values)
            / earning.total_profit.values
            )
        earning = earning.drop(dependencies, axis=1)
        revenue_quality = pd.merge(revenue_quality, earning, how='outer', on="security_code")
        return revenue_quality

    @staticmethod
    def OperatingNIToTPTTM(ttm_revenue_quanlity, revenue_quality, dependencies=['total_operating_revenue', 'total_operating_cost', 'total_profit']):
        """
        :name: 经营活动净收益/利润总额(TTM)
        :desc: 经营活动净收益(TTM)/利润总额（TTM)*100%（注，对于非金融企业 经营活动净收益=营业总收入-营业总成本； 对于金融企业 经营活动净收益=营业收入-公允价值变动损益-投资收益-汇兑损益-营业支出 此处以非金融企业的方式计算）
        :unit:
        :view_dimension: 0.01
        """
        earning = ttm_revenue_quanlity.loc[:, dependencies]
        earning['OperatingNIToTPTTM'] = np.where(
            CalcTools.is_zero(earning.total_profit.values), 0,
            (earning.total_operating_revenue.values -
             earning.total_operating_cost.values)
            / earning.total_profit.values)
        earning = earning.drop(dependencies, axis=1)
        revenue_quality = pd.merge(revenue_quality, earning, how='outer', on="security_code")
        return revenue_quality

    @staticmethod
    def OperatingNIToTP(tp_revenue_quanlity, revenue_quality, dependencies=['total_operating_revenue', 'total_operating_cost', 'total_profit']):
        """
        :name: 经营活动净收益/利润总额
        :desc:（注，对于非金融企业 经营活动净收益=营业总收入-营业总成本； 对于金融企业 经营活动净收益=营业收入-公允价值变动损益-投资收益-汇兑损益-营业支出 此处以非金融企业的方式计算）
        :unit:
        :view_dimension: 0.01
        """
        earning = tp_revenue_quanlity.loc[:, dependencies]
        earning['OperatingNIToTP'] = np.where(
            CalcTools.is_zero(earning.total_profit.values), 0,
            (earning.total_operating_revenue.values -
             earning.total_operating_cost.values)
            / earning.total_profit.values)
        earning = earning.drop(dependencies, axis=1)
        revenue_quality = pd.merge(revenue_quality, earning, how='outer', on="security_code")
        return revenue_quality

    @staticmethod
    def OptCFToCurrLiabilityTTM(ttm_revenue_quanlity, revenue_quality, dependencies=['net_operate_cash_flow', 'total_current_liability']):
        """
        :name: 经营活动产生的现金流量净额（TTM）/流动负债（TTM）
        :desc: 经营活动产生的现金流量净额（TTM）/流动负债（TTM）
        :unit:
        :view_dimension: 0.01
        """
        cash_flow = ttm_revenue_quanlity.loc[:, dependencies]
        cash_flow['OptCFToCurrLiabilityTTM'] = np.where(
            CalcTools.is_zero(cash_flow.total_current_liability.values), 0,
            cash_flow.net_operate_cash_flow.values / cash_flow.total_current_liability.values)
        cash_flow = cash_flow.drop(dependencies, axis=1)
        revenue_quality = pd.merge(revenue_quality, cash_flow, how='outer', on="security_code")
        return revenue_quality

    @staticmethod
    def NVALCHGITOTP(ttm_revenue_quanlity, revenue_quality, dependencies=['NVALCHGITOTP']):
        """
        :name: 价值变动净收益/利润总额(TTM)
        :desc: 价值变动净收益（TTM)/利润总额（TTM)
        :unit:
        :view_dimension: 0.01
        """
        historical_value = ttm_revenue_quanlity.loc[:, dependencies]
        revenue_quality = pd.merge(revenue_quality, historical_value, how='outer', on='security_code')
        return revenue_quality

    @staticmethod
    def OPToTPTTM(ttm_revenue_quanlity, revenue_quality,
                  dependencies=['operating_profit', 'total_profit']):
        """
        :name: 营业利润/利润总额(TTM)
        :desc: 营业利润（TTM)/利润总额（TTM）
        :unit:
        :view_dimension: 0.01
        """
        historical_value = ttm_revenue_quanlity.loc[:, dependencies]
        func = lambda x: x[0] / x[1] if x[1] is not None and x[1] != 0 else None

        historical_value['OPToTPTTM'] = historical_value.apply(func, axis=1)
        historical_value = historical_value.drop(dependencies, axis=1)
        revenue_quality = pd.merge(revenue_quality, historical_value, how='outer', on='security_code')
        return revenue_quality

    @staticmethod
    def PriceToRevRatioTTM(ttm_revenue_quanlity, revenue_quality,
                           dependencies=['net_profit', 'market_cap']):
        """
        :name: 收益市值比(TTM)
        :desc: 营业利润（TTM)/利润总额（TTM）
        :unit:
        :view_dimension: 0.01
        """
        historical_value = ttm_revenue_quanlity.loc[:, dependencies]

        func = lambda x: x[0] / x[1] if x[1] is not None and x[1] != 0 else None

        historical_value['PriceToRevRatioTTM'] = historical_value[dependencies].apply(func, axis=1)
        historical_value = historical_value.drop(dependencies, axis=1)
        revenue_quality = pd.merge(revenue_quality, historical_value, how='outer', on='security_code')
        return revenue_quality

    @staticmethod
    def PftMarginTTM(ttm_revenue_quanlity, revenue_quality,
                     dependencies=['total_profit', 'operating_revenue']):
        """
        :name: 利润率(TTM)
        :desc: 利润总额(TTM)/营业收入（TTM)
        :unit:
        :view_dimension: 0.01
        """
        historical_value = ttm_revenue_quanlity.loc[:, dependencies]

        func = lambda x: x[0] / x[1] if x[1] is not None and x[1] != 0 else None

        historical_value['PftMarginTTM'] = historical_value[dependencies].apply(func, axis=1)
        historical_value = historical_value.drop(dependencies, axis=1)
        revenue_quality = pd.merge(revenue_quality, historical_value, how='outer', on='security_code')
        return revenue_quality

    @staticmethod
    def PriceToRevRatioAvg5YTTM(ttm_revenue_quanlity, revenue_quality, dependencies=['net_profit_5', 'circulating_market_cap_5', 'market_cap_5']):
        """
        :name: 5年平均收益市值比(TTM)
        :desc: AVG(净利润/当日总市值，5年）限定只计算过去五年的年报
        :unit:
        :view_dimension: 0.01
        """
        historical_value = ttm_revenue_quanlity.loc[:, dependencies]

        fun = lambda x: x[0] / x[1] if x[1] is not None and x[1] != 0 else (x[0] / x[2] if x[2] is not None and x[2] !=0 else None)
        historical_value['PriceToRevRatioAvg5YTTM'] = historical_value[dependencies].apply(fun, axis=1)
        historical_value = historical_value.drop(columns=dependencies, axis=1)
        revenue_quality = pd.merge(revenue_quality, historical_value, how='outer', on="security_code")
        return revenue_quality
