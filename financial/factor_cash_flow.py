#! /usr/bin/env python
# -*- coding: utf-8 -*-

"""
@version:
@author: li
@file: factor_cash_flow.py
@time: 2019-05-30
"""

import gc, six
import json
import numpy as np
import pandas as pd

from utilities.calc_tools import CalcTools
from utilities.singleton import Singleton

# from basic_derivation import app
# from ultron.cluster.invoke.cache_data import cache_data
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


@six.add_metaclass(Singleton)
class FactorCashFlow(object):
    """
    现金流量
    """
    def __init__(self):
        __str__ = 'factor_cash_flow'
        self.name = '财务指标'
        self.factor_type1 = '财务指标'
        self.factor_type2 = '现金流量'
        self.description = '财务指标的二级指标-现金流量'

    @staticmethod
    def CashOfSales(tp_cash_flow, factor_cash_flow, dependencies=['net_operate_cash_flow', 'operating_revenue']):
        """
        :name: 经验活动产生的现金流量净额/营业收入
        :desc: 经营活动产生的现金流量净额/营业收入(MRQ)
        :unit:
        :view_dimension: 0.01
        """
        cash_flow = tp_cash_flow.loc[:, dependencies]
        cash_flow['CashOfSales'] = np.where(CalcTools.is_zero(cash_flow.operating_revenue.values),
                                            0,
                                            cash_flow.net_operate_cash_flow.values / cash_flow.operating_revenue.values)
        cash_flow = cash_flow.drop(dependencies, axis=1)
        factor_cash_flow = pd.merge(factor_cash_flow, cash_flow, how='outer', on="security_code")
        # factor_cash_flow['CashOfSales'] = cash_flow['CashOfSales']
        return factor_cash_flow

    @staticmethod
    def NOCFToOpt(tp_cash_flow, factor_cash_flow, dependencies=['net_operate_cash_flow', 'total_operating_revenue', 'total_operating_cost']):
        """
        :name: 经营活动产生的现金流量净额/(营业总收入-营业总成本)
        :desc: 经营活动产生的现金流量净额/(营业总收入-营业总成本)
        :unit:
        :view_dimension: 0.01
        """
        cash_flow = tp_cash_flow.loc[:, dependencies]
        cash_flow['NOCFToOpt'] = np.where(
            CalcTools.is_zero((cash_flow.total_operating_revenue.values - cash_flow.total_operating_cost.values)), 0,
            cash_flow.net_operate_cash_flow.values / (
                    cash_flow.total_operating_revenue.values - cash_flow.total_operating_cost.values))
        cash_flow = cash_flow.drop(dependencies, axis=1)
        factor_cash_flow = pd.merge(factor_cash_flow, cash_flow, how='outer', on="security_code")
        # factor_cash_flow['NOCFToOpt'] = cash_flow['NOCFToOpt']
        return factor_cash_flow

    @staticmethod
    def SalesServCashToOR(tp_cash_flow, factor_cash_flow, dependencies=['goods_sale_and_service_render_cash', 'operating_revenue']):
        """
        :name: 销售商品和提供劳务收到的现金/营业收入
        :desc: 销售商品和提供劳务收到的现金/营业收入
        :unit:
        :view_dimension: 0.01
        """
        cash_flow = tp_cash_flow.loc[:, dependencies]
        cash_flow['SalesServCashToOR'] = np.where(CalcTools.is_zero(cash_flow.operating_revenue.values),
                                                  0,
                                                  cash_flow.goods_sale_and_service_render_cash.values / cash_flow.operating_revenue.values)
        cash_flow = cash_flow.drop(dependencies, axis=1)
        factor_cash_flow = pd.merge(factor_cash_flow, cash_flow, how='outer', on="security_code")
        # factor_cash_flow['SalesServCashToOR'] = cash_flow['SalesServCashToOR']
        return factor_cash_flow

    @staticmethod
    def OptOnReToAssetTTM(ttm_cash_flow, factor_cash_flow, dependencies=['net_operate_cash_flow', 'net_profit', 'total_assets']):
        """
        :name:(经营活动产生的金流量净额(TTM)-净利润(TTM)) /总资产(TTM)
        :desc:(经营活动产生的金流量净额(TTM) - 净利润(TTM)) /总资产(TTM)
        :unit:
        :view_dimension: 0.01
        """
        cash_flow = ttm_cash_flow.loc[:, dependencies]
        cash_flow['OptOnReToAssetTTM'] = np.where(CalcTools.is_zero(cash_flow.total_assets.values), 0,
                                                  (cash_flow.net_operate_cash_flow.values - cash_flow.net_profit.values)
                                                  / cash_flow.total_assets.values)
        cash_flow = cash_flow.drop(dependencies, axis=1)
        factor_cash_flow = pd.merge(factor_cash_flow, cash_flow, how='outer', on="security_code")
        # factor_cash_flow['OptOnReToAssetTTM'] = cash_flow['OptOnReToAssetTTM']
        return factor_cash_flow

    @staticmethod
    def NetProCashCoverTTM(ttm_cash_flow, factor_cash_flow, dependencies=['net_operate_cash_flow', 'np_parent_company_owners']):
        """
        :name: 经营活动产生的现金流量净额(TTM)/归属于母公司所有者的净利润(TTM)
        :desc: 经营活动产生的现金流量净额(TTM)/归属于母公司所有者的净利润(TTM)
        :unit:
        :view_dimension: 0.01
        """
        cash_flow = ttm_cash_flow.loc[:, dependencies]
        cash_flow['NetProCashCoverTTM'] = np.where(
            CalcTools.is_zero(cash_flow.np_parent_company_owners.values), 0,
            cash_flow.net_operate_cash_flow.values / cash_flow.np_parent_company_owners.values)
        cash_flow = cash_flow.drop(dependencies, axis=1)
        factor_cash_flow = pd.merge(factor_cash_flow, cash_flow, how='outer', on="security_code")
        # factor_cash_flow['NetProCashCoverTTM'] = cash_flow['NetProCashCoverTTM']
        return factor_cash_flow

    @staticmethod
    def OptToEnterpriseTTM(ttm_cash_flow, factor_cash_flow, dependencies=['net_operate_cash_flow', 'longterm_loan', 'shortterm_loan', 'market_cap', 'cash_and_equivalents_at_end']):
        """
        :name: 经营活动产生的现金流量净额(TTM)/企业价值(TTM)
        :desc: 经营活动产生的现金流量净额(TTM)/(长期借款(TTM)+ 短期借款(TTM)+ 总市值 - 期末现金及现金等价物(TTM)
        :unit:
        :view_dimension: 0.01
        """
        cash_flow = ttm_cash_flow.loc[:, dependencies]
        cash_flow['OptToEnterpriseTTM'] = np.where(CalcTools.is_zero(
            cash_flow.longterm_loan.values + cash_flow.shortterm_loan.values + \
            cash_flow.market_cap.values - cash_flow.cash_and_equivalents_at_end.values), 0,
            cash_flow.net_operate_cash_flow.values / (cash_flow.longterm_loan.values + cash_flow.shortterm_loan.values + \
                                                      cash_flow.market_cap.values - cash_flow.cash_and_equivalents_at_end.values))
        cash_flow = cash_flow.drop(dependencies, axis=1)
        factor_cash_flow = pd.merge(factor_cash_flow, cash_flow, how='outer', on="security_code")
        return factor_cash_flow

    @staticmethod
    def OptCFToRevTTM(ttm_cash_flow, factor_cash_flow, dependencies=['net_operate_cash_flow', 'operating_revenue']):
        """
        :name: 经营活动产生的现金流量净额(TTM)/营业收入(TTM)
        :desc: 经营活动产生的现金流量净额(TTM)/营业收入(TTM)
        :unit:
        :view_dimension: 0.01
        """
        cash_flow = ttm_cash_flow.loc[:, dependencies]
        cash_flow['OptCFToRevTTM'] = np.where(
            CalcTools.is_zero(cash_flow.operating_revenue.values), 0,
            cash_flow.net_operate_cash_flow.values / cash_flow.operating_revenue.values)
        cash_flow = cash_flow.drop(dependencies, axis=1)
        factor_cash_flow = pd.merge(factor_cash_flow, cash_flow, how='outer', on="security_code")
        # factor_cash_flow['OptCFToRevTTM'] = cash_flow['OptCFToRevTTM']
        return factor_cash_flow

    @staticmethod
    def OptToAssertTTM(ttm_cash_flow, factor_cash_flow, dependencies=['net_operate_cash_flow', 'total_assets']):
        """
        :name: 经营活动产生的现金流量净额(TTM)/总资产(TTM)
        :desc: 经营活动产生的现金流量净额(TTM)/总资产(TTM)
        :unit:
        :view_dimension: 0.01
        """
        cash_flow = ttm_cash_flow.loc[:, dependencies]
        cash_flow['OptToAssertTTM'] = np.where(CalcTools.is_zero(cash_flow.total_assets.values),
                                               0,
                                               cash_flow.net_operate_cash_flow.values / cash_flow.total_assets.values)
        cash_flow = cash_flow.drop(dependencies, axis=1)
        factor_cash_flow = pd.merge(factor_cash_flow, cash_flow, how='outer', on="security_code")
        # factor_cash_flow['OptToAssertTTM'] = cash_flow['OptToAssertTTM']
        return factor_cash_flow

    @staticmethod
    def SaleServCashToOptReTTM(ttm_cash_flow, factor_cash_flow, dependencies=['goods_sale_and_service_render_cash',
                                                                              'operating_revenue']):
        """
        :name: 销售商品和提供劳务收到的现金(TTM)/营业收入(TTM)
        :desc: 销售商品提供劳务收到的现金(TTM)/营业收入(TTM)
        :unit:
        :view_dimension: 0.01
        """
        cash_flow = ttm_cash_flow.loc[:, dependencies]
        cash_flow['SaleServCashToOptReTTM'] = np.where(
            CalcTools.is_zero(cash_flow.operating_revenue.values), 0,
            cash_flow.goods_sale_and_service_render_cash.values / cash_flow.operating_revenue.values)
        cash_flow = cash_flow.drop(dependencies, axis=1)
        factor_cash_flow = pd.merge(factor_cash_flow, cash_flow, how='outer', on="security_code")
        return factor_cash_flow

    @staticmethod
    def NOCFTOOPftTTM(ttm_cash_flow, factor_cash_flow, dependencies=['net_operate_cash_flow', 'operating_profit']):
        """
        :name: 经营活动产生的现金流量净额/营业利润(TTM)
        :desc: 经营活动产生的现金流量净额(TTM)/营业利润(TTM)
        :unit:
        :view_dimension: 0.01
        """
        cash_flow = ttm_cash_flow.loc[:, dependencies]

        func = lambda x: x[0] / x[1] if x[1] is not None and x[1] != 0 else None
        cash_flow['NOCFTOOPftTTM'] = cash_flow[dependencies].apply(func, axis=1)
        cash_flow = cash_flow.drop(dependencies, axis=1)
        factor_cash_flow = pd.merge(factor_cash_flow, cash_flow, how='outer', on="security_code")
        return factor_cash_flow

    @staticmethod
    def OptCFToNITTM(ttm_cash_flow, factor_cash_flow, dependencies=['net_operate_cash_flow',
                                                                    'total_operating_revenue',
                                                                    'total_operating_cost']):
        """
        :name: 经营活动产生的现金流量净额(TTM)/经营活动净收益(TTM)
        :desc: 经营活动产生的现金流量净额(TTM)/经营活动净收益(TTM)
        :unit:
        :view_dimension: 0.01
        """
        cash_flow = ttm_cash_flow.loc[:, dependencies]

        func = lambda x: x[0] / (x[1] - x[2]) if x[1] is not None and x[2] is not None and (x[1] - x[2]) != 0 else None
        cash_flow['OptCFToNITTM'] = cash_flow[dependencies].apply(func, axis=1)
        cash_flow = cash_flow.drop(dependencies, axis=1)
        factor_cash_flow = pd.merge(factor_cash_flow, cash_flow, how='outer', on="security_code")
        return factor_cash_flow


    @staticmethod
    def _ctop(tp_historical_value, factor_historical_value, dependencies=['pcd', 'sbd', 'circulating_market_cap', 'market_cap']):
        """
        现金流市值比 = 每股派现 * 分红前总股本/总市值
        :name:
        :desc:
        :unit:
        :view_dimension: 0.01
        """

        historical_value = tp_historical_value.loc[:, dependencies]

        fun = lambda x: x[0] * x[1] / x[2] if x[2] is not None and x[2] != 0 else (x[0] * x[1] / x[3] if x[3] is not None and x[3] != 0 else None)

        historical_value['historical_value_ctop_latest'] = historical_value[dependencies].apply(fun, axis=1)

        historical_value = historical_value.drop(columns=['pcd', 'sbd', 'circulating_market_cap', 'market_cap'], axis=1)
        factor_historical_value = pd.merge(factor_historical_value, historical_value, on="security_code")
        return factor_historical_value

    @staticmethod
    def _ctop5(tp_historical_value, factor_historical_value, dependencies=['pcd', 'sbd', 'circulating_market_cap_5', 'market_cap_5']):
        """
        5 年平均现金流市值比  = 近5年每股派现 * 分红前总股本/近5年总市值
        :name:
        :desc:
        :unit:
        :view_dimension: 0.01
        """
        historical_value = tp_historical_value.loc[:, dependencies]

        fun = lambda x: x[0] * x[1] / x[2] if x[2] is not None and x[2] != 0 else (
            x[0] * x[1] / x[3] if x[3] is not None and x[3] != 0 else None)

        historical_value['historical_value_ctop5_latest'] = historical_value[dependencies].apply(fun, axis=1)

        historical_value = historical_value.drop(columns=['pcd', 'sbd', 'circulating_market_cap_5', 'market_cap_5'],
                                                 axis=1)
        factor_historical_value = pd.merge(factor_historical_value, historical_value, on="security_code")
        return factor_historical_value


# OptCFToNITTM`
# CashDivCovMulti`
# CashToMrkRatio`
