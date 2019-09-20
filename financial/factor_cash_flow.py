#!/usr/bin/env python
# coding=utf-8

import gc, six
import json
import numpy as np
import pandas as pd
from pandas.io.json import json_normalize

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
        __str__ = 'factor_valuation'
        self.name = '财务指标'
        self.factor_type1 = '财务指标'
        self.factor_type2 = '现金流量'
        self.desciption = '现金流量因子'

    @staticmethod
    def CashOfSales(tp_cash_flow, factor_cash_flow, dependencies=['net_operate_cash_flow', 'operating_revenue']):
        """
        # 经验活动产生的现金流量净额 / 营业收入
        :param tp_cash_flow:
        :param factor_cash_flow:
        :param dependencies:
        :return:
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
        经营活动产生的现金流量净额（Latest）/(营业总收入（Latest）-营业总成本（Latest）)
        :param tp_cash_flow:
        :param factor_cash_flow:
        :param dependencies:
        :return:
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
        # 销售商品和提供劳务收到的现金（Latest）/营业收入（Latest）
        :param tp_cash_flow:
        :param factor_cash_flow:
        :param dependencies:
        :return:
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
        （经营活动产生的金流量净额（TTM） - 净利润（TTM）） /总资产（TTM）
        :param ttm_cash_flow:
        :param factor_cash_flow:
        :param dependencies:
        :return: OptOnReToAssetTTM
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
        # 经营活动产生的现金流量净额（TTM）/归属于母公司所有者的净利润（TTM）
        :param ttm_cash_flow:
        :param factor_cash_flow:
        :param dependencies:
        :return:
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
        # 经营活动产生的现金流量净额（TTM）/（长期借款（TTM）+ 短期借款（TTM）+ 总市值 - 期末现金及现金等价物（TTM）
        :param ttm_cash_flow:
        :param factor_cash_flow:
        :param dependencies:
        :return:
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
        # 经营活动产生的现金流量净额（TTM）/营业收入（TTM）
        :param ttm_cash_flow:
        :param factor_cash_flow:
        :param dependencies:
        :return:
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
        # 经营活动产生的现金流量净额（TTM）/总资产（TTM）
        :param ttm_cash_flow:
        :param factor_cash_flow:
        :param dependencies:
        :return:
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
    def SaleServCashToOptReTTM(ttm_cash_flow, factor_cash_flow, dependencies=['goods_sale_and_service_render_cash', 'operating_revenue']):
        """
        # 销售商品和提供劳务收到的现金（TTM）/营业收入（TTM）
        :param ttm_cash_flow:
        :param factor_cash_flow:
        :param dependencies:
        :return:
        """
        cash_flow = ttm_cash_flow.loc[:, dependencies]
        cash_flow['SaleServCashToOptReTTM'] = np.where(
            CalcTools.is_zero(cash_flow.operating_revenue.values), 0,
            cash_flow.goods_sale_and_service_render_cash.values / cash_flow.operating_revenue.values)
        cash_flow = cash_flow.drop(dependencies, axis=1)
        factor_cash_flow = pd.merge(factor_cash_flow, cash_flow, how='outer', on="security_code")
        return factor_cash_flow

    @staticmethod
    def ctop(tp_historical_value, factor_historical_value, dependencies=['pcd', 'sbd', 'circulating_market_cap', 'market_cap']):
        """
        现金流市值比 = 每股派现 * 分红前总股本/总市值
        :param tp_historical_value:
        :param factor_historical_value:
        :param dependencies:
        :return:
        """

        historical_value = tp_historical_value.loc[:, dependencies]

        fun = lambda x: x[0] * x[1] / x[2] if x[2] is not None and x[2] != 0 else (x[0] * x[1] / x[3] if x[3] is not None and x[3] != 0 else None)

        historical_value['historical_value_ctop_latest'] = historical_value[dependencies].apply(fun, axis=1)

        historical_value = historical_value.drop(columns=['pcd', 'sbd', 'circulating_market_cap', 'market_cap'], axis=1)
        factor_historical_value = pd.merge(factor_historical_value, historical_value, on="security_code")
        return factor_historical_value

    @staticmethod
    def ctop5(tp_historical_value, factor_historical_value, dependencies=['pcd', 'sbd', 'circulating_market_cap_5', 'market_cap_5']):
        """
        5 年平均现金流市值比  = 近5年每股派现 * 分红前总股本/近5年总市值
        :param tp_historical_value:
        :param factor_historical_value:
        :param dependencies:
        :return:
        """
        historical_value = tp_historical_value.loc[:, dependencies]

        fun = lambda x: x[0] * x[1] / x[2] if x[2] is not None and x[2] != 0 else (
            x[0] * x[1] / x[3] if x[3] is not None and x[3] != 0 else None)

        historical_value['historical_value_ctop5_latest'] = historical_value[dependencies].apply(fun, axis=1)

        historical_value = historical_value.drop(columns=['pcd', 'sbd', 'circulating_market_cap_5', 'market_cap_5'],
                                                 axis=1)
        factor_historical_value = pd.merge(factor_historical_value, historical_value, on="security_code")
        return factor_historical_value



'''
`OptCFToNITTM`
`NOCFTOOPftTTM`
`CashDivCovMulti`
`CashToMrkRatio`
'''


def calculate(trade_date, tp_cash_flow, ttm_factor_sets, factor_name):  # 计算对应因子
    tp_cash_flow = tp_cash_flow.set_index('security_code')
    ttm_factor_sets = ttm_factor_sets.set_index('security_code')

    cash_flow = FactorCashFlow()
    factor_cash_flow = pd.DataFrame()
    factor_cash_flow['security_code'] = tp_cash_flow.index
    factor_cash_flow = factor_cash_flow.set_index('security_code')

    # 非TTM计算
    factor_cash_flow = cash_flow.CashOfSales(tp_cash_flow, factor_cash_flow)
    factor_cash_flow = cash_flow.NOCFToOpt(tp_cash_flow, factor_cash_flow)
    factor_cash_flow = cash_flow.SalesServCashToOR(tp_cash_flow, factor_cash_flow)

    # TTM计算
    factor_cash_flow = cash_flow.OptOnReToAssetTTM(ttm_factor_sets, factor_cash_flow)
    factor_cash_flow = cash_flow.NetProCashCoverTTM(ttm_factor_sets, factor_cash_flow)
    factor_cash_flow = cash_flow.OptToEnterpriseTTM(ttm_factor_sets, factor_cash_flow)
    factor_cash_flow = cash_flow.OptCFToRevTTM(ttm_factor_sets, factor_cash_flow)
    factor_cash_flow = cash_flow.OptToAssertTTM(ttm_factor_sets, factor_cash_flow)
    factor_cash_flow = cash_flow.SaleServCashToOptReTTM(ttm_factor_sets, factor_cash_flow)

    factor_cash_flow['trade_date'] = str(trade_date)
    factor_cash_flow = factor_cash_flow.reset_index()
    print('factor_cash_flow: \n%s' % factor_cash_flow.head())
    return factor_cash_flow


# @app.task()
def factor_calculate(**kwargs):
    print("cash_flow_kwargs: {}".format(kwargs))
    date_index = kwargs['date_index']
    session = kwargs['session']
    factor_name = kwargs['factor_name']
    content1 = cache_data.get_cache(session + str(date_index) + "1", date_index)
    content2 = cache_data.get_cache(session + str(date_index) + "2", date_index)
    tp_cash_flow = json_normalize(json.loads(str(content1, encoding='utf8')))
    ttm_factor_sets = json_normalize(json.loads(str(content2, encoding='utf8')))
    tp_cash_flow.set_index('security_code', inplace=True)
    ttm_factor_sets.set_index('security_code', inplace=True)
    print("len_tp_cash_flow_data {}".format(len(tp_cash_flow)))
    print("len_ttm_cash_flow_data {}".format(len(ttm_factor_sets)))
    calculate(date_index, tp_cash_flow, ttm_factor_sets, factor_name)
