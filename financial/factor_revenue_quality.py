#!/usr/bin/env python
# coding=utf-8
import gc, six
import sys
sys.path.append("../")
sys.path.append("../../")
sys.path.append("../../../")
import numpy as np
import pandas as pd
import json
from pandas.io.json import json_normalize
from basic_derivation.factor_base import FactorBase
from utilities.calc_tools import CalcTools
from utilities.singleton import Singleton

# from basic_derivation import app
# from ultron.cluster.invoke.cache_data import cache_data
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


@six.add_metaclass(Singleton)
class RevenueQuality(object):
    """
    收益质量
    """
    def __init__(self):
        __str__ = 'factor_revenue_quality'
        self.name = '财务指标'
        self.factor_type1 = '财务指标'
        self.factor_type2 = '收益质量'
        self.desciption = '财务指标的二级指标， 收益质量'

    @staticmethod
    def NetNonOIToTP(tp_revenue_quanlity, revenue_quality, dependencies=['total_profit', 'non_operating_revenue', 'non_operating_expense']):
        """
        营业外收支净额/利润总额
        :param dependencies:
        :param tp_revenue_quanlity:
        :param revenue_quality:
        :return:
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
        营业外收支净额/利润总额
        :param dependencies:
        :param ttm_revenue_quanlity:
        :param revenue_quality:
        :return:
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
        经营活动净收益/利润总额
        （注，对于非金融企业 经营活动净收益=营业总收入-营业总成本；
        对于金融企业 经营活动净收益=营业收入-公允价值变动损益-投资收益-汇兑损益-营业支出
        此处以非金融企业的方式计算）
        :param dependencies:
        :param ttm_revenue_quanlity:
        :param revenue_quality:
        :return:
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
        经营活动净收益/利润总额
        （注，对于非金融企业 经营活动净收益=营业总收入-营业总成本；
        对于金融企业 经营活动净收益=营业收入-公允价值变动损益-投资收益-汇兑损益-营业支出
        此处以非金融企业的方式计算）
        :param dependencies:
        :param tp_revenue_quanlity:
        :param revenue_quality:
        :return:
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
        经营活动产生的现金流量净额（TTM）/流动负债（TTM）

        :param dependencies:
        :param ttm_revenue_quanlity:
        :param revenue_quality:
        :return:
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
        价值变动净收益/利润总额(TTM)
        :param ttm_revenue_quanlity:
        :param revenue_quality:
        :param dependencies:
        :return:
        """
        historical_value = ttm_revenue_quanlity.loc[:, dependencies]
        revenue_quality = pd.merge(revenue_quality, historical_value, how='outer', on='security_code')
        return revenue_quality

    @staticmethod
    def OPToTPTTM(ttm_revenue_quanlity, revenue_quality,
                  dependencies=['operating_profit', 'total_profit']):
        """
        营业利润/利润总额(TTM)
        :param ttm_revenue_quanlity:
        :param revenue_quality:
        :param dependencies:
        :return:
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
        收益市值比(TTM)
        :param ttm_revenue_quanlity:
        :param revenue_quality:
        :param dependencies:
        :return:
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
        利润率（TTM）
        :param ttm_revenue_quanlity:
        :param revenue_quality:
        :param dependencies:
        :return:
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
        5年平均收益市值比 = 近5年净利润 / 近5年总市值 TTM
        :param ttm_revenue_quanlity:
        :param revenue_quality:
        :param dependencies:
        :return:
        """
        historical_value = ttm_revenue_quanlity.loc[:, dependencies]

        fun = lambda x: x[0] / x[1] if x[1] is not None and x[1] != 0 else (x[0] / x[2] if x[2] is not None and x[2] !=0 else None)
        historical_value['PriceToRevRatioAvg5YTTM'] = historical_value[dependencies].apply(fun, axis=1)
        historical_value = historical_value.drop(columns=dependencies, axis=1)
        revenue_quality = pd.merge(revenue_quality, historical_value, how='outer', on="security_code")
        return revenue_quality


def calculate(trade_date, tp_revenue_quanlity, ttm_revenue_quanlity, factor_name):
    # 计算对应因子
    tp_revenue_quanlity = tp_revenue_quanlity.set_index('security_code')
    ttm_revenue_quanlity = ttm_revenue_quanlity.set_index('security_code')
    revenue_quality = RevenueQuality()

    factor_revenue = pd.DataFrame()
    factor_revenue['security_code'] = tp_revenue_quanlity.index
    factor_revenue = factor_revenue.set_index('security_code')
    # 非TTM计算
    factor_revenue = revenue_quality.NetNonOIToTP(tp_revenue_quanlity, factor_revenue)
    factor_revenue = revenue_quality.OperatingNIToTP(tp_revenue_quanlity, factor_revenue)
    factor_revenue = revenue_quality.PriceToRevRatioAvg5YTTM(tp_revenue_quanlity, factor_revenue)

    # TTM计算
    factor_revenue = revenue_quality.NetNonOIToTPTTM(ttm_revenue_quanlity, factor_revenue)
    factor_revenue = revenue_quality.OperatingNIToTPTTM(ttm_revenue_quanlity, factor_revenue)
    factor_revenue = revenue_quality.OptCFToCurrLiabilityTTM(ttm_revenue_quanlity, factor_revenue)
    factor_revenue = revenue_quality.OPToTPTTM(ttm_revenue_quanlity, factor_revenue)
    factor_revenue = revenue_quality.PriceToRevRatioTTM(ttm_revenue_quanlity, factor_revenue)
    factor_revenue = revenue_quality.NVALCHGITOTP(ttm_revenue_quanlity, factor_revenue)
    factor_revenue = revenue_quality.PftMarginTTM(ttm_revenue_quanlity, factor_revenue)
    factor_revenue = factor_revenue.reset_index()

    factor_revenue['trade_date'] = str(trade_date)
    print(factor_revenue.head())
    return factor_revenue


# @app.task()
def factor_calculate(**kwargs):
    print("constrain_kwargs: {}".format(kwargs))
    date_index = kwargs['date_index']
    session = kwargs['session']
    content1 = cache_data.get_cache(session + str(date_index) + '1', date_index)
    content2 = cache_data.get_cache(session + str(date_index) + '2', date_index)
    tp_revenue_quanlity = json_normalize(json.loads(str(content1, encoding='utf8')))
    ttm_revenue_quanlity = json_normalize(json.loads(str(content2, encoding='utf8')))
    tp_revenue_quanlity.set_index('security_code', inplace=True)
    ttm_revenue_quanlity.set_index('security_code', inplace=True)
    print("len_tp_revenue_quanlity {}".format(len(tp_revenue_quanlity)))
    print("len_ttm_revenue_quanlity {}".format(len(ttm_revenue_quanlity)))
    calculate(date_index, tp_revenue_quanlity, ttm_revenue_quanlity)
