#! /usr/bin/env python
# -*- coding: utf-8 -*-

"""
@version:
@author: li
@file: factor_operation_capacity.py
@time: 2019-05-31
"""
import sys
sys.path.append("../")
sys.path.append("../../")
sys.path.append("../../../")
import gc, six
import json
import numpy as np
import pandas as pd
from sklearn import linear_model
from utilities.calc_tools import CalcTools
from utilities.singleton import Singleton

from pandas.io.json import json_normalize

# from basic_derivation import app
# from ultron.cluster.invoke.cache_data import cache_data


@six.add_metaclass(Singleton)
class FactorEarning(object):
    """
    盈利能力
    """
    def __init__(self):
        __str__ = 'factor_earning'
        self.name = '财务指标'
        self.factor_type1 = '财务指标'
        self.factor_type2 = '盈利能力'
        self.description = '财务指标的二级指标-盈利能力'

    @staticmethod
    def _Rev5YChg(tp_earning, factor_earning, dependencies=['operating_revenue',
                                                            'operating_revenue_pre_year_1',
                                                            'operating_revenue_pre_year_2',
                                                            'operating_revenue_pre_year_3',
                                                            'operating_revenue_pre_year_4']):
        """
        五年营业收入增长率
        :name:
        :desc:
        :unit:
        :view_dimension: 0.01
        """
        regr = linear_model.LinearRegression()
        # 读取五年的时间和净利润
        historical_growth = tp_earning.loc[:, dependencies]
        if len(historical_growth) <= 0:
            return

        def has_non(a):
            tmp = 0
            for i in a.tolist():
                for j in i:
                    if j is None or j == 'nan':
                        tmp += 1
            if tmp >= 1:
                return True
            else:
                return False

        def fun2(x):
            aa = x[dependencies].fillna('nan').values.reshape(-1, 1)
            if has_non(aa):
                return None
            else:
                regr.fit(aa, range(0, 5))
                return regr.coef_[-1]

        historical_growth['coefficient'] = historical_growth.apply(fun2, axis=1)
        historical_growth['mean'] = historical_growth[dependencies].fillna(method='ffill').mean(axis=1)

        fun1 = lambda x: x[0] / abs(x[1]) if x[1] is not None and x[0] is not None and x[1] != 0 else None
        historical_growth['Rev5YChg'] = historical_growth[['coefficient', 'mean']].apply(fun1, axis=1)

        historical_growth = historical_growth[['Rev5YChg']]
        factor_earning = pd.merge(factor_earning, historical_growth, on='security_code')

        return factor_earning

    @staticmethod
    def _NetPft5YAvgChg(tp_earning, factor_earning,
                       dependencies=['net_profit', 'net_profit_pre_year_1', 'net_profit_pre_year_2',
                                      'net_profit_pre_year_3', 'net_profit_pre_year_4']):
        """
        5年收益增长率
        :name:
        :desc:
        :unit:
        :view_dimension: 0.01
        """

        regr = linear_model.LinearRegression()
        # 读取五年的时间和净利润
        historical_growth = tp_earning.loc[:, dependencies]
        if len(historical_growth) <= 0:
            return

        def has_non(a):
            tmp = 0
            for i in a.tolist():
                for j in i:
                    if j is None or j == 'nan':
                        tmp += 1
            if tmp >= 1:
                return True
            else:
                return False

        def fun2(x):
            aa = x[dependencies].fillna('nan').values.reshape(-1, 1)
            if has_non(aa):
                return None
            else:
                regr.fit(aa, range(0, 5))
                return regr.coef_[-1]

        historical_growth['coefficient'] = historical_growth.apply(fun2, axis=1)
        historical_growth['mean'] = historical_growth[dependencies].fillna(method='ffill').mean(axis=1)

        fun1 = lambda x: x[0] / abs(x[1]) if x[1] != 0 and x[1] is not None and x[0] is not None else None
        historical_growth['NetPft5YAvgChg'] = historical_growth[['coefficient', 'mean']].apply(fun1, axis=1)

        # historical_growth = historical_growth.drop(
        #     columns=['net_profit', 'net_profit_pre_year_1', 'net_profit_pre_year_2', 'net_profit_pre_year_3',
        #              'net_profit_pre_year_4', 'coefficient', 'mean'], axis=1)

        historical_growth = historical_growth[['security_code', 'NetPft5YAvgChg']]
        factor_earning = pd.merge(factor_earning, historical_growth, on='security_code')

        return factor_earning

    @staticmethod
    def _DGPR(ttm_earning, ttm_earning_p1y, factor_earning, dependencies=['operating_revenue', 'operating_cost']):
        """
        毛利率增长率，与去年同期相比
        :name:
        :desc:
        :unit:
        :view_dimension: 0.01
        """

        earning = ttm_earning.loc[:, dependencies]
        earning_p1y = ttm_earning_p1y.loc[:, dependencies]
        earning['gross_income_ratio'] = np.where(
            CalcTools.is_zero(earning.operating_revenue.values), 0,
            (earning.operating_revenue.values -
             earning.operating_cost.values)
            / earning.operating_revenue.values
                )
        earning_p1y['gross_income_ratio'] = np.where(
            CalcTools.is_zero(earning_p1y.operating_revenue.values), 0,
            (earning_p1y.operating_revenue.values -
             earning_p1y.operating_cost.values)
            / earning_p1y.operating_revenue.values)

        earning["DGPR"] = earning["gross_income_ratio"] - earning_p1y["gross_income_ratio"]
        dependencies = dependencies + ['gross_income_ratio']
        earning = earning.drop(dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, earning, on="security_code")
        return factor_earning

    @staticmethod
    def ROA5YChg(ttm_earning_5y, factor_earning, dependencies=['net_profit', 'total_assets']):
        """
        :name: 5年资产回报率
        :desc: 5年收益关于时间（年）进行线性回归的回归系数/（5年收益均值的绝对值）对于上市新股以上市前已披露的3年净利润计算之后新的年报数据披露后再计算四年、五年的收益增长率数据每年变化一次，在年报披露日
        :unit:
        :view_dimension: 0.01
        """
        earning = ttm_earning_5y.loc[:, dependencies]
        earning['ROA5YChg'] = np.where(
            CalcTools.is_zero(earning.total_assets.values), 0,
            earning.net_profit.values / earning.total_assets.values / 4)
        earning = earning.drop(dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, earning, on="security_code")
        return factor_earning

    @staticmethod
    def ROE5Y(ttm_earning_5y, factor_earning, dependencies=['net_profit', 'total_owner_equities']):
        """

        :name: 5年平均权益回报率
        :desc: AVG（净利润*2/（本年股东权益（MRQ）+上年末股东权益（MRQ）      限定只计算过去五年的年报
        :unit:
        :view_dimension: 0.01
        """
        earning = ttm_earning_5y.loc[:, dependencies]
        earning['ROE5Y'] = np.where(
            CalcTools.is_zero(earning.total_owner_equities.values), 0,
            earning.net_profit.values / earning.total_owner_equities.values / 4)
        earning = earning.drop(dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, earning, on="security_code")
        return factor_earning

    @staticmethod
    def NPCutToNP(tp_earning, factor_earning, dependencies=['adjusted_profit', 'net_profit']):
        """
        :name: 扣除非经常损益后的净利润/净利润
        :desc: 扣除非经常损益后的净利润/净利润
        :unit:
        :view_dimension: 0.01
        """
        earning = tp_earning.loc[:, dependencies]
        earning['NPCutToNP'] = np.where(
            CalcTools.is_zero(earning.net_profit.values), 0,
            earning.adjusted_profit.values
            / earning.net_profit.values)
        earning = earning.drop(dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, earning, on="security_code")
        return factor_earning

    @staticmethod
    def ROE(tp_earning, factor_earning,
            dependencies=['np_parent_company_owners', 'equities_parent_company_owners']):
        """
        :name: 净资产收益率（摊薄）
        :desc: 归属于母公司的净利润/期末归属于母公司的所有者权益
        :unit:
        :view_dimension: 0.01
        """
        earning = tp_earning.loc[:, dependencies]
        earning['ROE'] = np.where(
            CalcTools.is_zero(earning.equities_parent_company_owners.values), 0,
            earning.np_parent_company_owners.values /
            earning.equities_parent_company_owners.values / 4)
        earning = earning.drop(dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, earning, on="security_code")
        return factor_earning

    @staticmethod
    def ROEAvg(tp_earning, factor_earning,
               dependencies=['np_parent_company_owners', 'equities_parent_company_owners']):
        """
        :name:  净资产收益率（平均）
        :desc: 净资产收益率（平均）=归属于母公司的净利润*2/(期末归属于母公司的所有者权益 + 期初归属于母公司的所有者权益)
        :unit:
        :view_dimension: 0.01
        """

        earning = tp_earning.loc[:, dependencies]
        earning['ROEAvg'] = np.where(
            CalcTools.is_zero(earning.equities_parent_company_owners.values), 0,
            earning.np_parent_company_owners.values /
            earning.equities_parent_company_owners.values / 4)
        earning = earning.drop(dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, earning, on="security_code")
        return factor_earning

    @staticmethod
    def ROEcut(tp_earning, factor_earning, dependencies=['adjusted_profit', 'equities_parent_company_owners']):
        """
        :name: 净资产收益率（扣除/摊薄）
        :desc: 净资产收益率（扣除/摊薄）
        :unit:
        :view_dimension: 0.01
        """
        earning = tp_earning.loc[:, dependencies]
        earning['ROEcut'] = np.where(
            CalcTools.is_zero(earning.equities_parent_company_owners.values), 0,
            earning.adjusted_profit.values /
            earning.equities_parent_company_owners.values / 4)
        earning = earning.drop(dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, earning, on="security_code")
        return factor_earning

    @staticmethod
    def _invest_r_associates_to_tp_latest(tp_earning, factor_earning, dependencies=['invest_income_associates', 'total_profit']):
        """
        对联营和营公司投资收益/利润总额
        :name:
        :desc:
        :unit:
        :view_dimension: 0.01
        """

        earning = tp_earning.loc[:, dependencies]
        earning['invest_r_associates_to_tp_latest'] = np.where(
            CalcTools.is_zero(earning.total_profit.values), 0,
            earning.invest_income_associates.values
            / earning.total_profit.values)
        earning = earning.drop(dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, earning, on="security_code")
        return factor_earning

    @staticmethod
    def NetPft5YAvgChgTTM(ttm_earning, factor_earning,
                          dependencies=['net_profit', 'net_profit_pre_year_1', 'net_profit_pre_year_2',
                                        'net_profit_pre_year_3', 'net_profit_pre_year_4']):
        """
        :name: 5年收益增长率(TTM)
        :desc: 5年收益关于时间（年）进行线性回归的回归系数/（5年收益均值的绝对值）对于上市新股以上市前已披露的3年净利润计算之后新的年报数据披露后再计算四年、五年的收益增长率数据每年变化一次，在年报披露日
        :unit:
        :view_dimension: 0.01
        """
        regr = linear_model.LinearRegression()
        # 读取五年的时间和净利润
        historical_growth = ttm_earning.loc[:, dependencies]
        if len(historical_growth) <= 0:
            return

        def has_non(a):
            tmp = 0
            for i in a.tolist():
                for j in i:
                    if j is None or j == 'nan':
                        tmp += 1
            if tmp >= 1:
                return True
            else:
                return False

        def fun2(x):
            aa = x[dependencies].fillna('nan').values.reshape(-1, 1)
            if has_non(aa):
                return None
            else:
                regr.fit(aa, range(0, 5))
                return regr.coef_[-1]
        historical_growth['coefficient'] = historical_growth.apply(fun2, axis=1)
        historical_growth['mean'] = historical_growth[dependencies].fillna(method='ffill').mean(axis=1)
        fun1 = lambda x: x[0] / abs(x[1]) if x[1] != 0 and x[1] is not None and x[0] is not None else None
        historical_growth['NetPft5YAvgChgTTM'] = historical_growth[['coefficient', 'mean']].apply(fun1, axis=1)

        dependencies = dependencies + ['coefficient', 'mean']
        # historical_growth = historical_growth[['security_code', 'NetPft5YAvgChgTTM']]
        historical_growth = historical_growth.drop(dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, historical_growth, how='outer', on='security_code')

        return factor_earning

    @staticmethod
    def Sales5YChgTTM(ttm_earning, factor_earning, dependencies=['operating_revenue',
                                                                 'operating_revenue_pre_year_1',
                                                                 'operating_revenue_pre_year_2',
                                                                 'operating_revenue_pre_year_3',
                                                                 'operating_revenue_pre_year_4']):
        """
        :name: 5 年营业收入增长率(TTM)
        :desc: 5 年营收关于时间（年）进行线性回归的回归系数/5 年营业收入均值的绝对值
        :unit:
        :view_dimension: 0.01
        """
        regr = linear_model.LinearRegression()
        # 读取五年的时间和净利润
        historical_growth = ttm_earning.loc[:, dependencies]
        if len(historical_growth) <= 0:
            return

        def has_non(a):
            tmp = 0
            for i in a.tolist():
                for j in i:
                    if j is None or j == 'nan':
                        tmp += 1
            if tmp >= 1:
                return True
            else:
                return False

        def fun2(x):
            aa = x[dependencies].fillna('nan').values.reshape(-1, 1)
            if has_non(aa):
                return None
            else:
                regr.fit(aa, range(0, 5))
                return regr.coef_[-1]

        historical_growth['coefficient'] = historical_growth.apply(fun2, axis=1)
        historical_growth['mean'] = historical_growth[dependencies].fillna(method='ffill').mean(axis=1)

        fun1 = lambda x: x[0] / abs(x[1]) if x[1] is not None and x[0] is not None and x[1] != 0 else None
        historical_growth['Sales5YChgTTM'] = historical_growth[['coefficient', 'mean']].apply(fun1, axis=1)

        dependencies = dependencies + ['coefficient', 'mean']
        historical_growth = historical_growth.drop(columns=dependencies, axis=1)
        # historical_growth = historical_growth[['Sales5YChgTTM']]
        factor_earning = pd.merge(factor_earning, historical_growth, how='outer', on='security_code')

        return factor_earning

    @staticmethod
    def _roa(ttm_earning, factor_earning, dependencies=['net_profit', 'total_assets']):
        """
        资产回报率
        资产回报率=净利润/总资产
        :name:
        :desc:
        :unit:
        :view_dimension: 0.01
        """
        earning = ttm_earning.loc[:, dependencies]
        earning['roa'] = np.where(
            CalcTools.is_zero(earning.total_assets.values), 0,
            earning.net_profit.values / earning.total_assets.values)
        earning = earning.drop(dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, earning, how='outer', on="security_code")
        return factor_earning

    @staticmethod
    def AdminExpTTM(ttm_earning, factor_earning, dependencies=['administration_expense', 'total_operating_revenue']):
        """
        :name: 管理费用与营业总收入之比
        :desc: 管理费用/营业总收入
        :unit:
        :view_dimension: 0.01
        """
        constrains = ttm_earning.loc[:, dependencies]
        constrains['AdminExpTTM'] = np.where(
            CalcTools.is_zero(constrains['total_operating_revenue']), 0,
            constrains['administration_expense'] / constrains['total_operating_revenue']
        )
        constrains = constrains.drop(dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, constrains, how='outer', on="security_code")
        return factor_earning

    @staticmethod
    def BerryRtTTM(ttm_earning, factor_earning,
                   dependencies=['operating_revenue',
                                      'operating_cost',
                                      'financial_expense',
                                      'sale_expense',
                                      'administration_expense'
                                      ]):
        """
        :name: 贝里比率(TTM)
        :desc: 毛利（TTM）/运营费用（TTM），其中运营费用=销售费用+管理费用+财务费用
        :unit:
        :view_dimension: 0.01
        """
        constrains = ttm_earning.loc[:, dependencies]
        func = lambda x: (x[0] + x[1]) / (x[2] + x[3] + x[4])\
            if x[0] is not None and x[1] is not None \
               and x[2] is not None and x[3] is not None \
               and x[4] is not None and (x[2] + x[3] + x[4]) != 0 else None

        constrains['BerryRtTTM'] = constrains.apply(func, axis=1)
        constrains = constrains.drop(columns=dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, constrains, how='outer', on="security_code")
        return factor_earning

    @staticmethod
    def CFARatioMinusROATTM(ttm_earning, factor_earning,
                            dependencies=['net_finance_cash_flow',
                                          'net_profit',
                                          'total_assets_mrq',
                                          'total_assets_mrq_pre']):
        """
        :name: 现金流资产比-资产回报率（TTM）
        :desc: （筹资活动现金净流量（ITM）-净利润（TTM））*2/（本期资产总计（MRQ）+上年同期产总计（MRQ）)
        :unit:
        :view_dimension: 0.01
        """
        constrains = ttm_earning.loc[:, dependencies]
        func = lambda x: (x[0] - x[1]) * 2 / (x[2] + x[3]) if x[2] is not None and x[3] is not None and (x[2] + x[3]) !=0 else None
        constrains['CFARatioMinusROATTM'] = constrains.apply(func, axis=1)
        constrains = constrains.drop(dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, constrains, on="security_code")
        return factor_earning

    @staticmethod
    def SalesCostTTM(ttm_earning, factor_earning, dependencies=['operating_cost', 'operating_revenue']):
        """
        :name: 销售成本率(TTM)
        :desc: 营业成本(TTM)/营业收入(TTM)
        :unit:
        :view_dimension: 0.01
        """
        constrains = ttm_earning.loc[:, dependencies]
        constrains['SalesCostTTM'] = np.where(
            CalcTools.is_zero(constrains['operating_revenue']),
            0, constrains['operating_cost'] / constrains['operating_revenue']
        )
        constrains = constrains.drop(dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, constrains, on="security_code")
        return factor_earning

    # @staticmethod
    # def EBITToTORevTTM(ttm_earning, factor_earning, dependencies=['total_profit', 'financial_expense', 'interest_income', 'total_operating_revenue']):
    #     """
    #     缺利息收入
    #     :name: 息税前利润与营业总收入之比(TTM)
    #     :desc: （利润总额+利息支出-利息收入)/营业总收入
    #     """
    #     earning = ttm_earning.loc[:, dependencies]
    #     earning['EBITToTORevTTM'] = np.where(
    #         CalcTools.is_zero(earning.total_operating_revenue.values), 0,
    #         (earning.total_profit.values +
    #          earning.financial_expense.values -
    #          earning.interest_income.values)
    #         / earning.total_operating_revenue.values)
    #     earning = earning.drop(dependencies, axis=1)
    #     factor_earning = pd.merge(factor_earning, earning, on="security_code")
    #     return factor_earning

    @staticmethod
    def PeridCostTTM(ttm_earning, factor_earning, dependencies=['financial_expense', 'sale_expense', 'administration_expense', 'operating_revenue']):
        """
        :name: 销售期间费用率(TTM)
        :desc: (财务费用 + 销售费用 + 管理费用) / (营业收入)
        :unit:
        :view_dimension: 0.01
        """
        constrains = ttm_earning.loc[:, dependencies]
        constrains['PeridCostTTM'] = np.where(
            CalcTools.is_zero(constrains['operating_revenue']), 0,
            (constrains['financial_expense'] + constrains['sale_expense'] + constrains['administration_expense']) / \
            constrains['operating_revenue']
        )
        constrains = constrains.drop(dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, constrains, on="security_code")
        return factor_earning

    @staticmethod
    def FinExpTTM(ttm_earning, factor_earning, dependencies=['financial_expense', 'total_operating_cost']):
        """
        :name: 财务费用与营业总收入之比(TTM)
        :desc: 财务费用(TTM)/营业总收入(TTM)
        :unit:
        :view_dimension: 0.01
        """
        constrains = ttm_earning.loc[:, dependencies]
        constrains['FinExpTTM'] = np.where(
            CalcTools.is_zero(constrains['total_operating_cost']), 0,
            constrains['financial_expense'] / constrains['total_operating_cost']
        )
        constrains = constrains.drop(dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, constrains, on="security_code")
        return factor_earning

    @staticmethod
    def ImpLossToTOITTM(ttm_earning, factor_earning,
                        dependencies=['asset_impairment_loss', 'total_operating_revenue']):
        """
        :name: 资产减值损失/营业总收入(TTM)
        :desc: 资产减值损失/营业总收入(TTM)
        :unit:
        :view_dimension: 0.01
        """
        constrains = ttm_earning.loc[:, dependencies]
        func = lambda x: x[0] / x[1] if x[1] is not None and x[1] != 0 else None
        constrains['ImpLossToTOITTM'] = constrains.apply(func, axis=1)

        constrains = constrains.drop(columns=dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, constrains, how='outer', on="security_code")
        return factor_earning

    @staticmethod
    def OIAToOITTM(ttm_earning, factor_earning,
                   dependencies=['np_parent_company_owners', 'operating_revenue']):
        """
        :name: 归属母公司股东的净利润/营业收入(TTM)
        :desc: 归属母公司股东的净利润/营业收入 TTM
        :unit:
        :view_dimension: 0.01
        """
        constrains = ttm_earning.loc[:, dependencies]
        func = lambda x: x[0] / x[1] if x[1] is not None and x[1] != 0 else None
        constrains['OIAToOITTM'] = constrains.apply(func, axis=1)

        constrains = constrains.drop(columns=dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, constrains, how='outer', on="security_code")
        return factor_earning

    @staticmethod
    def ROAexTTM(ttm_earning, factor_earning,
                 dependencies=['np_parent_company_owners', 'total_assets_mrq']):
        """

        :name: 总资产净利率(TTM)(不含少数股东损益)
        :desc: 总资产净利率(不含少数股东损益)
        :unit:
        :view_dimension: 0.01
        """
        constrains = ttm_earning.loc[:, dependencies]
        func = lambda x: x[0] / x[1] if x[1] is not None and x[1] != 0 else None
        constrains['ROAexTTM'] = constrains.apply(func, axis=1)

        constrains = constrains.drop(columns=dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, constrains, how='outer', on="security_code")
        return factor_earning

    @staticmethod
    def NetNonOToTP(ttm_earning, factor_earning,
                     dependencies=['total_operating_cost', 'total_operating_revenue']):
        """
        :name: 营业总成本/营业总收入(TTM)
        :desc: 营业总成本/营业总收入
        :unit:
        :view_dimension: 0.01
        """
        constrains = ttm_earning.loc[:, dependencies]
        func = lambda x: x[0] / x[1] if x[1] is not None and x[1] != 0 else None
        constrains['NetNonOToTP'] = constrains.apply(func, axis=1)

        constrains = constrains.drop(columns=dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, constrains, how='outer', on="security_code")
        return factor_earning

    @staticmethod
    def NetProfitRtTTM(ttm_earning, factor_earning, dependencies=['net_profit', 'operating_revenue']):
        """
        :name: 销售净利率(TTM)
        :desc: 净利润/营业收入
        :unit:
        :view_dimension: 0.01
        """
        earning = ttm_earning.loc[:, dependencies]
        earning['NetProfitRtTTM'] = np.where(
            CalcTools.is_zero(earning.operating_revenue.values), 0,
            earning.net_profit.values / earning.operating_revenue.values)
        earning = earning.drop(dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, earning, on="security_code")
        return factor_earning

    @staticmethod
    def NPToTORevTTM(ttm_earning, factor_earning, dependencies=['net_profit', 'total_operating_revenue']):
        """
        :name: 净利润与营业总收入之比(TTM)
        :desc: 净利润/营业总收入
        :unit:
        :view_dimension: 0.01
        """
        earning = ttm_earning.loc[:, dependencies]
        earning['NPToTORevTTM'] = np.where(
            CalcTools.is_zero(earning.total_operating_revenue.values), 0,
            earning.net_profit.values / earning.total_operating_revenue.values)
        earning = earning.drop(dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, earning, on="security_code")
        return factor_earning

    @staticmethod
    def OperExpRtTTM(ttm_earning, factor_earning, dependencies=['sale_expense', 'total_operating_cost', 'total_operating_revenue']):
        """
        :name: 营业费用率(TTM)
        :desc: 营业费用与营业总收入之比=销售费用(TTM)/营业总收入(TTM)
        :unit:
        :view_dimension: 0.01
        """
        constrains = ttm_earning.loc[:, dependencies]
        constrains['OperExpRtTTM'] = np.where(
            CalcTools.is_zero(constrains['total_operating_cost']), 0,
            constrains['sale_expense'] / constrains['total_operating_revenue']
        )
        constrains = constrains.drop(dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, constrains, on="security_code")
        return factor_earning

    @staticmethod
    def OptProfitRtTTM(ttm_earning, factor_earning, dependencies=['operating_profit', 'operating_revenue']):
        """
        :name: 营业净利率(TTM)
        :desc: 营业利润/营业收入
        :unit:
        :view_dimension: 0.01
        """
        earning = ttm_earning.loc[:, dependencies]
        earning['OptProfitRtTTM'] = np.where(
            CalcTools.is_zero(earning.operating_revenue.values), 0,
            earning.operating_profit.values / earning.operating_revenue.values)
        earning = earning.drop(dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, earning, on="security_code")
        return factor_earning

    @staticmethod
    def _operating_profit_to_tor(ttm_earning, factor_earning, dependencies=['operating_profit', 'total_operating_revenue']):
        """
        :name: 营业利润与营业总收入之比
        :desc: 营业利润/营业总收入
        :unit:
        :view_dimension: 0.01
        """
        earning = ttm_earning.loc[:, dependencies]
        earning['operating_profit_to_tor'] = np.where(
            CalcTools.is_zero(earning.total_operating_revenue.values), 0,
            earning.operating_profit.values / earning.total_operating_revenue.values)
        earning = earning.drop(dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, earning, on="security_code")
        return factor_earning

    @staticmethod
    def ROCTTM(ttm_earning, factor_earning,
               dependencies=['net_profit',
                             'total_owner_equities_mrq',
                             'longterm_loan_mrq',
                             'total_owner_equities_mrq_pre',
                             'longterm_loan_mrq_pre'
                             ]):
        """
        :name: 资本报酬率(TTM)
        :desc: 净利润（TTM）*2/（本期股东权益（MRQ）+本期长期负债（MRQ）+上年同期股东权益（MRQ）+上年同期长期负债（MRQ））
        :unit:
        :view_dimension: 0.01
        """
        constrains = ttm_earning.loc[:, dependencies]
        func = lambda x: x[0] * 2 / (x[1] + x[2] + x[3] + x[4]) \
            if x[1] is not None and x[2] is not None and x[3] is not None and x[4] is not None and (x[1] + x[2] + x[3] + x[4]) !=0 else None
        constrains['ROCTTM'] = constrains.apply(func, axis=1)
        constrains = constrains.drop(dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, constrains, on="security_code")
        return factor_earning

    # @staticmethod
    # def ROTATTM(ttm_earning, factor_earning, dependencies=['total_profit', 'financial_expense', 'interest_income', 'total_assets']):
    #     """
    #     缺利息收入
    #     :name: 总资产报酬率(TTM)
    #     :desc: ROAEBIT = EBIT*2/(期初总资产+期末总资产）(注，此处用过去四个季度资产均值）
    #     """
    #
    #     earning = ttm_earning.loc[:, dependencies]
    #     earning['ROTATTM'] = np.where(
    #         CalcTools.is_zero(earning.total_assets.values), 0,
    #         (earning.total_profit.values +
    #          earning.financial_expense.values -
    #          earning.interest_income.values)
    #         / earning.total_assets.values / 4)
    #     earning = earning.drop(dependencies, axis=1)
    #     factor_earning = pd.merge(factor_earning, earning, on="security_code")
    #     return factor_earning

    @staticmethod
    def ROETTM(ttm_earning, factor_earning,
               dependencies=['np_parent_company_owners', 'equities_parent_company_owners_mrq']):
        """
        :name: 净资产收益率(TTM)
        :desc: 归属于母公司的净利润（TTM）/归属于母公司的股东权益（MRQ）
        :unit:
        :view_dimension: 0.01
        """
        constrains = ttm_earning.loc[:, dependencies]
        func = lambda x: x[0] / x[1] if x[1] is not None and x[1] != 0 else None
        constrains['ROETTM'] = constrains.apply(func, axis=1)

        constrains = constrains.drop(columns=dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, constrains, how='outer', on="security_code")
        return factor_earning

    @staticmethod
    def ROICTTM(ttm_earning, factor_earning,
                dependencies=['np_parent_company_owners', 'total_assets_mrq']):
        """
        :name:  归属母公司股东的净利润（TTM）/全部投入资本（MRQ）
        :desc:  归属母公司股东的净利润（TTM）/全部投入资本（MRQ）
        :unit:
        :view_dimension: 0.01
        """
        constrains = ttm_earning.loc[:, dependencies]
        func = lambda x: x[0] / x[1] if x[1] is not None and x[1] != 0 else None
        constrains['ROICTTM'] = constrains.apply(func, axis=1)

        constrains = constrains.drop(columns=dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, constrains, how='outer', on="security_code")
        return factor_earning

    @staticmethod
    def OwnROETTM(ttm_earning, factor_earning, dependencies=['net_profit', 'total_owner_equities']):
        """
        :name: 权益回报率(TTM)
        :desc: 净利润/股东权益
        :unit:
        :view_dimension: 0.01
        """
        earning = ttm_earning.loc[:, dependencies]
        earning['OwnROETTM'] = np.where(
            CalcTools.is_zero(earning.total_owner_equities.values), 0,
            earning.net_profit.values / earning.total_owner_equities.values / 4)
        earning = earning.drop(dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, earning, on="security_code")
        return factor_earning

    @staticmethod
    def SalesGrossMarginTTM(ttm_earning, factor_earning, dependencies=['operating_revenue', 'operating_cost']):
        """
        :name: 销售毛利率(TTM)
        :desc:（营业收入-营业成本）/营业收入
        :unit:
        :view_dimension: 0.01
        """
        earning = ttm_earning.loc[:, dependencies]
        earning['SalesGrossMarginTTM'] = np.where(
            CalcTools.is_zero(earning.operating_revenue.values), 0,
            (earning.operating_revenue.values - earning.operating_cost.values)
            / earning.operating_revenue.values)
        earning = earning.drop(dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, earning, on="security_code")
        return factor_earning

    @staticmethod
    def TaxRTTM(ttm_earning, factor_earning, dependencies=['operating_tax_surcharges', 'operating_revenue']):
        """
        :name: 销售税金率(TTM)
        :desc: 营业税金及附加(TTM)/营业收入(TTM)
        :unit:
        :view_dimension: 0.01
        """

        constrains = ttm_earning.loc[:, dependencies]
        constrains['TaxRTTM'] = np.where(
            CalcTools.is_zero(constrains['operating_revenue']), 0,
            constrains['operating_tax_surcharges'] / constrains['operating_revenue']
        )
        constrains = constrains.drop(dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, constrains, on="security_code")
        return factor_earning

    @staticmethod
    def TotaProfRtTTM(ttm_earning, factor_earning,
                      dependencies=['total_profit',
                                                  'operating_cost',
                                                  'financial_expense',
                                                  'sale_expense',
                                                  'administration_expense']):
        """
        :name:成本费用利润率(TTM)
        :desc: 利润总额 / (营业成本+财务费用+销售费用+管理费用
        :unit:
        :view_dimension: 0.01
        """
        constrains = ttm_earning.loc[:, dependencies]
        constrains['cost'] = (constrains.operating_cost +
                              constrains.financial_expense +
                              constrains.sale_expense +
                              constrains.administration_expense)

        constrains['TotaProfRtTTM'] = np.where(
            CalcTools.is_zero(constrains['cost']), 0,
            constrains['total_profit'] / constrains['cost'])

        dependencies = dependencies + ['cost']
        constrains = constrains.drop(dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, constrains, how='outer', on="security_code")
        return factor_earning

    # @staticmethod
    # def _invest_r_associates_to_tp_ttm(ttm_earning, factor_earning, dependencies=['invest_income_associates', 'total_profit']):
    #     """
    #     缺对联营和营公司投资收益
    #     对联营和营公司投资收益/利润总额
    #     :param dependencies:
    #     :param ttm_earning:
    #     :param factor_earning:
    #     :return:
    #     """
    #
    #     earning = ttm_earning.loc[:, dependencies]
    #     earning['invest_r_associates_to_tp_ttm'] = np.where(
    #         CalcTools.is_zero(earning.total_profit.values), 0,
    #         earning.invest_income_associates.values
    #         / earning.total_profit.values)
    #     earning = earning.drop(dependencies, axis=1)
    #     factor_earning = pd.merge(factor_earning, earning, on="security_code")
    #     return factor_earning

