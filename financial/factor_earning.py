#! /usr/bin/env python
# -*- coding: utf-8 -*-

"""
@version:
@author: Wang
@file: factor_operation_capacity.py
@time: 2019-05-31
"""
import sys
sys.path.append("../")
sys.path.append("../../")
sys.path.append("../../../")
import gc
import json
import numpy as np
import pandas as pd
from sklearn import linear_model
from basic_derivation.factor_base import FactorBase
from basic_derivation.utillities.calc_tools import CalcTools
from pandas.io.json import json_normalize

# from basic_derivation import app
from ultron.cluster.invoke.cache_data import cache_data


class FactorEarning(FactorBase):
    """
    盈利能力
    """
    def __init__(self, name):
        super(FactorEarning, self).__init__(name)

    def create_dest_tables(self):
        """
        创建数据库表
        :return:
        """
        drop_sql = """drop table if exists `{0}`""".format(self._name)
        create_sql = """create table `{0}`(
                    `id` INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
                    `security_code` varchar(32) NOT NULL,
                    `trade_date` date NOT NULL,
                    `ROA5YChg` decimal(19,4),
                    `ROE5Y` decimal(19,4),
                    `NPCutToNP` decimal(19,4),
                    `ROE` decimal(19,4),
                    `ROEAvg`  decimal(19,4),
                    `ROEcut` decimal(19,4),
                    `NetPft5YAvgChgTTM` decimal(19,4),
                    `Sales5YChgTTM` decimal(19,4),
                    `AdminExpTTM` decimal(19,4),
                    `BerryRtTTM` decimal(19,4),
                    `CFARatioMinusROATTM` decimal(19,4),
                    `SalesCostTTM` decimal(19,4),
                    `EBITToTORevTTM` decimal(19,4),
                    `PeridCostTTM` decimal(19,4),
                    `FinExpTTM` decimal(19,4),
                    `ImpLossToTOITTM` decimal(19,4),
                    `OIAToOITTM` decimal(19,4),
                    `ROAexTTM` decimal(19,4),
                    `NetNonOiToTP` decimal(19,4),
                    `NetProfitRtTTM` decimal(19,4),
                    `NPToTORevTTM` decimal(19,4),
                    `OperExpRtTTM` decimal(19,4),
                    `OptProfitRtTTM` decimal(19,4),
                    `ROCTTM` decimal(19,4),
                    `ROTATTM` decimal(19,4),
                    `ROETTM` decimal(19,4),
                    `ROICTTM` decimal(19,4),
                    `OwnROETTM` decimal(19,4),
                    `SalesGrossMarginTTM` decimal(19,4),
                    `TaxRTTM` decimal(19,4),
                    `TotaProfRtTTM` decimal(19,4),
                    constraint {0}_uindex
                    unique (`trade_date`,`security_code`)
                    )ENGINE=InnoDB DEFAULT CHARSET=utf8;""".format(self._name)
        super(FactorEarning, self)._create_tables(create_sql, drop_sql)

    @staticmethod
    def historical_sgro(tp_earning, factor_earning, dependencies=['operating_revenue',
                                                                  'operating_revenue_pre_year_1',
                                                                  'operating_revenue_pre_year_2',
                                                                  'operating_revenue_pre_year_3',
                                                                  'operating_revenue_pre_year_4']):
        """
        五年营业收入增长率
        :param dependencies:
        :param tp_earning:
        :param factor_earning:
        :return:
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
    def historical_egro(tp_earning, factor_earning,
                        dependencies=['net_profit', 'net_profit_pre_year_1', 'net_profit_pre_year_2',
                                      'net_profit_pre_year_3', 'net_profit_pre_year_4']):
        """
        5年收益增长率
        :param tp_earning:
        :param factor_earning:
        :param dependencies:
        :return:
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
    def degm(ttm_earning, ttm_earning_p1y, factor_earning, dependencies=['operating_revenue', 'operating_cost']):
        """
        毛利率增长率，与去年同期相比
        :param dependencies:
        :param ttm_earning_p1y:
        :param ttm_earning:
        :param factor_earning:
        :return:
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
        dependencies.append('gross_income_ratio')
        earning = earning.drop(dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, earning, on="security_code")
        return factor_earning

    @staticmethod
    def roa5(ttm_earning_5y, factor_earning, dependencies=['net_profit', 'total_assets']):
        """
        5年资产回报率
        5年权益回报率=净利润/总资产
        :param dependencies:
        :param ttm_earning_5y:
        :param factor_earning:
        :return:
        """
        earning = ttm_earning_5y.loc[:, dependencies]
        earning['ROA5YChg'] = np.where(
            CalcTools.is_zero(earning.total_assets.values), 0,
            earning.net_profit.values / earning.total_assets.values / 4)
        earning = earning.drop(dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, earning, on="security_code")
        return factor_earning

    @staticmethod
    def roe5(ttm_earning_5y, factor_earning, dependencies=['net_profit', 'total_owner_equities']):
        """
        5年平均权益回报率
        :param dependencies:
        :param ttm_earning_5y:
        :param factor_earning:
        :return:
        """
        earning = ttm_earning_5y.loc[:, dependencies]
        earning['ROE5Y'] = np.where(
            CalcTools.is_zero(earning.total_owner_equities.values), 0,
            earning.net_profit.values / earning.total_owner_equities.values / 4)
        earning = earning.drop(dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, earning, on="security_code")
        return factor_earning

    @staticmethod
    def npcut_to_np(tp_earning, factor_earning, dependencies=['adjusted_profit', 'net_profit']):
        """
        扣除非经常损益后的净利润/净利润
        :param dependencies:
        :param tp_earning:
        :param factor_earning:
        :return:
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
    def roe_diluted(tp_earning, factor_earning,
                    dependencies=['np_parent_company_owners', 'equities_parent_company_owners']):
        """
        净资产收益率（摊薄）
        净资产收益率（摊薄）=归属于母公司的净利润/期末归属于母公司的所有者权益
        :param dependencies:
        :param tp_earning:
        :param factor_earning:
        :return:
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
    def roe_avg(tp_earning, factor_earning,
                dependencies=['np_parent_company_owners', 'equities_parent_company_owners']):
        """
        净资产收益率（平均）
        净资产收益率（平均）=归属于母公司的净利润*2/(期末归属于母公司的所有者权益
        + 期初归属于母公司的所有者权益）*100%
        :param dependencies:
        :param tp_earning:
        :param factor_earning:
        :return:
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
    def roe_cut(tp_earning, factor_earning, dependencies=['adjusted_profit', 'equities_parent_company_owners']):
        """
        净资产收益率（扣除/摊薄）
        :param dependencies:
        :param tp_earning:
        :param factor_earning:
        :return:
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
    def invest_r_associates_to_tp_latest(tp_earning, factor_earning, dependencies=['invest_income_associates', 'total_profit']):
        """
        对联营和营公司投资收益/利润总额
        :param dependencies:
        :param tp_earning:
        :param factor_earning:
        :return:
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
    def historical_egro_ttm(ttm_earning, factor_earning,
                            dependencies=['net_profit', 'net_profit_pre_year_1', 'net_profit_pre_year_2',
                                          'net_profit_pre_year_3', 'net_profit_pre_year_4']):
        """
        5年收益增长率
        :param ttm_earning:
        :param factor_earning:
        :param dependencies:
        :return:
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
    def historical_sgro_ttm(ttm_earning, factor_earning, dependencies=['operating_revenue',
                                                                       'operating_revenue_pre_year_1',
                                                                       'operating_revenue_pre_year_2',
                                                                       'operating_revenue_pre_year_3',
                                                                       'operating_revenue_pre_year_4']):
        """
        五年营业收入增长率
        :param dependencies:
        :param ttm_earning:
        :param factor_earning:
        :return:
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
    def roa(ttm_earning, factor_earning, dependencies=['net_profit', 'total_assets']):
        """
        资产回报率
        资产回报率=净利润/总资产
        :param dependencies:
        :param ttm_earning:
        :param factor_earning:
        :return:
        """
        earning = ttm_earning.loc[:, dependencies]
        earning['roa'] = np.where(
            CalcTools.is_zero(earning.total_assets.values), 0,
            earning.net_profit.values / earning.total_assets.values)
        earning = earning.drop(dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, earning, on="security_code")
        return factor_earning

    @staticmethod
    def admini_expense_rate_ttm(ttm_earning, factor_earning, dependencies=['administration_expense', 'total_operating_revenue']):
        """
        管理费用率
        # 管理费用与营业总收入之比=管理费用/营业总收入
        :param ttm_earning:
        :param factor_earning:
        :param dependencies:
        :return:
        """
        constrains = ttm_earning.loc[:, dependencies]
        constrains['AdminExpTTM'] = np.where(
            CalcTools.is_zero(constrains['total_operating_revenue']), 0,
            constrains['administration_expense'] / constrains['total_operating_revenue']
        )
        constrains = constrains.drop(dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, constrains, on="security_code")
        return factor_earning

    @staticmethod
    def berry_ratio_ttm(ttm_earning, factor_earning,
                        dependencies=['operating_revenue',
                                      'operating_cost',
                                      'financial_expense',
                                      'sale_expense',
                                      'administration_expense'
                                      ]):
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
    def cash_flow_asset_ratio_minus_return_on_asset_ttm(ttm_earning, factor_earning,
                                                        dependencies=['net_finance_cash_flow',
                                                                      'net_profit',
                                                                      'total_assets_mrq',
                                                                      'total_assets_mrq_pre']):
        """
        现金流资产比-资产回报率（TTM）
        （筹资活动现金净流量（ITM）-净利润（TTM））*2/（本期资产总计（MRQ）+上年同期产总计（MRQ））*100
        :param ttm_earning:
        :param factor_earning:
        :param dependencies:
        :return:
        """
        constrains = ttm_earning.loc[:, dependencies]
        func = lambda x: (x[0] - x[1]) * 2 / (x[2] + x[3]) if x[2] is not None and x[3] is not None and (x[2] + x[3]) !=0 else None
        constrains['CFARatioMinusROATTM'] = constrains.apply(func, axis=1)
        constrains = constrains.drop(dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, constrains, on="security_code")
        return factor_earning

    @staticmethod
    def sales_cost_ratio_ttm(ttm_earning, factor_earning, dependencies=['operating_cost', 'operating_revenue']):
        """
        销售成本率=营业成本(TTM)/营业收入(TTM)
        :param ttm_earning:
        :param factor_earning:
        :param dependencies:
        :return:
        """
        constrains = ttm_earning.loc[:, dependencies]
        constrains['SalesCostTTM'] = np.where(
            CalcTools.is_zero(constrains['operating_revenue']),
            0, constrains['operating_cost'] / constrains['operating_revenue']
        )
        constrains = constrains.drop(dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, constrains, on="security_code")
        return factor_earning

    @staticmethod
    def ebit_to_tor_ttm(ttm_earning, factor_earning, dependencies=['total_profit', 'financial_expense', 'interest_income', 'total_operating_revenue']):
        """
        息税前利润与营业总收入之比
        息税前利润与营业总收入之比=（利润总额+利息支出-利息收入)/营业总收入
        :param dependencies:
        :param ttm_earning:
        :param factor_earning:
        :return:
        """
        earning = ttm_earning.loc[:, dependencies]
        earning['EBITToTORevTTM'] = np.where(
            CalcTools.is_zero(earning.total_operating_revenue.values), 0,
            (earning.total_profit.values +
             earning.financial_expense.values -
             earning.interest_income.values)
            / earning.total_operating_revenue.values)
        earning = earning.drop(dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, earning, on="security_code")
        return factor_earning

    @staticmethod
    def period_costs_rate_ttm(ttm_earning, factor_earning, dependencies=['financial_expense', 'sale_expense', 'administration_expense', 'operating_revenue']):
        """
        销售期间费用率 = (财务费用 + 销售费用 + 管理费用) / (营业收入)
        :param ttm_earning:
        :param factor_earning:
        :param dependencies:
        :return:
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
    def financial_expense_rate_ttm(ttm_earning, factor_earning, dependencies=['financial_expense', 'total_operating_cost']):
        """
        财务费用与营业总收入之比=财务费用(TTM)/营业总收入(TTM)
        :param ttm_earning:
        :param factor_earning:
        :param dependencies:
        :return:
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
    def impairment_loss_to_total_operating_income_ttm(ttm_earning, factor_earning,
                                                      dependencies=['asset_impairment_loss', 'total_operating_revenue']):
        """
        资产减值损失/营业总收入（TTM)
        :param ttm_earning:
        :param factor_earning:
        :param dependencies:
        :return:
        """
        constrains = ttm_earning.loc[:, dependencies]
        func = lambda x: x[0] / x[1] if x[1] is not None and x[1] != 0 else None
        constrains['ImpLossToTOITTM'] = constrains.apply(func, axis=1)

        constrains = constrains.drop(columns=dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, constrains, how='outer', on="security_code")
        return factor_earning

    @staticmethod
    def shareholders_of_parent_to_operating_income_ttm(ttm_earning, factor_earning,
                                                       dependencies=['np_parent_company_owners', 'operating_revenue']):
        """
        归属母公司股东的净利润/营业收入（TTM)
        :param ttm_earning:
        :param factor_earning:
        :param dependencies:
        :return:
        """
        constrains = ttm_earning.loc[:, dependencies]
        func = lambda x: x[0] / x[1] if x[1] is not None and x[1] != 0 else None
        constrains['OIAToOITTM'] = constrains.apply(func, axis=1)

        constrains = constrains.drop(columns=dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, constrains, how='outer', on="security_code")
        return factor_earning

    @staticmethod
    def net_interest_rate_on_total_assets_ttm(ttm_earning, factor_earning,
                                              dependencies=['np_parent_company_owners', 'total_assets_mrq']):
        """
        总资产净利率TTM（不含少数股东损益）
        :param ttm_earning:
        :param factor_earning:
        :param dependencies:
        :return:
        """
        constrains = ttm_earning.loc[:, dependencies]
        func = lambda x: x[0] / x[1] if x[1] is not None and x[1] != 0 else None
        constrains['ROAexTTM'] = constrains.apply(func, axis=1)

        constrains = constrains.drop(columns=dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, constrains, how='outer', on="security_code")
        return factor_earning

    @staticmethod
    def net_non_operating_income_to_total_profit(ttm_earning, factor_earning,
                                                 dependencies=['total_operating_cost', 'total_operating_revenue']):
        """
        营业总成本/营业总收入（TTM）_PIT
        :param ttm_earning:
        :param factor_earning:
        :param dependencies:
        :return:
        """
        constrains = ttm_earning.loc[:, dependencies]
        func = lambda x: x[0] / x[1] if x[1] is not None and x[1] != 0 else None
        constrains['NetNonOiToTP'] = constrains.apply(func, axis=1)

        constrains = constrains.drop(columns=dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, constrains, how='outer', on="security_code")
        return factor_earning

    @staticmethod
    def net_profit_ratio(ttm_earning, factor_earning, dependencies=['net_profit', 'operating_revenue']):
        """
        销售净利率（Net profit ratio）
        销售净利率=净利润/营业收入
        :param dependencies:
        :param ttm_earning:
        :param factor_earning:
        :return:
        """
        earning = ttm_earning.loc[:, dependencies]
        earning['NetProfitRtTTM'] = np.where(
            CalcTools.is_zero(earning.operating_revenue.values), 0,
            earning.net_profit.values / earning.operating_revenue.values)
        earning = earning.drop(dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, earning, on="security_code")
        return factor_earning

    @staticmethod
    def np_to_tor(ttm_earning, factor_earning, dependencies=['net_profit', 'total_operating_revenue']):
        """
        净利润与营业总收入之比
        净利润与营业总收入之比=净利润/营业总收入
        :param dependencies:
        :param ttm_earning:
        :param factor_earning:
        :return:
        """
        earning = ttm_earning.loc[:, dependencies]
        earning['NPToTORevTTM'] = np.where(
            CalcTools.is_zero(earning.total_operating_revenue.values), 0,
            earning.net_profit.values / earning.total_operating_revenue.values)
        earning = earning.drop(dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, earning, on="security_code")
        return factor_earning

    @staticmethod
    def operating_expense_rate_ttm(ttm_earning, factor_earning, dependencies=['sale_expense', 'total_operating_cost', 'total_operating_revenue']):
        """
        营业费用率
        # 营业费用与营业总收入之比=销售费用(TTM)/营业总收入(TTM)
        :param ttm_earning:
        :param factor_earning:
        :param dependencies:
        :return:
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
    def operating_profit_ratio(ttm_earning, factor_earning, dependencies=['operating_profit', 'operating_revenue']):
        """
        营业净利率
        营业净利率=营业利润/营业收入
        :param dependencies:
        :param ttm_earning:
        :param factor_earning:
        :return:
        """
        earning = ttm_earning.loc[:, dependencies]
        earning['OptProfitRtTTM'] = np.where(
            CalcTools.is_zero(earning.operating_revenue.values), 0,
            earning.operating_profit.values / earning.operating_revenue.values)
        earning = earning.drop(dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, earning, on="security_code")
        return factor_earning

    @staticmethod
    def operating_profit_to_tor(ttm_earning, factor_earning, dependencies=['operating_profit', 'total_operating_revenue']):
        """
        营业利润与营业总收入之比
        营业利润与营业总收入之比=营业利润/营业总收入
        :param dependencies:
        :param ttm_earning:
        :param factor_earning:
        :return:
        """
        earning = ttm_earning.loc[:, dependencies]
        earning['operating_profit_to_tor'] = np.where(
            CalcTools.is_zero(earning.total_operating_revenue.values), 0,
            earning.operating_profit.values / earning.total_operating_revenue.values)
        earning = earning.drop(dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, earning, on="security_code")
        return factor_earning

    @staticmethod
    def rate_of_return_on_capital_ttm(ttm_earning, factor_earning,
                                      dependencies=['net_profit',
                                                    'total_owner_equities_mrq',
                                                    'longterm_loan_mrq',
                                                    'total_owner_equities_mrq_pre',
                                                    'longterm_loan_mrq_pre'
                                                    ]):
        """
        资本报酬率（TTM）
        净利润（TTM）*2/（本期股东权益（MRQ）+本期长期负债（MRQ）+上年同期股东权益（MRQ）+上年同期长期负债（MRQ））*100
        :param ttm_earning:
        :param factor_earning:
        :param dependencies:
        :return:
        """
        constrains = ttm_earning.loc[:, dependencies]
        func = lambda x: x[0] * 2 / (x[1] + x[2] + x[3] + x[4]) \
            if x[1] is not None and x[2] is not None and x[3] is not None and x[4] is not None and (x[1] + x[2] + x[3] + x[4]) !=0 else None
        constrains['ROCTTM'] = constrains.apply(func, axis=1)
        constrains = constrains.drop(dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, constrains, on="security_code")
        return factor_earning

    @staticmethod
    def roa_ebit_ttm(ttm_earning, factor_earning, dependencies=['total_profit', 'financial_expense', 'interest_income', 'total_assets']):
        """
        总资产报酬率
        ROAEBIT = EBIT*2/(期初总资产+期末总资产）
        (注，此处用过去四个季度资产均值）
        :param dependencies:
        :param ttm_earning:
        :param factor_earning:
        :return:
        """

        earning = ttm_earning.loc[:, dependencies]
        earning['ROTATTM'] = np.where(
            CalcTools.is_zero(earning.total_assets.values), 0,
            (earning.total_profit.values +
             earning.financial_expense.values -
             earning.interest_income.values)
            / earning.total_assets.values / 4)
        earning = earning.drop(dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, earning, on="security_code")
        return factor_earning

    @staticmethod
    def return_on_equity_ttm(ttm_earning, factor_earning,
                             dependencies=['np_parent_company_owners', 'equities_parent_company_owners_mrq']):
        """
        净资产收益率（TTM）
        归属于母公司的净利润（TTM）/归属于母公司的股东权益（MRQ）*100%
        :param ttm_earning:
        :param factor_earning:
        :param dependencies:
        :return:
        """
        constrains = ttm_earning.loc[:, dependencies]
        func = lambda x: x[0] / x[1] if x[1] is not None and x[1] != 0 else None
        constrains['ROETTM'] = constrains.apply(func, axis=1)

        constrains = constrains.drop(columns=dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, constrains, how='outer', on="security_code")
        return factor_earning

    @staticmethod
    def return_on_invested_capital_ttm(ttm_earning, factor_earning,
                                       dependencies=['np_parent_company_owners', 'total_assets_mrq']):
        """
        投入资本回报率(TTM)
        归属母公司股东的净利润（TTM）/全部投入资本（MRQ）*100%
        :param ttm_earning:
        :param factor_earning:
        :param dependencies:
        :return:
        """
        constrains = ttm_earning.loc[:, dependencies]
        func = lambda x: x[0] / x[1] if x[1] is not None and x[1] != 0 else None
        constrains['ROICTTM'] = constrains.apply(func, axis=1)

        constrains = constrains.drop(columns=dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, constrains, how='outer', on="security_code")
        return factor_earning

    @staticmethod
    def roe(ttm_earning, factor_earning, dependencies=['net_profit', 'total_owner_equities']):
        """
        权益回报率
        权益回报率=净利润/股东权益
        :param dependencies:
        :param ttm_earning:
        :param factor_earning:
        :return:
        """
        earning = ttm_earning.loc[:, dependencies]
        earning['OwnROETTM'] = np.where(
            CalcTools.is_zero(earning.total_owner_equities.values), 0,
            earning.net_profit.values / earning.total_owner_equities.values / 4)
        earning = earning.drop(dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, earning, on="security_code")
        return factor_earning

    @staticmethod
    def gross_income_ratio(ttm_earning, factor_earning, dependencies=['operating_revenue', 'operating_cost']):
        """
        销售毛利率
        销售毛利率=（营业收入-营业成本）/营业收入
        :param dependencies:
        :param ttm_earning:
        :param factor_earning:
        :return:
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
    def tax_ratio_ttm(ttm_earning, factor_earning, dependencies=['operating_tax_surcharges', 'operating_revenue']):
        """
        销售税金率=营业税金及附加(TTM)/营业收入(TTM)
        :param ttm_earning:
        :param factor_earning:
        :param dependencies:
        :return:
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
    def total_profit_cost_ratio_ttm(ttm_earning, factor_earning,
                                    dependencies=['total_profit',
                                                  'operating_cost',
                                                  'financial_expense',
                                                  'sale_expense',
                                                  'administration_expense']):
        """
        成本费用利润率
        成本费用利润率 = 利润总额 / (营业成本+财务费用+销售费用+管理费用）
        :param dependencies:
        :param ttm_earning:
        :param factor_earning:
        :return:
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

    @staticmethod
    def invest_r_associates_to_tp_ttm(ttm_earning, factor_earning, dependencies=['invest_income_associates', 'total_profit']):
        """
        对联营和营公司投资收益/利润总额
        :param dependencies:
        :param ttm_earning:
        :param factor_earning:
        :return:
        """

        earning = ttm_earning.loc[:, dependencies]
        earning['invest_r_associates_to_tp_ttm'] = np.where(
            CalcTools.is_zero(earning.total_profit.values), 0,
            earning.invest_income_associates.values
            / earning.total_profit.values)
        earning = earning.drop(dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, earning, on="security_code")
        return factor_earning


def calculate(trade_date, tp_earning, ttm_earning, ttm_earning_5y, factor_name):  # 计算对应因子
    tp_earning = tp_earning.set_index('security_code')
    ttm_earning = ttm_earning.set_index('security_code')
    ttm_earning_5y = ttm_earning_5y.set_index('security_code')
    earning = FactorEarning(factor_name)  # 注意, 这里的name要与client中新建table时的name一致, 不然回报错

    # 因子计算
    factor_earning = pd.DataFrame()
    factor_earning['security_code'] = tp_earning.index
    factor_earning = factor_earning.set_index('security_code')
    factor_earning = earning.roa5(ttm_earning_5y, factor_earning)
    factor_earning = earning.roe5(ttm_earning_5y, factor_earning)
    factor_earning = earning.npcut_to_np(tp_earning, factor_earning)
    factor_earning = earning.roe_diluted(tp_earning, factor_earning)
    factor_earning = earning.roe_avg(tp_earning, factor_earning)
    factor_earning = earning.roe_cut(tp_earning, factor_earning)
    # factor_earning = earning.invest_r_associates_to_tp_latest(tp_earning, factor_earning)
    factor_earning = earning.historical_egro_ttm(ttm_earning, factor_earning)
    factor_earning = earning.historical_sgro_ttm(ttm_earning, factor_earning)
    # factor_earning = earning.roa(ttm_earning, factor_earning)
    factor_earning = earning.admini_expense_rate_ttm(ttm_earning, factor_earning)
    factor_earning = earning.berry_ratio_ttm(ttm_earning, factor_earning)
    factor_earning = earning.cash_flow_asset_ratio_minus_return_on_asset_ttm(ttm_earning, factor_earning)
    factor_earning = earning.sales_cost_ratio_ttm(ttm_earning, factor_earning)
    factor_earning = earning.ebit_to_tor_ttm(ttm_earning, factor_earning)
    factor_earning = earning.period_costs_rate_ttm(ttm_earning, factor_earning)
    factor_earning = earning.financial_expense_rate_ttm(ttm_earning, factor_earning)
    factor_earning = earning.impairment_loss_to_total_operating_income_ttm(ttm_earning, factor_earning)
    factor_earning = earning.shareholders_of_parent_to_operating_income_ttm(ttm_earning, factor_earning)
    factor_earning = earning.net_interest_rate_on_total_assets_ttm(ttm_earning, factor_earning)
    factor_earning = earning.net_non_operating_income_to_total_profit(ttm_earning, factor_earning)
    factor_earning = earning.net_profit_ratio(ttm_earning, factor_earning)
    factor_earning = earning.np_to_tor(ttm_earning, factor_earning)
    factor_earning = earning.operating_expense_rate_ttm(ttm_earning, factor_earning)
    factor_earning = earning.operating_profit_ratio(ttm_earning, factor_earning)
    # factor_earning = earning.operating_profit_to_tor(ttm_earning, factor_earning)
    factor_earning = earning.rate_of_return_on_capital_ttm(ttm_earning, factor_earning)
    factor_earning = earning.roa_ebit_ttm(ttm_earning, factor_earning)
    factor_earning = earning.return_on_equity_ttm(ttm_earning, factor_earning)
    factor_earning = earning.return_on_invested_capital_ttm(ttm_earning, factor_earning)
    factor_earning = earning.roe(ttm_earning, factor_earning)
    factor_earning = earning.gross_income_ratio(ttm_earning, factor_earning)
    factor_earning = earning.tax_ratio_ttm(ttm_earning, factor_earning)
    factor_earning = earning.total_profit_cost_ratio_ttm(ttm_earning, factor_earning)
    # factor_earning = earning.invest_r_associates_to_tp_ttm(ttm_earning, factor_earning)
    factor_earning = factor_earning.reset_index()
    # factor_earning['id'] = factor_earning['security_code'] + str(trade_date)
    factor_earning['trade_date'] = str(trade_date)
    print(factor_earning.head())
    earning._storage_data(factor_earning, trade_date)
    del earning
    gc.collect()


# @app.task()
def factor_calculate(**kwargs):
    print("constrain_kwargs: {}".format(kwargs))
    date_index = kwargs['date_index']
    session = kwargs['session']
    factor_name = kwargs['factor_name']
    content1 = cache_data.get_cache(session + str(date_index) + "1", date_index)
    content2 = cache_data.get_cache(session + str(date_index) + "2", date_index)
    content3 = cache_data.get_cache(session + str(date_index) + "3", date_index)
    print("len_con1: %s" % len(content1))
    print("len_con2: %s" % len(content2))
    print("len_con3: %s" % len(content3))
    tp_earning = json_normalize(json.loads(str(content1, encoding='utf8')))
    ttm_earning_5y = json_normalize(json.loads(str(content2, encoding='utf8')))
    ttm_earning = json_normalize(json.loads(str(content3, encoding='utf8')))
    # cache_date.get_cache使得index的名字丢失， 所以数据需要按照下面的方式设置index
    tp_earning.set_index('security_code', inplace=True)
    ttm_earning.set_index('security_code', inplace=True)
    ttm_earning_5y.set_index('security_code', inplace=True)
    # total_earning_data = {'tp_earning': tp_earning, 'ttm_earning_5y': ttm_earning_5y, 'ttm_earning': ttm_earning}
    calculate(date_index, tp_earning, ttm_earning, ttm_earning_5y, factor_name)

