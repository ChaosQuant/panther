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

import json
import numpy as np
import pandas as pd
from sklearn import linear_model
from factor.factor_base import FactorBase
from factor.utillities.calc_tools import CalcTools
from pandas.io.json import json_normalize

# from factor import app
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
                    `id` varchar(32) NOT NULL,
                    `symbol` varchar(24) NOT NULL,
                    `trade_date` date NOT NULL,
                    `net_profit_ratio` decimal(19,4),
                    `operating_profit_ratio` decimal(19,4),
                    `np_to_tor` decimal(19,4),
                    `operating_profit_to_tor` decimal(19,4),
                    `gross_income_ratio`  decimal(19,4),
                    `ebit_to_tor` decimal(19,4),
                    `roa` decimal(19,4),
                    `roa5` decimal(19,4),
                    `roe` decimal(19,4),
                    `roe5` decimal(19,4),
                    `roe_diluted` decimal(19,4),
                    `roe_avg` decimal(19,4),
                    `roe_cut` decimal(19,4),
                    `roa_ebit_ttm` decimal(19,4),
                    `operating_ni_to_tp_ttm` decimal(19,4),
                    `operating_ni_to_tp_latest` decimal(19,4),
                    `invest_r_associates_to_tp_ttm` decimal(19,4),
                    `invest_r_associates_to_tp_latest` decimal(19,4),
                    `npcut_to_np` decimal(19,4),
                    `interest_cover_ttm` decimal(19,4),
                    `net_non_oi_to_tp_ttm` decimal(19,4),
                    `net_non_oi_to_tp_latest` decimal(19,4),
                    PRIMARY KEY(`id`,`trade_date`,`symbol`)
                    )ENGINE=InnoDB DEFAULT CHARSET=utf8;""".format(self._name)
        super(FactorEarning, self)._create_tables(create_sql, drop_sql)

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

        # fun = lambda x: (regr.coef_[-1] if regr.fit(x[['net_profit', 'net_profit_pre_year_1', 'net_profit_pre_year_2',
        #                                                'net_profit_pre_year_3', 'net_profit_pre_year_4']].values.reshape(-1, 1),
        #                                             range(0, 5)) else None)

        historical_growth['coefficient'] = historical_growth.apply(fun2, axis=1)
        historical_growth['mean'] = historical_growth[dependencies].fillna('nan').mean(axis=1)

        fun1 = lambda x: x[0] / abs(x[1]) if x[1] != 0 and x[1] is not None and x[0] is not None else None
        historical_growth['NetPft5YAvgChgTTM'] = historical_growth[['coefficient', 'mean']].apply(fun1, axis=1)

        # historical_growth = historical_growth.drop(
        #     columns=['net_profit', 'net_profit_pre_year_1', 'net_profit_pre_year_2', 'net_profit_pre_year_3',
        #              'net_profit_pre_year_4', 'coefficient', 'mean'], axis=1)

        historical_growth = historical_growth[['symbol', 'NetPft5YAvgChgTTM']]
        factor_earning = pd.merge(factor_earning, historical_growth, on='symbol')

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

        # fun = lambda x: (regr.coef_[-1] if regr.fit(x[['net_profit', 'net_profit_pre_year_1', 'net_profit_pre_year_2',
        #                                                'net_profit_pre_year_3', 'net_profit_pre_year_4']].values.reshape(-1, 1),
        #                                             range(0, 5)) else None)

        historical_growth['coefficient'] = historical_growth.apply(fun2, axis=1)
        historical_growth['mean'] = historical_growth[dependencies].fillna('nan').mean(axis=1)

        fun1 = lambda x: x[0] / abs(x[1]) if x[1] != 0 and x[1] is not None and x[0] is not None else None
        historical_growth['NetPft5YAvgChgTTM'] = historical_growth[['coefficient', 'mean']].apply(fun1, axis=1)

        # historical_growth = historical_growth.drop(
        #     columns=['net_profit', 'net_profit_pre_year_1', 'net_profit_pre_year_2', 'net_profit_pre_year_3',
        #              'net_profit_pre_year_4', 'coefficient', 'mean'], axis=1)

        historical_growth = historical_growth[['symbol', 'NetPft5YAvgChgTTM']]
        factor_earning = pd.merge(factor_earning, historical_growth, on='symbol')

        return factor_earning

    @staticmethod
    def historical_sgro(tp_earning, factor_earning, dependencies=['operating_revenue', 'operating_revenue_pre_year_1',
                                                                                      'operating_revenue_pre_year_2', 'operating_revenue_pre_year_3', 'operating_revenue_pre_year_4']):
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
        historical_growth['mean'] = historical_growth[dependencies].fillna(0.0).mean(axis=1)

        fun1 = lambda x: x[0] / abs(x[1]) if x[1] is not None and x[0] is not None and x[1] != 0 else None
        historical_growth['Sales5YChgTTM'] = historical_growth[['coefficient', 'mean']].apply(fun1, axis=1)

        # historical_growth = historical_growth.drop(
        #     columns=['operating_revenue', 'operating_revenue_pre_year_1', 'operating_revenue_pre_year_2',
        #              'operating_revenue_pre_year_3', 'operating_revenue_pre_year_4', 'coefficient', 'mean'], axis=1)
        historical_growth = historical_growth[['Sales5YChgTTM']]
        factor_earning = pd.merge(factor_earning, historical_growth, on='symbol')

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
        factor_earning = pd.merge(factor_earning, earning, on="symbol")
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
        earning['roa5'] = np.where(
            CalcTools.is_zero(earning.total_assets.values), 0,
            earning.net_profit.values / earning.total_assets.values / 4)
        earning = earning.drop(dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, earning, on="symbol")
        return factor_earning

    @staticmethod
    def roe5(ttm_earning_5y, factor_earning, dependencies=['net_profit', 'total_owner_equities']):
        """
        5年权益回报率
        :param dependencies:
        :param ttm_earning_5y:
        :param factor_earning:
        :return:
        """
        earning = ttm_earning_5y.loc[:, dependencies]
        earning['roe5'] = np.where(
            CalcTools.is_zero(earning.total_owner_equities.values), 0,
            earning.net_profit.values / earning.total_owner_equities.values / 4)
        earning = earning.drop(dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, earning, on="symbol")
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

        contrarian = ttm_earning.loc[:, dependencies]
        contrarian['AdminExpTTM'] = np.where(
            CalcTools.is_zero(contrarian['total_operating_revenue']), 0,
            contrarian['administration_expense'] / contrarian['total_operating_revenue']
        )
        contrarian = contrarian.drop(dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, contrarian, on="symbol")
        return factor_earning

    @staticmethod
    def sales_cost_ratio_ttm(ttm_earning, factor_earning, dependencies=['operating_cost', 'operating_revenue']):
        """
        # 销售成本率=营业成本(TTM)/营业收入(TTM)
        :param ttm_earning:
        :param factor_earning:
        :param dependencies:
        :return:
        """

        contrarian = ttm_earning.loc[:, dependencies]
        contrarian['SalesCostTTM'] = np.where(
            CalcTools.is_zero(contrarian['operating_revenue']),
            0, contrarian['operating_cost'] / contrarian['operating_revenue']
        )
        contrarian = contrarian.drop(dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, contrarian, on="symbol")
        return factor_earning

    @staticmethod
    def ebit_to_tor(ttm_earning, factor_earning, dependencies=['total_profit', 'financial_expense', 'interest_income', 'total_operating_revenue']):
        """
        息税前利润与营业总收入之比
        息税前利润与营业总收入之比=（利润总额+利息支出-利息收入)/营业总收入
        :param dependencies:
        :param ttm_earning:
        :param factor_earning:
        :return:
        """
        earning = ttm_earning.loc[:, dependencies]
        earning['ebit_to_tor'] = np.where(
            CalcTools.is_zero(earning.total_operating_revenue.values), 0,
            (earning.total_profit.values +
             earning.financial_expense.values -
             earning.interest_income.values)
            / earning.total_operating_revenue.values)
        earning = earning.drop(dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, earning, on="symbol")
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
        contrarian = ttm_earning.loc[:, dependencies]
        contrarian['PeridCostTTM'] = np.where(
            CalcTools.is_zero(contrarian['operating_revenue']), 0,
            (contrarian['financial_expense'] + contrarian['sale_expense'] + contrarian['administration_expense']) / \
            contrarian['operating_revenue']
        )
        contrarian = contrarian.drop(dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, contrarian, on="symbol")
        return factor_earning

    @staticmethod
    def financial_expense_rate_ttm(ttm_earning, factor_earning, dependencies=['financial_expense', 'total_operating_cost']):
        """
        # 财务费用与营业总收入之比=财务费用(TTM)/营业总收入(TTM)
        :param ttm_earning:
        :param factor_earning:
        :param dependencies:
        :return:
        """
        contrarian = ttm_earning.loc[:, dependencies]
        contrarian['FinExpTTM'] = np.where(
            CalcTools.is_zero(contrarian['total_operating_cost']), 0,
            contrarian['financial_expense'] / contrarian['total_operating_cost']
        )
        contrarian = contrarian.drop(dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, contrarian, on="symbol")
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
        earning['npcut_to_np'] = np.where(
            CalcTools.is_zero(earning.net_profit.values), 0,
            earning.adjusted_profit.values
            / earning.net_profit.values)
        earning = earning.drop(dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, earning, on="symbol")
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
        earning['net_profit_ratio'] = np.where(
            CalcTools.is_zero(earning.operating_revenue.values), 0,
            earning.net_profit.values / earning.operating_revenue.values)
        earning = earning.drop(dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, earning, on="symbol")
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
        earning['np_to_tor'] = np.where(
            CalcTools.is_zero(earning.total_operating_revenue.values), 0,
            earning.net_profit.values / earning.total_operating_revenue.values)
        earning = earning.drop(dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, earning, on="symbol")
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
        contrarian = ttm_earning.loc[:, dependencies]
        contrarian['OperExpRtTTM'] = np.where(
            CalcTools.is_zero(contrarian['total_operating_cost']), 0,
            contrarian['sale_expense'] / contrarian['total_operating_revenue']
        )
        contrarian = contrarian.drop(dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, contrarian, on="symbol")
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
        earning['operating_profit_ratio'] = np.where(
            CalcTools.is_zero(earning.operating_revenue.values), 0,
            earning.operating_profit.values / earning.operating_revenue.values)
        earning = earning.drop(dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, earning, on="symbol")
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
        factor_earning = pd.merge(factor_earning, earning, on="symbol")
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
        earning['roa_ebit_ttm'] = np.where(
            CalcTools.is_zero(earning.total_assets.values), 0,
            (earning.total_profit.values +
             earning.financial_expense.values -
             earning.interest_income.values)
            / earning.total_assets.values / 4)
        earning = earning.drop(dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, earning, on="symbol")
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
        earning['roe_diluted'] = np.where(
            CalcTools.is_zero(earning.equities_parent_company_owners.values), 0,
            earning.np_parent_company_owners.values /
            earning.equities_parent_company_owners.values / 4)
        earning = earning.drop(dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, earning, on="symbol")
        return factor_earning

    @staticmethod
    def roe_avg(ttm_earning, factor_earning,
                dependencies=['np_parent_company_owners', 'equities_parent_company_owners']):
        """
        净资产收益率（平均）
        净资产收益率（平均）=归属于母公司的净利润*2/(期末归属于母公司的所有者权益
        + 期初归属于母公司的所有者权益）*100%
        :param dependencies:
        :param ttm_earning:
        :param factor_earning:
        :return:
        """

        earning = ttm_earning.loc[:, dependencies]
        earning['roe_avg'] = np.where(
            CalcTools.is_zero(earning.equities_parent_company_owners.values), 0,
            earning.np_parent_company_owners.values /
            earning.equities_parent_company_owners.values / 4)
        earning = earning.drop(dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, earning, on="symbol")
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
        earning['roe_cut'] = np.where(
            CalcTools.is_zero(earning.equities_parent_company_owners.values), 0,
            earning.adjusted_profit.values /
            earning.equities_parent_company_owners.values / 4)
        earning = earning.drop(dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, earning, on="symbol")
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
        earning['roe'] = np.where(
            CalcTools.is_zero(earning.total_owner_equities.values), 0,
            earning.net_profit.values / earning.total_owner_equities.values / 4)
        earning = earning.drop(dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, earning, on="symbol")
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
        earning['gross_income_ratio'] = np.where(
            CalcTools.is_zero(earning.operating_revenue.values), 0,
            (earning.operating_revenue.values - earning.operating_cost.values)
            / earning.operating_revenue.values)
        earning = earning.drop(dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, earning, on="symbol")
        return factor_earning

    @staticmethod
    def tax_ratio_ttm(ttm_earning, factor_earning, dependencies=['operating_tax_surcharges', 'operating_revenue']):
        """
        # 销售税金率=营业税金及附加(TTM)/营业收入(TTM)
        :param ttm_earning:
        :param factor_earning:
        :param dependencies:
        :return:
        """

        contrarian = ttm_earning.loc[:, dependencies]
        contrarian['TaxRTTM'] = np.where(
            CalcTools.is_zero(contrarian['operating_revenue']), 0,
            contrarian['operating_tax_surcharges'] / contrarian['operating_revenue']
        )
        contrarian = contrarian.drop(dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, contrarian, on="symbol")
        return factor_earning

    @staticmethod
    def total_profit_cost_ratio_ttm(ttm_management, factor_management,
                                    dependencies=['total_profit', 'operating_cost', 'financial_expense', 'sale_expense',
                                                  'administration_expense']):
        """
        成本费用利润率
        成本费用利润率 = 利润总额 / (营业成本+财务费用+销售费用+管理费用）
        :param dependencies:
        :param ttm_management:
        :param factor_management:
        :return:
        """

        management = ttm_management.loc[:, dependencies]
        management['cost'] = (management.operating_cost +
                              management.financial_expense +
                              management.sale_expense +
                              management.administration_expense)
        management['total_profit_cost_ratio_ttm'] = np.where(
            CalcTools.is_zero(management.cost.values), 0,
            management.total_profit.values / management.cost.values)
        dependencies.append('cost')
        management = management.drop(dependencies, axis=1)
        factor_management = pd.merge(factor_management, management, on="symbol")
        return factor_management

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
        factor_earning = pd.merge(factor_earning, earning, on="symbol")
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
        factor_earning = pd.merge(factor_earning, earning, on="symbol")
        return factor_earning


def calculate(trade_date, earning_sets_dic, earning):  # 计算对应因子
    print(trade_date)
    tp_earning = earning_sets_dic['tp_earning']
    ttm_earning = earning_sets_dic['ttm_earning']
    ttm_earning_5y = earning_sets_dic['ttm_earning_5y']

    # 因子计算
    factor_earning = pd.DataFrame()
    factor_earning['symbol'] = tp_earning.index

    factor_earning = earning.net_profit_ratio(ttm_earning, factor_earning)
    factor_earning = earning.operating_profit_ratio(ttm_earning, factor_earning)
    factor_earning = earning.np_to_tor(ttm_earning, factor_earning)
    factor_earning = earning.operating_profit_to_tor(ttm_earning, factor_earning)
    factor_earning = earning.gross_income_ratio(ttm_earning, factor_earning)
    factor_earning = earning.ebit_to_tor(ttm_earning, factor_earning)
    factor_earning = earning.roa(ttm_earning, factor_earning)
    factor_earning = earning.roa5(ttm_earning_5y, factor_earning)
    factor_earning = earning.roe(ttm_earning, factor_earning)
    factor_earning = earning.roe5(ttm_earning_5y, factor_earning)
    factor_earning = earning.roe_diluted(tp_earning, factor_earning)
    factor_earning = earning.roe_avg(ttm_earning, factor_earning)
    # factor_earning = self.roe_weighted()
    factor_earning = earning.roe_cut(tp_earning, factor_earning)
    # factor_earning = self.roe_cut_weighted()
    # factor_earning = self.roic()
    # factor_earning = self.roa_ebit()
    factor_earning = earning.roa_ebit_ttm(ttm_earning, factor_earning)
    factor_earning = earning.operating_ni_to_tp_ttm(ttm_earning, factor_earning)
    factor_earning = earning.operating_ni_to_tp_latest(tp_earning, factor_earning)
    factor_earning = earning.invest_r_associates_to_tp_ttm(ttm_earning, factor_earning)
    factor_earning = earning.invest_r_associates_to_tp_latest(tp_earning, factor_earning)
    factor_earning = earning.npcut_to_np(tp_earning, factor_earning)
    factor_earning = earning.interest_cover_ttm(ttm_earning, factor_earning)
    # factor_earning = self.degm(ttm_earning, ttm_earning_p1y, factor_earning)
    factor_earning = earning.net_non_oi_to_tp_ttm(ttm_earning, factor_earning)
    factor_earning = earning.net_non_oi_to_tp_latest(tp_earning, factor_earning)

    factor_earning['id'] = factor_earning['symbol'] + str(trade_date)
    factor_earning['trade_date'] = str(trade_date)
    earning._storage_data(factor_earning, trade_date)


# @app.task()
def factor_calculate(**kwargs):
    print("constrain_kwargs: {}".format(kwargs))
    date_index = kwargs['date_index']
    session = kwargs['session']
    earning = FactorEarning('factor_earning')  # 注意, 这里的name要与client中新建table时的name一致, 不然回报错
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
    tp_earning.set_index('symbol', inplace=True)
    ttm_earning.set_index('symbol', inplace=True)
    ttm_earning_5y.set_index('symbol', inplace=True)
    total_earning_data = {'tp_earning': tp_earning, 'ttm_earning_5y': ttm_earning_5y, 'ttm_earning': ttm_earning}
    calculate(date_index, total_earning_data, earning)

