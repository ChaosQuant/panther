#! /usr/bin/env python
# -*- coding: utf-8 -*-

"""
@version:
@author: Wang
@file: factor_management.py
@time: 2019-05-30
"""
import sys
sys.path.append('../')
sys.path.append('../../')
sys.path.append('../../../')
import json
import numpy as np
import pandas as pd
from factor.ttm_fundamental import *
from factor.factor_base import FactorBase
from vision.fm.signletion_engine import *
from pandas.io.json import json_normalize
from factor.utillities.calc_tools import CalcTools

from factor import app
from ultron.cluster.invoke.cache_data import cache_data


class FactorManagement(FactorBase):
    def __init__(self, name):
        super(FactorManagement, self).__init__(name)

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
                    `current_ratio` decimal(19,4),
                    `quick_ratio` decimal(19,4),
                    `long_debt_to_working_capital` decimal(19,4),
                    `equity_fixed_asset_ratio` decimal(19,4),
                    `long_debt_to_asset` decimal(19,4),
                    `long_term_debt_to_asset` decimal(19,4),
                    `current_assets_ratio` decimal(19,4),
                    `fix_asset_ratio` decimal(19,4),
                    `blev` decimal(19,4),
                    `mlev` decimal(19,4),
                    `super_quick_ratio` decimal(19,4),
                    `tsep_to_interest_bear_debt` decimal(19,4),
                    `tsep_to_total_capital` decimal(19,4),
                    `inventory_t_rate_ttm` decimal(19,4),
                    `inventory_t_days_ttm` decimal(19,4),
                    `ar_t_rate_ttm` decimal(19,4),
                    `ar_t_days_ttm` decimal(19,4),
                    `accounts_payables_t_rate_ttm` decimal(19,4),
                    `accounts_payables_t_days_ttm` decimal(19,4),
                    `current_assets_t_rate_ttm` decimal(19,4),
                    `fixed_assets_t_rate_ttm` decimal(19,4),
                    `equity_t_rate_ttm` decimal(19,4),
                    `total_assets_t_rate_ttm` decimal(19,4),
                    `cash_conversion_cycle` decimal(19,4),
                    `operating_cycle` decimal(19,4),
                    `equity_to_asset` decimal(19,4),
                    `total_profit_cost_ratio_ttm` decimal(19,4),
                    `bonds_payable_to_asset` decimal(19,4),
                    `non_current_assets_ratio` decimal(19,4),
                    `intangible_asset_ratio` decimal(19,4),
                    `tangible_a_to_inte_bear_debt` decimal(19,4),
                    `tangible_a_to_net_debt` decimal(19,4),
                    PRIMARY KEY(`id`,`trade_date`,`symbol`)
                    )ENGINE=InnoDB DEFAULT CHARSET=utf8;""".format(self._name)
        super(FactorManagement, self)._create_tables(create_sql, drop_sql)

    @staticmethod
    def current_ratio(tp_management, factor_management, dependencies=['total_current_assets', 'total_current_liability']):
        """
        流动比率
        流动比率 = 流动资产合计/流动负债合计
        :param dependencies:
        :param tp_management:
        :param factor_management:
        :return:
        """

        management = tp_management.loc[:, dependencies]
        management['current_ratio'] = np.where(
            CalcTools.is_zero(management.total_current_liability.values), 0,
            management.total_current_assets.values / management.total_current_liability.values)
        management = management.drop(dependencies, axis=1)
        factor_management = pd.merge(factor_management, management, on="symbol")
        return factor_management

    @staticmethod
    def quick_ratio(tp_management, factor_management, dependencies=['total_current_assets', 'total_current_liability', 'inventories']):
        """
        速动比率
        速动比率 = （流动资产合计-存货）/流动负债合计
        :param dependencies:
        :param tp_management:
        :param factor_management:
        :return:
        """

        management = tp_management.loc[:, dependencies]
        management['quick_ratio'] = np.where(
            CalcTools.is_zero(management.total_current_liability.values), 0,
            (management.total_current_assets.values - management.inventories.values)
            / management.total_current_liability.values)
        management = management.drop(dependencies, axis=1)
        factor_management = pd.merge(factor_management, management, on="symbol")
        return factor_management

    @staticmethod
    def long_debt_to_working_capital(tp_management, factor_management, dependencies=['total_current_assets', 'total_current_liability', 'total_non_current_assets']):
        """
        长期负债与营运资金比率
        长期负债与营运资金比率 = 非流动负债合计/（流动资产合计-流动负债合计）
        :param dependencies:
        :param tp_management:
        :param factor_management:
        :return:
        """

        management = tp_management.loc[:, dependencies]
        management['long_debt_to_working_capital'] = np.where(
            CalcTools.is_zero(management.total_current_assets.values - management.total_current_liability.values), 0,
            management.total_non_current_assets.values
            / (management.total_current_assets.values - management.total_current_liability.values))
        management = management.drop(dependencies, axis=1)
        factor_management = pd.merge(factor_management, management, on="symbol")
        return factor_management

    @staticmethod
    def equity_fixed_asset_ratio(tp_management, factor_management, dependencies=['total_owner_equities', 'fixed_assets', 'construction_materials', 'constru_in_process']):
        """
        股东权益与固定资产比率
        股东权益与固定资产比率 = 股东权益/（固定资产+工程物资+在建工程）
        :param dependencies:
        :param tp_management:
        :param factor_management:
        :return:
        """

        management = tp_management.loc[:, dependencies]
        management['equity_fixed_asset_ratio'] = np.where(
            CalcTools.is_zero(management.fixed_assets.values +
                              management.construction_materials.values +
                              management.constru_in_process.values), 0,
            management.total_owner_equities.values
            / (management.fixed_assets.values
               + management.construction_materials.values
               + management.constru_in_process.values))
        management = management.drop(dependencies, axis=1)
        factor_management = pd.merge(factor_management, management, on="symbol")
        return factor_management

    @staticmethod
    def long_debt_to_asset(tp_management, factor_management, dependencies=['longterm_loan', 'total_assets']):
        """
        长期借款与资产总计之比
        长期借款与资产总计之比 = 长期借款/总资产
        :param dependencies:
        :param tp_management:
        :param factor_management:
        :return:
        """

        management = tp_management.loc[:, dependencies]
        management['long_debt_to_asset'] = np.where(
            CalcTools.is_zero(management.total_assets.values), 0,
            management.longterm_loan.values / management.total_assets.values)
        management = management.drop(dependencies, axis=1)
        factor_management = pd.merge(factor_management, management, on="symbol")
        return factor_management

    @staticmethod
    def long_term_debt_to_asset(tp_management, factor_management, dependencies=['total_non_current_liability', 'total_assets']):
        """
        长期负债与资产总计之比
        长期负债与资产总计之比 = 非流动性负债合计/总资产
        :param dependencies:
        :param tp_management:
        :param factor_management:
        :return:
        """

        management = tp_management.loc[:, dependencies]
        management['long_term_debt_to_asset'] = np.where(
            CalcTools.is_zero(management.total_assets.values), 0,
            management.total_non_current_liability.values / management.total_assets.values)
        management = management.drop(dependencies, axis=1)
        factor_management = pd.merge(factor_management, management, on="symbol")
        return factor_management

    @staticmethod
    def current_assets_ratio(tp_management, factor_management, dependencies=['total_current_assets', 'total_assets']):
        """
        流动资产比率
        流动资产比率 = 流动资产合计/总资产
        :param dependencies:
        :param tp_management:
        :param factor_management:
        :return:
        """

        management = tp_management.loc[:, dependencies]
        management['current_assets_ratio'] = np.where(
            CalcTools.is_zero(management.total_assets.values), 0,
            management.total_current_assets.values / management.total_assets.values)
        management = management.drop(dependencies, axis=1)
        factor_management = pd.merge(factor_management, management, on="symbol")
        return factor_management

    @staticmethod
    def fix_asset_ratio(tp_management, factor_management, dependencies=['fixed_assets', 'construction_materials', 'constru_in_process', 'total_assets']):
        """
        固定资产比率
        固定资产比率 = （固定资产+工程物资+在建工程）/总资产
        :param dependencies:
        :param tp_management:
        :param factor_management:
        :return:
        """

        management = tp_management.loc[:, dependencies]
        management['fix_asset_ratio'] = np.where(
            CalcTools.is_zero(management.total_assets.values), 0,
            (management.fixed_assets.values +
             management.construction_materials.values +
             management.constru_in_process.values) / management.total_assets.values)
        management = management.drop(dependencies, axis=1)
        factor_management = pd.merge(factor_management, management, on="symbol")
        return factor_management

    @staticmethod
    def blev(tp_management, factor_management, dependencies=['total_non_current_liability', 'total_assets']):
        """
        账面杠杆
        账面杠杆 = 非流动负债合计/股东权益
        :param dependencies:
        :param tp_management:
        :param factor_management:
        :return:
        """

        management = tp_management.loc[:, dependencies]
        management['blev'] = np.where(
            CalcTools.is_zero(management.total_assets.values), 0,
            management.total_non_current_liability.values / management.total_assets.values)
        management = management.drop(dependencies, axis=1)
        factor_management = pd.merge(factor_management, management, on="symbol")
        return factor_management

    @staticmethod
    def mlev(tp_management, factor_management, dependencies=['total_non_current_liability', 'market_cap']):
        """
        市场杠杆
        市场杠杆 = 非流动负债合计/（非流动负债合计+总市值）
        :param dependencies:
        :param tp_management:
        :param factor_management:
        :return:
        """

        management = tp_management.loc[:, dependencies]
        management['mlev'] = np.where(
            CalcTools.is_zero(management.market_cap.values), 0,
            management.total_non_current_liability.values /
            (management.total_non_current_liability.values + management.market_cap.values))
        management = management.drop(dependencies, axis=1)
        factor_management = pd.merge(factor_management, management, on="symbol")
        return factor_management

    @staticmethod
    def super_quick_ratio(tp_management, factor_management, dependencies=['cash_equivalents', 'trading_assets',
                                                                          'bill_receivable', 'account_receivable',
                                                                          'other_receivable', 'total_current_liability']):
        """
        超速动比率
        超速动比率 = （货币资金+交易性金融资产+应收票据+应收账款+其他应收款）/流动负债合计
        :param tp_management:
        :param factor_management:
        :return:
        """

        management = tp_management.loc[:, dependencies]
        management['super_quick_ratio'] = np.where(
            CalcTools.is_zero(management.total_current_liability.values), 0,
            (management.cash_equivalents.values +
             management.trading_assets.values +
             management.bill_receivable.values +
             management.account_receivable.values +
             management.other_receivable.values) /
            management.total_current_liability.values)
        management = management.drop(dependencies, axis=1)
        factor_management = pd.merge(factor_management, management, on="symbol")
        return factor_management

    @staticmethod
    def tsep_to_interest_bear_debt(tp_management, factor_management, dependencies=['equities_parent_company_owners', 'shortterm_loan',
                                                                                   'non_current_liability_in_one_year', 'longterm_loan',
                                                                                   'bonds_payable', 'interest_payable']):
        """
        归属母公司股东的权益/带息负债
        （补充 带息负债 = 短期借款+一年内到期的长期负债+长期借款+应付债券+应付利息）
        :param tp_management:
        :param factor_management:
        :return:
        """

        management = tp_management.loc[:, dependencies]
        management["debt"] = (management.shortterm_loan +
                              management.non_current_liability_in_one_year +
                              management.longterm_loan +
                              management.bonds_payable +
                              management.interest_payable)
        management['tsep_to_interest_bear_debt'] = np.where(
            CalcTools.is_zero(management.debt.values), 0,
            management.equities_parent_company_owners.values / management.debt.values)
        dependencies.append('debt')
        management = management.drop(dependencies, axis=1)
        factor_management = pd.merge(factor_management, management, on="symbol")
        return factor_management

    @staticmethod
    def tsep_to_total_capital(tp_management, factor_management, dependencies=['equities_parent_company_owners', 'total_owner_equities',
                                                                              'shortterm_loan', 'non_current_liability_in_one_year',
                                                                              'longterm_loan', 'bonds_payable', 'interest_payable']):
        """
        归属母公司股东的权益/全部投入资本
        (补充 全部投入资本=所有者权益合计+带息债务）
        :param tp_management:
        :param factor_management:
        :return:
        """

        management = tp_management.loc[:, dependencies]
        management["tc"] = (management.total_owner_equities
                            + management.shortterm_loan
                            + management.non_current_liability_in_one_year
                            + management.longterm_loan
                            + management.bonds_payable
                            + management.interest_payable
                            )
        management['tsep_to_total_capital'] = np.where(
            CalcTools.is_zero(management.tc.values), 0,
            management.equities_parent_company_owners.values / management.tc.values)
        dependencies.append('tc')
        management = management.drop(dependencies, axis=1)
        factor_management = pd.merge(factor_management, management, on="symbol")
        return factor_management

    @staticmethod
    def inventory_t_rate_ttm(ttm_management, factor_management, dependencies=['operating_cost', 'inventories']):
        """
        存货周转率
        存货周转率 = 营业成本/存货
        (补充，此处存货为过去4期的均值）
        :param dependencies:
        :param ttm_management:
        :param factor_management:
        :return:
        """

        management = ttm_management.loc[:, dependencies]
        management['inventory_t_rate_ttm'] = np.where(
            CalcTools.is_zero(management.inventories.values), 0,
            management.operating_cost.values / management.inventories.values * 4)
        management = management.drop(dependencies, axis=1)
        factor_management = pd.merge(factor_management, management, on="symbol")
        return factor_management

    @staticmethod
    def inventory_t_days_ttm(ttm_management, factor_management, dependencies=['operating_cost', 'inventories']):
        """
        存货周转天数
        存货周转天数 = 360/存货周转率
        :param dependencies:
        :param ttm_management:
        :param factor_management:
        :return:
        """

        management = ttm_management.loc[:, dependencies]
        management['inventory_t_days_ttm'] = np.where(
            CalcTools.is_zero(management.operating_cost.values), 0,
            360 / management.operating_cost.values * management.inventories.values / 4)
        management = management.drop(dependencies, axis=1)
        factor_management = pd.merge(factor_management, management, on="symbol")
        return factor_management

    @staticmethod
    def ar_t_rate_ttm(ttm_management, factor_management, dependencies=['operating_revenue', 'account_receivable',
                                                                       'bill_receivable', 'advance_peceipts']):
        """
        应收账款周转率
        应收账款周转率 = 营业收入/（应收账款+应收票据-预收账款）
        :param ttm_management:
        :param factor_management:
        :return:
        """

        management = ttm_management.loc[:, dependencies]
        management['ar'] = (management.account_receivable
                            + management.bill_receivable
                            - management.advance_peceipts) / 4
        management['ar_t_rate_ttm'] = np.where(
            CalcTools.is_zero(management.ar.values), 0,
            management.operating_revenue.values / management.ar.values)
        dependencies.append('ar')
        management = management.drop(dependencies, axis=1)
        factor_management = pd.merge(factor_management, management, on="symbol")
        return factor_management

    @staticmethod
    def ar_t_days_ttm(ttm_management, factor_management, dependencies=['operating_revenue', 'bill_receivable',
                                                                       'account_receivable', 'advance_peceipts']):
        """
        应收账款周转天数
        应收账款周转率 = 360/应收账款周转率
        :param dependencies:
        :param ttm_management:
        :param factor_management:
        :return:
        """

        management = ttm_management.loc[:, dependencies]
        management['ar'] = (management.account_receivable
                            + management.bill_receivable
                            - management.advance_peceipts) / 4
        management['ar_t_days_ttm'] = np.where(
            CalcTools.is_zero(management.operating_revenue.values), 0,
            360 / management.operating_revenue.values * management.ar.values)
        dependencies.append('ar')
        management = management.drop(dependencies, axis=1)
        factor_management = pd.merge(factor_management, management, on="symbol")
        return factor_management

    @staticmethod
    def accounts_payables_t_rate_ttm(ttm_management, factor_management, dependencies=['operating_cost', 'accounts_payable',
                                                                                      'notes_payable', 'advance_payment']):
        """
        应付账款周转率
        应付账款周转率 = 营业成本/（应付账款+应付票据-预付账款）
        :param dependencies:
        :param ttm_management:
        :param factor_management:
        :return:
        """

        management = ttm_management.loc[:, dependencies]
        management['ap'] = (management.accounts_payable
                            + management.notes_payable
                            - management.advance_payment) / 4
        management['accounts_payables_t_rate_ttm'] = np.where(
            CalcTools.is_zero(management.ap.values), 0,
            management.operating_cost.values / management.ap.values)
        dependencies.append('ap')
        management = management.drop(dependencies, axis=1)
        factor_management = pd.merge(factor_management, management, on="symbol")
        return factor_management

    @staticmethod
    def accounts_payables_t_days_ttm(ttm_management, factor_management, dependencies=['operating_cost', 'accounts_payable',
                                                                                      'notes_payable', 'advance_payment']):
        """
        应付账款周转天数
        应付账款周转天数 = 360/应付账款周转率
        :param dependencies:
        :param ttm_management:
        :param factor_management:
        :return:
        """

        management = ttm_management.loc[:, dependencies]
        management['ap'] = (management.accounts_payable
                            + management.notes_payable
                            - management.advance_payment) / 4
        management['accounts_payables_t_days_ttm'] = np.where(
            CalcTools.is_zero(management.ap.values), 0,
            360 / management.operating_cost.values * management.ap.values)
        dependencies.append('ap')
        management = management.drop(dependencies, axis=1)
        factor_management = pd.merge(factor_management, management, on="symbol")
        return factor_management

    @staticmethod
    def current_assets_t_rate_ttm(ttm_management, factor_management, dependencies=['operating_revenue', 'total_current_assets']):
        """
        流动资产周转率
        流动资产周转率 = 营业收入/流动资产合计
        :param dependencies:
        :param ttm_management:
        :param factor_management:
        :return:
        """

        management = ttm_management.loc[:, dependencies]
        management['current_assets_t_rate_ttm'] = np.where(
            CalcTools.is_zero(management.total_current_assets.values), 0,
            management.operating_revenue.values / management.total_current_assets.values * 4)
        management = management.drop(dependencies, axis=1)
        factor_management = pd.merge(factor_management, management, on="symbol")
        return factor_management

    @staticmethod
    def fixed_assets_t_rate_ttm(ttm_management, factor_management, dependencies = ['operating_revenue', 'fixed_assets', 'construction_materials', 'constru_in_process']):
        """
        固定资产周转率
        固定资产周转率 = 营业收入/（固定资产+工程物资+在建工程）
        :param dependencies:
        :param ttm_management:
        :param factor_management:
        :return:
        """

        management = ttm_management.loc[:, dependencies]
        management['fa'] = (management.fixed_assets
                            + management.construction_materials
                            + management.constru_in_process
                            )
        management['fixed_assets_t_rate_ttm'] = np.where(
            CalcTools.is_zero(management.fa.values), 0,
            management.operating_revenue.values / management.fa.values * 4)
        dependencies.append('fa')
        management = management.drop(dependencies, axis=1)
        factor_management = pd.merge(factor_management, management, on="symbol")
        return factor_management

    @staticmethod
    def equity_t_rate_ttm(ttm_management, factor_management, dependencies=['operating_revenue', 'total_owner_equities']):
        """
        股东权益周转率
        股东权益周转率 = 营业收入/股东权益
        :param dependencies:
        :param ttm_management:
        :param factor_management:
        :return:
        """

        management = ttm_management.loc[:, dependencies]
        management['equity_t_rate_ttm'] = np.where(
            CalcTools.is_zero(management.total_owner_equities.values), 0,
            management.operating_revenue.values / management.total_owner_equities.values * 4)
        management = management.drop(dependencies, axis=1)
        factor_management = pd.merge(factor_management, management, on="symbol")
        return factor_management

    @staticmethod
    def total_assets_t_rate_ttm(ttm_management, factor_management, dependencies=['operating_revenue', 'total_assets']):
        """
        总资产周转率
        总资产周转率 = 营业收入/总资产
        :param dependencies:
        :param ttm_management:
        :param factor_management:
        :return:
        """

        management = ttm_management.loc[:, dependencies]
        management['total_assets_t_rate_ttm'] = np.where(
            CalcTools.is_zero(management.total_assets.values), 0,
            management.operating_revenue.values / management.total_assets * 4)
        management = management.drop(dependencies, axis=1)
        factor_management = pd.merge(factor_management, management, on="symbol")
        return factor_management

    @staticmethod
    def cash_conversion_cycle(factor_management):
        """
        现金转换周期
        现金转换周期 = 应收账款周转天数 + 存货周转天数 - 应付账款周转天数
        :param factor_management:
        :return:
        """
        factor_management['cash_conversion_cycle'] = (factor_management['ar_t_days_ttm'] +
                                                      factor_management['inventory_t_days_ttm'] -
                                                      factor_management['accounts_payables_t_days_ttm'])
        return factor_management

    @staticmethod
    def operating_cycle(factor_management):
        """
        营业周期
        营业周期 = 应收账款周转天数 + 存货周转天数
        :param factor_management:
        :return:
        """
        factor_management['operating_cycle'] = (factor_management['ar_t_days_ttm'] +
                                                factor_management['inventory_t_days_ttm'])
        return factor_management

    @staticmethod
    def equity_to_asset(tp_management, factor_management, dependencies=['total_owner_equities', 'total_assets']):
        """
        股东权益比率
        股东权益比率 = 股东权益/总资产
        :param dependencies:
        :param tp_management:
        :param factor_management:
        :return:
        """

        management = tp_management.loc[:, dependencies]
        management['equity_to_asset'] = np.where(
            CalcTools.is_zero(management.total_assets.values), 0,
            management.total_owner_equities.values / management.total_assets.values)
        management = management.drop(dependencies, axis=1)
        factor_management = pd.merge(factor_management, management, on="symbol")
        return factor_management

    @staticmethod
    def total_profit_cost_ratio_ttm(ttm_management, factor_management, dependencies=['total_profit', 'operating_cost', 'financial_expense', 'sale_expense', 'administration_expense']):
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
    def bonds_payable_to_asset(tp_management, factor_management, dependencies=['bonds_payable', 'total_assets']):
        """
        应付债券与总资产之比
        应付债券与总资产之比 = 应付债券 / 总资产
        :param dependencies:
        :param tp_management:
        :param factor_management:
        :return:
        """

        management = tp_management.loc[:, dependencies]
        management['bonds_payable_to_asset'] = np.where(
            CalcTools.is_zero(management.total_assets.values), 0,
            management.bonds_payable.values / management.total_assets.values)
        management = management.drop(dependencies, axis=1)
        factor_management = pd.merge(factor_management, management, on="symbol")
        return factor_management

    @staticmethod
    def non_current_assets_ratio(tp_management, factor_management, dependencies=['total_non_current_assets', 'total_assets']):
        """
        非流动资产比率
        非流动资产比率 = 非流动资产合计 / 总资产
        :param dependencies:
        :param tp_management:
        :param factor_management:
        :return:
        """

        management = tp_management.loc[:, dependencies]
        management['non_current_assets_ratio'] = np.where(
            CalcTools.is_zero(management.total_assets.values), 0,
            management.total_non_current_assets.values / management.total_assets.values)
        management = management.drop(dependencies, axis=1)
        factor_management = pd.merge(factor_management, management, on="symbol")
        return factor_management

    @staticmethod
    def intangible_asset_ratio(tp_management, factor_management, dependencies=['intangible_assets', 'development_expenditure', 'good_will', 'total_assets']):
        """
        无形资产比率
        无形资产比率 = （无形资产 + 研发支出 + 商誉）/ 总资产
        :param dependencies:
        :param tp_management:
        :param factor_management:
        :return:
        """

        management = tp_management.loc[:, dependencies]
        management["ia"] = (management.intangible_assets +
                            management.development_expenditure +
                            management.good_will)
        management['intangible_asset_ratio'] = np.where(
            CalcTools.is_zero(management.total_assets.values), 0,
            management.ia.values / management.total_assets.values)
        dependencies.append('ia')
        management = management.drop(dependencies, axis=1)
        factor_management = pd.merge(factor_management, management, on="symbol")
        return factor_management

    @staticmethod
    def tangible_a_to_inte_bear_debt(tp_management, factor_management, dependencies=['equities_parent_company_owners', 'intangible_assets',
                                                                                     'development_expenditure', 'good_will',
                                                                                     'long_deferred_expense', 'deferred_tax_assets',
                                                                                     'shortterm_loan', 'non_current_liability_in_one_year',
                                                                                     'longterm_loan', 'bonds_payable', 'interest_payable']):
        """
        有形净值 / 带息负债
        :param dependencies:
        :param tp_management:
        :param factor_management:
        :return:
        """

        management = tp_management.loc[:, dependencies]
        management['ta'] = (management.equities_parent_company_owners -
                            management.intangible_assets -
                            management.development_expenditure -
                            management.good_will -
                            management.long_deferred_expense -
                            management.deferred_tax_assets)
        management['ibd'] = (management.shortterm_loan +
                             management.non_current_liability_in_one_year +
                             management.longterm_loan +
                             management.bonds_payable +
                             management.interest_payable)
        management['tangible_a_to_inte_bear_debt'] = np.where(
            CalcTools.is_zero(management.ibd.values), 0,
            management.ta.values / management.ibd.values)
        dependencies.extend(['ta', 'ibd'])
        management = management.drop(dependencies, axis=1)
        factor_management = pd.merge(factor_management, management, on="symbol")
        return factor_management

    @staticmethod
    def tangible_a_to_net_debt(tp_management, factor_management, dependencies=['equities_parent_company_owners', 'intangible_assets',
                                                                               'development_expenditure', 'good_will',
                                                                               'long_deferred_expense', 'deferred_tax_assets',
                                                                               'shortterm_loan', 'non_current_liability_in_one_year',
                                                                               'longterm_loan', 'bonds_payable', 'interest_payable',
                                                                               'cash_equivalents']):
        """
        有形净值 / 净债务
        :param dependencies:
        :param tp_management:
        :param factor_management:
        :return:
        """

        management = tp_management.loc[:, dependencies]
        management['ta'] = (management.equities_parent_company_owners -
                            management.intangible_assets -
                            management.development_expenditure -
                            management.good_will -
                            management.long_deferred_expense -
                            management.deferred_tax_assets)
        management['nd'] = (management.shortterm_loan +
                            management.non_current_liability_in_one_year +
                            management.longterm_loan +
                            management.bonds_payable +
                            management.interest_payable -
                            management.cash_equivalents)
        management['tangible_a_to_net_debt'] = np.where(
            CalcTools.is_zero(management.nd.values), 0,
            management.ta.values / management.nd.values)
        dependencies.extend(['ta', 'nd'])
        management = management.drop(dependencies, axis=1)
        factor_management = pd.merge(factor_management, management, on="symbol")
        return factor_management


def calculate(trade_date, management_data_dic, management):  # 计算对应因子
    print(trade_date)
    # 读取目前涉及到的因子
    tp_management = management_data_dic['tp_management']
    ttm_management = management_data_dic['ttm_management']

    # 因子计算
    factor_management = pd.DataFrame()
    factor_management['symbol'] = tp_management.index
    factor_management = management.current_ratio(tp_management, factor_management)
    factor_management = management.quick_ratio(tp_management, factor_management)
    factor_management = management.long_debt_to_working_capital(tp_management, factor_management)
    factor_management = management.equity_fixed_asset_ratio(tp_management, factor_management)
    factor_management = management.long_debt_to_asset(tp_management, factor_management)
    factor_management = management.long_term_debt_to_asset(tp_management, factor_management)
    factor_management = management.current_assets_ratio(tp_management, factor_management)
    factor_management = management.fix_asset_ratio(tp_management, factor_management)
    factor_management = management.blev(tp_management, factor_management)
    factor_management = management.mlev(tp_management, factor_management)
    factor_management = management.super_quick_ratio(tp_management, factor_management)
    factor_management = management.tsep_to_interest_bear_debt(tp_management, factor_management)
    factor_management = management.tsep_to_total_capital(tp_management, factor_management)
    factor_management = management.inventory_t_rate_ttm(ttm_management, factor_management)
    factor_management = management.inventory_t_days_ttm(ttm_management, factor_management)
    factor_management = management.ar_t_rate_ttm(ttm_management, factor_management)
    factor_management = management.ar_t_days_ttm(ttm_management, factor_management)
    factor_management = management.accounts_payables_t_rate_ttm(ttm_management, factor_management)
    factor_management = management.accounts_payables_t_days_ttm(ttm_management, factor_management)
    factor_management = management.current_assets_t_rate_ttm(ttm_management, factor_management)
    factor_management = management.fixed_assets_t_rate_ttm(ttm_management, factor_management)
    factor_management = management.equity_t_rate_ttm(ttm_management, factor_management)
    factor_management = management.total_assets_t_rate_ttm(ttm_management, factor_management)
    factor_management = management.cash_conversion_cycle(factor_management)
    factor_management = management.operating_cycle(factor_management)
    factor_management = management.equity_to_asset(tp_management, factor_management)
    factor_management = management.total_profit_cost_ratio_ttm(ttm_management, factor_management)
    factor_management = management.bonds_payable_to_asset(tp_management, factor_management)
    factor_management = management.non_current_assets_ratio(tp_management, factor_management)
    factor_management = management.intangible_asset_ratio(tp_management, factor_management)
    factor_management = management.tangible_a_to_inte_bear_debt(tp_management, factor_management)
    factor_management = management.tangible_a_to_net_debt(tp_management, factor_management)

    factor_management['id'] = factor_management['symbol'] + str(trade_date)
    factor_management['trade_date'] = str(trade_date)
    management._storage_data(factor_management, trade_date)


@app.task()
def factor_calculate(**kwargs):
    print("management_kwargs: {}".format(kwargs))
    date_index = kwargs['date_index']
    session = kwargs['session']
    cash_flow = FactorManagement('factor_management')  # 注意, 这里的name要与client中新建table时的name一致, 不然回报错
    content1 = cache_data.get_cache(session + str(date_index) + "1", date_index)
    content2 = cache_data.get_cache(session + str(date_index) + "2", date_index)
    tp_management = json_normalize(json.loads(str(content1, encoding='utf8')))
    ttm_management = json_normalize(json.loads(str(content2, encoding='utf8')))
    tp_management.set_index('symbol', inplace=True)
    ttm_management.set_index('symbol', inplace=True)
    print("len_tp_management_data {}".format(len(tp_management)))
    print("len_ttm_management_data {}".format(len(ttm_management)))
    total_cash_flow_data = {'tp_management': tp_management, 'ttm_management': ttm_management}
    calculate(date_index, total_cash_flow_data, cash_flow)
