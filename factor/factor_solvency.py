#!/usr/bin/env python
# coding=utf-8


import json
import numpy as np
import pandas as pd
from pandas.io.json import json_normalize

from factor.factor_base import FactorBase
from factor.utillities.calc_tools import CalcTools

# from factor import app
# from ultron.cluster.invoke.cache_data import cache_data


class Solvency(FactorBase):
    """
    偿债能力
    """
    def __init__(self, name):
        super(Solvency, self).__init__(name)

    # 构建因子表
    def create_dest_tables(self):
        drop_sql = """drop table if exists `{0}`""".format(self._name)
        create_sql = """create table `{0}`(
                    `id` varchar(32) NOT NULL,
                    `security_code` varchar(24) NOT NULL,
                    `trade_date` date NOT NULL,
                    `OptCFToLiabilityTTM` decimal(19,4),
                    `OptCFToIBDTTM` decimal(19,4),
                    `OptCFToNetDebtTTM` decimal(19,4),
                    `OptCFToNetIncomeTTM` decimal(19,4),
                    `OptCFToCurrLiabilityTTM` decimal(19,4),
                    `CashRatioTTM` decimal(19,4),
                    
                    
                    `OptOnReToAssetTTM` decimal(19,4),
                    `CashOfSales` decimal(19,4),
                    `NetProCashCoverTTM` decimal(19,4),
                    `OptToAssertTTM` decimal(19,4),
                    `OptToEnterpriseTTM` decimal(19,4),
                    `OptCFToRevTTM` decimal(19,4),
                    `SaleServCashToOptReTTM` decimal(19,4),
                    `SalesServCashToOR` decimal(19,4),
                    `NOCFToOpt` decimal(19,4),                   
                    PRIMARY KEY(`id`,`trade_date`,`security_code`)
                    )ENGINE=InnoDB DEFAULT CHARSET=utf8;""".format(self._name)
        super(Solvency, self)._create_tables(create_sql, drop_sql)

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
        factor_management = pd.merge(factor_management, management, on="security_code")
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
        factor_management = pd.merge(factor_management, management, on="security_code")
        return factor_management

    @staticmethod
    def cash_to_current_liability_ttm(ttm_cash_flow, factor_cash_flow):
        """
        期末现金及现金等价物余额（TTM）/流动负债（TTM）
        :param ttm_cash_flow:
        :param factor_cash_flow:
        :return:
        """
        dependencies = ['cash_and_equivalents_at_end', 'total_current_assets']
        cash_flow = ttm_cash_flow.loc[:, dependencies]
        cash_flow['CashRatioTTM'] = np.where(CalcTools.is_zero(cash_flow.total_current_assets.values),
                                                              0,
                                                              cash_flow.cash_and_equivalents_at_end.values / cash_flow.total_current_assets.values)
        cash_flow = cash_flow.drop(dependencies, axis=1)
        factor_cash_flow = pd.merge(factor_cash_flow, cash_flow, on="security_code")
        return factor_cash_flow

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
        factor_management = pd.merge(factor_management, management, on="security_code")
        return factor_management

    # 债务总资产比=负债合计/资产合计
    @staticmethod
    def debts_asset_ratio_latest(tp_contrarian, factor_contrarian, dependencies=['total_liability', 'total_assets']):

        contrarian = tp_contrarian.loc[:, dependencies]
        contrarian['DA'] = np.where(
            CalcTools.is_zero(contrarian['total_assets']), 0,
            contrarian['total_liability'] / contrarian['total_assets'])
        contrarian = contrarian.drop(dependencies, axis=1)
        factor_contrarian = pd.merge(factor_contrarian, contrarian, on="security_code")
        return factor_contrarian

    # 负债合计/有形资产(流动资产+固定资产)
    @staticmethod
    def debt_tangible_equity_ratio_latest(tp_contrarian, factor_contrarian, dependencies=['total_liability', 'total_current_liability', 'fixed_assets']):
        """
        有形资产债务率
        :param tp_contrarian:
        :param factor_contrarian:
        :param dependencies:
        :return:
        """
        contrarian = tp_contrarian.loc[:, dependencies]
        contrarian['DTE'] = np.where(
            CalcTools.is_zero(contrarian['total_current_liability'] + contrarian['fixed_assets']), 0,
            contrarian['total_current_liability'] / (contrarian['total_current_liability'] + contrarian['fixed_assets'])
        )
        contrarian = contrarian.drop(dependencies, axis=1)
        factor_contrarian = pd.merge(factor_contrarian, contrarian, on="security_code")
        return factor_contrarian

    @staticmethod
    def a1():
        """
        权益比率
        :return:
        """

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
        factor_management = pd.merge(factor_management, management, on="security_code")
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
        factor_management = pd.merge(factor_management, management, on="security_code")
        return factor_management

    # InteBearDebtToTotalCapital = 有息负债/总资本   总资本=固定资产+净运营资本  净运营资本=流动资产-流动负债
    # InteBearDebtToTotalCapital = 有息负债/(固定资产 + 流动资产 - 流动负债)
    @staticmethod
    def inte_bear_debt_to_total_capital_latest(tp_contrarian, factor_contrarian, dependencies=['interest_bearing_liability', 'fixed_assets', 'total_current_assets', 'total_current_liability']):
        """
        带息负债 /全部投入资本
        :param tp_contrarian:
        :param factor_contrarian:
        :param dependencies:
        :return:
        """
        contrarian = tp_contrarian.loc[:, dependencies]
        contrarian['IntBDToCap'] = np.where(
            CalcTools.is_zero(contrarian['fixed_assets'] + contrarian['total_current_assets'] + \
                              contrarian['total_current_liability']), 0,
            contrarian['interest_bearing_liability'] / (contrarian['fixed_assets'] + \
                                                        contrarian['total_current_assets'] + contrarian[
                                                            'total_current_liability'])
        )
        contrarian = contrarian.drop(dependencies, axis=1)
        factor_contrarian = pd.merge(factor_contrarian, contrarian, on="security_code")
        return factor_contrarian

    @staticmethod
    def interest_cover_ttm(ttm_earning, factor_earning, dependencies=['total_profit', 'financial_expense', 'interest_income']):
        """
        利息保障倍数
        InterestCover=(TP + INT_EXP - INT_COME)/(INT_EXP - INT_COME)
        息税前利润/利息费用，息税前利润=利润总额+利息费用，利息费用=利息支出-利息收入
        :param dependencies:
        :param ttm_earning:
        :param factor_earning:
        :return:
        """

        earning = ttm_earning.loc[:, dependencies]
        earning['interest_cover_ttm'] = np.where(
            CalcTools.is_zero(earning.financial_expense.values - earning.interest_income.values), 0,
            (earning.total_profit.values + earning.financial_expense.values - earning.interest_income.values) /
            (earning.financial_expense.values - earning.interest_income.values))
        earning = earning.drop(dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, earning, on="security_code")
        return factor_earning

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
        factor_management = pd.merge(factor_management, management, on="security_code")
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
        factor_management = pd.merge(factor_management, management, on="security_code")
        return factor_management

    @staticmethod
    def nocf_to_t_liability_ttm(ttm_cash_flow, factor_cash_flow, dependencies=['net_operate_cash_flow', 'total_liability']):
        """
        # 经营活动净现金流（TTM）/负债（TTM）
        :param ttm_cash_flow:
        :param factor_cash_flow:
        :param dependencies:
        :return:
        """
        cash_flow = ttm_cash_flow.loc[:, dependencies]
        cash_flow['OptCFToLiabilityTTM'] = np.where(
            CalcTools.is_zero(cash_flow.total_liability.values), 0,
            cash_flow.net_operate_cash_flow.values / cash_flow.total_liability.values)
        cash_flow = cash_flow.drop(dependencies, axis=1)
        factor_cash_flow = pd.merge(factor_cash_flow, cash_flow, on="security_code")
        return factor_cash_flow

    @staticmethod
    def nocf_to_interest_bear_debt_ttm(ttm_cash_flow, factor_cash_flow, dependencies=['net_operate_cash_flow', 'total_liability', 'interest_bearing_liability']):
        """
        youwenti
        经营活动净现金流（TTM）/带息负债（TTM）
        :param ttm_cash_flow:
        :param factor_cash_flow:
        :param dependencies:
        :return:
        """
        cash_flow = ttm_cash_flow.loc[:, dependencies]
        cash_flow['OptCFToIBDTTM'] = np.where(
            CalcTools.is_zero(cash_flow.interest_bearing_liability.values), 0,
            cash_flow.net_operate_cash_flow.values / cash_flow.interest_bearing_liability.values)
        cash_flow = cash_flow.drop(dependencies, axis=1)
        factor_cash_flow = pd.merge(factor_cash_flow, cash_flow, on="security_code")
        return factor_cash_flow

    @staticmethod
    def nocf_to_net_debt_ttm(ttm_cash_flow, factor_cash_flow, dependencies=['net_operate_cash_flow', 'net_liability']):
        """
        经营活动净现金流（TTM）/净负债（TTM）
        :param ttm_cash_flow:
        :param factor_cash_flow:
        :param dependencies:
        :return:
        """
        cash_flow = ttm_cash_flow.loc[:, dependencies]
        cash_flow['OptCFToNetDebtTTM'] = np.where(CalcTools.is_zero(cash_flow.net_liability.values), 0,
                                                     cash_flow.net_operate_cash_flow.values / cash_flow.net_liability.values)
        cash_flow = cash_flow.drop(dependencies, axis=1)
        factor_cash_flow = pd.merge(factor_cash_flow, cash_flow, on="security_code")
        return factor_cash_flow

    @staticmethod
    def oper_cash_in_to_current_liability_mrq(ttm_cash_flow, factor_cash_flow, dependencies=['net_operate_cash_flow', 'total_current_liability']):
        """
        经营活动产生的现金流量净额（MRQ）/流动负债（MRQ）
        :param dependencies:
        :param ttm_cash_flow:
        :param factor_cash_flow:
        :return:
        """
        cash_flow = ttm_cash_flow.loc[:, dependencies]
        cash_flow['OptCFToCurrLiability'] = np.where(
            CalcTools.is_zero(cash_flow.total_current_liability.values), 0,
            cash_flow.net_operate_cash_flow.values / cash_flow.total_current_liability.values)
        cash_flow = cash_flow.drop(dependencies, axis=1)
        factor_cash_flow = pd.merge(factor_cash_flow, cash_flow, on="security_code")
        return factor_cash_flow

    @staticmethod
    def oper_cash_in_to_current_liability_ttm(ttm_cash_flow, factor_cash_flow, dependencies=['net_operate_cash_flow', 'total_current_liability']):
        """
        经营活动产生的现金流量净额（TTM）/流动负债（TTM）

        :param dependencies:
        :param ttm_cash_flow:
        :param factor_cash_flow:
        :return:
        """

        cash_flow = ttm_cash_flow.loc[:, dependencies]
        cash_flow['OptCFToCurrLiabilityTTM'] = np.where(
            CalcTools.is_zero(cash_flow.total_current_liability.values), 0,
            cash_flow.net_operate_cash_flow.values / cash_flow.total_current_liability.values)
        cash_flow = cash_flow.drop(dependencies, axis=1)
        factor_cash_flow = pd.merge(factor_cash_flow, cash_flow, on="security_code")
        return factor_cash_flow

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
        factor_management = pd.merge(factor_management, management, on="security_code")
        return factor_management

    @staticmethod
    def super_quick_ratio(tp_management, factor_management, dependencies=['cash_equivalents', 'trading_assets',
                                                                          'bill_receivable', 'account_receivable',
                                                                          'other_receivable', 'total_current_liability']):
        """
        超速动比率
        超速动比率 = （货币资金+交易性金融资产+应收票据+应收账款+其他应收款）/流动负债合计
        :param dependencies:
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
        factor_management = pd.merge(factor_management, management, on="security_code")
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
        factor_management = pd.merge(factor_management, management, on="security_code")
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
        factor_management = pd.merge(factor_management, management, on="security_code")
        return factor_management


def calculate(trade_date, tp_solvency, ttm_solvency):  # 计算对应因子

    solvency = Solvency('factor_solvency')  # 注意, 这里的name要与client中新建table时的name一致, 不然回报错

    print(trade_date)
    # 读取目前涉及到的因子
    factor_solvency = pd.DataFrame()
    factor_solvency['security_code'] = tp_solvency.index

    # 非TTM计算
    factor_solvency = solvency.nocf_to_operating_ni_latest(tp_cash_flow, factor_cash_flow)
    factor_solvency = solvency.cash_rate_of_sales_latest(tp_cash_flow, factor_cash_flow)
    factor_solvency = solvency.sales_service_cash_to_or_latest(tp_cash_flow, factor_cash_flow)

    # TTM计算
    factor_solvency = solvency.nocf_to_t_liability_ttm(ttm_factor_sets, factor_cash_flow)
    factor_solvency = solvency.nocf_to_interest_bear_debt_ttm(ttm_factor_sets, factor_cash_flow)
    factor_solvency = solvency.nocf_to_net_debt_ttm(ttm_factor_sets, factor_cash_flow)
    factor_solvency = solvency.sale_service_cash_to_or_ttm(ttm_factor_sets, factor_cash_flow)
    factor_solvency = solvency.cash_rate_of_sales_ttm(ttm_factor_sets, factor_cash_flow)
    factor_solvency = solvency.nocf_to_operating_ni_ttm(ttm_factor_sets, factor_cash_flow)
    factor_solvency = solvency.oper_cash_in_to_current_liability_ttm(ttm_factor_sets, factor_cash_flow)
    factor_solvency = solvency.cash_to_current_liability_ttm(ttm_factor_sets, factor_cash_flow)
    factor_solvency = solvency.cfo_to_ev_ttm(ttm_factor_sets, factor_cash_flow)
    factor_solvency = solvency.acca_ttm(ttm_factor_sets, factor_cash_flow)
    factor_solvency = solvency.net_profit_cash_cover_ttm(ttm_factor_sets, factor_cash_flow)
    factor_solvency = solvency.oper_cash_in_to_asset_ttm(ttm_factor_sets, factor_cash_flow)
    factor_solvency['id'] = factor_solvency['security_code'] + str(trade_date)
    factor_solvency['trade_date'] = str(trade_date)
    # solvency._storage_data(factor_cash_flow, trade_date)


# @app.task()
def factor_calculate(**kwargs):
    print("cash_flow_kwargs: {}".format(kwargs))
    date_index = kwargs['date_index']
    session = kwargs['session']
    content1 = cache_data.get_cache(session + str(date_index) + "1", date_index)
    content2 = cache_data.get_cache(session + str(date_index) + "2", date_index)
    tp_solvency = json_normalize(json.loads(str(content1, encoding='utf8')))
    ttm_solvency = json_normalize(json.loads(str(content2, encoding='utf8')))
    tp_solvency.set_index('security_code', inplace=True)
    ttm_solvency.set_index('security_code', inplace=True)
    print("len_tp_cash_flow_data {}".format(len(tp_solvency)))
    print("len_ttm_cash_flow_data {}".format(len(ttm_solvency)))
    calculate(date_index, tp_solvency, ttm_solvency)
