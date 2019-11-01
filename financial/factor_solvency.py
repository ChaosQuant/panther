#!/usr/bin/env python
# coding=utf-8
"""
@version: 0.1
@author: li
@file: factor_solvency.py
@time: 2019-01-28 11:33
"""
import gc, six
import json
import numpy as np
import pandas as pd
from pandas.io.json import json_normalize

from utilities.calc_tools import CalcTools
from utilities.singleton import Singleton

# from basic_derivation import app
# from ultron.cluster.invoke.cache_data import cache_data


@six.add_metaclass(Singleton)
class FactorSolvency(object):
    """
    偿债能力
    """

    def __init__(self):
        __str__ = 'factor_solvency'
        self.name = '财务指标'
        self.factor_type1 = '财务指标'
        self.factor_type2 = '偿债能力'
        self.description = '财务指标的二级指标-偿债能力'

    @staticmethod
    def BondsToAsset(tp_solvency, factor_solvency, dependencies=['bonds_payable', 'total_assets']):
        """
        :name: 应付债券与总资产之比
        :desc: 应付债券MRQ/资产总计MRQ
        :unit:
        :view_dimension: 0.01
        """
        management = tp_solvency.loc[:, dependencies]
        management['BondsToAsset'] = np.where(
            CalcTools.is_zero(management.total_assets.values), 0,
            management.bonds_payable.values / management.total_assets.values)
        management = management.drop(dependencies, axis=1)
        factor_solvency = pd.merge(factor_solvency, management, on="security_code")
        return factor_solvency

    @staticmethod
    def BookLev(tp_solvency, factor_solvency, dependencies=['total_non_current_liability', 'total_assets']):
        """
        :name: 账面杠杆
        :desc:非流动负债合计/股东权益合计（含少数股东权益）（MRQ)
        :unit:
        :view_dimension: 0.01
        """
        management = tp_solvency.loc[:, dependencies]
        management['BookLev'] = np.where(
            CalcTools.is_zero(management.total_assets.values), 0,
            management.total_non_current_liability.values / management.total_assets.values)
        management = management.drop(dependencies, axis=1)
        factor_solvency = pd.merge(factor_solvency, management, on="security_code")
        return factor_solvency

    @staticmethod
    def CurrentRatio(tp_solvency, factor_solvency, dependencies=['total_current_assets', 'total_current_liability']):
        """
        :name: 流动比率
        :desc: 流动资产合计/流动负债合计（MRQ）
        :unit:
        :view_dimension: 0.01
        """
        management = tp_solvency.loc[:, dependencies]
        management['CurrentRatio'] = np.where(
            CalcTools.is_zero(management.total_current_liability.values), 0,
            management.total_current_assets.values / management.total_current_liability.values)
        management = management.drop(dependencies, axis=1)
        factor_solvency = pd.merge(factor_solvency, management, on="security_code")
        return factor_solvency

    @staticmethod
    def DA(tp_solvency, factor_solvency, dependencies=['total_liability', 'total_assets']):
        """
        :name: 债务总资产比
        :desc:负债合计MRQ/资产总计MRQ
        :unit:
        :view_dimension: 0.01
        """
        contrarian = tp_solvency.loc[:, dependencies]
        contrarian['DA'] = np.where(
            CalcTools.is_zero(contrarian['total_assets']), 0,
            contrarian['total_liability'] / contrarian['total_assets'])
        contrarian = contrarian.drop(dependencies, axis=1)
        factor_solvency = pd.merge(factor_solvency, contrarian, on="security_code")
        return factor_solvency

    @staticmethod
    def DTE(tp_solvency, factor_solvency,
            dependencies=['total_liability', 'total_current_liability', 'fixed_assets']):
        """
        :name:有形净值债务率
        :desc:负债合计/有形净值（MRQ）
        :unit:
        :view_dimension: 0.01
        """
        contrarian = tp_solvency.loc[:, dependencies]
        contrarian['DTE'] = np.where(
            CalcTools.is_zero(contrarian['total_current_liability'] + contrarian['fixed_assets']), 0,
            contrarian['total_current_liability'] / (contrarian['total_current_liability'] + contrarian['fixed_assets'])
        )
        contrarian = contrarian.drop(dependencies, axis=1)
        factor_solvency = pd.merge(factor_solvency, contrarian, on="security_code")
        return factor_solvency

    @staticmethod
    def EquityRatio(tp_solvency, factor_solvency,
                    dependencies=['total_liability', 'equities_parent_company_owners']):
        """
        :name:权益比率
        :desc:负债合计/归属母公司股东的权益（MRQ）
        :unit:
        :view_dimension: 0.01
        """
        management = tp_solvency.loc[:, dependencies]

        func = lambda x: x[0] / x[1] if x[1] is not None and x[1] != 0 else None
        management['EquityRatio'] = management.apply(func, axis=1)
        management = management.drop(dependencies, axis=1)
        factor_solvency = pd.merge(management, factor_solvency, how='outer', on='security_code')
        return factor_solvency

    @staticmethod
    def EquityPCToIBDebt(tp_solvency, factor_solvency, dependencies=['equities_parent_company_owners',
                                                                               'shortterm_loan',
                                                                               'non_current_liability_in_one_year',
                                                                               'longterm_loan',
                                                                               'bonds_payable',
                                                                     'interest_payable']):
        """
        :name:归属母公司股东的权益/带息负债
        :desc:归属母公司股东的权益/带息负债（补充 带息负债 = 短期借款+一年内到期的长期负债+长期借款+应付债券+应付利息）
        :unit:
        :view_dimension: 0.01
        """

        management = tp_solvency.loc[:, dependencies]
        management["debt"] = (management.shortterm_loan +
                              management.non_current_liability_in_one_year +
                              management.longterm_loan +
                              management.bonds_payable +
                              management.interest_payable)
        management['EquityPCToIBDebt'] = np.where(
            CalcTools.is_zero(management.debt.values), 0,
            management.equities_parent_company_owners.values / management.debt.values)

        dependencies = dependencies + ['debt']
        management = management.drop(dependencies, axis=1)
        factor_solvency = pd.merge(factor_solvency, management, how='outer', on="security_code")
        return factor_solvency

    @staticmethod
    def EquityPCToTCap(tp_solvency, factor_solvency, dependencies=['equities_parent_company_owners',
                                                                          'total_owner_equities',
                                                                          'shortterm_loan',
                                                                          'non_current_liability_in_one_year',
                                                                          'longterm_loan', 'bonds_payable',
                                                                   'interest_payable']):
        """

        :name:归属母公司股东的权益/全部投入资本 (补充 全部投入资本=所有者权益合计+带息债务）
        :desc: 归属母公司股东的权益/全部投入资本 (补充 全部投入资本=所有者权益合计+带息债务）
        :unit:
        :view_dimension: 0.01
        """

        management = tp_solvency.loc[:, dependencies]
        management["tc"] = (management.total_owner_equities
                            + management.shortterm_loan
                            + management.non_current_liability_in_one_year
                            + management.longterm_loan
                            + management.bonds_payable
                            + management.interest_payable)
        management['EquityPCToTCap'] = np.where(
            CalcTools.is_zero(management.tc.values), 0,
            management.equities_parent_company_owners.values / management.tc.values)
        dependencies = dependencies + ['tc']
        management = management.drop(dependencies, axis=1)
        factor_solvency = pd.merge(factor_solvency, management, how='outer', on="security_code")
        return factor_solvency

    # InteBearDebtToTotalCapital = 有息负债/总资本   总资本=固定资产+净运营资本  净运营资本=流动资产-流动负债
    # InteBearDebtToTotalCapital = 有息负债/(固定资产 + 流动资产 - 流动负债)
    @staticmethod
    def IntBDToCap(tp_solvency, factor_solvency, dependencies=['shortterm_loan',
                                                               'non_current_liability_in_one_year',
                                                               'longterm_loan',
                                                               'bonds_payable',
                                                               'interest_payable',
                                                               'fixed_assets',
                                                               'total_current_assets',
                                                               'total_current_liability']):
        """

        :name:带息负债/全部投入资本
        :desc:带息债务/全部投入资本（MRQ）
        :unit:
        :view_dimension: 0.01
        """
        contrarian = tp_solvency.loc[:, dependencies]
        contrarian['interest_bearing_liability'] = contrarian['shortterm_loan'] + \
                                                   contrarian['non_current_liability_in_one_year'] + \
                                                   contrarian['longterm_loan'] + \
                                                   contrarian['bonds_payable'] + contrarian['interest_payable']
        contrarian['IntBDToCap'] = np.where(
            CalcTools.is_zero(contrarian['fixed_assets'] + contrarian['total_current_assets'] + \
                              contrarian['total_current_liability']), 0,
            contrarian['interest_bearing_liability'] / (contrarian['fixed_assets'] + contrarian['total_current_assets']
                                                        + contrarian['total_current_liability'])
        )
        dependencies = dependencies + ['interest_bearing_liability']
        contrarian = contrarian.drop(dependencies, axis=1)
        factor_solvency = pd.merge(factor_solvency, contrarian, how='outer', on="security_code")
        return factor_solvency

    @staticmethod
    def LDebtToWCap(tp_solvency, factor_solvency, dependencies=['total_current_assets',
                                                                'total_current_liability',
                                                                'total_non_current_assets']):
        """
        :name:长期负债与营运资金比率
        :desc:非流动负债合计/（流动资产合计-流动负债合计）
        :unit:
        :view_dimension: 0.01
        """
        management = tp_solvency.loc[:, dependencies]
        management['LDebtToWCap'] = np.where(
            CalcTools.is_zero(management.total_current_assets.values - management.total_current_liability.values), 0,
            management.total_non_current_assets.values
            / (management.total_current_assets.values - management.total_current_liability.values))
        management = management.drop(dependencies, axis=1)
        factor_solvency = pd.merge(factor_solvency, management, how='outer', on="security_code")
        return factor_solvency

    @staticmethod
    def MktLev(tp_solvency, factor_solvency, dependencies=['total_non_current_liability', 'market_cap']):
        """
        :name:市场杠杆
        :desc:非流动负债合计MRQ/（非流动负债台计MRO+总市值）
        :unit:
        :view_dimension: 0.01
        """

        management = tp_solvency.loc[:, dependencies]
        management['MktLev'] = np.where(
            CalcTools.is_zero(management.market_cap.values), 0,
            management.total_non_current_liability.values /
            (management.total_non_current_liability.values + management.market_cap.values))
        management = management.drop(dependencies, axis=1)
        factor_solvency = pd.merge(factor_solvency, management, how='outer', on="security_code")
        return factor_solvency

    @staticmethod
    def QuickRatio(tp_solvency, factor_solvency,
                   dependencies=['total_current_assets', 'total_current_liability', 'inventories']):
        """
        :name:速动比率
        :desc:（流动资产合计-存货）/流动负债合计（MRQ）
        :unit:
        :view_dimension: 0.01
        """
        management = tp_solvency.loc[:, dependencies]
        management['QuickRatio'] = np.where(
            CalcTools.is_zero(management.total_current_liability.values), 0,
            (management.total_current_assets.values - management.inventories.values)
            / management.total_current_liability.values)
        management = management.drop(dependencies, axis=1)
        factor_solvency = pd.merge(factor_solvency, management, how='outer', on="security_code")
        return factor_solvency

    # @staticmethod
    # def TNWorthToIBDebt(tp_solvency, factor_solvency, dependencies=['equities_parent_company_owners',
    #                                                                 'intangible_assets',
    #                                                                 'development_expenditure',
    #                                                                 'good_will',
    #                                                                 'long_deferred_expense',
    #                                                                 'deferred_tax_assets',
    #                                                                 'shortterm_loan',
    #                                                                 'non_current_liability_in_one_year',
    #                                                                 'longterm_loan',
    #                                                                 'bonds_payable',
    #                                                                 'interest_payable']):
    #     """
    #
    #     :name:有形净值/带息负债
    #     :desc:有形净值/带息负债（MRQ）
    #     """
    #
    #     management = tp_solvency.loc[:, dependencies]
    #     management['ta'] = (management.equities_parent_company_owners -
    #                         management.intangible_assets -
    #                         management.development_expenditure -
    #                         management.good_will -
    #                         management.long_deferred_expense -
    #                         management.deferred_tax_assets)
    #     management['ibd'] = (management.shortterm_loan +
    #                          management.non_current_liability_in_one_year +
    #                          management.longterm_loan +
    #                          management.bonds_payable +
    #                          management.interest_payable)
    #     management['TNWorthToIBDebt'] = np.where(
    #         CalcTools.is_zero(management.ibd.values), 0,
    #         management.ta.values / management.ibd.values)
    #     dependencies = dependencies + ['ta', 'ibd']
    #     management = management.drop(dependencies, axis=1)
    #     factor_solvency = pd.merge(factor_solvency, management, how='outer', on="security_code")
    #     return factor_solvency

    @staticmethod
    def SupQuickRatio(tp_solvency, factor_solvency, dependencies=['cash_equivalents',
                                                                      'trading_assets',
                                                                      'bill_receivable',
                                                                      'account_receivable',
                                                                      'other_receivable',
                                                                      'total_current_liability']):
        """
        :name:超速动比率
        :desc:（货币资金+交易性金融资资产+应收票据+应收帐款+其他应收款）/流动负债合计（MRQ）
        :unit:
        :view_dimension: 0.01
        """

        management = tp_solvency.loc[:, dependencies]
        management['SupQuickRatio'] = np.where(
            CalcTools.is_zero(management.total_current_liability.values), 0,
            (management.cash_equivalents.values +
             management.trading_assets.values +
             management.bill_receivable.values +
             management.account_receivable.values +
             management.other_receivable.values) /
            management.total_current_liability.values)
        management = management.drop(dependencies, axis=1)
        factor_solvency = pd.merge(factor_solvency, management, how='outer', on="security_code")
        return factor_solvency

    @staticmethod
    def TNWorthToNDebt(tp_solvency, factor_solvency, dependencies=['equities_parent_company_owners',
                                                                           'intangible_assets',
                                                                           'development_expenditure',
                                                                           'good_will',
                                                                           'long_deferred_expense',
                                                                   'deferred_tax_assets',
                                                                   'shortterm_loan',
                                                                   'non_current_liability_in_one_year',
                                                                   'longterm_loan',
                                                                   'bonds_payable',
                                                                   'interest_payable',
                                                                   'cash_equivalents']):
        """
        :name:有形净值/净债务
        :desc:有形净值/净债务（MRQ）
        :unit:
        :view_dimension: 0.01
        """

        management = tp_solvency.loc[:, dependencies]
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
        management['TNWorthToNDebt'] = np.where(
            CalcTools.is_zero(management.nd.values), 0,
            management.ta.values / management.nd.values)
        dependencies = dependencies + ['ta', 'nd']
        management = management.drop(dependencies, axis=1)
        factor_solvency = pd.merge(factor_solvency, management, how='outer', on="security_code")
        return factor_solvency

    @staticmethod
    def OPCToDebt(ttm_solvency, factor_solvency,
                  dependencies=['net_operate_cash_flow_mrq', 'total_current_liability']):
        """
        :name:现金流债务比
        :desc:经营活动现金净流量（MRQ）/流动负债（MRQ）
        :unit:
        :view_dimension: 0.01
        """
        cash_flow = ttm_solvency.loc[:, dependencies]
        cash_flow['OPCToDebt'] = np.where(
            CalcTools.is_zero(cash_flow.total_current_liability.values), 0,
            cash_flow.net_operate_cash_flow_mrq.values / cash_flow.total_current_liability.values)
        cash_flow = cash_flow.drop(dependencies, axis=1)
        factor_solvency = pd.merge(factor_solvency, cash_flow, how='outer', on="security_code")
        return factor_solvency

    @staticmethod
    def OptCFToCurrLiability(tp_solvency, factor_solvency, dependencies=['net_operate_cash_flow_mrq',
                                                                         'total_current_liability']):
        """
        :name:经营活动产生的现金流量净额（MRQ）/流动负债（MRQ）
        :desc:经营活动产生的现金流量净额（MRQ）/流动负债（MRQ）
        :unit:
        :view_dimension: 0.01
        """
        cash_flow = tp_solvency.loc[:, dependencies]
        cash_flow['OptCFToCurrLiability'] = np.where(
            CalcTools.is_zero(cash_flow.total_current_liability.values), 0,
            cash_flow.net_operate_cash_flow_mrq.values / cash_flow.total_current_liability.values)
        cash_flow = cash_flow.drop(dependencies, axis=1)
        factor_solvency = pd.merge(factor_solvency, cash_flow, how='outer', on="security_code")
        return factor_solvency

    @staticmethod
    def CashRatioTTM(ttm_solvency, factor_solvency, dependencies=['cash_and_equivalents_at_end',
                                                                                   'total_current_assets']):
        """
        :name:期末现金及现金等价物余额（TTM）/流动负债（TTM）
        :desc:期末现金及现金等价物余额（TTM）/流动负债（TTM）
        :unit:
        :view_dimension: 0.01
        """

        cash_flow = ttm_solvency.loc[:, dependencies]
        cash_flow['CashRatioTTM'] = np.where(CalcTools.is_zero(cash_flow.total_current_assets.values),
                                             0,
                                             cash_flow.cash_and_equivalents_at_end.values / cash_flow.total_current_assets.values)
        cash_flow = cash_flow.drop(dependencies, axis=1)
        factor_solvency = pd.merge(factor_solvency, cash_flow, how='outer', on="security_code")
        return factor_solvency

    # @staticmethod
    # def InterestCovTTM(ttm_solvency, factor_solvency, dependencies=['total_profit',
    #                                                                     'financial_expense',
    #                                                                     'interest_income']):
    #     """
    #     缺利息收入
    #     :name: 利息保障倍数
    #     :desc:息税前利润/利息费用，息税前利润=利润总额+利息费用，利息费用=利息支出-利息收入
    #     """
    #     earning = ttm_solvency.loc[:, dependencies]
    #     earning['InterestCovTTM'] = np.where(
    #         CalcTools.is_zero(earning.financial_expense.values - earning.interest_income.values), 0,
    #         (earning.total_profit.values + earning.financial_expense.values - earning.interest_income.values) /
    #         (earning.financial_expense.values - earning.interest_income.values))
    #     earning = earning.drop(dependencies, axis=1)
    #     factor_solvency = pd.merge(factor_solvency, earning, how='outer', on="security_code")
    #     return factor_solvency

    @staticmethod
    def OptCFToLiabilityTTM(ttm_solvency, factor_solvency,
                            dependencies=['net_operate_cash_flow', 'total_liability']):
        """
        :name:经营活动净现金流（TTM）/负债（TTM）
        :desc:经营活动净现金流（TTM）/负债（TTM）
        :unit:
        :view_dimension: 0.01
        """
        cash_flow = ttm_solvency.loc[:, dependencies]
        cash_flow['OptCFToLiabilityTTM'] = np.where(
            CalcTools.is_zero(cash_flow.total_liability.values), 0,
            cash_flow.net_operate_cash_flow.values / cash_flow.total_liability.values)
        cash_flow = cash_flow.drop(dependencies, axis=1)
        factor_solvency = pd.merge(factor_solvency, cash_flow, how='outer', on="security_code")
        return factor_solvency

    @staticmethod
    def OptCFToIBDTTM(ttm_solvency, factor_solvency, dependencies=['net_operate_cash_flow',
                                                                                    'shortterm_loan',
                                                                                    'non_current_liability_in_one_year_ttm',
                                                                                    'longterm_loan',
                                                                                    'bonds_payable',
                                                                   'interest_payable'
                                                                   ]):
        """
        :name:经营活动净现金流（TTM）/带息负债（TTM）
        :desc:经营活动净现金流（TTM）/带息负债（TTM）
        :unit:
        :view_dimension: 0.01
        """
        cash_flow = ttm_solvency.loc[:, dependencies]
        cash_flow['interest_bearing_liability'] = cash_flow['shortterm_loan'] + \
                                                  cash_flow['non_current_liability_in_one_year_ttm'] + \
                                                  cash_flow['longterm_loan'] + \
                                                  cash_flow['bonds_payable'] + cash_flow['interest_payable']
        cash_flow['OptCFToIBDTTM'] = np.where(
            CalcTools.is_zero(cash_flow.interest_bearing_liability.values), 0,
            cash_flow.net_operate_cash_flow.values / cash_flow.interest_bearing_liability.values)
        dependencies = dependencies + ['interest_bearing_liability']
        cash_flow = cash_flow.drop(dependencies, axis=1)
        factor_solvency = pd.merge(factor_solvency, cash_flow, how='outer', on="security_code")
        return factor_solvency

    @staticmethod
    def OptCFToNetDebtTTM(ttm_solvency, factor_solvency, dependencies=['net_operate_cash_flow', 'net_liability']):
        """
        :name:经营活动净现金流（TTM）/净负债（TTM）
        :desc:经营活动净现金流（TTM）/净负债（TTM）
        :unit:
        :view_dimension: 0.01
        """
        cash_flow = ttm_solvency.loc[:, dependencies]
        cash_flow['OptCFToNetDebtTTM'] = np.where(CalcTools.is_zero(cash_flow.net_liability.values), 0,
                                                  cash_flow.net_operate_cash_flow.values / cash_flow.net_liability.values)
        cash_flow = cash_flow.drop(dependencies, axis=1)
        factor_solvency = pd.merge(factor_solvency, cash_flow, how='outer', on="security_code")
        return factor_solvency

    @staticmethod
    def OPCToDebtTTM(ttm_solvency, factor_solvency,
                                dependencies=['net_operate_cash_flow', 'total_current_liability_ttm']):
        """
        :name:现金流债务比(TTM)
        :desc:经营活动现金净流量（TTM）/流动负债（MRQ）
        :unit:
        :view_dimension: 0.01
        """
        cash_flow = ttm_solvency.loc[:, dependencies]
        cash_flow['OPCToDebtTTM'] = np.where(
            CalcTools.is_zero(cash_flow.total_current_liability_ttm.values), 0,
            cash_flow.net_operate_cash_flow.values / cash_flow.total_current_liability_ttm.values)
        cash_flow = cash_flow.drop(dependencies, axis=1)
        factor_solvency = pd.merge(factor_solvency, cash_flow, how='outer', on="security_code")
        return factor_solvency
