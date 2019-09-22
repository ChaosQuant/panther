#!/usr/bin/env python
# coding=utf-8

import gc, six
import json
import numpy as np
import pandas as pd
from pandas.io.json import json_normalize

from basic_derivation.factor_base import FactorBase
from utilities.calc_tools import CalcTools
from utilities.singleton import Singleton

# from basic_derivation import app
# from ultron.cluster.invoke.cache_data import cache_data


@six.add_metaclass(Singleton)
class Solvency(object):
    """
    偿债能力
    """

    def __init__(self):
        __str__ = 'factor_solvency'
        self.name = '财务指标'
        self.factor_type1 = '财务指标'
        self.factor_type2 = '偿债能力'
        self.desciption = '财务指标的二级指标， 偿债能力'

    @staticmethod
    def BondsToAsset(tp_solvency, factor_solvency, dependencies=['bonds_payable', 'total_assets']):
        """
        应付债券与总资产之比
        应付债券与总资产之比 = 应付债券 / 总资产
        :param dependencies:
        :param tp_solvency:
        :param factor_solvency:
        :return:
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
        账面杠杆
        账面杠杆 = 非流动负债合计/股东权益
        :param dependencies:
        :param tp_solvency:
        :param factor_solvency:
        :return:
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
        流动比率
        流动比率 = 流动资产合计/流动负债合计
        :param dependencies:
        :param tp_solvency:
        :param factor_solvency:
        :return:
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
        债务总资产比=负债合计/资产合计
        :param tp_solvency:
        :param factor_solvency:
        :param dependencies:
        :return:
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
        有形资产债务率
        负债合计/有形资产(流动资产+固定资产)
        :param tp_solvency:
        :param factor_solvency:
        :param dependencies:
        :return:
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
        权益比率
        :param tp_solvency:
        :param factor_solvency:
        :param dependencies:
        :return:
        """
        management = tp_solvency.loc[:, dependencies]

        func = lambda x: x[0] / x[1] if x[1] is not None and x[1] != 0 else None
        management['EquityRatio'] = management.apply(func, axis=1)

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
        归属母公司股东的权益/带息负债
        （补充 带息负债 = 短期借款+一年内到期的长期负债+长期借款+应付债券+应付利息）
        :param dependencies:
        :param tp_solvency:
        :param factor_solvency:
        :return:
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
        归属母公司股东的权益/全部投入资本
        (补充 全部投入资本=所有者权益合计+带息债务）
        :param dependencies: 
        :param tp_solvency:
        :param factor_solvency:
        :return:
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
        带息负债 /全部投入资本
        :param tp_solvency:
        :param factor_solvency:
        :param dependencies:
        :return:
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
        长期负债与营运资金比率
        长期负债与营运资金比率 = 非流动负债合计/（流动资产合计-流动负债合计）
        :param dependencies:
        :param tp_solvency:
        :param factor_solvency:
        :return:
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
        市场杠杆
        市场杠杆 = 非流动负债合计/（非流动负债合计+总市值）
        :param dependencies:
        :param tp_solvency:
        :param factor_solvency:
        :return:
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
        速动比率
        速动比率 = （流动资产合计-存货）/流动负债合计
        :param dependencies:
        :param tp_solvency:
        :param factor_solvency:
        :return:
        """

        management = tp_solvency.loc[:, dependencies]
        management['QuickRatio'] = np.where(
            CalcTools.is_zero(management.total_current_liability.values), 0,
            (management.total_current_assets.values - management.inventories.values)
            / management.total_current_liability.values)
        management = management.drop(dependencies, axis=1)
        factor_solvency = pd.merge(factor_solvency, management, how='outer', on="security_code")
        return factor_solvency

    @staticmethod
    def TNWorthToIBDebt(tp_solvency, factor_solvency, dependencies=['equities_parent_company_owners',
                                                                                 'intangible_assets',
                                                                                 'development_expenditure',
                                                                                 'good_will',
                                                                                 'long_deferred_expense',
                                                                    'deferred_tax_assets',
                                                                    'shortterm_loan',
                                                                    'non_current_liability_in_one_year',
                                                                    'longterm_loan',
                                                                    'bonds_payable',
                                                                    'interest_payable']):
        """
        有形净值 / 带息负债
        :param dependencies:
        :param tp_solvency:
        :param factor_solvency:
        :return:
        """

        management = tp_solvency.loc[:, dependencies]
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
        management['TNWorthToIBDebt'] = np.where(
            CalcTools.is_zero(management.ibd.values), 0,
            management.ta.values / management.ibd.values)
        dependencies = dependencies + ['ta', 'ibd']
        management = management.drop(dependencies, axis=1)
        factor_solvency = pd.merge(factor_solvency, management, how='outer', on="security_code")
        return factor_solvency

    @staticmethod
    def SupQuickRatio(tp_solvency, factor_solvency, dependencies=['cash_equivalents',
                                                                      'trading_assets',
                                                                      'bill_receivable',
                                                                      'account_receivable',
                                                                      'other_receivable',
                                                                      'total_current_liability']):
        """
        超速动比率
        超速动比率 = （货币资金+交易性金融资产+应收票据+应收账款+其他应收款）/流动负债合计
        :param dependencies:
        :param tp_solvency:
        :param factor_solvency:
        :return:
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
        有形净值 / 净债务
        :param dependencies:
        :param tp_solvency:
        :param factor_solvency:
        :return:
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
    def OptCFToCurrLiability(tp_solvency, factor_solvency, dependencies=['net_operate_cash_flow_mrq',
                                                                                          'total_current_liability']):
        """
        经营活动产生的现金流量净额（MRQ）/流动负债（MRQ）
        :param dependencies:
        :param tp_solvency:
        :param factor_solvency:
        :return:
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
        期末现金及现金等价物余额（TTM）/流动负债（TTM）
        :param dependencies:
        :param ttm_solvency:
        :param factor_solvency:
        :return:
        """

        cash_flow = ttm_solvency.loc[:, dependencies]
        cash_flow['CashRatioTTM'] = np.where(CalcTools.is_zero(cash_flow.total_current_assets.values),
                                             0,
                                             cash_flow.cash_and_equivalents_at_end.values / cash_flow.total_current_assets.values)
        cash_flow = cash_flow.drop(dependencies, axis=1)
        factor_solvency = pd.merge(factor_solvency, cash_flow, how='outer', on="security_code")
        return factor_solvency

    @staticmethod
    def InterestCovTTM(ttm_solvency, factor_solvency, dependencies=['total_profit',
                                                                        'financial_expense',
                                                                        'interest_income']):
        """
        利息保障倍数
        InterestCover=(TP + INT_EXP - INT_COME)/(INT_EXP - INT_COME)
        息税前利润/利息费用，息税前利润=利润总额+利息费用，利息费用=利息支出-利息收入
        :param dependencies:
        :param ttm_solvency:
        :param factor_solvency:
        :return:
        """
        earning = ttm_solvency.loc[:, dependencies]
        earning['InterestCovTTM'] = np.where(
            CalcTools.is_zero(earning.financial_expense.values - earning.interest_income.values), 0,
            (earning.total_profit.values + earning.financial_expense.values - earning.interest_income.values) /
            (earning.financial_expense.values - earning.interest_income.values))
        earning = earning.drop(dependencies, axis=1)
        factor_solvency = pd.merge(factor_solvency, earning, how='outer', on="security_code")
        return factor_solvency

    @staticmethod
    def OptCFToLiabilityTTM(ttm_solvency, factor_solvency,
                            dependencies=['net_operate_cash_flow', 'total_liability']):
        """
        # 经营活动净现金流（TTM）/负债（TTM）
        :param ttm_solvency:
        :param factor_solvency:
        :param dependencies:
        :return:
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
        带息负债计算有问题
        经营活动净现金流（TTM）/带息负债（TTM）
        :param ttm_solvency:
        :param factor_solvency:
        :param dependencies:
        :return:
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
        经营活动净现金流（TTM）/净负债（TTM）
        :param ttm_solvency:
        :param factor_solvency:
        :param dependencies:
        :return:
        """
        cash_flow = ttm_solvency.loc[:, dependencies]
        cash_flow['OptCFToNetDebtTTM'] = np.where(CalcTools.is_zero(cash_flow.net_liability.values), 0,
                                                  cash_flow.net_operate_cash_flow.values / cash_flow.net_liability.values)
        cash_flow = cash_flow.drop(dependencies, axis=1)
        factor_solvency = pd.merge(factor_solvency, cash_flow, how='outer', on="security_code")
        return factor_solvency

    @staticmethod
    def OptCFToCurrLiabilityTTM(ttm_solvency, factor_solvency,
                                dependencies=['net_operate_cash_flow', 'total_current_liability_ttm']):
        """
        经营活动产生的现金流量净额（TTM）/流动负债（TTM）
        :param dependencies:
        :param ttm_solvency:
        :param factor_solvency:
        :return:
        """
        cash_flow = ttm_solvency.loc[:, dependencies]
        cash_flow['OptCFToCurrLiabilityTTM'] = np.where(
            CalcTools.is_zero(cash_flow.total_current_liability_ttm.values), 0,
            cash_flow.net_operate_cash_flow.values / cash_flow.total_current_liability_ttm.values)
        cash_flow = cash_flow.drop(dependencies, axis=1)
        factor_solvency = pd.merge(factor_solvency, cash_flow, how='outer', on="security_code")
        return factor_solvency
