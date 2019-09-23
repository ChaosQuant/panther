#! /usr/bin/env python
# -*- coding: utf-8 -*-

"""
@version:
@author: Wang
@file: factor_operation_capacity.py
@time: 2019-05-30
"""
import gc, six
import sys
sys.path.append('../')
sys.path.append('../../')
sys.path.append('../../../')
import json
import numpy as np
import pandas as pd
from pandas.io.json import json_normalize
from utilities.calc_tools import CalcTools
from utilities.singleton import Singleton

# from basic_derivation import app
# from ultron.cluster.invoke.cache_data import cache_data


@six.add_metaclass(Singleton)
class OperationCapacity(object):
    """
    营运能力
    """
    def __init__(self):
        __str__ = 'factor_operation_capacity'
        self.name = '财务指标'
        self.factor_type1 = '财务指标'
        self.factor_type2 = '营运能力'
        self.desciption = '财务指标的二级指标， 营运能力'

    @staticmethod
    def AccPayablesRateTTM(ttm_management, factor_management, dependencies=['operating_cost',
                                                                                      'accounts_payable',
                                                                                      'notes_payable',
                                                                                      'advance_payment']):
        """
        应付账款周转率
        应付账款周转率 = 营业成本/（应付账款+应付票据-预付账款）
        :param dependencies:
        :param ttm_management:
        :param factor_management:
        :return:
        """

        management = ttm_management.loc[:, dependencies]
        print(management.head())
        management['ap'] = (management.accounts_payable
                            + management.notes_payable
                            - management.advance_payment) / 4
        management['AccPayablesRateTTM'] = np.where(
            CalcTools.is_zero(management.ap.values), 0,
            management.operating_cost.values / management.ap.values)
        dependencies = dependencies + ['ap']
        management = management.drop(dependencies, axis=1)
        factor_management = pd.merge(factor_management, management, how='outer', on="security_code")
        # factor_management['AccPayablesRateTTM'] = management['AccPayablesRateTTM']
        return factor_management

    @staticmethod
    def AccPayablesDaysTTM(ttm_management, factor_management, dependencies=['operating_cost',
                                                                                      'accounts_payable',
                                                                                      'notes_payable',
                                                                                      'advance_payment']):
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
        management['AccPayablesDaysTTM'] = np.where(
            CalcTools.is_zero(management.ap.values), 0,
            360 / management.operating_cost.values * management.ap.values)
        dependencies = dependencies + ['ap']
        management = management.drop(dependencies, axis=1)
        factor_management = pd.merge(factor_management, management, how='outer', on="security_code")
        # factor_management['AccPayablesDaysTTM'] = management['AccPayablesDaysTTM']
        return factor_management

    @staticmethod
    def ARRateTTM(ttm_management, factor_management, dependencies=['operating_revenue', 'account_receivable',
                                                                       'bill_receivable', 'advance_peceipts']):
        """
        应收账款周转率
        应收账款周转率 = 营业收入/（应收账款+应收票据-预收账款）
        :param dependencies:
        :param ttm_management:
        :param factor_management:
        :return:
        """

        management = ttm_management.loc[:, dependencies]
        management['ar'] = (management.account_receivable
                            + management.bill_receivable
                            - management.advance_peceipts) / 4
        management['ARRateTTM'] = np.where(
            CalcTools.is_zero(management.ar.values), 0,
            management.operating_revenue.values / management.ar.values)
        dependencies = dependencies + ['ar']
        management = management.drop(dependencies, axis=1)
        factor_management = pd.merge(factor_management, management, how='outer', on="security_code")
        # factor_management['ARRateTTM'] = management['ARRateTTM']
        return factor_management

    @staticmethod
    def ARDaysTTM(ttm_management, factor_management, dependencies=['operating_revenue', 'bill_receivable',
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
        management['ARDaysTTM'] = np.where(
            CalcTools.is_zero(management.operating_revenue.values), 0,
            360 / management.operating_revenue.values * management.ar.values)
        dependencies = dependencies + ['ar']
        management = management.drop(dependencies, axis=1)
        factor_management = pd.merge(factor_management, management, how='outer', on="security_code")
        # factor_management['ARDaysTTM'] = management['ARDaysTTM']
        return factor_management

    @staticmethod
    def InvRateTTM(ttm_management, factor_management, dependencies=['operating_cost', 'inventories']):
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
        management['InvRateTTM'] = np.where(
            CalcTools.is_zero(management.inventories.values), 0,
            management.operating_cost.values / management.inventories.values * 4)
        management = management.drop(dependencies, axis=1)
        factor_management = pd.merge(factor_management, management, how='outer', on="security_code")
        # factor_management['InvRateTTM'] = management['InvRateTTM']
        return factor_management

    @staticmethod
    def InvDaysTTM(ttm_management, factor_management, dependencies=['operating_cost', 'inventories']):
        """
        存货周转天数
        存货周转天数 = 360/存货周转率
        :param dependencies:
        :param ttm_management:
        :param factor_management:
        :return:
        """

        management = ttm_management.loc[:, dependencies]
        management['InvDaysTTM'] = np.where(
            CalcTools.is_zero(management.operating_cost.values), 0,
            360 / management.operating_cost.values * management.inventories.values / 4)
        management = management.drop(dependencies, axis=1)
        factor_management = pd.merge(factor_management, management, how='outer', on="security_code")
        # factor_management['InvDaysTTM'] = management['InvDaysTTM']
        return factor_management

    @staticmethod
    def CashCovCycle(factor_management):
        """
        现金转换周期
        现金转换周期 = 应收账款周转天数 + 存货周转天数 - 应付账款周转天数
        :param factor_management:
        :return:
        """
        factor_management['CashCovCycle'] = (factor_management['ARDaysTTM'] +
                                             factor_management['InvDaysTTM'] -
                                             factor_management['AccPayablesDaysTTM'])
        return factor_management

    @staticmethod
    def CurAssetsRtTTM(ttm_management, factor_management, dependencies=['operating_revenue', 'total_current_assets']):
        """
        流动资产周转率
        流动资产周转率 = 营业收入/流动资产合计
        :param dependencies:
        :param ttm_management:
        :param factor_management:
        :return:
        """

        management = ttm_management.loc[:, dependencies]
        management['CurAssetsRtTTM'] = np.where(
            CalcTools.is_zero(management.total_current_assets.values), 0,
            management.operating_revenue.values / management.total_current_assets.values * 4)
        management = management.drop(dependencies, axis=1)
        factor_management = pd.merge(factor_management, management, how='outer', on="security_code")
        # factor_management['CurAssetsRtTTM'] = management['CurAssetsRtTTM']
        return factor_management

    @staticmethod
    def FixAssetsRtTTM(ttm_management, factor_management, dependencies=['operating_revenue',
                                                                                 'fixed_assets',
                                                                                 'construction_materials',
                                                                                 'constru_in_process']):
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
        management['FixAssetsRtTTM'] = np.where(
            CalcTools.is_zero(management.fa.values), 0,
            management.operating_revenue.values / management.fa.values * 4)
        dependencies = dependencies + ['fa']
        management = management.drop(dependencies, axis=1)
        factor_management = pd.merge(factor_management, management, how='outer', on="security_code")
        # factor_management['FixAssetsRtTTM'] = management['FixAssetsRtTTM']
        return factor_management

    @staticmethod
    def OptCycle(factor_management):
        """
        营业周期
        营业周期 = 应收账款周转天数 + 存货周转天数
        :param factor_management:
        :return:
        """
        factor_management['OptCycle'] = (factor_management['ARDaysTTM'] +
                                         factor_management['InvDaysTTM'])
        return factor_management

    @staticmethod
    def TotaAssetRtTTM(ttm_management, factor_management, dependencies=['operating_revenue',
                                                                                 'total_assets']):
        """
        总资产周转率
        总资产周转率 = 营业收入/总资产
        :param dependencies:
        :param ttm_management:
        :param factor_management:
        :return:
        """

        management = ttm_management.loc[:, dependencies]
        management['TotaAssetRtTTM'] = np.where(
            CalcTools.is_zero(management.total_assets.values), 0,
            management.operating_revenue.values / management.total_assets * 4)
        management = management.drop(dependencies, axis=1)
        factor_management = pd.merge(factor_management, management, how='outer', on="security_code")
        return factor_management
