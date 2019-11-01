#! /usr/bin/env python
# -*- coding: utf-8 -*-

"""
@version:
@author: li
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
class FactorOperationCapacity(object):
    """
    营运能力
    """
    def __init__(self):
        __str__ = 'factor_operation_capacity'
        self.name = '财务指标'
        self.factor_type1 = '财务指标'
        self.factor_type2 = '营运能力'
        self.description = '财务指标的二级指标-营运能力'

    @staticmethod
    def AccPayablesRateTTM(ttm_management, factor_management, dependencies=['operating_cost',
                                                                            'accounts_payable',
                                                                            'notes_payable',
                                                                            'advance_payment']):
        """
        :name: 应付账款周转率(TTM)
        :desc: 营业成本/（应付账款+应付票据-预付账款）
        :unit:
        :view_dimension: 0.01
        """

        management = ttm_management.loc[:, dependencies]
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
        :name:应付账款周转天数(TTM)
        :desc:360/应付账款周转率
        :unit: 天
        :view_dimension: 1
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
    def ARRateTTM(ttm_management, factor_management, dependencies=['operating_revenue',
                                                                   'account_receivable',
                                                                   'bill_receivable',
                                                                   'advance_peceipts']):
        """
        :name:应收账款周转率(TTM)
        :desc:营业收入/（应收账款+应收票据-预收账款）
        :unit:
        :view_dimension: 0.01
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
    def ARDaysTTM(ttm_management, factor_management, dependencies=['operating_revenue',
                                                                   'bill_receivable',
                                                                   'account_receivable',
                                                                   'advance_peceipts']):
        """
        :name:应收账款周转天数(TTM)
        :desc:360/应收账款周转率
        :unit: 天
        :view_dimension: 1
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
        :name:存货周转率(TTM)
        :desc:营业成本/存货 (补充，此处存货为过去4期的均值）
        :unit:
        :view_dimension: 0.01
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
        :name:存货周转天数(TTM)
        :desc:360/存货周转率
        :unit:
        :view_dimension: 0.01
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
        :name:现金转换周期(TTM)
        :desc:应收账款周转天数 + 存货周转天数 - 应付账款周转天数
        :unit: 天
        :view_dimension: 1
        """
        factor_management['CashCovCycle'] = (factor_management['ARDaysTTM'] +
                                             factor_management['InvDaysTTM'] -
                                             factor_management['AccPayablesDaysTTM'])
        return factor_management

    @staticmethod
    def CurAssetsRtTTM(ttm_management, factor_management, dependencies=['operating_revenue', 'total_current_assets']):
        """
        :name:流动资产周转率(TTM)
        :desc:营业收入/流动资产合计
        :unit:
        :view_dimension: 0.01
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
        :name: 固定资产周转率(TTM)
        :desc: 营业收入/（固定资产+工程物资+在建工程）
        :unit:
        :view_dimension: 0.01
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
        :name:营业周期(TTM)
        :desc:应收账款周转天数+存货周转天数。任意一项为空则不计算。
        :unit: 天
        :view_dimension: 1
        """
        factor_management['OptCycle'] = (factor_management['ARDaysTTM'] +
                                         factor_management['InvDaysTTM'])
        return factor_management

    @staticmethod
    def NetAssetTurnTTM(ttm_management, factor_management, dependencies=['total_operating_revenue', 'total_owner_equities']):
        """
        :name: 净资产周转率(TTM)
        :desc: 营业总收入/股东权益
        :unit:
        :view_dimension: 0.01
        """
        management = ttm_management.loc[:, dependencies]

        func = lambda x: x[0] / x[1] if x[1] is not None and x[0] !=0 else None
        management['NetAssetTurnTTM'] = management[dependencies].apply(func, axis=1)
        management = management.drop(dependencies, axis=1)

        factor_management = pd.merge(factor_management, management, how='outer', on="security_code")
        return factor_management

    @staticmethod
    def TotaAssetRtTTM(ttm_management, factor_management, dependencies=['operating_revenue',
                                                                                 'total_assets']):
        """
        :name: 总资产周转率(TTM)
        :desc: 营业收入/总资产
        :unit:
        :view_dimension: 0.01
        """

        management = ttm_management.loc[:, dependencies]
        management['TotaAssetRtTTM'] = np.where(
            CalcTools.is_zero(management.total_assets.values), 0,
            management.operating_revenue.values / management.total_assets * 4)
        management = management.drop(dependencies, axis=1)
        factor_management = pd.merge(factor_management, management, how='outer', on="security_code")
        return factor_management
