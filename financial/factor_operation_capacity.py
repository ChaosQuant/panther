#! /usr/bin/env python
# -*- coding: utf-8 -*-

"""
@version:
@author: Wang
@file: factor_operation_capacity.py
@time: 2019-05-30
"""
import gc
import sys
sys.path.append('../')
sys.path.append('../../')
sys.path.append('../../../')
import json
import numpy as np
import pandas as pd
from basic_derivation.factor_base import FactorBase
from pandas.io.json import json_normalize
from utilities.calc_tools import CalcTools

# from basic_derivation import app
# from ultron.cluster.invoke.cache_data import cache_data


class OperationCapacity(FactorBase):
    """
    营运能力
    """
    def __init__(self, name):
        super(OperationCapacity, self).__init__(name)

    def create_dest_tables(self):
        """
        创建数据库表
        :return:
        """
        drop_sql = """drop table if exists `{0}`""".format(self._name)
        create_sql = """create table `{0}`(
                    `id` INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
                    `security_code` varchar(24) NOT NULL,
                    `trade_date` date NOT NULL,
                    `AccPayablesRateTTM` decimal(19,4),
                    `AccPayablesDaysTTM` decimal(19,4),
                    `ARRateTTM` decimal(19,4),
                    `ARDaysTTM` decimal(19,4),
                    `CashCovCycle` decimal(19,4),
                    `CurAssetsRtTTM` decimal(19,4),
                    `FixAssetsRtTTM` decimal(19,4),
                    `InvRateTTM` decimal(19,4),
                    `InvDaysTTM` decimal(19,4),
                    `OptCycle` decimal(19,4),
                    `TotaAssetRtTTM` decimal(19,4),
                    constraint {0}_uindex
                    unique (`trade_date`,`security_code`)
                    )ENGINE=InnoDB DEFAULT CHARSET=utf8;""".format(self._name)
        super(OperationCapacity, self)._create_tables(create_sql, drop_sql)

    @staticmethod
    def accounts_payables_t_rate_ttm(ttm_management, factor_management, dependencies=['operating_cost',
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
    def accounts_payables_t_days_ttm(ttm_management, factor_management, dependencies=['operating_cost',
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
    def ar_t_rate_ttm(ttm_management, factor_management, dependencies=['operating_revenue', 'account_receivable',
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
        management['ARDaysTTM'] = np.where(
            CalcTools.is_zero(management.operating_revenue.values), 0,
            360 / management.operating_revenue.values * management.ar.values)
        dependencies = dependencies + ['ar']
        management = management.drop(dependencies, axis=1)
        factor_management = pd.merge(factor_management, management, how='outer', on="security_code")
        # factor_management['ARDaysTTM'] = management['ARDaysTTM']
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
        management['InvRateTTM'] = np.where(
            CalcTools.is_zero(management.inventories.values), 0,
            management.operating_cost.values / management.inventories.values * 4)
        management = management.drop(dependencies, axis=1)
        factor_management = pd.merge(factor_management, management, how='outer', on="security_code")
        # factor_management['InvRateTTM'] = management['InvRateTTM']
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
        management['InvDaysTTM'] = np.where(
            CalcTools.is_zero(management.operating_cost.values), 0,
            360 / management.operating_cost.values * management.inventories.values / 4)
        management = management.drop(dependencies, axis=1)
        factor_management = pd.merge(factor_management, management, how='outer', on="security_code")
        # factor_management['InvDaysTTM'] = management['InvDaysTTM']
        return factor_management

    @staticmethod
    def cash_conversion_cycle(factor_management):
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
        management['CurAssetsRtTTM'] = np.where(
            CalcTools.is_zero(management.total_current_assets.values), 0,
            management.operating_revenue.values / management.total_current_assets.values * 4)
        management = management.drop(dependencies, axis=1)
        factor_management = pd.merge(factor_management, management, how='outer', on="security_code")
        # factor_management['CurAssetsRtTTM'] = management['CurAssetsRtTTM']
        return factor_management

    @staticmethod
    def fixed_assets_t_rate_ttm(ttm_management, factor_management, dependencies=['operating_revenue',
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
    def operating_cycle(factor_management):
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
    def total_assets_t_rate_ttm(ttm_management, factor_management, dependencies=['operating_revenue',
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


def calculate(trade_date, ttm_operation_capacity, factor_name):  # 计算对应因子
    ttm_operation_capacity = ttm_operation_capacity.set_index('security_code')
    capacity = OperationCapacity(factor_name)  # 注意, 这里的name要与client中新建table时的name一致, 不然回报错
    # 读取目前涉及到的因子

    # 因子计算
    factor_management = pd.DataFrame()
    factor_management['security_code'] = ttm_operation_capacity.index
    factor_management = factor_management.set_index('security_code')

    factor_management = capacity.accounts_payables_t_rate_ttm(ttm_operation_capacity, factor_management)
    factor_management = capacity.accounts_payables_t_days_ttm(ttm_operation_capacity, factor_management)
    factor_management = capacity.ar_t_rate_ttm(ttm_operation_capacity, factor_management)
    factor_management = capacity.ar_t_days_ttm(ttm_operation_capacity, factor_management)
    factor_management = capacity.inventory_t_rate_ttm(ttm_operation_capacity, factor_management)
    factor_management = capacity.inventory_t_days_ttm(ttm_operation_capacity, factor_management)
    factor_management = capacity.cash_conversion_cycle(factor_management)
    factor_management = capacity.current_assets_t_rate_ttm(ttm_operation_capacity, factor_management)
    factor_management = capacity.fixed_assets_t_rate_ttm(ttm_operation_capacity, factor_management)
    factor_management = capacity.operating_cycle(factor_management)
    factor_management = capacity.total_assets_t_rate_ttm(ttm_operation_capacity, factor_management)

    factor_management = factor_management.reset_index()
    # factor_management['id'] = factor_management['security_code'] + str(trade_date)
    factor_management['trade_date'] = str(trade_date)
    print(factor_management.head())
    capacity._storage_data(factor_management, trade_date)
    del capacity
    gc.collect()


# @app.task()
def factor_calculate(**kwargs):
    print("management_kwargs: {}".format(kwargs))
    date_index = kwargs['date_index']
    session = kwargs['session']
    content1 = cache_data.get_cache(session + str(date_index) + "1", date_index)
    ttm_operation_capacity = json_normalize(json.loads(str(content1, encoding='utf8')))
    ttm_operation_capacity.set_index('security_code', inplace=True)
    print("len_tp_management_data {}".format(len(ttm_operation_capacity)))
    calculate(date_index, ttm_operation_capacity)
