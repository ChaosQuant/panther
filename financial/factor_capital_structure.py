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
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


class CapitalStructure(FactorBase):
    """
    资本结构
    """
    def __init__(self, name):
        super(CapitalStructure, self).__init__(name)

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
                    `NonCurrAssetRatio` decimal(19,4),
                    `LongDebtToAsset` decimal(19,4),
                    `LongBorrToAssert` decimal(19,4),
                    `IntangibleAssetRatio` decimal(19,4),
                    `FixAssetsRt` decimal(19,4),
                    `EquityToAsset` decimal(19,4),
                    `EquityToFixedAsset` decimal(19,4),
                    `CurAssetsR` decimal(19,4),
                    constraint {0}_uindex
                    unique (`trade_date`,`security_code`)
                    )ENGINE=InnoDB DEFAULT CHARSET=utf8;""".format(self._name)
        super(CapitalStructure, self)._create_tables(create_sql, drop_sql)

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
        management['NonCurrAssetRatio'] = np.where(
            CalcTools.is_zero(management.total_assets.values), 0,
            management.total_non_current_assets.values / management.total_assets.values)
        management = management.drop(dependencies, axis=1)
        factor_management = pd.merge(factor_management, management, how='outer', on="security_code")
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
        management['LongDebtToAsset'] = np.where(
            CalcTools.is_zero(management.total_assets.values), 0,
            management.total_non_current_liability.values / management.total_assets.values)
        management = management.drop(dependencies, axis=1)
        factor_management = pd.merge(factor_management, management, how='outer', on="security_code")
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
        management['LongBorrToAssert'] = np.where(
            CalcTools.is_zero(management.total_assets.values), 0,
            management.longterm_loan.values / management.total_assets.values)
        management = management.drop(dependencies, axis=1)
        factor_management = pd.merge(factor_management, management, how='outer', on="security_code")
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
        management["ia"] = (management.intangible_assets + management.development_expenditure + management.good_will)
        management['IntangibleAssetRatio'] = np.where(
            CalcTools.is_zero(management.total_assets.values), 0,
            management.ia.values / management.total_assets.values)
        dependencies = dependencies + ['ia']
        management = management.drop(dependencies, axis=1)
        factor_management = pd.merge(factor_management, management, how='outer', on="security_code")
        # factor_management['IntangibleAssetRatio'] = management['IntangibleAssetRatio']
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
        management['FixAssetsRt'] = np.where(
            CalcTools.is_zero(management.total_assets.values), 0,
            (management.fixed_assets.values +
             management.construction_materials.values +
             management.constru_in_process.values) / management.total_assets.values)
        management = management.drop(dependencies, axis=1)
        factor_management = pd.merge(factor_management, management, how='outer', on="security_code")
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
        management['EquityToAsset'] = np.where(
            CalcTools.is_zero(management.total_assets.values), 0,
            management.total_owner_equities.values / management.total_assets.values)
        management = management.drop(dependencies, axis=1)
        factor_management = pd.merge(factor_management, management, how='outer', on="security_code")
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
        management['EquityToFixedAsset'] = np.where(
            CalcTools.is_zero(management.fixed_assets.values +
                              management.construction_materials.values +
                              management.constru_in_process.values), 0,
            management.total_owner_equities.values
            / (management.fixed_assets.values
               + management.construction_materials.values
               + management.constru_in_process.values))
        management = management.drop(dependencies, axis=1)
        factor_management = pd.merge(factor_management, management, how='outer', on="security_code")
        return factor_management

    @staticmethod
    def current_assets_ratio(tp_management, factor_management, dependencies=['total_current_assets', 'total_assets']):
        """
        流动资产比率
        流动资产比率 = 流动资产合计M/总资产M
        :param dependencies:
        :param tp_management:
        :param factor_management:
        :return:
        """

        management = tp_management.loc[:, dependencies]
        management['CurAssetsR'] = np.where(
            CalcTools.is_zero(management.total_assets.values), 0,
            management.total_current_assets.values / management.total_assets.values)
        management = management.drop(dependencies, axis=1)
        factor_management = pd.merge(factor_management, management, how='outer', on="security_code")
        return factor_management


def calculate(trade_date, tp_management, factor_name):  # 计算对应因子
    tp_management = tp_management.set_index('security_code')

    # 读取目前涉及到的因子
    management = CapitalStructure(factor_name)  # 注意, 这里的name要与client中新建table时的name一致, 不然回报错
    # 因子计算
    factor_management = pd.DataFrame()
    factor_management['security_code'] = tp_management.index
    factor_management = factor_management.set_index('security_code')

    factor_management = management.non_current_assets_ratio(tp_management, factor_management)
    factor_management = management.long_term_debt_to_asset(tp_management, factor_management)
    factor_management = management.long_debt_to_asset(tp_management, factor_management)
    factor_management = management.intangible_asset_ratio(tp_management, factor_management)
    factor_management = management.fix_asset_ratio(tp_management, factor_management)
    factor_management = management.equity_to_asset(tp_management, factor_management)
    factor_management = management.equity_fixed_asset_ratio(tp_management, factor_management)
    factor_management = management.current_assets_ratio(tp_management, factor_management)

    factor_management = factor_management.reset_index()
    factor_management['trade_date'] = str(trade_date)
    print(factor_management.head())
    management._storage_data(factor_management, trade_date)
    del management, factor_management
    gc.collect()


# @app.task()
def factor_calculate(**kwargs):
    print("management_kwargs: {}".format(kwargs))
    date_index = kwargs['date_index']
    session = kwargs['session']
    factor_name = kwargs['factor_name']
    content1 = cache_data.get_cache(session + str(date_index) + "1", date_index)
    content2 = cache_data.get_cache(session + str(date_index) + "2", date_index)
    tp_management = json_normalize(json.loads(str(content1, encoding='utf8')))
    ttm_management = json_normalize(json.loads(str(content2, encoding='utf8')))
    tp_management.set_index('security_code', inplace=True)
    ttm_management.set_index('security_code', inplace=True)
    print("len_tp_management_data {}".format(len(tp_management)))
    print("len_ttm_management_data {}".format(len(ttm_management)))
    total_cash_flow_data = {'tp_management': tp_management, 'ttm_management': ttm_management}
    calculate(date_index, total_cash_flow_data, factor_name)
