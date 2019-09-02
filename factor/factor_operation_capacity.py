#! /usr/bin/env python
# -*- coding: utf-8 -*-

"""
@version:
@author: Wang
@file: factor_operation_capacity.py
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
    """
    营运能力
    """
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
                    `blev` decimal(19,4),
                    `mlev` decimal(19,4),
                    `super_quick_ratio` decimal(19,4),
                    `tsep_to_interest_bear_debt` decimal(19,4),
                    `tsep_to_total_capital` decimal(19,4),
                    `equity_t_rate_ttm` decimal(19,4),
                    `total_profit_cost_ratio_ttm` decimal(19,4),
                    `bonds_payable_to_asset` decimal(19,4),
                    `tangible_a_to_inte_bear_debt` decimal(19,4),
                    `tangible_a_to_net_debt` decimal(19,4),

                    
                    `non_current_assets_ratio` decimal(19,4),
                    `long_term_debt_to_asset` decimal(19,4),
                    `long_debt_to_asset` decimal(19,4),
                    `intangible_asset_ratio` decimal(19,4),
                    `fix_asset_ratio` decimal(19,4),
                    `equity_to_asset` decimal(19,4),
                    `equity_fixed_asset_ratio` decimal(19,4),
                    `current_assets_ratio` decimal(19,4),

                    `accounts_payables_t_rate_ttm` decimal(19,4),
                    `accounts_payables_t_days_ttm` decimal(19,4),
                    `ar_t_rate_ttm` decimal(19,4),
                    `ar_t_days_ttm` decimal(19,4),
                    `cash_conversion_cycle` decimal(19,4),
                    `current_assets_t_rate_ttm` decimal(19,4),
                    `fixed_assets_t_rate_ttm` decimal(19,4),
                    `inventory_t_rate_ttm` decimal(19,4),
                    `inventory_t_days_ttm` decimal(19,4),
                    `operating_cycle` decimal(19,4),
                    `total_assets_t_rate_ttm` decimal(19,4),
                    

                    PRIMARY KEY(`id`,`trade_date`,`symbol`)
                    )ENGINE=InnoDB DEFAULT CHARSET=utf8;""".format(self._name)
        super(FactorManagement, self)._create_tables(create_sql, drop_sql)

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
    factor_management = management.equity_to_asset(tp_management, factor_management)
    factor_management = management.total_profit_cost_ratio_ttm(ttm_management, factor_management)
    factor_management = management.bonds_payable_to_asset(tp_management, factor_management)
    factor_management = management.non_current_assets_ratio(tp_management, factor_management)
    factor_management = management.intangible_asset_ratio(tp_management, factor_management)
    factor_management = management.tangible_a_to_inte_bear_debt(tp_management, factor_management)
    factor_management = management.tangible_a_to_net_debt(tp_management, factor_management)


    factor_management = management.accounts_payables_t_days_ttm(ttm_management, factor_management)
    factor_management = management.accounts_payables_t_rate_ttm(ttm_management, factor_management)
    factor_management = management.ar_t_days_ttm(ttm_management, factor_management)
    factor_management = management.ar_t_rate_ttm(ttm_management, factor_management)
    factor_management = management.cash_conversion_cycle(factor_management)
    factor_management = management.current_assets_t_rate_ttm(ttm_management, factor_management)
    factor_management = management.fixed_assets_t_rate_ttm(ttm_management, factor_management)
    factor_management = management.inventory_t_rate_ttm(ttm_management, factor_management)
    factor_management = management.inventory_t_days_ttm(ttm_management, factor_management)
    factor_management = management.operating_cycle(factor_management)
    factor_management = management.total_assets_t_rate_ttm(ttm_management, factor_management)

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
