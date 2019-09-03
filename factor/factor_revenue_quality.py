#!/usr/bin/env python
# coding=utf-8

import sys
sys.path.append("../")
sys.path.append("../../")
sys.path.append("../../../")
import numpy as np
import pandas as pd
import json
from pandas.io.json import json_normalize
from factor.factor_base import FactorBase
from factor.utillities.calc_tools import CalcTools

from factor import app
from ultron.cluster.invoke.cache_data import cache_data


class RevenueQuality(FactorBase):
    """
    收益质量
    """
    def __init__(self, name):
        super(RevenueQuality, self).__init__(name)

    # 构建因子表
    def create_dest_tables(self):
        drop_sql = """drop table if exists `{0}`""".format(self._name)
        create_sql = """create table `{0}`(
                    `id` varchar(32) NOT NULL,
                    `symbol` varchar(24) NOT NULL,
                    `trade_date` date NOT NULL,
                    `SalesCostTTM` decimal(19,4),
                    `TaxRTTM` decimal(19,4),
                    `FinExpTTM` decimal(19,4),
                    `OperExpTTM` decimal(19,4),
                    `AdminExpTTM` decimal(19,4),
                    `PeridCostTTM` decimal(19,4),
                    `DTE` decimal(19,4),
                    `DA` decimal(19,4),
                    `IntBDToCap` decimal(19,4),
                     PRIMARY KEY(`id`,`trade_date`,`symbol`)
                    )ENGINE=InnoDB DEFAULT CHARSET=utf8;""".format(self._name)
        super(RevenueQuality, self)._create_tables(create_sql, drop_sql)

    @staticmethod
    def net_non_oi_to_tp_latest(tp_earning, factor_earning, dependencies=['total_profit', 'non_operating_revenue', 'non_operating_expense']):
        """
        营业外收支净额/利润总额
        :param dependencies:
        :param tp_earning:
        :param factor_earning:
        :return:
        """

        earning = tp_earning.loc[:, dependencies]
        earning['net_non_oi_to_tp_latest'] = np.where(
            CalcTools.is_zero(earning.total_profit.values), 0,
            (earning.non_operating_revenue.values +
             earning.non_operating_expense.values)
            / earning.total_profit.values
            )
        earning = earning.drop(dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, earning, on="symbol")
        return factor_earning

    @staticmethod
    def net_non_oi_to_tp_ttm(ttm_earning, factor_earning, dependencies=['total_profit', 'non_operating_revenue', 'non_operating_expense']):
        """
        营业外收支净额/利润总额
        :param dependencies:
        :param ttm_earning:
        :param factor_earning:
        :return:
        """
        earning = ttm_earning.loc[:, dependencies]
        earning['net_non_oi_to_tp_ttm'] = np.where(
            CalcTools.is_zero(earning.total_profit.values), 0,
            (earning.non_operating_revenue.values +
             earning.non_operating_expense.values)
            / earning.total_profit.values
            )
        earning = earning.drop(dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, earning, on="symbol")
        return factor_earning

    @staticmethod
    def operating_ni_to_tp_ttm(ttm_earning, factor_earning, dependencies=['total_operating_revenue', 'total_operating_cost', 'total_profit']):
        """
        经营活动净收益/利润总额
        （注，对于非金融企业 经营活动净收益=营业总收入-营业总成本；
        对于金融企业 经营活动净收益=营业收入-公允价值变动损益-投资收益-汇兑损益-营业支出
        此处以非金融企业的方式计算）
        :param dependencies:
        :param ttm_earning:
        :param factor_earning:
        :return:
        """
        earning = ttm_earning.loc[:, dependencies]
        earning['operating_ni_to_tp_ttm'] = np.where(
            CalcTools.is_zero(earning.total_profit.values), 0,
            (earning.total_operating_revenue.values -
             earning.total_operating_cost.values)
            / earning.total_profit.values)
        earning = earning.drop(dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, earning, on="symbol")
        return factor_earning

    @staticmethod
    def operating_ni_to_tp_latest(tp_earning, factor_earning, dependencies=['total_operating_revenue', 'total_operating_cost', 'total_profit']):
        """
        经营活动净收益/利润总额
        （注，对于非金融企业 经营活动净收益=营业总收入-营业总成本；
        对于金融企业 经营活动净收益=营业收入-公允价值变动损益-投资收益-汇兑损益-营业支出
        此处以非金融企业的方式计算）
        :param dependencies:
        :param tp_earning:
        :param factor_earning:
        :return:
        """
        earning = tp_earning.loc[:, dependencies]
        earning['operating_ni_to_tp_latest'] = np.where(
            CalcTools.is_zero(earning.total_profit.values), 0,
            (earning.total_operating_revenue.values -
             earning.total_operating_cost.values)
            / earning.total_profit.values)
        earning = earning.drop(dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, earning, on="symbol")
        return factor_earning

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
        factor_cash_flow = pd.merge(factor_cash_flow, cash_flow, on="symbol")
        return factor_cash_flow


def calculate(trade_date, total_constrain_data_dic, constrain):  # 计算对应因子
    balance_sets = total_constrain_data_dic['balance_sets']
    ttm_factors_sets = total_constrain_data_dic['ttm_factors_sets']

    factor_contrarian = pd.DataFrame()
    factor_contrarian['symbol'] = balance_sets.index
    # 非TTM计算
    factor_contrarian = constrain.inte_bear_debt_to_total_capital_latest(balance_sets, factor_contrarian)
    factor_contrarian = constrain.debts_asset_ratio_latest(balance_sets, factor_contrarian)
    factor_contrarian = constrain.debt_tangible_equity_ratio_latest(balance_sets, factor_contrarian)

    # TTM计算
    factor_contrarian = constrain.sales_cost_ratio_ttm(ttm_factors_sets, factor_contrarian)
    factor_contrarian = constrain.tax_ratio_ttm(ttm_factors_sets, factor_contrarian)
    factor_contrarian = constrain.financial_expense_rate_ttm(ttm_factors_sets, factor_contrarian)
    factor_contrarian = constrain.operating_expense_rate_ttm(ttm_factors_sets, factor_contrarian)
    factor_contrarian = constrain.admini_expense_rate_ttm(ttm_factors_sets, factor_contrarian)
    factor_contrarian = constrain.period_costs_rate_ttm(ttm_factors_sets, factor_contrarian)
    factor_contrarian['id'] = factor_contrarian['symbol'] + str(trade_date)
    factor_contrarian['trade_date'] = str(trade_date)
    constrain._storage_data(factor_contrarian, trade_date)


@app.task()
def factor_calculate(**kwargs):
    print("constrain_kwargs: {}".format(kwargs))
    date_index = kwargs['date_index']
    session = kwargs['session']
    constrain = RevenueQuality('factor_constrain')  # 注意, 这里的name要与client中新建table时的name一致, 不然会报错
    content1 = cache_data.get_cache(session + str(date_index) + '1', date_index)
    content2 = cache_data.get_cache(session + str(date_index) + '2', date_index)
    balance_sets = json_normalize(json.loads(str(content1, encoding='utf8')))
    ttm_factors_sets = json_normalize(json.loads(str(content2, encoding='utf8')))
    balance_sets.set_index('symbol', inplace=True)
    ttm_factors_sets.set_index('symbol', inplace=True)
    print("len_constrain_data {}".format(len(balance_sets)))
    print("len_ttm_constrain_data {}".format(len(ttm_factors_sets)))
    total_constrain_data_dic = {'balance_sets': balance_sets, 'ttm_factors_sets': ttm_factors_sets}
    calculate(date_index, total_constrain_data_dic, constrain)
