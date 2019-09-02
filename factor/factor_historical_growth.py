#! /usr/bin/env python
# -*- coding: utf-8 -*-

"""
@version: 0.1
@author: li
@file: factor_historical_growth.py
@time: 2019-02-12 10:03
"""
import json
import numpy as np
import pandas as pd
from sklearn import linear_model
from pandas.io.json import json_normalize

from factor import app
from factor.factor_base import FactorBase
from factor.ttm_fundamental import *
from ultron.cluster.invoke.cache_data import cache_data


class Growth(FactorBase):
    """
    历史成长
    """
    def __init__(self, name):
        super(Growth, self).__init__(name)

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
                    `NetAsset1YChg` decimal(19,4),
                    `TotalAsset1YChg` decimal(19,4),
                    `ORev1YChgTTM` decimal(19,4),
                    `OPft1YChgTTM` decimal(19,4),
                    `GrPft1YChgTTM` decimal(19,4),
                    `NetPft1YChgTTM` decimal(19,4),
                    `NetPftAP1YChgTTM` decimal(19,4),
                    `NetPft3YChgTTM` decimal(19,4),
                    `NetPft5YChgTTM` decimal(19,4),
                    `ORev3YChgTTM` decimal(19,4),
                    `ORev5YChgTTM` decimal(19,4),
                    `NetCF1YChgTTM` decimal(19,4),
                    `NetPftAPNNRec1YChgTTM` decimal(19,4),
                    `NetPft5YAvgChgTTM` decimal(19,4),
                    `StdUxpErn1YTTM` decimal(19,4),
                    `StdUxpGrPft1YTTM` decimal(19,4),
                    `FCF1YChgTTM` decimal(19,4),
                    `ICF1YChgTTM` decimal(19,4),
                    `OCF1YChgTTM` decimal(19,4),
                    `Sales5YChgTTM` decimal(19,4),
                    PRIMARY KEY(`id`,`trade_date`,`symbol`)
                    )ENGINE=InnoDB DEFAULT CHARSET=utf8;""".format(self._name)
        super(Growth, self)._create_tables(create_sql, drop_sql)

    @staticmethod
    def historical_financing_cash_grow_rate(tp_historical_growth, factor_historical_growth, dependencies=['net_finance_cash_flow', 'net_finance_cash_flow_pre_year']):
        """
        筹资活动产生的现金流量净额增长率
        :param dependencies:
        :param tp_historical_growth:
        :param factor_historical_growth:
        :return:
        """

        historical_growth = tp_historical_growth.loc[:, dependencies]
        if len(historical_growth) <= 0:
            return
        fun = lambda x: ((x[0] / x[1]) - 1 if x[1] and x[1] != 0 and x[1] is not None and x[0] is not None else None)
        historical_growth['FCF1YChgTTM'] = historical_growth[dependencies].apply(fun, axis=1)

        historical_growth = historical_growth.drop(columns=dependencies,
                                                   axis=1)
        factor_historical_growth = pd.merge(factor_historical_growth, historical_growth, on='symbol')
        return factor_historical_growth

    @staticmethod
    def degm(ttm_earning, ttm_earning_p1y, factor_earning, dependencies=['operating_revenue', 'operating_cost']):
        """
        毛利率增长率，与去年同期相比
        :param dependencies:
        :param ttm_earning_p1y:
        :param ttm_earning:
        :param factor_earning:
        :return:
        """

        earning = ttm_earning.loc[:, dependencies]
        earning_p1y = ttm_earning_p1y.loc[:, dependencies]
        earning['gross_income_ratio'] = np.where(
            CalcTools.is_zero(earning.operating_revenue.values), 0,
            (earning.operating_revenue.values -
             earning.operating_cost.values)
            / earning.operating_revenue.values
                )
        earning_p1y['gross_income_ratio'] = np.where(
            CalcTools.is_zero(earning_p1y.operating_revenue.values), 0,
            (earning_p1y.operating_revenue.values -
             earning_p1y.operating_cost.values)
            / earning_p1y.operating_revenue.values)
        earning["degm"] = earning["gross_income_ratio"] - earning_p1y["gross_income_ratio"]
        dependencies.append('gross_income_ratio')
        earning = earning.drop(dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, earning, on="symbol")
        return factor_earning

    @staticmethod
    def historical_total_profit_grow_rate(tp_historical_growth, factor_historical_growth, dependencies=['total_profit', 'total_profit_pre_year']):
        """
        利润总额增长率
        :param dependencies:
        :param tp_historical_growth:
        :param factor_historical_growth:
        :return:
        """

        historical_growth = tp_historical_growth.loc[:, dependencies]
        if len(historical_growth) <= 0:
            return
        fun = lambda x: ((x[0] / x[1]) - 1 if x[1] and x[1] != 0 and x[0] is not None and x[1] is not None else None)
        historical_growth['GrPft1YChgTTM'] = historical_growth[dependencies].apply(fun, axis=1)

        historical_growth = historical_growth.drop(columns=dependencies, axis=1)
        factor_historical_growth = pd.merge(factor_historical_growth, historical_growth, on='symbol')
        return factor_historical_growth

    @staticmethod
    def historical_invest_cash_grow_rate(tp_historical_growth, factor_historical_growth, dependencies=['net_invest_cash_flow', 'net_invest_cash_flow_pre_year']):
        """
        投资活动产生的现金流量净额增长率
        :param dependencies:
        :param tp_historical_growth:
        :param factor_historical_growth:
        :return:
        """

        historical_growth = tp_historical_growth.loc[:, dependencies]
        if len(historical_growth) <= 0:
            return
        fun = lambda x: ((x[0] / x[1]) - 1 if x[1] and x[1] != 0 and x[1] is not None and x[0] is not None else None)
        historical_growth['ICF1YChgTTM'] = historical_growth[dependencies].apply(fun, axis=1)

        historical_growth = historical_growth.drop(columns=dependencies,
                                                   axis=1)
        factor_historical_growth = pd.merge(factor_historical_growth, historical_growth, on='symbol')
        return factor_historical_growth


    @staticmethod
    def historical_net_asset_grow_rate(tp_historical_growth, factor_historical_growth, dependencies=['total_owner_equities', 'total_owner_equities_pre_year']):
        """
        净资产增长率
        :param factor_historical_growth:
        :param dependencies:
        :param tp_historical_growth:
        :return:
        """

        historical_growth = tp_historical_growth.loc[:, dependencies]

        if len(historical_growth) <= 0:
            return

        fun = lambda x: ((x[0] / x[1]) - 1.0 if x[1] and x[1] != 0 and x[0] is not None and x[1] is not None else None)

        historical_growth['NetAsset1YChg'] = historical_growth[dependencies].apply(fun, axis=1)

        historical_growth = historical_growth.drop(dependencies, axis=1)
        factor_historical_growth = pd.merge(factor_historical_growth, historical_growth, on='symbol')
        return factor_historical_growth

    @staticmethod
    def historical_net_cash_flow_grow_rate(tp_historical_growth, factor_historical_growth, dependencies=['n_change_in_cash', 'n_change_in_cash_pre']):
        """
        缺数据
        净现金流量增长率
        :param dependencies:
        :param tp_historical_growth:
        :param factor_historical_growth:
        :return:
        """

        historical_growth = tp_historical_growth.loc[:, dependencies]
        if len(historical_growth) <= 0:
            return
        fun = lambda x: ((x[0] / x[1]) - 1 if x[1] and x[1] != 0 and x[0] is not None and x[1] is not None else None)
        historical_growth['NetCF1YChgTTM'] = historical_growth[dependencies].apply(fun, axis=1)

        historical_growth = historical_growth.drop(columns=dependencies, axis=1)
        factor_historical_growth = pd.merge(factor_historical_growth, historical_growth, on='symbol')
        return factor_historical_growth

    @staticmethod
    def historical_np_parent_company_grow_rate(tp_historical_growth, factor_historical_growth, dependencies=['np_parent_company_owners', 'np_parent_company_owners_pre_year']):
        """
        归属母公司股东的净利润增长率
        :param dependencies:
        :param tp_historical_growth:
        :param factor_historical_growth:
        :return:
        """

        historical_growth = tp_historical_growth.loc[:, dependencies]
        if len(historical_growth) <= 0:
            return

        fun = lambda x: ((x[0] / x[1]) - 1 if x[1] and x[1] != 0 and x[0] is not None and x[1] is not None else None)
        historical_growth['NetPftAP1YChgTTM'] = historical_growth[dependencies].apply(fun, axis=1)

        historical_growth = historical_growth.drop(columns=dependencies, axis=1)
        factor_historical_growth = pd.merge(factor_historical_growth, historical_growth, on='symbol')
        return factor_historical_growth

    @staticmethod
    def historical_np_parent_company_cut_yoy(tp_historical_growth, factor_historical_growth, dependencies=['ni_attr_p_cut', 'ni_attr_p_cut_pre']):
        """
        缺失数据
        归属母公司股东的净利润（扣除）同比增长
        :param dependencies:
        :param tp_historical_growth:
        :param factor_historical_growth:
        :return:
        """

        historical_growth = tp_historical_growth.loc[:, dependencies]
        if len(historical_growth) <= 0:
            return
        fun = lambda x: ((x[0] / x[1]) - 1 if x[1] and x[1] != 0 and x[0] is not None and x[1] is not None else None)
        historical_growth['NetPftAPNNRec1YChgTTM'] = historical_growth[dependencies].apply(fun, axis=1)

        historical_growth = historical_growth.drop(columns=dependencies, axis=1)
        factor_historical_growth = pd.merge(factor_historical_growth, historical_growth, on='symbol')
        return factor_historical_growth

    @staticmethod
    def historical_net_profit_grow_rate(tp_historical_growth, factor_historical_growth, dependencies=['net_profit', 'net_profit_pre_year']):
        """
        净利润增长率
        :param dependencies:
        :param tp_historical_growth:
        :param factor_historical_growth:
        :return:
        """

        historical_growth = tp_historical_growth.loc[:, dependencies]
        if len(historical_growth) <= 0:
            return
        fun = lambda x: ((x[0] / x[1]) - 1 if x[1] and x[1] != 0 and x[0] is not None and x[1] is not None else None)
        historical_growth['NetPft1YChgTTM'] = historical_growth[dependencies].apply(fun, axis=1)

        historical_growth = historical_growth.drop(columns=dependencies, axis=1)
        factor_historical_growth = pd.merge(factor_historical_growth, historical_growth, on='symbol')
        return factor_historical_growth

    @staticmethod
    def historical_oper_cash_grow_rate(tp_historical_growth, factor_historical_growth, dependencies=['net_operate_cash_flow', 'net_operate_cash_flow_pre_year']):
        """
        经营活动产生的现金流量净额
        :param dependencies:
        :param tp_historical_growth:
        :param factor_historical_growth:
        :return:
        """

        historical_growth = tp_historical_growth.loc[:, dependencies]
        if len(historical_growth) <= 0:
            return
        fun = lambda x: ((x[0] / x[1]) - 1 if x[1] and x[1] != 0 and x[1] is not None and x[0] is not None else None)
        historical_growth['OCF1YChgTTM'] = historical_growth[dependencies].apply(fun, axis=1)

        historical_growth = historical_growth.drop(columns=dependencies,
                                                   axis=1)
        factor_historical_growth = pd.merge(factor_historical_growth, historical_growth, on='symbol')
        return factor_historical_growth

    @staticmethod
    def historical_operating_profit_grow_rate(tp_historical_growth, factor_historical_growth, dependencies=['operating_profit', 'operating_profit_pre_year']):
        """
        营业利润增长率
        :param dependencies:
        :param tp_historical_growth:
        :param factor_historical_growth:
        :return:
        """

        historical_growth = tp_historical_growth.loc[:, dependencies]
        if len(historical_growth) <= 0:
            return
        fun = lambda x: ((x[0] / x[1]) - 1.0 if x[1] and x[1] != 0 and x[0] is not None and x[1] is not None else None)
        historical_growth['OPft1YChgTTM'] = historical_growth[dependencies].apply(fun, axis=1)

        historical_growth = historical_growth.drop(columns=dependencies, axis=1)
        factor_historical_growth = pd.merge(factor_historical_growth, historical_growth, on='symbol')
        return factor_historical_growth

    @staticmethod
    def historical_operating_revenue_grow_rate(tp_historical_growth, factor_historical_growth, dependencies=['operating_revenue', 'operating_revenue_pre_year']):
        """
        营业收入增长率
        :param dependencies:
        :param tp_historical_growth:
        :param factor_historical_growth:
        :return:
        """

        historical_growth = tp_historical_growth.loc[:, dependencies]
        if len(historical_growth) <= 0:
            return

        fun = lambda x: ((x[0] / x[1]) - 1.0 if x[1] and x[1] != 0 and x[0] is not None and x[1] is not None else None)
        historical_growth['ORev1YChgTTM'] = historical_growth[dependencies].apply(fun, axis=1)

        historical_growth = historical_growth.drop(columns=dependencies, axis=1)
        factor_historical_growth = pd.merge(factor_historical_growth, historical_growth, on='symbol')
        return factor_historical_growth

    @staticmethod
    def historical_total_asset_grow_rate(tp_historical_growth, factor_historical_growth, dependencies=['total_assets', 'total_assets_pre_year']):
        """
        总资产增长率
        :param dependencies:
        :param tp_historical_growth:
        :param factor_historical_growth:
        :return:
        """

        historical_growth = tp_historical_growth.loc[:, dependencies]
        if len(historical_growth) <= 0:
            return
        fun = lambda x: ((x[0] / x[1]) - 1.0 if x[1] and x[1] != 0 and x[0] is not None and x[1] is not None else None)
        historical_growth['TotalAsset1YChg'] = historical_growth[dependencies].apply(fun, axis=1)

        historical_growth = historical_growth.drop(columns=dependencies, axis=1)
        factor_historical_growth = pd.merge(factor_historical_growth, historical_growth, on='symbol')
        return factor_historical_growth

    @staticmethod
    def historical_sue(tp_historical_growth, factor_historical_growth, dependencies=['net_profit', 'net_profit_pre_year_1', 'net_profit_pre_year_2', 'net_profit_pre_year_3', 'net_profit_pre_year_4']):
        """
        未预期盈余
        :param dependencies:
        :param tp_historical_growth:
        :param factor_historical_growth:
        :return:
        """

        historical_growth = tp_historical_growth.loc[:, dependencies]
        if len(historical_growth) <= 0:
            return
        historical_growth['mean'] = historical_growth[dependencies].fillna(0.0).mean(axis=1)
        historical_growth['std'] = historical_growth[dependencies].fillna(0.0).std(axis=1)

        fun = lambda x: (x[0] - x[1]) / x[2] if x[2] !=0 and x[1] is not None and x[0] is not None and x[2] is not None else None

        historical_growth['StdUxpErn1YTTM'] = historical_growth[['net_profit', 'mean', 'std']].apply(fun, axis=1)

        # historical_growth = historical_growth.drop(columns=['net_profit', 'std', 'mean'], axis=1)
        historical_growth = historical_growth[['StdUxpErn1YTTM']]
        factor_historical_growth = pd.merge(factor_historical_growth, historical_growth, on='symbol')

        return factor_historical_growth

    @staticmethod
    def historical_suoi(tp_historical_growth, factor_historical_growth, dependencies=['operating_revenue', 'operating_revenue_pre_year_1', 'operating_revenue_pre_year_2',
                                                                                      'operating_revenue_pre_year_3', 'operating_revenue_pre_year_4', 'operating_revenue_pre_year_5',
                                                                                      'operating_cost', 'operating_cost_pre_year_1', 'operating_cost_pre_year_2',
                                                                                      'operating_cost_pre_year_3', 'operating_cost_pre_year_4', 'operating_cost_pre_year_5']):
        """
        未预期毛利
        :param tp_historical_growth:
        :param factor_historical_growth:
        :return:
        """

        historical_growth = tp_historical_growth.loc[:, dependencies]
        if len(historical_growth) <= 0:
            return
        historical_growth['gi_1'] = historical_growth['operating_revenue'] - historical_growth['operating_cost']
        historical_growth['gi_2'] = historical_growth['operating_revenue_pre_year_2'] - historical_growth[
            'operating_cost_pre_year_2']
        historical_growth['gi_3'] = historical_growth['operating_revenue_pre_year_3'] - historical_growth[
            'operating_cost_pre_year_3']
        historical_growth['gi_4'] = historical_growth['operating_revenue_pre_year_4'] - historical_growth[
            'operating_cost_pre_year_4']
        historical_growth['gi_5'] = historical_growth['operating_revenue_pre_year_5'] - historical_growth[
            'operating_cost_pre_year_5']

        historical_growth['mean'] = historical_growth[['gi_2', 'gi_3', 'gi_4', 'gi_5']].mean(axis=1)
        historical_growth['std'] = historical_growth[['gi_2', 'gi_3', 'gi_4', 'gi_5']].std(axis=1)

        fun = lambda x: ((x[0] - x[1]) / x[2] if x[2] != 0 and x[1] is not None and x[0] is not None and x[2] is not None else None)
        historical_growth['StdUxpGrPft1YTTM'] = historical_growth[['gi_1', 'mean', 'std']].apply(fun, axis=1)

        historical_growth = historical_growth[['StdUxpGrPft1YTTM']]
        factor_historical_growth = pd.merge(factor_historical_growth, historical_growth, on='symbol')

        return factor_historical_growth

    @staticmethod
    def historical_net_profit_grow_rate_3y(tp_historical_growth, factor_historical_growth, dependencies=['net_profit', 'net_profit_pre_year_3']):
        """
        净利润3年复合增长率
        :param dependencies:
        :param tp_historical_growth:
        :param factor_historical_growth:
        :return:
        """

        historical_growth = tp_historical_growth.loc[:, dependencies]
        if len(historical_growth) <= 0:
            return
        fun = lambda x: (pow((x[0] / x[1]), 1 / 3.0) - 1 if x[1] and x[1] != 0 and x[0] is not None and x[1] is not None else None)
        historical_growth['NetPft3YChgTTM'] = historical_growth[dependencies].apply(fun, axis=1)

        historical_growth = historical_growth.drop(columns=dependencies, axis=1)
        factor_historical_growth = pd.merge(factor_historical_growth, historical_growth, on='symbol')
        return factor_historical_growth

    @staticmethod
    def historical_net_profit_grow_rate_5y(tp_historical_growth, factor_historical_growth, dependencies=['net_profit', 'net_profit_pre_year_5']):
        """
        净利润5年复合增长率
        :param dependencies:
        :param tp_historical_growth:
        :param factor_historical_growth:
        :return:
        """

        historical_growth = tp_historical_growth.loc[:, dependencies]
        if len(historical_growth) <= 0:
            return
        fun = lambda x: (pow((x[0] / x[1]), 1 / 5.0) - 1 if x[1] and x[1] != 0 and x[0] is not None and x[1] is not None else None)
        historical_growth['NetPft5YChgTTM'] = historical_growth[dependencies].apply(fun, axis=1)

        historical_growth = historical_growth.drop(columns=dependencies, axis=1)
        factor_historical_growth = pd.merge(factor_historical_growth, historical_growth, on='symbol')
        return factor_historical_growth

    @staticmethod
    def historical_operating_revenue_grow_rate_3y(tp_historical_growth, factor_historical_growth, dependencies=['operating_revenue', 'operating_revenue_pre_year_3']):
        """
        营业收入3年复合增长率
        :param dependencies:
        :param tp_historical_growth:
        :param factor_historical_growth:
        :return:
        """

        historical_growth = tp_historical_growth.loc[:, dependencies]
        if len(historical_growth) <= 0:
            return
        fun = lambda x: (pow((x[0] / x[1]), 1 / 3.0) - 1 if x[1] and x[1] != 0 and x[0] is not None and x[1] is not None else None)

        historical_growth['ORev3YChgTTM'] = historical_growth[dependencies].apply(fun, axis=1)

        historical_growth = historical_growth.drop(columns=dependencies, axis=1)
        factor_historical_growth = pd.merge(factor_historical_growth, historical_growth, on='symbol')
        return factor_historical_growth

    @staticmethod
    def historical_operating_revenue_grow_rate_5y(tp_historical_growth, factor_historical_growth, dependencies=['operating_revenue', 'operating_revenue_pre_year_3']):
        """
        营业收入5年复合增长率
        :param dependencies:
        :param tp_historical_growth:
        :param factor_historical_growth:
        :return:
        """

        historical_growth = tp_historical_growth.loc[:, dependencies]
        if len(historical_growth) <= 0:
            return
        fun = lambda x: (pow((x[0] / x[1]), 1 / 5.0) - 1 if x[1] and x[1] != 0 and x[0] is not None and x[1] is not None else None)

        historical_growth['ORev5YChgTTM'] = historical_growth[dependencies].apply(fun, axis=1)

        historical_growth = historical_growth.drop(columns=dependencies,
                                                   axis=1)
        factor_historical_growth = pd.merge(factor_historical_growth, historical_growth, on='symbol')
        return factor_historical_growth


def calculate(trade_date, growth_sets, growth):
    """
    :param growth: 成长类
    :param growth_sets: 基础数据
    :param trade_date: 交易日
    :return:
        """
    if len(growth_sets) <= 0:
        print("%s has no data" % trade_date)
        return

    factor_historical_growth = pd.DataFrame()
    factor_historical_growth['symbol'] = growth_sets.index

    factor_historical_growth = growth.historical_net_asset_grow_rate(growth_sets, factor_historical_growth)
    factor_historical_growth = growth.historical_total_asset_grow_rate(growth_sets, factor_historical_growth)
    factor_historical_growth = growth.historical_operating_revenue_grow_rate(growth_sets, factor_historical_growth)
    factor_historical_growth = growth.historical_operating_profit_grow_rate(growth_sets, factor_historical_growth)
    factor_historical_growth = growth.historical_total_profit_grow_rate(growth_sets, factor_historical_growth)
    factor_historical_growth = growth.historical_net_profit_grow_rate(growth_sets, factor_historical_growth)
    factor_historical_growth = growth.historical_np_parent_company_grow_rate(growth_sets, factor_historical_growth)
    factor_historical_growth = growth.historical_net_profit_grow_rate_3y(growth_sets, factor_historical_growth)
    factor_historical_growth = growth.historical_net_profit_grow_rate_5y(growth_sets, factor_historical_growth)
    factor_historical_growth = growth.historical_operating_revenue_grow_rate_3y(growth_sets, factor_historical_growth)
    factor_historical_growth = growth.historical_operating_revenue_grow_rate_5y(factor_historical_growth,
                                                                                factor_historical_growth)
    factor_historical_growth = growth.historical_net_cash_flow_grow_rate(factor_historical_growth,
                                                                         factor_historical_growth)
    factor_historical_growth = growth.historical_np_parent_company_cut_yoy(factor_historical_growth,
                                                                           factor_historical_growth)
    factor_historical_growth = growth.historical_egro(factor_historical_growth, factor_historical_growth)
    factor_historical_growth = growth.historical_sue(factor_historical_growth, factor_historical_growth)
    factor_historical_growth = growth.historical_suoi(factor_historical_growth, factor_historical_growth)
    factor_historical_growth = growth.historical_financing_cash_grow_rate(factor_historical_growth,
                                                                          factor_historical_growth)
    factor_historical_growth = growth.historical_oper_cash_grow_rate(factor_historical_growth,
                                                                     factor_historical_growth)
    factor_historical_growth = growth.historical_invest_cash_grow_rate(factor_historical_growth,
                                                                       factor_historical_growth)
    factor_historical_growth = growth.historical_sgro(factor_historical_growth, factor_historical_growth)
    factor_historical_growth = factor_historical_growth[['symbol',
                                                         'NetAsset1YChg',
                                                         'TotalAsset1YChg',
                                                         'ORev1YChgTTM',
                                                         'OPft1YChgTTM',
                                                         'GrPft1YChgTTM',
                                                         'NetPft1YChgTTM',
                                                         'NetPftAP1YChgTTM',
                                                         'NetPft3YChgTTM',
                                                         'NetPft5YChgTTM',
                                                         'ORev3YChgTTM',
                                                         'ORev5YChgTTM',
                                                         'NetCF1YChgTTM',
                                                         'NetPftAPNNRec1YChgTTM',
                                                         'NetPft5YAvgChgTTM',
                                                         'StdUxpErn1YTTM',
                                                         'StdUxpGrPft1YTTM',
                                                         'FCF1YChgTTM',
                                                         'ICF1YChgTTM',
                                                         'OCF1YChgTTM',
                                                         'Sales5YChgTTM']]

    factor_historical_growth['id'] = factor_historical_growth['symbol'] + str(trade_date)
    factor_historical_growth['trade_date'] = str(trade_date)
    growth._storage_data(factor_historical_growth, trade_date)


@app.task()
def factor_calculate(**kwargs):
    print("growth_kwargs: {}".format(kwargs))
    date_index = kwargs['date_index']
    session = kwargs['session']
    growth = Growth('factor_growth')  # 注意, 这里的name要与client中新建table时的name一致, 不然回报错
    content = cache_data.get_cache(session, "growth" + str(date_index))
    total_growth_data = json_normalize(json.loads(str(content, encoding='utf8')))
    print("len_total_growth_data {}".format(len(total_growth_data)))
    calculate(date_index, total_growth_data, growth)

