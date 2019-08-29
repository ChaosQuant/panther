#! /usr/bin/env python
# -*- coding: utf-8 -*-

"""
每股指标
@version: 0.1
@author: li
@file: factor_per_share_indicators.py
@time: 2019-02-12 10:02
"""

import sys
sys.path.append("../")
sys.path.append("../../")
sys.path.append("../../../")

import pandas as pd
import json
from pandas.io.json import json_normalize
from factor.ttm_fundamental import *
from factor.factor_base import FactorBase

from factor import app
from ultron.cluster.invoke.cache_data import cache_data


class PerShareIndicators(FactorBase):
    """
    收益质量
        --每股指标
    """
    def __init__(self, name):
        super(PerShareIndicators, self).__init__(name)

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
                    `EPS` decimal(19,4),
                    `DilutedEPSTTM` decimal(19,4),
                    `CashEquPS` decimal(19,4),
                    `DivPS` decimal(19,4),
                    `EPSTTM` decimal(19,4),
                    `NetAssetPS` decimal(19,4),
                    `TotalRevPS` decimal(19,4),
                    `TotalRevPSTTM` decimal(19,4),
                    `OptRevPSTTM` decimal(19,4),
                    `OptRevPS` decimal(19,4),
                    `OptProfitPSTTM` decimal(19,4),
                    `OptProfitPS` decimal(19,4),
                    `CapticalSurplusPS` decimal(19,4),
                    `SurplusReservePS` decimal(19,4),
                    `UndividedProfitPS` decimal(19,4),
                    `RetainedEarningsPS` decimal(19,4),
                    `OptCFPSTTM` decimal(19,4),
                    `CFPSTTM` decimal(19,4),
                    `EnterpriseFCFPS` decimal(19,4),
                    `ShareholderFCFPS` decimal(19,4),
                    PRIMARY KEY(`id`,`trade_date`,`symbol`)
                    )ENGINE=InnoDB DEFAULT CHARSET=utf8;""".format(self._name)
        super(PerShareIndicators, self)._create_tables(create_sql, drop_sql)

    @staticmethod
    def eps(tp_share_indicators, factor_share_indicators, dependencies=['basic_eps']):
        """
        基本每股收益
        :param dependencies:
        :param tp_share_indicators:
        :param factor_share_indicators:
        :return:
        """

        share_indicators = tp_share_indicators.loc[:, dependencies]
        share_indicators['EPS'] = share_indicators['basic_eps']
        # share_indicators = share_indicators.drop(columns=['basic_eps'], axis=1)
        share_indicators = share_indicators[['symbol', 'EPS']]
        factor_share_indicators = pd.merge(factor_share_indicators, share_indicators, on='symbol')
        return factor_share_indicators

    @staticmethod
    def diluted_eps(tp_share_indicators, factor_share_indicators, dependencies=['diluted_eps']):
        """
        稀释每股收益
        :param dependencies:
        :param tp_share_indicators:
        :param factor_share_indicators:
        :return:
        """

        share_indicators = tp_share_indicators.loc[:, dependencies]
        share_indicators['DilutedEPSTTM'] = share_indicators['diluted_eps']
        # share_indicators = share_indicators.drop(columns=['diluted_eps'], axis=1)
        share_indicators = share_indicators[['symbol', 'DilutedEPSTTM']]

        factor_share_indicators = pd.merge(factor_share_indicators, share_indicators, on='symbol')
        return factor_share_indicators

    @staticmethod
    def cash_equivalent_ps(tp_share_indicators, factor_share_indicators, dependencies=['capitalization', 'cash_and_equivalents_at_end']):
        """
        每股现金及现金等价物余额
        :param dependencies:
        :param tp_share_indicators:
        :param factor_share_indicators:
        :return:
        """

        share_indicators = tp_share_indicators.loc[:, dependencies]
        fun = lambda x: (x[1] / x[0] if x[0] and x[0] != 0 else None)
        share_indicators['CashEquPS'] = share_indicators[
            ['capitalization', 'cash_and_equivalents_at_end']].apply(fun, axis=1)

        # share_indicators = share_indicators.drop(columns=['cash_and_equivalents_at_end'], axis=1)
        share_indicators = share_indicators[['symbol', 'CashEquPS']]

        factor_share_indicators = pd.merge(factor_share_indicators, share_indicators, on='symbol')
        return factor_share_indicators

    @staticmethod
    def dividend_ps(tp_share_indicators, factor_share_indicators, dependencies=['dividend_receivable']):
        """
        每股股利（税前）
        :param dependencies:
        :param tp_share_indicators:
        :param factor_share_indicators:
        :return:
        """

        share_indicators = tp_share_indicators.loc[:, dependencies]
        share_indicators['DivPS'] = share_indicators['dividend_receivable']
        # share_indicators = share_indicators.drop(columns=['dividend_receivable'], axis=1)
        share_indicators = share_indicators[['symbol', 'DivPS']]

        factor_share_indicators = pd.merge(factor_share_indicators, share_indicators, on='symbol')
        return factor_share_indicators

    @staticmethod
    def eps_ttm(tp_share_indicators, factor_share_indicators, dependencies=['np_parent_company_owners_ttm', 'capitalization']):
        """
        每股收益 TTM
        :param dependencies:
        :param tp_share_indicators:
        :param factor_share_indicators:
        :return:
        """

        share_indicators = tp_share_indicators.loc[:, dependencies]
        fun = lambda x: (x[0] / x[1] if x[1] and x[1] != 0 else 0)
        share_indicators['EPSTTM'] = share_indicators[['np_parent_company_owners_ttm', 'capitalization']].apply(fun,
                                                                                                                axis=1)

        # share_indicators = share_indicators.drop(columns=['np_parent_company_owners_ttm'], axis=1)
        share_indicators = share_indicators[['symbol', 'EPSTTM']]

        factor_share_indicators = pd.merge(factor_share_indicators, share_indicators, on='symbol')
        return factor_share_indicators

    @staticmethod
    def net_asset_ps(tp_share_indicators, factor_share_indicators, dependencies=['total_owner_equities', 'capitalization']):
        """
        每股净资产
        :param dependencies:
        :param tp_share_indicators:
        :param factor_share_indicators:
        :return:
        """

        share_indicators = tp_share_indicators.loc[:, dependencies]
        fun = lambda x: (x[0] / x[1] if x[1] and x[1] != 0 else None)
        share_indicators['NetAssetPS'] = share_indicators[['total_owner_equities', 'capitalization']].apply(fun,
                                                                                                                   axis=1)

        # share_indicators = share_indicators.drop(columns=['total_owner_equities'], axis=1)
        share_indicators = share_indicators[['symbol', 'NetAssetPS']]
        factor_share_indicators = pd.merge(factor_share_indicators, share_indicators, on='symbol')
        return factor_share_indicators

    @staticmethod
    def tor_ps(tp_share_indicators, factor_share_indicators, dependencies=['total_operating_revenue_ttm', 'capitalization']):
        """
        每股营业总收入
        :param dependencies:
        :param tp_share_indicators:
        :param factor_share_indicators:
        :return:
        """

        share_indicators = tp_share_indicators.loc[:, dependencies]
        fun = lambda x: (x[0] / x[1] if x[1] and x[1] != 0 else None)
        share_indicators['TotalRevPSTTM'] = share_indicators[dependencies].apply(fun, axis=1)

        # share_indicators = share_indicators.drop(columns=['total_operating_revenue_ttm'], axis=1)
        share_indicators = share_indicators[['symbol', 'TotalRevPSTTM']]

        factor_share_indicators = pd.merge(factor_share_indicators, share_indicators, on='symbol')
        return factor_share_indicators

    @staticmethod
    def tor_ps_latest(tp_share_indicators, factor_share_indicators, dependencies=['total_operating_revenue', 'capitalization']):
        """
        每股营业总收入
        :param dependencies:
        :param tp_share_indicators:
        :param factor_share_indicators:
        :return:
        """

        share_indicators = tp_share_indicators.loc[:, dependencies]
        fun = lambda x: (x[0] / x[1] if x[1] and x[1] != 0 else None)
        share_indicators['TotalRevPS'] = share_indicators[['total_operating_revenue', 'capitalization']].apply(fun, axis=1)

        # share_indicators = share_indicators.drop(columns=['total_operating_revenue'], axis=1)
        share_indicators = share_indicators[['symbol', 'TotalRevPS']]

        factor_share_indicators = pd.merge(factor_share_indicators, share_indicators, on='symbol')
        return factor_share_indicators

    @staticmethod
    def operating_revenue_ps(tp_share_indicators, factor_share_indicators, dependencies=['operating_revenue_ttm', 'capitalization']):
        """
        每股营业收入
        :param dependencies:
        :param tp_share_indicators:
        :param factor_share_indicators:
        :return:
        """

        share_indicators = tp_share_indicators.loc[:, dependencies]
        fun = lambda x: (x[0] / x[1] if x[1] and x[1] != 0 else None)
        share_indicators['OptRevPSTTM'] = share_indicators[dependencies].apply(
            fun,
            axis=1)

        # share_indicators = share_indicators.drop(columns=['operating_revenue_ttm'], axis=1)
        share_indicators = share_indicators[['symbol', 'OptRevPSTTM']]
        factor_share_indicators = pd.merge(factor_share_indicators, share_indicators, on='symbol')
        return factor_share_indicators

    @staticmethod
    def operating_revenue_ps_latest(tp_share_indicators, factor_share_indicators, dependencies=['operating_revenue', 'capitalization']):
        """
        每股营业收入(最新)
        :param dependencies:
        :param tp_share_indicators:
        :param factor_share_indicators:
        :return:
        """

        share_indicators = tp_share_indicators.loc[:, dependencies]
        fun = lambda x: (x[0] / x[1] if x[1] and x[1] != 0 else None)
        share_indicators['OptRevPS'] = share_indicators[dependencies].apply(fun, axis=1)

        # share_indicators = share_indicators.drop(columns=['operating_revenue'], axis=1)
        share_indicators = share_indicators[['symbol', 'OptRevPS']]

        factor_share_indicators = pd.merge(factor_share_indicators, share_indicators, on='symbol')
        return factor_share_indicators

    @staticmethod
    def operating_profit_ps(tp_share_indicators, factor_share_indicators, dependencies=['operating_profit_ttm', 'capitalization']):
        """
        每股营业利润
        :param dependencies:
        :param tp_share_indicators:
        :param factor_share_indicators:
        :return:
        """

        share_indicators = tp_share_indicators.loc[:, dependencies]
        fun = lambda x: (x[0] / x[1] if x[1] and x[1] != 0 else None)
        share_indicators['OptProfitPSTTM'] = share_indicators[dependencies].apply(fun, axis=1)

        # share_indicators = share_indicators.drop(columns=['operating_profit_ttm'], axis=1)
        share_indicators = share_indicators[['symbol', 'OptProfitPSTTM']]

        factor_share_indicators = pd.merge(factor_share_indicators, share_indicators, on='symbol')
        return factor_share_indicators

    @staticmethod
    def operating_profit_ps_latest(tp_share_indicators, factor_share_indicators, dependencies=['operating_profit', 'capitalization']):
        """
        每股营业利润（最新）
        :param dependencies:
        :param tp_share_indicators:
        :param factor_share_indicators:
        :return:
        """

        share_indicators = tp_share_indicators.loc[:, dependencies]
        fun = lambda x: (x[0] / x[1] if x[1] and x[1] != 0 else None)
        share_indicators['OptProfitPS'] = share_indicators[['operating_profit', 'capitalization']].apply(fun, axis=1)

        # share_indicators = share_indicators.drop(columns=['operating_profit'], axis=1)
        share_indicators = share_indicators[['symbol', 'OptProfitPS']]
        factor_share_indicators = pd.merge(factor_share_indicators, share_indicators, on='symbol')
        return factor_share_indicators

    @staticmethod
    def capital_surplus_fund_ps(tp_share_indicators, factor_share_indicators, dependencies=['capital_reserve_fund', 'capitalization']):
        """
        每股资本公积金
        :param dependencies:
        :param tp_share_indicators:
        :param factor_share_indicators:
        :return:
        """

        share_indicators = tp_share_indicators.loc[:, dependencies]
        fun = lambda x: (x[0] / x[1] if x[1] and x[1] != 0 else None)
        share_indicators['CapticalSurplusPS'] = share_indicators[
            ['capital_reserve_fund', 'capitalization']].apply(
            fun,
            axis=1)

        # share_indicators = share_indicators.drop(columns=['capital_reserve_fund'], axis=1)
        share_indicators = share_indicators[['symbol', 'CapticalSurplusPS']]
        factor_share_indicators = pd.merge(factor_share_indicators, share_indicators, on='symbol')
        return factor_share_indicators

    @staticmethod
    def surplus_reserve_fund_ps(tp_share_indicators, factor_share_indicators, dependencies=['surplus_reserve_fund', 'capitalization']):
        """
        每股盈余公积金
        :param dependencies:
        :param tp_share_indicators:
        :param factor_share_indicators:
        :return:
        """

        share_indicators = tp_share_indicators.loc[:, dependencies]
        fun = lambda x: (x[0] / x[1] if x[1] and x[1] != 0 else None)
        share_indicators['SurplusReservePS'] = share_indicators[['surplus_reserve_fund', 'capitalization']].apply(fun, axis=1)

        # share_indicators = share_indicators.drop(columns=['surplus_reserve_fund'], axis=1)
        share_indicators = share_indicators[['symbol', 'SurplusReservePS']]

        factor_share_indicators = pd.merge(factor_share_indicators, share_indicators, on='symbol')
        return factor_share_indicators

    @staticmethod
    def undivided_pro_fit_ps(tp_share_indicators, factor_share_indicators, dependencies=['retained_profit', 'capitalization']):
        """
        每股未分配利润
        :param dependencies:
        :param tp_share_indicators:
        :param factor_share_indicators:
        :return:
        """

        share_indicators = tp_share_indicators.loc[:, dependencies]
        fun = lambda x: (x[0] / x[1] if x[1] and x[1] != 0 else None)
        share_indicators['UndividedProfitPS'] = share_indicators[
            ['retained_profit', 'capitalization']].apply(
            fun,
            axis=1)

        # share_indicators = share_indicators.drop(columns=['retained_profit'], axis=1)
        share_indicators = share_indicators[['symbol', 'UndividedProfitPS']]
        factor_share_indicators = pd.merge(factor_share_indicators, share_indicators, on='symbol')
        return factor_share_indicators

    @staticmethod
    def retained_earnings_ps(tp_share_indicators, factor_share_indicators, dependencies=['SurplusReservePS', 'UndividedProfitPS']):
        """
        每股留存收益
        :param dependencies:
        :param tp_share_indicators:
        :param factor_share_indicators:
        :return:
        """

        share_indicators = tp_share_indicators.loc[:, dependencies]
        share_indicators['RetainedEarningsPS'] = share_indicators['UndividedProfitPS'] + share_indicators['SurplusReservePS']

        # share_indicators = share_indicators.drop(columns=['UndividedProfitPS', 'SurplusReservePS'], axis=1)
        share_indicators = share_indicators[['symbol', 'RetainedEarningsPS']]
        factor_share_indicators = pd.merge(factor_share_indicators, share_indicators, on='symbol')
        return factor_share_indicators

    @staticmethod
    def oper_cash_flow_ps(tp_share_indicators, factor_share_indicators, dependencies=['net_operate_cash_flow_ttm', 'capitalization']):
        """
        每股经营活动产生的现金流量净额
        :param dependencies:
        :param tp_share_indicators:
        :param factor_share_indicators:
        :return:
        """

        share_indicators = tp_share_indicators.loc[:, dependencies]
        fun = lambda x: (x[0] / x[1] if x[1] and x[1] != 0 else None)
        share_indicators['OptCFPSTTM'] = share_indicators[dependencies].apply(fun, axis=1)

        # share_indicators = share_indicators.drop(columns=['net_operate_cash_flow_ttm'], axis=1)
        share_indicators = share_indicators[['symbol', 'OptCFPSTTM']]

        factor_share_indicators = pd.merge(factor_share_indicators, share_indicators, on='symbol')
        return factor_share_indicators

    @staticmethod
    def cash_flow_ps(tp_share_indicators, factor_share_indicators, dependencies=['n_change_in_cash_ttm', 'capitalization']):
        """
        每股现金流量净额
        :param dependencies:
        :param tp_share_indicators:
        :param factor_share_indicators:
        :return:
        """

        share_indicators = tp_share_indicators.loc[:, dependencies]
        fun = lambda x: (x[0] / x[1] if x[1] and x[1] != 0 else None)
        share_indicators['CFPSTTM'] = share_indicators[dependencies].apply(fun, axis=1)

        # share_indicators = share_indicators.drop(columns=['n_change_in_cash_ttm'], axis=1)
        share_indicators = share_indicators[['symbol', 'CFPSTTM']]

        factor_share_indicators = pd.merge(factor_share_indicators, share_indicators, on='symbol')
        return factor_share_indicators

    @staticmethod
    def enterprise_fcfps(tp_share_indicators, factor_share_indicators, dependencies=['enterprise_fcfps']):
        """
        每股企业自由现金流量
        缺每股企业自由现金流量
        :param dependencies:
        :param tp_share_indicators:
        :param factor_share_indicators:
        :return:
        """

        share_indicators = tp_share_indicators.loc[:, dependencies]
        share_indicators['EnterpriseFCFPS'] = share_indicators['enterprise_fcfps']
        # share_indicators = share_indicators.drop(columns=['enterprise_fcfps'], axis=1)
        share_indicators = share_indicators[['symbol', 'EnterpriseFCFPS']]
        factor_share_indicators = pd.merge(factor_share_indicators, share_indicators, on='symbol')
        return factor_share_indicators

    @staticmethod
    def shareholder_fcfps(tp_share_indicators, factor_share_indicators, dependencies=['shareholder_fcfps']):
        """
        每股股东自由现金流量
        缺每股股东自由现金流量
        :param dependencies:
        :param tp_share_indicators:
        :param factor_share_indicators:
        :return:
        """

        share_indicators = tp_share_indicators.loc[:, dependencies]
        share_indicators['ShareholderFCFPS'] = share_indicators['shareholder_fcfps']
        # share_indicators = share_indicators.drop(columns=['shareholder_fcfps'], axis=1)
        share_indicators = share_indicators[['symbol', 'ShareholderFCFPS']]
        factor_share_indicators = pd.merge(factor_share_indicators, share_indicators, on='symbol')
        return factor_share_indicators


def calculate(trade_date, valuation_sets, per_share):
    """
    规模
    :param per_share: 规模类
    :param valuation_sets: 基础数据
    :param trade_date: 交易日
    :return:
    """
    if len(valuation_sets) <= 0:
        print("%s has no data" % trade_date)
        return

    factor_share_indicators = pd.DataFrame()
    factor_share_indicators['symbol'] = valuation_sets.index

    # psindu
    factor_share_indicators = per_share.eps(valuation_sets, factor_share_indicators)
    factor_share_indicators = per_share.diluted_eps(valuation_sets, factor_share_indicators)
    factor_share_indicators = per_share.cash_equivalent_ps(valuation_sets, factor_share_indicators)
    factor_share_indicators = per_share.dividend_ps(valuation_sets, factor_share_indicators)
    factor_share_indicators = per_share.eps_ttm(valuation_sets, factor_share_indicators)
    factor_share_indicators = per_share.net_asset_ps(valuation_sets, factor_share_indicators)
    factor_share_indicators = per_share.tor_ps(valuation_sets, factor_share_indicators)
    factor_share_indicators = per_share.tor_ps_latest(factor_share_indicators, factor_share_indicators)   # memorydrror
    factor_share_indicators = per_share.operating_revenue_ps(factor_share_indicators, factor_share_indicators)   # memoryerror
    factor_share_indicators = per_share.operating_revenue_ps_latest(valuation_sets, factor_share_indicators)
    factor_share_indicators = per_share.operating_profit_ps(valuation_sets, factor_share_indicators)
    factor_share_indicators = per_share.operating_profit_ps_latest(valuation_sets, factor_share_indicators)
    factor_share_indicators = per_share.capital_surplus_fund_ps(factor_share_indicators, factor_share_indicators) # memoryerror
    factor_share_indicators = per_share.surplus_reserve_fund_ps(factor_share_indicators, factor_share_indicators)  # memorydrror
    factor_share_indicators = per_share.undivided_pro_fit_ps(factor_share_indicators, factor_share_indicators)  # memorydrror
    factor_share_indicators = per_share.retained_earnings_ps(factor_share_indicators, factor_share_indicators)  # memorydrror
    factor_share_indicators = per_share.oper_cash_flow_ps(factor_share_indicators, factor_share_indicators)  # memorydrror
    factor_share_indicators = per_share.cash_flow_ps(factor_share_indicators, factor_share_indicators)  # memorydrror

    # factor_share_indicators = factor_share_indicators[['symbol',
    #                                                    'EPS',
    #                                                    'DilutedEPSTTM',
    #                                                    'CashEquPS',
    #                                                    'DivPS',
    #                                                    'EPSTTM',
    #                                                    'NetAssetPS',
    #                                                    'TotalRevPS',
    #                                                    'TotalRevPSTTM',
    #                                                    'OptRevPSTTM',
    #                                                    'OptRevPS',
    #                                                    'OptProfitPSTTM',
    #                                                    'OptProfitPS',
    #                                                    'CapticalSurplusPS',
    #                                                    'SurplusReservePS',
    #                                                    'UndividedProfitPS',
    #                                                    'RetainedEarningsPS',
    #                                                    'OptCFPSTTM',
    #                                                    'CFPSTTM',
    #                                                    'EnterpriseFCFPS',
    #                                                    'ShareholderFCFPS']]

    factor_share_indicators = factor_share_indicators[['symbol',
                                                       'EPS',
                                                       'DilutedEPSTTM',
                                                       'CashEquPS',
                                                       'DivPS',
                                                       'EPSTTM',
                                                       'NetAssetPS',
                                                       'TotalRevPS',
                                                       'TotalRevPSTTM',
                                                       'OptRevPSTTM',
                                                       'OptRevPS',
                                                       'OptProfitPSTTM',
                                                       'OptProfitPS',
                                                       'CapticalSurplusPS',
                                                       'SurplusReservePS',
                                                       'UndividedProfitPS',
                                                       'RetainedEarningsPS',
                                                       'OptCFPSTTM',
                                                       'CFPSTTM']]

    factor_share_indicators['id'] = factor_share_indicators['symbol'] + str(trade_date)
    factor_share_indicators['trade_date'] = str(trade_date)
    per_share._storage_data(factor_share_indicators, trade_date)


@app.task()
def factor_calculate(**kwargs):
    print("per_share_kwargs: {}".format(kwargs))
    date_index = kwargs['date_index']
    session = kwargs['session']
    per_share = PerShareIndicators('factor_per_share')  # 注意, 这里的name要与client中新建table时的name一致, 不然回报错
    content = cache_data.get_cache(session + str(date_index), date_index)
    total_pre_share_data = json_normalize(json.loads(str(content, encoding='utf8')))
    print("len_total_per_share_data {}".format(len(total_pre_share_data)))
    calculate(date_index, total_pre_share_data, per_share)
