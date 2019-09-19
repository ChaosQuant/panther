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
import gc
import pandas as pd
import json
from pandas.io.json import json_normalize
from basic_derivation.factor_base import FactorBase

# from basic_derivation import app
# from ultron.cluster.invoke.cache_data import cache_data


class PerShareIndicators(FactorBase):
    """
    每股因子
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
                    `id` INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
                    `security_code` varchar(24) NOT NULL,
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
                    constraint {0}_uindex
                    unique (`trade_date`,`security_code`)
                    )ENGINE=InnoDB DEFAULT CHARSET=utf8;""".format(self._name)
        super(PerShareIndicators, self)._create_tables(create_sql, drop_sql)

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
        share_indicators['CapticalSurplusPS'] = share_indicators[dependencies].apply(fun, axis=1)

        share_indicators = share_indicators.drop(columns=dependencies, axis=1)
        factor_share_indicators = pd.merge(factor_share_indicators, share_indicators, how='outer', on='security_code')
        return factor_share_indicators

    @staticmethod
    def cash_equivalent_ps(tp_share_indicators, factor_share_indicators, dependencies=['capitalization', 'cash_and_equivalents_at_end']):
        """
        每股现金及现金等价物余额 = 期末现金及现金等价物余额 / 总股本
        :param dependencies:
        :param tp_share_indicators:
        :param factor_share_indicators:
        :return:
        """
        share_indicators = tp_share_indicators.loc[:, dependencies]
        fun = lambda x: (x[1] / x[0] if x[0] and x[0] != 0 else None)
        share_indicators['CashEquPS'] = share_indicators[dependencies].apply(fun, axis=1)

        share_indicators = share_indicators.drop(columns=dependencies, axis=1)
        factor_share_indicators = pd.merge(factor_share_indicators, share_indicators, how='outer', on='security_code')
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
        share_indicators = share_indicators.rename(columns={'dividend_receivable': 'DivPS'})
        # share_indicators = share_indicators.drop(columns=dependencies, axis=1)
        factor_share_indicators = pd.merge(factor_share_indicators, share_indicators, how='outer', on='security_code')
        return factor_share_indicators

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
        # print(share_indicators.head())
        share_indicators = share_indicators.rename(columns={'basic_eps': 'EPS'})
        # share_indicators = share_indicators.drop(columns=dependencies, axis=1)
        factor_share_indicators = pd.merge(factor_share_indicators, share_indicators,  how='outer', on='security_code')
        return factor_share_indicators

    @staticmethod
    def shareholder_fcfps(tp_share_indicators, factor_share_indicators, dependencies=['shareholder_fcfps', 'capitalization']):
        """
        每股股东自由现金流量
        :param dependencies:
        :param tp_share_indicators:
        :param factor_share_indicators:
        :return:
        """
        share_indicators = tp_share_indicators.loc[:, dependencies]
        fun = lambda x: (x[0] / x[1] if x[1] and x[1] != 0 else None)
        share_indicators['ShareholderFCFPS'] = share_indicators[dependencies].apply(fun, axis=1)

        share_indicators = share_indicators.drop(columns=dependencies, axis=1)
        factor_share_indicators = pd.merge(factor_share_indicators, share_indicators, how='outer', on='security_code')
        # factor_share_indicators['ShareholderFCFPS'] = share_indicators['ShareholderFCFPS']
        return factor_share_indicators

    @staticmethod
    def enterprise_fcfps(tp_share_indicators, factor_share_indicators, dependencies=['enterprise_fcfps', 'capitalization']):
        """
        每股企业自由现金流量
        :param dependencies:
        :param tp_share_indicators:
        :param factor_share_indicators:
        :return:
        """
        share_indicators = tp_share_indicators.loc[:, dependencies]
        fun = lambda x: (x[0] / x[1] if x[1] and x[1] != 0 else None)
        share_indicators['EnterpriseFCFPS'] = share_indicators[dependencies].apply(fun, axis=1)
        share_indicators = share_indicators.drop(columns=dependencies, axis=1)
        factor_share_indicators = pd.merge(factor_share_indicators, share_indicators, how='outer', on='security_code')
        # factor_share_indicators['EnterpriseFCFPS'] = share_indicators['EnterpriseFCFPS']

        return factor_share_indicators

    @staticmethod
    def net_asset_ps(tp_share_indicators, factor_share_indicators, dependencies=['total_owner_equities', 'capitalization']):
        """
        每股净资产 = 归属于母公司的所有者权益 / 总股本
        :param dependencies:
        :param tp_share_indicators:
        :param factor_share_indicators:
        :return:
        """
        share_indicators = tp_share_indicators.loc[:, dependencies]
        fun = lambda x: (x[0] / x[1] if x[1] and x[1] != 0 else None)
        share_indicators['NetAssetPS'] = share_indicators[dependencies].apply(fun, axis=1)

        share_indicators = share_indicators.drop(columns=dependencies, axis=1)
        factor_share_indicators = pd.merge(factor_share_indicators, share_indicators, how='outer', on='security_code')
        # factor_share_indicators['NetAssetPS'] = share_indicators['NetAssetPS']

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

        share_indicators = share_indicators.drop(columns=dependencies, axis=1)
        factor_share_indicators = pd.merge(factor_share_indicators, share_indicators, how='outer', on='security_code')
        # factor_share_indicators['OptRevPS'] = share_indicators['OptRevPS']

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
        share_indicators['SurplusReservePS'] = share_indicators[dependencies].apply(fun, axis=1)

        share_indicators = share_indicators.drop(columns=dependencies, axis=1)
        factor_share_indicators = pd.merge(factor_share_indicators, share_indicators, how='outer', on='security_code')
        # factor_share_indicators['SurplusReservePS'] = share_indicators['SurplusReservePS']

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
        share_indicators['OptProfitPS'] = share_indicators[dependencies].apply(fun, axis=1)

        share_indicators = share_indicators.drop(columns=dependencies, axis=1)
        factor_share_indicators = pd.merge(factor_share_indicators, share_indicators, how='outer', on='security_code')
        # factor_share_indicators['OptProfitPS'] = share_indicators['OptProfitPS']

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
        share_indicators['UndividedProfitPS'] = share_indicators[dependencies].apply(fun, axis=1)

        share_indicators = share_indicators.drop(columns=dependencies, axis=1)
        factor_share_indicators = pd.merge(factor_share_indicators, share_indicators, how='outer', on='security_code')
        # factor_share_indicators['UndividedProfitPS'] = share_indicators['UndividedProfitPS']

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
        share_indicators['RetainedEarningsPS'] = share_indicators['UndividedProfitPS'] + share_indicators[
            'SurplusReservePS']

        share_indicators = share_indicators.drop(columns=dependencies, axis=1)
        factor_share_indicators = pd.merge(factor_share_indicators, share_indicators, how='outer', on='security_code')
        # factor_share_indicators['RetainedEarningsPS'] = share_indicators['RetainedEarningsPS']

        return factor_share_indicators

    @staticmethod
    def tor_ps_latest(tp_share_indicators, factor_share_indicators, dependencies=['total_operating_revenue', 'capitalization']):
        """
        每股营业总收入 = 营业总收入 / 总股本
        :param dependencies:
        :param tp_share_indicators:
        :param factor_share_indicators:
        :return:
        """
        share_indicators = tp_share_indicators.loc[:, dependencies]
        fun = lambda x: (x[0] / x[1] if x[1] and x[1] != 0 else None)
        share_indicators['TotalRevPS'] = share_indicators[dependencies].apply(fun, axis=1)

        share_indicators = share_indicators.drop(columns=dependencies, axis=1)
        factor_share_indicators = pd.merge(factor_share_indicators, share_indicators, how='outer', on='security_code')
        # factor_share_indicators['TotalRevPS'] = share_indicators['TotalRevPS']
        return factor_share_indicators

    @staticmethod
    def cash_flow_ps(tp_share_indicators, factor_share_indicators, dependencies=['cash_equivalent_increase_ttm', 'capitalization']):
        """
        每股现金流量净额 = 现金及现金等价物净增加额TTM / 总股本
        :param dependencies:
        :param tp_share_indicators:
        :param factor_share_indicators:
        :return:
        """
        share_indicators = tp_share_indicators.loc[:, dependencies]
        fun = lambda x: (x[0] / x[1] if x[1] and x[1] != 0 else None)
        share_indicators['CFPSTTM'] = share_indicators[dependencies].apply(fun, axis=1)

        share_indicators = share_indicators.drop(columns=dependencies, axis=1)
        factor_share_indicators = pd.merge(factor_share_indicators, share_indicators, how='outer', on='security_code')
        # factor_share_indicators['CFPSTTM'] = share_indicators['CFPSTTM']

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
        share_indicators = share_indicators.rename(columns={'diluted_eps': 'DilutedEPSTTM'})
        # share_indicators = share_indicators.drop(columns=['diluted_eps'], axis=1)
        # share_indicators = share_indicators[['security_code', 'DilutedEPSTTM']]
        factor_share_indicators = pd.merge(factor_share_indicators, share_indicators, how='outer', on='security_code')
        # factor_share_indicators['DilutedEPSTTM'] = share_indicators['DilutedEPSTTM']

        return factor_share_indicators

    @staticmethod
    def eps_ttm(tp_share_indicators, factor_share_indicators, dependencies=['np_parent_company_owners_ttm', 'capitalization']):
        """
        每股收益 TTM = 归属于母公司所有者的净利润TTM / 总股本
        :param dependencies:
        :param tp_share_indicators:
        :param factor_share_indicators:
        :return:
        """

        share_indicators = tp_share_indicators.loc[:, dependencies]
        fun = lambda x: (x[0] / x[1] if x[1] and x[1] != 0 else 0)
        share_indicators['EPSTTM'] = share_indicators[dependencies].apply(fun, axis=1)

        share_indicators = share_indicators.drop(columns=dependencies, axis=1)
        factor_share_indicators = pd.merge(factor_share_indicators, share_indicators, how='outer', on='security_code')
        # factor_share_indicators['EPSTTM'] = share_indicators['EPSTTM']

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

        share_indicators = share_indicators.drop(columns=dependencies, axis=1)
        factor_share_indicators = pd.merge(factor_share_indicators, share_indicators, how='outer', on='security_code')
        # factor_share_indicators['OptCFPSTTM'] = share_indicators['OptCFPSTTM']

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

        share_indicators = share_indicators.drop(columns=dependencies, axis=1)
        factor_share_indicators = pd.merge(factor_share_indicators, share_indicators, how='outer', on='security_code')
        # factor_share_indicators['OptProfitPSTTM'] = share_indicators['OptProfitPSTTM']

        return factor_share_indicators

    @staticmethod
    def operating_revenue_ps(tp_share_indicators, factor_share_indicators, dependencies=['operating_revenue_ttm', 'capitalization']):
        """
        每股营业收入TTM
        :param dependencies:
        :param tp_share_indicators:
        :param factor_share_indicators:
        :return:
        """

        share_indicators = tp_share_indicators.loc[:, dependencies]
        fun = lambda x: (x[0] / x[1] if x[1] and x[1] != 0 else None)
        share_indicators['OptRevPSTTM'] = share_indicators[dependencies].apply(fun, axis=1)

        share_indicators = share_indicators.drop(columns=dependencies, axis=1)
        factor_share_indicators = pd.merge(factor_share_indicators, share_indicators, on='security_code')
        # factor_share_indicators['OptRevPSTTM'] = share_indicators['OptRevPSTTM']

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

        share_indicators = share_indicators.drop(columns=dependencies, axis=1)
        factor_share_indicators = pd.merge(factor_share_indicators, share_indicators, on='security_code')
        # factor_share_indicators['TotalRevPSTTM'] = share_indicators['TotalRevPSTTM']

        return factor_share_indicators


def calculate(trade_date, valuation_sets, factor_name):
    """
    :param valuation_sets: 基础数据
    :param trade_date: 交易日
    :return:
    """
    per_share = PerShareIndicators(factor_name)  # 注意, 这里的name要与client中新建table时的name一致, 不然回报错
    factor_share_indicators = pd.DataFrame()
    factor_share_indicators['security_code'] = valuation_sets['security_code']
    valuation_sets = valuation_sets.set_index('security_code')

    factor_share_indicators = factor_share_indicators.set_index('security_code')

    # psindu
    factor_share_indicators = per_share.eps(valuation_sets, factor_share_indicators)
    factor_share_indicators = per_share.diluted_eps(valuation_sets, factor_share_indicators)
    factor_share_indicators = per_share.cash_equivalent_ps(valuation_sets, factor_share_indicators)
    factor_share_indicators = per_share.dividend_ps(valuation_sets, factor_share_indicators)
    factor_share_indicators = per_share.eps_ttm(valuation_sets, factor_share_indicators)
    factor_share_indicators = per_share.net_asset_ps(valuation_sets, factor_share_indicators)
    factor_share_indicators = per_share.tor_ps(valuation_sets, factor_share_indicators)
    factor_share_indicators = per_share.tor_ps_latest(valuation_sets, factor_share_indicators)   # memorydrror
    factor_share_indicators = per_share.operating_revenue_ps(valuation_sets, factor_share_indicators)   # memoryerror
    factor_share_indicators = per_share.operating_revenue_ps_latest(valuation_sets, factor_share_indicators)
    factor_share_indicators = per_share.operating_profit_ps(valuation_sets, factor_share_indicators)
    factor_share_indicators = per_share.operating_profit_ps_latest(valuation_sets, factor_share_indicators)
    factor_share_indicators = per_share.capital_surplus_fund_ps(valuation_sets, factor_share_indicators) # memoryerror
    factor_share_indicators = per_share.surplus_reserve_fund_ps(valuation_sets, factor_share_indicators)  # memorydrror
    factor_share_indicators = per_share.undivided_pro_fit_ps(valuation_sets, factor_share_indicators)  # memorydrror
    factor_share_indicators = per_share.retained_earnings_ps(factor_share_indicators, factor_share_indicators)  # memorydrror
    factor_share_indicators = per_share.oper_cash_flow_ps(valuation_sets, factor_share_indicators)  # memorydrror
    factor_share_indicators = per_share.cash_flow_ps(valuation_sets, factor_share_indicators)  # memorydrror
    factor_share_indicators = per_share.enterprise_fcfps(valuation_sets, factor_share_indicators)  # memorydrror
    factor_share_indicators = per_share.shareholder_fcfps(valuation_sets, factor_share_indicators)  # memorydrror

    # factor_share_indicators = factor_share_indicators[['security_code',
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

    # factor_share_indicators = factor_share_indicators[['security_code',
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
    #                                                    'CFPSTTM']]
    factor_share_indicators = factor_share_indicators.reset_index()

    # factor_share_indicators['id'] = factor_share_indicators['security_code'] + str(trade_date)
    factor_share_indicators['trade_date'] = str(trade_date)
    print(factor_share_indicators.head())
    per_share._storage_data(factor_share_indicators, trade_date)
    del per_share
    gc.collect()


# @app.task()
def factor_calculate(**kwargs):
    print("per_share_kwargs: {}".format(kwargs))
    date_index = kwargs['date_index']
    session = kwargs['session']
    content = cache_data.get_cache(session + str(date_index), date_index)
    total_pre_share_data = json_normalize(json.loads(str(content, encoding='utf8')))
    print("len_total_per_share_data {}".format(len(total_pre_share_data)))
    calculate(date_index, total_pre_share_data)
