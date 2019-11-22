#! /usr/bin/env python
# -*- coding: utf-8 -*-

"""
@version:
@author: zzh
@file: factor_earning_expectation.py
@time: 2019-9-19
"""

import pandas as pd


class FactorEarningExpectation():
    """
    盈利预期
    """

    def __init__(self):
        __str__ = 'factor_earning_expectation'
        self.name = '盈利预测'
        self.factor_type1 = '盈利预测'
        self.factor_type2 = '盈利预测'
        self.description = '个股盈利预测因子'

    @staticmethod
    def NPFY1(tp_earning, factor_earning_expect, trade_date, dependencies=['net_profit_fy1']):
        """
        :name: 一致预期净利润(FY1)
        :desc: 一致预期净利润的未来第一年度的预测
        :unit: 元
        :view_dimension: 10000
        """
        earning_expect = tp_earning[tp_earning['publish_date'] == trade_date].loc[:, dependencies]
        earning_expect.rename(columns={'net_profit_fy1': 'NPFY1'}, inplace=True)
        factor_earning_expect = pd.merge(factor_earning_expect, earning_expect, on='security_code')
        return factor_earning_expect

    @staticmethod
    def NPFY2(tp_earning, factor_earning_expect, trade_date, dependencies=['net_profit_fy2']):
        """
        :name: 一致预期净利润(FY2)
        :desc: 一致预期净利润的未来第二年度的预测
        :unit: 元
        :view_dimension: 10000
        """
        earning_expect = tp_earning[tp_earning['publish_date'] == trade_date].loc[:, dependencies]
        earning_expect.rename(columns={'net_profit_fy2': 'NPFY2'}, inplace=True)
        factor_earning_expect = pd.merge(factor_earning_expect, earning_expect, on='security_code')
        return factor_earning_expect

    @staticmethod
    def EPSFY1(tp_earning, factor_earning_expect, trade_date, dependencies=['eps_fy1']):
        """
        :name: 一致预期每股收益（FY1）
        :desc: 一致预期每股收益未来第一年度的预测均值
        :unit: 元
        :view_dimension: 1
        """
        earning_expect = tp_earning[tp_earning['publish_date'] == trade_date].loc[:, dependencies]
        earning_expect.rename(columns={'eps_fy1': 'EPSFY1'}, inplace=True)
        factor_earning_expect = pd.merge(factor_earning_expect, earning_expect, on='security_code')
        return factor_earning_expect

    @staticmethod
    def EPSFY2(tp_earning, factor_earning_expect, trade_date, dependencies=['eps_fy2']):
        """
        :name: 一致预期每股收益（FY2）
        :desc: 一致预期每股收益未来第二年度的预测均值
        :unit: 元
        :view_dimension: 1
        """
        earning_expect = tp_earning[tp_earning['publish_date'] == trade_date].loc[:, dependencies]
        earning_expect.rename(columns={'eps_fy2': 'EPSFY2'}, inplace=True)
        factor_earning_expect = pd.merge(factor_earning_expect, earning_expect, on='security_code')
        return factor_earning_expect

    @staticmethod
    def OptIncFY1(tp_earning, factor_earning_expect, trade_date, dependencies=['operating_revenue_fy1']):
        """
        :name: 一致预期营业收入（FY1）
        :desc: 一致预期营业收入未来第一年度的预测均值
        :unit: 元
        :view_dimension: 10000
        """
        earning_expect = tp_earning[tp_earning['publish_date'] == trade_date].loc[:, dependencies]
        earning_expect.rename(columns={'operating_revenue_fy1': 'OptIncFY1'}, inplace=True)
        factor_earning_expect = pd.merge(factor_earning_expect, earning_expect, on='security_code')
        return factor_earning_expect

    @staticmethod
    def OptIncFY2(tp_earning, factor_earning_expect, trade_date, dependencies=['operating_revenue_fy2']):
        """
        :name: 一致预期营业收入（FY2）
        :desc: 一致预期营业收入未来第二年度的预测均值
        :unit: 元
        :view_dimension: 10000
        """
        earning_expect = tp_earning[tp_earning['publish_date'] == trade_date].loc[:, dependencies]
        earning_expect.rename(columns={'operating_revenue_fy2': 'OptIncFY2'}, inplace=True)
        factor_earning_expect = pd.merge(factor_earning_expect, earning_expect, on='security_code')
        return factor_earning_expect

    @staticmethod
    def CEPEFY1(tp_earning, factor_earning_expect, trade_date, dependencies=['pe_fy1']):
        """
        :name: 一致预期市盈率(PE)(FY1)
        :desc: 一致预期市盈率未来第一年度的预测均值
        :unit: 倍
        :view_dimension: 1
        """
        earning_expect = tp_earning[tp_earning['publish_date'] == trade_date].loc[:, dependencies]
        earning_expect.rename(columns={'pe_fy1': 'CEPEFY1'}, inplace=True)
        factor_earning_expect = pd.merge(factor_earning_expect, earning_expect, on='security_code')
        return factor_earning_expect

    @staticmethod
    def CEPEFY2(tp_earning, factor_earning_expect, trade_date, dependencies=['pe_fy2']):
        """
        :name: 一致预期市盈率(PE)(FY2)
        :desc: 一致预期市盈率未来第二年度的预测均值
        :unit: 倍
        :view_dimension: 1
        """
        earning_expect = tp_earning[tp_earning['publish_date'] == trade_date].loc[:, dependencies]
        earning_expect.rename(columns={'pe_fy2': 'CEPEFY2'}, inplace=True)
        factor_earning_expect = pd.merge(factor_earning_expect, earning_expect, on='security_code')
        return factor_earning_expect

    @staticmethod
    def CEPBFY1(tp_earning, factor_earning_expect, trade_date, dependencies=['pb_fy1']):
        """
        :name: 一致预期市净率(PB)(FY1)
        :desc: 一致预期市净率未来第一年度的预测均值
        :unit: 倍
        :view_dimension: 1
        """
        earning_expect = tp_earning[tp_earning['publish_date'] == trade_date].loc[:, dependencies]
        earning_expect.rename(columns={'pb_fy1': 'CEPBFY1'}, inplace=True)
        factor_earning_expect = pd.merge(factor_earning_expect, earning_expect, on='security_code')
        return factor_earning_expect

    @staticmethod
    def CEPBFY2(tp_earning, factor_earning_expect, trade_date, dependencies=['pb_fy2']):
        """
        :name: 一致预期市净率(PB)(FY2)
        :desc: 一致预期市净率未来第二年度的预测均值
        :unit: 倍
        :view_dimension: 1
        """
        earning_expect = tp_earning[tp_earning['publish_date'] == trade_date].loc[:, dependencies]
        earning_expect.rename(columns={'pb_fy2': 'CEPBFY2'}, inplace=True)
        factor_earning_expect = pd.merge(factor_earning_expect, earning_expect, on='security_code')
        return factor_earning_expect

    @staticmethod
    def CEPEGFY1(tp_earning, factor_earning_expect, trade_date, dependencies=['peg_fy1']):
        """
        :name: 市盈率相对盈利增长比率(FY1)
        :desc: 未来第一年度市盈率相对盈利增长比率
        :unit:
        :view_dimension: 0.01
        """
        earning_expect = tp_earning[tp_earning['publish_date'] == trade_date].loc[:, dependencies]
        earning_expect.rename(columns={'peg_fy1': 'CEPEGFY1'}, inplace=True)
        factor_earning_expect = pd.merge(factor_earning_expect, earning_expect, on='security_code')
        return factor_earning_expect

    @staticmethod
    def CEPEGFY2(tp_earning, factor_earning_expect, trade_date, dependencies=['peg_fy2']):
        """
        :name: 市盈率相对盈利增长比率(FY2)
        :desc: 未来第二年度市盈率相对盈利增长比率
        :unit:
        :view_dimension: 0.01
        """
        earning_expect = tp_earning[tp_earning['publish_date'] == trade_date].loc[:, dependencies]
        earning_expect.rename(columns={'peg_fy2': 'CEPEGFY2'}, inplace=True)
        factor_earning_expect = pd.merge(factor_earning_expect, earning_expect, on='security_code')
        return factor_earning_expect

    @staticmethod
    def _change_rate(tp_earning, trade_date, pre_trade_date, colunm, factor_name):
        earning_expect = tp_earning[tp_earning['publish_date'] == trade_date].loc[:, colunm]
        earning_expect_pre = tp_earning[tp_earning['publish_date'] == pre_trade_date].loc[:, colunm]
        earning_expect = pd.merge(earning_expect, earning_expect_pre, on='security_code', how='left')
        earning_expect[factor_name] = (earning_expect[colunm + '_x'] - earning_expect[colunm + '_y']) / \
                                      earning_expect[colunm + '_y']
        earning_expect.drop(columns=[colunm + '_x', colunm + '_y'], inplace=True)
        return earning_expect

    @staticmethod
    def _change_value(tp_earning, trade_date, pre_trade_date, colunm, factor_name):
        earning_expect = tp_earning[tp_earning['publish_date'] == trade_date].loc[:, colunm]
        earning_expect_pre = tp_earning[tp_earning['publish_date'] == pre_trade_date].loc[:, colunm]
        earning_expect = pd.merge(earning_expect, earning_expect_pre, on='security_code', how='left')
        earning_expect[factor_name] = (earning_expect[colunm + '_x'] - earning_expect[colunm + '_y'])
        earning_expect.drop(columns=[colunm + '_x', colunm + '_y'], inplace=True)
        return earning_expect

    @staticmethod
    def NPFY11WRT(tp_earning, factor_earning_expect, trade_date):
        """
        :name: 一致预测净利润(FY1)变化率_一周
        :desc: 未来第一年度一致预测净利润一周内预测值变化率
        :unit:
        :view_dimension: 0.01
        """
        trade_dates = sorted(set(tp_earning['publish_date']), reverse=True)
        if len(trade_dates) >= 2:
            earning_expect = FactorEarningExpectation._change_rate(tp_earning, trade_date, trade_dates[1],
                                                                   'net_profit_fy1',
                                                                   'NPFY11WRT')
            factor_earning_expect = pd.merge(factor_earning_expect, earning_expect, on='security_code')
        return factor_earning_expect

    @staticmethod
    def NPFY11MRT(tp_earning, factor_earning_expect, trade_date):
        """
        :name: 一致预测净利润(FY1)变化率_一月
        :desc: 未来第一年度一致预测净利润一月内预测值变化率
        :unit:
        :view_dimension: 0.01
        """
        trade_dates = sorted(set(tp_earning['publish_date']), reverse=True)
        if len(trade_dates) >= 3:
            earning_expect = FactorEarningExpectation._change_rate(tp_earning, trade_date, trade_dates[2],
                                                                   'net_profit_fy1',
                                                                   'NPFY11MRT')
            factor_earning_expect = pd.merge(factor_earning_expect, earning_expect, on='security_code')
        return factor_earning_expect

    @staticmethod
    def NPFY13MRT(tp_earning, factor_earning_expect, trade_date):
        """
        :name: 一致预测净利润(FY1)变化率_三月
        :desc: 未来第一年度一致预测净利润三月内预测值变化率
        :unit:
        :view_dimension: 0.01
        """
        trade_dates = sorted(set(tp_earning['publish_date']), reverse=True)
        if len(trade_dates) >= 4:
            earning_expect = FactorEarningExpectation._change_rate(tp_earning, trade_date, trade_dates[3],
                                                                   'net_profit_fy1',
                                                                   'NPFY13MRT')
            factor_earning_expect = pd.merge(factor_earning_expect, earning_expect, on='security_code')
        return factor_earning_expect

    @staticmethod
    def NPFY16MRT(tp_earning, factor_earning_expect, trade_date):
        """
        :name: 一致预测净利润(FY1)变化率_六月
        :desc: 未来第一年度一致预测净利润六月内预测值变化率
        :unit:
        :view_dimension: 0.01
        """
        trade_dates = sorted(set(tp_earning['publish_date']), reverse=True)
        if len(trade_dates) >= 5:
            earning_expect = FactorEarningExpectation._change_rate(tp_earning, trade_date, trade_dates[4],
                                                                   'net_profit_fy1',
                                                                   'NPFY16MRT')
            factor_earning_expect = pd.merge(factor_earning_expect, earning_expect, on='security_code')
        return factor_earning_expect

    @staticmethod
    def EPSFY11WChg(tp_earning, factor_earning_expect, trade_date):
        """
        :name: 一致预测每股收益(FY1)变化_一周
        :desc: 未来第一年度一致预测每股收益一周内预测值变化
        :unit: 元
        :view_dimension: 1
        """
        trade_dates = sorted(set(tp_earning['publish_date']), reverse=True)
        if len(trade_dates) >= 2:
            earning_expect = FactorEarningExpectation._change_value(tp_earning, trade_date, trade_dates[1],
                                                                    'eps_fy1',
                                                                    'EPSFY11WChg')
            factor_earning_expect = pd.merge(factor_earning_expect, earning_expect, on='security_code')
        return factor_earning_expect

    @staticmethod
    def EPSFY11MChg(tp_earning, factor_earning_expect, trade_date):
        """
        :name: 一致预测每股收益(FY1)变化_一月
        :desc: 未来第一年度一致预测每股收益一月内预测值变化
        :unit: 元
        :view_dimension: 1
        """
        trade_dates = sorted(set(tp_earning['publish_date']), reverse=True)
        if len(trade_dates) >= 3:
            earning_expect = FactorEarningExpectation._change_value(tp_earning, trade_date, trade_dates[2],
                                                                    'eps_fy1',
                                                                    'EPSFY11MChg')
            factor_earning_expect = pd.merge(factor_earning_expect, earning_expect, on='security_code')
        return factor_earning_expect

    @staticmethod
    def EPSFY13MChg(tp_earning, factor_earning_expect, trade_date):
        """
        :name: 一致预测每股收益(FY1)变化_三月
        :desc: 未来第一年度一致预测每股收益三月内预测值变化
        :unit: 元
        :view_dimension: 1
        """
        trade_dates = sorted(set(tp_earning['publish_date']), reverse=True)
        if len(trade_dates) >= 4:
            earning_expect = FactorEarningExpectation._change_value(tp_earning, trade_date, trade_dates[3],
                                                                    'eps_fy1',
                                                                    'EPSFY13MChg')
            factor_earning_expect = pd.merge(factor_earning_expect, earning_expect, on='security_code')
        return factor_earning_expect

    @staticmethod
    def EPSFY16MChg(tp_earning, factor_earning_expect, trade_date):
        """
        :name: 一致预测每股收益(FY1)变化_六月
        :desc: 未来第一年度一致预测每股收益六月内预测值变化
        :unit: 元
        :view_dimension: 1
        """
        trade_dates = sorted(set(tp_earning['publish_date']), reverse=True)
        if len(trade_dates) >= 5:
            earning_expect = FactorEarningExpectation._change_value(tp_earning, trade_date, trade_dates[4],
                                                                    'eps_fy1',
                                                                    'EPSFY16MChg')
            factor_earning_expect = pd.merge(factor_earning_expect, earning_expect, on='security_code')
        return factor_earning_expect

    @staticmethod
    def EPSFY11WRT(tp_earning, factor_earning_expect, trade_date):
        """
        :name: 一致预测每股收益(FY1)变化率_一周
        :desc: 未来第一年度一致预测每股收益一周内预测值变化率
        :unit:
        :view_dimension: 0.01
        """
        trade_dates = sorted(set(tp_earning['publish_date']), reverse=True)
        if len(trade_dates) >= 2:
            earning_expect = FactorEarningExpectation._change_value(tp_earning, trade_date, trade_dates[1],
                                                                    'eps_fy1',
                                                                    'EPSFY11WRT')
            factor_earning_expect = pd.merge(factor_earning_expect, earning_expect, on='security_code')
        return factor_earning_expect

    @staticmethod
    def EPSFY11MRT(tp_earning, factor_earning_expect, trade_date):
        """
        :name: 一致预测每股收益(FY1)变化率_一月
        :desc: 未来第一年度一致预测每股收益一月内预测值变化率
        :unit:
        :view_dimension: 0.01
        """
        trade_dates = sorted(set(tp_earning['publish_date']), reverse=True)
        if len(trade_dates) >= 3:
            earning_expect = FactorEarningExpectation._change_rate(tp_earning, trade_date, trade_dates[2],
                                                                   'eps_fy1',
                                                                   'EPSFY11MRT')
            factor_earning_expect = pd.merge(factor_earning_expect, earning_expect, on='security_code')
        return factor_earning_expect

    @staticmethod
    def EPSFY13MRT(tp_earning, factor_earning_expect, trade_date):
        """
        :name: 一致预测每股收益(FY1)变化率_三月
        :desc: 未来第一年度一致预测每股收益三月内预测值变化率
        :unit:
        :view_dimension: 0.01
        """
        trade_dates = sorted(set(tp_earning['publish_date']), reverse=True)
        if len(trade_dates) >= 4:
            earning_expect = FactorEarningExpectation._change_rate(tp_earning, trade_date, trade_dates[3],
                                                                   'eps_fy1',
                                                                   'EPSFY13MRT')
            factor_earning_expect = pd.merge(factor_earning_expect, earning_expect, on='security_code')
        return factor_earning_expect

    @staticmethod
    def EPSFY16MRT(tp_earning, factor_earning_expect, trade_date):
        """
        :name: 一致预测每股收益(FY1)变化率_六月
        :desc: 未来第一年度一致预测每股收益六月内预测值变化率
        :unit:
        :view_dimension: 0.01
        """
        trade_dates = sorted(set(tp_earning['publish_date']), reverse=True)
        if len(trade_dates) >= 5:
            earning_expect = FactorEarningExpectation._change_rate(tp_earning, trade_date, trade_dates[4],
                                                                   'eps_fy1',
                                                                   'EPSFY16MRT')
            factor_earning_expect = pd.merge(factor_earning_expect, earning_expect, on='security_code')
        return factor_earning_expect

    @staticmethod
    def NPFY11WChg(tp_earning, factor_earning_expect, trade_date):
        """
        :name: 一致预测净利润(FY1)变化_一周
        :desc: 未来第一年度一致预测净利润一周内预测值变化
        :unit: 元
        :view_dimension: 10000
        """
        trade_dates = sorted(set(tp_earning['publish_date']), reverse=True)
        if len(trade_dates) >= 2:
            earning_expect = FactorEarningExpectation._change_value(tp_earning, trade_date, trade_dates[1],
                                                                    'net_profit_fy1',
                                                                    'NPFY11WChg')
            factor_earning_expect = pd.merge(factor_earning_expect, earning_expect, on='security_code')
        return factor_earning_expect

    @staticmethod
    def NPFY11MChg(tp_earning, factor_earning_expect, trade_date):
        """
        :name: 一致预测净利润(FY1)变化_一月
        :desc: 未来第一年度一致预测净利润一月内预测值变化
        :unit: 元
        :view_dimension: 10000
        """
        trade_dates = sorted(set(tp_earning['publish_date']), reverse=True)
        if len(trade_dates) >= 3:
            earning_expect = FactorEarningExpectation._change_value(tp_earning, trade_date, trade_dates[2],
                                                                    'net_profit_fy1',
                                                                    'NPFY11MChg')
            factor_earning_expect = pd.merge(factor_earning_expect, earning_expect, on='security_code')
        return factor_earning_expect

    @staticmethod
    def NPFY13MChg(tp_earning, factor_earning_expect, trade_date):
        """
        :name: 一致预测净利润(FY1)变化_三月
        :desc: 未来第一年度一致预测净利润三月内预测值变化
        :unit: 元
        :view_dimension: 10000
        """
        trade_dates = sorted(set(tp_earning['publish_date']), reverse=True)
        if len(trade_dates) >= 4:
            earning_expect = FactorEarningExpectation._change_value(tp_earning, trade_date, trade_dates[3],
                                                                    'net_profit_fy1',
                                                                    'NPFY13MChg')
            factor_earning_expect = pd.merge(factor_earning_expect, earning_expect, on='security_code')
        return factor_earning_expect

    @staticmethod
    def NPFY16MChg(tp_earning, factor_earning_expect, trade_date):
        """
        :name: 一致预测净利润(FY1)变化_六月
        :desc: 未来第一年度一致预测净利润六月内预测值变化
        :unit: 元
        :view_dimension: 10000
        """
        trade_dates = sorted(set(tp_earning['publish_date']), reverse=True)
        if len(trade_dates) >= 5:
            earning_expect = FactorEarningExpectation._change_value(tp_earning, trade_date, trade_dates[4],
                                                                    'net_profit_fy1',
                                                                    'NPFY16MChg')
            factor_earning_expect = pd.merge(factor_earning_expect, earning_expect, on='security_code')
        return factor_earning_expect

    @staticmethod
    def ChgNPFY1FY2(tp_earning, factor_earning_expect, trade_date):
        """
        :name: 一致预测净利润(FY2)与一致预期净利润(FY1)的变化率
        :desc: 未来第二年度一致预测净利润与未来第一年度一致预测净利润变化率
        :unit:
        :view_dimension: 0.01
        """
        factor_earning_expect['ChgNPFY1FY2'] = factor_earning_expect['NPFY2'] - factor_earning_expect['NPFY1'] / abs(
            factor_earning_expect['NPFY1']) * 100
        return factor_earning_expect

    @staticmethod
    def ChgEPSFY1FY2(tp_earning, factor_earning_expect, trade_date):
        """
        :name: 一致预测每股收益(FY2)与一致预期每股收益(FY1)的变化率
        :desc: 未来第二年度一致预测每股收益与未来第一年度一致预测每股收益变化率
        :unit:
        :view_dimension: 0.01
        """
        factor_earning_expect['ChgEPSFY1FY2'] = factor_earning_expect['EPSFY2'] - factor_earning_expect['EPSFY1'] / abs(
            factor_earning_expect['EPSFY1']) * 100
        return factor_earning_expect

    @staticmethod
    def OptIncFY11WRT(tp_earning, factor_earning_expect, trade_date):
        """
        :name: 一致预测营业收入(FY1)变化_一周
        :desc: 未来第一年度一致预测营业收入一周内预测值变化
        :unit: 元
        :view_dimension: 10000
        """
        trade_dates = sorted(set(tp_earning['publish_date']), reverse=True)
        if len(trade_dates) >= 2:
            earning_expect = FactorEarningExpectation._change_value(tp_earning, trade_date, trade_dates[1],
                                                                    'operating_revenue_fy1',
                                                                    'OptIncFY11WRT')
            factor_earning_expect = pd.merge(factor_earning_expect, earning_expect, on='security_code')
        return factor_earning_expect

    @staticmethod
    def OptIncFY11MRT(tp_earning, factor_earning_expect, trade_date):
        """
        :name: 一致预测营业收入(FY1)变化_一月
        :desc: 未来第一年度一致预测营业收入一月内预测值变化
        :unit: 元
        :view_dimension: 10000
        """
        trade_dates = sorted(set(tp_earning['publish_date']), reverse=True)
        if len(trade_dates) >= 3:
            earning_expect = FactorEarningExpectation._change_value(tp_earning, trade_date, trade_dates[2],
                                                                    'operating_revenue_fy1',
                                                                    'OptIncFY11MRT')
            factor_earning_expect = pd.merge(factor_earning_expect, earning_expect, on='security_code')
        return factor_earning_expect

    @staticmethod
    def OptIncFY13MRT(tp_earning, factor_earning_expect, trade_date):
        """
        :name: 一致预测营业收入(FY1)变化_三月
        :desc: 未来第一年度一致预测营业收入三月内预测值变化
        :unit: 元
        :view_dimension: 10000
        """
        trade_dates = sorted(set(tp_earning['publish_date']), reverse=True)
        if len(trade_dates) >= 4:
            earning_expect = FactorEarningExpectation._change_value(tp_earning, trade_date, trade_dates[3],
                                                                    'operating_revenue_fy1',
                                                                    'OptIncFY13MRT')
            factor_earning_expect = pd.merge(factor_earning_expect, earning_expect, on='security_code')
        return factor_earning_expect

    @staticmethod
    def OptIncFY16MRT(tp_earning, factor_earning_expect, trade_date):
        """
        :name: 一致预测营业收入(FY1)变化_六月
        :desc: 未来第一年度一致预测营业收入六月内预测值变化
        :unit: 元
        :view_dimension: 10000
        """
        trade_dates = sorted(set(tp_earning['publish_date']), reverse=True)
        if len(trade_dates) >= 5:
            earning_expect = FactorEarningExpectation._change_value(tp_earning, trade_date, trade_dates[4],
                                                                    'operating_revenue_fy1',
                                                                    'OptIncFY16MRT')
            factor_earning_expect = pd.merge(factor_earning_expect, earning_expect, on='security_code')
        return factor_earning_expect

    @staticmethod
    def OptIncFY11WChg(tp_earning, factor_earning_expect, trade_date):
        """
        :name: 一致预测营业收入(FY1)变化率_一周
        :desc: 未来第一年度一致预测营业收入一周内预测值变化率
        :unit:
        :view_dimension: 0.01
        """
        trade_dates = sorted(set(tp_earning['publish_date']), reverse=True)
        if len(trade_dates) >= 2:
            earning_expect = FactorEarningExpectation._change_rate(tp_earning, trade_date, trade_dates[1],
                                                                   'operating_revenue_fy1',
                                                                   'OptIncFY11WChg')
            factor_earning_expect = pd.merge(factor_earning_expect, earning_expect, on='security_code')
        return factor_earning_expect

    @staticmethod
    def OptIncFY11MChg(tp_earning, factor_earning_expect, trade_date):
        """
        :name: 一致预测营业收入(FY1)变化率_一月
        :desc: 未来第一年度一致预测营业收入一月内预测值变化率
        :unit:
        :view_dimension: 0.01
        """
        trade_dates = sorted(set(tp_earning['publish_date']), reverse=True)
        if len(trade_dates) >= 3:
            earning_expect = FactorEarningExpectation._change_rate(tp_earning, trade_date, trade_dates[2],
                                                                   'operating_revenue_fy1',
                                                                   'OptIncFY11MChg')
            factor_earning_expect = pd.merge(factor_earning_expect, earning_expect, on='security_code')
        return factor_earning_expect

    @staticmethod
    def OptIncFY13MChg(tp_earning, factor_earning_expect, trade_date):
        """
        :name: 一致预测营业收入(FY1)变化率_三月
        :desc: 未来第一年度一致预测营业收入三月内预测值变化率
        :unit:
        :view_dimension: 0.01
        """
        trade_dates = sorted(set(tp_earning['publish_date']), reverse=True)
        if len(trade_dates) >= 4:
            earning_expect = FactorEarningExpectation._change_rate(tp_earning, trade_date, trade_dates[3],
                                                                   'operating_revenue_fy1',
                                                                   'OptIncFY13MChg')
            factor_earning_expect = pd.merge(factor_earning_expect, earning_expect, on='security_code')
        return factor_earning_expect

    @staticmethod
    def OptIncFY16MChg(tp_earning, factor_earning_expect, trade_date):
        """
        :name: 一致预测营业收入(FY1)变化率_六月
        :desc: 未来第一年度一致预测营业收入六月内预测值变化率
        :unit:
        :view_dimension: 0.01
        """
        trade_dates = sorted(set(tp_earning['publish_date']), reverse=True)
        if len(trade_dates) >= 5:
            earning_expect = FactorEarningExpectation._change_rate(tp_earning, trade_date, trade_dates[4],
                                                                   'operating_revenue_fy1',
                                                                   'OptIncFY16MChg')
            factor_earning_expect = pd.merge(factor_earning_expect, earning_expect, on='security_code')
        return factor_earning_expect

    # @staticmethod
    # def OptIncFY1SDT(tp_earning, factor_earning_expect, trade_date):
    #     """
    #     :name: 一致预测营业收入(FY1)标准差
    #     :desc: 未来第一年度一致预测营业收入标准差
    #     """
    #     return factor_earning_expect

    @staticmethod
    def CERATINGRATE1W(tp_earning, factor_earning_expect, trade_date):
        """
        :name: 一周评级变化率
        :desc: 研究机构买入评级一周内预测值变化率
        :unit:
        :view_dimension: 0.01
        """
        trade_dates = sorted(set(tp_earning['publish_date']), reverse=True)
        if len(trade_dates) >= 2:
            earning_expect = FactorEarningExpectation._change_rate(tp_earning, trade_date, trade_dates[1],
                                                                   'rating2',
                                                                   'CERATINGRATE1W')
            factor_earning_expect = pd.merge(factor_earning_expect, earning_expect, on='security_code')
        return factor_earning_expect

    @staticmethod
    def CERATINGRATE1M(tp_earning, factor_earning_expect, trade_date):
        """
        :name: 一月评级变化率
        :desc: 研究机构买入评级一月内预测值变化率
        :unit:
        :view_dimension: 0.01
        """
        trade_dates = sorted(set(tp_earning['publish_date']), reverse=True)
        if len(trade_dates) >= 3:
            earning_expect = FactorEarningExpectation._change_rate(tp_earning, trade_date, trade_dates[2],
                                                                   'rating2',
                                                                   'CERATINGRATE1M')
            factor_earning_expect = pd.merge(factor_earning_expect, earning_expect, on='security_code')
        return factor_earning_expect

    @staticmethod
    def CERATINGRATE3M(tp_earning, factor_earning_expect, trade_date):
        """
        :name: 三月评级变化率
        :desc: 研究机构买入评级三月内预测值变化率
        :unit:
        :view_dimension: 0.01
        """
        trade_dates = sorted(set(tp_earning['publish_date']), reverse=True)
        if len(trade_dates) >= 4:
            earning_expect = FactorEarningExpectation._change_rate(tp_earning, trade_date, trade_dates[3],
                                                                   'rating2',
                                                                   'CERATINGRATE3M')
            factor_earning_expect = pd.merge(factor_earning_expect, earning_expect, on='security_code')
        return factor_earning_expect

    @staticmethod
    def CERATINGRATE6M(tp_earning, factor_earning_expect, trade_date):
        """
        :name: 六月评级变化率
        :desc: 研究机构买入评级六月内预测值变化率
        :unit:
        :view_dimension: 0.01
        """
        trade_dates = sorted(set(tp_earning['publish_date']), reverse=True)
        if len(trade_dates) >= 5:
            earning_expect = FactorEarningExpectation._change_rate(tp_earning, trade_date, trade_dates[4],
                                                                   'rating2',
                                                                   'CERATINGRATE6M')
            factor_earning_expect = pd.merge(factor_earning_expect, earning_expect, on='security_code')
        return factor_earning_expect
