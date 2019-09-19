#! /usr/bin/env python
# -*- coding: utf-8 -*-

"""
@version:
@author: Wang
@file: factor_operation_capacity.py
@time: 2019-05-31
"""
import sys
from datetime import datetime

sys.path.append("../")
sys.path.append("../../")
sys.path.append("../../../")

import json
import pandas as pd
from basic_derivation.factor_base import FactorBase
from pandas.io.json import json_normalize


# from basic_derivation import app
# from ultron.ultron.cluster.invoke.cache_data import cache_data


class FactorEarningExpectation(FactorBase):
    """
    盈利预期
    """

    def __init__(self, name):
        super(FactorEarningExpectation, self).__init__(name)

    def create_dest_tables(self):
        """
        创建数据库表
        :return:
        """
        drop_sql = """drop table if exists `{0}`""".format(self._name)
        create_sql = """create table `{0}`(
                    `id`	INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
                    `security_code` varchar(24) NOT NULL,
                    `trade_date` date NOT NULL,
                    `NPFY1` decimal(19,4),
                    `NPFY2` decimal(19,4),
                    `EPSFY1` decimal(19,4),
                    `EPSFY2` decimal(19,4),
                    `OptIncFY1` decimal(19,4),
                    `OptIncFY2` decimal(19,4),
                    `CEPEFY1` decimal(19,4),
                    `CEPEFY2` decimal(19,4),
                    `CEPBFY1` decimal(19,4),
                    `CEPBFY2` decimal(19,4),
                    `CEPEGFY1` decimal(19,4),
                    `CEPEGFY2` decimal(19,4),
                    `NPFY11WRT` decimal(19,4),
                    `NPFY11MRT` decimal(19,4),
                    `NPFY13MRT` decimal(19,4),
                    `NPFY16MRT` decimal(19,4),
                    `NPFY11WChg` decimal(19,4),
                    `NPFY11MChg` decimal(19,4),
                    `NPFY13MChg` decimal(19,4),
                    `NPFY16MChg` decimal(19,4),
                    `NPFY1SDT` decimal(19,4),
                    `EPSFY11WRT` decimal(19,4),
                    `EPSFY11MRT` decimal(19,4),
                    `EPSFY13MRT` decimal(19,4),
                    `EPSFY16MRT` decimal(19,4),
                    `EPSFY11WChg` decimal(19,4),
                    `EPSFY11MChg` decimal(19,4),
                    `EPSFY13MChg` decimal(19,4),
                    `EPSFY16MChg` decimal(19,4),
                    `EPSFY1SDT` decimal(19,4),
                    `ChgNPFY1FY2` decimal(19,4),
                    `ChgEPSFY1FY2` decimal(19,4),
                    `OptIncFY11WRT` decimal(19,4),
                    `OptIncFY11MRT` decimal(19,4),
                    `OptIncFY13MRT` decimal(19,4),
                    `OptIncFY16MRT` decimal(19,4),
                    `OptIncFY11WChg` decimal(19,4),
                    `OptIncFY11MChg` decimal(19,4),
                    `OptIncFY13MChg` decimal(19,4),
                    `OptIncFY16MChg` decimal(19,4),
                    `OptIncFY1SDT` decimal(19,4),
                    `CERATINGRATE1W` decimal(19,4),
                    `CERATINGRATE1M` decimal(19,4),
                    `CERATINGRATE3M` decimal(19,4),
                    `CERATINGRATE6M` decimal(19,4),
                    constraint {0}_uindex
                    unique (`trade_date`,`security_code`)
                    )ENGINE=InnoDB DEFAULT CHARSET=utf8;""".format(self._name)
        super(FactorEarningExpectation, self)._create_tables(create_sql, drop_sql)

    @staticmethod
    def NPFY1(tp_earning, factor_earning_expect, trade_date, dependencies=['net_profit_fy1']):
        """
        一致预期净利润(FY1)
        :param dependencies:
        :param tp_earning:
        :param factor_earning_expect:
        :return:
        """
        earning_expect = tp_earning[tp_earning['publish_date'] == trade_date].loc[:, dependencies]
        earning_expect.rename(columns={'net_profit_fy1': 'NPFY1'}, inplace=True)
        factor_earning_expect = pd.merge(factor_earning_expect, earning_expect, on='security_code')
        return factor_earning_expect

    @staticmethod
    def NPFY2(tp_earning, factor_earning_expect, trade_date, dependencies=['net_profit_fy2']):
        """
        一致预期净利润(FY2)
        :param dependencies:
        :param tp_earning:
        :param factor_earning_expect:
        :return:
        """
        earning_expect = tp_earning[tp_earning['publish_date'] == trade_date].loc[:, dependencies]
        earning_expect.rename(columns={'net_profit_fy2': 'NPFY2'}, inplace=True)
        factor_earning_expect = pd.merge(factor_earning_expect, earning_expect, on='security_code')
        return factor_earning_expect

    @staticmethod
    def EPSFY1(tp_earning, factor_earning_expect, trade_date, dependencies=['eps_fy1']):
        """
        一致预期每股收益（FY1）
        :param dependencies:
        :param tp_earning:
        :param factor_earning_expect:
        :return:
        """
        earning_expect = tp_earning[tp_earning['publish_date'] == trade_date].loc[:, dependencies]
        earning_expect.rename(columns={'eps_fy1': 'EPSFY1'}, inplace=True)
        factor_earning_expect = pd.merge(factor_earning_expect, earning_expect, on='security_code')
        return factor_earning_expect

    @staticmethod
    def EPSFY2(tp_earning, factor_earning_expect, trade_date, dependencies=['eps_fy2']):
        """
        一致预期每股收益（FY2）
        :param dependencies:
        :param tp_earning:
        :param factor_earning_expect:
        :return:
        """
        earning_expect = tp_earning[tp_earning['publish_date'] == trade_date].loc[:, dependencies]
        earning_expect.rename(columns={'eps_fy2': 'EPSFY2'}, inplace=True)
        factor_earning_expect = pd.merge(factor_earning_expect, earning_expect, on='security_code')
        return factor_earning_expect

    @staticmethod
    def OptIncFY1(tp_earning, factor_earning_expect, trade_date, dependencies=['operating_revenue_fy1']):
        """
        一致预期营业收入（FY1）
        :param dependencies:
        :param tp_earning:
        :param factor_earning_expect:
        :return:
        """
        earning_expect = tp_earning[tp_earning['publish_date'] == trade_date].loc[:, dependencies]
        earning_expect.rename(columns={'operating_revenue_fy1': 'OptIncFY1'}, inplace=True)
        factor_earning_expect = pd.merge(factor_earning_expect, earning_expect, on='security_code')
        return factor_earning_expect

    @staticmethod
    def OptIncFY2(tp_earning, factor_earning_expect, trade_date, dependencies=['operating_revenue_fy2']):
        """
        一致预期营业收入（FY2）
        :param dependencies:
        :param tp_earning:
        :param factor_earning_expect:
        :return:
        """
        earning_expect = tp_earning[tp_earning['publish_date'] == trade_date].loc[:, dependencies]
        earning_expect.rename(columns={'operating_revenue_fy2': 'OptIncFY2'}, inplace=True)
        factor_earning_expect = pd.merge(factor_earning_expect, earning_expect, on='security_code')
        return factor_earning_expect

    @staticmethod
    def CEPEFY1(tp_earning, factor_earning_expect, trade_date, dependencies=['pe_fy1']):
        """
        一致预期市盈率(PE)(FY1)
        :param dependencies:
        :param tp_earning:
        :param factor_earning_expect:
        :return:
        """
        earning_expect = tp_earning[tp_earning['publish_date'] == trade_date].loc[:, dependencies]
        earning_expect.rename(columns={'pe_fy1': 'CEPEFY1'}, inplace=True)
        factor_earning_expect = pd.merge(factor_earning_expect, earning_expect, on='security_code')
        return factor_earning_expect

    @staticmethod
    def CEPEFY2(tp_earning, factor_earning_expect, trade_date, dependencies=['pe_fy2']):
        """
        一致预期市盈率(PE)(FY2)
        :param dependencies:
        :param tp_earning:
        :param factor_earning_expect:
        :return:
        """
        earning_expect = tp_earning[tp_earning['publish_date'] == trade_date].loc[:, dependencies]
        earning_expect.rename(columns={'pe_fy2': 'CEPEFY2'}, inplace=True)
        factor_earning_expect = pd.merge(factor_earning_expect, earning_expect, on='security_code')
        return factor_earning_expect

    @staticmethod
    def CEPBFY1(tp_earning, factor_earning_expect, trade_date, dependencies=['pb_fy1']):
        """
        一致预期市净率(PB)(FY1)
        :param dependencies:
        :param tp_earning:
        :param factor_earning_expect:
        :return:
        """
        earning_expect = tp_earning[tp_earning['publish_date'] == trade_date].loc[:, dependencies]
        earning_expect.rename(columns={'pb_fy1': 'CEPBFY1'}, inplace=True)
        factor_earning_expect = pd.merge(factor_earning_expect, earning_expect, on='security_code')
        return factor_earning_expect

    @staticmethod
    def CEPBFY2(tp_earning, factor_earning_expect, trade_date, dependencies=['pb_fy2']):
        """
        一致预期市净率(PB)(FY2)
        :param dependencies:
        :param tp_earning:
        :param factor_earning_expect:
        :return:
        """
        earning_expect = tp_earning[tp_earning['publish_date'] == trade_date].loc[:, dependencies]
        earning_expect.rename(columns={'pb_fy2': 'CEPBFY2'}, inplace=True)
        factor_earning_expect = pd.merge(factor_earning_expect, earning_expect, on='security_code')
        return factor_earning_expect

    @staticmethod
    def CEPEGFY1(tp_earning, factor_earning_expect, trade_date, dependencies=['peg_fy1']):
        """
        市盈率相对盈利增长比率(FY1)
        :param dependencies:
        :param tp_earning:
        :param factor_earning_expect:
        :return:
        """
        earning_expect = tp_earning[tp_earning['publish_date'] == trade_date].loc[:, dependencies]
        earning_expect.rename(columns={'peg_fy1': 'CEPEGFY1'}, inplace=True)
        factor_earning_expect = pd.merge(factor_earning_expect, earning_expect, on='security_code')
        return factor_earning_expect

    @staticmethod
    def CEPEGFY2(tp_earning, factor_earning_expect, trade_date, dependencies=['peg_fy2']):
        """
        市盈率相对盈利增长比率(FY2)
        :param dependencies:
        :param tp_earning:
        :param factor_earning_expect:
        :return:
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
                                      earning_expect[colunm + '_y'] * 100
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
        一致预测净利润(FY1)变化率_一周
        :param dependencies:
        :param tp_earning:
        :param factor_earning_expect:
        :return:
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
        一致预测净利润(FY1)变化率_一月
        :param dependencies:
        :param tp_earning:
        :param factor_earning_expect:
        :return:
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
        一致预测净利润(FY1)变化率_三月
        :param dependencies:
        :param tp_earning:
        :param factor_earning_expect:
        :return:
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
        一致预测净利润(FY1)变化率_六月
        :param dependencies:
        :param tp_earning:
        :param factor_earning_expect:
        :return:
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
        一致预测每股收益(FY1)变化_一周
        :param dependencies:
        :param tp_earning:
        :param factor_earning_expect:
        :return:
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
        一致预测每股收益(FY1)变化_一月
        :param dependencies:
        :param tp_earning:
        :param factor_earning_expect:
        :return:
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
        一致预测每股收益(FY1)变化_三月
        :param dependencies:
        :param tp_earning:
        :param factor_earning_expect:
        :return:
        """
        trade_dates = sorted(set(tp_earning['publish_date']), reverse=True)
        if len(trade_dates) >= 3:
            earning_expect = FactorEarningExpectation._change_value(tp_earning, trade_date, trade_dates[2],
                                                                    'eps_fy1',
                                                                    'EPSFY13MChg')
            factor_earning_expect = pd.merge(factor_earning_expect, earning_expect, on='security_code')
        return factor_earning_expect

    @staticmethod
    def EPSFY16MChg(tp_earning, factor_earning_expect, trade_date):
        """
        一致预测每股收益(FY1)变化_六月
        :param dependencies:
        :param tp_earning:
        :param factor_earning_expect:
        :return:
        """
        trade_dates = sorted(set(tp_earning['publish_date']), reverse=True)
        if len(trade_dates) >= 3:
            earning_expect = FactorEarningExpectation._change_value(tp_earning, trade_date, trade_dates[2],
                                                                    'eps_fy1',
                                                                    'EPSFY16MChg')
            factor_earning_expect = pd.merge(factor_earning_expect, earning_expect, on='security_code')
        return factor_earning_expect

    @staticmethod
    def EPSFY11WRT(tp_earning, factor_earning_expect, trade_date):
        """
        一致预测每股收益(FY1)变化率_一周
        :param dependencies:
        :param tp_earning:
        :param factor_earning_expect:
        :return:
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
        一致预测每股收益(FY1)变化率_一月
        :param dependencies:
        :param tp_earning:
        :param factor_earning_expect:
        :return:
        """
        trade_dates = sorted(set(tp_earning['publish_date']), reverse=True)
        if len(trade_dates) >= 3:
            earning_expect = FactorEarningExpectation._change_value(tp_earning, trade_date, trade_dates[2],
                                                                    'eps_fy1',
                                                                    'EPSFY11MRT')
            factor_earning_expect = pd.merge(factor_earning_expect, earning_expect, on='security_code')
        return factor_earning_expect

    @staticmethod
    def EPSFY13MRT(tp_earning, factor_earning_expect, trade_date):
        """
        一致预测每股收益(FY1)变化率_三月
        :param dependencies:
        :param tp_earning:
        :param factor_earning_expect:
        :return:
        """
        trade_dates = sorted(set(tp_earning['publish_date']), reverse=True)
        if len(trade_dates) >= 3:
            earning_expect = FactorEarningExpectation._change_value(tp_earning, trade_date, trade_dates[2],
                                                                    'eps_fy1',
                                                                    'EPSFY13MRT')
            factor_earning_expect = pd.merge(factor_earning_expect, earning_expect, on='security_code')
        return factor_earning_expect

    @staticmethod
    def EPSFY16MRT(tp_earning, factor_earning_expect, trade_date):
        """
        一致预测每股收益(FY1)变化率_六月
        :param dependencies:
        :param tp_earning:
        :param factor_earning_expect:
        :return:
        """
        trade_dates = sorted(set(tp_earning['publish_date']), reverse=True)
        if len(trade_dates) >= 3:
            earning_expect = FactorEarningExpectation._change_value(tp_earning, trade_date, trade_dates[2],
                                                                    'eps_fy1',
                                                                    'EPSFY16MRT')
            factor_earning_expect = pd.merge(factor_earning_expect, earning_expect, on='security_code')
        return factor_earning_expect

    @staticmethod
    def NPFY11WChg(tp_earning, factor_earning_expect, trade_date):
        """
        一致预测净利润(FY1)变化_一周
        :param dependencies:
        :param tp_earning:
        :param factor_earning_expect:
        :return:
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
        一致预测净利润(FY1)变化_一月
        :param dependencies:
        :param tp_earning:
        :param factor_earning_expect:
        :return:
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
        一致预测净利润(FY1)变化_三月
        :param dependencies:
        :param tp_earning:
        :param factor_earning_expect:
        :return:
        """
        trade_dates = sorted(set(tp_earning['publish_date']), reverse=True)
        if len(trade_dates) >= 3:
            earning_expect = FactorEarningExpectation._change_value(tp_earning, trade_date, trade_dates[2],
                                                                    'net_profit_fy1',
                                                                    'NPFY13MChg')
            factor_earning_expect = pd.merge(factor_earning_expect, earning_expect, on='security_code')
        return factor_earning_expect

    @staticmethod
    def NPFY16MChg(tp_earning, factor_earning_expect, trade_date):
        """
        一致预测净利润(FY1)变化_六月
        :param dependencies:
        :param tp_earning:
        :param factor_earning_expect:
        :return:
        """
        trade_dates = sorted(set(tp_earning['publish_date']), reverse=True)
        if len(trade_dates) >= 3:
            earning_expect = FactorEarningExpectation._change_value(tp_earning, trade_date, trade_dates[2],
                                                                    'net_profit_fy1',
                                                                    'NPFY16MChg')
            factor_earning_expect = pd.merge(factor_earning_expect, earning_expect, on='security_code')
        return factor_earning_expect

    @staticmethod
    def ChgNPFY1FY2(tp_earning, factor_earning_expect, trade_date):
        """
        一致预测净利润(FY2)与一致预期净利润(FY1)的变化率
        :param dependencies:
        :param tp_earning:
        :param factor_earning_expect:
        :return:
        """
        factor_earning_expect['ChgNPFY1FY2'] = factor_earning_expect['NPFY2'] - factor_earning_expect['NPFY1'] / abs(
            factor_earning_expect['NPFY1']) * 100
        return factor_earning_expect

    @staticmethod
    def ChgEPSFY1FY2(tp_earning, factor_earning_expect, trade_date):
        """
        一致预测每股收益(FY2)与一致预期每股收益(FY1)的变化率
        :param dependencies:
        :param tp_earning:
        :param factor_earning_expect:
        :return:
        """
        factor_earning_expect['ChgEPSFY1FY2'] = factor_earning_expect['EPSFY2'] - factor_earning_expect['EPSFY1'] / abs(
            factor_earning_expect['EPSFY1']) * 100
        return factor_earning_expect

    @staticmethod
    def OptIncFY11WRT(tp_earning, factor_earning_expect, trade_date):
        """
        一致预测营业收入(FY1)变化_一周
        :param dependencies:
        :param tp_earning:
        :param factor_earning_expect:
        :return:
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
        一致预测营业收入(FY1)变化_一月
        :param dependencies:
        :param tp_earning:
        :param factor_earning_expect:
        :return:
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
        一致预测营业收入(FY1)变化_三月
        :param dependencies:
        :param tp_earning:
        :param factor_earning_expect:
        :return:
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
        一致预测营业收入(FY1)变化_六月
        :param dependencies:
        :param tp_earning:
        :param factor_earning_expect:
        :return:
        """
        trade_dates = sorted(set(tp_earning['publish_date']), reverse=True)
        if len(trade_dates) >= 5:
            earning_expect = FactorEarningExpectation._change_value(tp_earning, trade_date, trade_dates[4],
                                                                    'operating_revenue_fy1',
                                                                    'OptIncFY16MRT')
            factor_earning_expect = pd.merge(factor_earning_expect, earning_expect, on='security_code')
        return factor_earning_expect

    @staticmethod
    def OptIncFY11MChg(tp_earning, factor_earning_expect, trade_date):
        """
        一致预测营业收入(FY1)变化率_一周
        :param dependencies:
        :param tp_earning:
        :param factor_earning_expect:
        :return:
        """
        trade_dates = sorted(set(tp_earning['publish_date']), reverse=True)
        if len(trade_dates) >= 2:
            earning_expect = FactorEarningExpectation._change_rate(tp_earning, trade_date, trade_dates[1],
                                                                   'operating_revenue_fy2',
                                                                   'OptIncFY11MChg')
            factor_earning_expect = pd.merge(factor_earning_expect, earning_expect, on='security_code')
        return factor_earning_expect

    @staticmethod
    def OptIncFY13MChg(tp_earning, factor_earning_expect, trade_date):
        """
        一致预测营业收入(FY1)变化率一月
        :param dependencies:
        :param tp_earning:
        :param factor_earning_expect:
        :return:
        """
        trade_dates = sorted(set(tp_earning['publish_date']), reverse=True)
        if len(trade_dates) >= 3:
            earning_expect = FactorEarningExpectation._change_rate(tp_earning, trade_date, trade_dates[2],
                                                                   'operating_revenue_fy2',
                                                                   'OptIncFY13MChg')
            factor_earning_expect = pd.merge(factor_earning_expect, earning_expect, on='security_code')
        return factor_earning_expect

    @staticmethod
    def OptIncFY16MChg(tp_earning, factor_earning_expect, trade_date):
        """
        一致预测营业收入(FY1)变化率_三月
        :param dependencies:
        :param tp_earning:
        :param factor_earning_expect:
        :return:
        """
        trade_dates = sorted(set(tp_earning['publish_date']), reverse=True)
        if len(trade_dates) >= 4:
            earning_expect = FactorEarningExpectation._change_rate(tp_earning, trade_date, trade_dates[3],
                                                                   'operating_revenue_fy2',
                                                                   'OptIncFY16MChg')
            factor_earning_expect = pd.merge(factor_earning_expect, earning_expect, on='security_code')
        return factor_earning_expect

    @staticmethod
    def OptIncFY1SDT(tp_earning, factor_earning_expect, trade_date):
        """
        一致预测营业收入(FY1)变化率_六月
        :param dependencies:
        :param tp_earning:
        :param factor_earning_expect:
        :return:
        """
        trade_dates = sorted(set(tp_earning['publish_date']), reverse=True)
        if len(trade_dates) >= 5:
            earning_expect = FactorEarningExpectation._change_rate(tp_earning, trade_date, trade_dates[4],
                                                                   'operating_revenue_fy2',
                                                                   'OptIncFY1SDT')
            factor_earning_expect = pd.merge(factor_earning_expect, earning_expect, on='security_code')
        return factor_earning_expect

    @staticmethod
    def CERATINGRATE1W(tp_earning, factor_earning_expect, trade_date):
        """
        一周评级变化率
        :param dependencies:
        :param tp_earning:
        :param factor_earning_expect:
        :return:
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
        一月评级变化率
        :param dependencies:
        :param tp_earning:
        :param factor_earning_expect:
        :return:
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
        三月评级变化率
        :param dependencies:
        :param tp_earning:
        :param factor_earning_expect:
        :return:
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
        六月评级变化率
        :param dependencies:
        :param tp_earning:
        :param factor_earning_expect:
        :return:
        """
        trade_dates = sorted(set(tp_earning['publish_date']), reverse=True)
        if len(trade_dates) >= 5:
            earning_expect = FactorEarningExpectation._change_rate(tp_earning, trade_date, trade_dates[4],
                                                                   'rating2',
                                                                   'CERATINGRATE6M')
            factor_earning_expect = pd.merge(factor_earning_expect, earning_expect, on='security_code')
        return factor_earning_expect


def calculate(trade_date, tp_earning):  # 计算对应因子
    print(trade_date)
    # tp_earning = earning_sets_dic['tp_earning']
    # ttm_earning = earning_sets_dic['ttm_earning']
    # ttm_earning_5y = earning_sets_dic['ttm_earning_5y']
    # tp_earning = tp_earning.set_index('security_code')

    earning = FactorEarningExpectation('factor_earning_expectation')  # 注意, 这里的name要与client中新建table时的name一致, 不然回报错
    tp_earning = tp_earning.set_index('security_code')
    # 因子计算
    factor_earning_expect = pd.DataFrame()
    factor_earning_expect['security_code'] = tp_earning[
        tp_earning['publish_date'] == datetime.strptime(trade_date, "%Y%m%d")].index

    factor_earning_expect = earning.NPFY1(tp_earning, factor_earning_expect, trade_date)
    factor_earning_expect = earning.NPFY2(tp_earning, factor_earning_expect, trade_date)
    factor_earning_expect = earning.EPSFY1(tp_earning, factor_earning_expect, trade_date)
    factor_earning_expect = earning.EPSFY2(tp_earning, factor_earning_expect, trade_date)
    factor_earning_expect = earning.OptIncFY1(tp_earning, factor_earning_expect, trade_date)
    factor_earning_expect = earning.OptIncFY2(tp_earning, factor_earning_expect, trade_date)
    factor_earning_expect = earning.CEPEFY1(tp_earning, factor_earning_expect, trade_date)
    factor_earning_expect = earning.CEPEFY2(tp_earning, factor_earning_expect, trade_date)
    factor_earning_expect = earning.CEPBFY1(tp_earning, factor_earning_expect, trade_date)
    factor_earning_expect = earning.CEPBFY2(tp_earning, factor_earning_expect, trade_date)
    factor_earning_expect = earning.CEPEGFY1(tp_earning, factor_earning_expect, trade_date)
    factor_earning_expect = earning.CEPEGFY2(tp_earning, factor_earning_expect, trade_date)
    factor_earning_expect = earning.NPFY11WRT(tp_earning, factor_earning_expect, trade_date)
    factor_earning_expect = earning.NPFY11MRT(tp_earning, factor_earning_expect, trade_date)
    factor_earning_expect = earning.NPFY13MRT(tp_earning, factor_earning_expect, trade_date)
    factor_earning_expect = earning.NPFY16MRT(tp_earning, factor_earning_expect, trade_date)
    factor_earning_expect = earning.NPFY11WChg(tp_earning, factor_earning_expect, trade_date)
    factor_earning_expect = earning.NPFY11MChg(tp_earning, factor_earning_expect, trade_date)
    factor_earning_expect = earning.NPFY13MChg(tp_earning, factor_earning_expect, trade_date)
    factor_earning_expect = earning.NPFY16MChg(tp_earning, factor_earning_expect, trade_date)
    # factor_earning_expect = earning.NPFY1SDT(tp_earning, factor_earning_expect, trade_date)
    factor_earning_expect = earning.EPSFY11WRT(tp_earning, factor_earning_expect, trade_date)
    factor_earning_expect = earning.EPSFY11MRT(tp_earning, factor_earning_expect, trade_date)
    factor_earning_expect = earning.EPSFY13MRT(tp_earning, factor_earning_expect, trade_date)
    factor_earning_expect = earning.EPSFY16MRT(tp_earning, factor_earning_expect, trade_date)
    factor_earning_expect = earning.EPSFY11WChg(tp_earning, factor_earning_expect, trade_date)
    factor_earning_expect = earning.EPSFY11MChg(tp_earning, factor_earning_expect, trade_date)
    factor_earning_expect = earning.EPSFY13MChg(tp_earning, factor_earning_expect, trade_date)
    factor_earning_expect = earning.EPSFY16MChg(tp_earning, factor_earning_expect, trade_date)
    # factor_earning_expect = earning.EPSFY1SDT(tp_earning, factor_earning_expect, trade_date)
    factor_earning_expect = earning.ChgNPFY1FY2(tp_earning, factor_earning_expect, trade_date)
    factor_earning_expect = earning.ChgEPSFY1FY2(tp_earning, factor_earning_expect, trade_date)
    factor_earning_expect = earning.OptIncFY11WRT(tp_earning, factor_earning_expect, trade_date)
    factor_earning_expect = earning.OptIncFY11MRT(tp_earning, factor_earning_expect, trade_date)
    factor_earning_expect = earning.OptIncFY13MRT(tp_earning, factor_earning_expect, trade_date)
    factor_earning_expect = earning.OptIncFY16MRT(tp_earning, factor_earning_expect, trade_date)
    # factor_earning_expect = earning.OptIncFY11WChg(tp_earning, factor_earning_expect, trade_date)
    factor_earning_expect = earning.OptIncFY11MChg(tp_earning, factor_earning_expect, trade_date)
    factor_earning_expect = earning.OptIncFY13MChg(tp_earning, factor_earning_expect, trade_date)
    factor_earning_expect = earning.OptIncFY16MChg(tp_earning, factor_earning_expect, trade_date)
    factor_earning_expect = earning.OptIncFY1SDT(tp_earning, factor_earning_expect, trade_date)
    factor_earning_expect = earning.CERATINGRATE1W(tp_earning, factor_earning_expect, trade_date)
    factor_earning_expect = earning.CERATINGRATE1M(tp_earning, factor_earning_expect, trade_date)
    factor_earning_expect = earning.CERATINGRATE3M(tp_earning, factor_earning_expect, trade_date)
    factor_earning_expect = earning.CERATINGRATE6M(tp_earning, factor_earning_expect, trade_date)

    factor_earning_expect['trade_date'] = str(trade_date)
    earning._storage_data(factor_earning_expect, trade_date)


# @app.task()
def factor_calculate(**kwargs):
    print("constrain_kwargs: {}".format(kwargs))
    date_index = kwargs['date_index']
    session = kwargs['session']
    content1 = cache_data.get_cache(session + str(date_index), date_index)
    print("len_con1: %s" % len(content1))
    tp_earning = json_normalize(json.loads(str(content1, encoding='utf8')))
    # cache_date.get_cache使得index的名字丢失， 所以数据需要按照下面的方式设置index
    tp_earning.set_index('security_code', inplace=True)
    calculate(date_index, tp_earning)
