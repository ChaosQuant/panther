#! /usr/bin/env python
# -*- coding: utf-8 -*-

"""
@version:
@author: Wang
@file: factor_management.py
@time: 2019-05-31
"""
import sys
sys.path.append("../")
sys.path.append("../../")
sys.path.append("../../../")

import json
import numpy as np
import pandas as pd
from factor.factor_base import FactorBase
from factor.utillities.calc_tools import CalcTools
from pandas.io.json import json_normalize

from factor import app
from ultron.cluster.invoke.cache_data import cache_data


class FactorEarning(FactorBase):
    """
    收益质量
        --盈利相关
    """
    def __init__(self, name):
        super(FactorEarning, self).__init__(name)

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
                    `net_profit_ratio` decimal(19,4),
                    `operating_profit_ratio` decimal(19,4),
                    `np_to_tor` decimal(19,4),
                    `operating_profit_to_tor` decimal(19,4),
                    `gross_income_ratio`  decimal(19,4),
                    `ebit_to_tor` decimal(19,4),
                    `roa` decimal(19,4),
                    `roa5` decimal(19,4),
                    `roe` decimal(19,4),
                    `roe5` decimal(19,4),
                    `roe_diluted` decimal(19,4),
                    `roe_avg` decimal(19,4),
                    `roe_cut` decimal(19,4),
                    `roa_ebit_ttm` decimal(19,4),
                    `operating_ni_to_tp_ttm` decimal(19,4),
                    `operating_ni_to_tp_latest` decimal(19,4),
                    `invest_r_associates_to_tp_ttm` decimal(19,4),
                    `invest_r_associates_to_tp_latest` decimal(19,4),
                    `npcut_to_np` decimal(19,4),
                    `interest_cover_ttm` decimal(19,4),
                    `net_non_oi_to_tp_ttm` decimal(19,4),
                    `net_non_oi_to_tp_latest` decimal(19,4),
                    PRIMARY KEY(`id`,`trade_date`,`symbol`)
                    )ENGINE=InnoDB DEFAULT CHARSET=utf8;""".format(self._name)
        super(FactorEarning, self)._create_tables(create_sql, drop_sql)

    @staticmethod
    def net_profit_ratio(ttm_earning, factor_earning, dependencies=['net_profit', 'operating_revenue']):
        """
        销售净利率（Net profit ratio）
        销售净利率=净利润/营业收入
        :param dependencies:
        :param ttm_earning:
        :param factor_earning:
        :return:
        """
        earning = ttm_earning.loc[:, dependencies]
        earning['net_profit_ratio'] = np.where(
            CalcTools.is_zero(earning.operating_revenue.values), 0,
            earning.net_profit.values / earning.operating_revenue.values)
        earning = earning.drop(dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, earning, on="symbol")
        return factor_earning

    @staticmethod
    def operating_profit_ratio(ttm_earning, factor_earning, dependencies=['operating_profit', 'operating_revenue']):
        """
        营业净利率
        营业净利率=营业利润/营业收入
        :param dependencies:
        :param ttm_earning:
        :param factor_earning:
        :return:
        """
        earning = ttm_earning.loc[:, dependencies]
        earning['operating_profit_ratio'] = np.where(
            CalcTools.is_zero(earning.operating_revenue.values), 0,
            earning.operating_profit.values / earning.operating_revenue.values)
        earning = earning.drop(dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, earning, on="symbol")
        return factor_earning

    @staticmethod
    def np_to_tor(ttm_earning, factor_earning, dependencies=['net_profit', 'total_operating_revenue']):
        """
        净利润与营业总收入之比
        净利润与营业总收入之比=净利润/营业总收入
        :param dependencies:
        :param ttm_earning:
        :param factor_earning:
        :return:
        """
        earning = ttm_earning.loc[:, dependencies]
        earning['np_to_tor'] = np.where(
            CalcTools.is_zero(earning.total_operating_revenue.values), 0,
            earning.net_profit.values / earning.total_operating_revenue.values)
        earning = earning.drop(dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, earning, on="symbol")
        return factor_earning

    @staticmethod
    def operating_profit_to_tor(ttm_earning, factor_earning, dependencies=['operating_profit', 'total_operating_revenue']):
        """
        营业利润与营业总收入之比
        营业利润与营业总收入之比=营业利润/营业总收入
        :param dependencies:
        :param ttm_earning:
        :param factor_earning:
        :return:
        """
        earning = ttm_earning.loc[:, dependencies]
        earning['operating_profit_to_tor'] = np.where(
            CalcTools.is_zero(earning.total_operating_revenue.values), 0,
            earning.operating_profit.values / earning.total_operating_revenue.values)
        earning = earning.drop(dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, earning, on="symbol")
        return factor_earning

    @staticmethod
    def gross_income_ratio(ttm_earning, factor_earning, dependencies=['operating_revenue', 'operating_cost']):
        """
        销售毛利率
        销售毛利率=（营业收入-营业成本）/营业收入
        :param dependencies:
        :param ttm_earning:
        :param factor_earning:
        :return:
        """
        earning = ttm_earning.loc[:, dependencies]
        earning['gross_income_ratio'] = np.where(
            CalcTools.is_zero(earning.operating_revenue.values), 0,
            (earning.operating_revenue.values-earning.operating_cost.values)
            / earning.operating_revenue.values)
        earning = earning.drop(dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, earning, on="symbol")
        return factor_earning

    @staticmethod
    def ebit_to_tor(ttm_earning, factor_earning, dependencies=['total_profit', 'financial_expense', 'interest_income', 'total_operating_revenue']):
        """
        息税前利润与营业总收入之比
        息税前利润与营业总收入之比=（利润总额+利息支出-利息收入)/营业总收入
        :param dependencies:
        :param ttm_earning:
        :param factor_earning:
        :return:
        """
        earning = ttm_earning.loc[:, dependencies]
        earning['ebit_to_tor'] = np.where(
            CalcTools.is_zero(earning.total_operating_revenue.values), 0,
            (earning.total_profit.values +
             earning.financial_expense.values -
             earning.interest_income.values)
            / earning.total_operating_revenue.values)
        earning = earning.drop(dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, earning, on="symbol")
        return factor_earning

    @staticmethod
    def roa(ttm_earning, factor_earning, dependencies=['net_profit', 'total_assets']):
        """
        资产回报率
        资产回报率=净利润/总资产
        :param dependencies:
        :param ttm_earning:
        :param factor_earning:
        :return:
        """
        earning = ttm_earning.loc[:, dependencies]
        earning['roa'] = np.where(
            CalcTools.is_zero(earning.total_assets.values), 0,
            earning.net_profit.values / earning.total_assets.values)
        earning = earning.drop(dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, earning, on="symbol")
        return factor_earning

    @staticmethod
    def roa5(ttm_earning_5y, factor_earning, dependencies=['net_profit', 'total_assets']):
        """
        5年权益回报率
        5年权益回报率=净利润/总资产
        :param dependencies:
        :param ttm_earning_5y:
        :param factor_earning:
        :return:
        """
        earning = ttm_earning_5y.loc[:, dependencies]
        earning['roa5'] = np.where(
            CalcTools.is_zero(earning.total_assets.values), 0,
            earning.net_profit.values / earning.total_assets.values / 4)
        earning = earning.drop(dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, earning, on="symbol")
        return factor_earning

    @staticmethod
    def roe(ttm_earning, factor_earning, dependencies=['net_profit', 'total_owner_equities']):
        """
        权益回报率
        权益回报率=净利润/股东权益
        :param dependencies:
        :param ttm_earning:
        :param factor_earning:
        :return:
        """
        earning = ttm_earning.loc[:, dependencies]
        earning['roe'] = np.where(
            CalcTools.is_zero(earning.total_owner_equities.values), 0,
            earning.net_profit.values / earning.total_owner_equities.values/4)
        earning = earning.drop(dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, earning, on="symbol")
        return factor_earning

    @staticmethod
    def roe5(ttm_earning_5y, factor_earning, dependencies=['net_profit', 'total_owner_equities']):
        """
        5年权益回报率
        :param dependencies:
        :param ttm_earning_5y:
        :param factor_earning:
        :return:
        """
        earning = ttm_earning_5y.loc[:, dependencies]
        earning['roe5'] = np.where(
            CalcTools.is_zero(earning.total_owner_equities.values), 0,
            earning.net_profit.values / earning.total_owner_equities.values / 4)
        earning = earning.drop(dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, earning, on="symbol")
        return factor_earning

    @staticmethod
    def roe_diluted(tp_earning, factor_earning, dependencies=['np_parent_company_owners', 'equities_parent_company_owners']):
        """
        净资产收益率（摊薄）
        净资产收益率（摊薄）=归属于母公司的净利润/期末归属于母公司的所有者权益
        :param dependencies:
        :param tp_earning:
        :param factor_earning:
        :return:
        """
        earning = tp_earning.loc[:, dependencies]
        earning['roe_diluted'] = np.where(
            CalcTools.is_zero(earning.equities_parent_company_owners.values), 0,
            earning.np_parent_company_owners.values /
            earning.equities_parent_company_owners.values/4)
        earning = earning.drop(dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, earning, on="symbol")
        return factor_earning

    @staticmethod
    def roe_avg(ttm_earning, factor_earning, dependencies=['np_parent_company_owners', 'equities_parent_company_owners']):
        """
        资产收益率（平均）
        资产收益率（平均）=归属于母公司的净利润*2/(期末归属于母公司的所有者权益
        + 期初归属于母公司的所有者权益）*100%
        :param dependencies:
        :param ttm_earning:
        :param factor_earning:
        :return:
        """

        earning = ttm_earning.loc[:, dependencies]
        earning['roe_avg'] = np.where(
            CalcTools.is_zero(earning.equities_parent_company_owners.values), 0,
            earning.np_parent_company_owners.values /
            earning.equities_parent_company_owners.values / 4)
        earning = earning.drop(dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, earning, on="symbol")
        return factor_earning

    def roe_weighted(self, ttm_earning, factor_earning):
        """
        :param ttm_earning:
        :param factor_earning:
        :return:
        """
        pass

    @staticmethod
    def roe_cut(tp_earning, factor_earning, dependencies=['adjusted_profit', 'equities_parent_company_owners']):
        """
        :param dependencies:
        :param tp_earning:
        :param factor_earning:
        :return:
        """

        earning = tp_earning.loc[:, dependencies]
        earning['roe_cut'] = np.where(
            CalcTools.is_zero(earning.equities_parent_company_owners.values), 0,
            earning.adjusted_profit.values /
            earning.equities_parent_company_owners.values / 4)
        earning = earning.drop(dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, earning, on="symbol")
        return factor_earning

    def roe_cut_weighted(self, ttm_earning, factor_earning):
        """
        :param ttm_earning:
        :param factor_earning:
        :return:
        """
        pass

    def roic(self, ttm_earning, factor_earning):
        """
        :param ttm_earning:
        :param factor_earning:
        :return:
        """
        pass

    def roa_ebit(self, ttm_earning, factor_earning):
        """
        :param ttm_earning:
        :param factor_earning:
        :return:
        """
        pass

    @staticmethod
    def roa_ebit_ttm(ttm_earning, factor_earning, dependencies=['total_profit', 'financial_expense', 'interest_income', 'total_assets']):
        """
        总资产报酬率
        ROAEBIT = EBIT*2/(期初总资产+期末总资产）
        (注，此处用过去四个季度资产均值）
        :param dependencies:
        :param ttm_earning:
        :param factor_earning:
        :return:
        """

        earning = ttm_earning.loc[:, dependencies]
        earning['roa_ebit_ttm'] = np.where(
            CalcTools.is_zero(earning.total_assets.values), 0,
            (earning.total_profit.values +
             earning.financial_expense.values -
             earning.interest_income.values)
            / earning.total_assets.values / 4)
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
    def invest_r_associates_to_tp_ttm(ttm_earning, factor_earning, dependencies=['invest_income_associates', 'total_profit']):
        """
        对联营和营公司投资收益/利润总额
        :param dependencies:
        :param ttm_earning:
        :param factor_earning:
        :return:
        """

        earning = ttm_earning.loc[:, dependencies]
        earning['invest_r_associates_to_tp_ttm'] = np.where(
            CalcTools.is_zero(earning.total_profit.values), 0,
            earning.invest_income_associates.values
            / earning.total_profit.values)
        earning = earning.drop(dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, earning, on="symbol")
        return factor_earning

    @staticmethod
    def invest_r_associates_to_tp_latest(tp_earning, factor_earning, dependencies=['invest_income_associates', 'total_profit']):
        """
        对联营和营公司投资收益/利润总额
        :param dependencies:
        :param tp_earning:
        :param factor_earning:
        :return:
        """

        earning = tp_earning.loc[:, dependencies]
        earning['invest_r_associates_to_tp_latest'] = np.where(
            CalcTools.is_zero(earning.total_profit.values), 0,
            earning.invest_income_associates.values
            / earning.total_profit.values)
        earning = earning.drop(dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, earning, on="symbol")
        return factor_earning

    @staticmethod
    def npcut_to_np(tp_earning, factor_earning, dependencies=['adjusted_profit', 'net_profit']):
        """
        扣除非经常损益后的净利润/净利润
        :param dependencies:
        :param tp_earning:
        :param factor_earning:
        :return:
        """

        earning = tp_earning.loc[:, dependencies]
        earning['npcut_to_np'] = np.where(
            CalcTools.is_zero(earning.net_profit.values), 0,
            earning.adjusted_profit.values
            / earning.net_profit.values)
        earning = earning.drop(dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, earning, on="symbol")
        return factor_earning

    @staticmethod
    def interest_cover_ttm(ttm_earning, factor_earning, dependencies=['total_profit', 'financial_expense', 'interest_income']):
        """
        利息保障倍数
        InterestCover=(TP + INT_EXP - INT_COME)/(INT_EXP - INT_COME)
        息税前利润/利息费用，息税前利润=利润总额+利息费用，利息费用=利息支出-利息收入
        :param dependencies:
        :param ttm_earning:
        :param factor_earning:
        :return:
        """

        earning = ttm_earning.loc[:, dependencies]
        earning['interest_cover_ttm'] = np.where(
            CalcTools.is_zero(earning.financial_expense.values - earning.interest_income.values), 0,
            (earning.total_profit.values + earning.financial_expense.values - earning.interest_income.values) /
            (earning.financial_expense.values - earning.interest_income.values))
        earning = earning.drop(dependencies, axis=1)
        factor_earning = pd.merge(factor_earning, earning, on="symbol")
        return factor_earning

    @staticmethod
    def degm(ttm_earning, ttm_earning_p1y, factor_earning, dependencies=['operating_revenue', 'operating_cost']):
        """
        毛利率增长，与去年同期相比
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


def calculate(trade_date, earning_sets_dic, earning):  # 计算对应因子
    print(trade_date)
    tp_earning = earning_sets_dic['tp_earning']
    ttm_earning = earning_sets_dic['ttm_earning']
    ttm_earning_5y = earning_sets_dic['ttm_earning_5y']

    # 因子计算
    factor_earning = pd.DataFrame()
    factor_earning['symbol'] = tp_earning.index

    factor_earning = earning.net_profit_ratio(ttm_earning, factor_earning)
    factor_earning = earning.operating_profit_ratio(ttm_earning, factor_earning)
    factor_earning = earning.np_to_tor(ttm_earning, factor_earning)
    factor_earning = earning.operating_profit_to_tor(ttm_earning, factor_earning)
    factor_earning = earning.gross_income_ratio(ttm_earning, factor_earning)
    factor_earning = earning.ebit_to_tor(ttm_earning, factor_earning)
    factor_earning = earning.roa(ttm_earning, factor_earning)
    factor_earning = earning.roa5(ttm_earning_5y, factor_earning)
    factor_earning = earning.roe(ttm_earning, factor_earning)
    factor_earning = earning.roe5(ttm_earning_5y, factor_earning)
    factor_earning = earning.roe_diluted(tp_earning, factor_earning)
    factor_earning = earning.roe_avg(ttm_earning, factor_earning)
    # factor_earning = self.roe_weighted()
    factor_earning = earning.roe_cut(tp_earning, factor_earning)
    # factor_earning = self.roe_cut_weighted()
    # factor_earning = self.roic()
    # factor_earning = self.roa_ebit()
    factor_earning = earning.roa_ebit_ttm(ttm_earning, factor_earning)
    factor_earning = earning.operating_ni_to_tp_ttm(ttm_earning, factor_earning)
    factor_earning = earning.operating_ni_to_tp_latest(tp_earning, factor_earning)
    factor_earning = earning.invest_r_associates_to_tp_ttm(ttm_earning, factor_earning)
    factor_earning = earning.invest_r_associates_to_tp_latest(tp_earning, factor_earning)
    factor_earning = earning.npcut_to_np(tp_earning, factor_earning)
    factor_earning = earning.interest_cover_ttm(ttm_earning, factor_earning)
    # factor_earning = self.degm(ttm_earning, ttm_earning_p1y, factor_earning)
    factor_earning = earning.net_non_oi_to_tp_ttm(ttm_earning, factor_earning)
    factor_earning = earning.net_non_oi_to_tp_latest(tp_earning, factor_earning)

    factor_earning['id'] = factor_earning['symbol'] + str(trade_date)
    factor_earning['trade_date'] = str(trade_date)
    earning._storage_data(factor_earning, trade_date)


@app.task()
def factor_calculate(**kwargs):
    print("constrain_kwargs: {}".format(kwargs))
    date_index = kwargs['date_index']
    session = kwargs['session']
    earning = FactorEarning('factor_earning')  # 注意, 这里的name要与client中新建table时的name一致, 不然回报错
    content1 = cache_data.get_cache(session + str(date_index) + "1", date_index)
    content2 = cache_data.get_cache(session + str(date_index) + "2", date_index)
    content3 = cache_data.get_cache(session + str(date_index) + "3", date_index)
    print("len_con1: %s" % len(content1))
    print("len_con2: %s" % len(content2))
    print("len_con3: %s" % len(content3))
    tp_earning = json_normalize(json.loads(str(content1, encoding='utf8')))
    ttm_earning_5y = json_normalize(json.loads(str(content2, encoding='utf8')))
    ttm_earning = json_normalize(json.loads(str(content3, encoding='utf8')))
    # cache_date.get_cache使得index的名字丢失， 所以数据需要按照下面的方式设置index
    tp_earning.set_index('symbol', inplace=True)
    ttm_earning.set_index('symbol', inplace=True)
    ttm_earning_5y.set_index('symbol', inplace=True)
    total_earning_data = {'tp_earning': tp_earning, 'ttm_earning_5y': ttm_earning_5y, 'ttm_earning': ttm_earning}
    calculate(date_index, total_earning_data, earning)

