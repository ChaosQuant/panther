#!/usr/bin/env python
# coding=utf-8


import json
from factor import app
import numpy as np
import pandas as pd
from pandas.io.json import json_normalize

from factor.factor_base import FactorBase
from factor.utillities.calc_tools import CalcTools

from ultron.cluster.invoke.cache_data import cache_data


class FactorCashFlow(FactorBase):
    """
    收益质量
        --现金流
    """
    def __init__(self, name):
        super(FactorCashFlow, self).__init__(name)

    # 构建因子表
    def create_dest_tables(self):
        drop_sql = """drop table if exists `{0}`""".format(self._name)
        create_sql = """create table `{0}`(
                    `id` varchar(32) NOT NULL,
                    `symbol` varchar(24) NOT NULL,
                    `trade_date` date NOT NULL,
                    `OptCFToLiabilityTTM` decimal(19,4),
                    `OptCFToIBDTTM` decimal(19,4),
                    `OptCFToNetDebtTTM` decimal(19,4),
                    `SaleServCashToOptReTTM` decimal(19,4),
                    `OptCFToRevTTM` decimal(19,4),
                    `OptCFToNetIncomeTTM` decimal(19,4),
                    `OptCFToCurrLiabilityTTM` decimal(19,4),
                    `CashRatioTTM` decimal(19,4),
                    `OptToEnterpriseTTM` decimal(19,4),
                    `OptOnReToAssetTTM` decimal(19,4),
                    `NetProCashCoverTTM` decimal(19,4),
                    `OptToAssertTTM` decimal(19,4),
                    `SalesServCashToOR` decimal(19,4),
                    `CashOfSales` decimal(19,4),
                    `NOCFToOpt` decimal(19,4),
                    PRIMARY KEY(`id`,`trade_date`,`symbol`)
                    )ENGINE=InnoDB DEFAULT CHARSET=utf8;""".format(self._name)
        super(FactorCashFlow, self)._create_tables(create_sql, drop_sql)

    # 经营活动净现金流（TTM）/负债（TTM）
    @staticmethod
    def nocf_to_t_liability_ttm(ttm_cash_flow, factor_cash_flow):
        dependencies = ['net_operate_cash_flow', 'total_liability']
        cash_flow = ttm_cash_flow.loc[:, dependencies]
        cash_flow['OptCFToLiabilityTTM'] = np.where(
            CalcTools.is_zero(cash_flow.total_liability.values), 0,
            cash_flow.net_operate_cash_flow.values / cash_flow.total_liability.values)
        cash_flow = cash_flow.drop(dependencies, axis=1)
        factor_cash_flow = pd.merge(factor_cash_flow, cash_flow, on="symbol")
        return factor_cash_flow

    # 经营活动净现金流（TTM）/带息负债（TTM）
    @staticmethod
    def nocf_to_interest_bear_debt_ttm(ttm_cash_flow, factor_cash_flow, dependencies=['net_operate_cash_flow', 'total_liability', 'interest_bearing_liability']):
        cash_flow = ttm_cash_flow.loc[:, dependencies]
        cash_flow['OptCFToIBDTTM'] = np.where(
            CalcTools.is_zero(cash_flow.interest_bearing_liability.values), 0,
            cash_flow.net_operate_cash_flow.values / cash_flow.interest_bearing_liability.values)
        cash_flow = cash_flow.drop(dependencies, axis=1)
        factor_cash_flow = pd.merge(factor_cash_flow, cash_flow, on="symbol")
        return factor_cash_flow

    # 经营活动净现金流（TTM）/净负债（TTM）
    @staticmethod
    def nocf_to_net_debt_ttm(ttm_cash_flow, factor_cash_flow, dependencies=['net_operate_cash_flow', 'net_liability']):

        cash_flow = ttm_cash_flow.loc[:, dependencies]
        cash_flow['OptCFToNetDebtTTM'] = np.where(CalcTools.is_zero(cash_flow.net_liability.values), 0,
                                                     cash_flow.net_operate_cash_flow.values / cash_flow.net_liability.values)
        cash_flow = cash_flow.drop(dependencies, axis=1)
        factor_cash_flow = pd.merge(factor_cash_flow, cash_flow, on="symbol")
        return factor_cash_flow

    # 销售商品和提供劳务收到的现金（TTM）/营业收入（TTM）
    @staticmethod
    def sale_service_cash_to_or_ttm(ttm_cash_flow, factor_cash_flow, dependencies=['goods_sale_and_service_render_cash', 'operating_revenue']):

        cash_flow = ttm_cash_flow.loc[:, dependencies]
        cash_flow['SaleServCashToOptReTTM'] = np.where(
            CalcTools.is_zero(cash_flow.operating_revenue.values), 0,
            cash_flow.goods_sale_and_service_render_cash.values / cash_flow.operating_revenue.values)
        cash_flow = cash_flow.drop(dependencies, axis=1)
        factor_cash_flow = pd.merge(factor_cash_flow, cash_flow, on="symbol")
        return factor_cash_flow

    # 经营活动产生的现金流量净额（TTM）/营业收入（TTM）
    @staticmethod
    def cash_rate_of_sales_ttm(ttm_cash_flow, factor_cash_flow, dependencies=['net_operate_cash_flow', 'operating_revenue']):

        cash_flow = ttm_cash_flow.loc[:, dependencies]
        cash_flow['OptCFToRevTTM'] = np.where(
            CalcTools.is_zero(cash_flow.operating_revenue.values), 0,
            cash_flow.net_operate_cash_flow.values / cash_flow.operating_revenue.values)
        cash_flow = cash_flow.drop(dependencies, axis=1)
        factor_cash_flow = pd.merge(factor_cash_flow, cash_flow, on="symbol")
        return factor_cash_flow

    # 经营活动产生的现金流量净额（TTM）/(营业总收入（TTM）-营业总成本（TTM）)
    @staticmethod
    def nocf_to_operating_ni_ttm(ttm_cash_flow, factor_cash_flow, dependencies=['net_operate_cash_flow', 'total_operating_revenue', 'total_operating_cost']):

        cash_flow = ttm_cash_flow.loc[:, dependencies]
        cash_flow['OptCFToNetIncomeTTM'] = np.where(
            CalcTools.is_zero(cash_flow.total_operating_revenue.values - cash_flow.total_operating_cost.values),
            0, cash_flow.net_operate_cash_flow.values / (
                    cash_flow.total_operating_revenue.values - cash_flow.total_operating_cost.values))
        cash_flow = cash_flow.drop(dependencies, axis=1)
        factor_cash_flow = pd.merge(factor_cash_flow, cash_flow, on="symbol")
        return factor_cash_flow

    # 经营活动产生的现金流量净额（TTM）/流动负债（TTM）
    @staticmethod
    def oper_cash_in_to_current_liability_ttm(ttm_cash_flow, factor_cash_flow, dependencies=['net_operate_cash_flow', 'total_current_liability']):

        cash_flow = ttm_cash_flow.loc[:, dependencies]
        cash_flow['OptCFToCurrLiabilityTTM'] = np.where(
            CalcTools.is_zero(cash_flow.total_current_liability.values), 0,
            cash_flow.net_operate_cash_flow.values / cash_flow.total_current_liability.values)
        cash_flow = cash_flow.drop(dependencies, axis=1)
        factor_cash_flow = pd.merge(factor_cash_flow, cash_flow, on="symbol")
        return factor_cash_flow

    # 期末现金及现金等价物余额（TTM）/流动负债（TTM）
    @staticmethod
    def cash_to_current_liability_ttm(ttm_cash_flow, factor_cash_flow, dependencies=['cash_and_equivalents_at_end', 'total_current_assets']):

        cash_flow = ttm_cash_flow.loc[:, dependencies]
        cash_flow['CashRatioTTM'] = np.where(CalcTools.is_zero(cash_flow.total_current_assets.values),
                                                              0,
                                                              cash_flow.cash_and_equivalents_at_end.values / cash_flow.total_current_assets.values)
        cash_flow = cash_flow.drop(dependencies, axis=1)
        factor_cash_flow = pd.merge(factor_cash_flow, cash_flow, on="symbol")
        return factor_cash_flow

    # 经营活动产生的现金流量净额（TTM）/（长期借款（TTM）+ 短期借款（TTM）+ 总市值 - 期末现金及现金等价物（TTM）
    @staticmethod
    def cfo_to_ev_ttm(ttm_cash_flow, factor_cash_flow, dependencies=['net_operate_cash_flow', 'longterm_loan', 'shortterm_loan', 'market_cap', 'cash_and_equivalents_at_end']):

        cash_flow = ttm_cash_flow.loc[:, dependencies]
        cash_flow['OptToEnterpriseTTM'] = np.where(CalcTools.is_zero(
            cash_flow.longterm_loan.values + cash_flow.shortterm_loan.values + \
            cash_flow.market_cap.values - cash_flow.cash_and_equivalents_at_end.values), 0,
            cash_flow.net_operate_cash_flow.values / (cash_flow.longterm_loan.values + cash_flow.shortterm_loan.values + \
                                                      cash_flow.market_cap.values - cash_flow.cash_and_equivalents_at_end.values))
        cash_flow = cash_flow.drop(dependencies, axis=1)
        factor_cash_flow = pd.merge(factor_cash_flow, cash_flow, on="symbol")
        return factor_cash_flow

    # （经营活动产生的金流量净额（TTM） - 净利润（TTM）） /总资产（TTM）
    @staticmethod
    def acca_ttm(ttm_cash_flow, factor_cash_flow, dependencies=['net_operate_cash_flow', 'net_profit', 'total_assets']):

        cash_flow = ttm_cash_flow.loc[:, dependencies]
        cash_flow['OptOnReToAssetTTM'] = np.where(CalcTools.is_zero(cash_flow.total_assets.values), 0,
                                         (cash_flow.net_operate_cash_flow.values - cash_flow.net_profit.values) / (
                                             cash_flow.total_assets.values))
        cash_flow = cash_flow.drop(dependencies, axis=1)
        factor_cash_flow = pd.merge(factor_cash_flow, cash_flow, on="symbol")
        return factor_cash_flow

    # 经营活动产生的现金流量净额（TTM）/归属于母公司所有者的净利润（TTM）
    @staticmethod
    def net_profit_cash_cover_ttm(ttm_cash_flow, factor_cash_flow, dependencies=['net_operate_cash_flow', 'np_parent_company_owners']):

        cash_flow = ttm_cash_flow.loc[:, dependencies]
        cash_flow['NetProCashCoverTTM'] = np.where(
            CalcTools.is_zero(cash_flow.np_parent_company_owners.values), 0,
            cash_flow.net_operate_cash_flow.values / cash_flow.np_parent_company_owners.values)
        cash_flow = cash_flow.drop(dependencies, axis=1)
        factor_cash_flow = pd.merge(factor_cash_flow, cash_flow, on="symbol")
        return factor_cash_flow

    # 经营活动产生的现金流量净额（TTM）/总资产（TTM）
    @staticmethod
    def oper_cash_in_to_asset_ttm(ttm_cash_flow, factor_cash_flow, dependencies=['net_operate_cash_flow', 'total_assets']):
        cash_flow = ttm_cash_flow.loc[:, dependencies]
        cash_flow['OptToAssertTTM'] = np.where(CalcTools.is_zero(cash_flow.total_assets.values),
                                               0,
                                               cash_flow.net_operate_cash_flow.values / cash_flow.total_assets.values)
        cash_flow = cash_flow.drop(dependencies, axis=1)
        factor_cash_flow = pd.merge(factor_cash_flow, cash_flow, on="symbol")
        return factor_cash_flow

    # 销售商品和提供劳务收到的现金（Latest）/营业收入（Latest）
    @staticmethod
    def sales_service_cash_to_or_latest(tp_cash_flow, factor_cash_flow, dependencies=['goods_sale_and_service_render_cash', 'operating_revenue']):

        cash_flow = tp_cash_flow.loc[:, dependencies]
        cash_flow['SalesServCashToOR'] = np.where(CalcTools.is_zero(cash_flow.operating_revenue.values),
                                                  0,
                                                  cash_flow.goods_sale_and_service_render_cash.values / cash_flow.operating_revenue.values)
        cash_flow = cash_flow.drop(dependencies, axis=1)
        factor_cash_flow = pd.merge(factor_cash_flow, cash_flow, on="symbol")
        return factor_cash_flow

    # 经验活动产生的现金流量净额 / 营业收入
    @staticmethod
    def cash_rate_of_sales_latest(tp_cash_flow, factor_cash_flow, dependencies=['net_operate_cash_flow', 'operating_revenue']):

        cash_flow = tp_cash_flow.loc[:, dependencies]
        cash_flow['CashOfSales'] = np.where(CalcTools.is_zero(cash_flow.operating_revenue.values),
                                                          0,
                                                          cash_flow.net_operate_cash_flow.values / cash_flow.operating_revenue.values)
        cash_flow = cash_flow.drop(dependencies, axis=1)
        factor_cash_flow = pd.merge(factor_cash_flow, cash_flow, on="symbol")
        return factor_cash_flow

    # 经营活动产生的现金流量净额（Latest）/(营业总收入（Latest）-营业总成本（Latest）)
    @staticmethod
    def nocf_to_operating_ni_latest(tp_cash_flow, factor_cash_flow, dependencies=['net_operate_cash_flow', 'total_operating_revenue', 'total_operating_cost']):

        cash_flow = tp_cash_flow.loc[:, dependencies]
        cash_flow['NOCFToOpt'] = np.where(
            CalcTools.is_zero((cash_flow.total_operating_revenue.values - cash_flow.total_operating_cost.values)), 0,
            cash_flow.net_operate_cash_flow.values / (
                    cash_flow.total_operating_revenue.values - cash_flow.total_operating_cost.values))
        cash_flow = cash_flow.drop(dependencies, axis=1)
        factor_cash_flow = pd.merge(factor_cash_flow, cash_flow, on="symbol")
        return factor_cash_flow


def calculate(trade_date, cash_flow_dic, cash_flow):  # 计算对应因子
    print(trade_date)
    # 读取目前涉及到的因子
    tp_cash_flow = cash_flow_dic['tp_cash_flow']
    ttm_factor_sets = cash_flow_dic['ttm_factor_sets']

    factor_cash_flow = pd.DataFrame()
    factor_cash_flow['symbol'] = tp_cash_flow.index

    # 非TTM计算
    factor_cash_flow = cash_flow.nocf_to_operating_ni_latest(tp_cash_flow, factor_cash_flow)
    factor_cash_flow = cash_flow.cash_rate_of_sales_latest(tp_cash_flow, factor_cash_flow)
    factor_cash_flow = cash_flow.sales_service_cash_to_or_latest(tp_cash_flow, factor_cash_flow)

    # TTM计算
    factor_cash_flow = cash_flow.nocf_to_t_liability_ttm(ttm_factor_sets, factor_cash_flow)
    factor_cash_flow = cash_flow.nocf_to_interest_bear_debt_ttm(ttm_factor_sets, factor_cash_flow)
    factor_cash_flow = cash_flow.nocf_to_net_debt_ttm(ttm_factor_sets, factor_cash_flow)
    factor_cash_flow = cash_flow.sale_service_cash_to_or_ttm(ttm_factor_sets, factor_cash_flow)
    factor_cash_flow = cash_flow.cash_rate_of_sales_ttm(ttm_factor_sets, factor_cash_flow)
    factor_cash_flow = cash_flow.nocf_to_operating_ni_ttm(ttm_factor_sets, factor_cash_flow)
    factor_cash_flow = cash_flow.oper_cash_in_to_current_liability_ttm(ttm_factor_sets, factor_cash_flow)
    factor_cash_flow = cash_flow.cash_to_current_liability_ttm(ttm_factor_sets, factor_cash_flow)
    factor_cash_flow = cash_flow.cfo_to_ev_ttm(ttm_factor_sets, factor_cash_flow)
    factor_cash_flow = cash_flow.acca_ttm(ttm_factor_sets, factor_cash_flow)
    factor_cash_flow = cash_flow.net_profit_cash_cover_ttm(ttm_factor_sets, factor_cash_flow)
    factor_cash_flow = cash_flow.oper_cash_in_to_asset_ttm(ttm_factor_sets, factor_cash_flow)
    factor_cash_flow['id'] = factor_cash_flow['symbol'] + str(trade_date)
    factor_cash_flow['trade_date'] = str(trade_date)
    cash_flow._storage_data(factor_cash_flow, trade_date)


@app.task()
def factor_calculate(**kwargs):
    print("cash_flow_kwargs: {}".format(kwargs))
    date_index = kwargs['date_index']
    session = kwargs['session']
    cash_flow = FactorCashFlow('factor_cash_flow')  # 注意, 这里的name要与client中新建table时的name一致, 不然回报错
    content1 = cache_data.get_cache(session + str(date_index) + "1", date_index)
    content2 = cache_data.get_cache(session + str(date_index) + "2", date_index)
    tp_cash_flow = json_normalize(json.loads(str(content1, encoding='utf8')))
    ttm_factor_sets = json_normalize(json.loads(str(content2, encoding='utf8')))
    tp_cash_flow.set_index('symbol', inplace=True)
    ttm_factor_sets.set_index('symbol', inplace=True)
    print("len_tp_cash_flow_data {}".format(len(tp_cash_flow)))
    print("len_ttm_cash_flow_data {}".format(len(ttm_factor_sets)))
    total_cash_flow_data = {'tp_cash_flow': tp_cash_flow, 'ttm_factor_sets': ttm_factor_sets}
    calculate(date_index, total_cash_flow_data, cash_flow)
