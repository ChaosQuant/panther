#! /usr/bin/env python
# -*- coding: utf-8 -*-

"""
@version: 0.1
@author: li
@file: factor_valuation.py
@time: 2019-01-28 11:33
"""
import sys
sys.path.append("..")
import json
import math
import numpy as np
import pandas as pd
from pandas.io.json import json_normalize

from factor import app
from factor.factor_base import FactorBase
from factor.ttm_fundamental import *
from factor.utillities.calc_tools import CalcTools
from ultron.cluster.invoke.cache_data import cache_data


class Valuation(FactorBase):
    """
    估值
    """

    def __init__(self, name):
        super(Valuation, self).__init__(name)

    # 构建因子表
    def create_dest_tables(self):
        """
        创建数据库表
        :return:
        """
        drop_sql = """drop table if exists `{0}`""".format(self._name)

        create_sql = """create table `{0}`(
                    `id` varchar(32) NOT NULL,
                    `security_code` varchar(24) NOT NULL,
                    `trade_date` date NOT NULL,
                    `PSIndu` decimal(19,4) NOT NULL,
                    `EPTTM` decimal(19,4),
                    `PEIndu` decimal(19,4),
                    `PEG3YChgTTM` decimal(19,4),
                    `PEG5YChgTTM` decimal(19, 4),
                    `PBIndu` decimal(19,4),
                    `PCFIndu` decimal(19,4),
                    `CEToPTTM` decimal(19,4),
                    PRIMARY KEY(`id`,`trade_date`,`security_code`)
                    )ENGINE=InnoDB DEFAULT CHARSET=utf8;""".format(self._name)
        super(Valuation, self)._create_tables(create_sql, drop_sql)

    @staticmethod
    def lcap(tp_historical_value, factor_historical_value, dependencies=['market_cap']):
        """
        总市值的对数
        # 对数市值 即市值的对数
        :param dependencies:
        :param tp_historical_value:
        :param factor_historical_value:
        :return:
        """

        historical_value = tp_historical_value.loc[:, dependencies]
        historical_value['historical_value_lcap_latest'] = historical_value['market_cap'].map(lambda x: math.log(abs(x)))
        # historical_value = historical_value.drop(columns=['market_cap'], axis=1)
        # factor_historical_value = pd.merge(factor_historical_value, historical_value, on="security_code")
        factor_historical_value['historical_value_lcap_latest'] = historical_value['historical_value_lcap_latest']
        return factor_historical_value

    @staticmethod
    def lflo(tp_historical_value, factor_historical_value, dependencies=['circulating_market_cap']):
        """
        流通总市值的对数
        # 对数市值 即流通市值的对数
        :param dependencies:
        :param tp_historical_value:
        :param factor_historical_value:
        :return:
        """

        historical_value = tp_historical_value.loc[:, dependencies]

        historical_value['historical_value_lflo_latest'] = historical_value['circulating_market_cap'].map(lambda x: math.log(abs(x)))
        historical_value = historical_value.drop(columns=['circulating_market_cap'], axis=1)
        factor_historical_value = pd.merge(factor_historical_value, historical_value, on="security_code")
        return factor_historical_value

    @staticmethod
    def nlsize(tp_historical_value, factor_historical_value, dependencies=['historical_value_lcap_latest']):
        """
        对数市值开立方
        :param dependencies:
        :param tp_historical_value:
        :param factor_historical_value:
        :return:
        """
        # 对数市值
        historical_value = tp_historical_value.loc[:, dependencies]
        historical_value['historical_value_nlsize_latest'] = historical_value['historical_value_lcap_latest'].map(lambda x: pow(math.log(abs(x)), 1/3.0))
        historical_value = historical_value.drop(columns=['historical_value_lcap_latest'], axis=1)
        factor_historical_value = pd.merge(factor_historical_value, historical_value, on="security_code")
        return factor_historical_value

    @staticmethod
    def log_total_asset_mrq(tp_historical_value, factor_historical_value, dependencies=['total_assets']):
        """
        对数总资产MRQ
        :param tp_historical_value:
        :param factor_historical_value:
        :param dependencies:
        :return:
        """
        historical_value = tp_historical_value.loc[:, dependencies]

        historical_value['LogTotalAssets'] = historical_value['total_assets'].map(lambda x: math.log(abs(x)))
        # historical_value = historical_value.drop(columns=['total_operating_revenue'], axis=1)
        # factor_historical_value = pd.merge(factor_historical_value, historical_value, on="security_code")
        factor_historical_value['LogTotalAssets'] = historical_value['LogTotalAssets']
        return factor_historical_value

    @staticmethod
    def market_cap_to_corporate_free_cash_flow(tp_historical_value, factor_historical_value, dependencies=['market_cap', 'enterprise_fcfps']):
        """
        市值/企业自由现金流
        :param tp_historical_value:
        :param factor_historical_value:
        :param dependencies:
        :return:
        """
        historical_value = tp_historical_value.loc[:, dependencies]

        func = lambda x: x[0] / x[1] if x[1] is not None and x[1] != 0 else None

        historical_value['LogTotalAssets'] = historical_value['total_assets'].apply(func, axis=1)
        # historical_value = historical_value.drop(columns=['total_operating_revenue'], axis=1)
        # factor_historical_value = pd.merge(factor_historical_value, historical_value, on="security_code")
        factor_historical_value['LogTotalAssets'] = historical_value['LogTotalAssets']
        return factor_historical_value






    @staticmethod
    def log_sales_ttm(tp_historical_value, factor_historical_value, dependencies=['total_operating_revenue']):
        """
        对数营业收入(TTM)
        :param tp_historical_value:
        :param factor_historical_value:
        :param dependencies:
        :return:
        """
        historical_value = tp_historical_value.loc[:, dependencies]

        historical_value['LogSalesTTM'] = historical_value['total_operating_revenue'].map(lambda x: math.log(abs(x)))
        # historical_value = historical_value.drop(columns=['total_operating_revenue'], axis=1)
        # factor_historical_value = pd.merge(factor_historical_value, historical_value, on="security_code")
        factor_historical_value['LogSalesTTM'] = historical_value['LogSalesTTM']
        return factor_historical_value

    @staticmethod
    def pcf_to_operating_cash_flow_ttm(tp_historical_value, factor_historical_value, dependencies=['market_cap', 'net_operate_cash_flow']):
        """
        市现率PCF(经营现金流TTM)
        :return:
        """
        historical_value = tp_historical_value.loc[:, dependencies]
        func = lambda x: x[0] / x[1] if x[1] is not None and x[1] != 0 else None
        historical_value['PCFToOptCashflowTTM'] = historical_value[dependencies].apply(func, axis=1)
        factor_historical_value['PCFToOptCashflowTTM'] = historical_value['PCFToOptCashflowTTM']
        return factor_historical_value


    @staticmethod
    def ps_indu(tp_historical_value, factor_historical_value, dependencies=['ps', 'isecurity_code']):
        """
        PEIndu， 市销率，以及同行业所有的公司的市销率
        # (PS – PS 的行业均值)/PS 的行业标准差
        :param dependencies:
        :param tp_historical_value:
        :param factor_historical_value:
        :return:
        """
        # 行业均值，行业标准差

        historical_value = tp_historical_value.loc[:, dependencies]
        historical_value_grouped = historical_value.groupby('isecurity_code')
        historical_value_mean = historical_value_grouped.mean()
        historical_value_std = historical_value_grouped.std()
        historical_value_std = historical_value_std.rename(columns={"ps": "ps_std"}).reset_index()
        historical_value_mean = historical_value_mean.rename(columns={"ps": "ps_mean"}).reset_index()
        historical_value = historical_value.merge(historical_value_std, on='isecurity_code')
        historical_value = historical_value.merge(historical_value_mean, on='isecurity_code')

        historical_value['PSIndu'] = (historical_value['ps'] - historical_value['ps_mean']) / historical_value["ps_std"]
        # historical_value = historical_value.drop(columns=['ps', 'isecurity_code', 'ps_mean', 'ps_std'], axis=1)
        # factor_historical_value = pd.merge(factor_historical_value, historical_value, on="security_code")
        factor_historical_value['PSIndu'] = historical_value['PSIndu']
        return factor_historical_value

    @staticmethod
    def etop(tp_historical_value, factor_historical_value, dependencies=['net_profit', 'market_cap']):
        """
        收益市值比= 净利润TTM/总市值
        :param dependencies:
        :param tp_historical_value:
        :param factor_historical_value:
        :return:
        """

        historical_value = tp_historical_value.loc[:, dependencies]

        historical_value['EPTTM'] = np.where(CalcTools.is_zero(historical_value['market_cap']),
                                                   0,
                                                   historical_value['net_profit'] /
                                                   historical_value['market_cap'])

        # historical_value = historical_value.drop(columns=['net_profit', 'market_cap'], axis=1)
        # factor_historical_value = pd.merge(factor_historical_value, historical_value, on="security_code")

        factor_historical_value['EPTTM'] = historical_value['EPTTM']
        return factor_historical_value

    @staticmethod
    def pe_deduction_ttm(tp_historical_value, factor_historical_value, dependencies=['market_cap', 'net_profit_cut_pre']):
        """
        市盈率PE(TTM)（扣除）
        :param tp_historical_value:
        :param factor_historical_value:
        :param dependencies:
        :return:
        """
        historical_value = tp_historical_value.loc[:, dependencies]

        historical_value['PECutTTM'] = np.where(CalcTools.is_zero(historical_value['net_profit_cut_pre']), 0,
                                                historical_value['market_cap'] /
                                                historical_value['net_profit_cut_pre'])

        factor_historical_value['PECutTTM'] = historical_value['PECutTTM']
        return factor_historical_value

    @staticmethod
    def etp5(tp_historical_value, factor_historical_value, dependencies=['net_profit_5', 'circulating_market_cap_5', 'market_cap_5']):
        """
        5年平均收益市值比 = 近5年净利润 / 近5年总市值
        :param tp_historical_value:
        :param factor_historical_value:
        :param dependencies:
        :return:
        """
        historical_value = tp_historical_value.loc[:, dependencies]

        fun = lambda x: x[0] / x[1] if x[1] is not None and x[1] != 0 else (x[0] / x[2] if x[2] is not None and x[2] !=0 else None)

        historical_value['historical_value_etp5_ttm'] = historical_value[dependencies].apply(fun, axis=1)
        # historical_value = historical_value.drop(columns=['net_profit_5', 'circulating_market_cap_5', 'market_cap_5'], axis=1)
        # factor_historical_value = pd.merge(factor_historical_value, historical_value, on="security_code")
        factor_historical_value['historical_value_etp5_ttm'] = historical_value['historical_value_etp5_ttm']
        return factor_historical_value

    @staticmethod
    def ctop(tp_historical_value, factor_historical_value, dependencies=['pcd', 'sbd', 'circulating_market_cap', 'market_cap']):
        """
        现金流市值比 = 每股派现 * 分红前总股本/总市值
        :param tp_historical_value:
        :param factor_historical_value:
        :param dependencies:
        :return:
        """

        historical_value = tp_historical_value.loc[:, dependencies]

        fun = lambda x: x[0] * x[1] / x[2] if x[2] is not None and x[2] != 0 else (x[0] * x[1] / x[3] if x[3] is not None and x[3] != 0 else None)

        historical_value['historical_value_ctop_latest'] = historical_value[dependencies].apply(fun, axis=1)

        historical_value = historical_value.drop(columns=['pcd', 'sbd', 'circulating_market_cap', 'market_cap'], axis=1)
        factor_historical_value = pd.merge(factor_historical_value, historical_value, on="security_code")
        return factor_historical_value

    @staticmethod
    def ctop5(tp_historical_value, factor_historical_value, dependencies=['pcd', 'sbd', 'circulating_market_cap_5', 'market_cap_5']):
        """
        5 年平均现金流市值比  = 近5年每股派现 * 分红前总股本/近5年总市值
        :param tp_historical_value:
        :param factor_historical_value:
        :param dependencies:
        :return:
        """
        historical_value = tp_historical_value.loc[:, dependencies]

        fun = lambda x: x[0] * x[1] / x[2] if x[2] is not None and x[2] != 0 else (
            x[0] * x[1] / x[3] if x[3] is not None and x[3] != 0 else None)

        historical_value['historical_value_ctop5_latest'] = historical_value[dependencies].apply(fun, axis=1)

        historical_value = historical_value.drop(columns=['pcd', 'sbd', 'circulating_market_cap_5', 'market_cap_5'],
                                                 axis=1)
        factor_historical_value = pd.merge(factor_historical_value, historical_value, on="security_code")
        return factor_historical_value

    @staticmethod
    def pe_avg(tp_historical_value, factor_historical_value, dependencies=['pe', 'issymbol']):
        historical_value = tp_historical_value.loc[:, dependencies]
        historical_value_grouped = historical_value.groupby('issymbol')
        historical_value_mean = historical_value_grouped.mean()
        pass

    @staticmethod
    def pe_std():
        pass

    @staticmethod
    def pb_avg():
        pass

    @staticmethod
    def pb_std():
        pass

    @staticmethod
    def pc_avg():
        pass

    @staticmethod
    def pc_std():
        pass


    @staticmethod
    def pe_indu(tp_historical_value, factor_historical_value, dependencies=['pe', 'issymbol']):
        """
        (PE – PE 的行业均值)/PE 的行业标准差
        :param dependencies:
        :param tp_historical_value:
        :param factor_historical_value:
        :return:
        """

        historical_value = tp_historical_value.loc[:, dependencies]
        historical_value_grouped = historical_value.groupby('issymbol')
        historical_value_mean = historical_value_grouped.mean()
        historical_value_std = historical_value_grouped.std()
        historical_value_std = historical_value_std.rename(columns={"pe": "pe_std"}).reset_index()
        historical_value_mean = historical_value_mean.rename(columns={"pe": "pe_mean"}).reset_index()
        historical_value = historical_value.merge(historical_value_std, on='isecurity_code')
        historical_value = historical_value.merge(historical_value_mean, on='isecurity_code')
        historical_value['PEIndu'] = (historical_value['pe'] - historical_value['pe_mean']) / historical_value["pe_std"]
        # historical_value = historical_value.drop(columns=['pe', 'isecurity_code', 'pe_mean', 'pe_std'], axis=1)
        # factor_historical_value = pd.merge(factor_historical_value, historical_value, on="security_code")
        factor_historical_value['PEIndu'] = historical_value['PEIndu']
        return factor_historical_value

    @staticmethod
    def total_assets_to_enterprise(tp_historical_value, factor_historical_value, dependencies=['total_assets',
                                                                                               'shortterm_loan',
                                                                                               'longterm_loan',
                                                                                               'market_cap',
                                                                                               'cash_equivalent_increase',
                                                                                                ]):
        """
        资产总计/企业价值 MRQ
        :param tp_historical_value:
        :param factor_historical_value:
        :param dependencies:
        :return:
        """
        historical_value = tp_historical_value.loc[:, dependencies]
        fuc = lambda x: x[1] + x[2] + x[3] - x[4]

        historical_value['temp'] = historical_value[dependencies].apply(fuc, axis=1)

        historical_value['TotalAssetsToEnterpriseValue'] = np.where(CalcTools.is_zero(historical_value['temp']), 0,
                                                                    historical_value['total_assets'] /
                                                                    historical_value['temp'])

        factor_historical_value['TotalAssetsToEnterpriseValue'] = historical_value['TotalAssetsToEnterpriseValue']
        return factor_historical_value

    @staticmethod
    def peg_3y(tp_historical_value, factor_historical_value, dependencies=['pe', 'np_parent_company_owners', 'np_parent_company_owners_3']):
        """
        # 市盈率/归属于母公司所有者净利润 3 年复合增长率
        :param dependencies:
        :param tp_historical_value:
        :param factor_historical_value:
        :return:
        """

        historical_value = tp_historical_value.loc[:, dependencies]

        tmp = np.where(CalcTools.is_zero(historical_value['np_parent_company_owners_3']), 0,
                       (historical_value['np_parent_company_owners'] / historical_value['np_parent_company_owners_3']))
        historical_value['PEG3YChgTTM'] = tmp / abs(tmp) * pow(abs(tmp), 1 / 3.0) - 1

        historical_value = historical_value.drop(
            columns=['pe', 'np_parent_company_owners', 'np_parent_company_owners_3'], axis=1)
        factor_historical_value = pd.merge(factor_historical_value, historical_value, on="security_code")
        return factor_historical_value

    @staticmethod
    def peg_5y(tp_historical_value, factor_historical_value, dependencies=['pe', 'np_parent_company_owners', 'np_parent_company_owners_5']):
        """
        # 市盈率/归属于母公司所有者净利润 5 年复合增长率
        :param dependencies:
        :param tp_historical_value:
        :param factor_historical_value:
        :return:
        """

        historical_value = tp_historical_value.loc[:, dependencies]

        tmp = np.where(CalcTools.is_zero(historical_value['np_parent_company_owners_5']), 0,
                       (historical_value['np_parent_company_owners'] / historical_value['np_parent_company_owners_5']))
        historical_value['PEG5YChgTTM'] = tmp / abs(tmp) * pow(abs(tmp), 1 / 5.0) - 1

        # historical_value = historical_value.drop(
        #     columns=['pe', 'np_parent_company_owners', 'np_parent_company_owners_5'], axis=1)
        # factor_historical_value = pd.merge(factor_historical_value, historical_value, on="security_code")
        factor_historical_value['PEG5YChgTTM'] = historical_value['PEG5YChgTTM']
        return factor_historical_value

    @staticmethod
    def pb_indu(tp_historical_value, factor_historical_value, dependencies=['pb', 'isecurity_code']):
        """
        # (PB – PB 的行业均值)/PB 的行业标准差
        :param dependencies:
        :param tp_historical_value:
        :param factor_historical_value:
        :return:
        """

        # 行业均值, 行业标准差
        historical_value = tp_historical_value.loc[:, dependencies]
        historical_value_grouped = historical_value.groupby('isecurity_code')
        historical_value_mean = historical_value_grouped.mean()
        historical_value_std = historical_value_grouped.std()
        historical_value_std = historical_value_std.rename(columns={"pb": "pb_std"}).reset_index()
        historical_value_mean = historical_value_mean.rename(columns={"pb": "pb_mean"}).reset_index()
        historical_value = historical_value.merge(historical_value_std, on='isecurity_code')
        historical_value = historical_value.merge(historical_value_mean, on='isecurity_code')
        historical_value['PBIndu'] = (historical_value['pb'] - historical_value['pb_mean']) / historical_value["pb_std"]
        # historical_value = historical_value.drop(columns=['pb', 'isecurity_code', 'pb_mean', 'pb_std'], axis=1)
        # factor_historical_value = pd.merge(factor_historical_value, historical_value, on="security_code")
        factor_historical_value['PBIndu'] = historical_value['PBIndu']
        return factor_historical_value

    @staticmethod
    def pcf_indu(tp_historical_value, factor_historical_value, dependencies=['pcf', 'isecurity_code']):
        """
        # (PCF – PCF 的行业均值)/PCF 的行业标准差
        :param dependencies:
        :param tp_historical_value:
        :param factor_historical_value:
        :return:
        """

        # 行业均值, 行业标准差
        historical_value = tp_historical_value.loc[:, dependencies]
        historical_value_grouped = historical_value.groupby('isecurity_code')
        historical_value_mean = historical_value_grouped.mean()
        historical_value_std = historical_value_grouped.std()
        historical_value_std = historical_value_std.rename(columns={"pcf": "pcf_std"}).reset_index()
        historical_value_mean = historical_value_mean.rename(columns={"pcf": "pcf_mean"}).reset_index()
        historical_value = historical_value.merge(historical_value_std, on='isecurity_code')
        historical_value = historical_value.merge(historical_value_mean, on='isecurity_code')

        historical_value['PCFIndu'] = (historical_value['pcf'] - historical_value['pcf_mean']) / historical_value[
            "pcf_std"]
        # historical_value = historical_value.drop(columns=['pcf', 'isecurity_code', 'pcf_mean', 'pcf_std'], axis=1)
        # factor_historical_value = pd.merge(factor_historical_value, historical_value, on="security_code")
        factor_historical_value['PCFIndu'] = historical_value['PCFIndu']
        return factor_historical_value

    @staticmethod
    def cetop(tp_historical_value, factor_historical_value, dependencies=['net_operate_cash_flow', 'market_cap']):
        """
        现金收益滚动收益与市值比
        经营活动产生的现金流量净额与市值比
        :param dependencies:
        :param tp_historical_value:
        :param factor_historical_value:
        :return:
        """

        historical_value = tp_historical_value.loc[:, dependencies]

        historical_value['CEToPTTM'] = np.where(CalcTools.is_zero(historical_value['market_cap']), 0,
                                                historical_value['net_operate_cash_flow'] /
                                                historical_value['market_cap'])

        historical_value = historical_value.drop(columns=['net_operate_cash_flow', 'market_cap'], axis=1)
        factor_historical_value = pd.merge(factor_historical_value, historical_value, on="security_code")

        return factor_historical_value

    @staticmethod
    def revenue_to_market_ratio_ttm(tp_historical_value, factor_historical_value, dependencies=['operating_revenue',
                                                                                                'market_cap']):
        """
        营收市值比(TTM)
        营业收入（TTM）/总市值
        :param tp_historical_value:
        :param factor_historical_value:
        :param dependencies:
        :return:
        """
        historical_value = tp_historical_value.loc[:, dependencies]

        historical_value['RevToMrktRatioTTM'] = np.where(CalcTools.is_zero(historical_value['market_cap']), 0,
                                                historical_value['operating_revenue'] /
                                                historical_value['market_cap'])

        factor_historical_value['RevToMrktRatioTTM'] = historical_value['RevToMrktRatioTTM']
        return factor_historical_value

    @staticmethod
    def operating_to_enterprise_ttm(tp_historical_value, factor_historical_value, dependencies=['operating_revenue',
                                                                                                'shortterm_loan',
                                                                                                'longterm_loan',
                                                                                                'market_cap',
                                                                                                'cash_equivalent_increase',
                                                                                                ]):
        """
        营业收入(TTM)/企业价值
        企业价值= 长期借款+ 短期借款+ 总市值- 现金及现金等价物
        :param tp_historical_value:
        :param factor_historical_value:
        :param dependencies:
        :return:
        """
        historical_value = tp_historical_value.loc[:, dependencies]

        fuc = lambda x: x[1] + x[2] + x[3] - x[4]

        historical_value['temp'] = historical_value[dependencies].apply(fuc, axis=1)

        historical_value['OptIncToEnterpriseValueTTM'] = np.where(CalcTools.is_zero(historical_value['temp']), 0,
                                                                  historical_value['operating_revenue'] /
                                                                  historical_value['temp'])

        factor_historical_value['OptIncToEnterpriseValueTTM'] = historical_value['OptIncToEnterpriseValueTTM']
        return factor_historical_value



def calculate(trade_date, valuation_sets):
    """
    :param valuation_sets:
    :param trade_date:
    :return:
    """
    valuation_sets = valuation_sets.set_index('security_code')
    historical_value = Valuation('factor_historical_value')

    factor_historical_value = pd.DataFrame()
    factor_historical_value['security_code'] = valuation_sets.index
    factor_historical_value = factor_historical_value.set_index('security_code')

    # psindu
    factor_historical_value = historical_value.ps_indu(valuation_sets, factor_historical_value)
    factor_historical_value = historical_value.etop(valuation_sets, factor_historical_value)
    # factor_historical_value = historical_value.etp5(valuation_sets, factor_historical_value)
    factor_historical_value = historical_value.pe_indu(valuation_sets, factor_historical_value)
    factor_historical_value = historical_value.peg_3y(valuation_sets, factor_historical_value)
    factor_historical_value = historical_value.peg_5y(valuation_sets, factor_historical_value)
    factor_historical_value = historical_value.pb_indu(valuation_sets, factor_historical_value)
    # factor_historical_value = historical_value.lcap(valuation_sets, factor_historical_value)
    # factor_historical_value = historical_value.lflo(factor_historical_value, factor_historical_value)
    # factor_historical_value = historical_value.nlsize(factor_historical_value, factor_historical_value)
    factor_historical_value = historical_value.pcf_indu(valuation_sets, factor_historical_value)
    factor_historical_value = historical_value.cetop(factor_historical_value, factor_historical_value)
    # factor_historical_value = historical_value.ctop(valuation_sets, factor_historical_value)
    # factor_historical_value = historical_value.ctop5(valuation_sets, factor_historical_value)

    # etp5 因子没有提出， 使用该部分的时候， 数据库字段需要添加
    # factor_historical_value = factor_historical_value[['security_code', 'PSIndu',
    #                                                    'historical_value_etp5_ttm',
    #                                                    'EPTTM',
    #                                                    'PEIndu', 'PEG3YChgTTM',
    #                                                    'PEG5YChgTTM', 'PBIndu',
    #                                                    'historical_value_lcap_latest',
    #                                                    'historical_value_lflo_latest',
    #                                                    'historical_value_nlsize_latest',
    #                                                    'PCFIndu',
    #                                                    'CEToPTTM',
    #                                                    'historical_value_ctop_latest',
    #                                                    'historical_value_ctop5_latest']]
    factor_historical_value = factor_historical_value[['security_code',
                                                       'PSIndu',
                                                       'EPTTM',
                                                       'PEIndu',
                                                       'PEG3YChgTTM',
                                                       'PEG5YChgTTM',
                                                       'PBIndu',
                                                       'PCFIndu',
                                                       'CEToPTTM',
                                                       ]]

    factor_historical_value['id'] = factor_historical_value['security_code'] + str(trade_date)
    factor_historical_value['trade_date'] = str(trade_date)
    # historical_value._storage_data(factor_historical_value, trade_date)


@app.task()
def factor_calculate(**kwargs):
    print("history_value_kwargs: {}".format(kwargs))
    date_index = kwargs['date_index']
    session = kwargs['session']
    # historical_value = Valuation('factor_historical_value')  # 注意, 这里的name要与client中新建table时的name一致, 不然回报错
    content = cache_data.get_cache(session + str(date_index), date_index)
    total_history_data = json_normalize(json.loads(str(content, encoding='utf8')))
    print("len_history_value_data {}".format(len(total_history_data)))
    calculate(date_index, total_history_data)

