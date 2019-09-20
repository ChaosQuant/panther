#! /usr/bin/env python
# -*- coding: utf-8 -*-

"""
@version: 0.1
@author: li
@file: factor_valuation.py
@time: 2019-01-28 11:33
"""
import sys, six
import gc
sys.path.append("..")
import json
import math
import numpy as np
import pandas as pd
from pandas.io.json import json_normalize
from basic_derivation.factor_base import FactorBase
from utilities.calc_tools import CalcTools
from utilities.singleton import Singleton

# from basic_derivation import app
# from ultron.cluster.invoke.cache_data import cache_data
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


@six.add_metaclass(Singleton)
class ValuationEstimation(object):
    """
    估值
    """
    def __init__(self):
        __str__ = 'factor_valuation'
        self.name = '估值类'
        self.factor_type1 = '估值类'
        self.factor_type2 = '估值类'
        self.desciption = '估值类因子'

    @staticmethod
    def LogofMktValue(valuation_sets, factor_historical_value, dependencies=['market_cap']):
        """
        总市值的对数
        # 对数市值 即市值的对数
        :param dependencies:
        :param valuation_sets:
        :param factor_historical_value:
        :return: 总市值的对数
        """
        historical_value = valuation_sets.loc[:, dependencies]
        func = lambda x: math.log(abs(x[0])) if x[0] is not None and x[0] != 0 else None

        historical_value['LogofMktValue'] = historical_value[dependencies].apply(func, axis=1)

        historical_value = historical_value.drop(columns=dependencies, axis=1)
        factor_historical_value = pd.merge(factor_historical_value, historical_value, how='outer', on="security_code")
        return factor_historical_value

    @staticmethod
    def LogofNegMktValue(valuation_sets, factor_historical_value, dependencies=['circulating_market_cap']):
        """
        流通总市值的对数
        # 对数市值 即流通市值的对数
        :param dependencies:
        :param valuation_sets:
        :param factor_historical_value:
        :return:流通总市值的对数
        """
        historical_value = valuation_sets.loc[:, dependencies]
        func = lambda x: math.log(abs(x[0])) if x[0] is not None and x[0] != 0 else None

        historical_value['LogofNegMktValue'] = historical_value[dependencies].apply(func, axis=1)

        historical_value = historical_value.drop(columns=dependencies, axis=1)
        factor_historical_value = pd.merge(factor_historical_value, historical_value, how='outer', on="security_code")
        return factor_historical_value

    @staticmethod
    def NLSIZE(valuation_sets, factor_historical_value, dependencies=['market_cap']):
        """
        对数市值开立方
        :param dependencies:
        :param valuation_sets:
        :param factor_historical_value:
        :return:
        """
        # 对数市值
        historical_value = valuation_sets.loc[:, dependencies]
        func = lambda x: pow(math.log(abs(x[0])), 1/3.0) if x[0] is not None and x[0] != 0 else None
        historical_value['NLSIZE'] = historical_value[dependencies].apply(func, axis=1)

        historical_value = historical_value.drop(columns=dependencies, axis=1)
        factor_historical_value = pd.merge(factor_historical_value, historical_value, how='outer', on="security_code")
        return factor_historical_value

    @staticmethod
    def MrktCapToCorFreeCashFlow(valuation_sets, factor_historical_value, dependencies=['market_cap', 'enterprise_fcfps']):
        """
        市值/企业自由现金流
        :param valuation_sets:
        :param factor_historical_value:
        :param dependencies:
        :return:
        """
        historical_value = valuation_sets.loc[:, dependencies]
        func = lambda x: x[0] / x[1] if x[1] is not None and x[1] != 0 else None

        historical_value['MrktCapToCorFreeCashFlow'] = historical_value[dependencies].apply(func, axis=1)

        historical_value = historical_value.drop(columns=dependencies, axis=1)
        factor_historical_value = pd.merge(factor_historical_value, historical_value, how='outer', on="security_code")
        return factor_historical_value

    @staticmethod
    def PBAvgOnSW1(valuation_sets, sw_industry, factor_historical_value, dependencies=['pb']):
        """
        PB 均值
        :param valuation_sets:
        :param sw_industry:
        :param factor_historical_value:
        :param dependencies:
        :return: PB 均值
        """
        valuation_sets = valuation_sets.loc[:, dependencies]
        valuation_sets = pd.merge(valuation_sets, sw_industry, how='outer', on='security_code')

        historical_value_tmp = valuation_sets.groupby('isymbol')
        historical_value_tmp = historical_value_tmp.mean().rename(columns={"pb": "PBAvgOnSW1"})
        historical_value_tmp['PBAvgOnSW1'] = historical_value_tmp['PBAvgOnSW1']
        historical_value = pd.merge(valuation_sets, historical_value_tmp, how='outer', on='isymbol')

        dependencies = dependencies + ['isymbol']
        historical_value = historical_value.drop(dependencies, axis=1)

        factor_historical_value = pd.merge(historical_value, factor_historical_value, how='outer', on='security_code')

        return factor_historical_value

    @staticmethod
    def PBStdOnSW1(valuation_sets, sw_industry, factor_historical_value=None, dependencies=['pb']):
        """
        PB 标准差
        :param valuation_sets:
        :param sw_industry:
        :param factor_historical_value:
        :param dependencies:
        :return: PB 标准差
        """
        valuation_sets = valuation_sets.loc[:, dependencies]
        valuation_sets = pd.merge(valuation_sets, sw_industry, how='outer', on='security_code')

        historical_value_tmp = valuation_sets.groupby('isymbol')
        historical_value_tmp = historical_value_tmp.std().rename(columns={"pb": "PBStdOnSW1"})

        historical_value = pd.merge(valuation_sets, historical_value_tmp, how='outer', on='isymbol')

        dependencies = dependencies + ['isymbol']
        historical_value = historical_value.drop(dependencies, axis=1)
        factor_historical_value = pd.merge(historical_value, factor_historical_value, how='outer', on='security_code')

        return factor_historical_value

    @staticmethod
    def PBIndu(valuation_sets, factor_historical_value, dependencies=['pb']):
        """
        (Pb – Pb 的行业均值)/Pb 的行业标准差
        :param dependencies:
        :param valuation_sets:
        :param factor_historical_value:
        :return: (Pb – Pb 的行业均值)/Pb 的行业标准差
        """
        historical_value = valuation_sets.loc[:, dependencies]
        factor_historical_value = pd.merge(historical_value, factor_historical_value, how='outer', on='security_code')

        factor_historical_value['PBIndu'] = (factor_historical_value['pb'] - factor_historical_value['PBAvgOnSW1']) / factor_historical_value["PBStdOnSW1"]
        factor_historical_value = factor_historical_value.drop(dependencies, axis=1)

        return factor_historical_value

    @staticmethod
    def PEToAvg6M(valuation_sets, factor_historical_value, dependencies=['pe', 'pe_mean_6m']):
        """

        :param valuation_sets:
        :param factor_historical_value:
        :param dependencies:
        :return:
        """
        historical_value = valuation_sets.loc[:, dependencies]
        func = lambda x: x[0] / x[1] if x[1] is not None and x[1] != 0 else None

        historical_value['PEToAvg6M'] = historical_value[dependencies].apply(func, axis=1)
        historical_value = historical_value.drop(columns=dependencies, axis=1)
        factor_historical_value = pd.merge(factor_historical_value, historical_value, how='outer', on="security_code")

        return factor_historical_value

    @staticmethod
    def PEToAvg3M(valuation_sets, factor_historical_value, dependencies=['pe', 'pe_mean_3m']):
        """

        :param valuation_sets:
        :param factor_historical_value:
        :param dependencies:
        :return:
        """
        historical_value = valuation_sets.loc[:, dependencies]
        func = lambda x: x[0] / x[1] if x[1] is not None and x[1] != 0 else None

        historical_value['PEToAvg3M'] = historical_value[dependencies].apply(func, axis=1)
        historical_value = historical_value.drop(columns=dependencies, axis=1)
        factor_historical_value = pd.merge(factor_historical_value, historical_value, how='outer', on="security_code")

        return factor_historical_value

    @staticmethod
    def PEToAvg2M(valuation_sets, factor_historical_value, dependencies=['pe', 'pe_mean_2m']):
        """

        :param valuation_sets:
        :param factor_historical_value:
        :param dependencies:
        :return:
        """
        historical_value = valuation_sets.loc[:, dependencies]
        func = lambda x: x[0] / x[1] if x[1] is not None and x[1] != 0 else None

        historical_value['PEToAvg2M'] = historical_value[dependencies].apply(func, axis=1)
        historical_value = historical_value.drop(columns=dependencies, axis=1)
        factor_historical_value = pd.merge(factor_historical_value, historical_value, how='outer', on="security_code")

        return factor_historical_value

    @staticmethod
    def PEToAvg1Y(valuation_sets, factor_historical_value, dependencies=['pe', 'pe_mean_1y']):
        """

        :param valuation_sets:
        :param factor_historical_value:
        :param dependencies:
        :return:
        """
        historical_value = valuation_sets.loc[:, dependencies]
        func = lambda x: x[0] / x[1] if x[1] is not None and x[1] != 0 else None

        historical_value['PEToAvg1Y'] = historical_value[dependencies].apply(func, axis=1)
        historical_value = historical_value.drop(columns=dependencies, axis=1)
        factor_historical_value = pd.merge(factor_historical_value, historical_value, how='outer', on="security_code")

        return factor_historical_value

    @staticmethod
    def TotalAssets(valuation_sets, factor_historical_value, dependencies=['total_assets_report']):
        """
        总资产
        :param valuation_sets:
        :param factor_historical_value:
        :param dependencies:
        :return: 总资产
        """
        historical_value = valuation_sets.loc[:, dependencies]

        factor_historical_value = pd.merge(historical_value, factor_historical_value, how='outer', on='security_code')
        factor_historical_value = factor_historical_value.rename(columns={"total_assets_report": "TotalAssets"})

        return factor_historical_value

    @staticmethod
    def MktValue(valuation_sets, factor_historical_value, dependencies=['market_cap']):
        """
        总市值
        :param valuation_sets:
        :param factor_historical_value:
        :param dependencies:
        :return: 总市值
        """
        historical_value = valuation_sets.loc[:, dependencies]
        factor_historical_value = pd.merge(historical_value, factor_historical_value, how='outer', on='security_code')
        factor_historical_value = factor_historical_value.rename(columns={"market_cap": "MktValue"})
        return factor_historical_value

    @staticmethod
    def CirMktValue(valuation_sets, factor_historical_value, dependencies=['circulating_market_cap']):
        """
        流通市值
        :param valuation_sets:
        :param factor_historical_value:
        :param dependencies:
        :return: 流通市值
        """
        historical_value = valuation_sets.loc[:, dependencies]
        factor_historical_value = pd.merge(historical_value, factor_historical_value, how='outer', on='security_code')
        factor_historical_value = factor_historical_value.rename(columns={"circulating_market_cap": "CirMktValue"})
        return factor_historical_value

    # MRQ
    @staticmethod
    def LogTotalAssets(valuation_sets, factor_historical_value, dependencies=['total_assets']):
        """
        对数总资产MRQ
        :param valuation_sets:
        :param factor_historical_value:
        :param dependencies:
        :return: 对数总资产MRQ
        """
        historical_value = valuation_sets.loc[:, dependencies]
        func = lambda x: math.log(abs(x[0])) if x[0] is not None and x[0] != 0 else None

        historical_value['LogTotalAssets'] = historical_value[dependencies].apply(func, axis=1)
        historical_value = historical_value.drop(columns=dependencies, axis=1)
        factor_historical_value = pd.merge(factor_historical_value, historical_value, how='outer', on="security_code")
        # factor_historical_value['LogTotalAssets'] = historical_value['LogTotalAssets']
        return factor_historical_value

    @staticmethod
    def BMInduAvgOnSW1(valuation_sets, sw_industry, factor_historical_value,
                                       dependencies=['equities_parent_company_owners', 'market_cap']):
        """
        归属于母公司的股东权益（MRQ) / 总市值
        :param valuation_sets:
        :param sw_industry:
        :param factor_historical_value:
        :param dependencies:
        :return: 归属于母公司的股东权益（MRQ) / 总市值
        """
        valuation_sets = valuation_sets.loc[:, dependencies]

        func = lambda x: x[0] / x[1] if x[1] is not None and x[1] != 0 else None
        valuation_sets['tmp'] = valuation_sets[dependencies].apply(func, axis=1)

        valuation_sets = pd.merge(valuation_sets, sw_industry, how='outer', on='security_code')

        historical_value_tmp = valuation_sets.groupby('isymbol')

        historical_value_tmp = historical_value_tmp.mean().rename(columns={"tmp": "BMInduAvgOnSW1"})
        historical_value_tmp = historical_value_tmp['BMInduAvgOnSW1']

        historical_value = pd.merge(valuation_sets, historical_value_tmp, how='outer', on='isymbol')

        dependencies = dependencies + ['isymbol', 'tmp']
        historical_value = historical_value.drop(dependencies, axis=1)
        factor_historical_value = pd.merge(historical_value, factor_historical_value, how='outer', on='security_code')

        return factor_historical_value

    @staticmethod
    def BMInduSTDOnSW1(valuation_sets, sw_industry, factor_historical_value,
                                       dependencies=['equities_parent_company_owners', 'market_cap']):
        """
        归属于母公司的股东权益（MRQ) / 总市值
        :param valuation_sets:
        :param sw_industry:
        :param factor_historical_value:
        :param dependencies:
        :return: 归属于母公司的股东权益（MRQ) / 总市值
        """
        valuation_sets = valuation_sets.loc[:, dependencies]

        func = lambda x: x[0] / x[1] if x[1] is not None and x[1] != 0 else None
        valuation_sets['tmp'] = valuation_sets[dependencies].apply(func, axis=1)

        valuation_sets = pd.merge(valuation_sets, sw_industry, how='outer', on='security_code')

        historical_value_tmp = valuation_sets.groupby('isymbol')

        historical_value_tmp = historical_value_tmp.std().rename(columns={"tmp": "BMInduSTDOnSW1"})
        historical_value_tmp = historical_value_tmp['BMInduSTDOnSW1']

        historical_value = pd.merge(valuation_sets, historical_value_tmp, how='outer', on='isymbol')

        dependencies = dependencies + ['isymbol', 'tmp']
        historical_value = historical_value.drop(dependencies, axis=1)
        factor_historical_value = pd.merge(historical_value, factor_historical_value, how='outer', on='security_code')

        return factor_historical_value

    @staticmethod
    def BookValueToIndu(valuation_sets, factor_historical_value,
                             dependencies=['equities_parent_company_owners', 'market_cap']):
        """
        归属于母公司的股东权益（MRQ) / 总市值(行业)
        :param valuation_sets:
        :param factor_historical_value:
        :param dependencies:
        :return: 归属于母公司的股东权益（MRQ) / 总市值(行业)
        """
        valuation_sets = valuation_sets.loc[:, dependencies]

        func = lambda x: x[0] / x[1] if x[1] is not None and x[1] != 0 else None
        valuation_sets['tmp'] = valuation_sets[dependencies].apply(func, axis=1)

        factor_historical_value = pd.merge(valuation_sets, factor_historical_value, how='outer', on='security_code')

        factor_historical_value['BookValueToIndu'] = (factor_historical_value['tmp'] - factor_historical_value['BMInduAvgOnSW1'])\
                                                     / factor_historical_value["BMInduSTDOnSW1"]
        dependencies = dependencies + ['tmp']
        factor_historical_value = factor_historical_value.drop(dependencies, axis=1)
        return factor_historical_value

    @staticmethod
    def TotalAssetsToEnterpriseValue(valuation_sets, factor_historical_value, dependencies=['total_assets_report',
                                                                                          'shortterm_loan',
                                                                                          'longterm_loan',
                                                                                          'market_cap',
                                                                                          'cash_and_equivalents_at_end',
                                                                                          ]):
        """
        资产总计/企业价值 MRQ
        :param valuation_sets:
        :param factor_historical_value:
        :param dependencies:
        :return: 资产总计/企业价值 MRQ
        """
        historical_value = valuation_sets.loc[:, dependencies]
        fuc = lambda x: x[1] + x[2] + x[3] - x[4]

        historical_value['temp'] = historical_value[dependencies].apply(fuc, axis=1)

        historical_value['TotalAssetsToEnterpriseValue'] = np.where(CalcTools.is_zero(historical_value['temp']), 0,
                                                                    historical_value['total_assets_report'] /
                                                                    historical_value['temp'])

        dependencies = dependencies + ['temp']
        historical_value = historical_value.drop(dependencies, axis=1)
        factor_historical_value = pd.merge(factor_historical_value, historical_value, how='outer', on="security_code")

        return factor_historical_value

    # TTM
    @staticmethod
    def LogSalesTTM(valuation_sets, factor_historical_value, dependencies=['total_operating_revenue']):
        """
        对数营业收入(TTM)
        :param valuation_sets:
        :param factor_historical_value:
        :param dependencies:
        :return: 对数营业收入(TTM)
        """
        historical_value = valuation_sets.loc[:, dependencies]
        func = lambda x: math.log(abs(x[0])) if x[0] is not None and x[0] != 0 else None

        historical_value['LogSalesTTM'] = historical_value[dependencies].apply(func, axis=1)
        historical_value = historical_value.drop(columns=dependencies, axis=1)
        factor_historical_value = pd.merge(factor_historical_value, historical_value, how='outer', on="security_code")
        # factor_historical_value['LogSalesTTM'] = historical_value['LogSalesTTM']
        return factor_historical_value

    @staticmethod
    def PCFToOptCashflowTTM(valuation_sets, factor_historical_value, dependencies=['market_cap', 'net_operate_cash_flow']):
        """
        市现率PCF(经营现金流TTM)
        :param valuation_sets:
        :param factor_historical_value:
        :param dependencies:
        :return: 市现率PCF(经营现金流TTM)
        """
        historical_value = valuation_sets.loc[:, dependencies]
        func = lambda x: x[0] / x[1] if x[1] is not None and x[1] != 0 else None
        historical_value['PCFToOptCashflowTTM'] = historical_value[dependencies].apply(func, axis=1)

        historical_value = historical_value.drop(columns=dependencies, axis=1)
        factor_historical_value = pd.merge(factor_historical_value, historical_value, how='outer', on="security_code")
        # factor_historical_value['PCFToOptCashflowTTM'] = historical_value['PCFToOptCashflowTTM']
        return factor_historical_value

    @staticmethod
    def EPTTM(valuation_sets, factor_historical_value, dependencies=['net_profit', 'market_cap']):
        """
        收益市值比 = 净利润TTM/总市值
        :param dependencies:
        :param valuation_sets:
        :param factor_historical_value:
        :return: 收益市值比
        """
        historical_value = valuation_sets.loc[:, dependencies]

        historical_value['EPTTM'] = np.where(CalcTools.is_zero(historical_value['market_cap']), 0,
                                             historical_value['net_profit'] /
                                             historical_value['market_cap'])
        historical_value = historical_value.drop(columns=dependencies, axis=1)
        factor_historical_value = pd.merge(factor_historical_value, historical_value, how='outer', on="security_code")
        # factor_historical_value['EPTTM'] = historical_value['EPTTM']
        return factor_historical_value

    @staticmethod
    def PECutTTM(valuation_sets, factor_historical_value, dependencies=['market_cap', 'net_profit_cut_pre']):
        """
        市盈率PE(TTM)（扣除）
        :param valuation_sets:
        :param factor_historical_value:
        :param dependencies:
        :return: 市盈率PE(TTM)（扣除）
        """
        historical_value = valuation_sets.loc[:, dependencies]

        historical_value['PECutTTM'] = np.where(CalcTools.is_zero(historical_value['net_profit_cut_pre']), 0,
                                                historical_value['market_cap'] /
                                                historical_value['net_profit_cut_pre'])

        # factor_historical_value['PECutTTM'] = historical_value['PECutTTM']
        historical_value = historical_value.drop(columns=dependencies, axis=1)
        factor_historical_value = pd.merge(factor_historical_value, historical_value, how='outer', on="security_code")
        return factor_historical_value

    @staticmethod
    def PEAvgOnSW1(valuation_sets, sw_industry, factor_historical_value, dependencies=['pe']):
        """
        PE 均值
        :param valuation_sets:
        :param sw_industry:
        :param factor_historical_value:
        :param dependencies:
        :return: PE 均值
        """
        valuation_sets = valuation_sets.loc[:, dependencies]
        valuation_sets = pd.merge(valuation_sets, sw_industry, how='outer', on='security_code')

        historical_value_tmp = valuation_sets.groupby('isymbol')
        historical_value_tmp = historical_value_tmp.mean().rename(columns={"pe": "PEAvgOnSW1"})
        historical_value = pd.merge(valuation_sets, historical_value_tmp, how='outer', on='isymbol')

        dependencies = dependencies + ['isymbol']
        historical_value = historical_value.drop(dependencies, axis=1)
        factor_historical_value = pd.merge(historical_value, factor_historical_value, how='outer', on='security_code')

        return factor_historical_value

    @staticmethod
    def PEStdOnSW1(valuation_sets, sw_industry, factor_historical_value, dependencies=['pe']):
        """
        PE 标准差
        :param valuation_sets:
        :param sw_industry:
        :param factor_historical_value:
        :param dependencies:
        :return: PE 标准差
        """
        valuation_sets = valuation_sets.loc[:, dependencies]
        valuation_sets = pd.merge(valuation_sets, sw_industry, how='outer', on='security_code')

        historical_value_tmp = valuation_sets.groupby('isymbol')
        historical_value_tmp = historical_value_tmp.std().rename(columns={"pe": "PEStdOnSW1"})
        historical_value = pd.merge(valuation_sets, historical_value_tmp, how='outer', on='isymbol')

        dependencies = dependencies + ['isymbol']
        historical_value = historical_value.drop(dependencies, axis=1)
        factor_historical_value = pd.merge(historical_value, factor_historical_value, how='outer', on='security_code')

        return factor_historical_value

    @staticmethod
    def PSAvgOnSW1(valuation_sets, sw_industry, factor_historical_value, dependencies=['ps']):
        """
        PS 均值 TTM
        :param valuation_sets:
        :param sw_industry:
        :param factor_historical_value:
        :param dependencies:
        :return: PS 行业均值 TTM
        """
        valuation_sets = valuation_sets.loc[:, dependencies]
        valuation_sets = pd.merge(valuation_sets, sw_industry, how='outer', on='security_code')

        historical_value_tmp = valuation_sets.groupby('isymbol')
        historical_value_tmp = historical_value_tmp.mean().rename(columns={"ps": "PSAvgOnSW1"})

        historical_value = pd.merge(valuation_sets, historical_value_tmp, how='outer', on='isymbol')

        dependencies = dependencies + ['isymbol']
        historical_value = historical_value.drop(dependencies, axis=1)
        factor_historical_value = pd.merge(historical_value, factor_historical_value, how='outer', on='security_code')

        return factor_historical_value

    @staticmethod
    def PSStdOnSW1(valuation_sets, sw_industry, factor_historical_value, dependencies=['ps']):
        """
        PS 标准差 TTM
        :param valuation_sets:
        :param sw_industry:
        :param factor_historical_value:
        :param dependencies:
        :return: PS 行业标准差 TTM
        """
        valuation_sets = valuation_sets.loc[:, dependencies]
        valuation_sets = pd.merge(valuation_sets, sw_industry, how='outer', on='security_code')

        historical_value_tmp = valuation_sets.groupby('isymbol')
        historical_value_tmp = historical_value_tmp.std().rename(columns={"ps": "PSStdOnSW1"})

        historical_value = pd.merge(valuation_sets, historical_value_tmp, how='outer', on='isymbol')

        dependencies = dependencies + ['isymbol']
        historical_value = historical_value.drop(dependencies, axis=1)
        factor_historical_value = pd.merge(historical_value, factor_historical_value, how='outer', on='security_code')

        return factor_historical_value

    @staticmethod
    def PCFAvgOnSW1(valuation_sets, sw_industry, factor_historical_value, dependencies=['pcf']):
        """
        PCF 均值
        :param valuation_sets:
        :param sw_industry:
        :param factor_historical_value:
        :param dependencies:
        :return: PCF 均值
        """
        valuation_sets = valuation_sets.loc[:, dependencies]
        valuation_sets = pd.merge(valuation_sets, sw_industry, how='outer', on='security_code')

        historical_value_tmp = valuation_sets.groupby('isymbol')
        historical_value_tmp = historical_value_tmp.mean().rename(columns={"pcf": "PCFAvgOnSW1"})

        historical_value = pd.merge(valuation_sets, historical_value_tmp, how='outer', on='isymbol')

        dependencies = dependencies + ['isymbol']
        historical_value = historical_value.drop(dependencies, axis=1)
        factor_historical_value = pd.merge(historical_value, factor_historical_value, how='outer', on='security_code')

        return factor_historical_value

    @staticmethod
    def PCFStdOnSW1(valuation_sets, sw_industry, factor_historical_value, dependencies=['pcf']):
        """
        PCF 标准差
        :param valuation_sets:
        :param sw_industry:
        :param factor_historical_value:
        :param dependencies:
        :return: PCF 标准差
        """
        valuation_sets = valuation_sets.loc[:, dependencies]
        valuation_sets = pd.merge(valuation_sets, sw_industry, how='outer', on='security_code')

        historical_value_tmp = valuation_sets.groupby('isymbol')
        historical_value_tmp = historical_value_tmp.std().rename(columns={"pcf": "PCFStdOnSW1"})

        historical_value = pd.merge(valuation_sets, historical_value_tmp, how='outer', on='isymbol')

        dependencies = dependencies + ['isymbol']
        historical_value = historical_value.drop(dependencies, axis=1)
        factor_historical_value = pd.merge(historical_value, factor_historical_value, how='outer', on='security_code')

        return factor_historical_value

    @staticmethod
    def PEIndu(tp_historical_value, factor_historical_value, dependencies=['pe']):
        """
        (PE – PE 的行业均值)/PE 的行业标准差 TTM
        :param dependencies:
        :param tp_historical_value:
        :param factor_historical_value:
        :return: (PE – PE 的行业均值)/PE 的行业标准差 TTM
        """
        historical_value = tp_historical_value.loc[:, dependencies]
        factor_historical_value = pd.merge(historical_value, factor_historical_value, how='outer', on='security_code')

        factor_historical_value['PEIndu'] = (factor_historical_value['pe'] - factor_historical_value['PEAvgOnSW1']) / factor_historical_value["PEStdOnSW1"]
        factor_historical_value = factor_historical_value.drop(dependencies, axis=1)

        return factor_historical_value

    @staticmethod
    def PSIndu(valuation_sets, factor_historical_value, dependencies=['ps']):
        """
        (Ps – Ps 的行业均值)/Ps 的行业标准差 TTM
        :param dependencies:
        :param valuation_sets:
        :param factor_historical_value:
        :return: (Ps – Ps 的行业均值)/Ps 的行业标准差 TTM
        """
        historical_value = valuation_sets.loc[:, dependencies]
        factor_historical_value = pd.merge(historical_value, factor_historical_value, how='outer', on='security_code')

        factor_historical_value['PSIndu'] = (factor_historical_value['ps'] - factor_historical_value['PSAvgOnSW1']) / factor_historical_value["PSStdOnSW1"]
        factor_historical_value = factor_historical_value.drop(dependencies, axis=1)

        return factor_historical_value

    @staticmethod
    def PCFIndu(valuation_sets, factor_historical_value, dependencies=['pcf']):
        """
        (Pcf – Pcf 的行业均值)/Pcf 的行业标准差 TTM
        :param dependencies:
        :param valuation_sets:
        :param factor_historical_value:
        :return:  (Pcf – Pcf 的行业均值)/Pcf 的行业标准差 TTM
        """
        historical_value = valuation_sets.loc[:, dependencies]
        factor_historical_value = pd.merge(historical_value, factor_historical_value, how='outer', on='security_code')

        factor_historical_value['PCFIndu'] = (factor_historical_value['pcf'] - factor_historical_value['PCFAvgOnSW1']) / factor_historical_value["PCFStdOnSW1"]
        factor_historical_value = factor_historical_value.drop(dependencies, axis=1)

        return factor_historical_value

    @staticmethod
    def TotalMrktAVGToEBIDAOnSW1(valuation_sets, sw_industry, factor_historical_value,
                                dependencies=['market_cap', 'total_profit']):
        """

        :param valuation_sets:
        :param sw_industry:
        :param factor_historical_value:
        :param dependencies:
        :return: 总市值/ 利润总额TTM（行业平均）
        """
        valuation_sets = valuation_sets.loc[:, dependencies]

        func = lambda x: x[0] / x[1] if x[1] is not None and x[1] != 0 else None
        valuation_sets['tmp'] = valuation_sets[dependencies].apply(func, axis=1)

        valuation_sets = pd.merge(valuation_sets, sw_industry, how='outer', on='security_code')

        historical_value_tmp = valuation_sets.groupby('isymbol')

        historical_value_tmp = historical_value_tmp.mean().rename(columns={"tmp": "TotalMrktAVGToEBIDAOnSW1"})
        historical_value_tmp = historical_value_tmp['TotalMrktAVGToEBIDAOnSW1']

        historical_value = pd.merge(valuation_sets, historical_value_tmp, how='outer', on='isymbol')

        dependencies = dependencies + ['isymbol', 'tmp']
        historical_value = historical_value.drop(dependencies, axis=1)
        factor_historical_value = pd.merge(historical_value, factor_historical_value, how='outer', on='security_code')

        return factor_historical_value

    @staticmethod
    def TotalMrktSTDToEBIDAOnSW1(valuation_sets, sw_industry, factor_historical_value,
                                dependencies=['market_cap', 'total_profit']):
        """
        总市值/ 利润总额TTM
        :param valuation_sets:
        :param sw_industry:
        :param factor_historical_value:
        :param dependencies:
        :return: 总市值/ 利润总额TTM
        """
        valuation_sets = valuation_sets.loc[:, dependencies]

        func = lambda x: x[0] / x[1] if x[1] is not None and x[1] != 0 else None
        valuation_sets['tmp'] = valuation_sets[dependencies].apply(func, axis=1)

        valuation_sets = pd.merge(valuation_sets, sw_industry, how='outer', on='security_code')

        historical_value_tmp = valuation_sets.groupby('isymbol')

        historical_value_tmp = historical_value_tmp.std().rename(columns={"tmp": "TotalMrktSTDToEBIDAOnSW1"})
        historical_value_tmp = historical_value_tmp["TotalMrktSTDToEBIDAOnSW1"]

        historical_value = pd.merge(valuation_sets, historical_value_tmp, how='outer', on='isymbol')

        dependencies = dependencies + ['isymbol', 'tmp']
        historical_value = historical_value.drop(dependencies, axis=1)
        factor_historical_value = pd.merge(historical_value, factor_historical_value, how='outer', on='security_code')
        return factor_historical_value

    @staticmethod
    def TotalMrktToEBIDATTM(valuation_sets, factor_historical_value,
                                     dependencies=['market_cap', 'total_profit']):

        """
        总市值/ 利润总额TTM(行业)
        :param valuation_sets:
        :param factor_historical_value:
        :param dependencies:
        :return: 总市值/ 利润总额TTM（行业）
        """
        valuation_sets = valuation_sets.loc[:, dependencies]

        func = lambda x: x[0] / x[1] if x[1] is not None and x[1] != 0 else None
        valuation_sets['tmp'] = valuation_sets[dependencies].apply(func, axis=1)

        factor_historical_value = pd.merge(valuation_sets, factor_historical_value, how='outer', on='security_code')

        factor_historical_value['TotalMrktToEBIDATTM'] = (factor_historical_value['tmp'] - factor_historical_value['TotalMrktAVGToEBIDAOnSW1'])\
                                                     / factor_historical_value["TotalMrktSTDToEBIDAOnSW1"]
        dependencies = dependencies + ['tmp']
        factor_historical_value = factor_historical_value.drop(dependencies, axis=1)
        return factor_historical_value

    @staticmethod
    def PEG3YChgTTM(valuation_sets, factor_historical_value, dependencies=['pe', 'np_parent_company_owners', 'np_parent_company_owners_3']):
        """
        市盈率/归属于母公司所有者净利润 3 年复合增长率
        :param dependencies:
        :param valuation_sets:
        :param factor_historical_value:
        :return: 市盈率/归属于母公司所有者净利润 3 年复合增长率
        """
        historical_value = valuation_sets.loc[:, dependencies]

        tmp = np.where(CalcTools.is_zero(historical_value['np_parent_company_owners_3']), 0,
                       (historical_value['np_parent_company_owners'] / historical_value['np_parent_company_owners_3']))
        historical_value['PEG3YChgTTM'] = tmp / abs(tmp) * pow(abs(tmp), 1 / 3.0) - 1

        historical_value = historical_value.drop(columns=dependencies, axis=1)
        factor_historical_value = pd.merge(factor_historical_value, historical_value, how='outer', on="security_code")
        return factor_historical_value

    @staticmethod
    def PEG5YChgTTM(valuation_sets, factor_historical_value, dependencies=['pe', 'np_parent_company_owners', 'np_parent_company_owners_5']):
        """
        市盈率/归属于母公司所有者净利润 5 年复合增长率
        :param dependencies:
        :param valuation_sets:
        :param factor_historical_value:
        :return: 市盈率/归属于母公司所有者净利润 5 年复合增长率
        """
        historical_value = valuation_sets.loc[:, dependencies]

        tmp = np.where(CalcTools.is_zero(historical_value['np_parent_company_owners_5']), 0,
                       (historical_value['np_parent_company_owners'] / historical_value['np_parent_company_owners_5']))
        historical_value['PEG5YChgTTM'] = tmp / abs(tmp) * pow(abs(tmp), 1 / 5.0) - 1

        historical_value = historical_value.drop(columns=dependencies, axis=1)
        factor_historical_value = pd.merge(factor_historical_value, historical_value, how='outer', on="security_code")
        # factor_historical_value['PEG5YChgTTM'] = historical_value['PEG5YChgTTM']
        return factor_historical_value

    @staticmethod
    def CEToPTTM(valuation_sets, factor_historical_value, dependencies=['net_operate_cash_flow', 'market_cap']):
        """
        现金收益滚动收益与市值比
        经营活动产生的现金流量净额与市值比
        :param dependencies:
        :param valuation_sets:
        :param factor_historical_value:
        :return: 现金收益滚动收益与市值比
        """
        historical_value = valuation_sets.loc[:, dependencies]

        historical_value['CEToPTTM'] = np.where(CalcTools.is_zero(historical_value['market_cap']), 0,
                                                historical_value['net_operate_cash_flow'] /
                                                historical_value['market_cap'])

        historical_value = historical_value.drop(columns=dependencies, axis=1)
        factor_historical_value = pd.merge(factor_historical_value, historical_value, how='outer', on="security_code")

        return factor_historical_value

    @staticmethod
    def RevToMrktRatioTTM(valuation_sets, factor_historical_value, dependencies=['operating_revenue',
                                                                                           'market_cap']):
        """
        营收市值比(TTM)
        营业收入（TTM）/总市值
        :param valuation_sets:
        :param factor_historical_value:
        :param dependencies:
        :return: 营收市值比(TTM)
        """
        historical_value = valuation_sets.loc[:, dependencies]

        historical_value['RevToMrktRatioTTM'] = np.where(CalcTools.is_zero(historical_value['market_cap']), 0,
                                                historical_value['operating_revenue'] /
                                                historical_value['market_cap'])

        historical_value = historical_value.drop(columns=dependencies, axis=1)
        factor_historical_value = pd.merge(factor_historical_value, historical_value, how='outer', on="security_code")
        return factor_historical_value

    @staticmethod
    def OptIncToEnterpriseValueTTM(valuation_sets, factor_historical_value, dependencies=['operating_revenue',
                                                                                           'shortterm_loan',
                                                                                           'longterm_loan',
                                                                                           'market_cap',
                                                                                           'cash_and_equivalents_at_end',
                                                                                           ]):
        """
        营业收入(TTM)/企业价值
        企业价值= 长期借款+ 短期借款+ 总市值- 现金及现金等价物
        :param valuation_sets:
        :param factor_historical_value:
        :param dependencies:
        :return: 营业收入(TTM)/企业价值
        """
        historical_value = valuation_sets.loc[:, dependencies]

        fuc = lambda x: x[1] + x[2] + x[3] - x[4]
        historical_value['temp'] = historical_value[dependencies].apply(fuc, axis=1)
        historical_value['OptIncToEnterpriseValueTTM'] = np.where(CalcTools.is_zero(historical_value['temp']), 0,
                                                                  historical_value['operating_revenue'] /
                                                                  historical_value['temp'])
        dependencies = dependencies + ['temp']
        historical_value = historical_value.drop(columns=dependencies, axis=1)
        factor_historical_value = pd.merge(factor_historical_value, historical_value, how='outer', on="security_code")
        return factor_historical_value
