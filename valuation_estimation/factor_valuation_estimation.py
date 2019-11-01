#! /usr/bin/env python
# -*- coding: utf-8 -*-

"""
@version: 0.1
@author: li
@file: factor_valuation_estimation.py
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
from utilities.calc_tools import CalcTools
from utilities.singleton import Singleton

# from basic_derivation import app
# from ultron.cluster.invoke.cache_data import cache_data
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


@six.add_metaclass(Singleton)
class FactorValuationEstimation(object):
    """
    估值
    """
    def __init__(self):
        __str__ = 'factor_valuation_estimation'
        self.name = '估值类'
        self.factor_type1 = '估值类'
        self.factor_type2 = '估值类'
        self.description = '估值类因子'

    @staticmethod
    def LogofMktValue(valuation_sets, factor_historical_value, dependencies=['market_cap']):
        """
        :name: 总市值的对数
        :desc: 市值的对数
        :unit:
        :view_dimension: 1
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
        :name: 流通总市值的对数
        :desc: 流通市值的对数
        :unit:
        :view_dimension: 1
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
        :name: 对数市值立方
        :desc: 对数市值开立方
        :unit:
        :view_dimension: 1
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
        :name: 市值/企业自由现金流
        :desc: 总市值/企业自由现金流LYR 企业自由现金流取截止指定日最新年报
        :unit:
        :view_dimension: 0.01
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
        :name: 所属申万一级行业的PB均值
        :desc: 所属申万一级行业的PB均值。注：剔除PB负值。
        :unit: 倍
        :view_dimension: 1
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
        :name: 所属申万一级行业的PB标准差
        :desc: 所属申万一级行业的PB标准差。注：剔除PB负值。
        :unit:
        :view_dimension: 1
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
        :name: PB行业相对值
        :desc: (Pb – Pb 的行业均值)/Pb 的行业标准差
        :unit:
        :view_dimension: 0.01
        """
        historical_value = valuation_sets.loc[:, dependencies]
        factor_historical_value = pd.merge(historical_value, factor_historical_value, how='outer', on='security_code')

        factor_historical_value['PBIndu'] = (factor_historical_value['pb'] - factor_historical_value['PBAvgOnSW1']) / factor_historical_value["PBStdOnSW1"]
        factor_historical_value = factor_historical_value.drop(dependencies, axis=1)
        return factor_historical_value

    @staticmethod
    def PEToAvg6M(valuation_sets, factor_historical_value, dependencies=['pe', 'pe_mean_6m']):
        """
        :name: 市盈率PE/过去六个月的PE的均值
        :desc: PE/过去120个市场交易日的PE均值
        :unit:
        :view_dimension: 0.01
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
        :name: 市盈率PE/过去三个月的PE的均值
        :desc: PE/过去90个市场交易日的PE均值
        :unit:
        :view_dimension: 0.01
        """
        historical_value = valuation_sets.loc[:, dependencies]
        func = lambda x: x[0] / x[1] if x[1] is not None and x[1] != 0 else None

        historical_value['PEToAvg3M'] = historical_value[dependencies].apply(func, axis=1)
        historical_value = historical_value.drop(columns=dependencies, axis=1)
        factor_historical_value = pd.merge(factor_historical_value, historical_value, how='outer', on="security_code")

        return factor_historical_value

    @staticmethod
    def PEToAvg1M(valuation_sets, factor_historical_value, dependencies=['pe', 'pe_mean_1m']):
        """
        :name: 市盈率PE/过去一个月的PE的均值
        :desc: PE/过去20个市场交易日的PE均值
        :unit:
        :view_dimension: 0.01
        """
        historical_value = valuation_sets.loc[:, dependencies]
        func = lambda x: x[0] / x[1] if x[1] is not None and x[1] != 0 else None

        historical_value['PEToAvg1M'] = historical_value[dependencies].apply(func, axis=1)
        historical_value = historical_value.drop(columns=dependencies, axis=1)
        factor_historical_value = pd.merge(factor_historical_value, historical_value, how='outer', on="security_code")

        return factor_historical_value

    @staticmethod
    def PEToAvg1Y(valuation_sets, factor_historical_value, dependencies=['pe', 'pe_mean_1y']):
        """
        :name: 市盈率PE/过去一年的PE的均值
        :desc: PE/过去250个市场交易日的PE均值
        :unit:
        :view_dimension: 0.01
        """
        historical_value = valuation_sets.loc[:, dependencies]
        func = lambda x: x[0] / x[1] if x[1] is not None and x[1] != 0 else None

        historical_value['PEToAvg1Y'] = historical_value[dependencies].apply(func, axis=1)
        historical_value = historical_value.drop(columns=dependencies, axis=1)
        factor_historical_value = pd.merge(factor_historical_value, historical_value, how='outer', on="security_code")

        return factor_historical_value

    @staticmethod
    def _TotalAssets(valuation_sets, factor_historical_value, dependencies=['total_assets_report']):
        """
        基础衍生中有该因子
        :name: 总资产
        :desc: 总资产
        :unit:
        :view_dimension: 0.01
        """
        historical_value = valuation_sets.loc[:, dependencies]

        factor_historical_value = pd.merge(historical_value, factor_historical_value, how='outer', on='security_code')
        factor_historical_value = factor_historical_value.rename(columns={"total_assets_report": "TotalAssets"})

        return factor_historical_value

    @staticmethod
    def MktValue(valuation_sets, factor_historical_value, dependencies=['market_cap']):
        """
        :name: 总市值
        :desc: 总市值
        :unit: 元
        :view_dimension: 100000000
        """
        historical_value = valuation_sets.loc[:, dependencies]
        factor_historical_value = pd.merge(historical_value, factor_historical_value, how='outer', on='security_code')
        factor_historical_value = factor_historical_value.rename(columns={"market_cap": "MktValue"})
        return factor_historical_value

    @staticmethod
    def CirMktValue(valuation_sets, factor_historical_value, dependencies=['circulating_market_cap']):
        """
        :name: 流通市值
        :desc: 流通市值
        :unit: 元
        :view_dimension: 100000000
        """
        historical_value = valuation_sets.loc[:, dependencies]
        factor_historical_value = pd.merge(historical_value, factor_historical_value, how='outer', on='security_code')
        factor_historical_value = factor_historical_value.rename(columns={"circulating_market_cap": "CirMktValue"})
        return factor_historical_value

    # MRQ
    @staticmethod
    def LogTotalAssets(valuation_sets, factor_historical_value, dependencies=['total_assets']):
        """
        :name: 对数总资产（MRQ）
        :desc: 总资产MRQ的对数
        :unit:
        :view_dimension: 1
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
        :name: 所属申万一级行业的账面市值比行业均值
        :desc: 所属申万一级行业的账面市值比行业均值 分子取值归属于母公司的股东权益（MRQ）分母取值总市值
        :unit:
        :view_dimension: 0.01
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
        :name: 所属申万一级行业的账面市值比行业标准差
        :desc: 所属申万一级行业的账面市值比行业标准差。分子取值归属于母公司的股东权益（MRQ）分母取值总市值
        :unit:
        :view_dimension: 0.01
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
        :name: 账面市值比行业相对值
        :desc: （账面市值比/（行业平均账面市值比））/行业账面市值标准差 其中：账面市值比=归属于母公司的股东权益（MRQ）/总市值"
        :unit:
        :view_dimension: 0.01
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
        :name: 资产总计/企业价值 MRQ
        :desc: 资产总计/企业价值 MRQ
        :unit:
        :view_dimension: 0.01
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
        :name: 对数营业收入(TTM)
        :desc: 对数营业收入(TTM)
        :unit:
        :view_dimension: 1
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
        :name: 市现率PCF(经营现金流TTM)
        :desc: 市现率PCF(经营现金流TTM)
        :unit: 倍
        :view_dimension: 1
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
        :name: 收益市值比
        :desc: 净利润TTM/总市值
        :unit:
        :view_dimension: 0.01
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
        :name: 市盈率PE(TTM)（扣除）
        :desc: "扣非后的市盈率（TTM）=总市值/前推12个月扣除非经常性损益后的净利润 注：扣除非经常性损益后的净利润（TTM根据报告期扣除非经常性损益后的净利润”计算"
        :unit: 倍
        :view_dimension: 1
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
        :name: 所属申万一级行业的PE均值
        :desc: 所属申万一级行业的PE等权均值。 注：剔除负值及1000以上的PE极值。
        :unit:
        :view_dimension: 1
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
        :name: 所属申万一级行业的PE标准差
        :desc: 所属申万一级行业的PE标准差。注：剔除负值及1000以上的PE极值。
        :unit:
        :view_dimension: 0.01
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
        :name: 所属申万一级行业的PS均值
        :desc: 所属申万一级行业的PS等权均值。 注：剔除负值及1000以上的PE极值。
        :unit: 倍
        :view_dimension: 1
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
        :name: 所属申万一级行业的PS标准差
        :desc: 所属申万一级行业的PS标准差。PS取值市销率PS（TTM）
        :unit:
        :view_dimension: 0.01
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
        :name: 所属申万一级行业的PCF均值
        :desc: 所属申万一级行业的PCF均值。PCF取值市现率PCF经营现金流TTM
        :unit:
        :view_dimension: 1
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
        :name: 所属申万一级行业的PCF标准差
        :desc: 所属申万一级行业的PCF标准差。PCF取值市现率PCF经营现金流TTM
        :unit:
        :view_dimension: 1
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
        :name: PE行业相对值 TTM
        :desc: (PE – PE 的行业均值)/PE 的行业标准差 TTM
        :unit:
        :view_dimension: 0.01
        """
        historical_value = tp_historical_value.loc[:, dependencies]
        factor_historical_value = pd.merge(historical_value, factor_historical_value, how='outer', on='security_code')

        factor_historical_value['PEIndu'] = (factor_historical_value['pe'] - factor_historical_value['PEAvgOnSW1']) / factor_historical_value["PEStdOnSW1"]
        factor_historical_value = factor_historical_value.drop(dependencies, axis=1)

        return factor_historical_value

    @staticmethod
    def PSIndu(valuation_sets, factor_historical_value, dependencies=['ps']):
        """
        :name: PS行业相对值
        :desc: (Ps – Ps 的行业均值)/Ps 的行业标准差 TTM
        :unit:
        :view_dimension: 0.01
        """
        historical_value = valuation_sets.loc[:, dependencies]
        factor_historical_value = pd.merge(historical_value, factor_historical_value, how='outer', on='security_code')

        factor_historical_value['PSIndu'] = (factor_historical_value['ps'] - factor_historical_value['PSAvgOnSW1']) / factor_historical_value["PSStdOnSW1"]
        factor_historical_value = factor_historical_value.drop(dependencies, axis=1)

        return factor_historical_value

    @staticmethod
    def PCFIndu(valuation_sets, factor_historical_value, dependencies=['pcf']):
        """
        :name: PCF行业相对值
        :desc: (Pcf – Pcf 的行业均值)/Pcf 的行业标准差 TTM
        :unit:
        :view_dimension: 1
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
        :name: 所属申万一级行业的总市值/息税折旧及摊销前利润TTM均值
        :desc: 所属申万一级行业的总市值/息税折旧及摊销前利润TTM均值   分子取值总市值，分母取值息税折旧及摊销前利润TTM（反推法
        :unit:
        :view_dimension: 0.01
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
        :name: 所属申万一级行业的总市值/息税折旧及摊销前利润TTM标准差
        :desc: 所属申万一级行业的总市值/息税折旧及摊销前利润TTM标准差    分子取值总市值，分母取值息税折旧及摊销前利润TTM（反推法）
        :unit:
        :view_dimension: 0.01
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
        :name: 总市值/息税折旧及摊销前利润TTM行业相对值
        :desc: （总市值/息税折旧及摊销利润TTM-总市值/息税折旧及摊销前利润TTM的行业均值）/（总市值/息税折旧及摊销前利润TTM行业标准差）
        :unit:
        :view_dimension: 0.01
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
        :name: PEG3 年复合增长率(TTM)
        :desc: 市盈率/归属于母公司所有者净利润 3 年复合增长率
        :unit:
        :view_dimension: 0.01
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
        :name: PEG5 年复合增长率(TTM)
        :desc: 市盈率/归属于母公司所有者净利润 5 年复合增长率
        :unit:
        :view_dimension: 0.01
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

        :name: 现金收益滚动收益与市值比
        :desc: 经营活动产生的现金流量净额与市值比
        :unit:
        :view_dimension: 0.01
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
        :name: 营收市值比(TTM)
        :desc: 营业收入（TTM）/总市值
        :unit:
        :view_dimension: 0.01
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
        :name: 营业收入(TTM)/企业价值
        :desc: 企业价值= 长期借款+ 短期借款+ 总市值- 现金及现金等价物
        :unit:
        :view_dimension: 0.01
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
