#! /usr/bin/env python
# -*- coding: utf-8 -*-

"""
@version:
@author: Wang
@file: factor_operation_capacity.py
@time: 2019-05-30
"""
import gc
import sys
sys.path.append('../')
sys.path.append('../../')
sys.path.append('../../../')
import six
import json
import pandas as pd
from basic_derivation.factor_base import FactorBase
from pandas.io.json import json_normalize
from utilities.singleton import Singleton

# from basic_derivation import app
# from ultron.cluster.invoke.cache_data import cache_data
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


@six.add_metaclass(Singleton)
class Derivation(object):
    """
    基础衍生类因子
    """
    def __init__(self):
        __str__ = 'factor_basic_derivation'
        self.name = '基础衍生'
        self.factor_type1 = '基础衍生'
        self.factor_type2 = '基础衍生'
        self.desciption = '基础衍生类因子'

    @staticmethod
    def FCFF(tp_derivation, factor_derivation, dependencies=['FCFF']):
        """
        :name: 企业自由现金流量(MRQ)
        :desc: 企业自由现金流量(MRQ)
        :param dependencies:
        :param tp_derivation:
        :param factor_derivation:
        :return:
        """
        management = tp_derivation.loc[:, dependencies]
        if len(management) <=0:
            return None
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def FCFE(tp_derivation, factor_derivation, dependencies=['FCFE']):
        """
        :name: 股东自由现金流量(MRQ)
        :desc: 股东自由现金流量(MRQ)
        :param dependencies:
        :param tp_derivation:
        :param factor_derivation:
        :return:
        """
        management = tp_derivation.loc[:, dependencies]
        if len(management) <= 0:
            return None
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def NonRecGainLoss(tp_derivation, factor_derivation, dependencies=['NEGAL']):
        """
        :name: 非经常性损益(MRQ)
        :desc: 非经常性损益(MRQ)
        :param dependencies:
        :param tp_derivation:
        :param factor_derivation:
        :return:
        """
        management = tp_derivation.loc[:, dependencies]
        if len(management) <= 0:
            return None
        management = management.rename(columns={'NEGAL': 'NonRecGainLoss'})
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def NetOptInc(tp_derivation, factor_derivation, dependencies=['NOPI']):
        """

        :name: 经营活动净收益(MRQ)
        :desc: 经营活动净收益(MRQ) indicator
        :param dependencies:
        :param tp_derivation:
        :param factor_derivation:
        :return:
        """
        management = tp_derivation.loc[:, dependencies]
        if len(management) <=0:
            return None
        management = management.rename(columns={'NOPI': 'NetOptInc'})
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def WorkingCap(tp_derivation, factor_derivation, dependencies=['WORKCAP']):
        """

        :name:  运营资本(MRQ)
        :desc:  运营资本(MRQ)
        :param dependencies:
        :param tp_derivation:
        :param factor_derivation:
        :return:
        """
        management = tp_derivation.loc[:, dependencies]
        if len(management) <=0:
            return None
        management = management.rename(columns={'WORKCAP': 'WorkingCap'})
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def TangibleAssets(tp_derivation, factor_derivation, dependencies=['RIGHAGGR',
                                                                        'MINYSHARRIGH',
                                                                        'INTAASSET',
                                                                        'DEVEEXPE',
                                                                        'GOODWILL',
                                                                        'LOGPREPEXPE',
                                                                        'DEFETAXASSET']):
        """
        :name: 有形资产(MRQ)
        :desc: 股东权益（不含少数股东权益）-无形资产+开发支出+商誉+长期待摊费用+递延所得税资产）
        :param dependencies:
        :param tp_derivation:
        :param factor_derivation:
        :return:
        """
        management = tp_derivation.loc[:, dependencies]
        if len(management) <=0:
            return None
        func = lambda x: x[0] - x[1] - x[2] + x[3] + x[4] + x[5] + x[6]
        management['TangibleAssets'] = management[dependencies].apply(func, axis=1)

        management = management.drop(dependencies, axis=1)
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def RetainedEarnings(tp_derivation, factor_derivation, dependencies=['RETAINEDEAR']):
        """
        :name: 留存收益(MRQ)
        :desc: 留存收益(MRQ)
        :param dependencies:
        :param tp_derivation:
        :param factor_derivation:
        :return:
        """
        management = tp_derivation.loc[:, dependencies]
        if len(management) <=0:
            return None
        management = management.rename(columns={'RETAINEDEAR': 'RetainedEarnings'})
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def InterestBearingLiabilities(tp_derivation, factor_derivation, dependencies=['shortterm_loan',
                                                                                     'non_current_liability_in_one_year',
                                                                                     'longterm_loan',
                                                                                     'bonds_payable',
                                                                                     'interest_payable']):
        """
        :name: 带息负债(MRQ)
        :desc: 带息负债 = 短期借款+一年内到期的长期负债+长期借款+应付债券+应付利息
        :param dependencies:
        :param tp_derivation:
        :param factor_derivation:
        :return:
        """
        management = tp_derivation.loc[:, dependencies]
        if len(management) <=0:
            return None
        func = lambda x: x[0] + x[1] + x[2] + x[3] + x[4]
        management['InterestBearingLiabilities'] = management[dependencies].apply(func, axis=1)

        management = management.drop(dependencies, axis=1)
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def NetDebt(tp_derivation, factor_derivation, dependencies=['NDEBT']):
        """
        :name: 净债务(MRQ)
        :desc: 净债务(MRQ)
        :param dependencies:
        :param tp_derivation:
        :param factor_derivation:
        :return:
        """
        management = tp_derivation.loc[:, dependencies]
        if len(management) <=0:
            return None
        management = management.rename(columns={'NDEBT': 'NetDebt'})
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def InterestFreeCurLb(tp_derivation, factor_derivation, dependencies=['NONINTCURLIABS']):
        """
        :name: 无息流动负债(MRQ)
        :desc: 无息流动负债(MRQ)
        :param dependencies:
        :param tp_derivation:
        :param factor_derivation:
        :return:
        """
        management = tp_derivation.loc[:, dependencies]
        if len(management) <=0:
            return None
        management = management.rename(columns={'NONINTCURLIABS': 'InterestFreeCurLb'})
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def InterestFreeNonCurLb(tp_derivation, factor_derivation, dependencies=['NONINTNONCURLIAB']):
        """
        :name: 无息非流动负债(MRQ)
        :desc: 无息非流动负债(MRQ)
        :param dependencies:
        :param tp_derivation:
        :param factor_derivation:
        :return:
        """
        management = tp_derivation.loc[:, dependencies]
        if len(management) <=0:
            return None
        management = management.rename(columns={'NONINTNONCURLIAB': 'InterestFreeNonCurLb'})
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def DepAndAmo(tp_derivation, factor_derivation, dependencies=['CURDEPANDAMOR']):
        """
        :name: 折旧和摊销(MRQ)
        :desc: 折旧和摊销(MRQ)
        :param dependencies:
        :param tp_derivation:
        :param factor_derivation:
        :return:
        """
        management = tp_derivation.loc[:, dependencies]
        if len(management) <=0:
            return None
        management = management.rename(columns={'CURDEPANDAMOR': 'DepAndAmo'})
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def EquityPC(tp_derivation, factor_derivation, dependencies=['PARESHARRIGH']):
        """
        :name: 归属于母公司的股东权益(MRQ)
        :desc: 归属于母公司的股东权益(MRQ) balance
        :param dependencies:
        :param tp_derivation:
        :param factor_derivation:
        :return:
        """
        management = tp_derivation.loc[:, dependencies]
        if len(management) <=0:
            return None
        management = management.rename(columns={'PARESHARRIGH': 'EquityPC'})
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def TotalInvestedCap(tp_derivation, factor_derivation, dependencies=['TOTIC']):
        """
        :name: 全部投入资本(MRQ)
        :desc: 全部投入资本(MRQ)
        :param dependencies:
        :param tp_derivation:
        :param factor_derivation:
        :return:
        """
        management = tp_derivation.loc[:, dependencies]
        if len(management) <=0:
            return None
        management = management.rename(columns={'TOTIC': 'TotalInvestedCap'})
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def TotalAssets(tp_derivation, factor_derivation, dependencies=['TOTASSET']):
        """
        :name: 资产总计(MRQ)
        :desc: 资产总计(MRQ) balance
        :param dependencies:
        :param tp_derivation:
        :param factor_derivation:
        :return:
        """
        management = tp_derivation.loc[:, dependencies]
        if len(management) <=0:
            return None
        management = management.rename(columns={'TOTASSET': 'TotalAssets'})
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def TotalFixedAssets(tp_derivation, factor_derivation, dependencies=['FIXEDASSECLEATOT']):
        """
        :name: 固定资产合计(MRQ)
        :desc: 固定资产合计(MRQ)balance
        :param dependencies:
        :param tp_derivation:
        :param factor_derivation:
        :return:
        """
        management = tp_derivation.loc[:, dependencies]
        if len(management) <=0:
            return None
        management = management.rename(columns={'FIXEDASSECLEATOT': 'TotalFixedAssets'})
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def TotalLib(tp_derivation, factor_derivation, dependencies=['TOTLIAB']):
        """
        :name: 负债合计(MRQ)
        :desc: 负债合计(MRQ)balance
        :param dependencies:
        :param tp_derivation:
        :param factor_derivation:
        :return:
        """
        management = tp_derivation.loc[:, dependencies]
        if len(management) <=0:
            return None
        management = management.rename(columns={'TOTLIAB': 'TotalLib'})
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def ShEquity(tp_derivation, factor_derivation, dependencies=['RIGHAGGR']):
        """
        :name: 股东权益(MRQ)
        :desc: 股东权益(MRQ) balance
        :param dependencies:
        :param tp_derivation:
        :param factor_derivation:
        :return:
        """
        management = tp_derivation.loc[:, dependencies]
        if len(management) <=0:
            return None
        management = management.rename(columns={'RIGHAGGR': 'ShEquity'})
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def CashAndCashEqu(tp_derivation, factor_derivation, dependencies=['FINALCASHBALA']):
        """
        :name: 期末现金及现金等价物(MRQ)
        :desc: 期末现金及现金等价物(MRQ) cashflow
        :param dependencies:
        :param tp_derivation:
        :param factor_derivation:
        :return:
        """
        management = tp_derivation.loc[:, dependencies]
        if len(management) <=0:
            return None
        management = management.rename(columns={'FINALCASHBALA': 'CashAndCashEqu'})
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def EBIAT(tp_derivation, factor_derivation, dependencies=['EBIT',
                                                                                               'INCOTAXEXPE',
                                                                                               ]):
        """
        :name: 息前税后利润(MRQ)
        :desc: 息前税后利润 = 息税前利润－息税前利润所得税。 息税前利润所得税 = 全部所得税－利息净损益所得税
        :param dependencies:
        :param tp_derivation:
        :param factor_derivation:
        :return:
        """
        management = tp_derivation.loc[:, dependencies]
        if len(management) <=0:
            return None
        func = lambda x: x[0] - x[1] if x[0] is not None and x[1] is not None else None
        management['EBIAT'] = management[dependencies].apply(func, axis=1)
        management = management.drop(dependencies, axis=1)
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def SalesTTM(tp_derivation, factor_derivation, dependencies=['BIZTOTINCO']):
        """
        :name: 营业总收入(TTM)
        :desc: 营业总收入(TTM) income
        :param dependencies:
        :param tp_derivation:
        :param factor_derivation:
        :return:
        """
        management = tp_derivation.loc[:, dependencies]
        if len(management) <=0:
            return None
        management = management.rename(columns={'BIZTOTINCO': 'SalesTTM'})
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def TotalOptCostTTM(tp_derivation, factor_derivation, dependencies=['BIZTOTCOST']):
        """
        :name: 营业总成本(TTM)
        :desc: 营业总成本(TTM) income
        :param dependencies:
        :param tp_derivation:
        :param factor_derivation:
        :return:
        """
        management = tp_derivation.loc[:, dependencies]
        if len(management) <=0:
            return None
        management = management.rename(columns={'BIZTOTCOST': 'TotalOptCostTTM'})
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def OptIncTTM(tp_derivation, factor_derivation, dependencies=['BIZINCO']):
        """
        :name: 营业收入(TTM)
        :desc: 营业收入(TTM) income
        :param dependencies:
        :param tp_derivation:
        :param factor_derivation:
        :return:
        """
        management = tp_derivation.loc[:, dependencies]
        if len(management) <=0:
            return None
        management = management.rename(columns={'BIZINCO': 'OptIncTTM'})
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def GrossMarginTTM(tp_derivation, factor_derivation, dependencies=['OPGPMARGIN']):
        """
        :name: 毛利(TTM) 营业毛利润
        :desc: 毛利(TTM) 营业毛利润 indicator
        :param dependencies:
        :param tp_derivation:
        :param factor_derivation:
        :return:
        """
        management = tp_derivation.loc[:, dependencies]
        if len(management) <=0:
            return None
        management = management.rename(columns={'OPGPMARGIN': 'GrossMarginTTM'})
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def SalesExpensesTTM(tp_derivation, factor_derivation, dependencies=['SALESEXPE']):
        """
        :name: 销售费用(TTM)
        :desc: 销售费用(TTM) income
        :param dependencies:
        :param tp_derivation:
        :param factor_derivation:
        :return:
        """
        management = tp_derivation.loc[:, dependencies]
        if len(management) <=0:
            return None
        management = management.rename(columns={'SALESEXPE': 'SalesExpensesTTM'})
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def AdmFeeTTM(tp_derivation, factor_derivation, dependencies=['MANAEXPE']):
        """
        :name: 管理费用(TTM)
        :desc: 管理费用(TTM) income
        :param dependencies:
        :param tp_derivation:
        :param factor_derivation:
        :return:
        """
        management = tp_derivation.loc[:, dependencies]
        if len(management) <=0:
            return None
        management = management.rename(columns={'MANAEXPE': 'AdmFeeTTM'})
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def FinFeeTTM(tp_derivation, factor_derivation, dependencies=['FINEXPE']):
        """
        :name: 财务费用(TTM)
        :desc: 财务费用(TTM) income
        :param dependencies:
        :param tp_derivation:
        :param factor_derivation:
        :return:
        """
        management = tp_derivation.loc[:, dependencies]
        if len(management) <=0:
            return None
        management = management.rename(columns={'FINEXPE': 'FinFeeTTM'})
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def PerFeeTTM(tp_derivation, factor_derivation, dependencies=['SALESEXPE',
                                                                   'MANAEXPE',
                                                                   'FINEXPE',
                                                                   ]):
        """
        :name: 期间费用(TTM)
        :desc: 期间费用(TTM) income
        :param dependencies:
        :param tp_derivation:
        :param factor_derivation:
        :return:
        """
        management = tp_derivation.loc[:, dependencies]
        if len(management) <=0:
            return None
        func = lambda x: x[0] + x[1] + x[2]
        management['PerFeeTTM'] = management[dependencies].apply(func, axis=1)
        management = management.drop(dependencies, axis=1)
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def InterestExpTTM(tp_derivation, factor_derivation, dependencies=['INTEEXPE']):
        """
        :name: 利息支出(TTM)
        :desc: 利息支出(TTM) income
        :param dependencies:
        :param tp_derivation:
        :param factor_derivation:
        :return:
        """
        management = tp_derivation.loc[:, dependencies]
        if len(management) <=0:
            return None
        management = management.rename(columns={'INTEEXPE': 'InterestExpTTM'})
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def MinorInterestTTM(tp_derivation, factor_derivation, dependencies=['minority_profit']):
        """
        :name: 少数股东损益(TTM)
        :desc: 少数股东损益(TTM) income
        :param dependencies:
        :param tp_derivation:
        :param factor_derivation:
        :return:
        """
        management = tp_derivation.loc[:, dependencies]
        if len(management) <=0:
            return None
        management = management.rename(columns={'minority_profit': 'MinorInterestTTM'})
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def AssetImpLossTTM(tp_derivation, factor_derivation, dependencies=['ASSEIMPALOSS']):
        """
        :name: 资产减值损失(TTM)
        :desc: 资产减值损失(TTM) income
        :param dependencies:
        :param tp_derivation:
        :param factor_derivation:
        :return:
        """
        management = tp_derivation.loc[:, dependencies]
        if len(management) <=0:
            return None
        management = management.rename(columns={'ASSEIMPALOSS': 'AssetImpLossTTM'})
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def NetIncFromOptActTTM(tp_derivation, factor_derivation, dependencies=['MANANETR']):
        """
        :name: 经营活动净收益(TTM)
        :desc: 经营活动净收益(TTM) cashflow
        :param dependencies:
        :param tp_derivation:
        :param factor_derivation:
        :return:
        """
        management = tp_derivation.loc[:, dependencies]
        if len(management) <=0:
            return None
        management = management.rename(columns={'MANANETR': 'NetIncFromOptActTTM'})
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def NetIncFromValueChgTTM(tp_derivation, factor_derivation, dependencies=['NVALCHGIT']):
        """
        :name: 价值变动净收益(TTM)
        :desc: 价值变动净收益(TTM)
        :param dependencies:
        :param tp_derivation:
        :param factor_derivation:
        :return:
        """
        management = tp_derivation.loc[:, dependencies]
        if len(management) <= 0:
            return None
        management = management.rename(columns={'NVALCHGIT': 'NetIncFromValueChgTTM'})
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def OptProfitTTM(tp_derivation, factor_derivation, dependencies=['PERPROFIT']):
        """
        :name: 营业利润(TTM) income
        :desc: 营业利润(TTM) income
        :param dependencies:
        :param tp_derivation:
        :param factor_derivation:
        :return:
        """
        management = tp_derivation.loc[:, dependencies]
        if len(management) <=0:
            return None
        management = management.rename(columns={'PERPROFIT': 'OptProfitTTM'})
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def NetNonOptIncAndExpTTM(tp_derivation, factor_derivation, dependencies=['NONOREVE',
                                                                                                 'NONOEXPE',]):
        """
        :name: 营业外收支净额(TTM)
        :desc: 营业外收支净额(TTM)
        :param dependencies:
        :param tp_derivation:
        :param factor_derivation:
        :return:
        """
        management = tp_derivation.loc[:, dependencies]
        if len(management) <=0:
            return None
        func = lambda x: x[0] - x[1]
        management['NetNonOptIncAndExpTTM'] = management[dependencies].apply(func, axis=1)

        management = management.drop(dependencies, axis=1)
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def EBITTTM(tp_derivation, factor_derivation, dependencies=['EBIT']):
        """
        :name: 息税前利润(TTM)
        :desc: 息税前利润(TTM)
        :param dependencies:
        :param tp_derivation:
        :param factor_derivation:
        :return:
        """
        management = tp_derivation.loc[:, dependencies]

        if len(management) <=0:
            return None
        management = management.rename(columns={'EBIT': 'EBITTTM'})
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def IncTaxTTM(tp_derivation, factor_derivation, dependencies=['INCOTAXEXPE']):
        """
        :name: 所得税(TTM)
        :desc:
        :param dependencies:
        :param tp_derivation:
        :param factor_derivation:
        :return:
        """
        management = tp_derivation.loc[:, dependencies]
        if len(management) <= 0:
            return None
        management = management.rename(columns={'INCOTAXEXPE': 'IncTaxTTM'})
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def TotalProfTTM(tp_derivation, factor_derivation, dependencies=['TOTPROFIT']):
        """
        :name: 利润总额(TTM)
        :desc: 利润总额(TTM) income
        :param dependencies:
        :param tp_derivation:
        :param factor_derivation:
        :return:
        """
        management = tp_derivation.loc[:, dependencies]
        if len(management) <=0:
            return None
        management = management.rename(columns={'TOTPROFIT': 'TotalProfTTM'})
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def NetIncTTM(tp_derivation, factor_derivation, dependencies=['NETPROFIT']):
        """
        :name: 净利润(TTM)
        :desc: 净利润(TTM)
        :param dependencies:
        :param tp_derivation:
        :param factor_derivation:
        :return:
        """
        management = tp_derivation.loc[:, dependencies]
        if len(management) <=0:
            return None
        management = management.rename(columns={'NETPROFIT': 'NetIncTTM'})
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def NetProfToPSTTM(tp_derivation, factor_derivation, dependencies=['PARENETP']):
        """
        :name: 归属母公司股东的净利润(TTM)
        :desc: 归属母公司股东的净利润(TTM) income
        :param dependencies:
        :param tp_derivation:
        :param factor_derivation:
        :return:
        """
        management = tp_derivation.loc[:, dependencies]
        if len(management) <=0:
            return None
        management = management.rename(columns={'PARENETP': 'NetProfToPSTTM'})
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def NetProfAfterNonRecGainsAndLossTTM(tp_derivation, factor_derivation, dependencies=['NPCUT']):
        """
        :name: 可出非经常性损益后的净利润(TTM)
        :desc: 可出非经常性损益后的净利润(TTM)
        :param dependencies:
        :param tp_derivation:
        :param factor_derivation:
        :return:
        """
        management = tp_derivation.loc[:, dependencies]
        if len(management) <=0:
            return None
        management = management.rename(columns={'NPCUT': 'NetProfAfterNonRecGainsAndLossTTM'})
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def EBITFORPTTM(tp_derivation, factor_derivation, dependencies=['EBITFORP']):
        """
        :name: ebit(TTM)
        :desc: ebit(TTM)
        :param dependencies:
        :param tp_derivation:
        :param factor_derivation:
        :return:
        """
        management = tp_derivation.loc[:, dependencies]
        if len(management) <=0:
            return None
        management = management.rename(columns={'EBITFORP': 'EBITFORPTTM'})
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def EBITDATTM(tp_derivation, factor_derivation, dependencies=['EBITDA']):
        """
        :name: EBITDA(TTM)
        :desc: EBITDA(TTM)
        :param dependencies:
        :param tp_derivation:
        :param factor_derivation:
        :return:
        """
        management = tp_derivation.loc[:, dependencies]
        if len(management) <=0:
            return None
        management = management.rename(columns={'EBITDA': 'EBITDATTM'})
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def CashRecForSGAndPSTTM(tp_derivation, factor_derivation, dependencies=['LABORGETCASH']):
        """
        :name: 销售商品提供劳务收到的现金(TTM)
        :desc: 销售商品提供劳务收到的现金(TTM) cashflow
        :param dependencies:
        :param tp_derivation:
        :param factor_derivation:
        :return:
        """
        management = tp_derivation.loc[:, dependencies]
        if len(management) <=0:
            return None
        management = management.rename(columns={'LABORGETCASH': 'CashRecForSGAndPSTTM'})
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def NCFOTTM(tp_derivation, factor_derivation, dependencies=['MANANETR']):
        """
        :name: 经营活动现金净流量(TTM)
        :desc: 经营活动现金净流量(TTM)cashflow
        :param dependencies:
        :param tp_derivation:
        :param factor_derivation:
        :return:
        """
        management = tp_derivation.loc[:, dependencies]
        if len(management) <=0:
            return None
        management = management.rename(columns={'MANANETR': 'NCFOTTM'})
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def NetCashFlowFromInvActTTM(tp_derivation, factor_derivation, dependencies=['INVNETCASHFLOW']):
        """
        :name: 投资活动现金净流量(TTM)
        :desc: 投资活动现金净流量(TTM)cashflow
        :param dependencies:
        :param tp_derivation:
        :param factor_derivation:
        :return:
        """
        management = tp_derivation.loc[:, dependencies]
        if len(management) <=0:
            return None
        management = management.rename(columns={'INVNETCASHFLOW': 'NetCashFlowFromInvActTTM'})
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def NetCashFlowFromFundActTTM(tp_derivation, factor_derivation, dependencies=['FINNETCFLOW']):
        """
        :name: 筹资活动现金净流量(TTM)
        :desc: 筹资活动现金净流量(TTM)cashflow
        :param dependencies:
        :param tp_derivation:
        :param factor_derivation:
        :return:
        """
        management = tp_derivation.loc[:, dependencies]
        if len(management) <=0:
            return None
        management = management.rename(columns={'FINNETCFLOW': 'NetCashFlowFromFundActTTM'})
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def NetCashFlowTTM(tp_derivation, factor_derivation, dependencies=['CASHNETI']):
        """
        :name:现金净流量(TTM)
        :desc:现金净流量(TTM) calshflow
        :param dependencies:
        :param tp_derivation:
        :param factor_derivation:
        :return:
        """
        management = tp_derivation.loc[:, dependencies]
        if len(management) <=0:
            return None
        management = management.rename(columns={'CASHNETI': 'NetCashFlowTTM'})
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def BusTaxAndSuchgTTM(tp_derivation, factor_derivation, dependencies=['BIZTAX']):
        """

        :name: 营业税金及附加(TTM)
        :desc: 营业税金及附加(TTM) income
        :param dependencies:
        :param tp_derivation:
        :param factor_derivation:
        :return:
        """
        management = tp_derivation.loc[:, dependencies]
        if len(management) <=0:
            return None
        management = management.rename(columns={'BIZTAX': 'BusTaxAndSuchgTTM'})
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation
