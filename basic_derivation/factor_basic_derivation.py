#! /usr/bin/env python
# -*- coding: utf-8 -*-

"""
@version:
@author: li
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
from pandas.io.json import json_normalize
from utilities.singleton import Singleton

# from basic_derivation import app
# from ultron.cluster.invoke.cache_data import cache_data
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


@six.add_metaclass(Singleton)
class FactorBasicDerivation(object):
    """
    基础衍生类因子
    """
    def __init__(self):
        __str__ = 'factor_basic_derivation'
        self.name = '基础衍生'
        self.factor_type1 = '基础衍生'
        self.factor_type2 = '基础衍生'
        self.description = '基础衍生类因子'


    @staticmethod
    def FCFF(tp_derivation, factor_derivation, dependencies=['FCFF']):
        """
        :name: 企业自由现金流量(MRQ)
        :desc: 息前税后利润+折旧与摊销-营运资本增加-资本支出 = 息税前利润(1-所得税率)+ 折旧与摊销-营运资本增加-构建固定无形和长期资产支付的现金
        :unit: 元
        :view_dimension: 10000
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
        :desc: 企业自由现金流量-偿还债务所支付的现金+取得借款收到的现金+发行债券所收到的现金（MRQ)
        :unit: 元
        :view_dimension: 10000
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
        :unit: 元
        :view_dimension: 10000
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
        :desc: "旧准则：主营业务利润+其他业务利润-营业费用-管理费用-财务费用 新准则（一般企业）：营业总收入-营业总成本新准则（银行）：手续费及佣金净收入+利息净收入+其他业务净收益-营业支出新准则（证券）：手续费及佣金净收入+利息净收入+其他业务收入-营业支出新准则（保险）：营业收入-营业支出-（公允价值变动净收益+投资争收益+汇兑净收益）"
        :unit: 元
        :view_dimension: 10000
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
        :desc:  流动资产（MRQ）-流动负债（MRQ）
        :unit: 元
        :view_dimension: 10000
        """
        management = tp_derivation.loc[:, dependencies]
        if len(management) <= 0:
            return None
        management = management.rename(columns={'WORKCAP': 'WorkingCap'})
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    # @staticmethod
    # def TangibleAssets(tp_derivation, factor_derivation, dependencies=['RIGHAGGR',
    #                                                                    'MINYSHARRIGH',
    #                                                                    'INTAASSET',
    #                                                                    'DEVEEXPE',
    #                                                                    'GOODWILL',
    #                                                                    'LOGPREPEXPE',
    #                                                                    'DEFETAXASSET']):
    #     """
    #     缺少开发支出
    #     :name: 有形资产(MRQ)
    #     :desc: 股东权益（不含少数股东权益）-无形资产+开发支出+商誉+长期待摊费用+递延所得税资产）
    #     """
    #     management = tp_derivation.loc[:, dependencies]
    #     if len(management) <=0:
    #         return None
    #     func = lambda x: x[0] - x[1] - x[2] + x[3] + x[4] + x[5] + x[6]
    #     management['TangibleAssets'] = management[dependencies].apply(func, axis=1)
    #
    #     management = management.drop(dependencies, axis=1)
    #     factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
    #     return factor_derivation

    @staticmethod
    def RetainedEarnings(tp_derivation, factor_derivation, dependencies=['RETAINEDEAR']):
        """
        :name: 留存收益(MRQ)
        :desc: 盈余公积+未分配利润（MRQ）
        :unit: 元
        :view_dimension: 10000
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
        :unit: 元
        :view_dimension: 10000
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
        :desc: 带息债务-货币资金（MRQ）
        :unit: 元
        :view_dimension: 10000
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
        :desc: 应付帐款+预收款项+应付职工薪酬+应交税费+其他应付款+预提费用+递延收益.流动负债+其他流动负债
        :unit: 元
        :view_dimension: 10000
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
        :desc: 旧准则：长期负债合计-长期借款-应付债券 新准则：非流动负债合计-长期借款-应付债券
        :unit: 元
        :view_dimension: 10000
        """
        management = tp_derivation.loc[:, dependencies]
        if len(management) <=0:
            return None
        management = management.rename(columns={'NONINTNONCURLIAB': 'InterestFreeNonCurLb'})
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    # @staticmethod
    # def DepAndAmo(tp_derivation, factor_derivation, dependencies=['CURDEPANDAMOR']):
    #     """
    #     缺少折旧和摊销
    #     :name: 折旧和摊销(MRQ)
    #     :desc: 折旧和摊销(MRQ)
    #     """
    #     management = tp_derivation.loc[:, dependencies]
    #     if len(management) <=0:
    #         return None
    #     management = management.rename(columns={'CURDEPANDAMOR': 'DepAndAmo'})
    #     factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
    #     return factor_derivation

    @staticmethod
    def EquityPC(tp_derivation, factor_derivation, dependencies=['PARESHARRIGH']):
        """
        :name: 归属于母公司的股东权益(MRQ)
        :desc: 归属于母公司的股东权益(MRQ) balance
        :unit: 元
        :view_dimension: 10000
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
        :unit: 元
        :view_dimension: 10000
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
        :unit: 元
        :view_dimension: 10000
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
        :unit: 元
        :view_dimension: 10000
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
        :unit: 元
        :view_dimension: 10000
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
        :unit: 元
        :view_dimension: 10000
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
        :unit: 元
        :view_dimension: 10000
        """
        management = tp_derivation.loc[:, dependencies]
        if len(management) <=0:
            return None
        management = management.rename(columns={'FINALCASHBALA': 'CashAndCashEqu'})
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def EBIAT(tp_derivation, factor_derivation, dependencies=['EBIT',
                                                              'INCOTAXEXPE']):
        """
        :name: 息前税后利润(MRQ)
        :desc: 息前税后利润 = 息税前利润－息税前利润所得税。 息税前利润所得税 = 全部所得税－利息净损益所得税 利润总额+财务费用
        :unit: 元
        :view_dimension: 10000
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
        :desc: 根据截止指定日已披露的最新报告期“营业总收入”计算：（1）最新报告期是年报。则TTM=年报；（2）最新报告期不是年报，Q则TTM=本期+（上年年报-上年同期合并数），如果上年年报非空，本期、上年同期台并数存在空值，则返回上年年报。
        :unit: 元
        :view_dimension: 10000
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
        :desc: 根据截止指定日已披露的最新报告期“营业总成本”计算：（1）最新报告期是年报。则TTM=年报；（2）最新报告期不是年报，Q则TTM=本期+（上年年报-上年同期合并数），如果上年年报非空，本期、上年同期台并数存在空值，则返回上年年报。
        :unit: 元
        :view_dimension: 10000
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
        :desc: 根据截止指定日已披露的最新报告期“营业收入”计算：（1）最新报告期是年报。则TTM=年报；（2）最新报告期不是年报，Q则TTM=本期+（上年年报-上年同期合并数），如果上年年报非空，本期、上年同期台并数存在空值，则返回上年年报。
        :unit: 元
        :view_dimension: 10000
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
        :desc: 根据截止指定日已披露的最新报告期“毛利”计算：（1）最新报告期是年报。则TTM=年报；（2）最新报告期不是年报，Q则TTM=本期+（上年年报-上年同期合并数），如果上年年报非空，本期、上年同期台并数存在空值，则返回上年年报。
        :unit: 元
        :view_dimension: 10000
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
        :desc: 根据截止指定日已披露的最新报告期“销售费用”计算：（1）最新报告期是年报。则TTM=年报；（2）最新报告期不是年报，Q则TTM=本期+（上年年报-上年同期合并数），如果上年年报非空，本期、上年同期台并数存在空值，则返回上年年报。
        :unit: 元
        :view_dimension: 10000
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
        :desc: 根据截止指定日已披露的最新报告期“管理费用”计算：（1）最新报告期是年报。则TTM=年报；（2）最新报告期不是年报，Q则TTM=本期+（上年年报-上年同期合并数），如果上年年报非空，本期、上年同期台并数存在空值，则返回上年年报。
        :unit: 元
        :view_dimension: 10000
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
        :desc: 根据截止指定日已披露的最新报告期“财务费用”计算：（1）最新报告期是年报。则TTM=年报；（2）最新报告期不是年报，Q则TTM=本期+（上年年报-上年同期合并数），如果上年年报非空，本期、上年同期台并数存在空值，则返回上年年报。
        :unit: 元
        :view_dimension: 10000
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
        :desc: 根据截止指定日已披露的最新报告期“期间费用”计算：（1）最新报告期是年报。则TTM=年报；（2）最新报告期不是年报，Q则TTM=本期+（上年年报-上年同期合并数），如果上年年报非空，本期、上年同期台并数存在空值，则返回上年年报。
        :unit: 元
        :view_dimension: 10000
        """
        management = tp_derivation.loc[:, dependencies]
        if len(management) <=0:
            return None
        func = lambda x: x[0] + x[1] + x[2] if x[0] is not None and x[1] is not None and x[2] is not None else None
        management['PerFeeTTM'] = management[dependencies].apply(func, axis=1)
        management = management.drop(dependencies, axis=1)
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    # @staticmethod
    # def InterestExpTTM(tp_derivation, factor_derivation, dependencies=['INTEEXPE']):
    #     """
    #     缺少利息支出
    #     :name: 利息支出(TTM)
    #     :desc: 根据截止指定日已披露的最新报告期“利息支出”计算：（1）最新报告期是年报。则TTM=年报；（2）最新报告期不是年报，Q则TTM=本期+（上年年报-上年同期合并数），如果上年年报非空，本期、上年同期台并数存在空值，则返回上年年报。
    #     """
    #     management = tp_derivation.loc[:, dependencies]
    #     if len(management) <=0:
    #         return None
    #     management = management.rename(columns={'INTEEXPE': 'InterestExpTTM'})
    #     factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
    #     return factor_derivation

    @staticmethod
    def MinorInterestTTM(tp_derivation, factor_derivation, dependencies=['minority_profit']):
        """
        :name: 少数股东损益(TTM)
        :desc: 根据截止指定日已披露的最新报告期“少数股东损益”计算：（1）最新报告期是年报。则TTM=年报；（2）最新报告期不是年报，Q则TTM=本期+（上年年报-上年同期合并数），如果上年年报非空，本期、上年同期台并数存在空值，则返回上年年报。
        :unit: 元
        :view_dimension: 10000
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
        :desc: 根据截止指定日已披露的最新报告期“资产减值损失”计算：（1）最新报告期是年报。则TTM=年报；（2）最新报告期不是年报，Q则TTM=本期+（上年年报-上年同期合并数），如果上年年报非空，本期、上年同期台并数存在空值，则返回上年年报。
        :unit: 元
        :view_dimension: 10000
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
        :desc: 根据截止指定日已披露的最新报告期“经营活动净收益”计算：（1）最新报告期是年报。则TTM=年报；（2）最新报告期不是年报，Q则TTM=本期+（上年年报-上年同期合并数），如果上年年报非空，本期、上年同期台并数存在空值，则返回上年年报。
        :unit: 元
        :view_dimension: 10000
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
        :desc: 根据截止指定日已披露的最新报告期“价值变动净收益”计算：（1）最新报告期是年报。则TTM=年报；（2）最新报告期不是年报，Q则TTM=本期+（上年年报-上年同期合并数），如果上年年报非空，本期、上年同期台并数存在空值，则返回上年年报。
        :unit: 元
        :view_dimension: 10000
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
        :desc: 根据截止指定日已披露的最新报告期“营业利润”计算：（1）最新报告期是年报。则TTM=年报；（2）最新报告期不是年报，Q则TTM=本期+（上年年报-上年同期合并数），如果上年年报非空，本期、上年同期台并数存在空值，则返回上年年报。
        :unit: 元
        :view_dimension: 10000
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
        :desc: 根据截止指定日已披露的最新报告期“营业外收支净额”计算：（1）最新报告期是年报。则TTM=年报；（2）最新报告期不是年报，Q则TTM=本期+（上年年报-上年同期合并数），如果上年年报非空，本期、上年同期台并数存在空值，则返回上年年报。
        :unit: 元
        :view_dimension: 10000
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
        :desc: 根据截止指定日已披露的最新报告期“EBIT(反推法）”计算：（1）最新报告期是年报。则TTM=年报；（2）最新报告期不是年报，Q则TTM=本期+（上年年报-上年同期合并数），如果上年年报非空，本期、上年同期台并数存在空值，则返回上年年报。
        :unit: 元
        :view_dimension: 10000
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
        :desc:根据截止指定日已披露的最新报告期“所得税”计算：（1）最新报告期是年报。则TTM=年报；（2）最新报告期不是年报，Q则TTM=本期+（上年年报-上年同期合并数），如果上年年报非空，本期、上年同期台并数存在空值，则返回上年年报。
        :unit: 元
        :view_dimension: 10000
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
        :desc: 根据截止指定日已披露的最新报告期“利润总额”计算：（1）最新报告期是年报。则TTM=年报；（2）最新报告期不是年报，Q则TTM=本期+（上年年报-上年同期合并数），如果上年年报非空，本期、上年同期台并数存在空值，则返回上年年报。
        :unit: 元
        :view_dimension: 10000
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
        :desc: 根据截止指定日已披露的最新报告期“净利润（含少数股东权益）”计算：（1）最新报告期是年报。则TTM=年报；（2）最新报告期不是年报，Q则TTM=本期+（上年年报-上年同期合并数），如果上年年报非空，本期、上年同期台并数存在空值，则返回上年年报。
        :unit: 元
        :view_dimension: 10000
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
        :desc: 根据截止指定日已披露的最新报告期“归属母公司股东的净利润”计算：（1）最新报告期是年报。则TTM=年报；（2）最新报告期不是年报，Q则TTM=本期+（上年年报-上年同期合并数），如果上年年报非空，本期、上年同期台并数存在空值，则返回上年年报。注：交易日匹配财报数据披露日，业绩快报数据不参与计算
        :unit: 元
        :view_dimension: 10000
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
        :name: 扣除非经常性损益后的净利润(TTM)
        :desc: 根据截止指定日已披露的最新报告期“扣除非经常性损益后的净利润”计算：（1）最新报告期是年报。则TTM=年报；（2）最新报告期不是年报，Q则TTM=本期+（上年年报-上年同期合并数），如果上年年报非空，本期、上年同期台并数存在空值，则返回上年年报。
        :unit: 元
        :view_dimension: 10000
        """
        management = tp_derivation.loc[:, dependencies]
        if len(management) <=0:
            return None
        management = management.rename(columns={'NPCUT': 'NetProfAfterNonRecGainsAndLossTTM'})
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    def EBITFORPTTM(tp_derivation, factor_derivation, dependencies=['TOTPROFIT',
                                                                    'FINEXPE']):
        """
        :name: EBIT(TTM)
        :desc: EBIT=利润总额+财务费用, 根据截止指定日已披露的最新报告期“EBIT(正向）”计算：（1）最新报告期是年报。则TTM=年报；（2）最新报告期不是年报，Q则TTM=本期+（上年年报-上年同期合并数），如果上年年报非空，本期、上年同期台并数存在空值，则返回上年年报。
        :unit: 元
        :view_dimension: 10000
        """
        management = tp_derivation.loc[:, dependencies]
        if len(management) <=0:
            return None
        func = lambda x: x[0] + x[1] if x[0] is not None and x[1] is not None else None
        management['EBITFORPTTM'] = management[dependencies].apply(func, axis=1)
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def EBITDATTM(tp_derivation, factor_derivation, dependencies=['EBITDA']):
        """
        :name: EBITDA(TTM)
        :desc: 根据截止指定日已披露的最新报告期“EBITDA(正向）”计算：（1）最新报告期是年报。则TTM=年报；（2）最新报告期不是年报，Q则TTM=本期+（上年年报-上年同期合并数），如果上年年报非空，本期、上年同期台并数存在空值，则返回上年年报。
        :unit: 元
        :view_dimension: 10000
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
        :desc: 根据截止指定日已披露的最新报告期“销售商品提供劳务收到的现金”计算：（1）最新报告期是年报。则TTM=年报；（2）最新报告期不是年报，Q则TTM=本期+（上年年报-上年同期合并数），如果上年年报非空，本期、上年同期台并数存在空值，则返回上年年报。
        :unit: 元
        :view_dimension: 10000
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
        :desc: 根据截止指定日已披露的最新报告期“经营活动现金净流量”计算：（1）最新报告期是年报。则TTM=年报；（2）最新报告期不是年报，Q则TTM=本期+（上年年报-上年同期合并数），如果上年年报非空，本期、上年同期台并数存在空值，则返回上年年报。
        :unit: 元
        :view_dimension: 10000
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
        :desc: 根据截止指定日已披露的最新报告期“投资活动现金净流量”计算：（1）最新报告期是年报。则TTM=年报；（2）最新报告期不是年报，Q则TTM=本期+（上年年报-上年同期合并数），如果上年年报非空，本期、上年同期台并数存在空值，则返回上年年报。
        :unit: 元
        :view_dimension: 10000
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
        :desc: 根据截止指定日已披露的最新报告期“筹资活动现金净流量”计算：（1）最新报告期是年报。则TTM=年报；（2）最新报告期不是年报，Q则TTM=本期+（上年年报-上年同期合并数），如果上年年报非空，本期、上年同期台并数存在空值，则返回上年年报。
        :unit: 元
        :view_dimension: 10000
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
        :name: 现金净流量(TTM)
        :desc: 根据截止指定日已披露的最新报告期“现金及现金等价物净增加额”计算：（1）最新报告期是年报。则TTM=年报；（2）最新报告期不是年报，Q则TTM=本期+（上年年报-上年同期合并数），如果上年年报非空，本期、上年同期台并数存在空值，则返回上年年报。
        :unit: 元
        :view_dimension: 10000
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
        :desc: 根据截止指定日已披露的最新报告期“营业税金及附加”计算：（1）最新报告期是年报。则TTM=年报；（2）最新报告期不是年报，Q则TTM=本期+（上年年报-上年同期合并数），如果上年年报非空，本期、上年同期台并数存在空值，则返回上年年报。
        :unit: 元
        :view_dimension: 10000
        """
        management = tp_derivation.loc[:, dependencies]
        if len(management) <=0:
            return None
        management = management.rename(columns={'BIZTAX': 'BusTaxAndSuchgTTM'})
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation
