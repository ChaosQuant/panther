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
import json
import numpy as np
import pandas as pd
from basic_derivation.factor_base import FactorBase
from pandas.io.json import json_normalize
from basic_derivation.utillities.calc_tools import CalcTools

# from basic_derivation import app
# from ultron.cluster.invoke.cache_data import cache_data
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


class Derivation(FactorBase):
    """
    资本结构
    """
    def __init__(self, name):
        super(Derivation, self).__init__(name)

    def create_dest_tables(self):
        """
        创建数据库表
        :return:
        """
        drop_sql = """drop table if exists `{0}`""".format(self._name)
        create_sql = """create table `{0}`(
                    `id` INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
                    `security_code` varchar(32) NOT NULL,
                    `trade_date` date NOT NULL,
                    `FCFF` decimal(19,4),
                    `FCFE` decimal(19,4),
                    `NonRecGainLoss` decimal(19,4),
                    `NetOptInc` decimal(19,4),
                    `WorkingCap` decimal(19,4),
                    `TangibleAssets` decimal(19,4),
                    `RetainedEarnings` decimal(19,4),
                    `InterestBearingLiabilities` decimal(19,4),
                    `NetDebt` decimal(19,4),
                    `InterestFreeCurLb` decimal(19,4),
                    `InterestFreeNonCurLb` decimal(19,4),
                    `EBIAT` decimal(19,4),
                    `DepAndAmo` decimal(19,4),
                    `EquityPC` decimal(19,4),
                    `TotalInvestedCap` decimal(19,4),
                    `TotalAssets` decimal(19,4),
                    `TotalFixedAssets` decimal(19,4),
                    `TotalLib` decimal(19,4),
                    `ShEquity` decimal(19,4),
                    `CashAndCashEqu` decimal(19,4),
                    `SalesTTM` decimal(19,4),
                    `TotalOptCostTTM` decimal(19,4),
                    `OptIncTTM` decimal(19,4),
                    `GrossMarginTTM` decimal(19,4),
                    `SalesExpensesTTM` decimal(19,4),
                    `AdmFeeTTM` decimal(19,4),
                    `FinFeeTTM` decimal(19,4),
                    `PerFeeTTM` decimal(19,4),
                    `InterestExpTTM` decimal(19,4),
                    `MinorInterestTTM` decimal(19,4),
                    `AssetImpLossTTM` decimal(19,4),
                    `NetIncFromOptActTTM` decimal(19,4),
                    `NetIncFromValueChgTTM` decimal(19,4),
                    `OptProfitTTM` decimal(19,4),
                    `NetNonOptIncAndExpTTM` decimal(19,4),
                    `EBITTTM` decimal(19,4),
                    `IncTaxTTM` decimal(19,4),
                    `TotalProfTTM` decimal(19,4),
                    `NetIncTTM` decimal(19,4),
                    `NetProfToPSTTM` decimal(19,4),
                    `NetProfAfterNonRecGainsAndLossTTM` decimal(19,4),
                    `EBITFORPTTM` decimal(19,4),
                    `EBITDATTM` decimal(19,4),
                    `CashRecForSGAndPSTTM` decimal(19,4),
                    `NCFOTTM` decimal(19,4),
                    `NetCashFlowFromInvActTTM` decimal(19,4),
                    `NetCashFlowFromFundActTTM` decimal(19,4),
                    `NetCashFlowTTM` decimal(19,4),
                    `BusTaxAndSuchgTTM` decimal(19,4),
                    constraint {0}_uindex
                    unique (`trade_date`,`security_code`)
                    )ENGINE=InnoDB DEFAULT CHARSET=utf8;""".format(self._name)
        super(Derivation, self)._create_tables(create_sql, drop_sql)

    @staticmethod
    def free_cash_flow_to_firm(tp_derivation, factor_derivation, dependencies=['FCFF']):
        """
        企业自由现金流量(MRQ)
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
    def free_cash_flow_to_equity(tp_derivation, factor_derivation, dependencies=['FCFE']):
        """
        股东自由现金流量(MRQ)
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
    def non_recurring_gains_and_losses(tp_derivation, factor_derivation, dependencies=['NEGAL']):
        """
        非经常性损益(MRQ)
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
    def net_operating_income(tp_derivation, factor_derivation, dependencies=['NOPI']):
        """
        经营活动净收益(MRQ) indicator
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
    def working_capital(tp_derivation, factor_derivation, dependencies=['WORKCAP']):
        """
        运营资本(MRQ)
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
    def tangible_assets(tp_derivation, factor_derivation, dependencies=['RIGHAGGR',
                                                                        'MINYSHARRIGH',
                                                                        'INTAASSET',
                                                                        'DEVEEXPE',
                                                                        'GOODWILL',
                                                                        'LOGPREPEXPE',
                                                                        'DEFETAXASSET']):
        """
        有形资产(MRQ)
        股东权益（不含少数股东权益）-无形资产+开发支出+商誉+长期待摊费用+递延所得税资产）
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
    def retained_earnings(tp_derivation, factor_derivation, dependencies=['RETAINEDEAR']):
        """
        留存收益(MRQ)
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
    def interest_bearing_liabilities(tp_derivation, factor_derivation, dependencies=['shortterm_loan',
                                                                                     'non_current_liability_in_one_year',
                                                                                     'longterm_loan',
                                                                                     'bonds_payable',
                                                                                     'interest_payable']):
        """
        带息负债(MRQ) balance
        带息负债 = 短期借款+一年内到期的长期负债+长期借款+应付债券+应付利息
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
    def net_debt(tp_derivation, factor_derivation, dependencies=['NDEBT']):
        """
        净债务(MRQ)
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
    def interest_free_current_liabilities(tp_derivation, factor_derivation, dependencies=['NONINTCURLIABS']):
        """
        无息流动负债(MRQ)
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
    def interest_free_non_current_liabilities(tp_derivation, factor_derivation, dependencies=['NONINTNONCURLIAB']):
        """
        无息非流动负债(MRQ)
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
    def depreciation_and_amortization(tp_derivation, factor_derivation, dependencies=['CURDEPANDAMOR']):
        """
        折旧和摊销(MRQ)
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
    def equity_to_shareholders_of_parent_company(tp_derivation, factor_derivation, dependencies=['PARESHARRIGH']):
        """
        归属于母公司的股东权益(MRQ) balance
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
    def total_invested_capital(tp_derivation, factor_derivation, dependencies=['TOTIC']):
        """
        全部投入资本(MRQ)
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
    def total_assets(tp_derivation, factor_derivation, dependencies=['TOTASSET']):
        """
        资产总计(MRQ) balance
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
    def total_fixed_assets(tp_derivation, factor_derivation, dependencies=['FIXEDASSECLEATOT']):
        """
        固定资产合计(MRQ)balance
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
    def total_liabilities(tp_derivation, factor_derivation, dependencies=['TOTLIAB']):
        """
        负债合计(MRQ)balance
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
    def shareholders_equity(tp_derivation, factor_derivation, dependencies=['RIGHAGGR']):
        """
        股东权益(MRQ) balance
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
    def cash_and_cash_equivalents(tp_derivation, factor_derivation, dependencies=['FINALCASHBALA']):
        """
        期末现金及现金等价物(MRQ) cashflow
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
    def earnings_before_interest_and_after_tax(tp_derivation, factor_derivation, dependencies=['EBIT',
                                                                                               'INCOTAXEXPE',
                                                                                               ]):
        """
        息前税后利润(MRQ)
        息前税后利润 = 息税前利润－息税前利润所得税
        息税前利润所得税 = 全部所得税－利息净损益所得税
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
    def sales(tp_derivation, factor_derivation, dependencies=['BIZTOTINCO']):
        """
        营业总收入(TTM) income
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
    def total_operating_cost(tp_derivation, factor_derivation, dependencies=['BIZTOTCOST']):
        """
        营业总成本(TTM) income
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
    def operating_income(tp_derivation, factor_derivation, dependencies=['BIZINCO']):
        """
        营业收入(TTM) income
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
    def gross_margin(tp_derivation, factor_derivation, dependencies=['OPGPMARGIN']):
        """
        毛利(TTM) 营业毛利润 indicator
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
    def sales_expenses(tp_derivation, factor_derivation, dependencies=['SALESEXPE']):
        """
        销售费用(TTM) income
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
    def administration_fee(tp_derivation, factor_derivation, dependencies=['MANAEXPE']):
        """
        管理费用(TTM) income
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
    def financial_expenses(tp_derivation, factor_derivation, dependencies=['FINEXPE']):
        """
        财务费用(TTM) income
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
    def period_fee(tp_derivation, factor_derivation, dependencies=['SALESEXPE',
                                                                   'MANAEXPE',
                                                                   'FINEXPE',
                                                                   ]):
        """
        期间费用(TTM) income
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
    def interest_expense(tp_derivation, factor_derivation, dependencies=['INTEEXPE']):
        """
        利息支出(TTM) income
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
    def minority_interest(tp_derivation, factor_derivation, dependencies=['minority_profit']):
        """
        少数股东损益(TTM) income
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
    def asset_impairment_loss(tp_derivation, factor_derivation, dependencies=['ASSEIMPALOSS']):
        """
        资产减值损失(TTM) income
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
    def net_income_from_operating_activities(tp_derivation, factor_derivation, dependencies=['MANANETR']):
        """
        经营活动净收益(TTM) cashflow
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
    def net_income_from_value_changes(tp_derivation, factor_derivation, dependencies=['NVALCHGIT']):
        """
        价值变动净收益(TTM)
        :param dependencies:
        :param tp_derivation:
        :param factor_derivation:
        :return:
        """
        management = tp_derivation.loc[:, dependencies]
        if len(management) <=0:
            return None
        management = management.rename(columns={'NVALCHGIT': 'NetIncFromValueChgTTM'})
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def operating_profit(tp_derivation, factor_derivation, dependencies=['PERPROFIT']):
        """
        营业利润(TTM) income
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
    def net_non_operating_income_and_expenditure(tp_derivation, factor_derivation, dependencies=['NONOREVE',
                                                                                                 'NONOEXPE',]):
        """
        营业外收支净额(TTM)
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
    def earnings_before_interest_and_tax(tp_derivation, factor_derivation, dependencies=['EBIT']):
        """
        息税前利润(TTM)
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
    def income_tax(tp_derivation, factor_derivation, dependencies=['INCOTAXEXPE']):
        """
        所得税(TTM)
        :param dependencies:
        :param tp_derivation:
        :param factor_derivation:
        :return:
        """
        management = tp_derivation.loc[:, dependencies]
        if len(management) <=0:
            return None
        management = management.rename(columns={'INCOTAXEXPE': 'IncTaxTTM'})
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def total_profit(tp_derivation, factor_derivation, dependencies=['TOTPROFIT']):
        """
        利润总额(TTM) income
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
    def net_income(tp_derivation, factor_derivation, dependencies=['NETPROFIT']):
        """
        净利润(TTM)
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
    def net_profit_attributable_to_the_shareholders_of_parent_company(tp_derivation, factor_derivation, dependencies=['PARENETP']):
        """
        归属母公司股东的净利润(TTM) income
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
    def net_profit_after_non_recurring_gains_and_losses(tp_derivation, factor_derivation, dependencies=['NPCUT']):
        """
        可出非经常性损益后的净利润(TTM)
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
    def ebit(tp_derivation, factor_derivation, dependencies=['EBITFORP']):
        """
        ebit(TTM)
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
    def ebitda(tp_derivation, factor_derivation, dependencies=['EBITDA']):
        """
        EBITDA(TTM)
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
    def cash_received_for_selling_goods(tp_derivation, factor_derivation, dependencies=['LABORGETCASH']):
        """
        销售商品提供劳务收到的现金(TTM) cashflow
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
    def net_cash_flow_from_operating_activities(tp_derivation, factor_derivation, dependencies=['MANANETR']):
        """
        经营活动现金净流量(TTM)cashflow
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
    def net_cash_flow_from_investment_activities(tp_derivation, factor_derivation, dependencies=['INVNETCASHFLOW']):
        """
        投资活动现金净流量(TTM)cashflow
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
    def net_cash_flow_from_financing_activities(tp_derivation, factor_derivation, dependencies=['FINNETCFLOW']):
        """
        筹资活动现金净流量(TTM)cashflow
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
    def net_cash_flow(tp_derivation, factor_derivation, dependencies=['CASHNETI']):
        """
        现金净流量(TTM) calshflow
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
    def business_taxes_and_surcharges(tp_derivation, factor_derivation, dependencies=['BIZTAX']):
        """
        营业税金及附加(TTM) income
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


def calculate(trade_date, tp_derivation, ttm_derivation, factor_name):  # 计算对应因子
    tp_derivation = tp_derivation.set_index('security_code')
    ttm_derivation = ttm_derivation.set_index('security_code')

    # 读取目前涉及到的因子
    derivation = Derivation(factor_name)  # 注意, 这里的name要与client中新建table时的name一致, 不然回报错
    # 因子计算
    factor_derivation = pd.DataFrame()
    factor_derivation['security_code'] = tp_derivation.index
    factor_derivation = factor_derivation.set_index('security_code')

    factor_derivation = derivation.free_cash_flow_to_firm(tp_derivation, factor_derivation)
    factor_derivation = derivation.free_cash_flow_to_equity(tp_derivation, factor_derivation)
    factor_derivation = derivation.non_recurring_gains_and_losses(tp_derivation, factor_derivation)
    factor_derivation = derivation.net_operating_income(tp_derivation, factor_derivation)
    factor_derivation = derivation.working_capital(tp_derivation, factor_derivation)
    factor_derivation = derivation.tangible_assets(tp_derivation, factor_derivation)

    factor_derivation = derivation.retained_earnings(tp_derivation, factor_derivation)
    factor_derivation = derivation.interest_bearing_liabilities(tp_derivation, factor_derivation)
    factor_derivation = derivation.net_debt(tp_derivation, factor_derivation)
    factor_derivation = derivation.interest_free_current_liabilities(tp_derivation, factor_derivation)
    factor_derivation = derivation.interest_free_non_current_liabilities(tp_derivation, factor_derivation)

    factor_derivation = derivation.earnings_before_interest_and_after_tax(tp_derivation, factor_derivation)

    factor_derivation = derivation.depreciation_and_amortization(tp_derivation, factor_derivation)
    factor_derivation = derivation.equity_to_shareholders_of_parent_company(tp_derivation, factor_derivation)
    factor_derivation = derivation.total_invested_capital(tp_derivation, factor_derivation)
    factor_derivation = derivation.total_assets(tp_derivation, factor_derivation)
    factor_derivation = derivation.total_fixed_assets(tp_derivation, factor_derivation)
    factor_derivation = derivation.total_liabilities(tp_derivation, factor_derivation)
    factor_derivation = derivation.shareholders_equity(tp_derivation, factor_derivation)
    factor_derivation = derivation.cash_and_cash_equivalents(tp_derivation, factor_derivation)
    factor_derivation = derivation.sales(ttm_derivation, factor_derivation)
    factor_derivation = derivation.total_operating_cost(ttm_derivation, factor_derivation)
    factor_derivation = derivation.operating_income(ttm_derivation, factor_derivation)
    factor_derivation = derivation.gross_margin(ttm_derivation, factor_derivation)
    factor_derivation = derivation.sales_expenses(ttm_derivation, factor_derivation)
    factor_derivation = derivation.administration_fee(ttm_derivation, factor_derivation)
    factor_derivation = derivation.financial_expenses(ttm_derivation, factor_derivation)
    factor_derivation = derivation.period_fee(ttm_derivation, factor_derivation)
    factor_derivation = derivation.interest_expense(ttm_derivation, factor_derivation)
    factor_derivation = derivation.minority_interest(ttm_derivation, factor_derivation)

    factor_derivation = derivation.asset_impairment_loss(ttm_derivation, factor_derivation)
    factor_derivation = derivation.net_income_from_operating_activities(ttm_derivation, factor_derivation)
    factor_derivation = derivation.net_income_from_value_changes(ttm_derivation, factor_derivation)

    factor_derivation = derivation.operating_profit(ttm_derivation, factor_derivation)
    factor_derivation = derivation.net_non_operating_income_and_expenditure(ttm_derivation, factor_derivation)

    factor_derivation = derivation.earnings_before_interest_and_tax(ttm_derivation, factor_derivation)

    factor_derivation = derivation.income_tax(ttm_derivation, factor_derivation)

    factor_derivation = derivation.total_profit(ttm_derivation, factor_derivation)
    factor_derivation = derivation.net_income(ttm_derivation, factor_derivation)
    factor_derivation = derivation.net_profit_attributable_to_the_shareholders_of_parent_company(ttm_derivation, factor_derivation)
    factor_derivation = derivation.net_profit_after_non_recurring_gains_and_losses(ttm_derivation, factor_derivation)
    factor_derivation = derivation.ebitda(ttm_derivation, factor_derivation)

    factor_derivation = derivation.cash_received_for_selling_goods(ttm_derivation, factor_derivation)
    factor_derivation = derivation.net_cash_flow_from_operating_activities(ttm_derivation, factor_derivation)
    factor_derivation = derivation.net_cash_flow_from_investment_activities(ttm_derivation, factor_derivation)
    factor_derivation = derivation.net_cash_flow_from_financing_activities(ttm_derivation, factor_derivation)
    factor_derivation = derivation.net_cash_flow(ttm_derivation, factor_derivation)
    factor_derivation = derivation.business_taxes_and_surcharges(ttm_derivation, factor_derivation)

    factor_derivation = factor_derivation.reset_index()
    factor_derivation['trade_date'] = str(trade_date)
    print('len_factor_derivation: %s' % len(factor_derivation))
    derivation._storage_data(factor_derivation, trade_date)
    del derivation, factor_derivation
    gc.collect()


# @app.task()
def factor_calculate(**kwargs):
    print("management_kwargs: {}".format(kwargs))
    date_index = kwargs['date_index']
    session = kwargs['session']
    factor_name = kwargs['factor_name']
    content1 = cache_data.get_cache(session + str(date_index) + "1", date_index)
    content2 = cache_data.get_cache(session + str(date_index) + "2", date_index)
    tp_derivation = json_normalize(json.loads(str(content1, encoding='utf8')))
    ttm_derivation = json_normalize(json.loads(str(content2, encoding='utf8')))
    tp_derivation.set_index('security_code', inplace=True)
    ttm_derivation.set_index('security_code', inplace=True)
    print("len_tp_management_data {}".format(len(tp_derivation)))
    print("len_ttm_management_data {}".format(len(ttm_derivation)))
    # total_cash_flow_data = {'tp_management': tp_derivation, 'ttm_management': ttm_derivation}
    calculate(date_index, tp_derivation, ttm_derivation, factor_name)
