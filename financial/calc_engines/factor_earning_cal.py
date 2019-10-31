# -*- coding: utf-8 -*-

import pdb,importlib,inspect,time,datetime,json
# from PyFin.api import advanceDateByCalendar
# from data.polymerize import DBPolymerize
from data.storage_engine import StorageEngine
import time
import pandas as pd
import numpy as np
from datetime import timedelta, datetime
from financial import factor_earning

from data.model import BalanceMRQ, BalanceTTM, BalanceReport
from data.model import CashFlowTTM, CashFlowReport
from data.model import IndicatorReport
from data.model import IncomeReport, IncomeTTM

from vision.db.signletion_engine import *
from data.sqlengine import sqlEngine
# pd.set_option('display.max_columns', None)
# pd.set_option('display.max_rows', None)
# from ultron.cluster.invoke.cache_data import cache_data


class CalcEngine(object):
    def __init__(self, name, url, methods=[{'packet':'financial.factor_earning','class':'FactorEarning'},]):
        self._name = name
        self._methods = methods
        self._url = url

    def get_trade_date(self, trade_date, n, days=365):
        """
        获取当前时间前n年的时间点，且为交易日，如果非交易日，则往前提取最近的一天。
        :param days:
        :param trade_date: 当前交易日
        :param n:
        :return:
        """
        syn_util = SyncUtil()
        trade_date_sets = syn_util.get_all_trades('001002', '19900101', trade_date)
        trade_date_sets = trade_date_sets['TRADEDATE'].values

        time_array = datetime.strptime(str(trade_date), "%Y%m%d")
        time_array = time_array - timedelta(days=days) * n
        date_time = int(datetime.strftime(time_array, "%Y%m%d"))
        if str(date_time) < min(trade_date_sets):
            # print('date_time %s is out of trade_date_sets' % date_time)
            return str(date_time)
        else:
            while str(date_time) not in trade_date_sets:
                date_time = date_time - 1
            # print('trade_date pre %s year %s' % (n, date_time))
            return str(date_time)

    def _func_sets(self, method):
        # 私有函数和保护函数过滤
        return list(filter(lambda x: not x.startswith('_') and callable(getattr(method,x)), dir(method)))

    def loading_data(self, trade_date):
        """
        获取基础数据
        按天获取当天交易日所有股票的基础数据
        :param trade_date: 交易日
        :return:
        """
        # 转换时间格式
        time_array = datetime.strptime(trade_date, "%Y-%m-%d")
        trade_date = datetime.strftime(time_array, '%Y%m%d')
        # 读取目前涉及到的因子
        trade_date_pre_year = self.get_trade_date(trade_date, 1)
        trade_date_pre_year_2 = self.get_trade_date(trade_date, 2)
        trade_date_pre_year_3 = self.get_trade_date(trade_date, 3)
        trade_date_pre_year_4 = self.get_trade_date(trade_date, 4)
        trade_date_pre_year_5 = self.get_trade_date(trade_date, 5)

        engine = sqlEngine()
        columns = ['COMPCODE', 'PUBLISHDATE', 'ENDDATE', 'symbol', 'company_id', 'trade_date']

        # Report Data
        cash_flow_sets = engine.fetch_fundamentals_pit_extend_company_id(CashFlowReport,
                                                                         [CashFlowReport.LABORGETCASH,
                                                                          CashFlowReport.FINALCASHBALA,
                                                                          ], dates=[trade_date])
        for column in columns:
            if column in list(cash_flow_sets.keys()):
                cash_flow_sets = cash_flow_sets.drop(column, axis=1)
        cash_flow_sets = cash_flow_sets.rename(
            columns={'LABORGETCASH': 'goods_sale_and_service_render_cash',  # 销售商品、提供劳务收到的现金
                     'FINALCASHBALA': 'cash_and_equivalents_at_end',  # 期末现金及现金等价物余额
                     })

        income_sets = engine.fetch_fundamentals_pit_extend_company_id(IncomeReport,
                                                                      [IncomeReport.BIZTOTINCO,
                                                                       IncomeReport.BIZINCO,
                                                                       IncomeReport.PERPROFIT,
                                                                       IncomeReport.PARENETP,
                                                                       IncomeReport.NETPROFIT,
                                                                       ], dates=[trade_date])
        for column in columns:
            if column in list(income_sets.keys()):
                income_sets = income_sets.drop(column, axis=1)
        income_sets = income_sets.rename(columns={'NETPROFIT': 'net_profit',  # 净利润
                                                  'BIZTOTINCO': 'total_operating_revenue',  # 营业总收入
                                                  'BIZINCO': 'operating_revenue',  # 营业收入
                                                  'PERPROFIT': 'operating_profit',  # 营业利润
                                                  'PARENETP': 'np_parent_company_owners',  # 归属于母公司所有者的净利润
                                                  })

        indicator_sets = engine.fetch_fundamentals_pit_extend_company_id(IndicatorReport,
                                                                         [
                                                                             IndicatorReport.NETPROFITCUT,
                                                                             # 扣除非经常损益后的净利润
                                                                             IndicatorReport.MGTEXPRT
                                                                         ], dates=[trade_date])
        for column in columns:
            if column in list(indicator_sets.keys()):
                indicator_sets = indicator_sets.drop(column, axis=1)
        indicator_sets = indicator_sets.rename(columns={'NETPROFITCUT': 'adjusted_profit',  # 扣除非经常损益后的净利润
                                                        })

        balance_sets = engine.fetch_fundamentals_pit_extend_company_id(BalanceReport,
                                                                       [BalanceReport.PARESHARRIGH,
                                                                        ], dates=[trade_date])
        for column in columns:
            if column in list(balance_sets.keys()):
                balance_sets = balance_sets.drop(column, axis=1)
        balance_sets = balance_sets.rename(columns={'PARESHARRIGH': 'equities_parent_company_owners',  # 归属于母公司股东权益合计
                                                    })

        income_sets_pre_year_1 = engine.fetch_fundamentals_pit_extend_company_id(IncomeReport,
                                                                                 [IncomeReport.BIZINCO,  # 营业收入
                                                                                  IncomeReport.NETPROFIT,  # 净利润
                                                                                  ], dates=[trade_date_pre_year])
        for column in columns:
            if column in list(income_sets_pre_year_1.keys()):
                income_sets_pre_year_1 = income_sets_pre_year_1.drop(column, axis=1)
        income_sets_pre_year_1 = income_sets_pre_year_1.rename(columns={'NETPROFIT': 'net_profit_pre_year_1',  # 净利润
                                                                        'BIZINCO': 'operating_revenue_pre_year_1',
                                                                        # 营业收入
                                                                        })

        income_sets_pre_year_2 = engine.fetch_fundamentals_pit_extend_company_id(IncomeReport,
                                                                                 [IncomeReport.BIZINCO,
                                                                                  IncomeReport.NETPROFIT,
                                                                                  ], dates=[trade_date_pre_year_2])
        for column in columns:
            if column in list(income_sets_pre_year_2.keys()):
                income_sets_pre_year_2 = income_sets_pre_year_2.drop(column, axis=1)
        income_sets_pre_year_2 = income_sets_pre_year_2.rename(columns={'NETPROFIT': 'net_profit_pre_year_2',  # 净利润
                                                                        'BIZINCO': 'operating_revenue_pre_year_2',
                                                                        # 营业收入
                                                                        })

        income_sets_pre_year_3 = engine.fetch_fundamentals_pit_extend_company_id(IncomeReport,
                                                                                 [IncomeReport.BIZINCO,
                                                                                  IncomeReport.NETPROFIT,
                                                                                  ], dates=[trade_date_pre_year_3])
        for column in columns:
            if column in list(income_sets_pre_year_3.keys()):
                income_sets_pre_year_3 = income_sets_pre_year_3.drop(column, axis=1)
        income_sets_pre_year_3 = income_sets_pre_year_3.rename(columns={'NETPROFIT': 'net_profit_pre_year_3',  # 净利润
                                                                        'BIZINCO': 'operating_revenue_pre_year_3',
                                                                        # 营业收入
                                                                        })

        income_sets_pre_year_4 = engine.fetch_fundamentals_pit_extend_company_id(IncomeReport,
                                                                                 [IncomeReport.BIZINCO,
                                                                                  IncomeReport.NETPROFIT,
                                                                                  ], dates=[trade_date_pre_year_4])
        for column in columns:
            if column in list(income_sets_pre_year_4.keys()):
                income_sets_pre_year_4 = income_sets_pre_year_4.drop(column, axis=1)
        income_sets_pre_year_4 = income_sets_pre_year_4.rename(columns={'NETPROFIT': 'net_profit_pre_year_4',  # 净利润
                                                                        'BIZINCO': 'operating_revenue_pre_year_4',
                                                                        # 营业收入
                                                                        })

        tp_earning = pd.merge(cash_flow_sets, income_sets, how='outer', on='security_code')
        tp_earning = pd.merge(indicator_sets, tp_earning, how='outer', on='security_code')
        tp_earning = pd.merge(balance_sets, tp_earning, how='outer', on='security_code')
        tp_earning = pd.merge(income_sets_pre_year_1, tp_earning, how='outer', on='security_code')
        tp_earning = pd.merge(income_sets_pre_year_2, tp_earning, how='outer', on='security_code')
        tp_earning = pd.merge(income_sets_pre_year_3, tp_earning, how='outer', on='security_code')
        tp_earning = pd.merge(income_sets_pre_year_4, tp_earning, how='outer', on='security_code')

        # MRQ
        balance_mrq_sets = engine.fetch_fundamentals_pit_extend_company_id(BalanceMRQ,
                                                                           [BalanceMRQ.TOTASSET,  # 资产总计
                                                                            BalanceMRQ.PARESHARRIGH,  # 归属于母公司股东权益合计
                                                                            BalanceMRQ.RIGHAGGR,  # 所有者权益（或股东权益）合计
                                                                            BalanceMRQ.LONGBORR,  # 长期借款
                                                                            ], dates=[trade_date])
        for column in columns:
            if column in list(balance_mrq_sets.keys()):
                balance_mrq_sets = balance_mrq_sets.drop(column, axis=1)
        balance_mrq_sets = balance_mrq_sets.rename(columns={'TOTASSET': 'total_assets_mrq',
                                                            'PARESHARRIGH': 'equities_parent_company_owners_mrq',
                                                            # 归属于母公司股东权益合计
                                                            'RIGHAGGR': 'total_owner_equities_mrq',  # 所有者权益（或股东权益）合计
                                                            'LONGBORR': 'longterm_loan_mrq',  # 长期借款
                                                            })

        balance_mrq_sets_pre = engine.fetch_fundamentals_pit_extend_company_id(BalanceMRQ,
                                                                               [BalanceMRQ.TOTASSET,  # 资产总计
                                                                                BalanceMRQ.RIGHAGGR,  # 所有者权益(或股东权益)合计
                                                                                BalanceMRQ.LONGBORR,  # 长期借款
                                                                                ], dates=[trade_date])
        for column in columns:
            if column in list(balance_mrq_sets_pre.keys()):
                balance_mrq_sets_pre = balance_mrq_sets_pre.drop(column, axis=1)
        balance_mrq_sets_pre = balance_mrq_sets_pre.rename(columns={'TOTASSET': 'total_assets_mrq_pre',
                                                                    'RIGHAGGR': 'total_owner_equities_mrq_pre',
                                                                    # 所有者权益（或股东权益）合计
                                                                    'LONGBORR': 'longterm_loan_mrq_pre',  # 长期借款
                                                                    })

        # TTM Data
        cash_flow_ttm_sets = engine.fetch_fundamentals_pit_extend_company_id(CashFlowTTM,
                                                                             [CashFlowTTM.FINNETCFLOW,
                                                                              ], dates=[trade_date])
        for column in columns:
            if column in list(cash_flow_ttm_sets.keys()):
                cash_flow_ttm_sets = cash_flow_ttm_sets.drop(column, axis=1)
        cash_flow_ttm_sets = cash_flow_ttm_sets.rename(columns={'FINNETCFLOW': 'net_finance_cash_flow'})

        income_ttm_sets = engine.fetch_fundamentals_pit_extend_company_id(IncomeTTM,
                                                                          [IncomeTTM.BIZINCO,  # 营业收入
                                                                           IncomeTTM.NETPROFIT,  # 净利润
                                                                           IncomeTTM.MANAEXPE,  # 管理费用
                                                                           IncomeTTM.BIZTOTINCO,  # 营业总收入
                                                                           IncomeTTM.TOTPROFIT,  # 利润总额
                                                                           IncomeTTM.FINEXPE,  # 财务费用
                                                                           # IncomeTTM.INTEINCO,  # 利息收入
                                                                           IncomeTTM.SALESEXPE,  # 销售费用
                                                                           IncomeTTM.BIZTOTCOST,  # 营业总成本
                                                                           IncomeTTM.PERPROFIT,  # 营业利润
                                                                           IncomeTTM.PARENETP,  # 归属于母公司所有者的净利润
                                                                           IncomeTTM.BIZCOST,  # 营业成本
                                                                           # IncomeTTM.ASSOINVEPROF,  # 对联营企业和合营企业的投资收益
                                                                           IncomeTTM.BIZTAX,  # 营业税金及附加
                                                                           IncomeTTM.ASSEIMPALOSS,  # 资产减值损失
                                                                           ], dates=[trade_date])
        for column in columns:
            if column in list(income_ttm_sets.keys()):
                income_ttm_sets = income_ttm_sets.drop(column, axis=1)
        income_ttm_sets = income_ttm_sets.rename(columns={'BIZINCO': 'operating_revenue',  # 营业收入
                                                          'NETPROFIT': 'net_profit',  # 净利润
                                                          'MANAEXPE': 'administration_expense',  # 管理费用
                                                          'BIZTOTINCO': 'total_operating_revenue',  # 营业总收入
                                                          'TOTPROFIT': 'total_profit',  # 利润总额
                                                          'FINEXPE': 'financial_expense',  # 财务费用
                                                          # 'INTEINCO': 'interest_income',  # 利息收入
                                                          'SALESEXPE': 'sale_expense',  # 销售费用
                                                          'BIZTOTCOST': 'total_operating_cost',  # 营业总成本
                                                          'PERPROFIT': 'operating_profit',  # 营业利润
                                                          'PARENETP': 'np_parent_company_owners',  # 归属于母公司所有者的净利润
                                                          'BIZCOST': 'operating_cost',  # 营业成本
                                                          # 'ASSOINVEPROF': 'invest_income_associates',  # 对联营企业和合营企业的投资收益
                                                          'BIZTAX': 'operating_tax_surcharges',  # 营业税金及附加
                                                          'ASSEIMPALOSS': 'asset_impairment_loss',  # 资产减值损失
                                                          })

        balance_ttm_sets = engine.fetch_fundamentals_pit_extend_company_id(BalanceTTM,
                                                                           [BalanceTTM.TOTASSET,  # 资产总计
                                                                            BalanceTTM.RIGHAGGR,  # 所有者权益（或股东权益）合计
                                                                            BalanceTTM.PARESHARRIGH,  # 归属于母公司股东权益合计
                                                                            ], dates=[trade_date])
        for column in columns:
            if column in list(balance_ttm_sets.keys()):
                balance_ttm_sets = balance_ttm_sets.drop(column, axis=1)
        balance_ttm_sets = balance_ttm_sets.rename(
            columns={'PARESHARRIGH': 'equities_parent_company_owners',  # 归属于母公司股东权益合计
                     'RIGHAGGR': 'total_owner_equities',  # 所有者权益（或股东权益）合计
                     'TOTASSET': 'total_assets',  # 资产总计
                     })

        income_ttm_sets_pre_year_1 = engine.fetch_fundamentals_pit_extend_company_id(IncomeTTM,
                                                                                     [IncomeTTM.BIZINCO,
                                                                                      IncomeTTM.NETPROFIT,
                                                                                      ], dates=[trade_date_pre_year])
        for column in columns:
            if column in list(income_ttm_sets_pre_year_1.keys()):
                income_ttm_sets_pre_year_1 = income_ttm_sets_pre_year_1.drop(column, axis=1)
        income_ttm_sets_pre_year_1 = income_ttm_sets_pre_year_1.rename(
            columns={'BIZINCO': 'operating_revenue_pre_year_1',  # 营业收入
                     'NETPROFIT': 'net_profit_pre_year_1',  # 净利润
                     })

        income_ttm_sets_pre_year_2 = engine.fetch_fundamentals_pit_extend_company_id(IncomeTTM,
                                                                                     [IncomeTTM.BIZINCO,
                                                                                      IncomeTTM.NETPROFIT,
                                                                                      ], dates=[trade_date_pre_year_2])
        for column in columns:
            if column in list(income_ttm_sets_pre_year_2.keys()):
                income_ttm_sets_pre_year_2 = income_ttm_sets_pre_year_2.drop(column, axis=1)
        income_ttm_sets_pre_year_2 = income_ttm_sets_pre_year_2.rename(
            columns={'BIZINCO': 'operating_revenue_pre_year_2',  # 营业收入
                     'NETPROFIT': 'net_profit_pre_year_2',  # 净利润
                     })

        income_ttm_sets_pre_year_3 = engine.fetch_fundamentals_pit_extend_company_id(IncomeTTM,
                                                                                     [IncomeTTM.BIZINCO,
                                                                                      IncomeTTM.NETPROFIT,
                                                                                      ], dates=[trade_date_pre_year_3])
        for column in columns:
            if column in list(income_ttm_sets_pre_year_3.keys()):
                income_ttm_sets_pre_year_3 = income_ttm_sets_pre_year_3.drop(column, axis=1)
        income_ttm_sets_pre_year_3 = income_ttm_sets_pre_year_3.rename(
            columns={'BIZINCO': 'operating_revenue_pre_year_3',  # 营业收入
                     'NETPROFIT': 'net_profit_pre_year_3',  # 净利润
                     })

        income_ttm_sets_pre_year_4 = engine.fetch_fundamentals_pit_extend_company_id(IncomeTTM,
                                                                                     [IncomeTTM.BIZINCO,
                                                                                      IncomeTTM.NETPROFIT,
                                                                                      ], dates=[trade_date_pre_year_4])
        for column in columns:
            if column in list(income_ttm_sets_pre_year_4.keys()):
                income_ttm_sets_pre_year_4 = income_ttm_sets_pre_year_4.drop(column, axis=1)
        income_ttm_sets_pre_year_4 = income_ttm_sets_pre_year_4.rename(
            columns={'BIZINCO': 'operating_revenue_pre_year_4',  # 营业收入
                     'NETPROFIT': 'net_profit_pre_year_4',  # 净利润
                     })

        # indicator_ttm_sets = engine.fetch_fundamentals_pit_extend_company_id(IndicatorTTM,
        #                                                                      [IndicatorTTM.ROIC,   # 投入资本回报率
        #                                                                       ], dates=[trade_date]).drop(columns, axis=1)
        #
        # indicator_ttm_sets = indicator_ttm_sets.rename(columns={'ROIC': '',
        #                                                         })

        ttm_earning = pd.merge(income_ttm_sets, balance_ttm_sets, how='outer', on='security_code')
        ttm_earning = pd.merge(ttm_earning, cash_flow_ttm_sets, how='outer', on='security_code')
        ttm_earning = pd.merge(ttm_earning, income_ttm_sets_pre_year_1, how='outer', on='security_code')
        ttm_earning = pd.merge(ttm_earning, income_ttm_sets_pre_year_2, how='outer', on='security_code')
        ttm_earning = pd.merge(ttm_earning, income_ttm_sets_pre_year_3, how='outer', on='security_code')
        ttm_earning = pd.merge(ttm_earning, income_ttm_sets_pre_year_4, how='outer', on='security_code')
        ttm_earning = pd.merge(ttm_earning, balance_mrq_sets, how='outer', on='security_code')
        ttm_earning = pd.merge(ttm_earning, balance_mrq_sets_pre, how='outer', on='security_code')

        balance_con_sets = engine.fetch_fundamentals_pit_extend_company_id(BalanceTTM,
                                                                           [BalanceTTM.TOTASSET,  # 资产总计
                                                                            BalanceTTM.RIGHAGGR,  # 所有者权益（或股东权益）合计
                                                                            ],
                                                                           dates=[trade_date,
                                                                                  trade_date_pre_year,
                                                                                  trade_date_pre_year_2,
                                                                                  trade_date_pre_year_3,
                                                                                  trade_date_pre_year_4,
                                                                                  ])
        for column in columns:
            if column in list(balance_con_sets.keys()):
                balance_con_sets = balance_con_sets.drop(column, axis=1)
        balance_con_sets = balance_con_sets.groupby(['security_code'])
        balance_con_sets = balance_con_sets.sum()
        balance_con_sets = balance_con_sets.rename(columns={'TOTASSET': 'total_assets',
                                                            'RIGHAGGR': 'total_owner_equities'})

        # cash_flow_con_sets = engine.fetch_fundamentals_pit_extend_company_id(CashFlowReport,
        #                                                                      [CashFlowReport.FINALCASHBALA,
        #                                                                   ],
        #                                                                  dates=[trade_date,
        #                                                                         trade_date_pre_year,
        #                                                                         trade_date_pre_year_2,
        #                                                                         trade_date_pre_year_3,
        #                                                                         trade_date_pre_year_4,
        #                                                                         trade_date_pre_year_5,
        #                                                                         ]).drop(columns, axis=1)
        # cash_flow_con_sets = cash_flow_con_sets.groupby(['security_code'])
        # cash_flow_con_sets = cash_flow_con_sets.sum()
        # cash_flow_con_sets = cash_flow_con_sets.rename(columns={'FINALCASHBALA':'cash_and_equivalents_at_end'})

        income_con_sets = engine.fetch_fundamentals_pit_extend_company_id(IncomeReport,
                                                                          [IncomeReport.NETPROFIT,
                                                                           ],
                                                                          dates=[trade_date,
                                                                                 trade_date_pre_year,
                                                                                 trade_date_pre_year_2,
                                                                                 trade_date_pre_year_3,
                                                                                 trade_date_pre_year_4,
                                                                                 trade_date_pre_year_5,
                                                                                 ])
        for column in columns:
            if column in list(income_con_sets.keys()):
                income_con_sets = income_con_sets.drop(column, axis=1)
        income_con_sets = income_con_sets.groupby(['security_code'])
        income_con_sets = income_con_sets.sum()
        income_con_sets = income_con_sets.rename(columns={'NETPROFIT': 'net_profit'}).reset_index()
        ttm_earning_5y = pd.merge(balance_con_sets, income_con_sets, how='outer', on='security_code')

        return tp_earning, ttm_earning, ttm_earning_5y

    def process_calc_factor(self, trade_date, tp_earning, ttm_earning, ttm_earning_5y):
        tp_earning = tp_earning.set_index('security_code')
        ttm_earning = ttm_earning.set_index('security_code')
        ttm_earning_5y = ttm_earning_5y.set_index('security_code')
        earning = factor_earning.FactorEarning()

        # 因子计算
        earning_sets = pd.DataFrame()
        earning_sets['security_code'] = tp_earning.index
        earning_sets = earning_sets.set_index('security_code')
        earning_sets = earning.ROA5YChg(ttm_earning_5y, earning_sets)
        earning_sets = earning.ROE5Y(ttm_earning_5y, earning_sets)
        earning_sets = earning.NPCutToNP(tp_earning, earning_sets)
        earning_sets = earning.ROE(tp_earning, earning_sets)
        earning_sets = earning.ROEAvg(tp_earning, earning_sets)
        earning_sets = earning.ROEcut(tp_earning, earning_sets)
        # factor_earning = earning.invest_r_associates_to_tp_latest(tp_earning, earning_sets)
        earning_sets = earning.NetPft5YAvgChgTTM(ttm_earning, earning_sets)
        earning_sets = earning.Sales5YChgTTM(ttm_earning, earning_sets)
        # factor_earning = earning.roa(ttm_earning, earning_sets)
        earning_sets = earning.AdminExpTTM(ttm_earning, earning_sets)
        earning_sets = earning.BerryRtTTM(ttm_earning, earning_sets)
        earning_sets = earning.CFARatioMinusROATTM(ttm_earning, earning_sets)
        earning_sets = earning.SalesCostTTM(ttm_earning, earning_sets)
        # earning_sets = earning.EBITToTORevTTM(ttm_earning, earning_sets)
        earning_sets = earning.PeridCostTTM(ttm_earning, earning_sets)
        earning_sets = earning.FinExpTTM(ttm_earning, earning_sets)
        earning_sets = earning.ImpLossToTOITTM(ttm_earning, earning_sets)
        earning_sets = earning.OIAToOITTM(ttm_earning, earning_sets)
        earning_sets = earning.ROAexTTM(ttm_earning, earning_sets)
        earning_sets = earning.NetNonOToTP(ttm_earning, earning_sets)
        earning_sets = earning.NetProfitRtTTM(ttm_earning, earning_sets)
        earning_sets = earning.NPToTORevTTM(ttm_earning, earning_sets)
        earning_sets = earning.OperExpRtTTM(ttm_earning, earning_sets)
        earning_sets = earning.OptProfitRtTTM(ttm_earning, earning_sets)
        # factor_earning = earning.operating_profit_to_tor(ttm_earning, earning_sets)
        earning_sets = earning.ROCTTM(ttm_earning, earning_sets)
        # earning_sets = earning.ROTATTM(ttm_earning, earning_sets)
        earning_sets = earning.ROETTM(ttm_earning, earning_sets)
        earning_sets = earning.ROICTTM(ttm_earning, earning_sets)
        earning_sets = earning.OwnROETTM(ttm_earning, earning_sets)
        earning_sets = earning.SalesGrossMarginTTM(ttm_earning, earning_sets)
        earning_sets = earning.TaxRTTM(ttm_earning, earning_sets)
        earning_sets = earning.TotaProfRtTTM(ttm_earning, earning_sets)
        # factor_earning = earning.invest_r_associates_to_tp_ttm(ttm_earning, earning_sets)
        earning_sets = earning_sets.reset_index()
        earning_sets['trade_date'] = str(trade_date)
        earning_sets.replace([-np.inf, np.inf, None], np.nan, inplace=True)
        return earning_sets

    def local_run(self, trade_date):
        print('trade_date %s' % trade_date)
        tic = time.time()
        tp_earning, ttm_earning, ttm_earning_5y = self.loading_data(trade_date)
        print('data load time %s' % (time.time()-tic))

        storage_engine = StorageEngine(self._url)
        result = self.process_calc_factor(trade_date, tp_earning, ttm_earning, ttm_earning_5y)
        print('cal_time %s' % (time.time() - tic))
        storage_engine.update_destdb(str(self._methods[-1]['packet'].split('.')[-1]), trade_date, result)
        # storage_engine.update_destdb('factor_earning', trade_date, result)

        
    # def remote_run(self, trade_date):
    #     total_data = self.loading_data(trade_date)
    #     #存储数据
    #     session = str(int(time.time() * 1000000 + datetime.datetime.now().microsecond))
    #     cache_data.set_cache(session, 'alphax', total_data.to_json(orient='records'))
    #     distributed_factor.delay(session, json.dumps(self._methods), self._name)
    #
    # def distributed_factor(self, total_data):
    #     mkt_df = self.calc_factor_by_date(total_data,trade_date)
    #     result = self.calc_factor('alphax.alpha191','Alpha191',mkt_df,trade_date)
        
# @app.task
# def distributed_factor(session, trade_date, packet_sets, name):
#     calc_engines = CalcEngine(name, packet_sets)
#     content = cache_data.get_cache(session, factor_name)
#     total_data = json_normalize(json.loads(content))
#     calc_engines.distributed_factor(total_data)
#
# # @app.task()
# def factor_calculate(**kwargs):
#     print("constrain_kwargs: {}".format(kwargs))
#     date_index = kwargs['date_index']
#     session = kwargs['session']
#     factor_name = kwargs['factor_name']
#     content1 = cache_data.get_cache(session + str(date_index) + "1", date_index)
#     content2 = cache_data.get_cache(session + str(date_index) + "2", date_index)
#     content3 = cache_data.get_cache(session + str(date_index) + "3", date_index)
#     print("len_con1: %s" % len(content1))
#     print("len_con2: %s" % len(content2))
#     print("len_con3: %s" % len(content3))
#     tp_earning = json_normalize(json.loads(str(content1, encoding='utf8')))
#     ttm_earning_5y = json_normalize(json.loads(str(content2, encoding='utf8')))
#     ttm_earning = json_normalize(json.loads(str(content3, encoding='utf8')))
#     # cache_date.get_cache使得index的名字丢失， 所以数据需要按照下面的方式设置index
#     tp_earning.set_index('security_code', inplace=True)
#     ttm_earning.set_index('security_code', inplace=True)
#     ttm_earning_5y.set_index('security_code', inplace=True)
#     # total_earning_data = {'tp_earning': tp_earning, 'ttm_earning_5y': ttm_earning_5y, 'ttm_earning': ttm_earning}
#     calculate(date_index, tp_earning, ttm_earning, ttm_earning_5y, factor_name)


