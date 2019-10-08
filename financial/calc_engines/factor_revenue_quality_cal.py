# -*- coding: utf-8 -*-

import pdb, importlib, inspect, time, datetime, json
# from PyFin.api import advanceDateByCalendar
# from data.polymerize import DBPolymerize
from data.storage_engine import StorageEngine
import time
import pandas as pd
import numpy as np
from datetime import timedelta, datetime
from financial import factor_revenue_quality

from data.model import BalanceTTM, BalanceReport
from data.model import CashFlowTTM, CashFlowReport
from data.model import IndicatorTTM
from data.model import IncomeReport, IncomeTTM
from vision.table.valuation import Valuation
from vision.db.signletion_engine import *
from data.sqlengine import sqlEngine

# pd.set_option('display.max_columns', None)
# pd.set_option('display.max_rows', None)
# from ultron.cluster.invoke.cache_data import cache_data


class CalcEngine(object):
    def __init__(self, name, url,
                 methods=[{'packet': 'financial.factor_revenue_quality', 'class': 'FactorRevenueQuality'}, ]):
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
        return list(filter(lambda x: not x.startswith('_') and callable(getattr(method, x)), dir(method)))

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
        engine = sqlEngine()
        columns = ['COMPCODE', 'PUBLISHDATE', 'ENDDATE', 'symbol', 'company_id', 'trade_date']

        # Report Data
        cash_flow_sets = engine.fetch_fundamentals_pit_extend_company_id(CashFlowReport,
                                                                         [CashFlowReport.BIZNETCFLOW,
                                                                          ],
                                                                         dates=[trade_date]).drop(columns, axis=1)
        cash_flow_sets = cash_flow_sets.rename(columns={'BIZNETCFLOW': 'net_operate_cash_flow',  # 经营活动产生的现金流量净额
                                                        })

        income_sets = engine.fetch_fundamentals_pit_extend_company_id(IncomeReport,
                                                                      [IncomeReport.TOTPROFIT,
                                                                       IncomeReport.NONOREVE,
                                                                       IncomeReport.NONOEXPE,
                                                                       IncomeReport.BIZTOTCOST,
                                                                       IncomeReport.BIZTOTINCO,
                                                                       ], dates=[trade_date])
        for col in columns:
            if col in list(income_sets.keys()):
                income_sets = income_sets.drop(col, axis=1)
        income_sets = income_sets.rename(columns={'TOTPROFIT': 'total_profit',  # 利润总额
                                                  'NONOREVE': 'non_operating_revenue',  # 营业外收入
                                                  'NONOEXPE': 'non_operating_expense',  # 营业外支出
                                                  'BIZTOTCOST': 'total_operating_cost',  # 营业总成本
                                                  'BIZTOTINCO': 'total_operating_revenue',  # 营业总收入
                                                  })

        balance_sets = engine.fetch_fundamentals_pit_extend_company_id(BalanceReport,
                                                                       [BalanceReport.TOTALCURRLIAB
                                                                        ], dates=[trade_date])
        for col in columns:
            if col in list(balance_sets.keys()):
                balance_sets = balance_sets.drop(col, axis=1)
        balance_sets = balance_sets.rename(columns={'TOTALCURRLIAB': 'total_current_liability',  # 流动负债合计
                                                    })
        tp_revenue_quanlity = pd.merge(cash_flow_sets, income_sets, on='security_code')
        tp_revenue_quanlity = pd.merge(balance_sets, tp_revenue_quanlity, on='security_code')

        trade_date_pre_year = self.get_trade_date(trade_date, 1)
        trade_date_pre_year_2 = self.get_trade_date(trade_date, 2)
        trade_date_pre_year_3 = self.get_trade_date(trade_date, 3)
        trade_date_pre_year_4 = self.get_trade_date(trade_date, 4)

        income_con_sets = engine.fetch_fundamentals_pit_extend_company_id(IncomeTTM,
                                                                          [IncomeTTM.NETPROFIT,
                                                                           ],
                                                                          dates=[trade_date,
                                                                                 trade_date_pre_year,
                                                                                 trade_date_pre_year_2,
                                                                                 trade_date_pre_year_3,
                                                                                 trade_date_pre_year_4,
                                                                                 ])
        for col in columns:
            if col in list(income_con_sets.keys()):
                income_con_sets = income_con_sets.drop(col, axis=1)
        income_con_sets = income_con_sets.groupby(['security_code'])
        income_con_sets = income_con_sets.sum()
        income_con_sets = income_con_sets.rename(columns={'NETPROFIT': 'net_profit_5'})

        # TTM Data
        cash_flow_ttm_sets = engine.fetch_fundamentals_pit_extend_company_id(CashFlowTTM,
                                                                             [CashFlowTTM.BIZNETCFLOW,
                                                                              ], dates=[trade_date])
        for col in columns:
            if col in list(cash_flow_ttm_sets.keys()):
                cash_flow_ttm_sets = cash_flow_ttm_sets.drop(col, axis=1)
        cash_flow_ttm_sets = cash_flow_ttm_sets.rename(
            columns={'BIZNETCFLOW': 'net_operate_cash_flow',  # 经营活动产生的现金流量净额
                     })

        income_ttm_sets = engine.fetch_fundamentals_pit_extend_company_id(IncomeTTM,
                                                                          [IncomeTTM.TOTPROFIT,
                                                                           IncomeTTM.NONOREVE,
                                                                           IncomeTTM.NONOEXPE,
                                                                           IncomeTTM.BIZTOTCOST,
                                                                           IncomeTTM.BIZTOTINCO,
                                                                           IncomeTTM.PERPROFIT,
                                                                           IncomeTTM.NETPROFIT,
                                                                           IncomeTTM.BIZINCO,
                                                                           ], dates=[trade_date])
        for col in columns:
            if col in list(income_ttm_sets.keys()):
                income_ttm_sets = income_ttm_sets.drop(col, axis=1)
        income_ttm_sets = income_ttm_sets.rename(
            columns={'TOTPROFIT': 'total_profit',  # 利润总额
                     'NONOREVE': 'non_operating_revenue',  # 营业外收入
                     'NONOEXPE': 'non_operating_expense',  # 营业外支出
                     'BIZTOTCOST': 'total_operating_cost',  # 营业总成本
                     'BIZTOTINCO': 'total_operating_revenue',  # 营业总收入
                     'PERPROFIT': 'operating_profit',  # 营业利润
                     'NETPROFIT': 'net_profit',  # 净利润
                     'BIZINCO': 'operating_revenue',  # 营业收入
                     })

        balance_ttm_sets = engine.fetch_fundamentals_pit_extend_company_id(BalanceTTM,
                                                                           [BalanceTTM.TOTALCURRLIAB
                                                                            ], dates=[trade_date])
        for col in columns:
            if col in list(balance_ttm_sets.keys()):
                balance_ttm_sets = balance_ttm_sets.drop(col, axis=1)
        balance_ttm_sets = balance_ttm_sets.rename(
            columns={'TOTALCURRLIAB': 'total_current_liability',  # 流动负债合计
                     })

        column = ['trade_date']
        valuation_sets = get_fundamentals(query(Valuation.security_code,
                                                Valuation.trade_date,
                                                Valuation.market_cap, )
                                          .filter(Valuation.trade_date.in_([trade_date])))
        for col in column:
            if col in list(valuation_sets.keys()):
                valuation_sets = valuation_sets.drop(col, axis=1)

        indicator_ttm_sets = engine.fetch_fundamentals_pit_extend_company_id(IndicatorTTM,
                                                                             [IndicatorTTM.NVALCHGITOTP,
                                                                              ], dates=[trade_date])
        for col in columns:
            if col in list(indicator_ttm_sets.keys()):
                indicator_ttm_sets = indicator_ttm_sets.drop(col, axis=1)
        ttm_revenue_quanlity = pd.merge(cash_flow_ttm_sets, income_ttm_sets, on='security_code')
        ttm_revenue_quanlity = pd.merge(balance_ttm_sets, ttm_revenue_quanlity, on='security_code')
        ttm_revenue_quanlity = pd.merge(valuation_sets, ttm_revenue_quanlity, on='security_code')
        ttm_revenue_quanlity = pd.merge(indicator_ttm_sets, ttm_revenue_quanlity, on='security_code')
        ttm_revenue_quanlity = pd.merge(income_con_sets, ttm_revenue_quanlity, on='security_code')

        valuation_con_sets = get_fundamentals(query(Valuation.security_code,
                                                    Valuation.trade_date,
                                                    Valuation.market_cap,
                                                    Valuation.circulating_market_cap,
                                                    )
                                              .filter(Valuation.trade_date.in_([trade_date,
                                                                                trade_date_pre_year,
                                                                                trade_date_pre_year_2,
                                                                                trade_date_pre_year_3,
                                                                                trade_date_pre_year_4])))
        for col in column:
            if col in list(valuation_con_sets.keys()):
                valuation_con_sets = valuation_con_sets.drop(col, axis=1)

        valuation_con_sets = valuation_con_sets.groupby(['security_code'])
        valuation_con_sets = valuation_con_sets.sum()
        valuation_con_sets = valuation_con_sets.rename(columns={'market_cap': 'market_cap_5',
                                                                'circulating_market_cap': 'circulating_market_cap_5'
                                                                })
        # tp_revenue_quanlity = pd.merge(valuation_con_sets, tp_revenue_quanlity, on='security_code')
        ttm_revenue_quanlity = pd.merge(valuation_con_sets, ttm_revenue_quanlity, on='security_code')

        return tp_revenue_quanlity, ttm_revenue_quanlity

    def process_calc_factor(self, trade_date, tp_revenue_quanlity, ttm_revenue_quanlity):

        tp_revenue_quanlity = tp_revenue_quanlity.set_index('security_code')
        ttm_revenue_quanlity = ttm_revenue_quanlity.set_index('security_code')
        revenue_quality = factor_revenue_quality.FactorRevenueQuality()

        factor_revenue = pd.DataFrame()
        factor_revenue['security_code'] = tp_revenue_quanlity.index
        factor_revenue = factor_revenue.set_index('security_code')
        # 非TTM计算
        factor_revenue = revenue_quality.NetNonOIToTP(tp_revenue_quanlity, factor_revenue)
        factor_revenue = revenue_quality.OperatingNIToTP(tp_revenue_quanlity, factor_revenue)

        # TTM计算
        factor_revenue = revenue_quality.NetNonOIToTPTTM(ttm_revenue_quanlity, factor_revenue)
        factor_revenue = revenue_quality.OperatingNIToTPTTM(ttm_revenue_quanlity, factor_revenue)
        factor_revenue = revenue_quality.OptCFToCurrLiabilityTTM(ttm_revenue_quanlity, factor_revenue)
        factor_revenue = revenue_quality.OPToTPTTM(ttm_revenue_quanlity, factor_revenue)
        factor_revenue = revenue_quality.PriceToRevRatioTTM(ttm_revenue_quanlity, factor_revenue)
        factor_revenue = revenue_quality.NVALCHGITOTP(ttm_revenue_quanlity, factor_revenue)
        factor_revenue = revenue_quality.PftMarginTTM(ttm_revenue_quanlity, factor_revenue)
        factor_revenue = revenue_quality.PriceToRevRatioAvg5YTTM(ttm_revenue_quanlity, factor_revenue)
        factor_revenue = factor_revenue.reset_index()
        factor_revenue['trade_date'] = str(trade_date)
        factor_revenue.replace([-np.inf, np.inf, None], 'null', inplace=True)
        return factor_revenue

    def local_run(self, trade_date):
        print('当前交易日: %s' % trade_date)
        tic = time.time()
        tp_revenue_quanlity, ttm_revenue_quanlity = self.loading_data(trade_date)
        print('data load time %s' % (time.time() - tic))

        storage_engine = StorageEngine(self._url)
        result = self.process_calc_factor(trade_date, tp_revenue_quanlity, ttm_revenue_quanlity)
        print('cal_time %s' % (time.time() - tic))
        storage_engine.update_destdb(str(self._methods[-1]['packet'].split('.')[-1]), trade_date, result)
        # storage_engine.update_destdb('factor_revenue_quality', trade_date, result)

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
#     print("per_share_kwargs: {}".format(kwargs))
#     date_index = kwargs['date_index']
#     session = kwargs['session']
#     content = cache_data.get_cache(session + str(date_index), date_index)
#     total_pre_share_data = json_normalize(json.loads(str(content, encoding='utf8')))
#     print("len_total_per_share_data {}".format(len(total_pre_share_data)))
#     calculate(date_index, total_pre_share_data)
