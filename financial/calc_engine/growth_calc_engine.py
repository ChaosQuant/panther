# -*- coding: utf-8 -*-

import pdb,importlib,inspect,time,datetime,json
# from PyFin.api import advanceDateByCalendar
# from data.polymerize import DBPolymerize
from data.storage_engine import StorageEngine
import time
from datetime import timedelta
from financial import factor_history_growth

from data.model import BalanceMRQ, BalanceTTM, BalanceReport
from data.model import CashFlowTTM, CashFlowReport
from data.model import IndicatorReport
from data.model import IncomeReport, IncomeTTM

from vision.vision.db.signletion_engine import *
from data.sqlengine import sqlEngine
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
# from ultron.cluster.invoke.cache_data import cache_data


class CalcEngine(object):
    def __init__(self, name, url, methods=[{'packet':'financial.factor_historical_growth','class':'Growth'},]):
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
        # print('trade_date %s' % trade_date)
        # print('trade_date_pre_year %s' % trade_date_pre_year)
        # print('trade_date_pre_year_2 %s' % trade_date_pre_year_2)
        # print('trade_date_pre_year_3 %s' % trade_date_pre_year_3)
        # print('trade_date_pre_year_4 %s' % trade_date_pre_year_4)
        # print('trade_date_pre_year_5 %s' % trade_date_pre_year_5)

        engine = sqlEngine()
        columns = ['COMPCODE', 'PUBLISHDATE', 'ENDDATE', 'symbol', 'company_id', 'trade_date']
        # report data
        balance_sets = engine.fetch_fundamentals_pit_extend_company_id(BalanceReport,
                                                                       [BalanceReport.TOTASSET,  # 总资产（资产合计）
                                                                        BalanceReport.RIGHAGGR,  # 股东权益合计
                                                                        ],
                                                                       dates=[trade_date]).drop(columns, axis=1)
        balance_sets = balance_sets.rename(columns={'TOTASSET': 'total_assets',  # 资产总计
                                                    'RIGHAGGR': 'total_owner_equities',  # 股东权益合计
                                                    })

        balance_sets_pre_year = engine.fetch_fundamentals_pit_extend_company_id(BalanceReport,
                                                                                [BalanceReport.TOTASSET,  # 总资产（资产合计）
                                                                                 BalanceReport.RIGHAGGR,  # 股东权益合计
                                                                                 ],
                                                                                dates=[trade_date_pre_year]).drop(
            columns, axis=1)
        balance_sets_pre_year = balance_sets_pre_year.rename(columns={"TOTASSET": "total_assets_pre_year",
                                                                      "RIGHAGGR": "total_owner_equities_pre_year"})

        balance_sets = pd.merge(balance_sets, balance_sets_pre_year, on='security_code')
        print('get_balabce_sets')

        # ttm 计算
        ttm_factor_sets = engine.fetch_fundamentals_pit_extend_company_id(IncomeTTM,
                                                                          [IncomeTTM.BIZINCO,  # 营业收入
                                                                           IncomeTTM.PERPROFIT,  # 营业利润
                                                                           IncomeTTM.TOTPROFIT,  # 利润总额
                                                                           IncomeTTM.NETPROFIT,  # 净利润
                                                                           IncomeTTM.BIZCOST,  # 营业成本
                                                                           IncomeTTM.PARENETP],  # 归属于母公司所有者的净利润
                                                                          dates=[trade_date]).drop(columns, axis=1)

        ttm_cash_flow_sets = engine.fetch_fundamentals_pit_extend_company_id(CashFlowTTM,
                                                                             [CashFlowTTM.FINNETCFLOW,  # 筹资活动产生的现金流量净额
                                                                              CashFlowTTM.MANANETR,  # 经营活动产生的现金流量净额
                                                                              CashFlowTTM.INVNETCASHFLOW,
                                                                              # 投资活动产生的现金流量净额
                                                                              CashFlowTTM.CASHNETI,  # 现金及现金等价物的净增加额
                                                                              ],
                                                                             dates=[trade_date]).drop(columns, axis=1)

        # field_key = ttm_cash_flow_sets.keys()
        # for i in field_key:
        #     ttm_factor_sets[i] = ttm_cash_flow_sets[i]
        ttm_factor_sets = pd.merge(ttm_factor_sets, ttm_cash_flow_sets, how='outer', on='security_code')

        ttm_factor_sets = ttm_factor_sets.rename(
            columns={"BIZINCO": "operating_revenue",
                     "PERPROFIT": "operating_profit",
                     "TOTPROFIT": "total_profit",
                     "NETPROFIT": "net_profit",
                     "BIZCOST": "operating_cost",
                     "PARENETP": "np_parent_company_owners",
                     "FINNETCFLOW": "net_finance_cash_flow",
                     "MANANETR": "net_operate_cash_flow",
                     "INVNETCASHFLOW": "net_invest_cash_flow",
                     'CASHNETI': 'n_change_in_cash'
                     })

        ttm_income_sets_pre = engine.fetch_fundamentals_pit_extend_company_id(IncomeTTM,
                                                                              [IncomeTTM.BIZINCO,  # 营业收入
                                                                               IncomeTTM.PERPROFIT,  # 营业利润
                                                                               IncomeTTM.TOTPROFIT,  # 利润总额
                                                                               IncomeTTM.NETPROFIT,  # 净利润
                                                                               IncomeTTM.BIZCOST,  # 营业成本
                                                                               IncomeTTM.PARENETP  # 归属于母公司所有者的净利润
                                                                               ],
                                                                              dates=[trade_date_pre_year]).drop(columns,
                                                                                                                axis=1)

        ttm_factor_sets_pre = ttm_income_sets_pre.rename(
            columns={"BIZINCO": "operating_revenue_pre_year",
                     "PERPROFIT": "operating_profit_pre_year",
                     "TOTPROFIT": "total_profit_pre_year",
                     "NETPROFIT": "net_profit_pre_year",
                     "BIZCOST": "operating_cost_pre_year",
                     "PARENETP": "np_parent_company_owners_pre_year",
                     })

        # field_key = ttm_factor_sets_pre.keys()
        # for i in field_key:
        #     ttm_factor_sets[i] = ttm_factor_sets_pre[i]
        ttm_factor_sets = pd.merge(ttm_factor_sets, ttm_factor_sets_pre, how='outer', on='security_code')



        ttm_cash_flow_sets_pre = engine.fetch_fundamentals_pit_extend_company_id(CashFlowTTM,
                                                                                 [CashFlowTTM.FINNETCFLOW,
                                                                                  # 筹资活动产生的现金流量净额
                                                                                  CashFlowTTM.MANANETR,  # 经营活动产生的现金流量净额
                                                                                  CashFlowTTM.INVNETCASHFLOW,
                                                                                  # 投资活动产生的现金流量净额
                                                                                  CashFlowTTM.CASHNETI,  # 现金及现金等价物的净增加额
                                                                                  ],
                                                                                 dates=[trade_date_pre_year]).drop(
            columns, axis=1)
        ttm_cash_flow_sets_pre = ttm_cash_flow_sets_pre.rename(
            columns={"FINNETCFLOW": "net_finance_cash_flow_pre_year",
                     "MANANETR": "net_operate_cash_flow_pre_year",
                     "INVNETCASHFLOW": "net_invest_cash_flow_pre_year",
                     'CASHNETI': 'n_change_in_cash_pre_year',
                     })

        # field_key = ttm_cash_flow_sets_pre.keys()
        # for i in field_key:
        #     ttm_factor_sets[i] = ttm_cash_flow_sets_pre[i]
        ttm_factor_sets = pd.merge(ttm_factor_sets, ttm_cash_flow_sets, how='outer', on='security_code')
        print('get_ttm_factor_sets_pre')

        # ttm 连续
        ttm_factor_sets_pre_year_2 = engine.fetch_fundamentals_pit_extend_company_id(IncomeTTM,
                                                                                     [IncomeTTM.NETPROFIT,
                                                                                      IncomeTTM.BIZINCO,
                                                                                      IncomeTTM.BIZCOST,
                                                                                      ],
                                                                                     dates=[
                                                                                         trade_date_pre_year_2]).drop(
            columns, axis=1)
        ttm_factor_sets_pre_year_2 = ttm_factor_sets_pre_year_2.rename(
            columns={"BIZINCO": "operating_revenue_pre_year_2",
                     "BIZCOST": "operating_cost_pre_year_2",
                     "NETPROFIT": "net_profit_pre_year_2",
                     })
        ttm_factor_sets = pd.merge(ttm_factor_sets, ttm_factor_sets_pre_year_2, how='outer', on="security_code")
        # field_key = ttm_factor_sets_pre_year_2.keys()
        # for i in field_key:
        #     ttm_factor_sets[i] = ttm_factor_sets_pre_year_2[i]
        print('get_ttm_factor_sets_2')

        ttm_factor_sets_pre_year_3 = engine.fetch_fundamentals_pit_extend_company_id(IncomeTTM,
                                                                                     [IncomeTTM.NETPROFIT,
                                                                                      IncomeTTM.BIZINCO,
                                                                                      IncomeTTM.BIZCOST,
                                                                                      ],
                                                                                     dates=[
                                                                                         trade_date_pre_year_3]).drop(
            columns, axis=1)
        ttm_factor_sets_pre_year_3 = ttm_factor_sets_pre_year_3.rename(
            columns={"BIZINCO": "operating_revenue_pre_year_3",
                     "BIZCOST": "operating_cost_pre_year_3",
                     "NETPROFIT": "net_profit_pre_year_3",
                     })
        ttm_factor_sets = pd.merge(ttm_factor_sets, ttm_factor_sets_pre_year_3, how='outer', on="security_code")
        # field_key = ttm_factor_sets_pre_year_3.keys()
        # for i in field_key:
        #     ttm_factor_sets[i] = ttm_factor_sets_pre_year_3[i]

        print('get_ttm_factor_sets_3')

        ttm_factor_sets_pre_year_4 = engine.fetch_fundamentals_pit_extend_company_id(IncomeTTM,
                                                                                     [IncomeTTM.NETPROFIT,
                                                                                      IncomeTTM.BIZINCO,
                                                                                      IncomeTTM.BIZCOST,
                                                                                      ],
                                                                                     dates=[
                                                                                         trade_date_pre_year_4]).drop(
            columns, axis=1)
        ttm_factor_sets_pre_year_4 = ttm_factor_sets_pre_year_4.rename(
            columns={"BIZINCO": "operating_revenue_pre_year_4",
                     "BIZCOST": "operating_cost_pre_year_4",
                     "NETPROFIT": "net_profit_pre_year_4",
                     })
        ttm_factor_sets = pd.merge(ttm_factor_sets, ttm_factor_sets_pre_year_4, how='outer', on="security_code")
        # field_key = ttm_factor_sets_pre_year_4.keys()
        # for i in field_key:
        #     ttm_factor_sets[i] = ttm_factor_sets_pre_year_4[i]
        print('get_ttm_factor_sets_4')

        ttm_factor_sets_pre_year_5 = engine.fetch_fundamentals_pit_extend_company_id(IncomeTTM,
                                                                                     [IncomeTTM.NETPROFIT,
                                                                                      IncomeTTM.BIZINCO,
                                                                                      IncomeTTM.BIZCOST,
                                                                                      ],
                                                                                     dates=[
                                                                                         trade_date_pre_year_5]).drop(
            columns, axis=1)

        ttm_factor_sets_pre_year_5 = ttm_factor_sets_pre_year_5.rename(
            columns={"BIZINCO": "operating_revenue_pre_year_5",
                     "BIZCOST": "operating_cost_pre_year_5",
                     "NETPROFIT": "net_profit_pre_year_5",
                     })
        ttm_factor_sets = pd.merge(ttm_factor_sets, ttm_factor_sets_pre_year_5, how='outer', on="security_code")
        # field_key = ttm_factor_sets_pre_year_5.keys()
        # for i in field_key:
        #     ttm_factor_sets[i] = ttm_factor_sets_pre_year_5[i]
        # print('get_ttm_factor_sets_5')

        growth_sets = pd.merge(ttm_factor_sets, balance_sets, how='outer', on='security_code')
        return growth_sets

    def process_calc_factor(self, trade_date, growth_sets):
        growth_sets = growth_sets.set_index('security_code')
        # print(growth_sets.head())
        print(len(growth_sets))
        print(growth_sets.keys())
        growth = factor_history_growth.Growth()
        if len(growth_sets) <= 0:
            print("%s has no data" % trade_date)
            return
        historical_growth_sets = pd.DataFrame()
        historical_growth_sets['security_code'] = growth_sets.index
        historical_growth_sets = historical_growth_sets.set_index('security_code')

        historical_growth_sets = growth.NetAsset1YChg(growth_sets, historical_growth_sets)
        historical_growth_sets = growth.TotalAsset1YChg(growth_sets, historical_growth_sets)
        historical_growth_sets = growth.ORev1YChgTTM(growth_sets, historical_growth_sets)
        historical_growth_sets = growth.OPft1YChgTTM(growth_sets, historical_growth_sets)
        historical_growth_sets = growth.GrPft1YChgTTM(growth_sets, historical_growth_sets)
        historical_growth_sets = growth.NetPft1YChgTTM(growth_sets, historical_growth_sets)
        historical_growth_sets = growth.NetPftAP1YChgTTM(growth_sets, historical_growth_sets)
        historical_growth_sets = growth.NetPft3YChgTTM(growth_sets, historical_growth_sets)
        historical_growth_sets = growth.NetPft5YChgTTM(growth_sets, historical_growth_sets)
        historical_growth_sets = growth.ORev3YChgTTM(growth_sets, historical_growth_sets)
        historical_growth_sets = growth.ORev5YChgTTM(growth_sets, historical_growth_sets)
        historical_growth_sets = growth.NetCF1YChgTTM(growth_sets, historical_growth_sets)
        # factor_historical_growth = growth.NetPftAPNNRec1YChgTTM(growth_sets, factor_historical_growth)
        historical_growth_sets = growth.StdUxpErn1YTTM(growth_sets, historical_growth_sets)
        historical_growth_sets = growth.StdUxpGrPft1YTTM(growth_sets, historical_growth_sets)
        historical_growth_sets = growth.FCF1YChgTTM(growth_sets, historical_growth_sets)
        historical_growth_sets = growth.OCF1YChgTTM(growth_sets, historical_growth_sets)
        historical_growth_sets = growth.ICF1YChgTTM(growth_sets, historical_growth_sets)

        historical_growth_sets = historical_growth_sets.reset_index()
        historical_growth_sets['trade_date'] = str(trade_date)
        print(historical_growth_sets.head())
        return historical_growth_sets

    def local_run(self, trade_date):
        print('trade_date %s' % trade_date)
        tic = time.time()
        growth_sets = self.loading_data(trade_date)

        print('data load time %s' % (time.time()-tic))

        storage_engine = StorageEngine(self._url)
        result = self.process_calc_factor(trade_date, growth_sets)
        print('cal_time %s' % (time.time() - tic))
        # storage_engine.update_destdb(str(method['packet'].split('.')[-1]), trade_date, result)
        # storage_engine.update_destdb('test_factor_valuation', trade_date, result)

        
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
#     calc_engine = CalcEngine(name, packet_sets)
#     content = cache_data.get_cache(session, factor_name)
#     total_data = json_normalize(json.loads(content))
#     calc_engine.distributed_factor(total_data)
#

# # @app.task()
# def factor_calculate(**kwargs):
#     print("growth_kwargs: {}".format(kwargs))
#     date_index = kwargs['date_index']
#     session = kwargs['session']
#     content = cache_data.get_cache(session, "growth" + str(date_index))
#     total_growth_data = json_normalize(json.loads(str(content, encoding='utf8')))
#     print("len_total_growth_data {}".format(len(total_growth_data)))
#     calculate(date_index, total_growth_data)


