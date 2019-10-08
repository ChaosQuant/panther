# -*- coding: utf-8 -*-
import pdb,importlib,inspect,time,datetime,json
# from PyFin.api import advanceDateByCalendar
# from data.polymerize import DBPolymerize
from data.storage_engine import StorageEngine
import time
import pandas as pd
import numpy as np
from datetime import datetime
from financial import factor_cash_flow

from data.model import BalanceTTM
from data.model import CashFlowTTM, CashFlowReport
from data.model import IncomeReport, IncomeTTM

from vision.table.valuation import Valuation
from vision.db.signletion_engine import *
from data.sqlengine import sqlEngine
# pd.set_option('display.max_columns', None)
# pd.set_option('display.max_rows', None)
# from ultron.cluster.invoke.cache_data import cache_data


class CalcEngine(object):
    def __init__(self, name, url, methods=[{'packet':'financial.factor_cash_flow','class':'FactorCashFlow'},]):
        self._name = name
        self._methods = methods
        self._url = url

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
        columns = ['COMPCODE', 'PUBLISHDATE', 'ENDDATE', 'symbol', 'company_id', 'trade_date']
        # report data
        engine = sqlEngine()
        cash_flow_sets = engine.fetch_fundamentals_pit_extend_company_id(CashFlowReport,
                                                                         [CashFlowReport.MANANETR,  # 经营活动现金流量净额
                                                                          CashFlowReport.LABORGETCASH,  # 销售商品、提供劳务收到的现金
                                                                          ], dates=[trade_date]).drop(columns, axis=1)

        income_sets = engine.fetch_fundamentals_pit_extend_company_id(IncomeReport,
                                                                      [IncomeReport.BIZINCO,  # 营业收入
                                                                       IncomeReport.BIZTOTCOST,  # 营业总成本
                                                                       IncomeReport.BIZTOTINCO,  # 营业总收入
                                                                       ], dates=[trade_date]).drop(columns, axis=1)

        tp_cash_flow = pd.merge(cash_flow_sets, income_sets, on="security_code")

        tp_cash_flow = tp_cash_flow.rename(columns={'MANANETR': 'net_operate_cash_flow',  # 经营活动现金流量净额
                                                    'LABORGETCASH': 'goods_sale_and_service_render_cash', # 销售商品、提供劳务收到的现金
                                                    'BIZINCO': 'operating_revenue',  # 营业收入
                                                    'BIZTOTINCO': 'total_operating_revenue',  # 营业总收入
                                                    'BIZTOTCOST': 'total_operating_cost',  # 营业总成本
                                                    })

        # ttm data
        balance_ttm_sets = engine.fetch_fundamentals_pit_extend_company_id(BalanceTTM,
                                                                           [BalanceTTM.TOTLIAB,  # 负债合计
                                                                            BalanceTTM.SHORTTERMBORR,  # 短期借款
                                                                            BalanceTTM.LONGBORR,  # 长期借款
                                                                            BalanceTTM.TOTALCURRLIAB,  # 流动负债合计
                                                                            BalanceTTM.TOTCURRASSET,  # 流动资产合计
                                                                            BalanceTTM.TOTASSET,  # 资产总计
                                                                            ],
                                                                           dates=[trade_date]).drop(columns, axis=1)

        cash_flow_ttm_sets = engine.fetch_fundamentals_pit_extend_company_id(CashFlowTTM,
                                                                             [CashFlowTTM.MANANETR,  # 经营活动现金流量净额
                                                                              CashFlowTTM.FINALCASHBALA,  # 期末现金及现金等价物余额
                                                                              CashFlowTTM.LABORGETCASH,  # 销售商品、提供劳务收到的现金
                                                                              ],
                                                                             dates=[trade_date]).drop(columns, axis=1)

        income_ttm_sets = engine.fetch_fundamentals_pit_extend_company_id(IncomeTTM,
                                                                          [IncomeTTM.BIZTOTCOST,  # 营业总成本
                                                                           IncomeTTM.BIZINCO,  # 营业收入
                                                                           IncomeTTM.BIZTOTINCO,  # 营业总收入
                                                                           IncomeTTM.NETPROFIT,  # 净利润
                                                                           IncomeTTM.PARENETP,  # 归属于母公司所有者的净利润
                                                                           IncomeTTM.PERPROFIT,  # 营业利润
                                                                           ],
                                                                          dates=[trade_date]).drop(columns, axis=1)

        ttm_cash_flow = pd.merge(balance_ttm_sets, cash_flow_ttm_sets, on="security_code")
        ttm_cash_flow = pd.merge(income_ttm_sets, ttm_cash_flow, on="security_code")
        ttm_cash_flow = ttm_cash_flow.rename(columns={'MANANETR': 'net_operate_cash_flow',  # 经营活动现金流量净额
                                                      'BIZINCO': 'operating_revenue',  # 营业收入
                                                      'BIZTOTINCO': 'total_operating_revenue',  # 营业总收入
                                                      'BIZTOTCOST': 'total_operating_cost',  # 营业总成本
                                                      'NETPROFIT': 'net_profit',  # 净利润
                                                      'PARENETP': 'np_parent_company_owners',  # 归属于母公司所有者的净利润
                                                      'TOTLIAB': 'total_liability',  # 负债合计
                                                      'SHORTTERMBORR': 'shortterm_loan',  # 短期借款
                                                      'LONGBORR': 'longterm_loan',  # 长期借款
                                                      'TOTALCURRLIAB': 'total_current_liability',  # 流动负债合计
                                                      'LABORGETCASH': 'goods_sale_and_service_render_cash',  # 销售商品、提供劳务收到的现金
                                                      # 'NDEBT':'net_liability',  # 净负债
                                                      'TOTCURRASSET': 'total_current_assets',  # 流动资产合计
                                                      'TOTASSET': 'total_assets',  # 资产总计
                                                      'FINALCASHBALA': 'cash_and_equivalents_at_end',  # 期末现金及现金等价物余额
                                                      'PERPROFIT': 'operating_profit',  # 期末现金及现金等价物余额
                                                      })

        column = ['trade_date']
        valuation_sets = get_fundamentals(query(Valuation.security_code,
                                                Valuation.trade_date,
                                                Valuation.market_cap, )
                                          .filter(Valuation.trade_date.in_([trade_date]))).drop(column, axis=1)

        ttm_cash_flow = pd.merge(ttm_cash_flow, valuation_sets, how='outer', on='security_code')
        tp_cash_flow = pd.merge(tp_cash_flow, valuation_sets, how='outer', on='security_code')

        return tp_cash_flow, ttm_cash_flow

    def process_calc_factor(self, trade_date, tp_cash_flow, ttm_factor_sets):
        tp_cash_flow = tp_cash_flow.set_index('security_code')
        ttm_factor_sets = ttm_factor_sets.set_index('security_code')

        cash_flow = factor_cash_flow.FactorCashFlow()
        cash_flow_sets = pd.DataFrame()
        cash_flow_sets['security_code'] = tp_cash_flow.index
        cash_flow_sets = cash_flow_sets.set_index('security_code')

        # 非TTM计算
        cash_flow_sets = cash_flow.CashOfSales(tp_cash_flow, cash_flow_sets)
        cash_flow_sets = cash_flow.NOCFToOpt(tp_cash_flow, cash_flow_sets)
        cash_flow_sets = cash_flow.SalesServCashToOR(tp_cash_flow, cash_flow_sets)

        # TTM计算
        cash_flow_sets = cash_flow.OptOnReToAssetTTM(ttm_factor_sets, cash_flow_sets)
        cash_flow_sets = cash_flow.NetProCashCoverTTM(ttm_factor_sets, cash_flow_sets)
        cash_flow_sets = cash_flow.OptToEnterpriseTTM(ttm_factor_sets, cash_flow_sets)
        cash_flow_sets = cash_flow.OptCFToRevTTM(ttm_factor_sets, cash_flow_sets)
        cash_flow_sets = cash_flow.OptToAssertTTM(ttm_factor_sets, cash_flow_sets)
        cash_flow_sets = cash_flow.SaleServCashToOptReTTM(ttm_factor_sets, cash_flow_sets)
        cash_flow_sets = cash_flow.NOCFTOOPftTTM(ttm_factor_sets, cash_flow_sets)
        cash_flow_sets = cash_flow.OptCFToNITTM(ttm_factor_sets, cash_flow_sets)

        cash_flow_sets['trade_date'] = str(trade_date)
        cash_flow_sets = cash_flow_sets.reset_index()
        cash_flow_sets.replace([-np.inf, np.inf, None], np.nan, inplace=True)
        return cash_flow_sets

    def local_run(self, trade_date):
        print('当前交易日: %s' % trade_date)
        tic = time.time()
        tp_cash_flow, ttm_cash_flow_sets = self.loading_data(trade_date)
        print('data load time %s' % (time.time()-tic))

        storage_engine = StorageEngine(self._url)
        result = self.process_calc_factor(trade_date, tp_cash_flow, ttm_cash_flow_sets)
        print('cal_time %s' % (time.time() - tic))
        storage_engine.update_destdb(str(self._methods[-1]['packet'].split('.')[-1]), trade_date, result)
        # storage_engine.update_destdb('factor_cash_flow', trade_date, result)

        
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
#     print("management_kwargs: {}".format(kwargs))
#     date_index = kwargs['date_index']
#     session = kwargs['session']
#     factor_name = kwargs['factor_name']
#     content1 = cache_data.get_cache(session + str(date_index) + "1", date_index)
#     content2 = cache_data.get_cache(session + str(date_index) + "2", date_index)
#     tp_derivation = json_normalize(json.loads(str(content1, encoding='utf8')))
#     ttm_derivation = json_normalize(json.loads(str(content2, encoding='utf8')))
#     tp_derivation.set_index('security_code', inplace=True)
#     ttm_derivation.set_index('security_code', inplace=True)
#     print("len_tp_management_data {}".format(len(tp_derivation)))
#     print("len_ttm_management_data {}".format(len(ttm_derivation)))
#     # total_cash_flow_data = {'tp_management': tp_derivation, 'ttm_management': ttm_derivation}
#     calculate(date_index, tp_derivation, ttm_derivation, factor_name)

