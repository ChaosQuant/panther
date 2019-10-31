# -*- coding: utf-8 -*-

import pdb,importlib,inspect,time,datetime,json
# from PyFin.api import advanceDateByCalendar
# from data.polymerize import DBPolymerize
from data.storage_engine import StorageEngine
import time
import pandas as pd
import numpy as np
from datetime import datetime
from basic_derivation import factor_basic_derivation

from data.model import BalanceMRQ
from data.model import CashFlowMRQ, CashFlowTTM
from data.model import IndicatorMRQ, IndicatorTTM
from data.model import IncomeMRQ, IncomeTTM

from vision.db.signletion_engine import *
from data.sqlengine import sqlEngine
# pd.set_option('display.max_columns', None)
# pd.set_option('display.max_rows', None)
# from ultron.cluster.invoke.cache_data import cache_data


class CalcEngine(object):
    def __init__(self, name, url, methods=[{'packet': 'basic_derivation.factor_basic_derivation',
                                            'class': 'FactorBasicDerivation'},]):
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
        engine = sqlEngine()
        cash_flow_sets = engine.fetch_fundamentals_pit_extend_company_id(CashFlowMRQ,
                                                                         [CashFlowMRQ.FINALCASHBALA,
                                                                          ], dates=[trade_date])
        for col in columns:
            if col in list(cash_flow_sets.keys()):
                cash_flow_sets = cash_flow_sets.drop(col, axis=1)
        balance_sets = engine.fetch_fundamentals_pit_extend_company_id(BalanceMRQ,
                                                                       [BalanceMRQ.SHORTTERMBORR,
                                                                        BalanceMRQ.DUENONCLIAB,
                                                                        BalanceMRQ.LONGBORR,
                                                                        BalanceMRQ.BDSPAYA,
                                                                        BalanceMRQ.INTEPAYA,
                                                                        BalanceMRQ.PARESHARRIGH,
                                                                        BalanceMRQ.TOTASSET,
                                                                        BalanceMRQ.FIXEDASSECLEATOT,
                                                                        BalanceMRQ.TOTLIAB,
                                                                        BalanceMRQ.RIGHAGGR,
                                                                        BalanceMRQ.INTAASSET,
                                                                        # BalanceMRQ.DEVEEXPE,
                                                                        BalanceMRQ.GOODWILL,
                                                                        BalanceMRQ.LOGPREPEXPE,
                                                                        BalanceMRQ.DEFETAXASSET,
                                                                        BalanceMRQ.MINYSHARRIGH,
                                                                        ], dates=[trade_date])
        for col in columns:
            if col in list(balance_sets.keys()):
                balance_sets = balance_sets.drop(col, axis=1)
        balance_sets = balance_sets.rename(columns={
            'SHORTTERMBORR': 'shortterm_loan',  # 短期借款
            'DUENONCLIAB': 'non_current_liability_in_one_year',  # 一年内到期的非流动负债
            'LONGBORR': 'longterm_loan',  # 长期借款
            'BDSPAYA': 'bonds_payable',  # 应付债券
            'INTEPAYA': 'interest_payable',  # 应付利息
        })

        indicator_sets = engine.fetch_fundamentals_pit_extend_company_id(IndicatorMRQ,
                                                                         [IndicatorMRQ.FCFF,
                                                                          IndicatorMRQ.FCFE,
                                                                          IndicatorMRQ.NEGAL,
                                                                          IndicatorMRQ.NOPI,
                                                                          IndicatorMRQ.WORKCAP,
                                                                          IndicatorMRQ.RETAINEDEAR,
                                                                          IndicatorMRQ.NDEBT,
                                                                          IndicatorMRQ.NONINTCURLIABS,
                                                                          IndicatorMRQ.NONINTNONCURLIAB,
                                                                          # IndicatorMRQ.CURDEPANDAMOR,
                                                                          IndicatorMRQ.TOTIC,
                                                                          IndicatorMRQ.EBIT,
                                                                          ], dates=[trade_date])
        for col in columns:
            if col in list(indicator_sets.keys()):
                indicator_sets = indicator_sets.drop(col, axis=1)
        income_sets = engine.fetch_fundamentals_pit_extend_company_id(IncomeMRQ,
                                                                      [IncomeMRQ.INCOTAXEXPE,
                                                                       ], dates=[trade_date])
        for col in columns:
            if col in list(income_sets.keys()):
                income_sets = income_sets.drop(col, axis=1)

        tp_detivation = pd.merge(cash_flow_sets, balance_sets, how='outer', on='security_code')
        tp_detivation = pd.merge(indicator_sets, tp_detivation, how='outer', on='security_code')
        tp_detivation = pd.merge(income_sets, tp_detivation, how='outer', on='security_code')

        # balance_ttm_sets = engine.fetch_fundamentals_pit_extend_company_id(BalanceTTM,
        #                                                                    [BalanceTTM.MINYSHARRIGH,
        #                                                                    ], dates=[trade_date]).drop(columns, axis=1)
        income_ttm_sets = engine.fetch_fundamentals_pit_extend_company_id(IncomeTTM,
                                                                          [IncomeTTM.BIZTOTINCO,
                                                                           IncomeTTM.BIZTOTCOST,
                                                                           IncomeTTM.BIZINCO,
                                                                           IncomeTTM.SALESEXPE,
                                                                           IncomeTTM.MANAEXPE,
                                                                           IncomeTTM.FINEXPE,
                                                                           # IncomeTTM.INTEEXPE,
                                                                           IncomeTTM.ASSEIMPALOSS,
                                                                           IncomeTTM.PERPROFIT,
                                                                           IncomeTTM.TOTPROFIT,
                                                                           IncomeTTM.NETPROFIT,
                                                                           IncomeTTM.PARENETP,
                                                                           IncomeTTM.BIZTAX,
                                                                           IncomeTTM.NONOREVE,
                                                                           IncomeTTM.NONOEXPE,
                                                                           IncomeTTM.MINYSHARRIGH,
                                                                           IncomeTTM.INCOTAXEXPE,
                                                                           ], dates=[trade_date])
        for col in columns:
            if col in list(income_ttm_sets.keys()):
                income_ttm_sets = income_ttm_sets.drop(col, axis=1)
        income_ttm_sets = income_ttm_sets.rename(columns={'MINYSHARRIGH': 'minority_profit'})

        cash_flow_ttm_sets = engine.fetch_fundamentals_pit_extend_company_id(CashFlowTTM,
                                                                             [CashFlowTTM.MANANETR,
                                                                              CashFlowTTM.LABORGETCASH,
                                                                              CashFlowTTM.INVNETCASHFLOW,
                                                                              CashFlowTTM.FINNETCFLOW,
                                                                              CashFlowTTM.CASHNETI,
                                                                              ], dates=[trade_date])
        for col in columns:
            if col in list(cash_flow_ttm_sets.keys()):
                cash_flow_ttm_sets = cash_flow_ttm_sets.drop(col, axis=1)

        indicator_ttm_sets = engine.fetch_fundamentals_pit_extend_company_id(IndicatorTTM,
                                                                             [IndicatorTTM.OPGPMARGIN,
                                                                              IndicatorTTM.NPCUT,
                                                                              IndicatorTTM.NVALCHGIT,
                                                                              IndicatorTTM.EBITDA,
                                                                              IndicatorTTM.EBIT,
                                                                              IndicatorTTM.EBITFORP,
                                                                              ], dates=[trade_date])
        for col in columns:
            if col in list(indicator_ttm_sets.keys()):
                indicator_ttm_sets = indicator_ttm_sets.drop(col, axis=1)

        ttm_derivation = pd.merge(income_ttm_sets, cash_flow_ttm_sets, how='outer', on='security_code')
        ttm_derivation = pd.merge(indicator_ttm_sets, ttm_derivation, how='outer', on='security_code')

        return tp_detivation, ttm_derivation

    def process_calc_factor(self, trade_date, tp_derivation, ttm_derivation):
        tp_derivation = tp_derivation.set_index('security_code')
        ttm_derivation = ttm_derivation.set_index('security_code')

        # 读取目前涉及到的因子
        derivation = factor_basic_derivation.FactorBasicDerivation()
        # 因子计算
        factor_derivation = pd.DataFrame()
        factor_derivation['security_code'] = tp_derivation.index
        factor_derivation = factor_derivation.set_index('security_code')

        factor_derivation = derivation.FCFF(tp_derivation, factor_derivation)
        factor_derivation = derivation.FCFE(tp_derivation, factor_derivation)
        factor_derivation = derivation.NonRecGainLoss(tp_derivation, factor_derivation)
        factor_derivation = derivation.NetOptInc(tp_derivation, factor_derivation)
        factor_derivation = derivation.WorkingCap(tp_derivation, factor_derivation)
        # factor_derivation = derivation.TangibleAssets(tp_derivation, factor_derivation)
        factor_derivation = derivation.RetainedEarnings(tp_derivation, factor_derivation)
        factor_derivation = derivation.InterestBearingLiabilities(tp_derivation, factor_derivation)
        factor_derivation = derivation.NetDebt(tp_derivation, factor_derivation)
        factor_derivation = derivation.InterestFreeCurLb(tp_derivation, factor_derivation)
        factor_derivation = derivation.InterestFreeNonCurLb(tp_derivation, factor_derivation)
        # factor_derivation = derivation.DepAndAmo(tp_derivation, factor_derivation)
        factor_derivation = derivation.EquityPC(tp_derivation, factor_derivation)
        factor_derivation = derivation.TotalInvestedCap(tp_derivation, factor_derivation)
        factor_derivation = derivation.TotalAssets(tp_derivation, factor_derivation)
        factor_derivation = derivation.TotalFixedAssets(tp_derivation, factor_derivation)
        factor_derivation = derivation.TotalLib(tp_derivation, factor_derivation)
        factor_derivation = derivation.ShEquity(tp_derivation, factor_derivation)
        factor_derivation = derivation.CashAndCashEqu(tp_derivation, factor_derivation)
        # factor_derivation = derivation.EBIAT(tp_derivation, factor_derivation)
        factor_derivation = derivation.SalesTTM(ttm_derivation, factor_derivation)
        factor_derivation = derivation.TotalOptCostTTM(ttm_derivation, factor_derivation)
        factor_derivation = derivation.OptIncTTM(ttm_derivation, factor_derivation)
        factor_derivation = derivation.GrossMarginTTM(ttm_derivation, factor_derivation)
        factor_derivation = derivation.SalesExpensesTTM(ttm_derivation, factor_derivation)
        factor_derivation = derivation.AdmFeeTTM(ttm_derivation, factor_derivation)
        factor_derivation = derivation.FinFeeTTM(ttm_derivation, factor_derivation)
        factor_derivation = derivation.PerFeeTTM(ttm_derivation, factor_derivation)
        # factor_derivation = derivation.InterestExpTTM(ttm_derivation, factor_derivation)
        factor_derivation = derivation.MinorInterestTTM(ttm_derivation, factor_derivation)
        factor_derivation = derivation.AssetImpLossTTM(ttm_derivation, factor_derivation)
        factor_derivation = derivation.NetIncFromOptActTTM(ttm_derivation, factor_derivation)
        # factor_derivation = derivation.NetIncFromValueChgTTM(ttm_derivation, factor_derivation)
        factor_derivation = derivation.OptProfitTTM(ttm_derivation, factor_derivation)
        factor_derivation = derivation.NetNonOptIncAndExpTTM(ttm_derivation, factor_derivation)
        # factor_derivation = derivation.EBITTTM(ttm_derivation, factor_derivation)
        factor_derivation = derivation.IncTaxTTM(ttm_derivation, factor_derivation)
        factor_derivation = derivation.TotalProfTTM(ttm_derivation, factor_derivation)
        factor_derivation = derivation.NetIncTTM(ttm_derivation, factor_derivation)
        factor_derivation = derivation.NetProfToPSTTM(ttm_derivation, factor_derivation)
        factor_derivation = derivation.NetProfAfterNonRecGainsAndLossTTM(ttm_derivation, factor_derivation)
        # factor_derivation = derivation.EBITFORPTTM(ttm_derivation, factor_derivation)
        # factor_derivation = derivation.EBITDATTM(ttm_derivation, factor_derivation)
        factor_derivation = derivation.CashRecForSGAndPSTTM(ttm_derivation, factor_derivation)
        factor_derivation = derivation.NCFOTTM(ttm_derivation, factor_derivation)
        factor_derivation = derivation.NetCashFlowFromInvActTTM(ttm_derivation, factor_derivation)
        factor_derivation = derivation.NetCashFlowFromFundActTTM(ttm_derivation, factor_derivation)
        factor_derivation = derivation.NetCashFlowTTM(ttm_derivation, factor_derivation)
        factor_derivation = derivation.BusTaxAndSuchgTTM(ttm_derivation, factor_derivation)

        factor_derivation = factor_derivation.reset_index()
        factor_derivation['trade_date'] = str(trade_date)
        factor_derivation.replace([-np.inf, np.inf, None], np.nan, inplace=True)
        return factor_derivation

    def local_run(self, trade_date):
        print('当前交易日: %s' % trade_date)
        tic = time.time()
        tp_detivation, ttm_derivation = self.loading_data(trade_date)
        print('data load time %s' % (time.time()-tic))

        storage_engine = StorageEngine(self._url)
        result = self.process_calc_factor(trade_date, tp_detivation, ttm_derivation)
        print('cal_time %s' % (time.time() - tic))
        storage_engine.update_destdb(str(self._methods[-1]['packet'].split('.')[-1]), trade_date, result)
        # storage_engine.update_destdb('factor_basic_derivation', trade_date, result)

        
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

