# -*- coding: utf-8 -*-

import pdb,importlib,inspect,time,datetime,json
# from PyFin.api import advanceDateByCalendar
# from data.polymerize import DBPolymerize
from data.storage_engine import StorageEngine
import time
import pandas as pd
from datetime import timedelta, datetime
from valuation_estimation import factor_valuation_estimation

from data.model import BalanceMRQ, BalanceReport
from data.model import CashFlowMRQ, CashFlowTTM
from data.model import IndicatorReport, IndicatorTTM
from data.model import IncomeTTM

from vision.db.signletion_engine import *
from vision.table.valuation import Valuation
from vision.table.industry import Industry
from data.sqlengine import sqlEngine
from utilities.sync_util import SyncUtil
# pd.set_option('display.max_columns', None)
# pd.set_option('display.max_rows', None)
# from ultron.cluster.invoke.cache_data import cache_data


class CalcEngine(object):
    def __init__(self, name, url, methods=[{'packet': 'valuation_estimation.factor_valuation_estimation', 'class': 'FactorValuationEstimation'}]):
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
        time_array = datetime.strptime(trade_date, "%Y-%m-%d")
        trade_date = datetime.strftime(time_array, '%Y%m%d')
        engine = sqlEngine()
        trade_date_1y = self.get_trade_date(trade_date, 1)
        trade_date_3y = self.get_trade_date(trade_date, 3)
        trade_date_4y = self.get_trade_date(trade_date, 4)
        trade_date_5y = self.get_trade_date(trade_date, 5)

        # report data
        columns = ['COMPCODE', 'PUBLISHDATE', 'ENDDATE', 'symbol', 'company_id', 'trade_date']
        indicator_sets = engine.fetch_fundamentals_pit_extend_company_id(IndicatorReport,
                                                                         [IndicatorReport.FCFF,
                                                                          ], dates=[trade_date])
        for column in columns:
            if column in list(indicator_sets.keys()):
                indicator_sets = indicator_sets.drop(column, axis=1)
        indicator_sets = indicator_sets.rename(columns={
            'FCFF': 'enterprise_fcfps',  # 企业自由现金流
        })

        balance_sets = engine.fetch_fundamentals_pit_extend_company_id(BalanceReport,
                                                                       [BalanceReport.TOTASSET,
                                                                        ], dates=[trade_date])
        for column in columns:
            if column in list(balance_sets.keys()):
                balance_sets = balance_sets.drop(column, axis=1)
        balance_sets = balance_sets.rename(columns={
            'TOTASSET': 'total_assets_report',  # 资产总计
        })
        valuation_report_sets = pd.merge(indicator_sets, balance_sets, how='outer', on='security_code')

        # MRQ data
        cash_flow_mrq = engine.fetch_fundamentals_pit_extend_company_id(CashFlowMRQ,
                                                                        [CashFlowMRQ.FINALCASHBALA,
                                                                         ], dates=[trade_date])
        for column in columns:
            if column in list(cash_flow_mrq.keys()):
                cash_flow_mrq = cash_flow_mrq.drop(column, axis=1)
        cash_flow_mrq = cash_flow_mrq.rename(columns={
            'FINALCASHBALA': 'cash_and_equivalents_at_end',  # 期末现金及现金等价物余额
        })

        balance_mrq = engine.fetch_fundamentals_pit_extend_company_id(BalanceMRQ,
                                                                      [BalanceMRQ.LONGBORR,
                                                                       BalanceMRQ.TOTASSET,
                                                                       BalanceMRQ.SHORTTERMBORR,
                                                                       BalanceMRQ.PARESHARRIGH,
                                                                       ], dates=[trade_date])
        for column in columns:
            if column in list(balance_mrq.keys()):
                balance_mrq = balance_mrq.drop(column, axis=1)
        balance_mrq = balance_mrq.rename(columns={
            'SHORTTERMBORR': 'shortterm_loan',  # 短期借款
            'LONGBORR': 'longterm_loan',  # 长期借款
            'TOTASSET': 'total_assets',  # 资产总计
            'PARESHARRIGH': 'equities_parent_company_owners',  # 归属于母公司股东权益合计
        })
        valuation_mrq = pd.merge(cash_flow_mrq, balance_mrq, on='security_code')

        # TTM data
        # 总市值合并到TTM数据中，
        cash_flow_ttm_sets = engine.fetch_fundamentals_pit_extend_company_id(CashFlowTTM,
                                                                             [CashFlowTTM.MANANETR,
                                                                              ], dates=[trade_date])
        for column in columns:
            if column in list(cash_flow_ttm_sets.keys()):
                cash_flow_ttm_sets = cash_flow_ttm_sets.drop(column, axis=1)
        cash_flow_ttm_sets = cash_flow_ttm_sets.rename(columns={
            'MANANETR': 'net_operate_cash_flow',  # 经营活动现金流量净额
        })

        indicator_ttm_sets = engine.fetch_fundamentals_pit_extend_company_id(IndicatorTTM,
                                                                             [IndicatorTTM.NETPROFITCUT,
                                                                              ], dates=[trade_date_1y])
        for column in columns:
            if column in list(indicator_ttm_sets.keys()):
                indicator_ttm_sets = indicator_ttm_sets.drop(column, axis=1)
        indicator_ttm_sets = indicator_ttm_sets.rename(columns={
            'NETPROFITCUT': 'net_profit_cut_pre',  # 扣除非经常性损益的净利润
        })

        income_ttm_sets = engine.fetch_fundamentals_pit_extend_company_id(IncomeTTM,
                                                                          [IncomeTTM.NETPROFIT,
                                                                           IncomeTTM.PARENETP,
                                                                           IncomeTTM.BIZTOTINCO,
                                                                           IncomeTTM.BIZINCO,
                                                                           IncomeTTM.TOTPROFIT,
                                                                           ], dates=[trade_date])
        for column in columns:
            if column in list(income_ttm_sets.keys()):
                income_ttm_sets = income_ttm_sets.drop(column, axis=1)
        income_ttm_sets = income_ttm_sets.rename(columns={
            'TOTPROFIT': 'total_profit',  # 利润总额 ttm
            'NETPROFIT': 'net_profit',  # 净利润
            'PARENETP': 'np_parent_company_owners',  # 归属于母公司所有者的净利润
            'BIZTOTINCO': 'total_operating_revenue',  # 营业总收入
            'BIZINCO': 'operating_revenue',  # 营业收入
        })

        income_ttm_sets_3 = engine.fetch_fundamentals_pit_extend_company_id(IncomeTTM,
                                                                            [IncomeTTM.PARENETP,
                                                                             ], dates=[trade_date_3y])
        for column in columns:
            if column in list(income_ttm_sets_3.keys()):
                income_ttm_sets_3 = income_ttm_sets_3.drop(column, axis=1)
        income_ttm_sets_3 = income_ttm_sets_3.rename(columns={
            'PARENETP': 'np_parent_company_owners_3',  # 归属于母公司所有者的净利润
        })

        income_ttm_sets_5 = engine.fetch_fundamentals_pit_extend_company_id(IncomeTTM,
                                                                            [IncomeTTM.PARENETP,
                                                                             ], dates=[trade_date_5y])
        for column in columns:
            if column in list(income_ttm_sets_5.keys()):
                income_ttm_sets_5 = income_ttm_sets_5.drop(column, axis=1)
        income_ttm_sets_5 = income_ttm_sets_5.rename(columns={
            'PARENETP': 'np_parent_company_owners_5',  # 归属于母公司所有者的净利润
        })

        valuation_ttm_sets = pd.merge(cash_flow_ttm_sets, income_ttm_sets, how='outer', on='security_code')
        valuation_ttm_sets = pd.merge(valuation_ttm_sets, indicator_ttm_sets, how='outer', on='security_code')
        valuation_ttm_sets = pd.merge(valuation_ttm_sets, income_ttm_sets_3, how='outer', on='security_code')
        valuation_ttm_sets = pd.merge(valuation_ttm_sets, income_ttm_sets_5, how='outer', on='security_code')

        # PS, PE, PB, PCF
        column = ['trade_date']
        valuation_sets = get_fundamentals(query(Valuation.security_code,
                                                Valuation.trade_date,
                                                Valuation.pe,
                                                Valuation.ps,
                                                Valuation.pb,
                                                Valuation.pcf,
                                                Valuation.market_cap,
                                                Valuation.circulating_market_cap)
                                          .filter(Valuation.trade_date.in_([trade_date])))
        for col in column:
            if col in list(valuation_sets.keys()):
                valuation_sets = valuation_sets.drop(col, axis=1)

        trade_date_6m = self.get_trade_date(trade_date, 1, 180)
        trade_date_3m = self.get_trade_date(trade_date, 1, 90)
        # trade_date_2m = self.get_trade_date(trade_date, 1, 60)
        trade_date_1m = self.get_trade_date(trade_date, 1, 20)

        pe_set = get_fundamentals(query(Valuation.security_code,
                                        Valuation.trade_date,
                                        Valuation.pe, ).filter(Valuation.trade_date.in_([trade_date])))
        for col in column:
            if col in list(pe_set.keys()):
                pe_set = pe_set.drop(col, axis=1)

        pe_sets_6m = get_fundamentals(query(Valuation.security_code,
                                            Valuation.trade_date,
                                            Valuation.pe)
                                      .filter(Valuation.trade_date.between(trade_date_6m, trade_date)))
        for col in column:
            if col in list(pe_sets_6m.keys()):
                pe_sets_6m = pe_sets_6m.drop(col, axis=1)
        pe_sets_6m = pe_sets_6m.groupby('security_code').mean().rename(columns={'pe': 'pe_mean_6m'})

        pe_sets_3m = get_fundamentals(query(Valuation.security_code,
                                            Valuation.trade_date,
                                            Valuation.pe)
                                      .filter(Valuation.trade_date.between(trade_date_3m, trade_date)))
        for col in column:
            if col in list(pe_sets_3m.keys()):
                pe_sets_3m = pe_sets_3m.drop(col, axis=1)
        pe_sets_3m = pe_sets_3m.groupby('security_code').mean().rename(columns={'pe': 'pe_mean_3m'})

        pe_sets_2m = get_fundamentals(query(Valuation.security_code,
                                            Valuation.trade_date,
                                            Valuation.pe)
                                      .filter(Valuation.trade_date.between(trade_date_1m, trade_date)))
        for col in column:
            if col in list(pe_sets_2m.keys()):
                pe_sets_2m = pe_sets_2m.drop(col, axis=1)
        pe_sets_2m = pe_sets_2m.groupby('security_code').mean().rename(columns={'pe': 'pe_mean_1m'})

        pe_sets_1y = get_fundamentals(query(Valuation.security_code,
                                            Valuation.trade_date,
                                            Valuation.pe)
                                      .filter(Valuation.trade_date.between(trade_date_1y, trade_date)))
        for col in column:
            if col in list(pe_sets_1y.keys()):
                pe_sets_1y = pe_sets_1y.drop(col, axis=1)
        pe_sets_1y = pe_sets_1y.groupby('security_code').mean().rename(columns={'pe': 'pe_mean_1y'})

        pe_sets = pd.merge(pe_sets_6m, pe_sets_3m, how='outer', on='security_code')
        pe_sets = pd.merge(pe_sets, pe_sets_2m, how='outer', on='security_code')
        pe_sets = pd.merge(pe_sets, pe_sets_1y, how='outer', on='security_code')
        pe_sets = pd.merge(pe_sets, pe_set, how='outer', on='security_code')

        industry_set = ['801010', '801020', '801030', '801040', '801050', '801080', '801110', '801120', '801130',
                        '801140', '801150', '801160', '801170', '801180', '801200', '801210', '801230', '801710',
                        '801720', '801730', '801740', '801750', '801760', '801770', '801780', '801790', '801880',
                        '801890']
        column_sw = ['trade_date', 'symbol', 'company_id']
        sw_indu = get_fundamentals_extend_internal(query(Industry.trade_date,
                                                         Industry.symbol,
                                                         Industry.isymbol)
                                                   .filter(Industry.trade_date.in_([trade_date])),
                                                   internal_type='symbol')
        for col in column_sw:
            if col in list(sw_indu.keys()):
                sw_indu = sw_indu.drop(col, axis=1)
        sw_indu = sw_indu[sw_indu['isymbol'].isin(industry_set)]

        valuation_sets = pd.merge(valuation_sets, valuation_report_sets, how='outer', on='security_code')
        valuation_sets = pd.merge(valuation_sets, valuation_mrq, how='outer', on='security_code')
        valuation_sets = pd.merge(valuation_sets, valuation_ttm_sets, how='outer', on='security_code')

        return valuation_sets, sw_indu, pe_sets

    def process_calc_factor(self, trade_date, valuation_sets, pe_sets, sw_industry):
        valuation_sets = valuation_sets.set_index('security_code')
        pe_sets = pe_sets.set_index('security_code')
        historical_value = factor_valuation_estimation.FactorValuationEstimation()

        factor_historical_value = pd.DataFrame()
        factor_historical_value['security_code'] = valuation_sets.index
        factor_historical_value = factor_historical_value.set_index('security_code')

        # psindu
        factor_historical_value = historical_value.LogofMktValue(valuation_sets, factor_historical_value)
        factor_historical_value = historical_value.LogofNegMktValue(valuation_sets, factor_historical_value)
        factor_historical_value = historical_value.NLSIZE(valuation_sets, factor_historical_value)
        factor_historical_value = historical_value.MrktCapToCorFreeCashFlow(valuation_sets, factor_historical_value)
        factor_historical_value = historical_value.PBAvgOnSW1(valuation_sets, sw_industry, factor_historical_value)
        factor_historical_value = historical_value.PBStdOnSW1(valuation_sets, sw_industry, factor_historical_value)
        factor_historical_value = historical_value.PBIndu(valuation_sets, factor_historical_value)
        factor_historical_value = historical_value.PEToAvg6M(pe_sets, factor_historical_value)
        factor_historical_value = historical_value.PEToAvg3M(pe_sets, factor_historical_value)
        factor_historical_value = historical_value.PEToAvg1M(pe_sets, factor_historical_value)
        factor_historical_value = historical_value.PEToAvg1Y(pe_sets, factor_historical_value)
        factor_historical_value = historical_value.MktValue(valuation_sets, factor_historical_value)
        factor_historical_value = historical_value.CirMktValue(valuation_sets, factor_historical_value)
        factor_historical_value = historical_value.LogTotalAssets(valuation_sets, factor_historical_value)
        factor_historical_value = historical_value.BMInduAvgOnSW1(valuation_sets, sw_industry, factor_historical_value)
        factor_historical_value = historical_value.BMInduSTDOnSW1(valuation_sets, sw_industry, factor_historical_value)
        factor_historical_value = historical_value.BookValueToIndu(valuation_sets, factor_historical_value)
        factor_historical_value = historical_value.TotalAssetsToEnterpriseValue(valuation_sets, factor_historical_value)
        factor_historical_value = historical_value.LogSalesTTM(valuation_sets, factor_historical_value)
        factor_historical_value = historical_value.PCFToOptCashflowTTM(valuation_sets, factor_historical_value)
        factor_historical_value = historical_value.EPTTM(valuation_sets, factor_historical_value)
        factor_historical_value = historical_value.PECutTTM(valuation_sets, factor_historical_value)
        factor_historical_value = historical_value.PEAvgOnSW1(valuation_sets, sw_industry, factor_historical_value)
        factor_historical_value = historical_value.PEStdOnSW1(valuation_sets, sw_industry, factor_historical_value)
        factor_historical_value = historical_value.PSAvgOnSW1(valuation_sets, sw_industry, factor_historical_value)
        factor_historical_value = historical_value.PSStdOnSW1(valuation_sets, sw_industry, factor_historical_value)
        factor_historical_value = historical_value.PCFAvgOnSW1(valuation_sets, sw_industry, factor_historical_value)
        factor_historical_value = historical_value.PCFStdOnSW1(valuation_sets, sw_industry, factor_historical_value)
        factor_historical_value = historical_value.PEIndu(valuation_sets, factor_historical_value)
        factor_historical_value = historical_value.PSIndu(valuation_sets, factor_historical_value)
        factor_historical_value = historical_value.PCFIndu(valuation_sets, factor_historical_value)
        factor_historical_value = historical_value.TotalMrktAVGToEBIDAOnSW1(valuation_sets, sw_industry, factor_historical_value)
        factor_historical_value = historical_value.TotalMrktSTDToEBIDAOnSW1(valuation_sets, sw_industry, factor_historical_value)
        factor_historical_value = historical_value.TotalMrktToEBIDATTM(valuation_sets, factor_historical_value)
        factor_historical_value = historical_value.PEG3YChgTTM(valuation_sets, factor_historical_value)
        factor_historical_value = historical_value.PEG5YChgTTM(valuation_sets, factor_historical_value)
        factor_historical_value = historical_value.CEToPTTM(valuation_sets, factor_historical_value)
        factor_historical_value = historical_value.RevToMrktRatioTTM(valuation_sets, factor_historical_value)
        factor_historical_value = historical_value.OptIncToEnterpriseValueTTM(valuation_sets, factor_historical_value)

        # factor_historical_value = factor_historical_value.reset_index()
        factor_historical_value['trade_date'] = str(trade_date)
        return factor_historical_value
    
    def local_run(self, trade_date):
        print('当前交易日； %s' % trade_date)
        tic = time.time()
        valuation_sets, sw_industry, pe_sets = self.loading_data(trade_date)
        print('data load time %s' % (time.time()-tic))
        # 保存
        storage_engine = StorageEngine(self._url)
        result = self.process_calc_factor(trade_date, valuation_sets, pe_sets, sw_industry)
        print('cal_time %s' % (time.time() - tic))
        storage_engine.update_destdb(str(self._methods[-1]['packet'].split('.')[-1]), trade_date, result)
        print('----------------->')
        # storage_engine.update_destdb('factor_valuation', trade_date, result)

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

# # @app.task()
# def factor_calculate(**kwargs):
#     print("history_value_kwargs: {}".format(kwargs))
#     date_index = kwargs['date_index']
#     session = kwargs['session']
#     # historical_value = Valuation('factor_historical_value')  # 注意, 这里的name要与client中新建table时的name一致, 不然回报错
#     content = cache_data.get_cache(session + str(date_index), date_index)
#     total_history_data = json_normalize(json.loads(str(content, encoding='utf8')))
#     print("len_history_value_data {}".format(len(total_history_data)))
#     calculate(date_index, total_history_data)

