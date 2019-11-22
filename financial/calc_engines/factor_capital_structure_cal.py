# -*- coding: utf-8 -*-

import pdb,importlib,inspect,time,datetime,json
# from PyFin.api import advanceDateByCalendar
# from data.polymerize import DBPolymerize
from data.storage_engine import StorageEngine
import time
import numpy as np
import pandas as pd
from datetime import datetime
from financial import factor_capital_structure

from data.model import BalanceMRQ

from vision.db.signletion_engine import *
from data.sqlengine import sqlEngine
# pd.set_option('display.max_columns', None)
# pd.set_option('display.max_rows', None)
# from ultron.cluster.invoke.cache_data import cache_data


class CalcEngine(object):
    def __init__(self, name, url, methods=[{'packet':'financial.factor_capital_structure','class':'FactorCapitalStructure'},]):
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
        columns = ['COMPCODE', 'PUBLISHDATE', 'ENDDATE', 'symbol', 'company_id', 'trade_date']
        engine = sqlEngine()
        balance_sets = engine.fetch_fundamentals_pit_extend_company_id(BalanceMRQ,
                                                                       [BalanceMRQ.TOTALNONCASSETS,
                                                                        BalanceMRQ.TOTASSET,
                                                                        BalanceMRQ.TOTALNONCLIAB,
                                                                        BalanceMRQ.LONGBORR,
                                                                        BalanceMRQ.INTAASSET,
                                                                        # BalanceMRQ.DEVEEXPE,
                                                                        BalanceMRQ.GOODWILL,
                                                                        BalanceMRQ.FIXEDASSENET,
                                                                        BalanceMRQ.ENGIMATE,
                                                                        BalanceMRQ.CONSPROG,
                                                                        BalanceMRQ.RIGHAGGR,
                                                                        BalanceMRQ.TOTCURRASSET,
                                                                        ], dates=[trade_date])
        for col in columns:
            if col in list(balance_sets.keys()):
                balance_sets = balance_sets.drop(col, axis=1)
        balance_sets = balance_sets.rename(columns={
            'TOTALNONCASSETS': 'total_non_current_assets',  # 非流动资产合计
            'TOTASSET': 'total_assets',  # 资产总计
            'TOTALNONCLIAB': 'total_non_current_liability',  # 非流动负债合计
            'LONGBORR': 'longterm_loan',  # 长期借款
            'INTAASSET': 'intangible_assets',  # 无形资产
            # 'DEVEEXPE': 'development_expenditure',  # 开发支出
            'GOODWILL': 'good_will',  # 商誉
            'FIXEDASSENET': 'fixed_assets',  # 固定资产
            'ENGIMATE': 'construction_materials',  # 工程物资
            'CONSPROG': 'constru_in_process',  # 在建工程
            'RIGHAGGR': 'total_owner_equities',  # 股东权益合计
            'TOTCURRASSET': 'total_current_assets',  # 流动资产合计
        })

        return balance_sets

    def process_calc_factor(self, trade_date, tp_management):
        tp_management = tp_management.set_index('security_code')

        # 读取目前涉及到的因子
        management = factor_capital_structure.FactorCapitalStructure()
        # 因子计算
        factor_management = pd.DataFrame()
        factor_management['security_code'] = tp_management.index
        factor_management = factor_management.set_index('security_code')

        factor_management = management.NonCurrAssetRatio(tp_management, factor_management)
        factor_management = management.LongDebtToAsset(tp_management, factor_management)
        factor_management = management.LongBorrToAssert(tp_management, factor_management)
        # factor_management = management.IntangibleAssetRatio(tp_management, factor_management)
        factor_management = management.FixAssetsRt(tp_management, factor_management)
        factor_management = management.EquityToAsset(tp_management, factor_management)
        factor_management = management.EquityToFixedAsset(tp_management, factor_management)
        factor_management = management.CurAssetsR(tp_management, factor_management)

        factor_management = factor_management.reset_index()
        factor_management['trade_date'] = str(trade_date)
        # factor_management.fillna(0, inplace=True)
        factor_management.replace([-np.inf, np.inf, None], np.nan, inplace=True)

        return factor_management

    def local_run(self, trade_date):
        print('当前交易日: %s' % trade_date)
        tic = time.time()
        balance_sets = self.loading_data(trade_date)
        print('data load time %s' % (time.time()-tic))

        storage_engine = StorageEngine(self._url)
        result = self.process_calc_factor(trade_date, balance_sets)
        print('cal_time %s' % (time.time() - tic))
        storage_engine.update_destdb(str(self._methods[-1]['packet'].split('.')[-1]), trade_date, result)
        # storage_engine.update_destdb('factor_capital_structure', trade_date, result)

        
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
#     tp_management = json_normalize(json.loads(str(content1, encoding='utf8')))
#     ttm_management = json_normalize(json.loads(str(content2, encoding='utf8')))
#     tp_management.set_index('security_code', inplace=True)
#     ttm_management.set_index('security_code', inplace=True)
#     print("len_tp_management_data {}".format(len(tp_management)))
#     print("len_ttm_management_data {}".format(len(ttm_management)))
#     total_cash_flow_data = {'tp_management': tp_management, 'ttm_management': ttm_management}
#     calculate(date_index, total_cash_flow_data, factor_name)


