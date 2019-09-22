# -*- coding: utf-8 -*-

import pdb,importlib,inspect,time,datetime,json
# from PyFin.api import advanceDateByCalendar
# from data.polymerize import DBPolymerize
from data.storage_engine import StorageEngine
import time
from datetime import timedelta
from financial import factor_operation_capacity

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
    def __init__(self, name, url, methods=[{'packet':'financial.factor_operation_capacity','class':'OperationCapacity'},]):
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
        engine = sqlEngine()
        columns = ['COMPCODE', 'PUBLISHDATE', 'ENDDATE', 'symbol', 'company_id', 'trade_date']
        ttm_cash_flow = engine.fetch_fundamentals_pit_extend_company_id(CashFlowTTM,
                                                                        [CashFlowTTM.MANANETR,
                                                                         CashFlowTTM.FINALCASHBALA,
                                                                         ], dates=[trade_date]).drop(columns, axis=1)

        ttm_cash_flow = ttm_cash_flow.rename(columns={
            'MANANETR': 'net_operate_cash_flow',  # 经营活动现金流量净额
            'FINALCASHBALA': 'cash_and_equivalents_at_end',  # 期末现金及现金等价物余额
        })

        ttm_income = engine.fetch_fundamentals_pit_extend_company_id(IncomeTTM,
                                                                     [IncomeTTM.BIZCOST,
                                                                      IncomeTTM.BIZINCO,
                                                                      ], dates=[trade_date]).drop(columns, axis=1)
        ttm_income = ttm_income.rename(columns={
            'BIZCOST': 'operating_cost',  # 营业成本
            'BIZINCO': 'operating_revenue',  # 营业收入
        })

        ttm_balance = engine.fetch_fundamentals_pit_extend_company_id(BalanceTTM,
                                                                      [BalanceTTM.ACCORECE,
                                                                       BalanceTTM.NOTESRECE,
                                                                       BalanceTTM.PREP,
                                                                       BalanceTTM.INVE,
                                                                       BalanceTTM.TOTCURRASSET,
                                                                       BalanceTTM.FIXEDASSENET,
                                                                       BalanceTTM.ENGIMATE,
                                                                       BalanceTTM.CONSPROG,
                                                                       BalanceTTM.TOTASSET,
                                                                       BalanceTTM.ADVAPAYM,
                                                                       ], dates=[trade_date]).drop(columns, axis=1)
        ttm_balance = ttm_balance.rename(columns={
            'NOTESRECE': 'bill_receivable',  # 应收票据
            'PREP': 'advance_payment',  # 预付款项
            'INVE': 'inventories',  # 存货
            'TOTCURRASSET': 'total_current_assets',  # 流动资产合计
            'FIXEDASSENET': 'fixed_assets',  # 固定资产
            'ENGIMATE': 'construction_materials',  # 工程物资
            'CONSPROG': 'constru_in_process',  # 在建工程
            'TOTASSET': 'total_assets',  # 资产总计
            'ADVAPAYM': 'advance_peceipts',  # 预收款项
            'ACCORECE': 'accounts_payable',  # 应付账款
        })

        ttm_operation_capacity = pd.merge(ttm_cash_flow, ttm_income, on='security_code')
        ttm_operation_capacity = pd.merge(ttm_balance, ttm_operation_capacity, on='security_code')
        return ttm_operation_capacity

    def process_calc_factor(self, trade_date, ttm_operation_capacity):
        ttm_operation_capacity = ttm_operation_capacity.set_index('security_code')
        capacity = factor_operation_capacity.OperationCapacity()

        # 因子计算
        factor_management = pd.DataFrame()
        factor_management['security_code'] = ttm_operation_capacity.index
        factor_management = factor_management.set_index('security_code')

        factor_management = capacity.AccPayablesRateTTM(ttm_operation_capacity, factor_management)
        factor_management = capacity.AccPayablesDaysTTM(ttm_operation_capacity, factor_management)
        factor_management = capacity.ARRateTTM(ttm_operation_capacity, factor_management)
        factor_management = capacity.ARDaysTTM(ttm_operation_capacity, factor_management)
        factor_management = capacity.InvRateTTM(ttm_operation_capacity, factor_management)
        factor_management = capacity.InvDaysTTM(ttm_operation_capacity, factor_management)
        factor_management = capacity.CashCovCycle(factor_management)
        factor_management = capacity.CurAssetsRtTTM(ttm_operation_capacity, factor_management)
        factor_management = capacity.FixAssetsRtTTM(ttm_operation_capacity, factor_management)
        factor_management = capacity.OptCycle(factor_management)
        factor_management = capacity.TotaAssetRtTTM(ttm_operation_capacity, factor_management)

        factor_management = factor_management.reset_index()
        factor_management['trade_date'] = str(trade_date)
        print(factor_management.head())
        return factor_management

    def local_run(self, trade_date):
        print('trade_date %s' % trade_date)
        tic = time.time()
        ttm_operation_capacity = self.loading_data(trade_date)
        print('data load time %s' % (time.time()-tic))

        storage_engine = StorageEngine(self._url)
        result = self.process_calc_factor(trade_date, ttm_operation_capacity)
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
#     print("management_kwargs: {}".format(kwargs))
#     date_index = kwargs['date_index']
#     session = kwargs['session']
#     content1 = cache_data.get_cache(session + str(date_index) + "1", date_index)
#     ttm_operation_capacity = json_normalize(json.loads(str(content1, encoding='utf8')))
#     ttm_operation_capacity.set_index('security_code', inplace=True)
#     print("len_tp_management_data {}".format(len(ttm_operation_capacity)))
#     calculate(date_index, ttm_operation_capacity)

