#! /usr/bin/env python
# -*- coding: utf-8 -*-

"""
@version: ??
@author: zzh
@file: calc_engines.py
@time: 2019-09-10 14:46
"""
import json

from dateutil.relativedelta import relativedelta
import time
from pandas.io.json import json_normalize
from earning_expectation.factor_earning_expectation import FactorEarningExpectation
from data.storage_engine import StorageEngine
from PyFin.api import *
from ultron.cluster.invoke.cache_data import cache_data
from vision.table.stk_consensus_expectation import StkConsensusExpectation
from vision.table.stk_consensus_rating import StkConsensusRating
from vision.db.signletion_engine import *
from earning_expectation import app
import numpy as np


class CalcEngine(object):
    def __init__(self, name, url, methods=[
        {'packet': 'earning_expectation.factor_earning_expectation', 'class': 'FactorEarningExpectation'},
    ]):
        self._name = name
        self._methods = methods
        self._url = url

    def _func_sets(self, method):
        # 私有函数和保护函数过滤
        return list(filter(lambda x: not x.startswith('_') and callable(getattr(method, x)), dir(method)))

    def get_trade_date(self, trade_date, delta):
        """
        获取当前时间相对某时间之前的日期，且为交易日，如果非交易日，则往前提取最近的一天。
        :param trade_date: 当前交易日
        :param n:
        :return:
        """
        end_date = datetime.strptime(str(trade_date), "%Y-%m-%d") - delta
        freq = '1b'
        dates = makeSchedule(end_date, end_date, freq, 'china.sse', BizDayConventions.Preceding)
        return dates[0].strftime('%Y%m%d')

    def loadon_data(self, trade_date):
        # 读取目前涉及到的因子
        trade_date_pre_weak = self.get_trade_date(trade_date, relativedelta(weeks=+1))
        trade_date_pre_month_1 = self.get_trade_date(trade_date, relativedelta(months=+1))
        trade_date_pre_month_3 = self.get_trade_date(trade_date, relativedelta(months=+3))
        trade_date_pre_month_6 = self.get_trade_date(trade_date, relativedelta(months=+6))

        # Report Data
        q1 = query(StkConsensusExpectation.publish_date, StkConsensusExpectation.security_code,
                   StkConsensusExpectation.symbol,
                   StkConsensusExpectation.net_profit_fy1,
                   StkConsensusExpectation.net_profit_fy2,
                   StkConsensusExpectation.eps_fy1,
                   StkConsensusExpectation.eps_fy2,
                   StkConsensusExpectation.operating_revenue_fy1,
                   StkConsensusExpectation.operating_revenue_fy2,
                   StkConsensusExpectation.pe_fy1,
                   StkConsensusExpectation.pe_fy2,
                   StkConsensusExpectation.pb_fy1,
                   StkConsensusExpectation.pb_fy2,
                   StkConsensusExpectation.peg_fy1,
                   StkConsensusExpectation.peg_fy2).filter(StkConsensusExpectation.publish_date.in_(
            [trade_date, trade_date_pre_weak, trade_date_pre_month_1, trade_date_pre_month_3, trade_date_pre_month_6]))
        consensus_expectation_sets = get_fundamentals(q1)

        q2 = query(StkConsensusRating.publish_date, StkConsensusRating.symbol,
                   StkConsensusRating.rating2).filter(StkConsensusRating.publish_date.in_(
            [trade_date, trade_date_pre_weak, trade_date_pre_month_1, trade_date_pre_month_3, trade_date_pre_month_6]),
            StkConsensusRating.date_type == 5)
        consensus_rating_sets = get_fundamentals(q2)

        tp_earning = pd.merge(consensus_expectation_sets, consensus_rating_sets, on=['publish_date', 'symbol'],
                              how='left')
        tp_earning['publish_date'] = pd.to_datetime(tp_earning['publish_date'], format='%Y-%m-%d')
        # 原始数据单位为万元
        tp_earning['net_profit_fy1'] = tp_earning['net_profit_fy1'] * 10000
        tp_earning['net_profit_fy2'] = tp_earning['net_profit_fy2'] * 10000
        tp_earning['operating_revenue_fy1'] = tp_earning['operating_revenue_fy1'] * 10000
        tp_earning['operating_revenue_fy2'] = tp_earning['operating_revenue_fy2'] * 10000
        return tp_earning

    # 计算因子
    def local_run(self, trade_date):
        total_data = self.loadon_data(trade_date)
        storage_engine = StorageEngine(self._url)
        result = self.process_calc_factor(total_data, trade_date)
        storage_engine.update_destdb('factor_earning_expectation', trade_date, result)
        print('----')

    def remote_run(self, trade_date):
        total_data = self.loadon_data(trade_date)
        # 存储数据
        session = str(int(time.time() * 1000000 + datetime.now().microsecond))
        cache_data.set_cache(session, 'factor_earning_expectation', total_data.to_json(orient='records'))
        distributed_factor.delay(session, trade_date, json.dumps(self._methods), self._name)

    def distributed_factor(self, total_data, trade_date):
        storage_engine = StorageEngine(self._url)
        result = self.process_calc_factor(total_data, trade_date)
        storage_engine.update_destdb('factor_earning_expectation', trade_date, result)
        print('----')

    def process_calc_factor(self, tp_earning, trade_date):  # 计算对应因子
        print(trade_date)

        earning = FactorEarningExpectation()  # 注意, 这里的name要与client中新建table时的name一致, 不然回报错
        tp_earning = tp_earning.set_index('security_code')
        # 因子计算
        factor_earning_expect = pd.DataFrame()
        factor_earning_expect['security_code'] = tp_earning[
            tp_earning['publish_date'] == datetime.strptime(trade_date, "%Y-%m-%d")].index

        factor_earning_expect = earning.NPFY1(tp_earning, factor_earning_expect, trade_date)
        factor_earning_expect = earning.NPFY2(tp_earning, factor_earning_expect, trade_date)
        factor_earning_expect = earning.EPSFY1(tp_earning, factor_earning_expect, trade_date)
        factor_earning_expect = earning.EPSFY2(tp_earning, factor_earning_expect, trade_date)
        factor_earning_expect = earning.OptIncFY1(tp_earning, factor_earning_expect, trade_date)
        factor_earning_expect = earning.OptIncFY2(tp_earning, factor_earning_expect, trade_date)
        factor_earning_expect = earning.CEPEFY1(tp_earning, factor_earning_expect, trade_date)
        factor_earning_expect = earning.CEPEFY2(tp_earning, factor_earning_expect, trade_date)
        factor_earning_expect = earning.CEPBFY1(tp_earning, factor_earning_expect, trade_date)
        factor_earning_expect = earning.CEPBFY2(tp_earning, factor_earning_expect, trade_date)
        factor_earning_expect = earning.CEPEGFY1(tp_earning, factor_earning_expect, trade_date)
        factor_earning_expect = earning.CEPEGFY2(tp_earning, factor_earning_expect, trade_date)
        factor_earning_expect = earning.NPFY11WRT(tp_earning, factor_earning_expect, trade_date)
        factor_earning_expect = earning.NPFY11MRT(tp_earning, factor_earning_expect, trade_date)
        factor_earning_expect = earning.NPFY13MRT(tp_earning, factor_earning_expect, trade_date)
        factor_earning_expect = earning.NPFY16MRT(tp_earning, factor_earning_expect, trade_date)
        factor_earning_expect = earning.NPFY11WChg(tp_earning, factor_earning_expect, trade_date)
        factor_earning_expect = earning.NPFY11MChg(tp_earning, factor_earning_expect, trade_date)
        factor_earning_expect = earning.NPFY13MChg(tp_earning, factor_earning_expect, trade_date)
        factor_earning_expect = earning.NPFY16MChg(tp_earning, factor_earning_expect, trade_date)
        # factor_earning_expect = earning.NPFY1SDT(tp_earning, factor_earning_expect, trade_date)
        factor_earning_expect = earning.EPSFY11WRT(tp_earning, factor_earning_expect, trade_date)
        factor_earning_expect = earning.EPSFY11MRT(tp_earning, factor_earning_expect, trade_date)
        factor_earning_expect = earning.EPSFY13MRT(tp_earning, factor_earning_expect, trade_date)
        factor_earning_expect = earning.EPSFY16MRT(tp_earning, factor_earning_expect, trade_date)
        factor_earning_expect = earning.EPSFY11WChg(tp_earning, factor_earning_expect, trade_date)
        factor_earning_expect = earning.EPSFY11MChg(tp_earning, factor_earning_expect, trade_date)
        factor_earning_expect = earning.EPSFY13MChg(tp_earning, factor_earning_expect, trade_date)
        factor_earning_expect = earning.EPSFY16MChg(tp_earning, factor_earning_expect, trade_date)
        # factor_earning_expect = earning.EPSFY1SDT(tp_earning, factor_earning_expect, trade_date)
        factor_earning_expect = earning.ChgNPFY1FY2(tp_earning, factor_earning_expect, trade_date)
        factor_earning_expect = earning.ChgEPSFY1FY2(tp_earning, factor_earning_expect, trade_date)
        factor_earning_expect = earning.OptIncFY11WRT(tp_earning, factor_earning_expect, trade_date)
        factor_earning_expect = earning.OptIncFY11MRT(tp_earning, factor_earning_expect, trade_date)
        factor_earning_expect = earning.OptIncFY13MRT(tp_earning, factor_earning_expect, trade_date)
        factor_earning_expect = earning.OptIncFY16MRT(tp_earning, factor_earning_expect, trade_date)
        factor_earning_expect = earning.OptIncFY11WChg(tp_earning, factor_earning_expect, trade_date)
        factor_earning_expect = earning.OptIncFY11MChg(tp_earning, factor_earning_expect, trade_date)
        factor_earning_expect = earning.OptIncFY13MChg(tp_earning, factor_earning_expect, trade_date)
        factor_earning_expect = earning.OptIncFY16MChg(tp_earning, factor_earning_expect, trade_date)
        # factor_earning_expect = earning.OptIncFY1SDT(tp_earning, factor_earning_expect, trade_date)
        factor_earning_expect = earning.CERATINGRATE1W(tp_earning, factor_earning_expect, trade_date)
        factor_earning_expect = earning.CERATINGRATE1M(tp_earning, factor_earning_expect, trade_date)
        factor_earning_expect = earning.CERATINGRATE3M(tp_earning, factor_earning_expect, trade_date)
        factor_earning_expect = earning.CERATINGRATE6M(tp_earning, factor_earning_expect, trade_date)

        factor_earning_expect['trade_date'] = str(trade_date)
        factor_earning_expect.replace([-np.inf, np.inf], np.nan, inplace=True)
        return factor_earning_expect


@app.task
def distributed_factor(session, trade_date, packet_sets, name):
    calc_engine = CalcEngine(name, packet_sets)
    content = cache_data.get_cache(session, 'factor_earning_expectation')
    total_data = json_normalize(json.loads(content))
    calc_engine.distributed_factor(total_data, trade_date)
