#! /usr/bin/env python
# -*- coding: utf-8 -*-

"""
@version: 0.1
@author: zzh
@file: factor_volatility_value.py
@time: 2019-01-28 11:33
"""
import sys
sys.path.append("../")
sys.path.append("../../")
sys.path.append("../../../")
import argparse
import time
import collections
from datetime import datetime, timedelta
from factor.factor_base import FactorBase
from vision.fm.signletion_engine import *
from vision.file_unit.sk_daily_price import SKDailyPrice
from ultron.cluster.invoke.cache_data import cache_data
from factor import factor_volatility_value_task
import json
# from factor.utillities.trade_date import TradeDate


class FactorVolatilityValue(FactorBase):
    """
    收益风险
    """

    def __init__(self, name):
        super(FactorVolatilityValue, self).__init__(name)
        self._trade_date = TradeDate()

    # 构建因子表
    def create_dest_tables(self):
        """
        创建数据库表
        :return:
        """
        drop_sql = """drop table if exists `{0}`""".format(self._name)
        create_sql = """create table `{0}`(
                                    `id` varchar(32) NOT NULL,
                                    `symbol` varchar(24) NOT NULL,
                                    `trade_date` date NOT NULL,
                                    `variance_20d` decimal(19,4) NOT NULL,
                                    `variance_60d` decimal(19,4) NOT NULL,
                                    `variance_120d` decimal(19,4) NOT NULL,
                                    `kurtosis_20d` decimal(19,4) NOT NULL,
                                    `kurtosis_60d` decimal(19,4) NOT NULL,
                                    `kurtosis_120d` decimal(19,4) NOT NULL,
                                    `alpha_20d` decimal(19,4) NOT NULL,
                                    `alpha_60d` decimal(19,4) NOT NULL,
                                    `alpha_120d` decimal(19,4) NOT NULL,
                                    `beta_20d` decimal(19,4) NOT NULL,
                                    `beta_60d` decimal(19,4) NOT NULL,
                                    `beta_120d` decimal(19,4) NOT NULL,
                                    `sharp_20d` decimal(19,4) NOT NULL,
                                    `sharp_60d` decimal(19,4) NOT NULL,
                                    `sharp_120d` decimal(19,4) NOT NULL,
                                    `tr_20d` decimal(19,4) NOT NULL,
                                    `tr_60d` decimal(19,4) NOT NULL,
                                    `tr_120d` decimal(19,4) NOT NULL,
                                    `ir_20d` decimal(19,4) NOT NULL,
                                    `ir_60d` decimal(19,4) NOT NULL,
                                    `ir_120d` decimal(19,4) NOT NULL,
                                    `gain_variance_20d` decimal(19,4) NOT NULL,
                                    `gain_variance_60d` decimal(19,4) NOT NULL,
                                    `gain_variance_120d` decimal(19,4) NOT NULL,
                                    `loss_variance_20d` decimal(19,4) NOT NULL,
                                    `loss_variance_60d` decimal(19,4) NOT NULL,
                                    `loss_variance_120d` decimal(19,4) NOT NULL,
                                    `gain_loss_variance_ratio_20d` decimal(19,4) NOT NULL,
                                    `gain_loss_variance_ratio_60d` decimal(19,4) NOT NULL,
                                    `gain_loss_variance_ratio_120d` decimal(19,4) NOT NULL,
                                    `dastd_252d` decimal(19,4) NOT NULL,
                                    `ddnsr_12m` decimal(19,4) NOT NULL,
                                    `ddncr_12m` decimal(19,4) NOT NULL,
                                    `dvrat` decimal(19,4) NOT NULL,
                                    PRIMARY KEY(`id`,`trade_date`,`symbol`)
                                    )ENGINE=InnoDB DEFAULT CHARSET=utf8;""".format(self._name)
        super(FactorVolatilityValue, self)._create_tables(create_sql, drop_sql)

    def get_trade_date(self, trade_date, n):
        """
        获取当前时间前n年的时间点，且为交易日，如果非交易日，则往前提取最近的一天。
        :param trade_date: 当前交易日
        :param n:
        :return:
        """
        # print("trade_date %s" % trade_date)
        trade_date_sets = collections.OrderedDict(
            sorted(self._trade_date._trade_date_sets.items(), key=lambda t: t[0], reverse=False))

        time_array = datetime.strptime(str(trade_date), "%Y%m%d")
        time_array = time_array - timedelta(days=365) * n
        date_time = int(datetime.strftime(time_array, "%Y%m%d"))
        if date_time < min(trade_date_sets.keys()):
            # print('date_time %s is outof trade_date_sets' % date_time)
            return date_time
        else:
            while not date_time in trade_date_sets:
                date_time = date_time - 1
            # print('trade_date pre %s year %s' % (n, date_time))
            return date_time

    def get_basic_data(self, trade_date):
        """
        获取基础数据
        按天获取当天交易日所有股票的基础数据
        :param trade_date: 交易日
        :return:
        """
        # market_cap，circulating_market_cap，total_operating_revenue
        count = 300
        sk_daily_price_sets = get_sk_history_price([], trade_date, count, [SKDailyPrice.symbol,
                                                                           SKDailyPrice.trade_date, SKDailyPrice.open,
                                                                           SKDailyPrice.close, SKDailyPrice.high,
                                                                           SKDailyPrice.low])

        index_daily_price_sets = get_index_history_price(["000300.XSHG"], trade_date, count,
                                                         ["symbol", "trade_date", "close"])
        temp_price_sets = index_daily_price_sets[index_daily_price_sets.trade_date <= trade_date]
        return sk_daily_price_sets, temp_price_sets[:count]

    def prepare_calculate(self, trade_date):
        self.trade_date = trade_date

        tp_price_return, temp_price_sets = self.get_basic_data(trade_date)
        # tp_price_return.set_index('symbol', inplace=True)
        # tp_price_return['symbol'] = tp_price_return.index
        # symbol_sets = list(set(tp_price_return['symbol']))
        # tp_price_return_list = pd.DataFrame()
        #
        # for symbol in symbol_sets:
        #     if len(tp_price_return[tp_price_return['symbol'] == symbol]) < 3:
        #         continue
        #     tp_price_return_list = tp_price_return_list.append(
        #         tp_price_return.loc[symbol].sort_values(by='trade_date', ascending=True))

        if len(tp_price_return) <= 0:
            print("%s has no data" % trade_date)
            return
        else:
            session = str(int(time.time() * 1000000 + datetime.now().microsecond))
            data = {
                'total_data': tp_price_return.to_json(orient='records'),
                'index_daily_price_sets': temp_price_sets.to_json(orient='records')
            }
            cache_data.set_cache(session, 'volatility' + str(trade_date),
                                 json.dumps(data))
            # cache_data.set_cache(session, 'volatility' + str(trade_date) + '_a',
            #                      tp_price_return_list.to_json(orient='records'))
            # cache_data.set_cache(session, 'volatility' + str(trade_date) + '_b',
            #                      temp_price_sets.to_json(orient='records'))
            factor_volatility_value_task.calculate.delay(factor_name='volatility' + str(trade_date),
                                                         trade_date=trade_date,
                                                         session=session)

    def do_update(self, start_date, end_date, count):
        # 读取本地交易日
        trade_date_sets = self._trade_date.trade_date_sets_ago(start_date, end_date, count)
        for trade_date in trade_date_sets:
            print('因子计算日期： %s' % trade_date)
            self.prepare_calculate(trade_date)
        print('----->')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--start_date', type=int, default=20070101)
    parser.add_argument('--end_date', type=int, default=0)
    parser.add_argument('--count', type=int, default=1)
    parser.add_argument('--rebuild', type=bool, default=False)
    parser.add_argument('--update', type=bool, default=False)
    parser.add_argument('--schedule', type=bool, default=False)

    args = parser.parse_args()
    if args.end_date == 0:
        end_date = int(datetime.now().date().strftime('%Y%m%d'))
    else:
        end_date = args.end_date
    if args.rebuild:
        processor = FactorVolatilityValue('factor_volatility_value')
        processor.create_dest_tables()
        processor.do_update(args.start_date, end_date, args.count)
    if args.update:
        processor = FactorVolatilityValue('factor_volatility_value')
        processor.do_update(args.start_date, end_date, args.count)
