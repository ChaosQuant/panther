#!/usr/bin/env python
# coding=utf-8
import pdb
import sys
import os
import pandas as pd
from collections import OrderedDict
import collections

sys.path.append("../../")
from factor import factor_config


class TradeDate(object):

    def __init__(self):
        self._all_trade_file = factor_config.RECORD_BASE_DIR + 'trade_date/' + 'trade_date.csv'
        self._trade_date_sets = OrderedDict()
        self._load_trade_date()

    def _load_trade_date(self):
        if os.path.exists(self._all_trade_file):
            trade_date = pd.read_csv(self._all_trade_file, index_col=0)

        for index in trade_date.index:
            self._trade_date_sets[int(trade_date.loc[index].values[0])] = int(trade_date.loc[index].values[0])

        self._trade_date_sets = collections.OrderedDict(sorted(self._trade_date_sets.items(),
                                                               key=lambda t: t[0], reverse=False))

    def trade_date_sets_ago(self, start_date, end_date, count):
        sub_trade_date = []
        trade_date_sets = collections.OrderedDict(
            sorted(self._trade_date_sets.items(), key=lambda t: t[0], reverse=True))
        start_flag = 0
        start_count = 0
        for trade_date, values in trade_date_sets.items():
            # print(trade_date, start_date, end_date)
            if trade_date <= end_date:
                start_flag = 1

            if start_flag == 1:
                if start_date <= trade_date and start_count != count:
                    sub_trade_date.append(trade_date)
                    start_count += 1
                else:
                    break

        return sub_trade_date

    def trade_date_sets(self, start_date, end_date):
        sub_trade_date = []
        trade_date_sets = collections.OrderedDict(
            sorted(self._trade_date_sets.items(), key=lambda t: t[0], reverse=False))
        start_flag = 0
        for trade_date, values in trade_date_sets.items():
            print(trade_date, start_date, end_date)
            if trade_date == start_date:
                start_flag = 1

            if start_flag == 1:
                sub_trade_date.append(trade_date)

            if end_date <= trade_date:
                break
        return sub_trade_date

    def trade_date_sets_range(self, start_date, range_day, flag=1):
        start_count = 0
        sub_trade_date = []
        if flag == 0:
            trade_date_sets = collections.OrderedDict(
                sorted(self._trade_date_sets.items(), key=lambda t: t[0], reverse=False))
        else:
            trade_date_sets = collections.OrderedDict(
                sorted(self._trade_date_sets.items(), key=lambda t: t[0], reverse=True))
        start_flag = 0
        for trade_date, values in trade_date_sets.items():
            if trade_date == start_date:
                start_flag = 1

            if start_flag == 1:
                sub_trade_date.append(trade_date)
                start_count += 1

            if start_count >= range_day:
                break
        return sub_trade_date


if __name__ == '__main__':
    tr = TradeDate()
    result = tr.trade_date_sets_ago(20070101, 20190101, -1)
    print(result)
