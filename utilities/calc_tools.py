#!/usr/bin/env python
# coding=utf-8
import numpy as np
import pandas as pd


class CalcTools(object):
    @classmethod
    def is_zero(cls, data_frame):
        return np.where(data_frame > -0.000001,
                        np.where(data_frame < 0.000001, True, False)
                        , False)

    def change_single(params):
        fundamentals_sets_year = params['fundamentals_sets_year']
        sub_columns = params['sub_columns']

        def year_update(df):
            df.loc[df.index, sub_columns] = df[sub_columns] - df[sub_columns].shift(-1).fillna(0)
            return df

        stock_list = list(set(fundamentals_sets_year['code']))
        new_fundamentals_sets = pd.DataFrame()
        i = 0
        for stock in stock_list:
            i += 1
            if i % 100 == 0:
                print(params['year'], ':', i, '/', len(stock_list))
            new_fundamentals_sets = new_fundamentals_sets.append(year_update(
                fundamentals_sets_year[
                    fundamentals_sets_year['code'] == stock]))
        return new_fundamentals_sets

    def change_single_by_symbol(params):
        fundamentals_sets = params['fundamentals_sets_symbol']
        sub_columns = params['sub_columns']

        def year_update(df):
            df.loc[df.index, sub_columns] = df[sub_columns] - df[sub_columns].shift(-1).fillna(0)
            return df

        new_fundamentals_sets = pd.DataFrame()
        year_list = list(set(fundamentals_sets['year']))
        year_list.sort(reverse=True)
        stock_list = list(set(fundamentals_sets['code']))
        i = 0
        for stock in stock_list:
            i += 1
            if i % 100 == 0:
                print('cpu', params['cpu'], ':', i, '/', len(stock_list))
            for year in year_list:
                fundamentals_sets_stock = fundamentals_sets[fundamentals_sets['code'] == stock]
                new_fundamentals_sets = new_fundamentals_sets.append(year_update(
                    fundamentals_sets_stock[
                        fundamentals_sets_stock['year'] == year]))
        return new_fundamentals_sets
