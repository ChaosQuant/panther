# coding=utf-8
import time

from pandas.io.json import json_normalize, json
import pandas as pd
import math
import sys
sys.path.append("../")
sys.path.append("../../")
sys.path.append("../../../")
from basic_derivation import app
from basic_derivation.factor_base import FactorBase
from ultron.cluster.invoke.cache_data import cache_data


def lcap(tp_historical_value, factor_scale_value):
    """
    :param tp_historical_value:
    :param factor_scale_value:
    :return:
    """
    dependencies = ['market_cap']
    historical_value = tp_historical_value.loc[:, dependencies]
    historical_value['log_of_mkt_value'] = historical_value['market_cap'].map(lambda x: math.log(abs(x)))
    historical_value = historical_value.drop(columns=['market_cap'], axis=1)
    factor_scale_value = pd.merge(factor_scale_value, historical_value, on="symbol")
    return factor_scale_value


def lflo(tp_historical_value, factor_scale_value, dependencies=['circulating_market_cap']):
    """
    :param tp_historical_value:
    :param factor_scale_value:
    :return:
    """

    historical_value = tp_historical_value.loc[:, dependencies]

    historical_value['log_of_neg_mkt_value'] = historical_value[dependencies].map(lambda x: math.log(abs(x)))
    historical_value = historical_value.drop(columns=dependencies, axis=1)
    factor_scale_value = pd.merge(factor_scale_value, historical_value, on="symbol")
    return factor_scale_value


def nlsize(tp_historical_value, factor_scale_value, dependencies=['log_of_mkt_value']):
    """
    :param dependencies:
    :param tp_historical_value:
    :param factor_scale_value:
    :return:
    """

    historical_value = tp_historical_value.loc[:, dependencies]
    historical_value['nl_size'] = historical_value[dependencies].map(
        lambda x: pow(x, 3))
    historical_value = historical_value.drop(columns=dependencies, axis=1)
    factor_scale_value = pd.merge(factor_scale_value, historical_value, on="symbol")
    return factor_scale_value


def lst(tp_historical_value, factor_scale_value, dependencies=['total_operating_revenue']):
    """
    :param dependencies:
    :param tp_historical_value:
    :param factor_scale_value:
    :return:
    """

    historical_value = tp_historical_value.loc[:, dependencies]

    historical_value['log_sales_ttm'] = historical_value[dependencies].map(
        lambda x: math.log(abs(x)))
    historical_value = historical_value.drop(columns=dependencies, axis=1)
    factor_scale_value = pd.merge(factor_scale_value, historical_value, on="symbol")
    return factor_scale_value


def ltlqa(tp_historical_value, factor_scale_value, dependencies=['total_assets']):
    """
    :param dependencies:
    :param tp_historical_value:
    :param factor_scale_value:
    :return:
    """

    historical_value = tp_historical_value.loc[:, dependencies]

    historical_value['log_total_last_qua_assets'] = historical_value[dependencies].map(
        lambda x: math.log(abs(x)))
    historical_value = historical_value.drop(columns=dependencies, axis=1)
    factor_scale_value = pd.merge(factor_scale_value, historical_value, on="symbol")
    return factor_scale_value


@app.task(ignore_result=True)
def calculate(**kwargs):
    """
    :param trade_date:
    :return:
    """
    fb = FactorBase('factor_scale_value')
    print(kwargs)
    factor_name = kwargs['factor_name']
    session = kwargs['session']
    trade_date = kwargs['trade_date']
    content = cache_data.get_cache(session, factor_name)
    total_data = json_normalize(json.loads(content))
    print(len(total_data))

    factor_scale_value = lcap(total_data, total_data)
    factor_scale_value = lflo(factor_scale_value, factor_scale_value)
    factor_scale_value = nlsize(factor_scale_value, factor_scale_value)
    factor_scale_value = lst(factor_scale_value, factor_scale_value)
    factor_scale_value = ltlqa(factor_scale_value, factor_scale_value)
    factor_scale_value.rename(columns={
        'market_cap': 'MktValue',
        'circulating_market_cap': 'CirMktValue',
        'total_operating_revenue': 'SalesTTM',
        'total_assets': 'TotalAssets',
        'log_of_mkt_value': 'LogofMktValue',
        'log_of_neg_mkt_value': 'LogofNegMktValue',
        'nl_size': 'NLSIZE',
        'log_total_last_qua_assets': 'LogSalesTTM',
        'log_sales_ttm': 'LogTotalLastQuaAssets'
    }, inplace=True)
    factor_scale_value = factor_scale_value[[
        'symbol',
        'MktValue',
        'CirMktValue',
        'SalesTTM',
        'TotalAssets',
        'LogofMktValue',
        'LogofNegMktValue',
        'NLSIZE',
        'LogSalesTTM',
        'LogTotalLastQuaAssets'
    ]]

    factor_scale_value['id'] = factor_scale_value['symbol'] + str(trade_date)
    factor_scale_value['trade_date'] = str(trade_date)
    # super(HistoricalValue, self)._storage_data(factor_scale_value, trade_date)
    fb._storage_data(factor_scale_value, trade_date)

# calculate(factor_name='scale20180202', trade_date=20180202, session='1562054216473773')
# calculate(factor_name='scale20180202', trade_date=20180202, session='1562901137622956')
