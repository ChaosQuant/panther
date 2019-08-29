# coding=utf-8

from pandas.io.json import json_normalize, json
import pandas as pd
import numpy as np
from scipy import stats
import sys
sys.path.append("..")
from factor import app
from factor.factor_base import FactorBase
from ultron.cluster.invoke.cache_data import cache_data

from vision.fm.signletion_engine import *

_columns_list = ['variance_20d', 'variance_60d', 'variance_120d', 'kurtosis_20d',
                 'kurtosis_60d', 'kurtosis_120d', 'alpha_20d', 'alpha_60d',
                 'alpha_120d', 'beta_20d', 'beta_60d', 'beta_120d', 'sharp_20d',
                 'sharp_60d', 'sharp_120d', 'tr_20d', 'tr_60d',
                 'tr_120d', 'ir_20d', 'ir_60d', 'ir_120d',
                 'gain_variance_20d', 'gain_variance_60d', 'gain_variance_120d', 'loss_variance_20d',
                 'loss_variance_60d',
                 'loss_variance_120d', 'gain_loss_variance_ratio_20d', 'gain_loss_variance_ratio_60d',
                 'gain_loss_variance_ratio_120d',
                 'dastd_252d', 'ddnsr_12m', 'ddncr_12m', 'dvrat']
rf = 0.04
dayrf = rf / 252
trade_date = None
tp_index = None
golbal_obj = {
    'rf': 0.04,
    'dayrf': dayrf,
    'tp_index': tp_index,
    'trade_date': trade_date
}


def get_index_dict():
    tp_index_dict = {}
    tp_index = golbal_obj['tp_index']
    index_sets = list(set(tp_index.index))
    for index in index_sets:
        if len(tp_index[tp_index.index == index]) < 3:
            continue
        tp_index_dict[index] = tp_index.loc[index].sort_values(by="trade_date", ascending=True)
    return tp_index_dict


def variancex(tp_price_flow, x):
    columns_list = ["close"]
    price = tp_price_flow.loc[:, columns_list]
    variance = np.var(price.close.pct_change().iloc[-x:]) * 250
    return variance


def variance_20d(tp_price_flow):
    return variancex(tp_price_flow, 20)


def variance_60d(tp_price_flow):
    return variancex(tp_price_flow, 60)


def variance_120d(tp_price_flow):
    return variancex(tp_price_flow, 120)


def kurtosis_xd(tp_price_flow, x):
    columns_list = ["close"]
    price = tp_price_flow.loc[:, columns_list]
    kurt = stats.kurtosis(price.close.pct_change().iloc[-x:])
    return kurt


def kurtosis_20d(tp_price_flow):
    return kurtosis_xd(tp_price_flow, 20)


def kurtosis_60d(tp_price_flow):
    return kurtosis_xd(tp_price_flow, 60)


def kurtosis_120d(tp_price_flow):
    return kurtosis_xd(tp_price_flow, 120)


def alpha_xd(tp_price_flow, x):
    tp_index_dict = get_index_dict()
    columns_list = ["trade_date", "close"]
    price = tp_price_flow.loc[:, columns_list].reset_index()
    price_close_len = len(price.close.pct_change())
    if (x >= price_close_len):
        x = price_close_len - 1
    index = tp_index_dict["000300.XSHG"].loc[:, columns_list].reset_index()
    # index["trade_date"] = index['trade_date'].apply(lambda x: int(x[0:4] + x[5:7] + x[8:]))
    total_df = pd.merge(index, price, on="trade_date")
    total_df["close_x"] = total_df["close_x"].pct_change()
    total_df["close_y"] = total_df["close_y"].pct_change()
    total_df = total_df[-x:]
    beta = beta_xd(tp_price_flow, x)
    er = np.mean(total_df["close_y"])
    total_df["close_x"] = total_df["close_x"].map(lambda x: x - dayrf)
    erm_rf = np.mean(total_df["close_x"])
    alpha = (er - dayrf) - beta * erm_rf
    return alpha


def alpha_20d(tp_price_flow):
    return alpha_xd(tp_price_flow, 20)


def alpha_60d(tp_price_flow):
    return alpha_xd(tp_price_flow, 60)


def alpha_120d(tp_price_flow):
    return alpha_xd(tp_price_flow, 120)


def beta_xd(tp_price_flow, x):
    tp_index_dict = get_index_dict()
    columns_list = ["close"]
    price = tp_price_flow.loc[:, columns_list]
    price = price[price.close > 0]
    index = tp_index_dict["000300.XSHG"].loc[:, columns_list]
    price_close_len = len(price.close.pct_change())
    if (x >= price_close_len):
        x = price_close_len - 1
    cov_mat = np.cov(price.close.pct_change().iloc[-x:], index.close.pct_change().iloc[-x:])
    beta = cov_mat[0][1] / cov_mat[1][1]
    return beta


def beta_20d(tp_price_flow):
    return beta_xd(tp_price_flow, 20)


def beta_60d(tp_price_flow):
    return beta_xd(tp_price_flow, 60)


def beta_120d(tp_price_flow):
    return beta_xd(tp_price_flow, 120)


def sharp_xd(tp_price_flow, x):
    columns_list = ["close"]
    price = tp_price_flow.loc[:, columns_list]
    avg = np.mean(price.close.iloc[-x:].pct_change())
    stdValue = np.std(price.close.iloc[-x:])
    if (stdValue > 0.001 or stdValue < -0.001):
        result = (avg - rf) / stdValue
    else:
        result = 0.0
    return result


def sharp_20d(tp_price_flow):
    return sharp_xd(tp_price_flow, 20)


def sharp_60d(tp_price_flow):
    return sharp_xd(tp_price_flow, 60)


def sharp_120d(tp_price_flow):
    return sharp_xd(tp_price_flow, 120)


def tr_xd(tp_price_flow, x):
    # tp_index_dict = get_index_dict()
    columns_list = ["close"]
    price = tp_price_flow.loc[:, columns_list]
    avg = np.mean(price.close.iloc[-x:])
    bValue = beta_xd(tp_price_flow, x)
    result = (avg - rf) / bValue
    return result


def tr_20d(tp_price_flow):
    return tr_xd(tp_price_flow, 20)


def tr_60d(tp_price_flow):
    return tr_xd(tp_price_flow, 60)


def tr_120d(tp_price_flow):
    return tr_xd(tp_price_flow, 120)


def ir_xd(tp_price_flow, x):
    tp_index_dict = get_index_dict()
    columns_list = ["close"]
    price = tp_price_flow.loc[:, columns_list]
    price_close_len = len(price.close.pct_change())
    if (x >= price_close_len):
        x = price_close_len - 1
    index = tp_index_dict["000300.XSHG"].loc[:, columns_list]
    diff = price.close.pct_change().values[-x:] - index.close.pct_change().values[-x:]
    diff_mean = np.mean(diff[-x:])
    diff_std = np.std(diff[-x:])
    if (diff_std > 0.001 or diff_std < -0.001):
        ir = diff_mean / diff_std
    else:
        ir = 0.0
    return ir


def ir_20d(tp_price_flow):
    return ir_xd(tp_price_flow, 20)


def ir_60d(tp_price_flow):
    return ir_xd(tp_price_flow, 60)


def ir_120d(tp_price_flow):
    return ir_xd(tp_price_flow, 120)


def gain_variance_xd(tp_price_flow, x):
    columns_list = ["close"]
    price = tp_price_flow.loc[:, columns_list]
    price = price[price.close > 0]
    close_series = price.close.pct_change()
    close_series = close_series[close_series >= 0]
    close_series2 = np.power(close_series, 2)
    close_mean = np.mean(close_series.values[-x:])
    close_mean2 = np.mean(close_series2.values[-x:])
    result = (close_mean2 - close_mean * close_mean) * 250
    return result


def gain_variance_20d(tp_price_flow):
    return gain_variance_xd(tp_price_flow, 20)


def gain_variance_60d(tp_price_flow):
    return gain_variance_xd(tp_price_flow, 60)


def gain_variance_120d(tp_price_flow):
    return gain_variance_xd(tp_price_flow, 120)


def loss_variance_xd(tp_price_flow, x):
    columns_list = ["close"]
    price = tp_price_flow.loc[:, columns_list]
    close_series = price.close.pct_change()
    close_series = close_series[close_series <= 0]
    close_series2 = np.power(close_series, 2)
    close_mean = np.mean(close_series.values[-x:])
    close_mean2 = np.mean(close_series2.values[-x:])
    result = (close_mean2 - close_mean * close_mean) * 250
    return result


def loss_variance_20d(tp_price_flow):
    return loss_variance_xd(tp_price_flow, 20)


def loss_variance_60d(tp_price_flow):
    return loss_variance_xd(tp_price_flow, 60)


def loss_variance_120d(tp_price_flow):
    return loss_variance_xd(tp_price_flow, 120)


def gain_loss_variance_ratio_20d(tp_price_flow):
    return gain_variance_20d(tp_price_flow) / loss_variance_20d(tp_price_flow)


def gain_loss_variance_ratio_60d(tp_price_flow):
    return gain_variance_60d(tp_price_flow) / loss_variance_60d(tp_price_flow)


def gain_loss_variance_ratio_120d(tp_price_flow):
    return gain_variance_120d(tp_price_flow) / loss_variance_120d(tp_price_flow)


def dastd_252d(tp_price_flow):
    columns_list = ["close"]
    price = tp_price_flow.loc[:, columns_list]
    price['close_pct_change'] = price.close.pct_change()
    price['close_pct_change'] = price['close_pct_change'].map(lambda x: x - dayrf)
    close_pct_change = price['close_pct_change'].iloc[-252:]
    std_value = np.std(close_pct_change)
    return std_value


def ddnsr_12m(tp_price_flow):
    tp_index_dict = get_index_dict()
    columns_list = ["trade_date", "close"]
    price = tp_price_flow.loc[:, columns_list].reset_index()
    index = tp_index_dict["000300.XSHG"].loc[:, columns_list].reset_index()
    # index["trade_date"] = index['trade_date'].apply(lambda x: int(x[0:4] + x[5:7] + x[8:]))
    total_df = pd.merge(index, price, on="trade_date")
    total_df["close_x"] = total_df["close_x"].pct_change()
    total_df["close_y"] = total_df["close_y"].pct_change()
    total_df = total_df[-252:]
    total_df = total_df[total_df['close_x'] < 0]
    stdr = np.std(total_df["close_y"])
    stdrm = np.std(total_df["close_x"])
    if (stdrm > 0.001 or stdrm < -0.001):
        result = stdr / stdrm
    else:
        result = 0.0
    return result


def ddncr_12m(tp_price_flow):
    tp_index_dict = get_index_dict()
    columns_list = ["trade_date", "close"]
    price = tp_price_flow.loc[:, columns_list].reset_index()
    index = tp_index_dict["000300.XSHG"].loc[:, columns_list].reset_index()
    # index["trade_date"] = index['trade_date'].apply(lambda x: int(x[0:4] + x[5:7] + x[8:]))
    total_df = pd.merge(index, price, on="trade_date")
    total_df["close_x"] = total_df["close_x"].pct_change()
    total_df["close_y"] = total_df["close_y"].pct_change()
    total_df = total_df[-252:]
    total_df = total_df[total_df['close_x'] < 0]
    stdr = np.std(total_df["close_y"])
    stdrm = np.std(total_df["close_x"])
    cov_mat = np.cov(total_df["close_y"], total_df["close_x"])
    stdValue = stdr * stdrm
    if (stdValue > 0.001 or stdValue < -0.001):
        result = cov_mat[0][1] / stdValue
    else:
        result = 0.0
    return result


def dvrat(tp_price_flow):
    q = 10
    t = 252 * 2
    m = q * (t - q + 1) * (1 - q / t)
    columns_list = ["close"]
    price = tp_price_flow.loc[:, columns_list]
    price = price[price.close > 0]
    price['close_pct_change'] = price.close.pct_change()
    close_pct_change = price['close_pct_change'].iloc[-t:]
    count = 0
    temp_list = []
    temp_items = []
    temp_e2s = []
    for item in close_pct_change:
        if (str(item) == "nan"):
            continue
        temp_e2s.append(item * item)
        temp_items.append(item)
        count = count + 1
        if (count >= 10):
            value = np.sum(temp_items)
            if (str(value) != "nan"):
                temp_list.append(value * value)
            temp_items = temp_items[1:]
    e2q = np.sum(temp_list) / m
    e2s = np.sum(temp_e2s) / (t - 1)
    result = e2q / e2s - 1
    return result


def symbol_calcu(tp_price):
    tp_price.set_index('symbol', inplace=True)
    tp_price = tp_price[tp_price['close'] > 0]
    price_return = {}
    for func_name in _columns_list:
        # func = getattr(func_name, None)
        func = globals().get(func_name)
        if func:
            price_return[func_name] = func(tp_price)
    price_return['symbol'] = tp_price.index[0]  # .decode("unicode_escape").encode("utf8")
    return price_return


def get_index_history_price_local(universe, end_date, count, entities=None):
    global index_daily_price_sets
    if (index_daily_price_sets is None):
        index_daily_price_sets = get_index_history_price(universe, end_date, 450, entities)
    temp_price_sets = index_daily_price_sets[index_daily_price_sets.trade_date <= end_date]
    return temp_price_sets[:count]


@app.task(ignore_result=True)
def calculate(**kwargs):
    fb = FactorBase('factor_volatility_value')
    print(kwargs)
    factor_name = kwargs['factor_name']
    session = kwargs['session']
    trade_date = kwargs['trade_date']
    golbal_obj['trade_date'] = trade_date
    content = cache_data.get_cache(session, factor_name)
    data = json.loads(content)

    total_data = json_normalize(json.loads(data['total_data']))
    index_daily_price_sets = json_normalize(json.loads(data['index_daily_price_sets']))

    index_daily_price_sets.set_index("symbol", inplace=True)
    golbal_obj['tp_index'] = index_daily_price_sets

    # content_a = cache_data.get_cache(session, factor_name+'_a')
    # content_b = cache_data.get_cache(session, factor_name+'_b')
    # total_data = json_normalize(json.loads(content_a))
    # golbal_obj['index_daily_price_sets'] = json_normalize(json.loads(content_b))
    print(len(total_data))
    print(len(golbal_obj['tp_index']))
    total_data.sort_values(by=['symbol', 'trade_date'], ascending=True, inplace=True)
    symbol_sets = list(set(total_data['symbol']))
    symbol_sets.sort()
    factor_list = []

    for symbol in symbol_sets:
        tp_price = total_data[total_data['symbol'] == symbol]
        if (tp_price.iloc[-1]['close'] != 0 and len(tp_price) > 120):
            factor_list.append(symbol_calcu(tp_price))

    factor_volatility_value = pd.DataFrame(factor_list)
    factor_volatility_value.rename(columns={
        'variance_20d': 'Variance20D', 'variance_60d': 'Variance60D', 'variance_120d': 'Variance120D',
        'kurtosis_20d': 'Kurtosis20D', 'kurtosis_60d': 'Kurtosis60D', 'kurtosis_120d': 'Kurtosis120D',
        'alpha_20d': 'Alpha20D', 'alpha_60d': 'Alpha60D', 'alpha_120d': 'Alpha120D',
        'beta_20d': 'Beta20D', 'beta_60d': 'Beta60D', 'beta_120d': 'Beta120D',
        'sharp_20d': 'Sharp20D', 'sharp_60d': 'Sharp60D', 'sharp_120d': 'Sharp120D',
        'tr_20d': 'TR20D', 'tr_60d': 'TR60D', 'tr_120d': 'TR120D',
        'ir_20d': 'IR20D', 'ir_60d': 'IR60D', 'ir_120d': 'IR120D',
        'gain_variance_20d': 'GainVariance20D', 'gain_variance_60d': 'GainVariance60D',
        'gain_variance_120d': 'GainVariance120D',
        'loss_variance_20d': 'LossVariance20D', 'loss_variance_60d': 'LossVariance60D',
        'loss_variance_120d': 'LossVariance120D',
        'gain_loss_variance_ratio_20d': 'GainLossVarianceRatio20D',
        'gain_loss_variance_ratio_60d': 'GainLossVarianceRatio60D',
        'gain_loss_variance_ratio_120d': 'GainLossVarianceRatio120D',
        'dastd_252d': 'DailyReturnSTD252D', 'ddnsr_12m': 'DDNSR12M', 'ddncr_12m': 'DDNCR12M', 'dvrat': 'DVRAT'
    }, inplace=True)
    factor_volatility_value = factor_volatility_value[[
        'symbol',
        'Variance20D',
        'Variance60D',
        'Variance120D',
        'Kurtosis20D',
        'Kurtosis60D',
        'Kurtosis120D',
        'Alpha20D',
        'Alpha60D',
        'Alpha120D',
        'Beta20D',
        'Beta60D',
        'Beta120D',
        'Sharp20D',
        'Sharp60D',
        'Sharp120D',
        'TR20D',
        'TR60D',
        'TR120D',
        'IR20D',
        'IR60D',
        'IR120D',
        'GainVariance20D',
        'GainVariance60D',
        'GainVariance120D',
        'LossVariance20D',
        'LossVariance60D',
        'LossVariance120D',
        'GainLossVarianceRatio20D',
        'GainLossVarianceRatio60D',
        'GainLossVarianceRatio120D',
        'DailyReturnSTD252D',
        'DDNSR12M',
        'DDNCR12M',
        'DVRAT'
    ]]

    factor_volatility_value['id'] = factor_volatility_value['symbol'] + str(trade_date)
    factor_volatility_value['trade_date'] = str(trade_date)
    fb._storage_data(factor_volatility_value, trade_date)

# calculate(factor_name='volatility20180202', trade_date=20180202, session='1562216985610666')
# calculate(factor_name='volatility20180202', trade_date=20180202, session='1562224570816022')
