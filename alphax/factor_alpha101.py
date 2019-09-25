# -*- coding: utf-8 -*-

import pdb
import numba
import six
import pandas as pd
import numpy as np
import inspect
import datetime
from sklearn import preprocessing
from numpy import log
from utilities.singleton import Singleton

# rolling corr of two pandas dataframes
def rolling_corr(x, y, win):
    corr_df = pd.DataFrame(data=np.NaN, index=x.index, columns=x.columns)
    for begin, end in zip(x.index[:-win + 1], x.index[win - 1:]):
        corr_df.loc[end] = x.loc[begin:end].corrwith(y.loc[begin:end])
    return corr_df


# rolling cov of two pandas dataframes
def rolling_cov(x, y, win):
    cov_df = pd.DataFrame(data=np.NaN, index=x.index, columns=x.columns)
    for begin, end in zip(x.index[:-win + 1], x.index[win - 1:]):
        x_std = x.loc[begin:end].std()
        y_std = y.loc[begin:end].std()
        cov_df.loc[end] = x.loc[begin:end].corrwith(y.loc[begin:end]) * x_std * y_std
    return cov_df


# rolling rank of a pandas dataframe
def rolling_rank(df, win):
    rank_df = pd.DataFrame(data=np.NaN, index=df.index, columns=df.columns)
    for begin, end in zip(df.index[:-win + 1], df.index[win - 1:]):
        rank_df.loc[end] = df.loc[begin:end].rank(axis=0, pct=True).iloc[-1]
    return rank_df


# rolling dot of a pandas dataframe
def rolling_dot(df, x, win):
    dot_df = pd.DataFrame(data=np.NaN, index=df.index, columns=df.columns)
    for begin, end in zip(df.index[:-win + 1], df.index[win - 1:]):
        # dot_df.loc[end] = x.dot(df.loc[begin:end])
        dot_df.loc[end] = np.dot(x, df.loc[begin:end].values)
    return dot_df


# rolling regression residual
def rolling_regresi(y, x, win):
    resi_df = pd.DataFrame(data=np.NaN, index=y.index, columns=y.columns)
    for begin, end in zip(y.index[:-win + 1], y.index[win - 1:]):
        yy = y.loc[begin:end]
        xx = x.loc[begin:end]
        resi_df.loc[end] = sm.OLS(yy, sm.add_constant(xx)).fit().resid.loc[end]
    return resi_df


# columns covariance of two dataframes
def df_covariance(x, y):
    y = y[x.columns]
    corr_se = x.corrwith(y)
    x_cov, y_cov = np.diag(np.cov(x.T)), np.diag(np.cov(y.T))
    cov_se = (corr_se * np.sqrt(x_cov) * np.sqrt(y_cov))
    return cov_se


# return a series of decay linear sum value of last win rows of dataframe df.
def decay_linear(df, win):
    weights = list(range(1, win + 1))
    weights = [x * 1. / np.sum(weights) for x in weights]
    dot_df = rolling_dot(df.iloc[-win:], weights, win)
    return dot_df.iloc[-1]


# return a series of decay linear sum value of last win rows of dataframe df.
def decay_linear(df, win):
    weights = list(range(1, win + 1))
    weights = [x * 1. / np.sum(weights) for x in weights]
    dot_df = rolling_dot(df.iloc[-win:], weights, win)
    return dot_df.iloc[-1]


# return a dataframe of rolling decay linear sum value of dataframe df.
def rolling_decay(df, win):
    weights = list(range(1, win + 1))
    weights = [x * 1. / np.sum(weights) for x in weights]
    dot_df = rolling_dot(df, weights, win)
    return dot_df


# return winsorized series
def se_winsorize(se, method='sigma', limits=(3.0, 3.0), drop=False):
    se = se.copy(deep=True)
    if method == 'quantile':
        down, up = se.quantile([limits[0], 1.0 - limits[1]])
    elif method == 'sigma':
        std, mean = se.std(), se.mean()
        down, up = mean - limits[0]*std, mean + limits[1]*std

    if drop:
        se[se<down] = np.NaN
        se[se>up] = np.NaN
    else:
        se[se<down] = down
        se[se>up] = up
    return se


# return standardized series
def se_standardize(se):
    try:
        res = (se - se.mean()) / se.std()
    except:
        res = pd.Series(data=np.NaN, index=se.index)
    return res


# return indneutralized series
def se_indneutralize(se, indu_dict):
    date = se.name[0] if type(se.name) is tuple else se.name
    indu = indu_dict[date]

    try:
        se = se_winsorize(se, method='quantile', limits=(0.05, 0.05))
        se = se_winsorize(se, method='sigma', limits=(3.0, 3.0))
        se = se.dropna()
        if se.empty:
            return se
        codes = se.index.intersection(indu.index)
        se = se.loc[codes]
        indu = indu.loc[codes]

        x = np.linalg.lstsq(indu.values, np.matrix(se).T)[0]
        y = se - indu.dot(x)[0]
    except:
        print(date, ':  neutralize error!')
    return y


# return indneutralized pandas dataframe
def df_indneutralize(df, indu_dict):
    neu_dict = {}
    for bar_id in df.index:
        se = df.loc[bar_id]
        neu_dict[bar_id] = se_indneutralize(se, indu_dict)

    neu_df = pd.DataFrame(neu_dict).T
    return neu_df



@six.add_metaclass(Singleton)
class FactorAlpha101(object):
    def __init__(self):
        __str__ = 'factor_alpha101'
        self.name = 'Alpha101'
        self.factor_type1 = 'Features'
        self.factor_type2 = 'Features'
        self.desciption = 'price and volumns features'
    
    def alpha101_2(self, data, param1=2, param2=6, dependencies=['turnover_vol', 'close_price', 'open_price'], 
                max_window=10):
        # -1 * correlation(rank(delta(LOG(VOLUME), 2)), rank(((CLOSE - OPEN) / OPEN)), 6)
        # 价格和成交量都是一阶diff，所以可以都用复权价
    
        rank_price = ((data['close_price'] - data['open_price']) / data['open_price']).rank(axis=1, pct=True)
        rank_volume = (np.log(data['turnover_vol'])).diff(periods=param1).rank(axis=1, pct=True)
    
        corr_win = param2
        id_begin = rank_price.index[-corr_win]
        alpha = rank_price.loc[id_begin:].corrwith(rank_volume.loc[id_begin:])
        return -1.0 * alpha
    
    def alpha101_3(self, data, param1=10, param2=-1, dependencies=['open_price', 'turnover_vol'], max_window=11):
        # -1 * correlation(rank(OPEN), rank(VOLUME), 10)

        rank_open_df = data['open_price'].rank(axis=1, pct=True)
        rank_volume_df = data['turnover_vol'].rank(axis=1, pct=True)

        corr_win = param1
        id_begin = rank_open_df.index[-corr_win]
        alpha = rank_open_df.loc[id_begin:].corrwith(rank_volume_df.loc[id_begin:])
        return float(param2) * alpha
    
    def alpha101_4(self, data, param1=9, param2=-1, dependencies=['lowest_price'], max_window=10):
        # -1 * ts_rank(rank(LOW), 9)

        rank_low = data['lowest_price'].rank(axis=1, pct=True)
        ts_win = param1
        id_begin = rank_low.index[-ts_win]
        alpha = rank_low.loc[id_begin:].rank(axis=0, pct=True).iloc[-1]
        return float(param2) * alpha
    
    
    def alpha101_5(self, data, param1=10, dependencies=['open_price', 
                                                     'close_price', 'vwap'], max_window=10):
        # rank((OPEN - (sum(VWAP, 10) / 10))) * (-1 * abs(rank((CLOSE - VWAP))))

        mean_win = param1
        open_vwap = data['open_price'] - data['vwap'].rolling(window=mean_win).mean()
        rank_open = open_vwap.rank(axis=1, pct=True)
        abs_rank = (data['close_price'] - data['vwap']).rank(axis=1, pct=True).abs() * (-1.0)
        alpha = (rank_open * abs_rank).iloc[-1]
        return alpha
    
    def alpha101_6(self, data, param1=10, param2=-1, dependencies=['open_price', 'turnover_vol'], max_window=11):
        # -1 * correlation(OPEN, VOLUME, 10)
        # correlation of window history price and volume, use adjusted data here.

        corr_win = param1
        id_begin = data['open_price'].index[-corr_win]
        alpha = data['open_price'].loc[id_begin:].corrwith(data['turnover_vol'].loc[id_begin:])
        return float(param2) * alpha
    
    def alpha101_11(self, data, param1=3, param2=3, 
                 dependencies=['close_price', 'vwap', 'turnover_vol'], max_window=5):
    # (rank(ts_max((VWAP - CLOSE), 3)) + rank(ts_min((VWAP - CLOSE), 3))) * rank(delta(VOLUME, 3))

        ts_max = (data['vwap'] - data['close_price']).rolling(window=param1).max()
        ts_min = (data['vwap'] - data['close_price']).rolling(window=param1).min()
        delta_volume = data['turnover_vol'].diff(periods=param2)

        rank_ts_max = ts_max.rank(axis=1, pct=True)
        rank_ts_min = ts_min.rank(axis=1, pct=True)
        rank_vol = delta_volume.rank(axis=1, pct=True)
        alpha = ((rank_ts_max + rank_ts_min) * rank_vol).iloc[-1]
        return alpha
    
    def alpha101_12(self, data, param1=1, param2=-1, dependencies=['close_price', 'turnover_vol'], max_window=2):
        # sign(delta(VOLUME, 1)) * (-1 * delta(CLOSE, 1))

        alpha = np.sign(data['turnover_vol'].diff(periods=param1)) * data['close_price'].diff(periods=param1)
        alpha = alpha.iloc[-1] * float(param2)
        return alpha
    
    def alpha101_13(self, data, param1=5, param2=-1, dependencies=['close_price', 'turnover_vol'], max_window=6):
    # -1 * rank(covariance(rank(CLOSE), rank(VOLUME), 5))

        rank_close_df = data['close_price'].rank(axis=1, pct=True)
        rank_volume_df = data['turnover_vol'].rank(axis=1, pct=True)

        corr_win = param1
        id_begin = rank_close_df.index[-corr_win]
        alpha = df_covariance(rank_close_df.loc[id_begin:], rank_volume_df.loc[id_begin:])
        return float(param2) * alpha.rank(pct=True)
    
    def alpha101_14(self, data, param1=10, param2=3, param3=-1, 
                 dependencies=['open_price', 'turnover_vol', 'returns'], max_window=10):
        # (-1 * rank(delta(RETURNS, 3))) * correlation(OPEN, VOLUME, 10)

        corr_win = param1
        id_begin = data['open_price'].index[-corr_win]
        corr_se = data['open_price'].loc[id_begin:].corrwith(data['turnover_vol'].loc[id_begin:])
        rank_ret_se = (param3) * data['returns'].diff(periods=param2).rank(axis=1, pct=True).iloc[-1]
        alpha = rank_ret_se * corr_se
        return alpha
    
    def alpha101_15(self, data, param1=3, param2=3, param3=-1,
                 dependencies=['highest_price', 'turnover_vol'], max_window=6):
    # -1 * sum(rank(correlation(rank(HIGH), rank(VOLUME), 3)), 3)

        rank_high_df = data['highest_price'].rank(axis=1, pct=True)
        rank_volume_df = data['turnover_vol'].rank(axis=1, pct=True)

        corr_win = param1
        corr_df = rolling_corr(rank_high_df, rank_volume_df, win=corr_win)
        sum_win = param2
        id_begin = corr_df.index[-sum_win]
        alpha = corr_df.loc[id_begin:].rank(axis=1, pct=True).sum()
        return float(param3) * alpha
    
    def alpha101_16(self, data, param1=5, param2=-1,
                 dependencies=['highest_price', 'turnover_vol'], max_window=6):
        # -1 * rank(covariance(rank(HIGH), rank(VOLUME), 5))

        rank_high_df = data['highest_price'].rank(axis=1, pct=True)
        rank_volume_df = data['turnover_vol'].rank(axis=1, pct=True)

        corr_win = param1
        id_begin = rank_high_df.index[-corr_win]
        alpha = df_covariance(rank_high_df.loc[id_begin:], rank_volume_df.loc[id_begin:])
        return float(param2) * alpha.rank(pct=True)
    
    def alpha101_18(self, data, param1=10, param2=5, param3=-1,
                 dependencies=['open_price', 'close_price'], max_window=10):
    # -1 * rank((stddev(abs((CLOSE - OPEN)), 5) + (CLOSE - OPEN)) + correlation(CLOSE, OPEN, 10))

        corr_win = param1
        id_begin = data['open_price'].index[-corr_win]
        corr_se = data['open_price'].loc[id_begin:].corrwith(data['close_price'].loc[id_begin:])

        price = data['close_price'] - data['open_price']
        price_se = (price.abs().rolling(window=param2).std() + price).iloc[-1]

        alpha = float(param3) * (price_se + corr_se).rank(pct=True)
        return alpha
    
    def alpha101_19(self, data, param1=7, param2=70, param3=1, param4=-1,
                 dependencies=['close_price', 'returns'], max_window=70):
    # -1 * sign(((CLOSE - delay(CLOSE, 7)) + delta(CLOSE, 7))) * (1 + rank(1 + sum(RETURNS, 250)))

        sign_se = np.sign(data['close_price'].diff(param1)).iloc[-1]
        # rank_se = (1.0 + data['returns'].rolling(window=250).sum()).iloc[-1].rank(pct=True) + 1.0
        ret_win = param2
        rank_se = (float(param3) + data['returns'].rolling(window=ret_win).sum()).iloc[-1].rank(pct=True)
        alpha = float(param4) * sign_se * rank_se
        return alpha
    
    def alpha101_22(self, data, param1=5, param2=20, param3=-1, 
                 dependencies=['close_price', 'highest_price', 'turnover_vol'], max_window=20):
        # -1 * (delta(correlation(HIGH, VOLUME, 5), 5) * rank(stddev(CLOSE, 20)))

        corr_df = rolling_corr(data['highest_price'], data['turnover_vol'], win=param1)
        delta_corr_se = corr_df.diff(periods=param1).iloc[-1]
        rank_std_se = data['close_price'].rolling(window=param2).std().rank(axis=1, pct=True).iloc[-1]
        alpha = float(param3) * delta_corr_se * rank_std_se
        return alpha
    
    
    def alpha101_23(self, data, param1=20, param2=-1, param3=2, param4=0.25,
                 dependencies=['highest_price', 'highest_price'], max_window=20):
        # ((sum(HIGH, 20) / 20) < HIGH) ? (-1 * delta(HIGH, 2)) : 0

        # # original factor calc method
        # mark = data['high'].rolling(window=20).mean() < data['high']
        # delta_high = -1.0 * data['high_raw'].diff(2)
        # delta_high[mark==False] = 0.0
        # alpha = delta_high.iloc[-1]

        # adjusted factor calc method
        mark = data['highest_price'].rolling(window=param1).mean() < data['highest_price']
        delta_high = float(param2) * data['highest_price'].diff(param3)
        delta_high[mark==False] = delta_high[mark==False] * param4
        alpha = delta_high.iloc[-1]
        return alpha
    
    def alpha101_24(self, data, param1=40, param2=20, param3=6, param4=-1,
                 dependencies=['close_price'], max_window=70):
        # (((delta((sum(CLOSE, 100) / 100), 100) / delay(CLOSE, 100)) < 0.05) ||
        # ((delta((sum(CLOSE, 100) / 100), 100) / delay(CLOSE, 100))== 0.05)) ?
        # (-1 * (CLOSE - ts_min(CLOSE, 100))) : (-1 * delta(CLOSE, 3))

        # # rearranged
        # mask = (delta((sum(CLOSE, 100) / 100), 100) / delay(CLOSE, 100))
        # mask > 0.05 ? (-1 * delta(CLOSE, 3)) : (-1 * (CLOSE - ts_min(CLOSE, 100)))

        # # original factor calc method
        # delta_close = data['close'].rolling(window=100).mean().diff(periods=100)
        # delay_close = data['close'].shift(periods=100)
        # mask = delta_close / delay_close
        # mask_se = mask.iloc[-1] > 0.05

        # true_se = -1.0 * data['close_raw'].diff(periods=3).iloc[-1]
        # false_se = -1.0 * (data['close_raw'] - data['close_raw'].rolling(window=100).min()).iloc[-1]
        # true_se = true_se.reindex(mask_se.index)
        # false_index = mask_se[mask_se==False].index
        # true_se.loc[false_index] = false_se.loc[false_index]

        # # adjusted factor calc method
        delta_close = data['close_price'].rolling(window=param1).mean().diff(periods=param2)
        delay_close = data['close_price'].shift(periods=param2)
        mask = delta_close / delay_close
        mask_se = mask.iloc[-1] > mask.iloc[-1].median()
        true_se = float(param4) * data['close_price'].diff(periods=param3).iloc[-1]
        false_se = float(param4) * (data['close_price'] - data['close_price'].rolling(
            window=param1).min()).iloc[-1]
        true_se = true_se.reindex(mask_se.index)
        false_index = mask_se[mask_se==False].index
        true_se.loc[false_index] = false_se.loc[false_index]
        return true_se
    
    def alpha101_26(self, data, param1=10, param2=10, param3=5, param4=-1,
                 dependencies=['highest_price', 'turnover_vol'], max_window=30):
        # -1 * ts_max(correlation(ts_rank(VOLUME, 5), ts_rank(HIGH, 5), 5), 3)

        ts_rank_vol = rolling_rank(data['turnover_vol'], win=param1)
        ts_rank_high = rolling_rank(data['highest_price'], win=param1)

        corr_df = rolling_corr(ts_rank_vol, ts_rank_high, win=param2)
        alpha = float(param4) * corr_df.rolling(window=param3).max().iloc[-1]
        return alpha
    
    def alpha101_27(self, data, param1=10, param2=2, param3=-1,
                 dependencies=['vwap', 'turnover_vol'], max_window=12):
        # (0.5 < rank((sum(correlation(rank(VOLUME), rank(VWAP), 6), 2) / 2.0))) ? (-1 * 1) : 1

        rank_vol = data['turnover_vol'].rank(axis=1, pct=True)
        rank_vwap = data['vwap'].rank(axis=1, pct=True)

        # # original factor calc method
        # corr_df = rolling_corr(rank_vol, rank_vwap, win=10)
        # corr_mean = corr_df.rolling(window=2).mean()
        # alpha = corr_mean.rank(axis=1, pct=True).iloc[-1]
        # alpha = -1.0 * np.sign((alpha - 0.5))

        # adjusted factor calc method
        # sum(correlation(rank(VOLUME), rank(VWAP), 6), 2) / 2.0
        corr_df = rolling_corr(rank_vol, rank_vwap, win=param1)
        corr_mean = corr_df.rolling(window=param2).mean()
        alpha = float(param3)* corr_mean.iloc[-1]
        return alpha
    
    def alpha101_29(self, data, param1=5, param2=4, param3=3,param4=-1,param5=6,param7=20,
                 dependencies=['close_price', 'returns'], max_window=30):
        # # original formula
        # min(product(rank(sum(ts_min(rank(-1 * rank(delta(CLOSE, 5))), 2), 1)), 1), 5) +
        # ts_rank(delay((-1 * RETURNS), 6), 5)

        # # adjusted formula
        # min(product(rank(sum(ts_min(rank(-1 * rank(delta(CLOSE, 5))), 4), 3)), 3), 5) +
        # ts_rank(delay((-1 * RETURNS), 6), 20)

        df = (float(param4) * data['close_price'].diff(periods=param1).rank(axis=1, pct=True)).rank(axis=1, pct=True)
        df = np.log(df.rolling(window=param3).min().rolling(window=param3).sum()).rank(axis=1, pct=True)
        df = df.rolling(window=param3).apply(lambda x: np.prod(x)).rolling(window=param1).min()

        delay_ret = (float(param4) * data['returns']).shift(periods=param5)
        rank_win = param7
        id_begin = data['returns'].index[-rank_win]
        ts_rank_ret = delay_ret.loc[id_begin:].rank(axis=0, pct=True)

        alpha = df.iloc[-1] + ts_rank_ret.iloc[-1]
        return alpha
    '''
    def alpha101_32(self, data, param1=7, param2=40, param3=5, param4=20,
                 dependencies=['close_price', 'vwap'], max_window=50):
        # # original formula
        # scale((sum(CLOSE, 7) / 7) - CLOSE) + 20 * scale(correlation(VWAP, delay(CLOSE, 5), 230))

        # # adjusted formula
        # scale((sum(CLOSE, 7) / 7) - CLOSE) + 20 * scale(correlation(VWAP, delay(CLOSE, 5), 40))

        close_se = (data['close_price'].rolling(window=param1).mean() - data['close_price']).iloc[-1]
        scale_close_se = close_se / close_se.abs().sum()
        corr_win = param2
        id_begin = data['close_price'].index[-corr_win]
        corr_se = data['close_price'].shift(periods=param3).loc[id_begin:].corrwith(data['vwap'].loc[id_begin:])
        scale_corr_se = corr_se / corr_se.abs().sum()

        alpha = scale_close_se + param4 * scale_corr_se
        return alpha
    '''
    
    def alpha101_36(self, data, param1=15, param2=6, param3=10, param4=20, param5=50,
                 param6=2.21, param7=0.7, param8=0.73, param9=0.6, param10=-1,
                 dependencies=['close_price', 'open_price', 'close_price', 
                               'vwap', 'turnover_vol', 'returns'], max_window=60):
        # # original formula
        # 2.21 * rank(correlation((CLOSE - OPEN), delay(VOLUME, 1), 15)) +
        # 0.7 * rank(OPEN - CLOSE) + 0.73 * rank(ts_rank(delay((-1 * RETURNS), 6), 5)) +
        # rank(abs(correlation(VWAP, ADV20, 6))) + 0.6 * rank((sum(CLOSE, 200) / 200 - OPEN) * (CLOSE - OPEN))

        # rank(correlation((CLOSE - OPEN), delay(VOLUME, 1), 15))
        corr_win = param1
        id_begin = data['close_price'].index[-corr_win]
        corr_se = data['turnover_vol'].shift(periods=1
                                            ).loc[id_begin:].corrwith((data['close_price'] - data['open_price']).loc[id_begin:])
        part1 = corr_se.rank(pct=True)

        # rank(OPEN - CLOSE)
        part2 = (data['open_price'] - data['close_price']).iloc[-1].rank(pct=True)

        # rank(ts_rank(delay((-1 * RETURNS), 6), 5))
        ts_rank_win = param1    # change from orignal 5 to 15
        id_begin = data['returns'].index[-ts_rank_win]
        ts_rank_df = (float(param10) * data['returns']).shift(periods=param2).loc[id_begin:].rank(axis=0, pct=True)
        part3 = ts_rank_df.iloc[-1].rank(pct=True)

        # rank(abs(correlation(VWAP, ADV20, 6)))
        corr_win = param3      # change from orignal 6 to 10
        id_begin = data['vwap'].index[-corr_win]
        adv20 = data['turnover_vol'].rolling(window=param4).mean()
        corr_se = data['vwap'].loc[id_begin:].corrwith(adv20.loc[id_begin:])
        part4 = corr_se.abs().rank(pct=True)

        # rank((sum(CLOSE, 200) / 200 - OPEN) * (CLOSE - OPEN))
        sum_win = param5      # change from orignal 200 to 50
        sum_close = data['close_price'].rolling(window=sum_win).mean() - data['open_price']
        close_open = data['close_price'] - data['open_price']
        part5 = (sum_close * close_open).iloc[-1].rank(pct=True)

        alpha = param6 * part1 + param7 * part2 + param8 * part3 + part4 + param9 * part5
        return alpha
    
    def alpha101_40(self, data, param1=10, param2=10, param3=-1,
                 dependencies=['highest_price',  'turnover_vol'], max_window=12):
        # (-1 * rank(stddev(HIGH, 10))) * correlation(HIGH, VOLUME, 10)

        part1 = float(param3) * data['highest_price'].rolling(window=param1).std().iloc[-1].rank(pct=True)
        corr_win = param2
        id_begin = data['highest_price'].index[-corr_win]
        part2 = data['highest_price'].loc[id_begin:].corrwith(data['turnover_vol'].loc[id_begin:])

        alpha = part1 * part2
        return alpha
    
    def alpha101_44(self, data, param1=5, param2=-1,
                 dependencies=['highest_price', 'turnover_value'], max_window=11):
        # -1 * correlation(HIGH, rank(VOLUME), 5)

        high_df = data['highest_price']
        rank_volume_df = data['turnover_value'].rank(axis=1, pct=True)

        corr_win = param1
        id_begin = high_df.index[-corr_win]
        alpha = high_df.loc[id_begin:].corrwith(rank_volume_df.loc[id_begin:])
        return float(param2) * alpha
    
    def alpha101_45(self, data, param1=5, param2=20, param3=6, param4=6, param5=5, param6=20, param7=-1,
                 dependencies=['close_price', 'turnover_vol'], max_window=30):
        # -1 * rank(sum(delay(CLOSE, 5), 20) / 20) * correlation(CLOSE, VOLUME, 2) * rank(correlation(sum(CLOSE, 5), sum(CLOSE, 20), 2))

        # rank(sum(delay(CLOSE, 5), 20) / 20)
        part1 = data['close_price'].shift(periods=param1).rolling(window=param2).mean().iloc[-1].rank(pct=True)

        # correlation(CLOSE, VOLUME, 2)
        corr_win = param3    # change from orignal 2 to 6
        id_begin = data['close_price'].index[-corr_win]
        part2 = data['close_price'].loc[id_begin:].corrwith(data['turnover_vol'].loc[id_begin:])

        # rank(correlation(sum(CLOSE, 5), sum(CLOSE, 20), 2))
        corr_win = param4    # change from orignal 2 to 6
        id_begin = data['close_price'].index[-corr_win]
        close_sum5 = data['close_price'].rolling(window=param5).sum()
        close_sum20 = data['close_price'].rolling(window=param6).sum()
        part3 = (close_sum5.loc[id_begin:].corrwith(close_sum20.loc[id_begin:])).rank(pct=True)

        alpha = float(param7) * part1 * part2 * part3
        return alpha
    
    def alpha101_50(self, data, param1=5, param2=5, param3=-1,
                 dependencies=['vwap', 'turnover_vol'], max_window=10):
        # -1 * ts_max(rank(correlation(rank(VOLUME), rank(VWAP), 5)), 5)

        rank_vwap_df = data['vwap'].rank(axis=1, pct=True)
        rank_volume_df = data['turnover_vol'].rank(axis=1, pct=True)

        corr_win = param1
        corr_df = rolling_corr(rank_vwap_df, rank_volume_df, win=corr_win)
        ts_max_win = param2
        id_begin = corr_df.index[-ts_max_win]
        alpha = corr_df.loc[id_begin:].rank(axis=1, pct=True).max()
        return float(param3) * alpha
    
    def alpha101_52(self, data, param1=8, param2=8, param3=80, param4=8, param5=8,
                 dependencies=['lowest_price', 'returns', 'turnover_vol'], max_window=10):
        # (-ts_min(LOW, 5) + delay(ts_min(LOW, 5), 5)) *
        # rank(((sum(RETURNS, 240) - sum(RETURNS, 20)) / 220)) * ts_rank(VOLUME, 5)

        # (-ts_min(LOW, 5) + delay(ts_min(LOW, 5), 5))
        ts_max_win = param1
        id_begin = data['lowest_price'].index[-ts_max_win]
        part1 = data['lowest_price'].shift(periods=param2
                                              ).loc[id_begin:].min() - data['lowest_price'].loc[id_begin:].min()

        # rank(((sum(RETURNS, 240) - sum(RETURNS, 20)) / 220))
        long_win, short_win = param3, param4    # change from original 240,20 to 80,8
        ret_se = data['returns'].iloc[-long_win:].sum() - data['returns'].iloc[-short_win:].sum()
        part2 = (ret_se / (1.0 * (long_win - short_win))).rank(pct=True)

        # ts_rank(VOLUME, 5)
        ts_rank_win = param5
        part3 = data['turnover_vol'].iloc[-ts_rank_win:].rank(axis=0, pct=True).iloc[-1]

        alpha = part1 * part2 * part3
        return alpha
    
    def alpha101_53(self, data, param1=2, param2=9, param3=0.001,
                 dependencies=['close_price', 'lowest_price', 'highest_price'], max_window=12):
        # -1 * delta((((CLOSE - LOW) - (HIGH - CLOSE)) / (CLOSE - LOW)), 9)
        # rearranged formula
        # -1 * delta(((2 * CLOSE - LOW - HIGH) / (CLOSE - LOW)), 9)

        price_df = (data['close_price'] * float(param1) - data['lowest_price'] - data['highest_price']) / (
            (data['close_price'] - data['lowest_price']) + param3)
        alpha = price_df.diff(periods=param2).iloc[-1]
        return alpha
    
    def alpha101_54(self, data, param1=0.001, param2=5, param3=4, param5=-1,
                 dependencies=['close_price', 'lowest_price', 'highest_price', 'open_price'], max_window=5):
        # (-1 * ((LOW - CLOSE) * (OPEN^5))) / ((LOW - HIGH) * (CLOSE^5))

        numerator = (data['lowest_price'] - data['close_price'] + param1) * (data['open_price'] ** param2)
        denominator = (data['lowest_price'] - data['highest_price'] + param1) * (data['close_price'] ** param2)

        # use mean average factor of ma_win bars
        ma_win = param3
        alpha = (float(param5)* numerator / denominator).iloc[-ma_win:].mean()
        alpha[alpha==float(param5)] = np.NaN
        return alpha
    
    def alpha101_55(self, data, param1=12, param2=12, param3=6, param4=-1,
                 dependencies=['close_price','lowest_price', 'highest_price', 
                               'turnover_value'], max_window=20):
        # -1 * correlation(rank((CLOSE - ts_min(LOW, 12)) / (ts_max(HIGH, 12) - ts_min(LOW, 12))), rank(VOLUME), 6)
        # CLOSE - ts_min(LOW, 12)) / (ts_max(HIGH, 12) - ts_min(LOW, 12)): 此项价格相除无量纲，所以
        # 用复权价格计算； 后续的volume使用value代替

        ts_min_low = data['lowest_price'].rolling(window=param1).min()
        ts_max_high = data['highest_price'].rolling(window=param2).max()

        price_df = (data['close_price'] -  ts_min_low) / (ts_max_high - ts_min_low)
        rank_price = price_df.rank(axis=1, pct=True)
        rank_volume = data['turnover_value'].rank(axis=1, pct=True)

        corr_win = param3
        corr_df = rolling_corr(rank_price, rank_volume, win=corr_win)
        return float(param4) * corr_df.iloc[-1]
    
    def alpha101_57(self, data, param1=2, param2=30, param3=4, param4=-1,
                 dependencies=['close_price', 'vwap'], max_window=35):
        # -1 * (CLOSE - VWAP) / decay_linear(rank(ts_argmax(CLOSE, 30)), 2)

        # (CLOSE - VWAP)
        ma_win = param1
        numerator = (data['close_price'] - data['vwap']).iloc[-ma_win:].mean()

        # decay_linear(rank(ts_argmax(CLOSE, 30)), 2)
        rank_df = data['close_price'].rolling(window=param2).apply(lambda x: x.argmax()).rank(axis=1, pct=True)
        denominator = decay_linear(rank_df, win=param3)    # change win from original 2 to 4

        alpha = (float(param4) * numerator / denominator)
        return alpha
    
    def alpha101_58(self, data, param1=9, param2=8, param3=7, param4=-1,
                 dependencies=['vwap', 'turnover_vol','indu'], max_window=25):
        # -1 * ts_rank(decay_linear(correlation(indneutralize(VWAP, indclass), VOLUME, 3.92795), 7.89291), 5.50322)
    
        # indneutralize(VWAP, indclass)
        neu_df = df_indneutralize(data['vwap'], data['indu'])
    
        # # original formula
        # corr_win, decay_win, ts_rank_win = 9, 8, 7
        # decay_df = rolling_decay(rolling_corr(neu_df, data['volume_raw'], win=corr_win), win=decay_win)
        # ts_rank_se = decay_df.iloc[-ts_rank_win:].rank(axis=0, pct=True).iloc[-1]
        # alpha = -1.0 * ts_rank_se
    
        # adjusted formula --- use a new method instead of ts rank.
        corr_win, decay_win, ts_mean_win = param1, param2, param3
        decay_df = rolling_decay(rolling_corr(neu_df, data['turnover_vol'], win=corr_win), win=decay_win)
        data_se = decay_df.iloc[-1] - decay_df.iloc[-ts_mean_win:].mean(axis=0)
        alpha = float(param4)* data_se
        
        return alpha

    def alpha101_59(self, data, param1=0.7, param2=0.3, param3=9, param4=12, param5=10, param6=-1,
                 dependencies=['vwap', 'close_price', 'turnover_vol','indu'], max_window=30):
        # -1 * ts_rank(decay_linear(correlation(indneutralize(((VWAP * 0.728317) + (VWAP * (1 - 0.728317))),
        # indclass), VOLUME, 4.25197), 16.2289), 8.19648)
        # Caution: the original formula is almost same with alpha101_58 (
        # ((VWAP * 0.728317) + (VWAP * (1 - 0.728317))) == VWAP), so we take an adjusted formula here.
        # adjusted formula
        # -1 * ts_rank(decay_linear(correlation(indneutralize(((VWAP * 0.728317) + (CLOSE * (1 - 0.728317))),
        # indclass), VOLUME, 4.25197), 16.2289), 8.19648)

        # indneutralize(VWAP, indclass)
        neu_df = df_indneutralize(data['vwap'] * param1 + data['close_price'] * param2, data['indu'])

        # # original formula
        # corr_win, decay_win, ts_rank_win = 9, 12, 10
        # decay_df = rolling_decay(rolling_corr(neu_df, data['volume_raw'], win=corr_win), win=decay_win)
        # ts_rank_se = decay_df.iloc[-ts_rank_win:].rank(axis=0, pct=True).iloc[-1]
        # alpha = -1.0 * ts_rank_se

        # adjusted formula --- use a new method instead of ts rank.
        corr_win, decay_win, ts_mean_win = param3, param4, param5
        decay_df = rolling_decay(rolling_corr(neu_df, data['turnover_vol'], win=corr_win), win=decay_win)
        data_se = decay_df.iloc[-1] - decay_df.iloc[-ts_mean_win:].mean(axis=0)
        alpha = float(param6) * data_se

        return alpha
    
    def alpha101_62(self, data, param1=20, param2=10, param3=10, param4=2, param5=-1,
                 dependencies=['turnover_vol', 'vwap', 'open_price', 'highest_price', 
                               'lowest_price'], max_window=40):
        # -1.0 * (rank(correlation(VWAP, sum(ADV20, 22.4101), 9.91009)) <
        # rank(((rank(OPEN) * 2) < (rank(((HIGH + LOW) / 2)) + rank(HIGH)))))

        # adjusted formula: between two parts, use - instead of <; between two parts
        # in the second condition, use - instead of < too;
        # -1.0 * (rank(correlation(VWAP, sum(ADV20, 22.4101), 9.91009)) -
        # rank(((rank(OPEN) * 2) - (rank(((HIGH + LOW) / 2)) + rank(HIGH)))))

        # rank(correlation(VWAP, sum(ADV20, 22.4101), 9.91009))
        adv_win, sum_adv_win, corr_win = param1, param2, param3
        sum_adv = data['turnover_vol'].rolling(window=adv_win).mean().rolling(window=sum_adv_win).mean()
        part1 = data['vwap'].iloc[-corr_win:].corrwith(sum_adv.iloc[-corr_win:]).rank(pct=True)

        # rank(((rank(OPEN) * 2) - (rank(((HIGH + LOW) / 2)) + rank(HIGH))))
        rank_open = data['open_price'].rank(axis=1, pct=True)
        rank_high_low = ((data['highest_price'] + data['lowest_price']) / float(param4)).rank(axis=1, pct=True)
        rank_high = data['highest_price'].rank(axis=1, pct=True)
        part2 = (rank_open - rank_high_low - rank_high).rank(axis=1, pct=True).iloc[-1]

        alpha = float(param5) * (part1 - part2)
        return alpha
    
    def alpha101_66(self, data, param1=0.001, param2=2, param3=12, param4=7, param5=-1,
                 dependencies=['vwap', 'lowest_price', 'open_price', 'highest_price'], max_window=20):
        # -1 * (rank(decay_linear(delta(VWAP, 3.51013), 7.23052)) +
        # ts_rank(decay_linear((((LOW * 0.96633) + (LOW * (1 - 0.96633))) - VWAP) / (OPEN - ((HIGH + LOW) / 2)), 11.4157), 6.72611))

        # rank(decay_linear(delta(VWAP, 3.51013), 7.23052))
        part1 = decay_linear(data['vwap'].diff(periods=4), win=8).rank(pct=True)

        # ts_rank(decay_linear((((LOW * 0.96633) + (LOW * (1 - 0.96633))) - VWAP) / (OPEN - ((HIGH + LOW) / 2)), 11.4157), 6.72611)
        # rearranged
        # ts_rank(decay_linear((LOW - VWAP) / (OPEN - ((HIGH + LOW) / 2)), 11.4157), 6.72611)
        price_df = (data['lowest_price'] - data['lowest_price'] + param1
                   ) / (data['open_price'] - (data['highest_price'] + data['lowest_price']) / float(param2) + param1)
        price_df = (data['lowest_price'] - data['vwap']) / (
            data['open_price'] - (data['highest_price'] + data['lowest_price']) / float(param2))
        decay_win, ts_win = param3, param4
        part2 = rolling_decay(price_df, win=decay_win).iloc[-ts_win:].rank(axis=0, pct=True).iloc[-1]

        alpha = float(param5) * (part1 + part2)
        return alpha
    
    def alpha101_67(self, data, param1=20, param2=10, param3=5, param4=8, param5=-1,
                 dependencies=['highest_price', 'vwap', 'turnover_vol','indu'], max_window=30):
        # -1.0 * rank(HIGH - ts_min(HIGH, 2.14593))^
        # rank(correlation(indneutralize(VWAP, indclass), indneutralize(ADV20, indclass), 6.02936))

        # rank(HIGH - ts_min(HIGH, 2.14593))
        # use adjusted formula: mean(rank(HIGH - ts_min(HIGH, 10)), 10)
        high_df = data['highest_price'] - data['highest_price'].rolling(window=param1).min()
        part1 = high_df.rank(axis=1, pct=True).iloc[-param2:].mean()

        # rank(correlation(indneutralize(VWAP, indclass), indneutralize(ADV20, indclass), 6.02936))
        neu_vwap = df_indneutralize(data['vwap'], data['indu'])
        neu_adv = df_indneutralize(data['turnover_vol'].rolling(window=param3).mean(), data['indu'])
        corr_win = param4
        part2 = neu_vwap.iloc[-corr_win:].corrwith(neu_adv.iloc[-corr_win:]).rank(pct=True)

        alpha = float(param5) * part1 ** part2
        return alpha
    
    def alpha101_69(self, data, param1=3, param2=5, param3=8, param4=-1,
                 dependencies=['vwap', 'turnover_vol','indu'], max_window=15):
        # -1 * rank(ts_max(delta(indneutralize(VWAP, indclass), 2.72412), 4.79344))^
        # ts_rank(correlation(((CLOSE * 0.490655) + (VWAP * (1 - 0.490655))), ADV20, 4.92416), 9.0615)

        neu_vwap = df_indneutralize(data['vwap'], data['indu'])
        neu_adv = df_indneutralize(data['turnover_vol'].rolling(window=5).mean(), data['indu'])

        # rank(ts_max(delta(indneutralize(VWAP, indclass), 2.72412), 4.79344))
        diff_win, ts_max_win = param1, param2
        ts_max_df = neu_vwap.diff(periods=diff_win).rolling(window=ts_max_win).max()
        part1 = ts_max_df.iloc[-1].rank(pct=True)

        # rank(correlation(indneutralize(VWAP, indclass), indneutralize(ADV20, indclass), 6.02936))
        corr_win = param3
        part2 = neu_vwap.iloc[-corr_win:].corrwith(neu_adv.iloc[-corr_win:]).rank(pct=True)

        alpha = float(param4) * (part1 ** part2)
        return alpha
    
    def alpha101_72(self, data, param1=5, param2=1.0e6, param3=9, param4=10, param5=2,
                 param6=8, param7=20, param8=7,param9=3,
                 dependencies=['turnover_vol', 'lowest_price', 'highest_price', 'vwap'], max_window=30):
        # rank(decay_linear(correlation(((HIGH + LOW) / 2), ADV40, 8.93345), 10.1519)) /
        # rank(decay_linear(correlation(ts_rank(VWAP, 3.72469), ts_rank(VOLUME, 18.5188), 6.86671), 2.95011))

        # rank(decay_linear(correlation(((HIGH + LOW) / 2), ADV40, 8.93345), 10.1519))
        ma_vol_win = param1
        avg_vol = data['turnover_vol'].rolling(window=ma_vol_win).mean() / param2
        corr_win, decay_win = param3, param4
        part1 = decay_linear(rolling_corr((data['highest_price'] + data['lowest_price'])/float(param5), 
                                          avg_vol, win=corr_win), win=decay_win).rank(pct=True)

        # rank(decay_linear(correlation(ts_rank(VWAP, 3.72469), ts_rank(VOLUME, 18.5188), 6.86671), 2.95011))
        ts_rank_vwap = rolling_rank(data['vwap'], win=param6)
        ts_rank_vol = rolling_rank(data['turnover_vol'], win=param7)
        corr_win, decay_win = param8, param9
        part2 = decay_linear(rolling_corr(ts_rank_vwap, ts_rank_vol, win=corr_win), win=decay_win).rank(pct=True)

        alpha = part1 / part2
        return alpha
    
    def alpha101_73(self, data, param1=5, param2=3, param3=0.147155, param4=0.147155,
                 param5=2, param6=4, param7=17,param8=-1,param9=-1,
                 dependencies=['vwap', 'lowest_price', 'open_price'], max_window=25):
        # -1 * max(rank(decay_linear(delta(VWAP, 4.72775), 2.91864)),
        # ts_rank(decay_linear((delta((OPEN * 0.147155 + LOW * (1 - 0.147155)), 2.03608) /
        # (OPEN * 0.147155 + LOW * (1 - 0.147155))) * -1, 3.33829), 16.7411))

        # rank(decay_linear(delta(VWAP, 4.72775), 2.91864))
        diff_win, decay_win = param1, param2
        part1 = decay_linear(data['vwap'].diff(periods=diff_win), win=decay_win).rank(pct=True)

        # (OPEN * 0.147155 + LOW * (1 - 0.147155))
        price = data['open_price'] * param3 + data['lowest_price'] * (1 - param4)
        # ts_rank(decay_linear((delta((OPEN * 0.147155 + LOW * (1 - 0.147155)), 2.03608) /
        # (OPEN * 0.147155 + LOW * (1 - 0.147155))) * -1, 3.33829), 16.7411)
        diff_win, decay_win, ts_rank_win = param5, param6, param7
        decay_df = rolling_decay(float(param8) * price.diff(periods=diff_win) / price, win=decay_win)
        part2 = decay_df.iloc[-ts_rank_win:].rank(axis=0, pct=True).iloc[-1]

        alpha = (param9) * pd.DataFrame({'part1': part1, 'part2': part2}).max(axis=1)
        return alpha
    
    def alpha101_74(self, data, param1=10, param2=16, param3=15, param4=0.0261661, param5=12, param6=-1,
                 dependencies=['turnover_vol', 'close_price', 'highest_price', 'vwap'], max_window=40):
        # -1 * (rank(correlation(CLOSE, sum(ADV30, 37.4843), 15.1365)) <
        # rank(correlation(rank(((HIGH * 0.0261661) + (VWAP * (1 - 0.0261661)))), rank(VOLUME), 11.4791)))
        # rearranged formula: between two parts, use - instead of <
        # -1 * (rank(correlation(CLOSE, sum(ADV30, 37.4843), 15.1365)) -
        # rank(correlation(rank(((HIGH * 0.0261661) + (VWAP * (1 - 0.0261661)))), rank(VOLUME), 11.4791)))

        # rank(correlation(CLOSE, sum(ADV30, 37.4843), 15.1365))
        mean_win, sum_win = param1, param2    # change from 30, 37.48 to 10, 16
        adv_sum = data['turnover_vol'].rolling(window=mean_win).mean().rolling(window=sum_win).sum()
        corr_win = param3      # change from orignal 15.13 to 15
        part1 = data['close_price'].iloc[-corr_win:].corrwith(adv_sum.iloc[-corr_win:]).rank(pct=True)

        # rank(correlation(rank(HIGH * 0.0261661 + VWAP * (1 - 0.0261661)), rank(VOLUME), 11.4791))
        rank_price = (data['highest_price'] * param4 + data['vwap'] * (1 - param4)).rank(axis=1, pct=True)
        rank_vol = data['turnover_vol'].rank(axis=1, pct=True)
        corr_win = param5      # change from orignal 11.4791 to 12
        part2 = rank_price.iloc[-corr_win:].corrwith(rank_vol.iloc[-corr_win:]).rank(pct=True)

        alpha = float(param6) * (part1 - part2)
        return alpha

    def alpha101_75(self, data, param1=8, param2=12, param3=12,
                 dependencies=['vwap', 'turnover_vol', 'lowest_price', 'turnover_vol'], max_window=30):
        # rank(correlation(VWAP, VOLUME, 4.24304)) < rank(correlation(rank(LOW), rank(ADV50), 12.4413))
        # rearranged formula: between two parts, use - instead of <
        # rank(correlation(VWAP, VOLUME, 4.24304)) - rank(correlation(rank(LOW), rank(ADV50), 12.4413))

        # rank(correlation(VWAP, VOLUME, 4.24304))
        corr_win = param1      # change from orignal 4.243 to 8
        part1 = data['vwap'].iloc[-corr_win:].corrwith(data['turnover_vol'].iloc[-corr_win:]).rank(pct=True)

        # rank(correlation(rank(LOW), rank(ADV50), 12.4413))
        mean_win = param2    # change from orignal 50 to 12
        rank_price = data['lowest_price'].rank(axis=1, pct=True)
        rank_adv = data['turnover_vol'].rolling(window=mean_win).mean().rank(axis=1, pct=True)
        corr_win = param3      # change from orignal 12.4413 to 12
        part2 = rank_price.iloc[-corr_win:].corrwith(rank_adv.iloc[-corr_win:]).rank(pct=True)

        alpha = part1 - part2
        return alpha
    
    def alpha101_76(self, data, param1=5, param2=1, param3=5, param4=8, param5=20, param6=5,
                 param7=20, param8=-1, param9=0.5,
                 dependencies=['close_price', 'vwap', 'lowest_price', 'turnover_vol','indu'], 
                 max_window=50):
        # -1 * max(rank(decay_linear(delta(VWAP, 1.24383), 11.8259)),
        # ts_rank(decay_linear(ts_rank(correlation(indneutralize(LOW, indclass), ADV81, 8.14941), 19.569), 17.1543), 19.383))

        neu_low = df_indneutralize(data['lowest_price'], data['indu'])
        adv = data['turnover_vol'].rolling(window=param1).mean()

        # rank(decay_linear(delta(VWAP, 1.24383), 11.8259))
        diff_win, decay_win = param2, param3
        decay_df = rolling_decay(data['vwap'].diff(periods=diff_win), win=decay_win)
        part1 = decay_df.iloc[-1].rank(pct=True)

        # ts_rank(decay_linear(ts_rank(correlation(indneutralize(LOW, indclass), ADV81, 8.14941), 19.569), 17.1543), 19.383)
        corr_win, ts_rank_win1, decay_win, ts_rank_win2 = param4, param5, param6, param7
        corr_df = rolling_corr(neu_low, adv, win=corr_win)
        decay_df = rolling_decay(rolling_rank(corr_df, win=ts_rank_win1), win=decay_win)
        part2 = decay_df.iloc[-ts_rank_win2:].rank(axis=0, pct=True).iloc[-1]

        res_df = pd.DataFrame({'part1': part1, 'part2': part2})
        # alpha = -1.0 * res_df.max(axis=1)
        # # use adjusted formula
        alpha = float(param8) * (res_df.max(axis=1) - param9 * res_df.min(axis=1))
        return alpha
    
    def alpha101_80(self, data, param1=0.85, param2=0.15, param3=5, param4=4, param5=5,
                 param6=6, param7=6, param8=-1,
                 dependencies=['open_price', 'highest_price', 'turnover_vol', 'highest_price','indu'], max_window=20):
        # -1 * (rank(sign(delta(indneutralize(((OPEN * 0.868128) + (HIGH * (1 - 0.868128))), indclass), 4.04545)))^
        # ts_rank(correlation(HIGH, ADV10, 5.11456), 5.53756))

        neu_price = df_indneutralize(data['open_price'] * param1 + data['highest_price'] * param2, data['indu'])
        adv = data['turnover_vol'].rolling(window=param3).mean()

        # rank(sign(delta(indneutralize(((OPEN * 0.868128) + (HIGH * (1 - 0.868128))), indclass), 4.04545)))
        # use decay_linear instead of sign in part1 formula
        # rank(decay_linear(delta(indneutralize(((OPEN * 0.868128) + (HIGH * (1 - 0.868128))), indclass), 4.04545), 5))
        diff_win, decay_win = param4, param5
        part1 = decay_linear(neu_price.diff(periods=diff_win), win=decay_win).rank(pct=True)

        # ts_rank(correlation(HIGH, ADV10, 5.11456), 5.53756)
        corr_win, ts_rank_win = param6, param7
        corr_df = rolling_corr(data['highest_price'], adv, win=corr_win)
        part2 = corr_df.iloc[-ts_rank_win:].rank(axis=0, pct=True).iloc[-1]

        alpha = float(param8) * part1 ** part2
        return alpha
    
    def alpha101_81(self, data, param1=10, param2=10, param3=8, param4=10, param5=4,
                 param6=8, param7=-1,
                 dependencies=['vwap', 'turnover_vol', 'vwap'], max_window=32):
        # -1 * (rank(LOG(product(rank((rank(correlation(VWAP, sum(ADV10, 49.6054), 8.47743))^4)), 14.9655))) <
        # rank(correlation(rank(VWAP), rank(VOLUME), 5.07914)))
        # rearranged formula: between two parts, use - instead of <
        # -1 * (rank(LOG(product(rank((rank(correlation(VWAP, sum(ADV10, 49.6054), 8.47743))^4)), 14.9655))) -
        # rank(correlation(rank(VWAP), rank(VOLUME), 5.07914)))

        # rank(LOG(product(rank((rank(correlation(VWAP, sum(ADV10, 49.6054), 8.47743))^4)), 14.9655)))
        mean_win, sum_win = param1, param2    # change from 10, 49.6054 to 10, 10
        adv_sum = data['turnover_vol'].rolling(window=mean_win).mean().rolling(window=sum_win).sum()
        corr_win, prod_win = param3, param4      # change from orignal 8.47743, 14.9655 to 8, 10
        corr_df = rolling_corr(data['vwap'], adv_sum, corr_win)
        prod_se = ((corr_df.rank(axis=1, pct=True)) ** param5).rank(axis=1, pct=True).iloc[-prod_win:].cumprod().iloc[-1]
        part1 = np.log(prod_se).rank(pct=True)

        # rank(correlation(rank(VWAP), rank(VOLUME), 5.07914))
        rank_price = data['vwap'].rank(axis=1, pct=True)
        rank_vol = data['turnover_vol'].rank(axis=1, pct=True)
        corr_win = param6      # change from orignal 5.07914 to 8
        part2 = rank_price.iloc[-corr_win:].corrwith(rank_vol.iloc[-corr_win:]).rank(pct=True)

        alpha = float(param7) * (part1 - part2)
        return alpha

    def alpha101_82(self, data, param1=1, param2=10, param3=16, param4=6, param5=14, param6=-1, param7=0.5,
                 dependencies=['open_price', 'turnover_vol','indu'], max_window=40):
        # -1 * min(rank(decay_linear(delta(OPEN, 1.46063), 14.8717)),
        # ts_rank(decay_linear(correlation(indneutralize(VOLUME, indclass), ((OPEN * 0.634196) + (OPEN * (1 - 0.634196))), 17.4842), 6.92131), 13.4283))
        # rearranged formula
        # -1 * min(rank(decay_linear(delta(OPEN, 1.46063), 14.8717)),
        # ts_rank(decay_linear(correlation(indneutralize(VOLUME, indclass), OPEN, 17.4842), 6.92131), 13.4283))

        # rank(decay_linear(delta(OPEN, 1.46063), 14.8717))
        diff_win, decay_win = param1, param2
        part1 = decay_linear(data['open_price'].diff(periods=diff_win), win=decay_win).rank(pct=True)

        # ts_rank(decay_linear(correlation(indneutralize(VOLUME, indclass), OPEN, 17.4842), 6.92131), 13.4283)
        neu_vol = df_indneutralize(data['turnover_vol'], data['indu'])
        corr_win, decay_win, ts_rank_win = param3, param4, param5
        decay_df = rolling_decay(rolling_corr(neu_vol, data['open_price'], win=corr_win), win=decay_win)
        part2 = decay_df.iloc[-ts_rank_win:].rank(axis=0, pct=True).iloc[-1]

        alpha101_df = pd.DataFrame({'part1': part1, 'part2': part2})

        # # original alpha formula
        # alpha = -1.0 * alpha101_df.min(axis=1)
        # adjusted alpha formula
        alpha = float(param6) * (alpha101_df.min(axis=1) - float(param7) * alpha101_df.max(axis=1))
        return alpha
    
    def alpha101_83(self, data, param1=10, param2=2,
                 dependencies=['highest_price', 'lowest_price', 
                               'close_price', 'turnover_vol', 
                               'vwap'], max_window=20):
        # (rank(delay(((HIGH - LOW) / (sum(CLOSE, 5) / 5)), 2)) * rank(VOLUME)) /
        # (((HIGH - LOW) / (sum(CLOSE, 5) / 5)) / (VWAP - CLOSE))
        # rearranged formula
        # rank(delay(((HIGH - LOW) / (sum(CLOSE, 5) / 5)), 2)) * rank(VOLUME) * (VWAP - CLOSE) /
        # ((HIGH - LOW) / (sum(CLOSE, 5) / 5))
    
        # rank(delay(((HIGH - LOW) / (sum(CLOSE, 5) / 5)), 2))
        mean_win, delay_win = param1, param2
        price_df = ((data['highest_price'] - data['lowest_price']) / data['close_price'].rolling(window=mean_win).mean())
        part1 = price_df.diff(periods=delay_win).iloc[-1].rank(pct=True)
    
        # rank(VOLUME) * (VWAP - CLOSE)
        part2 = (data['turnover_vol'].rank(axis=1, pct=True) * (data['vwap'] - data['close_price'])).iloc[-1]
    
        # ((HIGH - LOW) / (sum(CLOSE, 5) / 5))
        part3 = price_df.iloc[-1]
    
        alpha = part1 * part2 / part3
        return alpha
    
    def alpha101_84(self, data, param1=15, param2=20, param3=6,
                 dependencies=['vwap', 'close_price'], max_window=40):
        # signedpower(ts_rank((VWAP - ts_max(VWAP, 15.3217)), 20.7127), delta(CLOSE, 4.96796))

        # ts_rank((VWAP - ts_max(VWAP, 15.3217)), 20.7127)
        max_win, rank_win = param1, param2
        price_df = data['vwap'] - data['vwap'].rolling(window=max_win).max()
        part1 = price_df.iloc[-rank_win:].rank(axis=0, pct=True).iloc[-1]
    
        # delta(CLOSE, 4.96796)
        diff_win = param3
        part2 = data['close_price'].diff(periods=diff_win).iloc[-1]
        part2 = data['close_price'].diff(periods=diff_win).iloc[-1].rank(pct=True)
    
        alpha = np.sign(part1) * (part1.abs() ** part2)
        return alpha

    def alpha101_87(self, data, param1=2, param2=3, param3=0.37, param4=0.63, param5=12, param6=5,
                 param7=14,
                 dependencies=['close_price', 'vwap', 'turnover_vol','indu'], max_window=30):
        # -1 * max(rank(decay_linear(delta(((CLOSE * 0.369701) + (VWAP * (1 - 0.369701))), 1.91233), 2.65461)),
        # ts_rank(decay_linear(abs(correlation(indneutralize(ADV81, indclass), CLOSE, 13.4132)), 4.89768), 14.4535))
    
        # rank(decay_linear(delta(((CLOSE * 0.369701) + (VWAP * (1 - 0.369701))), 1.91233), 2.65461))
        diff_win, decay_win = param1, param2
        price_df = data['close_price'] * param3 + data['vwap'] * param4
        part1 = decay_linear(price_df.diff(periods=diff_win), win=decay_win).rank(pct=True)

        # ts_rank(decay_linear(abs(correlation(indneutralize(ADV81, indclass), CLOSE, 13.4132)), 4.89768), 14.4535)
        neu_adv = df_indneutralize(data['turnover_vol'].rolling(window=8).mean(), data['indu'])
        corr_win, decay_win, ts_rank_win = param5, param6, param6
        decay_df = rolling_decay(rolling_corr(neu_adv, data['close_price'], win=corr_win).abs(), win=decay_win)
        part2 = decay_df.iloc[-ts_rank_win:].rank(axis=0, pct=True).iloc[-1]
    
        alpha101_df = pd.DataFrame({'part1': part1, 'part2': part2})
    
        # # original alpha formula
        # alpha = -1.0 * alpha101_df.max(axis=1)
        # adjusted alpha formula
        alpha = -1.0 * (alpha101_df.max(axis=1) - 0.5 * alpha101_df.min(axis=1))
        return alpha
    
    def alpha101_88(self, data, param1=8, param2=20, param3=9, param4=20, param5=9, param6=6, param7=20,
                 dependencies=['open_price', 'highest_price', 
                               'lowest_price', 'close_price', 
                               'turnover_vol'], max_window=50):
        # min(rank(decay_linear(((rank(OPEN) + rank(LOW)) - (rank(HIGH) + rank(CLOSE))), 8.06882)),
        # ts_rank(decay_linear(correlation(ts_rank(CLOSE, 8.44728), ts_rank(ADV60, 20.6966), 8.01266), 6.65053), 2.61957))

        # rank(decay_linear(((rank(OPEN) + rank(LOW)) - (rank(HIGH) + rank(CLOSE))), 8.06882))
        decay_win = param1
        open_low = data['open_price'].rank(axis=1, pct=True) + data['lowest_price'].rank(axis=1, pct=True)
        high_close = data['highest_price'].rank(axis=1, pct=True) + data['close_price'].rank(axis=1, pct=True)
        part1 = decay_linear(open_low - high_close, win=decay_win).rank(pct=True)

        # ts_rank(decay_linear(correlation(ts_rank(CLOSE, 8.44728), ts_rank(ADV60, 20.6966), 8.01266), 6.65053), 2.61957)
        adv_win, ts_close_win, ts_adv_win = param2, param3, param4
        adv_df = data['turnover_vol'].rolling(window=adv_win).mean()
        rank_close = rolling_rank(data['close_price'], win=ts_close_win)
        rank_adv = rolling_rank(adv_df, win=ts_adv_win)
        corr_win, decay_win, ts_rank_win = param5, param6, param7   # change from original 8.01266, 6.65053, 2.61957 to 9, 6, 10
        decay_df = rolling_decay(rolling_corr(rank_close, rank_adv, win=corr_win), win=decay_win)
        part2 = decay_df.iloc[-ts_rank_win:].rank(axis=0, pct=True).iloc[-1]
    
        # original
        alpha = pd.DataFrame({'part1': part1, 'part2': part2}).min(axis=1)
        # # adjusted formula
        # alpha = pd.DataFrame({'part1': part1, 'part2': part2}).mean(axis=1)
        return alpha
    
    def alpha101_90(self, data, param1=5, param2=4, param3=8, param4=8, param5=6, param6=-1,
                 dependencies=['close_price', 'lowest_price', 'turnover_vol','indu'], max_window=20):
        # -1 *(rank((CLOSE - ts_max(CLOSE, 4.66719)))^
        # ts_rank(correlation(indneutralize(ADV40, indclass), LOW, 5.38375), 3.21856))
    
        # rank((CLOSE - ts_max(CLOSE, 4.66719)))
        # add a decay linear
        close_df = data['close_price'] - data['close_price'].rolling(window=param1).max()
        part1 = decay_linear(close_df.rank(axis=1, pct=True), win=param2).rank(pct=True)
    
        # ts_rank(correlation(indneutralize(ADV40, indclass), LOW, 5.38375), 3.21856)
        neu_adv = df_indneutralize(data['turnover_vol'].rolling(window=param3).mean(), data['indu'])
        corr_win, ts_rank_win = param4, param5
        corr_df = rolling_corr(neu_adv, data['lowest_price'], win=corr_win)
        part2 = corr_df.iloc[-ts_rank_win:].rank(axis=0, pct=True).iloc[-1]
    
        alpha = float(param6) * part1 ** part2
        return alpha
    
    def alpha101_91(self, data, param1=5, param2=10, param3=10, param4=3, param5=10, 
                 param6=8, param7=3, param8=-1,
                 dependencies=['close_price', 'turnover_vol', 
                               'vwap','indu'], max_window=32):
        # -1 * (ts_rank(decay_linear(decay_linear(correlation(indneutralize(CLOSE, indclass), VOLUME, 9.74928), 16.398), 3.83219), 4.8667) -
        # rank(decay_linear(correlation(VWAP, ADV30, 4.01303), 2.6809)))

        neu_close = df_indneutralize(data['close_price'], data['indu'])
        adv = data['turnover_vol'].rolling(window=param1).mean()

        # ts_rank(decay_linear(decay_linear(correlation(indneutralize(CLOSE, indclass), VOLUME, 9.74928), 16.398), 3.83219), 4.8667)
        corr_win, decay_win1, decay_win2, ts_rank_win = param2, param3, param4, param5
        decay_df = rolling_decay(rolling_decay(rolling_corr(neu_close, data['turnover_vol'], 
                                                            win=corr_win), win=decay_win1), win=decay_win2)
        part1 = decay_df.iloc[-ts_rank_win:].rank(axis=0, pct=True).iloc[-1]

        # rank(decay_linear(correlation(VWAP, ADV30, 4.01303), 2.6809))
        corr_win, decay_win = param6, param7
        part2 = decay_linear(rolling_corr(data['vwap'], adv, win=corr_win), win=decay_win)
    
        alpha = float(param8) * (part1 - part2)
        return alpha
    
    def alpha101_96(self, data, param1=6, param2=4, param3=14, param4=20, param5=8, param6=6,
                 param7=8, param8=12, param9=6, param10=13, param11=-1,
                 dependencies=['vwap', 'turnover_vol', 
                               'close_price'], max_window=50):
        # -1.0 * max(ts_rank(decay_linear(correlation(rank(VWAP), rank(VOLUME), 3.83878), 4.16783), 8.38151),
        # ts_rank(decay_linear(ts_argmax(correlation(ts_rank(CLOSE, 7.45404), ts_rank(ADV60, 4.13242), 3.65459), 12.6556), 14.0365), 13.4143))

        # ts_rank(decay_linear(correlation(rank(VWAP), rank(VOLUME), 3.83878), 4.16783), 8.38151)
        rank_vwap = data['vwap'].rank(axis=1, pct=True)
        rank_vol = data['turnover_vol'].rank(axis=1, pct=True)
        corr_win, decay_win, ts_rank_win = param1, param2, param3
        decay_df = rolling_decay(rolling_corr(rank_vwap, rank_vol, win=corr_win), win=decay_win)
        part1 = decay_df.iloc[-ts_rank_win:].rank(axis=0, pct=True).iloc[-1]

        # ts_rank(decay_linear(ts_argmax(correlation(ts_rank(CLOSE, 7.45404), ts_rank(ADV60, 4.13242), 3.65459), 12.6556), 14.0365), 13.4143)
        adv_win, ts_close_win, ts_adv_win = param4, param5, param6
        adv_df = data['turnover_vol'].rolling(window=adv_win).mean()
        rank_close = rolling_rank(data['close_price'], win=ts_close_win)
        rank_adv = rolling_rank(adv_df, win=ts_adv_win)
        # change from original 3.65459, 12.6556, 14.0365, 13.4143 to 8, 12, 6, 13
        corr_win, ts_max_win, decay_win, ts_rank_win = param7, param8, param9, param10
        corr_df = rolling_corr(rank_close, rank_adv, win=corr_win)
        ts_argmax_df = corr_df.rolling(window=ts_max_win).apply(lambda x: x.argmax())
        decay_df = rolling_decay(ts_argmax_df, win=decay_win)
        part2 = decay_df.iloc[-ts_rank_win:].rank(axis=0, pct=True).iloc[-1]
    
        # original formula
        alpha = float(param11) * pd.DataFrame({'part1': part1, 'part2': part2}).max(axis=1)
        # # adjusted formula
        # alpha = -1 * pd.DataFrame({'part1': part1, 'part2': part2}).mean(axis=1)
        return alpha
    
    def alpha101_97(self, data, param1=4, param2=12, param3=0.7, param4=10, param5=17,
                 param6=8, param7=18, param8=5, param9=16, param10=-1,
                 dependencies=['lowest_price', 'vwap', 'turnover_vol', 'lowest_price','indu'], max_window=45):
        # -(rank(decay_linear(delta(indneutralize(((LOW * 0.721001) + (VWAP * (1 - 0.721001))), indclass), 3.3705), 20.4523)) -
            # ts_rank(decay_linear(ts_rank(correlation(ts_rank(LOW, 7.87871), ts_rank(ADV60, 17.255), 4.97547), 18.5925), 15.7152), 6.71659))
    
        # rank(decay_linear(delta(indneutralize(((LOW * 0.721001) + (VWAP * (1 - 0.721001))), indclass), 3.3705), 20.4523))
        diff_win, decay_win = param1, param2
        price_df = data['lowest_price'] * param3 + data['vwap'] * (1 - param3)
        part1 = decay_linear(df_indneutralize(price_df, data['indu']).diff(periods=diff_win), win=decay_win).rank(pct=True)
    
            # ts_rank(decay_linear(ts_rank(correlation(ts_rank(LOW, 7.87871), ts_rank(ADV60, 17.255), 4.97547), 18.5925), 15.7152), 6.71659)
        ts_rank_low = rolling_rank(data['lowest_price'], win=param4)
        ts_rank_adv = rolling_rank(data['turnover_vol'].rolling(window=param4).mean(), win=param5)
        corr_win, ts_win1, decay_win, ts_win2 = param6, param7, param8, param9
        decay_df = rolling_decay(rolling_rank(rolling_corr(ts_rank_low, ts_rank_adv, win=corr_win), win=ts_win1), win=decay_win)
        part2 = decay_df.iloc[-ts_win2:].rank(axis=0, pct=True).iloc[-1]
    
        alpha = float(param10) * (part1 - part2)
        return alpha
    
    def alpha101_99(self, data, param1=20, param2=16, param3=16, param4=9, param5=2, param6=7, param7=-1,
                 dependencies=['highest_price', 'lowest_price', 'turnover_vol'], max_window=50):
        # -1 * (rank(correlation(sum(((HIGH + LOW) / 2), 19.8975), sum(ADV60, 19.8975), 8.8136)) <
        # rank(correlation(LOW, VOLUME, 6.28259)))
        # rearranged formula: between two parts, use - instead of <
        # -1 * (rank(correlation(sum(((HIGH + LOW) / 2), 19.8975), sum(ADV60, 19.8975), 8.8136)) -
        # rank(correlation(LOW, VOLUME, 6.28259)))

        # rank(correlation(sum(((HIGH + LOW) / 2), 19.8975), sum(ADV60, 19.8975), 8.8136))
        adv_win, sum_price_win, sum_adv_win, corr_win = param1, param2, param3, param4
        sum_price = ((data['highest_price'] + data['lowest_price']) / float(param5)).rolling(window=sum_price_win).mean()
        sum_adv = data['turnover_vol'].rolling(window=adv_win).mean().rolling(window=sum_adv_win).mean()
        part1 = sum_price.iloc[-corr_win:].corrwith(sum_adv.iloc[-corr_win:]).rank(pct=True)
    
        # rank(correlation(LOW, VOLUME, 6.28259))
        corr_win = param6
        part2 = data['lowest_price'].iloc[-corr_win:].corrwith(data['turnover_vol'].iloc[-corr_win:]).rank(pct=True)
    
        alpha = float(param7) * (part1 - part2)
        return alpha
