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

# rolling cov of two pandas dataframes
def rolling_cov(x, y, win):
    cov_df = pd.DataFrame(data=np.NaN, index=x.index, columns=x.columns)
    for begin, end in zip(x.index[:-win + 1], x.index[win - 1:]):
        x_std = x.loc[begin:end].std()
        y_std = y.loc[begin:end].std()
        cov_df.loc[end] = x.loc[begin:end].corrwith(y.loc[begin:end]) * x_std * y_std
    return cov_df

# rolling corr of two pandas dataframes
def rolling_corr(x, y, win):
    corr_df = pd.DataFrame(data=np.NaN, index=x.index, columns=x.columns)
    for begin, end in zip(x.index[:-win + 1], x.index[win - 1:]):
        corr_df.loc[end] = x.loc[begin:end].corrwith(y.loc[begin:end])
    return corr_df

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


@six.add_metaclass(Singleton)
class FactorAlpha191(object):
    def __init__(self):
        __str__ = 'factor_alpha191'
        self.name = 'Alpha191'
        self.factor_type1 = 'Features'
        self.factor_type2 = 'Features'
        self.desciption = 'price and volumns features'
    
    # (-1*CORR(RANK(DELTA(LOG(VOLUME),1)),RANK(((CLOSE-OPEN)/OPEN)),6)
    def alpha191_1(self, data, param1=1, param2=6, dependencies=['close_price','open_price',
                                                'turnover_vol'], max_window=7):
        '''
        This is alpha191_1
        :param param1: int
        :param param2: int
        :return: (-1*CORR(RANK(DELTA(LOG(VOLUME),1)),RANK(((CLOSE-OPEN)/OPEN)),6)
        '''
        vol = (np.log(data['turnover_vol']).diff(param1)).rank(axis=1, pct=True)
        ret = (data['close_price'] / data['open_price'] - 1.0).rank(axis=1, pct=True)
        vol = vol.tail(param2)
        ret = ret.tail(param2)
        res = vol.corrwith(ret).sort_index() * (-1)
        return res
    
    # -1*delta(((close-low)-(high-close))/(high-low),1)
    def alpha191_2(self, data, param1=2, param2=1, param3=-1, dependencies=['close_price','lowest_price',
                                                                         'highest_price'], max_window=2):
        '''
        This is alpha191_1
        :param param1: int
        :param param2: int
        :param param3: int
        :return: -1*delta(((close-low)-(high-close))/(high-low),1)
        '''
        win_ratio = (param1*data['close_price']-data['lowest_price']-data['highest_price'])\
                                /(data['highest_price']-data['lowest_price'])
        return win_ratio.diff(param2).iloc[-1] * (param3)
    
    # SUM计算问题 fix me
    #-1*SUM((CLOSE=DELAY(CLOSE,1)?0:CLOSE-(CLOSE>DELAY(CLOSE,1)?MIN(LOW,DELAY(CLOSE,1)):MAX(HIGH,DELAY(CLOSE,1)))),6)
    # 这里SUM应该为TSSUM
    def alpha191_3(self, data, param1=-1, dependencies=['close_price','lowest_price','highest_price'], max_window=6):
        
        '''
        This is alpha191_1
        :param param1: int
        :return: -1*SUM((CLOSE=DELAY(CLOSE,1)?0:CLOSE-(CLOSE>DELAY(CLOSE,1)?MIN(LOW,DELAY(CLOSE,1)):MAX(HIGH,DELAY(CLOSE,1)))),6)
        '''
        alpha = pd.DataFrame(np.zeros(data['close_price'].values.shape), 
                             index=data['close_price'].index, 
                             columns=data['close_price'].columns)
        condition2 = data['close_price'].diff(periods=1) > 0.0
        condition3 = data['close_price'].diff(periods=1) < 0.0
        alpha[condition2] = data['close_price'][condition2] - np.minimum(data['close_price'][condition2].shift(1), 
                                                                         data['lowest_price'][condition2])
        alpha[condition3] = data['close_price'][condition3] - np.maximum(data['close_price'][condition3].shift(1), 
                                                                         data['highest_price'][condition3])
        return alpha.sum(axis=0) * (param1)
    
    # (((SUM(CLOSE,8)/8)+STD(CLOSE,8))<(SUM(CLOSE,2)/2))?-1:(SUM(CLOSE,2)/2<(SUM(CLOSE,8)/8-STD(CLOSE,8))?1:(1<=(VOLUME/MEAN(VOLUME,20))?1:-1))
    def alpha191_4(self, data, param1=8, param2=8, param3=2, param4=2, param5=8, param6=8, param7=20,
                dependencies=['close_price','turnover_vol'], max_window=20):        
        '''
        This is alpha191_1
        :param param1: int
        :param param2: int
        :param param3: int
        :param param4: int
        :param param5: int
        :param param6: int
        :param param7: int
        :return: (((SUM(CLOSE,8)/8)+STD(CLOSE,8))<(SUM(CLOSE,2)/2))?-1:(SUM(CLOSE,2)/2<(SUM(CLOSE,8)/8-STD(CLOSE,8))?1:(1<=(VOLUME/MEAN(VOLUME,20))?1:-1))
        '''
    # 注:取值排序有随机性
        condition1 = data['close_price'].rolling(center=False, window=param1).std() + data['close_price'].rolling(
            center=False, window=param2).mean() < data['close_price'].rolling(center=False, window=param3).mean()
        
        condition2 = data['close_price'].rolling(center=False, window=param4).mean() < data['close_price'].rolling(
            center=False, window=param5).mean() - data['close_price'].rolling(center=False, window=param6).std()
        
        condition3 = 1 <= data['turnover_vol'] / data['turnover_vol'].rolling(center=False, window=param7).mean()
        
        indicator1 = pd.DataFrame(np.ones(data['close_price'].shape), index=data['close_price'].index,
                                  columns=data['close_price'].columns)
        indicator2 = -pd.DataFrame(np.ones(data['close_price'].shape), index=data['close_price'].index,
                                   columns=data['close_price'].columns)
        part1 = indicator2[condition1].fillna(0)
        part2 = (indicator1[~condition1][condition2]).fillna(0)
        part3 = (indicator1[~condition1][~condition2][condition3]).fillna(0)
        part4 = (indicator2[~condition1][~condition2][~condition3]).fillna(0)
        result = (part1 + part2 + part3 + part4).iloc[-1]
        return result
    
    # -1*TSMAX(CORR(TSRANK(VOLUME,5),TSRANK(HIGH,5),5),3)
    def alpha191_5(self, data, param1=5, param2=5, param3=5, param4=-3, param5=-1,
                    dependencies=['turnover_vol', 'highest_price'], max_window=13):
        corr_win = param1
        corr_df = rolling_corr(rolling_rank(data['turnover_vol'], param2), rolling_rank(data['highest_price'], param3), corr_win)
        alpha = corr_df.iloc[-param4:].max(axis=0) * (param5)
        return alpha
    
    # -1*RANK(SIGN(DELTA(OPEN*0.85+HIGH*0.15,4)))
    def alpha191_6(self, data, param1=0.85, param2=0.15, param3=4, param4=-1,
                dependencies=['open_price', 'highest_price'], max_window=5):
        
        '''
        This is alpha191_1
        :param param1: double
        :param param2: double
        :param param3: double
        :param param4: double
        :return: -1*RANK(SIGN(DELTA(OPEN*0.85+HIGH*0.15,4)))
        '''
        # 注:取值排序有随机性
        signs = np.sign((data['open_price'] * param1 + data['highest_price'] * param2).diff(param3))
        alpha = (signs.rank(axis=1, pct=True)).iloc[-1] * (param4)
        return alpha
    
    # (RANK(MAX(VWAP-CLOSE,3))+RANK(MIN(VWAP-CLOSE,3)))*RANK(DELTA(VOLUME,3))
    def alpha191_7(self, data, param1=3, param2=3, param3=3, 
                dependencies=['turnover_vol', 'vwap', 'close_price'], max_window=4):
        '''
        This is alpha191_1
        :param param1: double
        :param param2: double
        :param param3: double
        :return: (RANK(MAX(VWAP-CLOSE,3))+RANK(MIN(VWAP-CLOSE,3)))*RANK(DELTA(VOLUME,3))
        '''
            
        # 感觉MAX应该为TSMAX
        part1 = (data['vwap'] - data['close_price']).rolling(window=param1,min_periods=param1).max().rank(axis=1, pct=True)
        part2 = (data['vwap'] - data['close_price']).rolling(window=param2,min_periods=param2).min().rank(axis=1, pct=True)
        part3 = data['turnover_vol'].diff(param3).rank(axis=1, pct=True).iloc[-1]
        alpha = (part1 + part2) * part3
        return alpha.iloc[-1]
    
    # -1*RANK(DELTA((HIGH+LOW)/10+VWAP*0.8,4))
    def alpha191_8(self, data,param1=0.1, param2=0.8,param3=4,param4=-1,
                dependencies=['turnover_vol', 'vwap', 'highest_price', 'lowest_price'], max_window=5):
        '''
        This is alpha191_1
        :param param1: double
        :param param2: double
        :param param3: double
        :param param4: double
        :return: -1*RANK(DELTA((HIGH+LOW)/10+VWAP*0.8,4))
        '''
            
        # 受股价单价影响,反转
        ma_price = data['lowest_price']*param1 + data['lowest_price']*param1 + data['vwap']*param2
        alpha = ma_price.diff(param3).rank(axis=1, pct=True, na_option='keep').iloc[-1] * (param4)
        return alpha
    
    # SMA(((HIGH+LOW)/2-(DELAY(HIGH,1)+DELAY(LOW,1))/2)*(HIGH-LOW)/VOLUME,7,2)
    def alpha191_9(self, data, param1=0.5, param2=1, param3=0.5, param4=7, param5=2,
                dependencies=['highest_price', 'lowest_price', 'turnover_vol'], max_window=8):
        
        '''
        This is alpha191_1
        :param param1: double
        :param param2: double
        :param param3: double
        :param param4: double
        :param param5: double
        :return:SMA(((HIGH+LOW)/2-(DELAY(HIGH,1)+DELAY(LOW,1))/2)*(HIGH-LOW)/VOLUME,7,2)
        '''
        part1 = (data['highest_price']+data['lowest_price'])*param1-(data['highest_price'].shift(param2)+\
                                                                    data['lowest_price'].shift(param2))*param3
        part2 = part1 * (data['highest_price']-data['lowest_price']) / data['turnover_vol']
        alpha = part2.ewm(adjust=False, alpha=float(param4)/param4, min_periods=0, ignore_na=False).mean().iloc[-1]
        return alpha

    # RANK(MAX(((RET<0)?STD(RET,20):CLOSE)^2,5))
    def alpha191_10(self, data, param1=20, param2=2, param3=5,
                 dependencies=['close_price'], max_window=25):
    # 没法解释,感觉MAX应该为TSMAX
        '''
        This is alpha191_1
        :param param1: double
        :param param2: double
        :param param3: double
        :return:RANK(MAX(((RET<0)?STD(RET,20):CLOSE)^2,5))
        '''
        
        ret = data['close_price'].pct_change(periods=1)
        part1 = ret.rolling(window=param1, min_periods=param1).std()
        condition = ret >= 0.0
        part1[condition] = data['close_price'][condition]
        alpha = (part1 ** param2).rolling(window=param3,min_periods=param3).max().rank(axis=1, pct=True)
        return alpha.iloc[-1]
    
        
    # SUM计算问题 fix me
    # SUM(((CLOSE-LOW)-(HIGH-CLOSE))/(HIGH-LOW).*VOLUME,6)
    def alpha191_11(self, data, param1=2, dependencies=['close_price','lowest_price','highest_price','turnover_vol'], max_window=6):
        # 近6天获利盘比例
        var = (param1*data['close_price']-data['lowest_price']-data['highest_price'])/(data['highest_price']-data['lowest_price'])*data['turnover_vol']
        return var.sum(axis=0) * (-1)
    
    # RANK(OPEN-MA(VWAP,10))*RANK(ABS(CLOSE-VWAP))*(-1)
    def alpha191_12(self, data, param1=10, param2=-1, dependencies=['open_price','close_price','turnover_vol', 'vwap'],
                 max_window=10):
        part1 = (data['open_price']-data['vwap'].rolling(window=param1,center=False).mean()).rank(axis=1, pct=True)
        part2 = abs(data['close_price']-data['vwap']).rank(axis=1, pct=True)
        alpha = part1.iloc[-1] * part2.iloc[-1] * (param2)
        return alpha
    
    # ((HIGH*LOW)^0.5)-VWAP fix me
    def alpha191_13(self, data, param1=0.5, dependencies=['highest_price','lowest_price', 'vwap'], max_window=1):
        # 要注意VWAP/price是否复权
        alpha = np.sqrt(data['highest_price'] * data['lowest_price']) - data['vwap']
        return alpha.iloc[-1]
    
    # CLOSE-DELAY(CLOSE,5)
    def alpha191_14(self, data, param1=5, dependencies=['close_price'], max_window=6):
        # 与股价相关，利好茅台
        
        return data['close_price'].diff(param1).iloc[-1]

    # OPEN/DELAY(CLOSE,1)-1
    def alpha191_15(self, data, param1=1, param2=-1, dependencies=['open_price', 'close_price'], max_window=2):
        # 跳空高开/低开
        return (data['open_price']/data['close_price'].shift(param1)+ param2).iloc[-1]
    
    # (-1*TSMAX(RANK(CORR(RANK(VOLUME),RANK(VWAP),5)),5))
    def alpha191_16(self, data, param1=5, param2=5, param3=-1, dependencies=['turnover_vol', 'vwap'], max_window=10):
        # 感觉其中有个TSRANK
        
        vol = data['turnover_vol'].rank(axis=1, pct=True)
        vwap = data['vwap'].rank(axis=1, pct=True)
        corr_win = param1
        corr_df = rolling_corr(vwap, vol, corr_win)
        alpha = corr_df.rank(axis=1, pct=True)
        alpha = alpha.iloc[param2:].max(axis=0) * (param3)
        return alpha
    
    # RANK(VWAP-MAX(VWAP,15))^DELTA(CLOSE,5)
    def alpha191_17(self, data, param1=15, param2=5, dependencies=['close_price', 'vwap'], max_window=16):
        delta_price = data['close_price'].diff(param2).iloc[-1]
        var = data['vwap'] -data['vwap'] .rolling(window=param1,min_periods=param1).max()
        alpha = var.rank(axis=1, pct=True).iloc[-1] ** delta_price
        return alpha
    
    
    # CLOSE/DELAY(CLOSE,5)
    def alpha191_18(self, data, param1=5, dependencies=['close_price'], max_window=6):
        # 近5日涨幅, REVS5
        return (data['close_price'] / data['close_price'].shift(param1)).iloc[-1]

    
    # (CLOSE<DELAY(CLOSE,5)?(CLOSE/DELAY(CLOSE,5)-1):(CLOSE=DELAY(CLOSE,5)?0:(1-DELAY(CLOSE,5)/CLOSE)))
    def alpha191_19(self, data, param1=5, param2=5, param3=5, dependencies=['close_price'], max_window=6):
        # 类似于近五日涨幅
        condition1 = data['close_price'] <= data['close_price'].shift(param1)
        alpha = pd.DataFrame(np.zeros(data['close_price'].shape), index=data['close_price'].index, 
                             columns=data['close_price'].columns)
        alpha[condition1] = data['close_price'].pct_change(periods=param2)[condition1]
        alpha[~condition1] = -data['close_price'].pct_change(periods=param3)[~condition1]
        return alpha.iloc[-1]
    
    # (CLOSE/DELAY(CLOSE,6)-1)*100
    def alpha191_20(self, data, param1=6, dependencies=['close_price'], max_window=7):
        # 近6日涨幅
        return (data['close_price'].pct_change(periods=param1) * 100.0).iloc[-1]
    ''' 
    # REGBETA(MEAN(CLOSE,6),SEQUENCE(6))
    def alpha191_21(self, data, param1=6, param2=6, dependencies=['close_price'], max_window=12):
        ma_price = data['close_price'].rolling(window=param1, min_periods=param1).mean()
        seq = np.array([i for i in range(1, param2+1)])
        win = param2
        a = np.vstack([np.array([i for i in range(1, win+1)]), np.ones(win)]).T
        alpha = ma_price.iloc[-param2:].apply(lambda x: np.linalg.lstsq(a, x.values.T)[0][0])
        return alpha
    '''
    # SMEAN((CLOSE/MEAN(CLOSE,6)-1-DELAY(CLOSE/MEAN(CLOSE,6)-1,3)),12,1)
    def alpha191_22(self, data, param1=6, param2=3, param3=12, 
                 dependencies=['close_price'], max_window=21):
        # 猜SMEAN是SMA
        ratio = data['close_price'] / data['close_price'].rolling(window=param1,min_periods=param1).mean() - 1.0
        alpha = ratio.diff(param2).ewm(adjust=False, alpha=float(1)/param3, min_periods=param3, 
                                       ignore_na=False).mean().iloc[-1]
        return alpha

    # SMA((CLOSE>DELAY(CLOSE,1)?STD(CLOSE,20):0),20,1) /
    # (SMA((CLOSE>DELAY(CLOSE,1)?STD(CLOSE,20):0),20,1)+SMA((CLOSE<=DELAY(CLOSE,1)?STD(CLOSE,20):0),20,1))
    # *100
    def alpha191_23(self, data, param1=20, param2=1, param3=20, param4=20, 
                 dependencies=['close_price'], max_window=40):
        prc_std = data['close_price'].rolling(window=param1, min_periods=param1).std()
        condition1 = data['close_price'] > data['close_price'].shift(param2)
        part1 = prc_std.copy(deep=True)
        part2 = prc_std.copy(deep=True)
        part1[~condition1] = 0.0
        part2[condition1] = 0.0
        alpha = part1.ewm(adjust=False, alpha=float(1)/param3, min_periods=param3, ignore_na=False).mean() / \
            (part1.ewm(adjust=False, alpha=float(1)/param3, min_periods=param3, ignore_na=False).mean() + \
             part2.ewm(adjust=False, alpha=float(1)/param4, min_periods=param4, ignore_na=False).mean()) * 100
        return alpha.iloc[-1]
    
    # SMA(CLOSE-DELAY(CLOSE,5),5,1)
    def alpha191_24(self, data, param1=5, param2=5, param3=1, dependencies=['close_price'], max_window=10):
        
        return data['close_price'].diff(param1).ewm(adjust=False, alpha=float(1)/param1, 
                                              min_periods=param2, ignore_na=False).mean().iloc[-1]
    
    # (-1*RANK(DELTA(CLOSE,7)*(1-RANK(DECAYLINEAR(VOLUME/MEAN(VOLUME,20),9)))))*(1+RANK(SUM(RET,250)))
    def alpha191_25(self, data, param1=7, param2=20, param3=9, param4=250,
                 dependencies=['close_price', 'turnover_vol'], max_window=251):
        w = preprocessing.normalize(np.matrix([i for i in range(1, param3+1)]),norm='l1',axis=1).reshape(-1)
        ret = data['close_price'].pct_change(periods=1)
        part1 = data['close_price'].diff(param1)
        part2 = data['turnover_vol']/(data['turnover_vol'].rolling(window=param2,min_periods=param2).mean())
        part2 = 1.0 - rolling_dot(part2, w, param3).rank(axis=1, pct=True)
        part3 = 1.0 + ret.rolling(window=param4, min_periods=param4).sum().rank(axis=1, pct=True)
        alpha = (-1.0) * (part1 * part2).rank(axis=1, pct=True) * part3
        return alpha.iloc[-1]

    # (SUM(CLOSE,7)/7-CLOSE+CORR(VWAP,DELAY(CLOSE,5),230))
    def alpha191_26(self, data, param1=7, param2=5, param3=230, dependencies=['close_price', 'vwap'], max_window=235):
        vwap = data['vwap']
        part1 = data['close_price'].rolling(window=param1, min_periods=param1).mean() - data['close_price']
        # part2 = vwap.rolling(window=230, min_periods=230).corr(data['close'].shift(5))
        corr_win = param3
        part2 = vwap.iloc[-corr_win:].corrwith(data['close_price'].shift(param2).iloc[-corr_win:])
        return (part1 + part2).iloc[-1]

    # WMA((CLOSE-DELTA(CLOSE,3))/DELAY(CLOSE,3)*100+(CLOSE-DELAY(CLOSE,6))/DELAY(CLOSE,6)*100,12)
    def alpha191_27(self, data, param1=3, param2=6, param3=12, dependencies=['close_price'], max_window=18):
        part1 = data['close_price'].pct_change(periods=param1) * 100.0 + data['close_price'].pct_change(periods=param2) * 100.0
        w = preprocessing.normalize(np.matrix([i for i in range(1, param3+1)]),norm='l1',axis=1).reshape(-1)
        return rolling_dot(part1, w, param3).iloc[-1]


    def alpha191_28(self, data, param1=9, param2=9, param3=3, param4=3,
                 dependencies=['close_price', 'lowest_price', 'highest_price'], max_window=100):
 
        # 类似于KDJ_J
        # 计算速度很快，又涉及到ewm，所以窗口期可以取长一点没关系
        low_tsmin = data['lowest_price'].rolling(window=param1, min_periods=param1).min()
        high_tsmax = data['highest_price'].rolling(window=param2, min_periods=param2).max()

        var = (data['close_price'] - low_tsmin) / (high_tsmax - low_tsmin) * 100
        var_sma = var.ewm(adjust=False, alpha=1.0/param3, min_periods=0, ignore_na=False).mean()
        kdj = var * param4 - var_sma.ewm(adjust=False, alpha=1.0/param3, min_periods=0, ignore_na=False).mean() * 2.0
        return kdj.iloc[-1]

    # (CLOSE-DELAY(CLOSE,6))/DELAY(CLOSE,6)*VOLUME
    def alpha191_29(self, data, param1=6, dependencies=['close_price', 'turnover_vol'], max_window=7):
        # 获利成交量
        return (data['close_price'].pct_change(periods=param1)*data['turnover_vol']).iloc[-1]

    # (CLOSE-MEAN(CLOSE,12))/MEAN(CLOSE,12)*100
    def alpha191_31(self, data, param1 = 12, dependencies=['close_price'], max_window=12):
        return ((data['close_price']/data['close_price'].rolling(window=param1,min_periods=param1).mean()-1.0)*100).iloc[-1]
    
    # (-1*SUM(RANK(CORR(RANK(HIGH),RANK(VOLUME),3)),3))
    def alpha191_32(self, data, param1=3, param2=-3, param3=-1, dependencies=['highest_price', 'turnover_vol'], max_window=6):
    # 量价齐升/反转
        high = data['highest_price'].rank(axis=1, pct=True)
        vol = data['turnover_vol'].rank(axis=1, pct=True)

        corr_win = param1
        corr_df = rolling_corr(high, vol, corr_win)
        alpha = corr_df.rank(axis=1, pct=True).iloc[param2:].sum(axis=0) * (param3)
        return alpha
    
    # (-1*TSMIN(LOW,5)+DELAY(TSMIN(LOW,5),5))*RANK((SUM(RET,240)-SUM(RET,20))/220)*TSRANK(VOLUME,5)
    def alpha191_33(self, data, param1=5, param2=5, param3=240, param4=20, param5=220, param6=-5,
                 dependencies=['lowest_price', 'close_price', 'turnover_vol'], max_window=241):
        part1 = data['lowest_price'].rolling(window=param1, min_periods=param1).min().diff(param2) * (-1)
        ret = data['close_price'].pct_change(periods=1)
        part2 = ((ret.rolling(window=param3, min_periods=param3).sum() - \
                  ret.rolling(window=param4, min_periods=param4).sum()) / param5).rank(axis=1, pct=True)
        part3 = data['turnover_vol'].iloc[param6:].rank(axis=0, pct=True)
        alpha = part1.iloc[-1] * part2.iloc[-1] * part3.iloc[-1]
        return alpha
    
    # MEAN(CLOSE,12)/CLOSE
    def alpha191_34(self, data, param1=12, dependencies=['close_price'], max_window=12):
        return (data['close_price'].rolling(window=param1, min_periods=param1).mean() / data['close_price']).iloc[-1]

    # (MIN(RANK(DECAYLINEAR(DELTA(OPEN,1),15)),RANK(DECAYLINEAR(CORR(VOLUME,OPEN*0.65+CLOSE*0.35,17),7)))*-1)
    def alpha191_35(self, data, param1=7, param2=15, param3=0.65, param4=0.35, param5=17, param6=-1,
                 dependencies=['open_price', 'close_price', 'turnover_vol'], max_window=24):
    # 猜后一项OPEN为CLOSE
        w7 = preprocessing.normalize(np.matrix([i for i in range(1, param1+1)]),norm='l1',axis=1).reshape(-1)
        w15 = preprocessing.normalize(np.matrix([i for i in range(1, param2+1)]),norm='l1',axis=1).reshape(-1)

        corr_df = rolling_corr(data['open_price']*param3+data['close_price']*param4, data['turnover_vol'], param5)
        part1 = rolling_dot(data['open_price'].diff(periods=1), w15, param2).rank(axis=1, pct=True)
        part2 = rolling_dot(corr_df, w7, param1).rank(axis=1, pct=True)
        alpha = np.minimum(part1.iloc[-1], part2.iloc[-1]) * (param6)
        return alpha
    
    # RANK(SUM(CORR(RANK(VOLUME),RANK(VWAP),6),2))
    def alpha191_36(self, data, param1=6, param2=2, dependencies=['vwap', 'turnover_vol'], max_window=9):
        # 量价齐升, TSSUM
        vwap = data['vwap'].rank(axis=1, pct=True)
        vol = data['turnover_vol'].rank(axis=1, pct=True)
        corr_win = param1
        corr_df = rolling_corr(vwap, vol, corr_win)
        alpha = corr_df.rolling(window=param2, min_periods=2).sum().rank(axis=1, pct=True).iloc[-1]
        return alpha

    #(-1*RANK(SUM(OPEN,5)*SUM(RET,5)-DELAY(SUM(OPEN,5)*SUM(RET,5),10)))
    def alpha191_37(self, data, param1=5, param2=5, param3=10, param4=-1, 
                 dependencies=['open_price', 'close_price'], max_window=16):
        part1 = data['open_price'].rolling(window=param1, min_periods=param1).sum() * \
            (data['close_price'].pct_change(periods=1).rolling(window=param2, min_periods=param2).sum())
        alpha = part1.diff(periods=param3).rank(axis=1, pct=True).iloc[-1] * (param4)
        return alpha

    # ((SUM(HIGH,20)/20)<HIGH)?(-1*DELTA(HIGH,2)):0
    def alpha191_38(self, data, param1=20, param2=2, param3=-1, dependencies=['highest_price'], max_window=20):
        # 与股价相关，利好茅台
        condition = data['highest_price'].rolling(window=param1, min_periods=param1).mean() < data['highest_price']
        alpha = data['highest_price'].diff(periods=param2) * (param3)
        alpha[~condition] = 0.0
        return alpha.iloc[-1]

    # (RANK(DECAYLINEAR(DELTA(CLOSE,2),8))-RANK(DECAYLINEAR(CORR(VWAP*0.3+OPEN*0.7,SUM(MEAN(VOLUME,180),37),14),12)))*-1
    def alpha191_39(self, data, param1=2, param2=8, param3=0.3, param4=0.7, param5=180, param6=37, param7=14, param8=12, 
                 param9=-1, dependencies=['close_price', 'open_price', 'vwap', 'turnover_vol'], max_window=243):
        w8 = preprocessing.normalize(np.matrix([i for i in range(1, param2+1)]), norm='l1', axis=1).reshape(-1)
        w12 = preprocessing.normalize(np.matrix([i for i in range(1, param8+1)]), norm='l1', axis=1).reshape(-1)
        parta = data['vwap'] * param3 + data['open_price'] * param4
        partb = data['turnover_vol'].rolling(window=param5, min_periods=param5).mean().rolling(window=param6,
                                                                                               min_periods=param6).sum()
        part1 = rolling_dot(data['close_price'].diff(periods=param1), w8, param2).rank(axis=1, pct=True)
        part2 = rolling_dot(rolling_corr(parta, partb, param7), w12, param8).rank(axis=1, pct=True)
        return (part1 - part2).iloc[-1] * (param9)

    # SUM(CLOSE>DELAY(CLOSE,1)?VOLUME:0,26)/SUM(CLOSE<=DELAY(CLOSE,1)?VOLUME:0,26)*100
    def alpha191_40(self, data, param1=26, param2=1, dependencies=['close_price', 'turnover_vol'], max_window=30):
        # 即VR技术指标
        condition = data['close_price'] > data['close_price'].shift(param2)
        num = (data['turnover_vol'][condition]).rolling(window=param1, min_periods=1).sum()
        denom = (data['turnover_vol'][~condition]).rolling(window=param1, min_periods=1).sum()
        return (num / denom).iloc[-1]
    
    # RANK(MAX(DELTA(VWAP,3),5))*-1
    def alpha191_41(self, data, param1=3, param2=5, dependencies=['vwap'], max_window=9):
        var = data['vwap'].diff(periods=param1).rolling(window=param2, min_periods=param2).max()
        return var.rank(axis=1, pct=True).iloc[-1] * (-1)
    
    # (-1*RANK(STD(HIGH,10)))*CORR(HIGH,VOLUME,10)
    def alpha191_42(self, data, param1=10, param2=10 ,dependencies=['highest_price', 'turnover_vol'], 
                 max_window=10):
        # 价稳/量价齐升
        part1 = data['highest_price'].rolling(window=param1,min_periods=param1).std().rank(axis=1,pct=True) * (-1)
        corr_win = param2
        part2 = rolling_corr(data['highest_price'], data['turnover_vol'], corr_win)
        return (part1 * part2).iloc[-1]
   
    # (SUM(CLOSE>DELAY(CLOSE,1)?VOLUME:(CLOSE<DELAY(CLOSE,1)?-VOLUME:0),6))
    def alpha191_43(self, data, param1=1, param2=1, param3=1, param4=6, 
                 dependencies=['close_price', 'turnover_vol'], max_window=10):
        # 即OBV6指标
        vol = data['turnover_vol']
        mark = pd.DataFrame(index=vol.index, columns=vol.columns)
        mark[data['close_price'] > data['close_price'].shift(param1)] = 1.0
        mark[data['close_price'] == data['close_price'].shift(param2)] = 0.0
        mark[data['close_price'] < data['close_price'].shift(param3)] = -1.0
        vol_adj = vol * mark
        return vol_adj.iloc[-param4:].sum()
    
    # (TSRANK(DECAYLINEAR(CORR(LOW,MEAN(VOLUME,10),7),6),4)+TSRANK(DECAYLINEAR(DELTA(VWAP,3),10),15))
    def alpha191_44(self, data, param1=10, param2=7, param3=6, param4=4, param5=3, param6=10, param7=15,
                 dependencies=['vwap', 'turnover_vol', 'lowest_price'], max_window=29):
        w10 = preprocessing.normalize(np.matrix([i for i in range(1, param1+1)]),norm='l1',axis=1).reshape(-1)
        w6 = preprocessing.normalize(np.matrix([i for i in range(1, param2)]),norm='l1',axis=1).reshape(-1)

        corr_df = rolling_corr(data['turnover_vol'].rolling(
            window=param6,min_periods=param6).mean(), data['lowest_price'], param2)
        part1 = rolling_dot(corr_df, w6, param3).iloc[-param4:].rank(axis=0, pct=True)
        part2 = rolling_dot(data['vwap'].diff(periods=param5), w10, param6).iloc[-param7:].rank(axis=0, pct=True)
        return (part1 + part2).iloc[-1]
    
    # (RANK(DELTA(CLOSE*0.6+OPEN*0.4,1))*RANK(CORR(VWAP,MEAN(VOLUME,150),15)))
    def alpha191_45(self, data, param1=0.6, param2=0.4,param3=150,param4=15, dependencies=['open_price', 'close_price', 'vwap', 'turnover_vol'], max_window=165):
        part1 = (data['close_price'] * param1 + data['open_price'] * param2).diff(periods=1).rank(axis=1,pct=True)
        vol = data['turnover_vol'].rolling(window=param3,min_periods=param3).mean()
        part2 = rolling_corr(vol, data['vwap'], param4).rank(axis=1,pct=True)
        return (part1 * part2).iloc[-1]
    
    
    # (MEAN(CLOSE,3)+MEAN(CLOSE,6)+MEAN(CLOSE,12)+MEAN(CLOSE,24))/(4*CLOSE)
    def alpha191_46(self, data, param1=3, param2=6, param3=12, param4=24, param5=4.0,
                 dependencies=['close_price'], max_window=30):
        # 即BBIC技术指标
        ma_sum = None
        for win in [param1,param2,param3,param4]:
            ma = data['close_price'].rolling(window=win, min_periods=win).mean()
            ma_sum = ma if ma_sum is None else ma_sum + ma
        alpha = ma_sum / param5 / data['close_price']
        return alpha.iloc[-1]

    
    # SMA((TSMAX(HIGH,6)-CLOSE)/(TSMAX(HIGH,6)-TSMIN(LOW,6))*100,9,1)
    def alpha191_47(self, data, param1=6, param2=6, param3=6, param4=9,
                 dependencies=['close_price', 'lowest_price', 'highest_price'], max_window=15):
        # RSV技术指标变种
        part1 = (data['highest_price'].rolling(window=param1,min_periods=param1).max()-data['close_price']) / \
            (data['highest_price'].rolling(window=param2,min_periods=param2).max()-\
             data['lowest_price'].rolling(window=param3,min_periods=param3).min()) * 100
        alpha = part1.ewm(adjust=False, alpha=float(1)/param4, min_periods=0, ignore_na=False).mean().iloc[-1]
        return alpha

    # -1*RANK(SIGN(CLOSE-DELAY(CLOSE,1))+SIGN(DELAY(CLOSE,1)-DELAY(CLOSE,2))+SIGN(DELAY(CLOSE,2)-DELAY(CLOSE,3)))*SUM(VOLUME,5)/SUM(VOLUME,20)
    def alpha191_48(self, data, param1=1, param2=1, param3=2, param4=5, param5=20, param6=-1,
                 dependencies=['close_price', 'turnover_vol'], max_window=20):
        # 下跌缩量
        diff1 = data['close_price'].diff(param1)
        part1 = (np.sign(diff1) + np.sign(diff1.shift(param2)) + np.sign(diff1.shift(param3))).rank(axis=1, pct=True)
        part2 = data['turnover_vol'].rolling(window=param4, min_periods=param4).sum() / data['turnover_vol'].rolling(
            window=param5, min_periods=param5).sum()
        return (part1 * part2).iloc[-1] * (param6)

    def alpha191_49(self, data, dependencies=['highest_price', 'lowest_price'], max_window=13):
        # SUM(HIGH+LOW>=DELAY(HIGH,1)+DELAY(LOW,1)?0:MAX(ABS(HIGH-DELAY(HIGH,1)),ABS(LOW-DELAY(LOW,1))),12)/
        # (SUM(HIGH+LOW>=DELAY(HIGH,1)+DELAY(LOW,1)?0:MAX(ABS(HIGH-DELAY(HIGH,1)),ABS(LOW-DELAY(LOW,1))),12)+
        # SUM(HIGH+LOW<=DELAY(HIGH,1)+DELAY(LOW,1)?0:MAX(ABS(HIGH-DELAY(HIGH,1)),ABS(LOW-DELAY(LOW,1))),12))
        condition1 = (data['highest_price'] + data['lowest_price']) >= (data['highest_price'] + data['lowest_price']).shift(1)
        condition2 = (data['highest_price'] + data['lowest_price']) <= (data['highest_price'] + data['lowest_price']).shift(1)
        part1 = pd.DataFrame(np.zeros(data['highest_price'].shape), index=data['highest_price'].index, 
                             columns=data['highest_price'].columns)
        part2 = pd.DataFrame(np.zeros(data['highest_price'].shape), index=data['highest_price'].index, 
                             columns=data['highest_price'].columns)
        part1[~condition1] = np.maximum(abs(data['highest_price'].diff(1)[~condition1]), 
                                        abs(data['lowest_price'].diff(1)[~condition1]))
        part2[~condition2] = np.maximum(abs(data['highest_price'].diff(1)[~condition2]), 
                                        abs(data['lowest_price'].diff(1)[~condition2]))
        alpha = part1.rolling(window=12,min_periods=12).sum() / (part1.rolling(window=12,min_periods=12).sum() + part2.rolling(window=12,min_periods=12).sum())
        return alpha.iloc[-1]

    
   # SUM(HIGH+LOW<=DELAY(HIGH,1)+DELAY(LOW,1)?0:MAX(ABS(HIGH-DELAY(HIGH,1)),ABS(LOW-DELAY(LOW,1))),12)/
   # (SUM(HIGH+LOW<=DELAY(HIGH,1)+DELAY(LOW,1)?0:MAX(ABS(HIGH-DELAY(HIGH,1)),ABS(LOW-DELAY(LOW,1))),12)
   # +SUM(HIGH+LOW>=DELAY(HIGH,1)+DELAY(LOW,1)?0:MAX(ABS(HIGH-DELAY(HIGH,1)),ABS(LOW-DELAY(LOW,1))),12))
   # -SUM(HIGH+LOW>=DELAY(HIGH,1)+DELAY(LOW,1)?0:MAX(ABS(HIGH-DELAY(HIGH,1)),ABS(LOW-DELAY(LOW,1))),12)/
   # (SUM(HIGH+LOW>=DELAY(HIGH,1)+DELAY(LOW,1)?0: MAX(ABS(HIGH-DELAY(HIGH,1)),ABS(LOW-DELAY(LOW,1))),12)
   # +SUM(HIGH+LOW<=DELAY(HIGH,1)+DELAY(LOW,1)?0:MAX(ABS(HIGH-DELAY(HIGH,1)),ABS(LOW-DELAY(LOW,1))),12))     
    def alpha191_50(self, data, param1=1, param2=12, dependencies=['highest_price', 'lowest_price'], max_window=13):
        condition1 = (data['highest_price'] + data['lowest_price']) >= (data['highest_price'] + \
                                                                        data['lowest_price']).shift(param1)
        condition2 = (data['highest_price'] + data['lowest_price']) <= (data['highest_price'] + \
                                                                        data['lowest_price']).shift(param1)
        part1 = pd.DataFrame(np.zeros(data['highest_price'].shape), index=data['highest_price'].index, 
                             columns=data['highest_price'].columns)
        part2 = pd.DataFrame(np.zeros(data['highest_price'].shape), index=data['highest_price'].index, 
                             columns=data['highest_price'].columns)
        part3 = pd.DataFrame(np.zeros(data['highest_price'].shape), index=data['highest_price'].index, 
                             columns=data['highest_price'].columns)
        part4 = pd.DataFrame(np.zeros(data['highest_price'].shape), index=data['highest_price'].index, 
                             columns=data['highest_price'].columns)
        part1[~condition1] = np.maximum(abs(data['highest_price'].diff(1)[~condition1]), 
                                        abs(data['lowest_price'].diff(param1)[~condition1]))
        part2[~condition2] = np.maximum(abs(data['highest_price'].diff(1)[~condition2]), 
                                        abs(data['lowest_price'].diff(param1)[~condition2]))
        part3[condition1] = np.maximum(abs(data['highest_price'].diff(1)[condition1]), 
                                       abs(data['lowest_price'].diff(param1)[condition1]))
        part4[condition2] = np.maximum(abs(data['highest_price'].diff(1)[condition2]), 
                                       abs(data['lowest_price'].diff(param1)[condition2]))
        alpha = part3.rolling(window=param2,min_periods=param2).sum() / (part3.rolling(window=param2,min_periods=param2).sum() + part4.rolling(window=param2,min_periods=param2).sum()) - \
            part1.rolling(window=param2,min_periods=param2).sum() / (part1.rolling(window=param2,min_periods=param2).sum() + part2.rolling(window=param2,min_periods=param2).sum())
        return alpha.iloc[-1]

    
    # SUM(((HIGH+LOW)<=(DELAY(HIGH,1)+DELAY(LOW,1))?0:MAX(ABS(HIGH-DELAY(HIGH,1)),ABS(LOW-DELAY(LOW,1)))),12)/
    # (SUM(((HIGH+LOW)<=(DELAY(HIGH,1)+DELAY(LOW,1))?0:MAX(ABS(HIGH-DELAY(HIGH,1)),ABS(LOW-DELAY(LOW,1)))),12)
    # +SUM(((HIGH+LOW)>=(DELAY(HIGH,1)+DELAY(LOW,1))?0:MAX(ABS(HIGH-DELAY(HIGH,1)),ABS(LOW-DELAY(LOW,1)))),12))
    def alpha191_51(self, data, param1=1, param2=12, dependencies=['highest_price', 'lowest_price'], max_window=13):
        condition1 = (data['highest_price'] + data['lowest_price']) <= (data['highest_price'] + data['lowest_price']).shift(1)
        condition2 = (data['highest_price'] + data['lowest_price']) >= (data['highest_price'] + data['lowest_price']).shift(1)
        part1 = pd.DataFrame(np.zeros(data['highest_price'].shape), index=data['highest_price'].index, columns=data['highest_price'].columns)
        part2 = pd.DataFrame(np.zeros(data['highest_price'].shape), index=data['highest_price'].index, columns=data['highest_price'].columns)
        part1[~condition1] = np.maximum(abs(data['highest_price'].diff(param1)[~condition1]), 
                                        abs(data['lowest_price'].diff(param1)[~condition1]))
        part2[~condition2] = np.maximum(abs(data['highest_price'].diff(param1)[~condition2]), 
                                        abs(data['lowest_price'].diff(param1)[~condition2]))
        alpha = part1.rolling(window=param1,min_periods=param1).sum() / (part1.rolling(window=param1,min_periods=param1).sum() + part2.rolling(window=param1,min_periods=param1).sum())
        return alpha.iloc[-1] 

    
    # SUM(MAX(0,HIGH-DELAY((HIGH+LOW+CLOSE)/3,1)),26)/SUM(MAX(0,DELAY((HIGH+LOW+CLOSE)/3,1)-L),26)*100
    def alpha191_52(self, data, param1=3, param2=26, param3=1,
                 dependencies=['highest_price', 'lowest_price', 'close_price'], max_window=27):
        ma = (data['highest_price'] + data['lowest_price'] + data['close_price']) / float(param1)
        part1 = (np.maximum(0.0, (data['highest_price'] - ma.shift(param3)))).rolling(window=param2, min_periods=param2).sum()
        part2 = (np.maximum(0.0, (ma.shift(param3) - data['lowest_price']))).rolling(window=param2, min_periods=param2).sum()
        return (part1 / part2 * 100.0).iloc[-1]
    
    # COUNT(CLOSE>DELAY(CLOSE,1),12)/12*100
    def alpha191_53(self, data, param1=1, param2=12, param3=12,
                 dependencies=['close_price'], max_window=13):
        return ((data['close_price'].diff(param1) > 0.0).rolling(window=param2, min_periods=param2
                                                         ).sum() / float(param3) * 100).iloc[-1]
    
    # (-1*RANK(STD(ABS(CLOSE-OPEN))+CLOSE-OPEN+CORR(CLOSE,OPEN,10)))
    # 注，这里STD没有指明周期
    def alpha191_54(self, data, param1=10, param2=-1, dependencies=['close_price', 'open_price'], max_window=10):
        corr_win = param1
        corr_df = rolling_corr(data['close_price'], data['open_price'], corr_win)
        part1 = abs(data['close_price']-data['open_price']).rolling(window=corr_win, min_periods=corr_win).std() + \
        data['close_price'] - data['open_price'] + corr_df
        return part1.rank(axis=1, pct=True).iloc[-1] * (param2)
    
    # SUM(16*(CLOSE+(CLOSE-OPEN)/2-DELAY(OPEN,1))/
    # ((ABS(HIGH-DELAY(CLOSE,1))>ABS(LOW-DELAY(CLOSE,1)) & ABS(HIGH-DELAY(CLOSE,1))>ABS(HIGH-DELAY(LOW,1)) ?
    # ABS(HIGH-DELAY(CLOSE,1))+ABS(LOW-DELAY(CLOSE,1))/2+ABS(DELAY(CLOSE,1)-DELAY(OPEN,1))/4:
    # (ABS(LOW-DELAY(CLOSE,1))>ABS(HIGH-DELAY(LOW,1)) & ABS(LOW-DELAY(CLOSE,1))>ABS(HIGH-DELAY(CLOSE,1)) ?
    # ABS(LOW-DELAY(CLOSE,1))+ABS(HIGH-DELAY(CLOSE,1))/2+ABS(DELAY(CLOSE,1)-DELAY(OPEN,1))/4:
    # ABS(HIGH-DELAY(LOW,1))+ABS(DELAY(CLOSE,1)-DELAY(OPEN,1))/4)))
    # *MAX(ABS(HIGH-DELAY(CLOSE,1)),ABS(LOW-DELAY(CLOSE,1))),20
    def alpha191_55(self, data, param1=1.5, param2=0.5, param3=2.0, param4=4.0, param5=16.0, param6=20,
                 dependencies=['open_price', 'lowest_price', 'close_price', 'highest_price'], max_window=21):
        
        part1 = data['close_price'] * param1 - data['open_price'] * param2 - data['open_price'].shift(1)
        part2 = abs(data['highest_price']-data['close_price'].shift(1)) + \
            abs(data['lowest_price']-data['close_price'].shift(1)) / param3 + \
            abs(data['close_price']-data['open_price']).shift(1) / param4
        
        condition1 = np.logical_and(abs(data['highest_price']-data['close_price'].shift(1)
                                       ) > abs(data['lowest_price']-data['close_price'].shift(1)),
                                    abs(data['highest_price']-data['close_price'].shift(1)
                                       ) > abs(data['highest_price']-data['lowest_price'].shift(1)))
        
        condition2 = np.logical_and(abs(data['lowest_price']-data['close_price'].shift(1)
                                       ) > abs(data['highest_price']-data['lowest_price'].shift(1)),
                               abs(data['lowest_price']-data['close_price'].shift(1)
                                  ) > abs(data['highest_price']-data['close_price'].shift(1)))
        
        part2[~condition1 & condition2] = abs(data['lowest_price']-data['close_price'].shift(1)) + \
            abs(data['highest_price']-data['close_price'].shift(1)) / param3 + abs(data['close_price']-\
                                                                      data['open_price']).shift(1) / param4
        
        part2[~condition1 & ~condition2] = abs(data['highest_price']-data['lowest_price'].shift(1)) + \
            abs(data['close_price']-data['open_price']).shift(1) / param3
        
        part3 = np.maximum(abs(data['highest_price']-data['close_price'].shift(1)), 
                           abs(data['lowest_price']-data['close_price'].shift(1)))
        
        alpha = (part1 / part2 * part3 * float(param5)).rolling(window=param6, min_periods=param6).sum().iloc[-1]
        return alpha
    
    
    # RANK(OPEN-TSMIN(OPEN,12))<RANK(RANK(CORR(SUM((HIGH +LOW)/2,19),SUM(MEAN(VOLUME,40),19),13))^5)
    def alpha191_56(self, data, param1=12, param2=0.5, param3=19, param4=40, param5=13, param6=5,
                 dependencies=['open_price', 'highest_price', 'lowest_price', 'turnover_vol'], max_window=73):
        # 这里就会有随机性,0/1
        part1 = (data['open_price'] - data['open_price'].rolling(window=param1, min_periods=param1).min()).rank(axis=1, pct=True)
        t1 = (data['highest_price']*param2+data['lowest_price']*param2).rolling(window=param3, min_periods=param3).sum()
        t2 = data['turnover_vol'].rolling(window=param4,
                                          min_periods=param4).mean().rolling(window=param3, 
                                                                             min_periods=param3).sum()
        corr_win = param5
        corr_df = rolling_corr(t1, t2, corr_win)
        part2 = ((corr_df.rank(axis=1, pct=True)) ** param6).rank(axis=1, pct=True)
        return (part2-part1).iloc[-1]

    # SMA((CLOSE-TSMIN(LOW,9))/(TSMAX(HIGH,9)-TSMIN(LOW,9))*100,3,1)
    def alpha191_57(self, data, param1=9, param2=3, param3=1, 
                 dependencies=['close_price', 'lowest_price', 'highest_price'], max_window=500):
        # KDJ_K
        low_tsmin = data['lowest_price'].rolling(window=param1, min_periods=param1).min()
        high_tsmax = data['highest_price'].rolling(window=param1, min_periods=param1).max()

        var = (data['close_price'] - low_tsmin) / (high_tsmax - low_tsmin) * 100
        var_sma = var.ewm(adjust=False, alpha=float(param3)/param2, min_periods=0, ignore_na=False).mean()
        return var_sma.iloc[-1]

    # COUNT(CLOSE>DELAY(CLOSE,1),20)/20*100
    def alpha191_58(self, data, param1=1, param2=20, dependencies=['close_price'], max_window=20):
        return ((data['close_price'].diff(param1) > 0.0).rolling(window=param2, min_periods=param2
                                                          ).sum() / float(param2) * 100).iloc[-1]


    
    # SUM((CLOSE=DELAY(CLOSE,1)?0:CLOSE-(CLOSE>DELAY(CLOSE,1)?MIN(LOW,DELAY(CLOSE,1)):MAX(HIGH,DELAY(CLOSE,1)))),20)
    def alpha191_59(self, data, param1=1, param2=20, dependencies=['close_price', 'lowest_price', 'highest_price'], 
                 max_window=21):
        # 受价格尺度影响
        alpha = pd.DataFrame(np.zeros(data['close_price'].shape), 
                             index=data['close_price'].index, columns=data['close_price'].columns)
        condition1 = data['close_price'].diff(param1) > 0.0
        condition2 = data['close_price'].diff(param1) < 0.0
        alpha[condition1] = data['close_price'][condition1] - np.minimum(data['lowest_price'][condition1],
                                                                         data['close_price'].shift(param1)[condition1])
        alpha[condition2] = data['close_price'][condition2] - np.maximum(data['highest_price'][condition2], 
                                                                         data['close_price'].shift(param1)[condition2])
        alpha = alpha.rolling(window=param2, min_periods=param2).sum().iloc[-1]
        return alpha

    # SUM((2*CLOSE-LOW-HIGH)./(HIGH-LOW).*VOLUME,20)
    def alpha191_60(self, data, param1=20, param2=2, 
                 dependencies=['close_price', 'open_price', 'lowest_price', 'highest_price', 'turnover_vol'], max_window=21):   
        part1 = (param2*data['close_price']-data['lowest_price']-data['highest_price']) / \
        (data['highest_price']-data['lowest_price']) * data['turnover_vol']
        return part1.rolling(window=param1, min_periods=param1).sum().iloc[-1]
    
    
    #MAX(RANK(DECAYLINEAR(DELTA(VWAP,1),12)),RANK(DECAYLINEAR(RANK(CORR(LOW,MEAN(VOLUME,80),8)),17)))*-1
    def alpha191_61(self, data, param1=1, param2=12, param3=80, param4=8, param5=17, 
                 dependencies=['lowest_price', 'vwap', 'turnover_vol'], max_window=106):
        w12 = preprocessing.normalize(np.matrix([i for i in range(1, param2+1)]),norm='l1',axis=1).reshape(-1)
        w17 = preprocessing.normalize(np.matrix([i for i in range(1, param5+1)]),norm='l1',axis=1).reshape(-1)
        turnover_ma = data['turnover_vol'].rolling(window=param3, min_periods=param3).mean()
        part1 = rolling_dot(data['vwap'].diff(periods=1), w12, param2).rank(axis=1, pct=True)
        corr_df = rolling_corr(turnover_ma, data['lowest_price'], param4)
        part2 = rolling_dot(corr_df.rank(axis=1,pct=True), w17, param5).rank(axis=1, pct=True)
        return np.maximum(part1.iloc[-1], part2.iloc[-1]) * (-1)

    # -1*CORR(HIGH,RANK(VOLUME),5)
    def alpha191_62(self, data, param1=5, dependencies=['turnover_vol', 'highest_price'], max_window=5):
        corr_win = param1
        corr_df = rolling_corr(data['turnover_vol'].rank(axis=1, pct=True), data['highest_price'], corr_win)
        return corr_df.iloc[-1] * (-1)

    # SMA(MAX(CLOSE-DELAY(CLOSE,1),0),6,1)/SMA(ABS(CLOSE-DELAY(CLOSE,1)),6,1)*100
    def alpha191_63(self, data, param1=1, param2=6, param3=1, dependencies=['close_price'], max_window=7):
        part1 = (np.maximum(data['close_price'].diff(param1), 0.0)).ewm(adjust=False, alpha=float(1) / param2, min_periods=0,
                                                              ignore_na=False).mean()
        part2 = abs(data['close_price']).diff(param1).ewm(adjust=False, alpha=float(param3
                                                                             ) / param2, min_periods=0, ignore_na=False).mean()
        return (part1 / part2 * 100.0).iloc[-1]

    # (MAX(RANK(DECAYLINEAR(CORR(RANK(VWAP),RANK(VOLUME),4),4)),RANK(DECAYLINEAR(MAX(CORR(RANK(CLOSE),RANK(MEAN(VOLUME,60)),4),13),14)))*-1)
    def alpha191_64(self, data, param1=4, param2=60, param3=13, param4=13, param5=14, param6=-1,
                 dependencies=['close_price', 'vwap', 'turnover_vol'], max_window=93):
    
        # 看上去是TSMAX
        w4 = preprocessing.normalize(np.matrix([i for i in range(1, param1+1)]), norm='l1', axis=1).reshape(-1)
        w14 = preprocessing.normalize(np.matrix([i for i in range(1, param5+1)]), norm='l1', axis=1).reshape(-1)
        corr_df = rolling_corr(data['vwap'].rank(axis=1, pct=True), data['turnover_vol'].rank(axis=1, pct=True), param1)
        part1 = rolling_dot(corr_df, w4, param1).rank(axis=1, pct=True)
        vol = data['turnover_vol'].rolling(window=param2, min_periods=param2).mean().rank(axis=1, pct=True)
        close = data['close_price'].rank(axis=1, pct=True)
        corr_df = rolling_corr(vol, close, 4)
        part2 = rolling_dot(corr_df.rolling(window=param4, min_periods=param4).max(), w14, param5).rank(axis=1, pct=True)
        alpha = np.maximum(part1.iloc[-1], part2.iloc[-1]) * (param6)
        return alpha

    # MEAN(CLOSE,6)/CLOSE
    def alpha191_65(self, data, param1=6, dependencies=['close_price'], max_window=6):
        
        return (data['close_price'].rolling(window=param1, min_periods=param1).mean() / data['close_price']).iloc[-1]

    # (CLOSE-MEAN(CLOSE,6))/MEAN(CLOSE,6)*100
    def alpha191_66(self, data, param1=6, dependencies=['close_price'], max_window=6):
        
        # BIAS6，用BIAS5简单替换下
        ma = data['close_price'].rolling(window=param1, min_periods=param1).mean()
        alpha = (data['close_price'] - ma) / data['close_price'] * 100
        return alpha.iloc[-1]

    # SMA(MAX(CLOSE-DELAY(CLOSE,1),0),24,1)/SMA(ABS(CLOSE-DELAY(CLOSE,1)),24,1)*100
    def alpha191_67(self, data, param1=1, param2=24, dependencies=['close_price'], max_window=25):
        
        # RSI24
        part1 = (np.maximum(data['close_price'].diff(param1), 0.0)).ewm(adjust=False, alpha=float(1) / param2, min_periods=0,
                                                              ignore_na=False).mean()
        part2 = (abs(data['close_price'].diff(param1))).ewm(adjust=False, alpha=float(param1) / param2, min_periods=0,
                                                  ignore_na=False).mean()
        return (part1 / part2 * 100).iloc[-1]

    
    # SMA(((HIGH+LOW)/2-(DELAY(HIGH,1)+DELAY(LOW,1))/2)*(HIGH-LOW)/VOLUME,15,2)
    def alpha191_68(self, data, param1=1, param2=0.5, param3=15, param4=2, 
                 dependencies=['highest_price', 'lowest_price', 'turnover_vol'], max_window=16):
        part1 = (data['highest_price'].diff(param1) * param2 + data['lowest_price'].diff(param1) * param2) * (
                data['highest_price'] - data['lowest_price']) / data['turnover_vol']
        return part1.ewm(adjust=False, alpha=float(param4) / param3, min_periods=0, ignore_na=False).mean().iloc[-1]

        # (SUM(DTM,20)>SUM(DBM,20)?(SUM(DTM,20)-SUM(DBM,20))/SUM(DTM,20):(SUM(DTM,20)=SUM(DBM,20)？0:(SUM(DTM,20)-SUM(DBM,20))/SUM(DBM,20)))
    # DTM: (OPEN<=DELAY(OPEN,1)?0:MAX((HIGH-OPEN),(OPEN-DELAY(OPEN,1))))
    # DBM: (OPEN>=DELAY(OPEN,1)?0:MAX((OPEN-LOW),(OPEN-DELAY(OPEN,1))))
    def alpha191_69(self, data, param1=1, param2=20, 
                 dependencies=['open_price', 'highest_price', 'lowest_price'], max_window=21):
        condition1 = data['open_price'].diff(param1) <= 0.0
        condition2 = data['open_price'].diff(param1) >= 0.0
        dtm = pd.DataFrame(np.zeros(data['open_price'].shape), index=data['open_price'].index,
                       columns=data['open_price'].columns)
        dbm = pd.DataFrame(np.zeros(data['open_price'].shape), index=data['open_price'].index,
                       columns=data['open_price'].columns)
        dtm[~condition1] = np.maximum(data['highest_price'] - data['open_price'], data['open_price'].diff(param1))[~condition1]
        dbm[~condition2] = np.maximum(data['open_price'] - data['lowest_price'], data['open_price'].diff(param1))[~condition2]
        dtm_sum = dtm.rolling(window=param2, min_periods=param2).sum()
        dbm_sum = dbm.rolling(window=param2, min_periods=param2).sum()
        alpha = (dtm_sum - dbm_sum) / dtm_sum
        alpha[dtm_sum < dbm_sum] = ((dtm_sum - dbm_sum) / dbm_sum)[dtm_sum < dbm_sum]
        return alpha.iloc[-1]

    # STD(AMOUNT,6)
    def alpha191_70(self, data, param1=6, dependencies=['turnover_value'], max_window=6):
        return data['turnover_value'].rolling(window=param1, min_periods=param1).std().iloc[-1]
    
    
    
    def alpha191_71(self, data, param1=24, dependencies=['close_price'], max_window=25):
        # (CLOSE-MEAN(CLOSE,24))/MEAN(CLOSE,24)*100
        # BIAS24
        close_ma = data['close_price'].rolling(window=param1, min_periods=param1).mean()
        return ((data['close_price'] - close_ma) / close_ma * 100).iloc[-1]


    def alpha191_72(self, data, param1=6, param2=15, param3=1, 
                 dependencies=['highest_price', 'lowest_price', 'close_price'], max_window=22):
        # SMA((TSMAX(HIGH,6)-CLOSE)/(TSMAX(HIGH,6)-TSMIN(LOW,6))*100,15,1)
        part1 = (data['highest_price'].rolling(window=param1, min_periods=param1).max() - data['close_price']) / \
                (data['highest_price'].rolling(window=param1, min_periods=param1).max() - data['lowest_price'].rolling(
            window=param1,min_periods=param1).min()) * 100.0
        return part1.ewm(adjust=False, alpha=float(param3) / param2, min_periods=0, ignore_na=False).mean().iloc[-1]


    def alpha191_73(self, data, param1=16, param2=10, param3=4, param4=5, param5=30, param6=4, param7=3, param8=-1,
                 dependencies=['vwap', 'turnover_vol', 'close_price'], max_window=38):
    # ((TSRANK(DECAYLINEAR(DECAYLINEAR(CORR(CLOSE,VOLUME,10),16),4),5)-RANK(DECAYLINEAR(CORR(VWAP,MEAN(VOLUME,30),4),3)))*-1)
        vwap = data['vwap']
        w16 = preprocessing.normalize(np.matrix([i for i in range(1, param1+1)]), norm='l1', axis=1).reshape(-1)
        w4 = preprocessing.normalize(np.matrix([i for i in range(1, param3+1)]), norm='l1', axis=1).reshape(-1)
        w3 = preprocessing.normalize(np.matrix([i for i in range(1, param7+1)]), norm='l1', axis=1).reshape(-1)
        corr_win = param2
        corr_df = rolling_corr(data['close_price'], data['turnover_vol'], corr_win)
        part1 = rolling_rank(rolling_dot(rolling_dot(corr_df, w16, param1), w4, param3), param4)
        corr_df = rolling_corr(data['turnover_vol'].rolling(window=param5, min_periods=param5).mean(), vwap, param6)
        part2 = rolling_dot(corr_df, w3, param7).rank(axis=1, pct=True)
        return (part1.iloc[-1] - part2.iloc[-1]) * (param8)


    def alpha191_74(self, data, param1=0.35, param2=0.65, param3=20, param4=40, param5=7, param6=6,
                 dependencies=['lowest_price', 'vwap', 'turnover_vol'], max_window=68):
        # RANK(CORR(SUM(LOW*0.35+VWAP*0.65,20),SUM(MEAN(VOLUME,40),20),7))+RANK(CORR(RANK(VWAP),RANK(VOLUME),6))
        vwap = data['vwap']
        price = ((data['lowest_price'] * param1 + vwap * param2).rolling(window=param3, min_periods=param3).sum())
        vol = data['turnover_vol'].rolling(window=param4, min_periods=param4).mean().rolling(window=param3,
                                                                                             min_periods=param3).sum()
        corr_df = rolling_corr(price, vol, param5)
        part1 = corr_df.rank(axis=1, pct=True)
        corr_df = rolling_corr(vwap.rank(axis=1, pct=True), data['turnover_vol'].rank(axis=1, pct=True), param6)
        part2 = corr_df.rank(axis=1, pct=True)
        return part1.iloc[-1] + part2.iloc[-1]


    def alpha191_75(self, data, param1=50, dependencies=['close_price', 'open_price'], max_window=51):
        # COUNT(CLOSE>OPEN & BANCHMARK_INDEX_CLOSE<BANCHMARK_INDEX_OPEN,50)/COUNT(BANCHMARK_INDEX_CLOSE<BANCHMARK_INDEX_OPEN,50)
        # 简化为等权benchmark
        bm = (data['close_price'].mean(axis=1) < data['open_price'].mean(axis=1))
        bm_den = pd.DataFrame(data=np.repeat(bm.values.reshape(len(bm.values), 1), len(data['close_price'].columns), axis=1),
                          index=data['close_price'].index, columns=data['close_price'].columns)
        alpha = np.logical_and(data['close_price'] > data['open_price'], bm_den).rolling(
            window=param1, min_periods=param1).sum() / \
            bm_den.rolling(window=param1, min_periods=param1).sum()
        return alpha.iloc[-1]

    # STD(ABS(CLOSE/DELAY(CLOSE,1)-1)/VOLUME,20)/MEAN(ABS(CLOSE/DELAY(CLOSE,1)-1)/VOLUME,20)
    def alpha191_76(self, data, param1=1, param2=20, dependencies=['close_price', 'turnover_vol'], max_window=21):
        ret_vol = abs(data['close_price'].pct_change(periods=param1)) / data['turnover_vol']
        return (ret_vol.rolling(window=param2, 
                                min_periods=param2).std() / ret_vol.rolling(window=param2,
                                min_periods=param2).mean()).iloc[-1]

    
    # MIN(RANK(DECAYLINEAR(HIGH*0.5+LOW*0.5-VWAP,20)),RANK(DECAYLINEAR(CORR(HIGH*0.5+LOW*0.5,MEAN(VOLUME,40),3),6)))
    def alpha191_77(self, data, param1=0.5, param2=20, param3=40, param4=3, param5=6,
                 dependencies=['lowest_price', 'highest_price', 'vwap', 'turnover_vol'], max_window=50):
        w6 = preprocessing.normalize(np.matrix([i for i in range(1, param5+1)]), norm='l1', axis=1).reshape(-1)
        w20 = preprocessing.normalize(np.matrix([i for i in range(1, param2+1)]), norm='l1', axis=1).reshape(-1)

        part1 = rolling_dot(data['highest_price']*param1 + data['lowest_price']*param1 - data['vwap'], w20, param2
                           ).rank(axis=1, pct=True)
        corr_df = rolling_corr(data['highest_price']*param1 + data['lowest_price']*param1,
                           data['turnover_vol'].rolling(window=param3, min_periods=param3).mean(), param4)
        part2 = rolling_dot(corr_df, w6, param5).rank(axis=1, pct=True)
        return np.minimum(part1.iloc[-1], part2.iloc[-1])

        # ((HIGH+LOW+CLOSE)/3-MA((HIGH+LOW+CLOSE)/3,12))/(0.015*MEAN(ABS(CLOSE-MEAN((HIGH+LOW+CLOSE)/3,12)),12))
        # 类似于是CCI12
        # 通达信的CCI定义为(代码为下面注释掉的部分)：
        # TYP:=(HIGH+LOW+CLOSE)/3;
        # CCI:(TYP-MA(TYP,N))/(0.015*AVEDEV(TYP,N));
        # 其中 AVEDEV(TYP,N) = MA(ABS(TYP-MA(TYP,N)))
    def alpha191_78(self, data, param1=12, param2=3, param3= 0.015, 
                 dependencies=['highest_price', 'lowest_price', 'close_price'], max_window=25):

        win = param1
        typ = (data['highest_price'] + data['lowest_price'] + data['close_price']) / float(param2)
        typ_ma = typ.rolling(window=win, min_periods=1).mean()
        alpha = (typ - typ_ma) / param3 / (data['close_price'] - typ_ma).abs().rolling(window=win, min_periods=1).mean()
        # var = typ - typ_ma
        # alpha = var / 0.015 / var.abs().rolling(window=win, min_periods=1).mean()
        return alpha.iloc[-1]

    # SMA(MAX(CLOSE-DELAY(CLOSE,1),0),12,1)/SMA(ABS(CLOSE-DELAY(CLOSE,1)),12,1)*100
    # 就是RSI12
    def alpha191_79(self, data, param1=12, param2=1, dependencies=['close_price'], max_window=13):

        part1 = (np.maximum(data['close_price'].diff(param1), 0.0)).ewm(adjust=False, alpha=float(1) / param1, 
                                                                        min_periods=0,ignore_na=False).mean()
        part2 = (abs(data['close_price'].diff(param1))).ewm(adjust=False, alpha=float(1) / param1, min_periods=0,
                                                  ignore_na=False).mean()
        return (part1 / part2 * 100).iloc[-1]


    def alpha191_80(self, data, param1=5, dependencies=['turnover_vol'], max_window=6):
        # (VOLUME-DELAY(VOLUME,5))/DELAY(VOLUME,5)*100
        return (data['turnover_vol'].pct_change(periods=param1) * 100.0).iloc[-1]
    
    
    
    def alpha191_81(self, data, param1=21, param2=2, dependencies=['turnover_vol'], max_window=21):
        # SMA(VOLUME,21,2)
        return data['turnover_vol'].ewm(adjust=False, alpha=float(param2)/param1, 
                                        min_periods=0, ignore_na=False).mean().iloc[-1]


    def alpha191_82(self, data, param1=6, param2=20, param3=1, 
                 dependencies=['lowest_price', 'highest_price', 'close_price'], max_window=26):
        # SMA((TSMAX(HIGH,6)-CLOSE)/(TSMAX(HIGH,6)-TSMIN(LOW,6))*100,20,1)
        # RSV技术指标变种
        part1 = (data['highest_price'].rolling(window=param1,min_periods=param1).max()-data['close_price']) / \
            (data['highest_price'].rolling(window=param1,min_periods=param1).max()-\
             data['lowest_price'].rolling(window=param1,min_periods=param1).min()) * 100
        alpha = part1.ewm(adjust=False, alpha=float(param3)/param2, min_periods=0, ignore_na=False).mean().iloc[-1]
        return alpha


    def alpha191_83(self, data, param1=5, dependencies=['highest_price', 'turnover_vol'], max_window=5):
        # (-1*RANK(COVIANCE(RANK(HIGH),RANK(VOLUME),5)))
        high = data['highest_price'].rank(axis=1, pct=True)
        vol = data['turnover_vol'].rank(axis=1, pct=True)
        corr_df = rolling_cov(high, vol, param1)
        return corr_df.rank(axis=1, pct=True).iloc[-1] * (-1)


    def alpha191_84(self, data, param1=1, param2=20, dependencies=['close_price', 'turnover_vol'], max_window=21):
        # SUM((CLOSE>DELAY(CLOSE,1)?VOLUME:(CLOSE<DELAY(CLOSE,1)?-VOLUME:0)),20)
        part1 = np.sign(data['close_price'].diff(param1)) * data['turnover_vol']
        return part1.rolling(window=param2, min_periods=param2).sum().iloc[-1]


    def alpha191_85(self, data, param1=20, param2=7, param3=8, dependencies=['close_price', 'turnover_vol'], max_window=40):
        # TSRANK(VOLUME/MEAN(VOLUME,20),20)*TSRANK(-1*DELTA(CLOSE,7),8)
        part1 = (data['turnover_vol'] / data['turnover_vol'].rolling(window=param1,min_periods=param1
                                                        ).mean()).iloc[-param1:].rank(axis=0, pct=True)
        part2 = (data['close_price'].diff(param2) * (-1)).iloc[-param3:].rank(axis=0, pct=True)
        return (part1 * part2).iloc[-1]


    def alpha191_86(self, data, param1=20, param2=0.1, param3=10, param4=0.2, param5=0.25,
                 param6=0.0,param7=-1, dependencies=['close_price'], max_window=21):
    # ((0.25<((DELAY(CLOSE,20)-DELAY(CLOSE,10))/10-(DELAY(CLOSE,10)-CLOSE)/10))?-1:((((DELAY(CLOSE,20)-DELAY(CLOSE,10))/10-(DELAY(CLOSE,10)-CLOSE)/10)<0)?1:(DELAY(CLOSE,1)-CLOSE)))
        condition1 = (data['close_price'].shift(param1) * param2 + \
                      data['close_price'] * param2 - data['close_price'].shift(param3) * param4) > param5
        condition2 = (data['close_price'].shift(param1) * param2 + \
                      data['close_price'] * param2 - data['close_price'].shift(param3) * param4) < param6
        alpha = pd.DataFrame((-1)*np.ones(data['close_price'].shape), index=data['close_price'].index,
                             columns=data['close_price'].columns)
        alpha[~condition1 & condition2] = 1.0
        alpha[~condition1 & ~condition2] = data['close_price'].diff(1)[~condition1 & ~condition2] * (param7)
        return alpha.iloc[-1]


    def alpha191_87(self, data, param1=4, param2=7, param3=0.5, param4=11,param5=-1,
                 dependencies=['vwap', 'turnover_vol', 'lowest_price', 'highest_price', 'open_price'], max_window=18):
        # (RANK(DECAYLINEAR(DELTA(VWAP,4),7))+TSRANK(DECAYLINEAR((LOW-VWAP)/(OPEN-(HIGH+LOW)/2),11),7))*-1
        w7 = preprocessing.normalize(np.matrix([i for i in range(1, param2+1)]),norm='l1',axis=1).reshape(-1)
        w11 = preprocessing.normalize(np.matrix([i for i in range(1, param4+1)]),norm='l1',axis=1).reshape(-1)
        part1 = rolling_dot(data['vwap'].diff(param1), w7, param2).rank(axis=1, pct=True)
        part2 = (data['lowest_price']-data['vwap'])/(data['open_price']\
                                                     -data['highest_price']*param3-data['lowest_price']*param3)
        part2 = rolling_dot(part2, w11, param4).iloc[-param2:].rank(axis=0, pct=True)
        return (part1 + part2).iloc[-1] * (param5)


    def alpha191_88(self, data, param1=20, dependencies=['close_price'], max_window=21):
        # (CLOSE-DELAY(CLOSE,20))/DELAY(CLOSE,20)*100
        # 就是REVS20
        alpha = (data['close_price'] / data['close_price'].shift(param1) - 1.0) * 100
        return alpha.iloc[-1]


    def alpha191_89(self, data, param1=13, param2=27, param3=10, param4=2, param5=2,
                 dependencies=['close_price'], max_window=37):
        # 2*(SMA(CLOSE,13,2)-SMA(CLOSE,27,2)-SMA(SMA(CLOSE,13,2)-SMA(CLOSE,27,2),10,2))
        part1 = data['close_price'].ewm(adjust=False, alpha=float(param4)/param1, min_periods=0, ignore_na=False).mean() - \
             data['close_price'].ewm(adjust=False, alpha=float(param4)/param2, min_periods=0, ignore_na=False).mean()
        alpha = (part1 - part1.ewm(adjust=False, alpha=float(param4)/param3, min_periods=0, 
                                   ignore_na=False).mean()) * float(param5)
        return alpha.iloc[-1]


    def alpha191_90(self, data, param1=5, param2=-1, dependencies=['vwap', 'turnover_vol'], max_window=5):
        # (RANK(CORR(RANK(VWAP),RANK(VOLUME),5))*-1)
        vwap = data['vwap'].rank(axis=1, pct=True)
        vol = data['turnover_vol'].rank(axis=1, pct=True)
        corr_df = rolling_corr(vwap, vol, param1)
        alpha = corr_df.rank(axis=1, pct=True).iloc[-1] * (param2)
        return alpha


    def alpha191_91(self, data, param1=5, param2=40, param3=-1, 
                 dependencies=['close_price', 'turnover_vol', 'lowest_price'], max_window=45):
        # ((RANK(CLOSE-MAX(CLOSE,5))*RANK(CORR(MEAN(VOLUME,40),LOW,5)))*-1)
        # 感觉是TSMAX
        part1 = (data['close_price'] - data['close_price'].rolling(
            window=param1, min_periods=param1).max()).rank(axis=1, pct=True)
        vol = data['turnover_vol'].rolling(window=param2, min_periods=param2).mean()
        low = data['lowest_price']
        part2 = rolling_corr(vol, low, param1).rank(axis=1, pct=True)
        return (part1.iloc[-1] * part2.iloc[-1]) * (param3)


    def alpha191_92(self, data, param1=3, param2=5, param3=180, param4=0.35, param5=0.65, param6=2, param7=3,
                 param8=13, param9=5, param10=15,param11=-1,
                 dependencies=['close_price', 'vwap', 'turnover_vol'], max_window=209):
        # (MAX(RANK(DECAYLINEAR(DELTA(CLOSE*0.35+VWAP*0.65,2),3)),TSRANK(DECAYLINEAR(ABS(CORR((MEAN(VOLUME,180)),CLOSE,13)),5),15))*-1)
        w3 = preprocessing.normalize(np.matrix([i for i in range(1, param1+1)]),norm='l1',axis=1).reshape(-1)
        w5 = preprocessing.normalize(np.matrix([i for i in range(1, param2+1)]),norm='l1',axis=1).reshape(-1)
        vol = data['turnover_vol'].rolling(window=param3, min_periods=param3).mean()
        part1 = rolling_dot((data['close_price'] * param4 + data['vwap'] * param4).diff(param6), w3, param7
                           ).rank(axis=1, pct=True)
        vol = data['turnover_vol'].rolling(window=param3, min_periods=param3).mean()
        part2 = rolling_dot(abs(rolling_corr(vol, data['close_price'], param8)), w5, param9
                           ).iloc[-param10:].rank(axis=0, pct=True)
        return np.maximum(part1.iloc[-1], part2.iloc[-1]) * (param11)


    def alpha191_93(self, data, param1=1, param2=20, param3=0, 
                 dependencies=['open_price', 'lowest_price'], max_window=21):
        # SUM(OPEN>=DELAY(OPEN,1)?0:MAX(OPEN-LOW,OPEN-DELAY(OPEN,1)),20)
        condition = data['open_price'].diff(param1) >= param3
        alpha= pd.DataFrame(np.zeros(data['open_price'].shape), index=data['open_price'].index,
                            columns=data['open_price'].columns)
        alpha[~condition] = np.maximum(data['open_price'] - data['lowest_price'], data['open_price'].diff(param1))[~condition]
        return alpha.rolling(window=param2, min_periods=param2).sum().iloc[-1]


    def alpha191_94(self, data, param1=1, param2=30, dependencies=['close_price', 'turnover_vol'], max_window=31):
        # SUM((CLOSE>DELAY(CLOSE,1)?VOLUME:(CLOSE<DELAY(CLOSE,1)?-VOLUME:0)),30)
        part1 = np.sign(data['close_price'].diff(param1)) * data['turnover_vol']
        return part1.rolling(window=param2, min_periods=param2).sum().iloc[-1]


    def alpha191_95(self, data, param1=20, dependencies=['turnover_value'], max_window=20):
        # STD(AMOUNT,20), 这里应该没有复权
        return data['turnover_value'].rolling(window=param1, min_periods=20).std().iloc[-1]
    
    def alpha191_96(self, data, param1=9, param2=1, param3=3, 
                 dependencies=['close_price', 'lowest_price', 'highest_price'], max_window=100):
        # SMA(SMA((CLOSE-TSMIN(LOW,9))/(TSMAX(HIGH,9)-TSMIN(LOW,9))*100,3,1),3,1)
        # 就是KDJ_D
        low_tsmin = data['lowest_price'].rolling(window=param1, min_periods=param1).min()
        high_tsmax = data['highest_price'].rolling(window=param1, min_periods=param1).max()

        var = (data['close_price'] - low_tsmin) / (high_tsmax - low_tsmin) * 100
        var_sma = var.ewm(adjust=False, alpha=float(param2)/param3, min_periods=0, ignore_na=False).mean()
        alpha = var_sma.ewm(adjust=False, alpha=float(param2)/param3, min_periods=0, ignore_na=False).mean()
        return alpha.iloc[-1]
    
    def alpha191_97(self, data, param1=10, param2=5, dependencies=['turnover_vol'], max_window=10):
        # STD(VOLUME,10)
        # 就是VSTD10
        alpha = data['turnover_vol'].rolling(window=param1, min_periods=param2).std()
        return alpha.iloc[-1]
    
    
    def alpha191_98(self, data, param1=100, param2=0.05, param3=3, param4=-1,
                 dependencies=['close_price'], max_window=201):
        # (DELTA(SUM(CLOSE,100)/100,100)/DELAY(CLOSE,100)<=0.05)?(-1*(CLOSE-TSMIN(CLOSE,100))):(-1*DELTA(CLOSE,3))
        condition1 = (data['close_price'].rolling(window=param1, min_periods=param1).sum() / param1).diff(periods=param1) / data['close_price'].shift(param1) <= param2
        alpha = (data['close_price'] - data['close_price'].rolling(window=param1, min_periods=param1).min()) * (param4)
        alpha[~condition1] = data['close_price'].diff(param3)[~condition1] * (param4)
        return alpha.iloc[-1]


    def alpha191_99(self, data, param1=5, dependencies=['close_price', 'turnover_vol'], max_window=5):
        # (-1*RANK(COVIANCE(RANK(CLOSE),RANK(VOLUME),5)))
        close = data['close_price'].rank(axis=1, pct=True)
        vol = data['turnover_vol'].rank(axis=1, pct=True)
        corr_df = rolling_cov(close, vol, param1)
        return corr_df.rank(axis=1, pct=True).iloc[-1] * (-1)


    def alpha191_100(self, data, param1=20, param2=10, dependencies=['turnover_vol'], max_window=20):
        # STD(VOLUME,20), 就是VSTD20
        alpha = data['turnover_vol'].rolling(window=param1, min_periods=param2).std()
        return alpha.iloc[-1]


    def alpha191_101(self, data, param1=30, param2=37, param3=15, param4=0.1, param5=0.9, param6=11, param7=-1,
                  dependencies=['vwap', 'turnover_vol', 'highest_price', 'close_price'], max_window=82):
        # (RANK(CORR(CLOSE,SUM(MEAN(VOLUME,30),37),15)) < RANK(CORR(RANK(HIGH*0.1+VWAP*0.9),RANK(VOLUME),11)))*-1
        part1 = (data['turnover_vol'].rolling(window=param1, min_periods=param1).mean()).rolling(window=param2,
                                                                                           min_periods=param2).sum()
        part1 = rolling_corr(part1, data['close_price'], param3).rank(axis=1, pct=True)
        part2 = (data['highest_price'] * param4 + data['vwap'] * param5).rank(axis=1, pct=True)
        part2 = rolling_corr(part2, data['turnover_vol'].rank(axis=1, pct=True), param6)
        part2 = part2.rank(axis=1, pct=True)
        return (part2 - part1).iloc[-1] * (param7)


    def alpha191_102(self, data, param1=1, param2=0, param3=6, param4=1, 
                  dependencies=['turnover_vol'], max_window=7):
        # SMA(MAX(VOLUME-DELAY(VOLUME,1),0),6,1)/SMA(ABS(VOLUME-DELAY(VOLUME,1)),6,1)*100
        part1 = (np.maximum(data['turnover_vol'].diff(param1), float(param2))).ewm(adjust=False, alpha=float(param4)/param3,
                                                                             min_periods=0, ignore_na=False).mean()
        part2 = abs(data['turnover_vol'].diff(param1)).ewm(adjust=False, alpha=float(param4)/param3, 
                                                     min_periods=0, ignore_na=False).mean()
        return (part1 / part2).iloc[-1] * 100


    def alpha191_103(self, data, param1=20, param2=20, dependencies=['lowest_price'], max_window=20):
        # ((20-LOWDAY(LOW,20))/20)*100
        return (param2 - data['lowest_price'].rolling(window=param1, min_periods=param1
                                        ).apply(lambda x: (param1-1)-x.argmin(axis=0))).iloc[-1] * float(100/param1)


    def alpha191_104(self, data, param1=5, param2=5, param3=20, param4=-1,
                  dependencies=['highest_price', 'turnover_vol', 'close_price'], max_window=20):
        # -1*(DELTA(CORR(HIGH,VOLUME,5),5)*RANK(STD(CLOSE,20)))
        corr_df = rolling_corr(data['highest_price'], data['turnover_vol'], param1)
        part1 = corr_df.diff(param2)
        part2 = (data['close_price'].rolling(window=param3, min_periods=param3).std()).rank(axis=1, pct=True)
        return (part1 * part2).iloc[-1] * (param4)


    def alpha191_105(self, data, param1=10, param2=-1, dependencies=['open_price', 'turnover_vol'], max_window=10):
        # -1*CORR(RANK(OPEN),RANK(VOLUME),10)
        corr_df = rolling_corr(data['open_price'].rank(axis=1, pct=True), data['turnover_vol'].rank(axis=1, pct=True), param1)
        return corr_df.iloc[-1] * (param2)
    
    
    def alpha191_106(self, data, param1=20, dependencies=['close_price'], max_window=21):
        # CLOSE-DELAY(CLOSE,20)
        return data['close_price'].diff(param1).iloc[-1]


    def alpha191_107(self, data, param1=1, param2=-1, 
                  dependencies=['open_price', 'close_price', 'highest_price', 'lowest_price'], max_window=2):
        # (-1*RANK(OPEN-DELAY(HIGH,1)))*RANK(OPEN-DELAY(CLOSE,1))*RANK(OPEN-DELAY(LOW,1))
        part1 = (data['open_price'] - data['highest_price'].shift(param1)).rank(axis=1, pct=True)
        part2 = (data['open_price'] - data['close_price'].shift(param1)).rank(axis=1, pct=True)
        part3 = (data['open_price'] - data['lowest_price'].shift(param1)).rank(axis=1, pct=True)
        return (part1 * part2 * part3).iloc[-1] * (param2)


    def alpha191_108(self, data, param1=2, param2=120, param3=6, param4=-1,
                  dependencies=['highest_price', 'vwap', 'turnover_vol'], max_window=126):
        # (RANK(HIGH-MIN(HIGH,2))^RANK(CORR(VWAP,MEAN(VOLUME,120),6)))*-1
        part1 = (data['highest_price'] - data['highest_price'].rolling(window=param1,min_periods=param1
                                                                      ).min()).rank(axis=1, pct=True)
        vol = data['turnover_vol'].rolling(window=param2, min_periods=param2).mean()
        vwap = data['vwap']
        part2 = rolling_corr(vol, vwap, param3).rank(axis=1, pct=True)
        return (part1.iloc[-1] ** part2.iloc[-1]) * (param4)


    def alpha191_109(sel, data, param1=2, param2=10, dependencies=['highest_price', 'lowest_price'], max_window=20):
        # SMA(HIGH-LOW,10,2)/SMA(SMA(HIGH-LOW,10,2),10,2)
        part1 = (data['highest_price']-data['lowest_price']).ewm(
            adjust=False, alpha=float(param1)/param2, min_periods=0, ignore_na=False).mean()
        return (part1 / part1.ewm(adjust=False, alpha=float(param1)/param2, min_periods=0, ignore_na=False).mean()).iloc[-1]


    def alpha191_110(self, data, param1=1, param2=20, 
                  dependencies=['close_price', 'highest_price', 'lowest_price'], max_window=21):
        # SUM(MAX(0,HIGH-DELAY(CLOSE,1)),20)/SUM(MAX(0,DELAY(CLOSE,1)-LOW),20)*100
        part1 = (np.maximum(data['highest_price']-data['close_price'].shift(param1),
                            0.0)).rolling(window=param2,min_periods=param2).sum()
        part2 = (np.maximum(data['close_price'].shift(param1)-data['lowest_price'],
                            0.0)).rolling(window=param2,min_periods=param2).sum()
        return (part1 / part2).iloc[-1] * 100.0


    def alpha191_111(self, data, para1=11, param2=2, param3=4, param4=2,
                  dependencies=['lowest_price', 'highest_price', 'close_price', 'turnover_vol'], max_window=11):
        # SMA(VOL*(2*CLOSE-LOW-HIGH)/(HIGH-LOW),11,2)-SMA(VOL*(2*CLOSE-LOW-HIGH)/(HIGH-LOW),4,2)
        win_vol = data['turnover_vol'] * (data['close_price'] * param2 - data['lowest_price'] - data['highest_price']) / (
                data['highest_price'] - data['lowest_price'])
        alpha = win_vol.ewm(adjust=False, alpha=float(param4) / para1, min_periods=0, ignore_na=False).mean() - win_vol.ewm(
            adjust=False, alpha=float(param4) / param3, min_periods=0, ignore_na=False).mean()
        return alpha.iloc[-1]


    def alpha191_112(self, data, param1=12, param2=1, dependencies=['close_price'], max_window=13):
        # (SUM((CLOSE-DELAY(CLOSE,1)>0?CLOSE-DELAY(CLOSE,1):0),12)-SUM((CLOSE-DELAY(CLOSE,1)<0?ABS(CLOSE-DELAY(CLOSE,1)):0),12))
            # /(SUM((CLOSE-DELAY(CLOSE,1)>0?CLOSE-DELAY(CLOSE,1):0),12)+SUM((CLOSE-DELAY(CLOSE,1)<0?ABS(CLOSE-DELAY(CLOSE,1)):0),12))*100
        part1 = (np.maximum(data['close_price'].diff(param2), 0.0)).rolling(window=param1, min_periods=param1).sum()
        part2 = abs(np.minimum(data['close_price'].diff(param2), 0.0)).rolling(window=param1, min_periods=param1).sum()
        return ((part1 - part2) / (part1 + part2)).iloc[-1] * 100


    def alpha191_113(self, data, param1=5, param2=20, param3=2, param4=5, param5=-1,
                  dependencies=['close_price', 'turnover_vol'], max_window=28):
        # -1*RANK(SUM(DELAY(CLOSE,5),20)/20)*CORR(CLOSE,VOLUME,2)*RANK(CORR(SUM(CLOSE,5),SUM(CLOSE,20),2))
        # 注意： 此处的第二第三部分的rolling corr的窗口为2，意味着计算得到的值只有1和-1两个，不知是否合理
        part1 = (data['close_price'].shift(param1).rolling(window=param2, min_periods=param2).mean()).rank(axis=1, pct=True)
        part2 = rolling_corr(data['close_price'], data['turnover_vol'], param3)
        corr_df = rolling_corr(data['close_price'].rolling(window=param4, min_periods=param4).sum(),
                           data['close_price'].rolling(window=param2, min_periods=param2).sum(), param3)
        part3 = corr_df.rank(axis=1, pct=True)
        return (part1 * part2 * part3).iloc[-1] * (param5)


    def alpha191_114(self, data, param1=5, param2=2,
                  dependencies=['highest_price', 'lowest_price', 'close_price', 'vwap', 'turnover_vol'], 
                  max_window=8):
        # RANK(DELAY((HIGH-LOW)/(SUM(CLOSE,5)/5),2))*RANK(RANK(VOLUME))/((HIGH-LOW)/(SUM(CLOSE,5)/5)/(VWAP-CLOSE))
        # RANK/RANK貌似没必要
        part1 = ((data['highest_price'] - data['lowest_price']) / (
            data['close_price'].rolling(window=param1, min_periods=param1).mean())).shift(param2).rank(axis=1, pct=True)
        part2 = data['turnover_vol'].rank(axis=1, pct=True).rank(axis=1, pct=True)
        part3 = (data['highest_price'] - data['lowest_price']) / (data['close_price'].rolling(
            window=param1, min_periods=param1).mean()) / (
                data['vwap'] - data['close_price'])
        return (part1 * part2 * part3).iloc[-1]


    def alpha191_115(self, data, param1=0.9, param2=0.1, param3=30, param4=10, param5=0.5,
                  param6=4, param7=10, pram8=7, 
                  dependencies=['highest_price', 'lowest_price', 'turnover_vol', 'close_price'], max_window=40):
        # (RANK(CORR(HIGH*0.9+CLOSE*0.1,MEAN(VOLUME,30),10))^RANK(CORR(TSRANK((HIGH+LOW)/2,4),TSRANK(VOLUME,10),7)))
        corr_df = rolling_corr(data['highest_price'] * param1 + data['close_price'] * param2,
                           data['turnover_vol'].rolling(window=param3, min_periods=param3).mean(), param4)
        part1 = corr_df.rank(axis=1, pct=True)
        price = rolling_rank(data['highest_price'] * param5 + data['lowest_price'] * param5, param6)
        vol = rolling_rank(data['turnover_vol'], param7)
        part2 = rolling_corr(price, vol, pram8).rank(axis=1, pct=True)
        return part1.iloc[-1] ** part2.iloc[-1]

    #@numba.jit
    '''
    def alpha191_116(self, data, param1=20, dependencies=['close_price'], max_window=20):
        # REGBETA(CLOSE,SEQUENCE,20)
        seq = np.array([i for i in range(1, param1+1)])
        close = data['close_price']
        # use np.linalg.lstsq计算回归: y = a*x + c
        win = param1
        a = np.vstack([seq, np.ones(win)]).T
        alpha = close.apply(lambda x: np.linalg.lstsq(a, x.iloc[-win:].values.T)[0][0])
        return alpha
     '''


    def alpha191_117(self, data, param1= 32, param2=16, param3=32, 
                  dependencies=['turnover_vol', 'close_price', 'highest_price', 'lowest_price'], 
                  max_window=32):
        # TSRANK(VOLUME,32)*(1-TSRANK(CLOSE+HIGH-LOW,16))*(1-TSRANK(RET,32))
        part1 = data['turnover_vol'].iloc[-param1:].rank(axis=0, pct=True)
        part2 = 1.0 - (data['close_price'] + data['highest_price'] - data['lowest_price']).iloc[-param2:].rank(axis=0, 
                                                                                                               pct=True)
        part3 = 1.0 - data['close_price'].pct_change(periods=1).iloc[-param3:].rank(axis=0, pct=True)
        return (part1 * part2 * part3).iloc[-1]


    def alpha191_118(self, data, param1=20, param2=20,
              dependencies=['highest_price', 'open_price', 'lowest_price'], max_window=20):
        # SUM(HIGH-OPEN,20)/SUM(OPEN-LOW,20)*100
        alpha = (data['highest_price'] - data['open_price']).rolling(window=param1, min_periods=param1).sum() / \
                (data['open_price'] - data['lowest_price']).rolling(window=param2, min_periods=param2).sum() * 100.0
        return alpha.iloc[-1]


    def alpha191_119(self, data, param1=5, param2=26, param3=5, param4=7,
                  param5=15, param6=21, param7=9, param8=7, param9=8,
                  dependencies=['vwap', 'turnover_vol', 'open_price'], max_window=62):
        # RANK(DECAYLINEAR(CORR(VWAP,SUM(MEAN(VOLUME,5),26),5),7))-RANK(DECAYLINEAR(TSRANK(MIN(CORR(RANK(OPEN),RANK(MEAN(VOLUME,15)),21),9),7),8))
        # 感觉有个TSMIN
        w7 = preprocessing.normalize(np.matrix([i for i in range(1, param4 + 1)]), norm='l1', axis=1).reshape(-1)
        w8 = preprocessing.normalize(np.matrix([i for i in range(1, param9 + 1)]), norm='l1', axis=1).reshape(-1)
        vol = (data['turnover_vol'].rolling(window=param1, min_periods=param1).mean()).rolling(
            window=param2, min_periods=param2).sum()
        corr_df = rolling_corr(vol, data['vwap'], param3)
        part1 = rolling_dot(corr_df, w7, param4).rank(axis=1, pct=True)
        vol = (data['turnover_vol'].rolling(window=param5, min_periods=param5).mean()).rank(axis=1, pct=True)
        corr_df = rolling_corr(vol, data['open_price'].rank(axis=1, pct=True), param6)
        corr_df = corr_df.rolling(window=param7, min_periods=param7).min()
        part2 = rolling_dot(rolling_rank(corr_df, param8), w8, param9).rank(axis=1, pct=True)
        return (part1 - part2).iloc[-1]


    def alpha191_120(self, data, dependencies=['vwap', 'close_price'], max_window=1):
        # RANK(VWAP-CLOSE)/RANK(VWAP+CLOSE)
        vwap = data['vwap']
        return ((vwap-data['close_price']).rank(axis=1,pct=True) / (vwap+data['close_price']).rank(axis=1,pct=True)).iloc[-1]


    def alpha191_121(self, data, param1=12, param2=60, param3=2, param4=20, param5=18, param6=3, param7=-1,
                  dependencies=['vwap', 'turnover_vol'], max_window=83):
        # (RANK(VWAP-MIN(VWAP,12))^TSRANK(CORR(TSRANK(VWAP,20),TSRANK(MEAN(VOLUME,60),2),18),3))*-1
        vwap = data['vwap']
        part1 = (vwap - vwap.rolling(window=param1, min_periods=param1).min()).rank(axis=1, pct=True)
        vol = rolling_rank(data['turnover_vol'].rolling(window=param2, min_periods=param2).mean(), param3)
        vwap = rolling_rank(vwap, param4)
        part2 = rolling_rank(rolling_corr(vwap, vol, param5), param6)
        return (part1 ** part2).iloc[-1] * (-1)
    
    
    
    def alpha191_122(self, data, param1=13, param2=2, 
                  dependencies=['close_price'], max_window=40):
        # (SMA(SMA(SMA(LOG(CLOSE),13,2),13,2),13,2)-DELAY(SMA(SMA(SMA(LOG(CLOSE),13,2),13,2),13,2),1))/DELAY(SMA(SMA(SMA(LOG(CLOSE),13,2),13,2),13,2),1)
        part1 = (np.log(data['close_price'])).ewm(adjust=False, 
                                                  alpha=float(param2)/param1, min_periods=0, ignore_na=False).mean()
        part1 = (part1.ewm(adjust=False, alpha=float(param2)/param1, min_periods=0, ignore_na=False).mean()).ewm(adjust=False, alpha=float(param2)/param1, min_periods=0, ignore_na=False).mean()
        return part1.pct_change(periods=1).iloc[-1]


    def alpha191_123(self, data, param1=0.5, param2=20, param3=60, param4=9, param5=6,
                  dependencies=['highest_price', 'lowest_price', 'turnover_vol'], max_window=89):
        # (RANK(CORR(SUM((HIGH+LOW)/2,20),SUM(MEAN(VOLUME,60),20),9)) < RANK(CORR(LOW,VOLUME,6)))*-1
        price = (data['highest_price']*param1+data['lowest_price']*param1).rolling(window=param2, min_periods=param2).sum()
        vol = (data['turnover_vol'].rolling(window=param3,min_periods=param3).mean()).rolling(
            window=param2,min_periods=param2).sum()
        part1 = rolling_corr(vol, price, param4).rank(axis=1, pct=True)
        part2 = rolling_corr(data['lowest_price'], data['turnover_vol'], param5).rank(axis=1, pct=True)
        return (part2 - part1).iloc[-1] * (-1)


    def alpha191_124(self, data, param1=30, param2=2, 
                  dependencies=['close_price', 'vwap', 'turnover_vol'], max_window=32):
        # (CLOSE-VWAP)/DECAYLINEAR(RANK(TSMAX(CLOSE,30)),2)
        w2 = preprocessing.normalize(np.matrix([i for i in range(1, param2+1)]),norm='l1',axis=1).reshape(-1)
        part1 = data['close_price'] - data['vwap']
        part2 = rolling_dot((data['close_price'].rolling(
            window=param1,min_periods=param1).max()).rank(axis=1,pct=True), w2, param2)
        return (part1 / part2).iloc[-1]


    def alpha191_125(self, data, param1=80, param2=17, param3=20, param4=0.5,
                  param5=3, param6=16, dependencies=['close_price', 'vwap', 'turnover_vol'], max_window=117):
        # RANK(DECAYLINEAR(CORR(VWAP,MEAN(VOLUME,80),17),20))/RANK(DECAYLINEAR(DELTA(CLOSE*0.5+VWAP*0.5,3),16))
        vwap = data['vwap']
        w20 = preprocessing.normalize(np.matrix([i for i in range(1, param3+1)]),norm='l1',axis=1).reshape(-1)
        w16 = preprocessing.normalize(np.matrix([i for i in range(1, param6+1)]),norm='l1',axis=1).reshape(-1)
        part1 = rolling_corr(data['turnover_vol'].rolling(window=param1,min_periods=param1).mean(), vwap, 17)
        part1 = rolling_dot(part1, w20, param3).rank(axis=1, pct=True)
        part2 = rolling_dot((data['close_price']*param4+vwap*param4).diff(periods=param5), w16, param6).rank(axis=1,pct=True)
        return (part1 / part2).iloc[-1]


    def alpha191_126(self, data, param1=3, dependencies=['highest_price', 'lowest_price', 'close_price'], max_window=1):
        # (CLOSE+HIGH+LOW)/3
        return (data['close_price'] + data['highest_price'] + data['lowest_price']).iloc[-1] / float(param1)


    def alpha191_127(self, data, param1=12, param2=2, param3=0.5, dependencies=['close_price'], max_window=24):
        # MEAN((100*(CLOSE-MAX(CLOSE,12))/MAX(CLOSE,12))^2)^(1/2)
        # 这里貌似是TSMAX,MEAN少一个参数
        alpha = (data['close_price'] - data['close_price'].rolling(
            window=param1,min_periods=param1).max()) / data['close_price'].rolling(
            window=param1,min_periods=param1).max() * 100
        alpha = (alpha ** param2).rolling(window=param1, min_periods=param1).mean().iloc[-1] ** param3
        return alpha


    def alpha191_128(self, data, param1=3, param2=1, param3=14, dependencies=['highest_price', 'lowest_price', 'close_price', 'turnover_vol'], max_window=14):
        # 100-(100/(1+SUM(((HIGH+LOW+CLOSE)/3>DELAY((HIGH+LOW+CLOSE)/3,1)?(HIGH+LOW+CLOSE)/3*VOLUME:0),14)/
        # SUM(((HIGH+LOW+CLOSE)/3<DELAY((HIGH+LOW+CLOSE)/3,1)?(HIGH+LOW+CLOSE)/3*VOLUME:0),14)))
        condition1 = ((data['highest_price']+data['lowest_price']+data['close_price'])/float(param1)).diff(param2) > 0.0
        condition2 = ((data['highest_price']+data['lowest_price']+data['close_price'])/float(param1)).diff(param2) < 0.0
        part1 = (data['highest_price']+data['lowest_price']+data['close_price'])/float(param1)*data['turnover_vol']
        part2 = part1.copy(deep=True)
        part1[~condition1] = 0.0
        part1 = part1.rolling(window=param3, min_periods=param3).sum()
        part2[~condition2] = 0.0
        part2 = part2.rolling(window=param3, min_periods=param3).sum()
        return (100.0-(100.0/(1+part1/part2))).iloc[-1]


    def alpha191_129(self, data, param1=1, param2=0, param3=12, 
                  dependencies=['close_price'], max_window=13):
        # SUM((CLOSE-DELAY(CLOSE,1)<0?ABS(CLOSE-DELAY(CLOSE,1)):0),12)
        return (abs(np.minimum(data['close_price'].diff(param1), float(param2)))).rolling(
            window=param3, min_periods=param3).sum().iloc[-1]


    def alpha191_130(self, data, param1=10, param2=3, param3=40, param4=0.5,param5=9,
                  param6=10, param7=7, param8=3, 
                  dependencies=['lowest_price', 'highest_price', 'turnover_vol', 'vwap'], max_window=59):
        # (RANK(DECAYLINEAR(CORR((HIGH+LOW)/2,MEAN(VOLUME,40),9),10))/RANK(DECAYLINEAR(CORR(RANK(VWAP),RANK(VOLUME),7),3)))
        vwap = data['vwap']
        w10 = preprocessing.normalize(np.matrix([i for i in range(1, param1+1)]),norm='l1',axis=1).reshape(-1)
        w3 = preprocessing.normalize(np.matrix([i for i in range(1, param2+1)]),norm='l1',axis=1).reshape(-1)
        corr_df = rolling_corr(data['turnover_vol'].rolling(window=param3,min_periods=param3).mean(),
                               data['highest_price']*param4+data['lowest_price']*param4, param5)
        part1 = rolling_dot(corr_df, w10, param6).rank(axis=1, pct=True)
        corr_df = rolling_corr(data['turnover_vol'].rank(axis=1, pct=True), vwap.rank(axis=1, pct=True), param7)
        part2 = rolling_dot(corr_df, w3, param8).rank(axis=1, pct=True)
        return (part1 / part2).iloc[-1]


    def alpha191_131(self, data, param1=1, param2=50, param3=18, param4=18,
                  dependencies=['vwap', 'turnover_vol', 'close_price'], max_window=86):
        # (RANK(DELAT(VWAP,1))^TSRANK(CORR(CLOSE,MEAN(VOLUME,50),18),18))
        part1 = data['vwap'].diff(param1).rank(axis=1, pct=True).iloc[-1:]
        corr_df = rolling_corr(data['turnover_vol'].rolling(window=param2, 
                                                            min_periods=param2).mean(), data['close_price'], param4)
        part2 = corr_df.iloc[-param3:].rank(axis=0, pct=True)
        return (part1 ** part2).iloc[-1]


    def alpha191_132(self, data, param1=20, dependencies=['turnover_value'], max_window=20):
        # MEAN(AMOUNT,20)
        return data['turnover_value'].rolling(window=param1, min_periods=param1).mean().iloc[-1]


    def alpha191_133(self, data, param1=20, param2=20, param3=20, param4=20, 
                  dependencies=['lowest_price', 'highest_price'], max_window=20):
        # ((20-HIGHDAY(HIGH,20))/20)*100-((20-LOWDAY(LOW,20))/20)*100
        part1 = (param3 - data['highest_price'].rolling(window=param1, min_periods=param1
                                              ).apply(lambda x: (param3-1)-x.argmax(axis=0))) * (1.0*100/param4)
        part2 = (param3 - data['lowest_price'].rolling(window=param1, min_periods=param1)
                 .apply(lambda x: (param3-1)-x.argmin(axis=0))) * (1.0*100/param4)
        return (part1 -part2).iloc[-1]


    def alpha191_134(self, data, param1=12, dependencies=['close_price', 'turnover_vol'], max_window=13):
        # (CLOSE-DELAY(CLOSE,12))/DELAY(CLOSE,12)*VOLUME
        return (data['close_price'].pct_change(periods=param1) * data['turnover_vol']).iloc[-1]


    def alpha191_135(self, data, param1=20, param2=1, param3=20, param4=1, 
                  dependencies=['close_price'], max_window=42):
        # SMA(DELAY(CLOSE/DELAY(CLOSE,20),1),20,1)
        alpha = (data['close_price']/data['close_price'].shift(param1)).shift(param2)
        return alpha.ewm(adjust=False, alpha=float(param4)/param3, min_periods=0, ignore_na=False).mean().iloc[-1]


    def alpha191_136(self, data, param1=3, param2=10,  param3=-1, 
                  dependencies=['close_price', 'open_price', 'turnover_vol'], max_window=10):
        # -1*RANK(DELTA(RET,3))*CORR(OPEN,VOLUME,10)
        part1 = data['close_price'].pct_change(periods=1).diff(param1).rank(axis=1,pct=True)
        part2 = rolling_corr(data['open_price'], data['turnover_vol'], param2)
        return (part1 * part2).iloc[-1] * (param3)


    def alpha191_137(self, data, param1=1.5, param2=0.5, param3=1, param4=2.0, param5=4.0, param6=16.0,
                  dependencies=['open_price', 'lowest_price', 'close_price', 'highest_price'], max_window=4):
        # 16*(CLOSE+(CLOSE-OPEN)/2-DELAY(OPEN,1))/
        # ((ABS(HIGH-DELAY(CLOSE,1))>ABS(LOW-DELAY(CLOSE,1))&ABS(HIGH-DELAY(CLOSE,1))>ABS(HIGH-DELAY(LOW,1))?ABS(HIGH-DELAY(CLOSE,1))+ABS(LOW-DELAY(CLOSE,1))/2+ABS(DELAY(CLOSE,1)-DELAY(OPEN,1))/4:
        # (ABS(LOW-DELAY(CLOSE,1))>ABS(HIGH-DELAY(LOW,1)) & ABS(LOW-DELAY(CLOSE,1))>ABS(HIGH-DELAY(CLOSE,1))?ABS(LOW-DELAY(CLOSE,1))+ABS(HIGH-DELAY(CLOSE,1))/2+ABS(DELAY(CLOSE,1)-DELAY(OPEN,1))/4:ABS(HIGH-DELAY(LOW,1))+ABS(DELAY(CLOSE,1)-DELAY(OPEN,1))/4)))
        # *MAX(ABS(HIGH-DELAY(CLOSE,1)),ABS(LOW-DELAY(CLOSE,1)))
        
        part1 = data['close_price'] * param1 - data['open_price'] * param2 - data['open_price'].shift(param3)
        part2 = abs(data['highest_price']-data['close_price'].shift(param3)) + \
                    abs(data['lowest_price']-data['close_price'].shift(param3)) / param4 + \
                    abs(data['close_price']-data['open_price']).shift(param3) / param5
        
        condition1 = np.logical_and(abs(data['highest_price']-data['close_price'].shift(param3)) > \
                                    abs(data['lowest_price']-data['close_price'].shift(param3)),
                               abs(data['highest_price']-data['close_price'].shift(param3)) > \
                                    abs(data['highest_price']-data['lowest_price'].shift(param3)))
        
        condition2 = np.logical_and(abs(data['lowest_price']-data['close_price'].shift(param3)) > \
                                    abs(data['highest_price']-data['lowest_price'].shift(param3)),abs(data['lowest_price']-\
                                    data['close_price'].shift(param3)) > abs(data['highest_price']-data['close_price'].shift(
                                    param3)))
        
        part2[~condition1 & condition2] = abs(data['lowest_price']-data['close_price'].shift(param3)) + \
                                abs(data['highest_price']-data['close_price'].shift(param3)) / param4 + \
                                abs(data['close_price']-data['open_price']).shift(param3) / param5
        
        part2[~condition1 & ~condition2] = abs(data['highest_price']-data['lowest_price'].shift(param3)) + \
                                    abs(data['close_price']-data['open_price']).shift(param3) / param5
        part3 = np.maximum(abs(data['highest_price']-data['close_price'].shift(param3)), abs(data['lowest_price']-\
                                                            data['close_price'].shift(param3)))
        alpha = (part1 / part2 * part3 * param6).iloc[-1]
        return alpha


    def alpha191_138(self, data, param1=20, param2=16, param3=0.3, param4=0.7, param5=3, param6=20,
                  param7=60, param8=17, param9=8, param10=5, param11=19, param12=16, param13=7,
                  param14=-1, dependencies=['lowest_price','vwap','turnover_vol'], max_window=126):
        # ((RANK(DECAYLINEAR(DELTA(LOW*0.7+VWAP*0.3,3),20))-TSRANK(DECAYLINEAR(TSRANK(CORR(TSRANK(LOW,8),TSRANK(MEAN(VOLUME,60),17),5),19),16),7))* -1)
        
        w20 = preprocessing.normalize(np.matrix([i for i in range(1, param1+1)]),norm='l1',axis=1).reshape(-1)
        w16 = preprocessing.normalize(np.matrix([i for i in range(1, param2+1)]),norm='l1',axis=1).reshape(-1)
        part1 = rolling_dot((data['lowest_price']*param4+data['vwap']*param3).diff(param5), w20, param6
                           ).rank(axis=1, pct=True)
        rank_df = rolling_rank(data['turnover_vol'].rolling(window=param7, min_periods=param7).mean(), param8)
        corr_df = rolling_corr(rank_df, rolling_rank(data['lowest_price'], param9), param10)
        part2 = rolling_rank(rolling_dot(rolling_rank(corr_df, param11), w16, param12), param13)
        return (part1-part2).iloc[-1] * (param14)


    def alpha191_139(self, data, param1=10, param2=-1, dependencies=['open_price', 'turnover_vol'], max_window=10):
        # (-1*CORR(OPEN,VOLUME,10))
        corr_df = rolling_corr(data['open_price'], data['turnover_vol'], param1)
        return corr_df.iloc[-1] * (param2)


    def alpha191_140(self, data, param1=8, param2=60, param3=20, param4=8, param5=7, param6=3,
                  dependencies=['open_price', 'lowest_price', 'highest_price', 'close_price', 'turnover_vol'], max_window=99):
        # MIN(RANK(DECAYLINEAR(RANK(OPEN)+RANK(LOW)-RANK(HIGH)-RANK(CLOSE),8)),TSRANK(DECAYLINEAR(CORR(TSRANK(CLOSE,8),TSRANK(MEAN(VOLUME,60),20),8),7),3))
        w8 = preprocessing.normalize(np.matrix([i for i in range(1, param1+1)]), norm='l1', axis=1).reshape(-1)
        w7 = preprocessing.normalize(np.matrix([i for i in range(1, param5 +1)]), norm='l1', axis=1).reshape(-1)
        part1 = data['open_price'].rank(axis=1, pct=True) + data['lowest_price'].rank(axis=1, pct=True) - \
            data['highest_price'].rank(axis=1, pct=True) - data['close_price'].rank(axis=1, pct=True)
        part1 = rolling_dot(part1, w8, param4).rank(axis=1, pct=True)
        part2 = rolling_rank(data['turnover_vol'].rolling(window=param2, min_periods=param2).mean(), param3)
        part2 = rolling_corr(part2, rolling_rank(data['close_price'], param4), param4)
        part2 = rolling_rank(rolling_dot(part2, w7, param5), param6)
        return np.minimum(part1, part2).iloc[-1]


    def alpha191_141(self, data, param1=15, param2=9, param3=-1,
                  dependencies=['highest_price', 'turnover_vol'], max_window=25):
        # (RANK(CORR(RANK(HIGH),RANK(MEAN(VOLUME,15)),9))*-1)
        vol = data['turnover_vol'].rolling(window=param1, min_periods=param1).mean().rank(axis=1, pct=True)
        price = data['highest_price'].rank(axis=1, pct=True)
        alpha = rolling_corr(vol, price, param2).rank(axis=1, pct=True)
        return alpha.iloc[-1] * (param3)


    def alpha191_142(self, data, param1=10, param2=20, param3=5, param4=1, param5=-1,
                  dependencies=['close_price', 'turnover_vol'], max_window=25):
        # -1*RANK(TSRANK(CLOSE,10))*RANK(DELTA(DELTA(CLOSE,1),1))*RANK(TSRANK(VOLUME/MEAN(VOLUME,20),5))
        part1 = (rolling_rank(data['close_price'], param1)).rank(axis=1, pct=True)
        part2 = (data['close_price'].diff(1)).diff(param3).rank(axis=1, pct=True)
        vol = data['turnover_vol'] / data['turnover_vol'].rolling(window=param2, min_periods=20).mean()
        part3 = rolling_rank(vol, param3).rank(axis=1, pct=True)
        return (part1 * part2 * part3).iloc[-1] * (param5)


    def alpha191_143(self, data, param1=1, param2=1, parm3=20, param4=1, 
                  dependencies=['close_price'], max_window=21):
        # CLOSE>DELAY(CLOSE,1)?(CLOSE-DELAY(CLOSE,1))/DELAY(CLOSE,1)*SELF:SELF
        # 表示 t-1 日的 alpha191_143 因子计算结果
        # Notes: (CLOSE-DELAY(CLOSE,1))/DELAY(CLOSE,1) 为日收益，累乘有问题
        # 日收益 * SELF也有问题，从什么时候开始乘呢？ 所以改成如下因子
        condition = data['close_price'] > data['close_price'].shift(param1)
        ret = data['close_price'] / data['close_price'].shift(param1) - float(param2)
        alpha = ret[ret>0].rolling(window=parm3, min_periods=param4).sum()
        return alpha.iloc[-1]


    def alpha191_144(self, data, param1=1, param2=1, param3=20, param4=20, 
                  dependencies=['close_price', 'turnover_value'], max_window=21):
        # SUMIF(ABS(CLOSE/DELAY(CLOSE,1)-1)/AMOUNT,20,CLOSE<DELAY(CLOSE,1))/COUNT(CLOSE<DELAY(CLOSE,1),20)
        part1 = abs(data['close_price'].pct_change(periods=param1)) / data['turnover_value']
        part1[data['close_price'].diff(param2) >= 0] = 0.0
        part1 = part1.rolling(window=param3, min_periods=param3).sum()
        part2 = (data['close_price'].diff(param2) < 0.0).rolling(window=param4, min_periods=param4).sum()
        return (part1 / part2).iloc[-1]


    def alpha191_145(self, data, param1=9, param2=26, param3=12, dependencies=['turnover_vol'], max_window=26):
        # (MEAN(VOLUME,9)-MEAN(VOLUME,26))/MEAN(VOLUME,12)*100
        alpha = (data['turnover_vol'].rolling(
            window=param1, min_periods=param1).mean() - data['turnover_vol'].rolling(window=param2,
            min_periods=param2).mean()) / data['turnover_vol'].rolling(window=param3, 
                                                                 min_periods=param3).mean() * 100.0
        return alpha.iloc[-1]


    def alpha191_146(self, data, param1=61, param2=2, param3=20,
                  param4=60, param5=1, dependencies=['close_price'], max_window=121):
        # MEAN(RET-SMA(RET,61,2),20)*(RET-SMA(RET,61,2))/SMA(SMA(RET,61,2)^2,60)
        # 假设最后一个SMA(X,60,1)
        sma = (data['close_price'].pct_change(param5)).ewm(adjust=False, alpha=float(param2) / param1, min_periods=0,
                                                 ignore_na=False).mean()
        ret_excess = data['close_price'].pct_change(param5) - sma
        part1 = ret_excess.rolling(window=param3, min_periods=param3).mean() * ret_excess
        part2 = (sma ** param2).ewm(adjust=False, alpha=float(1) / param4, min_periods=0, ignore_na=False).mean()
        return (part1 / part2).iloc[-1]

    '''
    def alpha191_147(self, data, param1=12, param2=12, dependencies=['close_price'], max_window=24):
        # REGBETA(MEAN(CLOSE,12),SEQUENCE(12))
        ma_price = data['close_price'].rolling(window=param1, min_periods=param1).mean()
        win = 12
        a = np.vstack([np.array([i for i in range(1, param2+1)]), np.ones(win)]).T
        alpha = ma_price.iloc[-param2:].apply(lambda x: np.linalg.lstsq(a, x.values.T)[0][0])
        return alpha
    '''


    def alpha191_148(self, data, param1=60, param2=6, param3=14, param4=-1, param5=9,
                  dependencies=['open_price', 'turnover_vol'], max_window=75):
        # (RANK(CORR(OPEN,SUM(MEAN(VOLUME,60),9),6))<RANK(OPEN-TSMIN(OPEN,14)))*-1
        part1 = (data['turnover_vol'].rolling(window=param1, min_periods=param1).mean()).rolling(window=param5,
                                                                                                 min_periods=param5).sum()
        part1 = rolling_corr(part1, data['open_price'], param2).rank(axis=1, pct=True)
        part2 = (data['open_price'] - data['open_price'].rolling(window=param3, min_periods=param3).min()).rank(axis=1, pct=True)
        return (part2 - part1).iloc[-1] * (param4)

    '''
    def alpha191_149(self, data, param1=1, param2=252, dependencies=['close_price'], max_window=253):
        # REGBETA(FILTER(RET,BANCHMARK_INDEX_CLOSE<DELAY(BANCHMARK_INDEX_CLOSE,1)),
        # FILTER(BANCHMARK_INDEX_CLOSE/DELAY(BANCHMARK_INDEX_CLOSE,1)-1,BANCHMARK_INDEX_CLOSE<DELAY(BANCHMARK_INDEX_CLOSE,1)),252)
        bm = (data['close_price'].mean(axis=1).diff(param1) < 0.0)
        part1 = data['close_price'].pct_change(periods=param1).iloc[-param2:][bm]
        part2 = data['close_price'].pct_change(periods=param1).mean(axis=1).iloc[-param2:][bm]
        win = len(part2)
        a = np.vstack([part2.values, np.ones(win)]).T
        alpha = part1.apply(lambda x: np.linalg.lstsq(a, x.values.T)[0][0])
        return alpha
    '''


    def alpha191_150(self, data, param1=3, dependencies=['close_price', 'highest_price', 
                                                      'lowest_price', 'turnover_vol'], max_window=1):
        # (CLOSE+HIGH+LOW)/3*VOLUME
        return ((data['close_price'] + data['highest_price'] + data['lowest_price']
                ) / param1 * data['turnover_vol']).iloc[-1]


    def alpha191_151(self, data, param1=20, param2=1, dependencies=['close_price'], max_window=41):
        # SMA(CLOSE-DELAY(CLOSE,20),20,1)
        return (data['close_price'].diff(param1)).ewm(adjust=False, 
                alpha=float(param2) / param1, min_periods=0, ignore_na=False).mean().iloc[-1]


    def alpha191_152(self, data, param1=9, param2=12, param3=26, param4=1, param5=1,
                  dependencies=['close_price'], max_window=59):
        # A=DELAY(SMA(DELAY(CLOSE/DELAY(CLOSE,9),1),9,1),1)
        # SMA(MEAN(A,12)-MEAN(A,26),9,1)
        part1 = ((data['close_price'] / data['close_price'].shift(param1)).shift(param4)).ewm(
            adjust=False, alpha=float(param5) / param1,min_periods=0,ignore_na=False).mean().shift(1)
        
        alpha = (part1.rolling(window=param2, min_periods=param2).mean() - part1.rolling(window=param3,
                                     min_periods=param3).mean()).ewm(adjust=False, alpha=float(param5) / param1, 
                                                                     min_periods=0, ignore_na=False).mean()
        return alpha.iloc[-1]


    def alpha191_153(self, data, param1=3, param2=6, param3=12, param4=24, param5=4,
                  dependencies=['close_price'], max_window=24):
        # (MEAN(CLOSE,3)+MEAN(CLOSE,6)+MEAN(CLOSE,12)+MEAN(CLOSE,24))/4
        # 就是BBI
        ma_sum = None
        for win in [param1,param2,param3,param4]:
            ma = data['close_price'].rolling(window=win, min_periods=win).mean()
            ma_sum = ma if ma_sum is None else ma_sum + ma
        alpha = ma_sum / float(param5)
        return alpha.iloc[-1]


    def alpha191_154(self, data, param1=16, param2=180, param3=18,
                  dependencies=['vwap', 'turnover_vol'], max_window=198):
        # VWAP-MIN(VWAP,16)<CORR(VWAP,MEAN(VOLUME,180),18)
        # 感觉是TSMIN
        part1 = data['vwap'] - data['vwap'].rolling(window=param1, min_periods=param1).min()
        part2 = rolling_corr(data['turnover_vol'].rolling(window=param2, min_periods=param2).mean(), data['vwap'], param3)
        return (part2 - part1).iloc[-1]


    def alpha191_155(self, data, param1=13, param2=27, param3=10, param4=2, 
                  dependencies=['turnover_vol'], max_window=37):
        # SMA(VOLUME,13,2)-SMA(VOLUME,27,2)-SMA(SMA(VOLUME,13,2)-SMA(VOLUME,27,2),10,2)
        sma13 = data['turnover_vol'].ewm(adjust=False, alpha=float(param4) / param1, min_periods=0, ignore_na=False).mean()
        sma27 = data['turnover_vol'].ewm(adjust=False, alpha=float(param4) / param2, min_periods=0, ignore_na=False).mean()
        ssma = (sma13 - sma27).ewm(adjust=False, alpha=float(param4) / param3, min_periods=0, ignore_na=False).mean()
        return (sma13 - sma27 - ssma).iloc[-1]


    def alpha191_156(self, data, param1=5, param2=3, param3=0.15, param4=0.85,
                  param5=3, param6=-1, parm7=2,
                  dependencies=['vwap', 'turnover_vol', 'open_price', 'lowest_price'], max_window=9):
        # MAX(RANK(DECAYLINEAR(DELTA(VWAP,5),3)),RANK(DECAYLINEAR((DELTA(OPEN*0.15+LOW*0.85,2)/(OPEN*0.15+LOW*0.85)) * -1,3))) * -1
        w3 = preprocessing.normalize(np.matrix([i for i in range(1, param2+1)]), norm='l1', axis=1).reshape(-1)
        den = data['open_price'] * param3 + data['lowest_price'] * param3
        part1 = rolling_dot(data['vwap'].diff(param1), w3, param5).rank(axis=1, pct=True)
        part2 = rolling_dot(den.diff(parm7) / den * (param6), w3, param5).rank(axis=1, pct=True)
        return np.maximum(part1, part2).iloc[-1] * (param6)
    
    
    def alpha191_157(self, data, param1=1, param2=5, param3=-1, param4=2, pram5=5, param6=-1,
                  param7=6, dependencies=['close_price'], max_window=12):
            # MIN(PROD(RANK(LOG(SUM(TSMIN(RANK(-1*RANK(DELTA(CLOSE-1,5))),2),1))),1),5) +TSRANK(DELAY(-1*RET,6),5)
            part1 = np.log(
                (((data['close_price'] - float(param1)).diff(param2).rank(
                    axis=1, pct=True) * (param3)).rank(axis=1, pct=True)).rolling(window=param4,
                min_periods=param4).min())
            part1 = (part1.rank(axis=1, pct=True)).rolling(window=pram5, min_periods=pram5).min().iloc[-1:]
            part2 = ((data['close_price'].pct_change(periods=1) * (param6)).shift(param7)).iloc[-pram5:].rank(axis=0, pct=True)
            return (part1 + part2).iloc[-1]

    def alpha191_158(self, data, dependencies=['lowest_price', 'highest_price', 'close_price'], max_window=1):
        # (HIGH-LOW)/CLOSE
        return ((data['highest_price'] - data['lowest_price']) / data['close_price']).iloc[-1]


    def alpha191_159(self, data, param1=1, param2=6, param3=12, param4=24, 
                  dependencies=['close_price', 'lowest_price', 'highest_price'], max_window=25):
        # ((CLOSE-SUM(MIN(LOW,DELAY(CLOSE,1)),6))/SUM(MAX(HGIH,DELAY(CLOSE,1))-MIN(LOW,DELAY(CLOSE,1)),6)*12*24
        # +(CLOSE-SUM(MIN(LOW,DELAY(CLOSE,1)),12))/SUM(MAX(HGIH,DELAY(CLOSE,1))-MIN(LOW,DELAY(CLOSE,1)),12)*6*24
        # +(CLOSE-SUM(MIN(LOW,DELAY(CLOSE,1)),24))/SUM(MAX(HGIH,DELAY(CLOSE,1))-MIN(LOW,DELAY(CLOSE,1)),24)*6*24)*100/(6*12+6*24+12*24)
        min_low_close = np.minimum(data['lowest_price'], data['close_price'].shift(param1))
        max_high_close = np.maximum(data['highest_price'], data['close_price'].shift(param1))
        part1 = (data['close_price'] - min_low_close.rolling(window=param2, min_periods=param2).sum()) / \
            (max_high_close - min_low_close).rolling(window=param2, min_periods=param2).sum() * param3 * param4
        part2 = (data['close_price'] - min_low_close.rolling(window=param3, min_periods=param3).sum()) / \
            (max_high_close - min_low_close).rolling(window=param3, min_periods=param3).sum() * param2 * param4
        part3 = (data['close_price'] - min_low_close.rolling(window=param4, min_periods=param4).sum()) / \
            (max_high_close - min_low_close).rolling(window=param4, min_periods=param4).sum() * param2 * param3
        return (part1 + part2 + part3).iloc[-1] * 100.0 / (param3 * param2 + param2 * param4 + param3 * param4)


    def alpha191_160(self, data, param1=1, param2=20, param3=0, dependencies=['close_price'], max_window=41):
        # SMA((CLOSE<=DELAY(CLOSE,1)?STD(CLOSE,20):0),20,1)
        part1 = data['close_price'].rolling(window=param2, min_periods=param2).std()
        part1[data['close_price'].diff(param1) > 0] = float(param3)
        return part1.ewm(adjust=False, alpha=float(param1) / param2, min_periods=0, ignore_na=False).mean().iloc[-1]


    def alpha191_161(self, data, param1=1, param2=12, 
                  dependencies=['close_price', 'lowest_price', 'highest_price'], max_window=13):
        # MEAN(MAX(MAX(HIGH-LOW,ABS(DELAY(CLOSE,1)-HIGH)),ABS(DELAY(CLOSE,1)-LOW)),12)
        part1 = np.maximum(data['highest_price'] - data['lowest_price'], 
                           abs(data['close_price'].shift(param1) - data['highest_price']))
        part1 = np.maximum(part1, abs(data['close_price'].shift(param1) - data['lowest_price']))
        return part1.rolling(window=param2, min_periods=param2).mean().iloc[-1]


    def alpha191_162(self, data, param1=1, param2=1, param3=12, dependencies=['close_price'], 
                  max_window=25):
        # (SMA(MAX(CLOSE-DELAY(CLOSE,1),0),12,1)/SMA(ABS(CLOSE-DELAY(CLOSE,1)),12,1)*100
        # -MIN(SMA(MAX(CLOSE-DELAY(CLOSE,1),0),12,1)/SMA(ABS(CLOSE-DELAY(CLOSE,1)),12,1)*100,12))
        # /(MAX(SMA(MAX(CLOSE-DELAY(CLOSE,1),0),12,1)/SMA(ABS(CLOSE-DELAY(CLOSE,1)),12,1)*100,12)
        # -MIN(SMA(MAX(CLOSE-DELAY(CLOSE,1),0),12,1)/SMA(ABS(CLOSE-DELAY(CLOSE,1)),12,1)*100,12))
        den = (np.maximum(data['close_price'].diff(param1), 0.0)).ewm(adjust=False, alpha=float(param2) / param3,
              min_periods=0,ignore_na=False).mean() / (abs(data['close_price'].diff(param1))).ewm(adjust=False, 
              alpha=float(param2) / param3, min_periods=0, ignore_na=False).mean() * 100.0
        alpha = (den - den.rolling(window=param3, min_periods=param3).min()) / (
                den.rolling(window=param3, min_periods=param3).max() - den.rolling(
                    window=param3, min_periods=param3).min())
        return alpha.iloc[-1]


    def alpha191_163(self, data, dependencies=['vwap', 'turnover_vol', 'close_price', 'highest_price'], max_window=20):
        # RANK((-1*RET)*MEAN(VOLUME,20)*VWAP*(HIGH-CLOSE))
        alpha = data['close_price'].pct_change(periods=1) * (data['turnover_vol'].rolling(window=20, min_periods=20).mean()) * \
                data['vwap'] * (data['highest_price'] - data['close_price']) * (-1)
        return alpha.rank(axis=1, pct=True).iloc[-1]


    def alpha191_164(self, data, param1=1, param2=1, param3=12, param4=2, 
                  dependencies=['close_price', 'highest_price', 'lowest_price'], max_window=26):
        # SMA(((CLOSE>DELAY(CLOSE,1)?1/(CLOSE-DELAY(CLOSE,1)):1)-MIN(CLOSE>DELAY(CLOSE,1)?1/(CLOSE-DELAY(CLOSE,1)):1,12))/(HIGH-LOW)*100,13,2)
        part1 = float(param1) / data['close_price'].diff(param2)
        part1[data['close_price'].diff(param2) <= 0] = float(param1)
        part2 = part1.rolling(window=param3, min_periods=param3).min()
        alpha = (part1 - part2) / (data['highest_price'] - data['lowest_price']) * 100.0
        return alpha.ewm(adjust=False, alpha=float(param4) / (param3+1), min_periods=0, ignore_na=False).mean().iloc[-1]


    def alpha191_165(self, data, param1=48, dependencies=['close_price'], max_window=144):
        # MAX(SUMAC(CLOSE-MEAN(CLOSE,48)))-MIN(SUMAC(CLOSE-MEAN(CLOSE,48)))/STD(CLOSE,48)
        # SUMAC少了前N项和,TSMAX/TSMIN
        part1 = ((data['close_price'] - data['close_price'].rolling(
            window=param1, min_periods=param1).mean()).rolling(window=param1,min_periods=param1).sum()).rolling(
            window=param1, min_periods=param1).max()
        
        part2 = ((data['close_price'] - data['close_price'].rolling(window=param1,
            min_periods=param1).mean()).rolling(window=param1,
            min_periods=param1).sum()).rolling(window=param1, min_periods=param1).min()
        
        part3 = data['close_price'].rolling(window=param1, min_periods=param1).std()
        return (part1 - part2 / part3).iloc[-1]


    def alpha191_166(self, data, param1=1, param2=20, param3=1.5, param4=2,
                  dependencies=['close_price'], max_window=41):
        # -20*(20-1)^1.5*SUM(CLOSE/DELAY(CLOSE,1)-1-MEAN(CLOSE/DELAY(CLOSE,1)-1,20),20)/((20-1)*(20-2)*(SUM((CLOSE/DELAY(CLOSE,1))^2,20))^1.5)
        part1 = data['close_price'].pct_change(periods=param1) - (
            data['close_price'].pct_change(periods=param1).rolling(window=param2, min_periods=param2).mean())
        part1 = part1.rolling(window=param2, min_periods=param2).sum() * ((-param2) * (param2-1) ** param3)
        part2 = (((data['close_price'] / data['close_price'].shift(param1)) ** param4).rolling(window=param2,
                                                                               min_periods=param2).sum() ** param3
                                                                                ) * (param2-1) * (param2-2)
        return (part1 / part2).iloc[-1]


    def alpha191_167(self, data, pararm1=1, param2=0, param3=12,
                  dependencies=['close_price'], max_window=13):
        # SUM(CLOSE-DELAY(CLOSE,1)>0?CLOSE-DELAY(CLOSE,1):0,12)
        return (np.maximum(data['close_price'].diff(pararm1), float(param2))).rolling(window=param3,
                min_periods=param3).sum().iloc[-1]


    def alpha191_168(self, data, param1=20, dependencies=['turnover_vol'], max_window=20):
        # -1*VOLUME/MEAN(VOLUME,20)
        return (data['turnover_vol'] / (data['turnover_vol'].rolling(window=param1, 
                                        min_periods=param1).mean())).iloc[-1] * (-1)


    def alpha191_169(self, data, param1=1, param2=1, param3=9, param4=12, param5=26, param6=10,
                   dependencies=['close_price'], max_window=48):
        # SMA(MEAN(DELAY(SMA(CLOSE-DELAY(CLOSE,1),9,1),1),12)-MEAN(DELAY(SMA(CLOSE-DELAY(CLOSE,1),9,1),1),26),10,1)
        part1 = (
            data['close_price'].diff(param1).ewm(adjust=False, alpha=float(param2) / 9, 
                                           min_periods=0, ignore_na=False).mean()).shift(param1)
        
        part2 = (part1.rolling(window=param4, min_periods=param4).mean() - part1.rolling(
            window=param5, min_periods=param5).mean()).ewm(adjust=False, alpha=float(param2) / param6, min_periods=0,
                                                           ignore_na=False).mean()
        return part2.iloc[-1]


    def alpha191_170(self, data, param1=1, param2=20, param3=5, param4=5,
                  dependencies=['close_price', 'turnover_vol', 'highest_price', 'vwap'], max_window=20):
        # ((RANK(1/CLOSE)*VOLUME)/MEAN(VOLUME,20))*(HIGH*RANK(HIGH-CLOSE)/(SUM(HIGH,5)/5))-RANK(VWAP-DELAY(VWAP,5))
        part1 = (float(param1) / data['close_price']).rank(axis=1, pct=True) * data['turnover_vol'] / (
            data['turnover_vol'].rolling(window=param2, min_periods=param2).mean())
        part2 = ((data['highest_price'] - data['close_price']).rank(axis=1, pct=True) * data['highest_price']) / (
                data['highest_price'].rolling(window=param3, min_periods=param3).sum() / float(param4))
        part3 = (data['vwap'].diff(param3)).rank(axis=1, pct=True)
        return (part1 * part2 - part3).iloc[-1]


    def alpha191_171(self, data, param1=5, param2=-1,
                  dependencies=['lowest_price', 'close_price', 'open_price', 'highest_price'], max_window=1):
        # (-1*(LOW-CLOSE)*(OPEN^5))/((CLOSE-HIGH)*(CLOSE^5))
        part1 = (data['lowest_price'] - data['close_price']) * (data['open_price'] ** param1) * (param2)
        part2 = (data['close_price'] - data['highest_price']) * (data['close_price'] ** param1)
        return (part1 / part2).iloc[-1]

    
    def alpha191_172(self, data, param1=14, param2=6,
                  dependencies=['highest_price', 'lowest_price', 'close_price'], max_window=30):
    # 即 DMI-ADX
    # MTR:=SUM(MAX(MAX(HIGH-LOW,ABS(HIGH-REF(CLOSE,1))),ABS(REF(CLOSE,1)-LOW)),N);
    # HD :=HIGH-REF(HIGH,1);
    # LD :=REF(LOW,1)-LOW;
    # DMP:=SUM(IF(HD>0&&HD>LD,HD,0),N);
    # DMM:=SUM(IF(LD>0&&LD>HD,LD,0),N);
    # PDI: DMP*100/MTR;
    # MDI: DMM*100/MTR;
    # ADX: MA(ABS(MDI-PDI)/(MDI+PDI)*100,M);
    # ADXR:(ADX+REF(ADX,M))/2;
    # ADXR 为另外一个因子alpha186

        n, m = param1, param2
        hdc = data['highest_price'] - data['close_price'].shift(1)
        ldc = data['close_price'].shift(1) - data['lowest_price']
        tr = np.maximum(np.maximum(data['highest_price'] - data['lowest_price'], hdc.abs()), ldc.abs())
        mtr = tr.rolling(window=n, min_periods=n).sum()

        hd = data['highest_price'] - data['highest_price'].shift(1)
        ld = data['lowest_price'].shift(1) - data['lowest_price']
        cond_dmp = (hd > 0) & (hd > ld)
        cond_dmm = (ld > 0) & (ld > hd)
        dmp = hd[cond_dmp].fillna(0.0).rolling(window=n, min_periods=n).sum()
        dmm = ld[cond_dmm].fillna(0.0).rolling(window=n, min_periods=n).sum()
        pdi = dmp * 100.0 / mtr
        mdi = dmm * 100.0 / mtr

        adx_var = (mdi - pdi).abs() / (mdi + pdi) * 100
        adx = adx_var.rolling(window=m, min_periods=m).mean()
    # adxr = (adx + adx.shift(m)) / 2.0
        return adx.iloc[-1]


    def alpha191_173(self, data, param1=13, param2=2, param3=2, param4=3, 
                  dependencies=['close_price'], max_window=39):
        # 3*SMA(CLOSE,13,2)-2*SMA(SMA(CLOSE,13,2),13,2)+SMA(SMA(SMA(LOG(CLOSE),13,2),13,2),13,2)
        den = data['close_price'].ewm(adjust=False, alpha=float(param2) / param1, min_periods=0, ignore_na=False).mean()
        part1 = param4 * den
        part2 = param3 * (den.ewm(adjust=False, alpha=float(param2) / param1, min_periods=0, 
                                  ignore_na=False).mean())
        part3 = ((np.log(data['close_price']).ewm(adjust=False, alpha=float(param2
                 ) / param1, min_periods=0, ignore_na=False).mean()).ewm(
                    adjust=False, alpha=float(param2) / param1, min_periods=0, ignore_na=False).mean()).ewm(
            adjust=False, alpha=float(param2) / param1, min_periods=0, ignore_na=False).mean()
        return (part1 - part2 + part3).iloc[-1]


    def alpha191_174(self, data, param1=20, param2=1, param3=0,
                      dependencies=['close_price'], max_window=41):
        # SMA((CLOSE>DELAY(CLOSE,1)?STD(CLOSE,20):0),20,1)
        part1 = data['close_price'].rolling(window=param1, min_periods=param1).std()
        part1[data['close_price'].diff(1) <= 0] = float(param3)
        return part1.ewm(adjust=False, alpha=float(param2) / param1, min_periods=0, ignore_na=False).mean().iloc[-1]


    def alpha191_175(self, data, param1=1, param2=6, 
                  dependencies=['lowest_price', 'highest_price', 'close_price'], max_window=7):
        # MEAN(MAX(MAX(HIGH-LOW,ABS(DELAY(CLOSE,1)-HIGH)),ABS(DELAY(CLOSE,1)-LOW)),6)
        alpha = np.maximum(data['highest_price'] - data['lowest_price'], 
                           abs(data['close_price'].shift(param1) - data['highest_price']))
        alpha = np.maximum(alpha, abs(data['close_price'].shift(param1) - data['lowest_price']))
        return alpha.rolling(window=param2, min_periods=param2).mean().iloc[-1]


    def alpha191_176(self, data, param1=12, param2=6, 
                  dependencies=['close_price', 'highest_price', 'lowest_price', 'turnover_vol'], max_window=18):
        # CORR(RANK((CLOSE-TSMIN(LOW,12))/(TSMAX(HIGH,12)-TSMIN(LOW,12))),RANK(VOLUME),6)
        part1 = ((data['close_price'] - data['lowest_price'].rolling(window=param1, min_periods=param1).min()) / (
            data['highest_price'].rolling(window=param1, min_periods=param1).max() - data['lowest_price'].rolling(window=param1,
            min_periods=param1).min())).rank(axis=1, pct=True)
        part2 = data['turnover_vol'].rank(axis=1, pct=True)
        return rolling_corr(part1, part2, param2).iloc[-1]


    def alpha191_177(self, data, param1=20, param2=5, dependencies=['highest_price'], max_window=20):
        # ((20-HIGHDAY(HIGH,20))/20)*100
        return (param1 - data['highest_price'].rolling(window=param1, min_periods=param1
              ).apply(lambda x: (param1-1) - x.argmax(axis=0))).iloc[-1] * float(param2)

    
    def alpha191_178(self, data, param1=1, dependencies=['close_price', 'turnover_vol'], max_window=2):
        # (CLOSE-DELAY(CLOSE,1))/DELAY(CLOSE,1)*VOLUME
        return (data['close_price'].pct_change(periods=param1) * data['turnover_vol']).iloc[-1]


    def alpha191_179(self, data, param1=4, param2=50, param3=12,
                  dependencies=['lowest_price', 'vwap', 'turnover_vol'], max_window=62):
        # RANK(CORR(VWAP,VOLUME,4))*RANK(CORR(RANK(LOW),RANK(MEAN(VOLUME,50)),12))
        part1 = rolling_corr(data['vwap'], data['turnover_vol'], param1).rank(axis=1, pct=True)
        vol = data['turnover_vol'].rolling(window=param2, min_periods=param2).mean().rank(axis=1, pct=True)
        part2 = rolling_corr(vol, data['lowest_price'].rank(axis=1, pct=True), param3).rank(axis=1,pct=True)
        return (part1 * part2).iloc[-1]


    def alpha191_180(self, data, param1=7, param2=20, param3=60, param4=-1,
                  dependencies=['turnover_vol', 'close_price'], max_window=68):
        # (MEAN(VOLUME,20)<VOLUME)?((-1*TSRANK(ABS(DELTA(CLOSE,7)),60))*SIGN(DELTA(CLOSE,7)):(-1*VOLUME))
        condition = data['turnover_vol'].rolling(window=param2, min_periods=param2).mean() < data['turnover_vol']
        alpha = abs(data['close_price'].diff(param1)).rolling(window=param3, min_periods=param3).apply(
            lambda x: stats.rankdata(x)[-1] / float(param3)) \
            * np.sign(data['close_price'].diff(param1)) * (param4)
        alpha[~condition] = param4 * data['turnover_vol'][~condition]
        return alpha.iloc[-1]


    def alpha191_181(self, data, param1=20, param2=2, param3=3, 
                  dependencies=['close_price'], max_window=40):
        # SUM(RET-MEAN(RET,20)-(BANCHMARK_INDEX_CLOSE-MEAN(BANCHMARK_INDEX_CLOSE,20))^2,20)/SUM((BANCHMARK_INDEX_CLOSE-MEAN(BANCHMARK_INDEX_CLOSE,20))^3)
        bm = data['close_price'].mean(axis=1)
        bm_mean = bm - bm.rolling(window=param1, min_periods=param1).mean()
        bm_mean = pd.DataFrame(
            data=np.repeat(bm_mean.values.reshape(len(bm_mean.values), 1), 
                           len(data['close_price'].columns), axis=1),
            index=data['close_price'].index, columns=data['close_price'].columns)
        ret = data['close_price'].pct_change(periods=1)
        part1 = (ret - ret.rolling(window=param1, min_periods=param1).mean() - bm_mean ** param2).rolling(window=param1,
                                                                                         min_periods=param1).sum()
        part2 = (bm_mean ** param3).rolling(window=param1, min_periods=param1).sum()
        return (part1 / part2).iloc[-1]


    def alpha191_182(self, data, param1=20, dependencies=['close_price', 'open_price'], max_window=20):
        # COUNT((CLOSE>OPEN & BANCHMARK_INDEX_CLOSE>BANCHMARK_INDEX_OPEN) OR (CLOSE<OPEN &BANCHMARK_INDEX_CLOSE<BANCHMARK_INDEX_OPEN),20)/20
        bm = data['close_price'].mean(axis=1) > data['open_price'].mean(axis=1)
        bm = pd.DataFrame(data=np.repeat(bm.values.reshape(len(bm.values), 1), 
                                         len(data['close_price'].columns), axis=1),
                      index=data['close_price'].index, columns=data['close_price'].columns)
        condition1 = np.logical_and(data['close_price'] > data['open_price'], bm)
        condition2 = np.logical_and(data['close_price'] < data['open_price'], ~bm)
        return np.logical_or(condition1, condition2).rolling(window=param1, min_periods=param1).mean().iloc[-1]


    def alpha191_183(self, data, param1=24, dependencies=['close_price'], max_window=72):
        # MAX(SUMAC(CLOSE-MEAN(CLOSE,24)))-MIN(SUMAC(CLOSE-MEAN(CLOSE,24)))/STD(CLOSE,24)
        close_ma = data['close_price'] - data['close_price'].rolling(window=param1, min_periods=param1).mean()
        sumac = close_ma.rolling(window=param1, min_periods=param1).sum()
        part1 = sumac.rolling(window=param1, min_periods=param1).max()
        part2 = sumac.rolling(window=param1, min_periods=param1).min()
        part3 = data['close_price'].rolling(window=param1, min_periods=param1).std()
        return (part1 - part2 / part3).iloc[-1]


    def alpha191_184(self, data, param1=1, param2=200,
                  dependencies=['close_price', 'open_price'], max_window=201):
        # RANK(CORR(DELAY(OPEN-CLOSE,1),CLOSE,200))+RANK(OPEN-CLOSE)
        part1 = rolling_corr((data['open_price'] - data['close_price']).shift(param1), 
                             data['close_price'], param2).rank(axis=1, pct=True)
        part2 = (data['open_price'] - data['close_price']).rank(axis=1, pct=True)
        return (part1 + part2).iloc[-1]

    
    def alpha191_185(self, data, param1=-1, param2=1, param3=2,
                  dependencies=['close_price', 'open_price'], max_window=1):
        # RANK(-1*(1-OPEN/CLOSE)^2)
        return (((float(param2) - data['open_price']/data['open_price']) ** param3) * (param1)).rank(axis=1, pct=True).iloc[-1]


    def alpha191_186(self, data, param1=14, param2=6, param3=2,
                  dependencies=['highest_price', 'lowest_price', 'close_price'], max_window=30):
    # 即 DMI-ADXR
    # MTR:=SUM(MAX(MAX(HIGH-LOW,ABS(HIGH-REF(CLOSE,1))),ABS(REF(CLOSE,1)-LOW)),N);
    # HD :=HIGH-REF(HIGH,1);
    # LD :=REF(LOW,1)-LOW;
    # DMP:=SUM(IF(HD>0&&HD>LD,HD,0),N);
    # DMM:=SUM(IF(LD>0&&LD>HD,LD,0),N);
    # PDI: DMP*100/MTR;
    # MDI: DMM*100/MTR;
    # ADX: MA(ABS(MDI-PDI)/(MDI+PDI)*100,M);
    # ADXR:(ADX+REF(ADX,M))/2;

        n, m = param1, param2
        hdc = data['highest_price'] - data['close_price'].shift(1)
        ldc = data['close_price'].shift(1) - data['lowest_price']
        tr = np.maximum(np.maximum(data['highest_price'] - data['lowest_price'], hdc.abs()), ldc.abs())
        mtr = tr.rolling(window=n, min_periods=n).sum()

        hd = data['highest_price'] - data['highest_price'].shift(1)
        ld = data['lowest_price'].shift(1) - data['lowest_price']
        cond_dmp = (hd > 0) & (hd > ld)
        cond_dmm = (ld > 0) & (ld > hd)
        dmp = hd[cond_dmp].fillna(0.0).rolling(window=n, min_periods=n).sum()
        dmm = ld[cond_dmm].fillna(0.0).rolling(window=n, min_periods=n).sum()
        pdi = dmp * 100.0 / mtr
        mdi = dmm * 100.0 / mtr

        adx_var = (mdi - pdi).abs() / (mdi + pdi) * 100
        adx = adx_var.rolling(window=m, min_periods=m).mean()
        adxr = (adx + adx.shift(m)) / float(param3)
        return adxr.iloc[-1]


    def alpha191_187(self, data, param1=1, param2=0, param3=20,
                  dependencies=['open_price', 'highest_price'], max_window=21):
        # SUM(OPEN<=DELAY(OPEN,1)?0:MAX(HIGH-OPEN,OPEN-DELAY(OPEN,1)),20)
        part1 = np.maximum(data['highest_price'] - data['open_price'], data['open_price'].diff(param1))
        part1[data['open_price'].diff(param1) <= param2] = 0.0
        return part1.rolling(window=param3, min_periods=param3).sum().iloc[-1]


    def alpha191_188(self, data, param1=11, param2=2, 
                  dependencies=['lowest_price', 'highest_price'], max_window=11):
        # ((HIGH-LOW–SMA(HIGH-LOW,11,2))/SMA(HIGH-LOW,11,2))*100
        sma = (data['highest_price'] - data['lowest_price']).ewm(adjust=False, alpha=float(param2)/param1, 
                                               min_periods=0, ignore_na=False).mean()
        return ((data['highest_price'] - data['lowest_price'] - sma) / sma).iloc[-1] * 100


    def alpha191_189(self, data, param1=6, param2=6, dependencies=['close_price'], max_window=12):
        # MEAN(ABS(CLOSE-MEAN(CLOSE,6)),6)
        var = abs(data['close_price'] - data['close_price'].rolling(window=param1, min_periods=param1).mean())
        return var.rolling(window=param2,min_periods=param2).mean().iloc[-1]


    def alpha191_190(self, data, param1=19, param2=20, param3=0.05, param4=1, param5=2,
                  dependencies=['close_price'], max_window=40):
    # LOG((COUNT(RET>((CLOSE/DELAY(CLOSE,19))^(1/20)-1),20)-1)
    # *SUMIF((RET-(CLOSE/DELAY(CLOSE,19))^(1/20)-1)^2,20,RET<(CLOSE/DELAY(CLOSE,19))^(1/20)-1)
    # /(COUNT(RET<(CLOSE/DELAY(CLOSE,19))^(1/20)-1,20)
    # *SUMIF((RET-((CLOSE/DELAY(CLOSE,19))^(1/20)-1))^2,20,RET>(CLOSE/DELAY(CLOSE,19))^(1/20)-1)))
        ret = data['close_price'].pct_change(periods=1)
        ret_19 = (data['close_price'] / data['close_price'].shift(param1)) ** param3 - float(param4)
        part1 = (ret > ret_19).rolling(window=param2, min_periods=param2).sum() - float(param4)
        part2 = (np.minimum(ret - ret_19, 0.0) ** param5).rolling(window=param2, min_periods=param2).sum()
        part3 = (ret < ret_19).rolling(window=param2, min_periods=param2).sum()
        part4 = (np.maximum(ret - ret_19, 0.0) ** param5).rolling(window=param2, min_periods=param2).sum()
        return np.log(part1 * part2 / part3 / part4).iloc[-1]


    def alpha191_191(self, data, param1=20, param2=5, param3=0.5,
                  dependencies=['turnover_vol', 'lowest_price', 'close_price', 'highest_price'], max_window=25):
        # CORR(MEAN(VOLUME,20),LOW,5)+(HIGH+LOW)/2-CLOSE
        part1 = rolling_corr(data['turnover_vol'].rolling(window=param1, min_periods=param1).mean(), 
                             data['lowest_price'], param2)
        return (part1 + data['highest_price'] * param3 + data['lowest_price'] * param3 - data['close_price']).iloc[-1]