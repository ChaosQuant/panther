# -*- coding: utf-8 -*-
import six,pdb
import numpy as np
import pandas as pd
from utilities.singleton import Singleton
import talib

@six.add_metaclass(Singleton)
class FactorReversal(object):
    def __init__(self):
        __str__ = 'factor_momentum'
        self.name = '反转指标'
        self.factor_type1 = '技术指标因子'
        self.factor_type2 = '反转指标'
        self.description = '主要用于捕捉趋势的转折点'
        
        
    def _ROCXD(self, data, param1, dependencies=['close_price']):
        return ((data['close_price'].shift(param1) / data['close_price']).iloc[-1] - 1)* 100
    
    def ROC6D(self, data, param1=6, dependencies=['close_price'], max_window=7):
        '''
        This is alpha191_1
        :name: 6 日变动速率
        :desc: 6 日变动速率（6-day Price Rate of Change）。是一个动能指标，其以当日的收盘价和 N 天前的收盘价比较，通过计算股价某一段时间内收盘价变动的比例，应用价格的移动比较来测量价位动量，属于反趋向的指标之一。ROC[t]=(close[t]/close[t-N]-1)*100
        '''
        return self._ROCXD(data, param1)
    
    def ROC20D(self, data, param1=20, dependencies=['close_price'], max_window=21):
        '''
        This is alpha191_1
        :name: 20 日变动速率
        :desc: 20 日变动速率（6-day Price Rate of Change）。是一个动能指标，其以当日的收盘价和 N 天前的收盘价比较，通过计算股价某一段时间内收盘价变动的比例，应用价格的移动比较来测量价位动量，属于反趋向的指标之一。ROC[t]=(close[t]/close[t-N]-1)*100
        '''
        return self._ROCXD(data, param1)
    
    
    def CMO20D(self, data, param1=20, dependencies=['close_price'], max_window=21):
        '''
        This is alpha191_1
        :name: 钱德动量摆动指标
        :desc: 钱德动量摆动指标（Chande Momentum Osciliator）。由 Tushar Chande 发明，与其他动量指标摆动指标如相对强弱指标（RSI）和随机指标（KDJ）不同，钱德动量指标在计算公式的分子中采用上涨日和下跌日的数据。
        '''
        close_price = data['close_price'].copy().fillna(method='ffill').fillna(0).T
        def _cmod(data):
            return talib.CMO(data, timeperiod=param1)[-1]
        close_price['cmo20'] = close_price.apply(_cmod,axis=1)
        return close_price['cmo20']
    
    def Mass25D(self, data, param1=25, dependencies=['close_price','highest_price','lowest_price'], max_window=26):
        '''
        This is alpha191_1
        :name: 梅斯线
        :desc: 梅斯线（Mass Index）。本指标是 Donald Dorsey 累积股价波幅宽度之后所设计的震荡曲线。其最主要的作用，在于寻找飙涨股或者极度弱势股的重要趋势反转点。
        '''
        expression1 = (data['highest_price'] - data['lowest_price']).fillna(method='ffill').fillna(0).T
        def _ema(data):
            return talib.EMA(data, timeperiod=9)
        def _emahl(data):
            return talib.EMA(data, timeperiod=9)
        def _sum(data):
            return data[-param1:].sum()
        EMAHL = expression1.apply(_ema,axis=1)
        EMAEMAHL = EMAHL.fillna(method='ffill').fillna(0).apply(_emahl,axis=1)
        EMA_Ratio = EMAHL / EMAEMAHL
        EMA_Ratio['mass_index'] = EMA_Ratio.fillna(method='ffill').fillna(0).apply(_sum, axis=1)
        return EMA_Ratio['mass_index']
    
    def BollDown20D(self, data, param1=20, dependencies=['close_price'], max_window=21):
        '''
        This is alpha191_1
        :name: 下轨线(布林线)指标
        :desc: 下轨线（布林线）指标（Lower Bollinger Bands），它是研判股价运动趋势的一种中长期技术分析工具。计算方法：中轨线为 N 日的移动平均线，上轨线为中轨线+两倍标准差。计算取 N=20
        '''
        close_price = data['close_price'].copy().fillna(method='ffill').fillna(0).T
        def _bbands(data):
            upperband, middleband, lowerband = talib.BBANDS(data, timeperiod=param1)
            return lowerband[-1]
        close_price['lowerband'] = close_price.apply(_bbands,axis=1)
        return close_price['lowerband']
    
    def RSI12D(self, data, param1=12, dependencies=['close_price'], max_window=13):
        '''
        This is alpha191_1
        :name: 相对强弱指标
        :desc: 相对强度 RS = MA(U, N) / MA(D, N) , 其中N取12
        '''
        close_price = data['close_price'].copy().fillna(method='ffill').fillna(0).T
        def _rsi(data):
            return talib.RSI(data, timeperiod=param1)[-1]
        close_price['rsi'] = close_price.apply(_rsi,axis=1)
        return close_price['rsi']
    
    def KDJK9D(self, data, dependencies=['highest_price','lowest_price', 'close_price'], max_window=16):
        '''
        This is alpha191_1
        :name: 随机指标
        :desc: 随机指标 (K Stochastic Oscillator)。它综合了动量观念、强弱指标及移动平均线的优点，用来度量股价脱离价格正常范围的变异程度
        '''
        highest_price = data['highest_price']
        lowest_price = data['lowest_price']
        close_price = data['close_price']
        cp = close_price.stack().reset_index().rename(columns={0:'close_price'})
        hp = highest_price.stack().reset_index().rename(columns={0:'highest_price'})
        lp = lowest_price.stack().reset_index().rename(columns={0:'lowest_price'})
        data_sets = lp.merge(cp,on=['security_code','trade_date']).merge(
            hp,on=['security_code','trade_date']).sort_values(by=['trade_date','security_code'],ascending=True)
        
        def _kdj(data):
            k, d = talib.STOCH(data.highest_price.values,
                               data.lowest_price.values,
                               data.close_price,
                               fastk_period=9,
                               slowk_period=3,
                               slowd_period=3)
            return k.iloc[-1]
        return data_sets.groupby('security_code').apply(_kdj)
    
    def _MFIXD(self, data, dependencies=['highest_price','lowest_price', 'close_price','turnover_vol']):
        highest_price = data['highest_price']
        lowest_price = data['lowest_price']
        close_price = data['close_price']
        turnover_vol = data['turnover_vol']
        cp = close_price.stack().reset_index().rename(columns={0:'close_price'})
        hp = highest_price.stack().reset_index().rename(columns={0:'highest_price'})
        lp = lowest_price.stack().reset_index().rename(columns={0:'lowest_price'})
        vol = turnover_vol.stack().reset_index().rename(columns={0:'turnover_vol'})
        data_sets = lp.merge(cp,on=['security_code','trade_date']).merge(
            hp,on=['security_code','trade_date']).merge(vol,on=['security_code','trade_date']).sort_values(
            by=['trade_date','security_code'],ascending=True)
        def _mfi(data):
            result = talib.MFI(data.highest_price.values,
                               data.lowest_price.values,
                               data.close_price,data.turnover_vol,
                               timeperiod=14)
            return result.iloc[-1]
        return data_sets.groupby('security_code').apply(_mfi)
        
    def MFI14D(self, data, dependencies=['highest_price','lowest_price', 'close_price','turnover_vol'], max_window=15):
        '''
        This is alpha191_1
        :name: 14日资金流量指标
        :desc: 14日资金流量指标（Money Flow Index），该指标是通过反映股价变动的四个元素：上涨的天数、下跌的天数、成交量增加幅度、成交量减少幅度来研判量能的趋势，预测市场供求关系和买卖力道。
        '''
        return self._MFIXD(data)
    
    def MFI21D(self, data, dependencies=['highest_price','lowest_price', 'close_price','turnover_vol'], max_window=22):
        '''
        This is alpha191_1
        :name: 21日资金流量指标
        :desc: 21日资金流量指标（Money Flow Index），该指标是通过反映股价变动的四个元素：上涨的天数、下跌的天数、成交量增加幅度、成交量减少幅度来研判量能的趋势，预测市场供求关系和买卖力道。
        '''
        return self._MFIXD(data)
