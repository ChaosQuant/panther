# -*- coding: utf-8 -*-
import six,pdb
import numpy as np
import pandas as pd
from utilities.singleton import Singleton
import talib
@six.add_metaclass(Singleton)
class Sentiment(object):
    def __init__(self):
        __str__ = 'sentiment'
        self.name = '情绪指标'
        self.factor_type1 = '技术指标因子'
        self.factor_type2 = '情绪指标'
        self.desciption = '反应市场对标的的买卖情绪'

    def AroonD26D(self, data, dependencies=['highest_price','lowest_price'], max_window=30):
        '''
        This is alpha191_1
        :name: Aroon 因子的中间变量
        :desc: 计算 Aroon 因子的中间变量 (Mediator in calculating Aroon, )。Aroon (Aroon oscillator) 通过计算自价格达到近期最高值和最低值以来所经过的期间数，帮助投资者预测证券价格从 趋势到区域区域或反转的变化。Aroon 指标分为 Aroon、AroonUp 和 AroonDown3 个具体指标。
        '''
        highest_price = data['highest_price']
        lowest_price = data['lowest_price']
        hp = highest_price.stack().reset_index().rename(columns={0:'highest_price'})
        lp = lowest_price.stack().reset_index().rename(columns={0:'lowest_price'})
        data_sets = lp.merge(hp,on=['security_code','trade_date']).sort_values(
            by=['trade_date','security_code'],ascending=True)
        def _aroon(data):
            aroondown, aroonup = talib.AROON(data.highest_price.values,
                               data.lowest_price.values,
                               timeperiod=26)
            return aroondown[-1]
        return data_sets.groupby('security_code').apply(_aroon)
    
    def AroonU26D(self, data, dependencies=['highest_price','lowest_price'], max_window=30):
        '''
        This is alpha191_1
        :name: Aroon 因子的中间变量
        :desc: 计算 Aroon 因子的中间变量 (Mediator in calculating Aroon, )。Aroon (Aroon oscillator) 通过计算自价格达到近期最高值和最低值以来所经过的期间数，帮助投资者预测证券价格从 趋势到区域区域或反转的变化。Aroon 指标分为 Aroon、AroonUp 和 AroonDown3 个具体指标。
        '''
        highest_price = data['highest_price']
        lowest_price = data['lowest_price']
        hp = highest_price.stack().reset_index().rename(columns={0:'highest_price'})
        lp = lowest_price.stack().reset_index().rename(columns={0:'lowest_price'})
        data_sets = lp.merge(hp,on=['security_code','trade_date']).sort_values(
            by=['trade_date','security_code'],ascending=True)
        def _aroon(data):
            aroondown, aroonup = talib.AROON(data.highest_price.values,
                               data.lowest_price.values,
                               timeperiod=26)
            return aroonup[-1]
        return data_sets.groupby('security_code').apply(_aroon)
    
    def Aroon26D(self, data, dependencies=['highest_price','lowest_price'], max_window=30):
        '''
        This is alpha191_1
        :name: 阿隆振荡
        :desc: :计算 Aroon 因子的中间变量 (Mediator in calculating Aroon, )。Aroon (Aroon oscillator) 通过计算自价格达到近期最高值和最低值以来所经过的期间数，帮助投资者预测证券价格从 趋势到区域区域或反转的变化。Aroon 指标分为 Aroon、AroonUp 和 AroonDown3 个具体指标。
        '''
        highest_price = data['highest_price']
        lowest_price = data['lowest_price']
        hp = highest_price.stack().reset_index().rename(columns={0:'highest_price'})
        lp = lowest_price.stack().reset_index().rename(columns={0:'lowest_price'})
        data_sets = lp.merge(hp,on=['security_code','trade_date']).sort_values(
            by=['trade_date','security_code'],ascending=True)
        def _aroon(data):
            aroon = talib.AROONOSC(data.highest_price.values,
                               data.lowest_price.values,
                               timeperiod=26)
            return aroon[-1]
        return data_sets.groupby('security_code').apply(_aroon)
    
    def _dm(self, data, dependencies=['lowest_price','highest_price']):
        prev_lowest = data['lowest_price'].shift(1)
        prev_highest = data['highest_price'].shift(1)
        condition1 = data['highest_price'] +  data['lowest_price']
        condition2 = prev_lowest + prev_highest
        condition3 =  condition1 - condition2
        result1 = np.maximum(abs(data['highest_price'].T - prev_highest.T),abs(data['lowest_price'].T - prev_lowest.T))
        dmz = result1[condition3.T > 0].fillna(method='ffill')
        
        dmf = result1[condition3.T < 0].fillna(method='ffill')
        return dmz, dmf
        
    def DIZ13D(self, data, dependencies=['lowest_price','highest_price'], max_window=13):
        '''
         This is alpha191_1
         :name: DDI 因子的中间变量
         :desc: DDI 因子的中间变量
        '''
        dmz,dmf = self._dm(data)
        dif = dmz.T.sum() / (dmz.T.sum() + dmf.T.sum())
        return dif
    
    
    def DIF13D(self, data, dependencies=['lowest_price','highest_price'], max_window=13):
        '''
         This is alpha191_1
         :name: DDI 因子的中间变量
         :desc: DDI 因子的中间变量
        '''
        dmz,dmf = self._dm(data)
        diz = dmf.T.sum() / (dmz.T.sum() + dmf.T.sum())
        return diz
        
    def DDI13D(self, data, dependencies=['lowest_price','highest_price'], max_window=14):
        '''
         This is alpha191_1
         :name: 方向标准离差指数
         :desc: 方向标准离差指数 (Directional Divergence Index)。观察一段时间内股价相对于前一天向上波动和向下波动的比例，并对其进行移动平均分析。DDI 指标倾向于显示一种长波段趋势的方向改变。
        '''
        return self.DIZ13D(data) - self.DIF13D(data)
    
    
    def BearPw13D(self, data, dependencies=['lowest_price','close_price'], max_window=13):
        '''
         This is alpha191_1
         :name: 空头力道
         :desc: 空头力道(Mediator in calculating Elder, Bear power indicator)，是计算 Elder 因子的中间变量
        '''
        close_price = data['close_price'].fillna(method='ffill').T
        def _ema(data):
            return talib.EMA(data, 13)
        ema_result = close_price.apply(_ema, axis=1)
        return (data['lowest_price'] - close_price.apply(_ema, axis=1).T).iloc[-1] 
    
    def BullPw13D(self, data, dependencies=['highest_price','close_price'], max_window=13):
        '''
         This is alpha191_1
         :name: 多头力道
         :desc: 多头力道 (Mediator in calculating Elder, Bull power indicator)，是计算 Elder 因子的中间变量。
        '''
        close_price = data['close_price'].fillna(method='ffill').T
        def _ema(data):
            return talib.EMA(data, 13)
        ema_result = close_price.apply(_ema, axis=1)
        return (data['highest_price'] - close_price.apply(_ema, axis=1).T).iloc[-1]
    
    def Elder13D(self, data, dependencies=['highest_price','lowest_price', 'close_price'], max_window=13):
        '''
         This is alpha191_1
         :name: 艾达透视指标
         :desc: 艾达透视指标（Elder-ray Index）。交易者可以经由该指标，观察市场表面之下的多头与空头力道。
        '''
        return (self.BearPw13D(data) - self.BullPw13D(data)) / data['close_price'].iloc[-1]
    
    def AR26D(self, data, dependencies=['highest_price','lowest_price', 'open_price'], max_window=26):
        '''
         This is alpha191_1
         :name: 人气指标
         :desc: 人气指标 (price movement indicator, compare buying power with selling power to open price)。是以当天开市 价为基础，即以当天市价分别比较当天最高，最低价，通过一定时期内开市价在股价中的地位，反映市场买卖人气
        '''
        condition1 = data['highest_price'] - data['open_price']
        condition2 = data['open_price'] - data['lowest_price']
        return condition1.sum() / condition2.sum()
    
    def BR26D(self, data, dependencies=['highest_price','lowest_price', 'open_price','close_price'], max_window=26):
        '''
         This is alpha191_1
         :name: 意愿指标
         :desc: 意愿指标 (price movement indicator, compare buying power with selling power to last day close price)。是以 昨日收市价为基础，分别与当日最高，最低价相比，通过一定时期收市收在股价中的地位，反映市场买卖意愿的程度。
        '''
        condition1 = data['highest_price'] - data['close_price'].shift(1)
        condition2 = data['close_price'] - data['lowest_price'].shift(1)
        condition3 = np.maximum(condition1, 0)
        condition4 = np.maximum(condition2, 0)
        return condition3.sum() / condition4.sum()
    
    def ARBR26D(self, data, dependencies=['highest_price','lowest_price', 
                                               'close_price','open_price'], max_window=26):
        '''
         This is alpha191_1
         :name: 人气-意愿指标
         :desc:人气指标(AR)和意愿指标(BR)都是以分析历史股价为手段的技术指标 (Difference between AR and BR)。人气 指标是以当天开市价为基础，即以当天市价分别比较当天最高，最低价，通过一定时期内开市价在股价中的地位，反映市场买卖人 气;意愿指标是以昨日收市价为基础，分别与当日最高，最低价相比，通过一定时期收市收在股价中的地位，反映市场买卖意愿的程度。
        '''
        return self.AR26D(data) - self.BR26D(data)
    
    def SBM(self, data, dependencies=['highest_price','lowest_price','open_price'], max_window=20):
        '''
         This is alpha191_1
         :name: 计算 ADTM 因子的中间变量 
         :desc:计算 ADTM 因子的中间变量 (mediator in calculating ADTM)。
        '''
        prev_open = data['open_price'].shift(1)
        expression1 = np.maximum(data['highest_price'] - data['open_price'], 
                                data['open_price'] - prev_open)
        expression2 = np.maximum(data['open_price'] - data['lowest_price'], 
                                data['open_price'] - prev_open)
        DTM = expression1[data['open_price'] > prev_open].fillna(method='ffill')
        DBM = expression2[data['open_price'] < prev_open].fillna(method='ffill')
        STM = DTM.sum()
        SBM = DBM.sum()
        return SBM
    
    def STM(self, data, dependencies=['highest_price','lowest_price','open_price'], max_window=20):
        '''
         This is alpha191_1
         :name: 计算 ADTM 因子的中间变量 
         :desc: 计算 ADTM 因子的中间变量 (mediator in calculating ADTM)。
        '''
        prev_open = data['open_price'].shift(1)
        expression1 = np.maximum(data['highest_price'] - data['open_price'], 
                                data['open_price'] - prev_open)
        expression2 = np.maximum(data['open_price'] - data['lowest_price'], 
                                data['open_price'] - prev_open)
        DTM = expression1[data['open_price'] > prev_open].fillna(method='ffill')
        DBM = expression2[data['open_price'] < prev_open].fillna(method='ffill')
        STM = DTM.sum()
        SBM = DBM.sum()
        return STM
    
    def ADTM(self, data, dependencies=['highest_price','lowest_price','open_price'], max_window=20):
        '''
         This is alpha191_1
         :name: 动态买卖气指标 
         :desc:动态买卖气指标 (Moving dynamic indicator)，用开盘价的向上波动幅度和向下波动幅度的距离差值来描述人气高低 的指标
        '''
        prev_open = data['open_price'].shift(1)
        expression1 = np.maximum(data['highest_price'] - data['open_price'], 
                                data['open_price'] - prev_open)
        expression2 = np.maximum(data['open_price'] - data['lowest_price'], 
                                data['open_price'] - prev_open)
        DTM = expression1[data['open_price'] > prev_open].fillna(method='ffill')
        DBM = expression2[data['open_price'] < prev_open].fillna(method='ffill')
        STM = DTM.sum()
        SBM = DBM.sum()
        return (STM - SBM) / np.maximum(STM, SBM)
    
    def _ATRXD(self, data, param1, dependencies=['highest_price','lowest_price', 'close_price']):
        highest_price = data['highest_price']
        lowest_price = data['lowest_price']
        close_price = data['close_price']
        cp = close_price.stack().reset_index().rename(columns={0:'close_price'})
        hp = highest_price.stack().reset_index().rename(columns={0:'highest_price'})
        lp = lowest_price.stack().reset_index().rename(columns={0:'lowest_price'})
        data_sets = lp.merge(cp,on=['security_code','trade_date']).merge(
            hp,on=['security_code','trade_date']).sort_values(
            by=['trade_date','security_code'],ascending=True)
        def _atr(data):
            result = talib.ATR(data.highest_price.values,
                               data.lowest_price.values,
                               data.close_price,
                               timeperiod=param1)
            return result.iloc[-1]
        return data_sets.groupby('security_code').apply(_atr)
    
    
    def ATR14D(self, data, dependencies=['highest_price','lowest_price', 'close_price'], max_window=15):
        '''
        This is alpha191_1
        :name: 14日均幅指标
        :desc: 14 日均幅指标(14-day Average True Range)。取一定时间周期内的股价波动幅度的移动平均值，是显示市场变化率 的指标，主要用于研判买卖时机。
        '''
        return self._ATRXD(data, 14)
    
    def ATR6D(self, data, dependencies=['highest_price','lowest_price', 'close_price'], max_window=7):
        '''
        This is alpha191_1
        :name: 14日均幅指标
        :desc: 14 日均幅指标(14-day Average True Range)。取一定时间周期内的股价波动幅度的移动平均值，是显示市场变化率 的指标，主要用于研判买卖时机。
        '''
        return self._ATRXD(data, 6)
