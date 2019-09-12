# -*- coding: utf-8 -*-
import six,pdb,talib
import numpy as np
from utilities.singleton import Singleton

@six.add_metaclass(Singleton)
class Momentum(object):
    def __init__(self):
        __str__ = 'momentum'
        self.name = '趋势指标'
        self.factor_type1 = '技术指标因子'
        self.factor_type2 = '趋势指标'
        self.desciption = '主要用于跟踪并预测股价的发展趋势'


    def _TRIXXD(self, data, timeperiod):
        '''
        This is TRIXXD
        :param param1: double
        :return:1.EMA3 = EMA(EMA(EMA(close, N), N), N)；2. TRIX = EMA3(t) / EMA3(t-1) – 1
        '''
        close_price = data['close_price'].copy().fillna(0).T
        close_price_shift = data['close_price'].copy().fillna(0).shift(1).T
        def _emaxd(data):
            expression1 = np.nan_to_num(talib.EMA(data.values, timeperiod))
            expression2 = np.nan_to_num(talib.EMA(expression1, timeperiod))
            expression3 = np.nan_to_num(talib.EMA(expression2, timeperiod))
            return expression3[-1]
        close_price['ema3'] = close_price.apply(_emaxd,axis=1)
        close_price_shift['ema3'] = close_price_shift.apply(_emaxd,axis=1)
        return close_price['ema3'] / close_price_shift['ema3'] - 1


    def TRIX5D(self, data, dependencies=['close_price'], max_window=18):
        '''
        This is alpha191_1
        :name: 5 日三重指数平滑移动平均指标变化率
        :desc: 5 日三重指数平滑移动平均指标变化率（5-day percent rate of change of triple exponetially smoothed moving average）。TRIX 根据移动平均线理论，对一条平均线进行三次平滑处理，再根据这条移动平均线的变动情况来预测股价的长期。计算方法：1. EMA3 = EMA(EMA(EMA(close, N), N), N)；2. TRIX = EMA3(t) / EMA3(t-1) – 1
        '''
        return self._TRIXXD(data, 5)


    def TRIX10D(self, data, dependencies=['close_price'], max_window=18):
        '''
        This is alpha191_1
        :name: 10 日三重指数平滑移动平均指标变化率
        :desc: 10 日三重指数平滑移动平均指标变化率（5-day percent rate of change of triple exponetially smoothed moving average）。TRIX 根据移动平均线理论，对一条平均线进行三次平滑处理，再根据这条移动平均线的变动情况来预测股价的长期。计算方法：1. EMA3 = EMA(EMA(EMA(close, N), N), N)；2. TRIX = EMA3(t) / EMA3(t-1) – 1
        '''
        return self._TRIXXD(data, 10)


    def _PMXD(self, data, param1, dependencies=['close_price']):
        '''
        This is PMXD
        :param param1: double
        :return:close[i]/close[i-5]
        '''
        return (data['close_price'].shift(param1) / data['close_price']).iloc[-1]

    def PM5D(self, data, dependencies=['close_price'], max_window=6):
        '''
        This is alpha191_1
        :name: 过去 5 天的价格动量
        :desc: 过去 5 天的价格动量=close[i]/close[i-5],注 1：若公司在过去的 5 天内有停牌，停牌日也计算在统计天数内；注 2：若公司在今天停牌，不计算该因子的值；
        '''
        return self._PMXD(data, 5)

    def PM10D(self, data, dependencies=['close_price'], max_window=11):
        '''
        This is alpha191_1
        :name: 过去 10 天的价格动量
        :desc: 过去 10 天的价格动量=close[i]/close[i-10],注 1：若公司在过去的 10 天内有停牌，停牌日也计算在统计天数内；注 2：若公司在今天停牌，不计算该因子的值；
        '''
        return self._PMXD(data, 10)

    def PM20D(self, data, dependencies=['close_price'], max_window=21):
        '''
         This is alpha191_1
         :name: 过去 20 天的价格动量
         :desc: 过去 20 天的价格动量=close[i]/close[i-20],注 1：若公司在过去的 20 天内有停牌，停牌日也计算在统计天数内；下同。注 2：若公司在今天停牌，不计算该因子的值；
        '''
        return self._PMXD(data, 20)

    def PM60D(self, data, dependencies=['close_price'], max_window=61):
        '''
         This is alpha191_1
         :name: 过去 60 天的价格动量
         :desc: 过去 60 天的价格动量=close[i]/close[i-60],注 1：若公司在过去的 6 天内有停牌，停牌日也计算在统计天数内；。注 2：若公司在今天停牌，不计算该因子的值；
        '''
        return self._PMXD(data, 60)

    def PM120D(self, data, dependencies=['close_price'], max_window=121):
        '''
         This is alpha191_1
         :name: 过去 120 天的价格动量
         :desc: 过去 120 天的价格动量=close[i]/close[i-120],注 1：若公司在过去的 120 天内有停牌，停牌日也计算在统计天数内；注 2：若公司在今天停牌，不计算该因子的值；
        '''
        return self._PMXD(data, 120)

    def PM250D(self, data, dependencies=['close_price'], max_window=251):
        '''
         This is alpha191_1
         :name: 过去 250 天的价格动量
         :desc: 过去 250 天的价格动量=close[i]/close[i-250],注 1：若公司在过去的 250 天内有停牌，停牌日也计算在统计天数内；注 2：若公司在今天停牌，不计算该因子的值；
        '''
        return self._PMXD(data, 250)

    def PMDif5D20D(self, data, dependencies=['close_price'], max_window=21):
        '''
         This is alpha191_1
         :name: 过去 5 天的价格动量减去过去 1 个月的价格动量
         :desc: 过去 5 天的价格动量减去过去 1 个月的价格动量, PMDif5D20D=PM5D- PM20D
        '''
        pm5d = self.PM5D(data)
        pm20d = self.PM20D(data)
        return pm5d - pm20d

    def PMDif5D60D(self, data, dependencies=['close_price'], max_window=61):
        '''
         This is alpha191_1
         :name: 过去 5 天的价格动量减去过去 3 个月的价格动量
         :desc: 过去 5 天的价格动量减去过去 3 个月的价格动量, PMDif5D60D=PM5D- PM60D
        '''
        pm5d = self.PM5D(data)
        pm20d = self.PM60D(data)
        return pm5d - pm20d

    def RCI12D(self, data, dependencies=['close_price'], max_window=13):
        '''
         This is alpha191_1
         :name: 12 日变化率指数
         :desc: 12 日变化率指数（12-day Rate of Change），类似于动力指数。如果价格始终是上升的，则变化率指数始终在 100%线以上，且如果变化速度指数在向上发展时，说明价格上升的速度在加快。公式：RCI[t]=close[t]/close[t-N]
        '''
        return self._PMXD(data, 12)

    def RCI24D(self, data, dependencies=['close_price'], max_window=25):
        '''
         This is alpha191_1
         :name: 24 日变化率指数
         :desc: 24 日变化率指数（24-day Rate of Change），类似于动力指数。如果价格始终是上升的，则变化率指数始终在 100%线以上，且如果变化速度指数在向上发展时，说明价格上升的速度在加快。公式：RCI[t]=close[t]/close[t-N]
        '''
        return self._PMXD(data, 24)
