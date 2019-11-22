# -*- coding: utf-8 -*-
import six,pdb,talib
import numpy as np
from utilities.singleton import Singleton

@six.add_metaclass(Singleton)
class FactorPowerVolume(object):
    def __init__(self):
        __str__ = 'factor_power_volume'
        self.name = '量能指标'
        self.factor_type1 = '技术指标因子'
        self.factor_type2 = '量能指标'
        self.description = '通过成交量与股价变动关系分析未来趋势'
        
    def VoT20D(self, data, dependencies=['turn_rate'], max_window=20):
        '''
        This is alpha191_1
        :name: 换手率相对波动率
        :desc: 换手率相对波动率(Volatility of daily turnover during the last 20 days)。
        :unit:
        :view_dimension:0.01
        '''
        return data['turn_rate'].std() / data['turn_rate'].mean()
    
    def _TVMAXD(self, data, param1, dependencies=['turnover_value']):
        turnover_value = data['turnover_value'].fillna(method='ffill').fillna(0).T
        def _ma(data):
            return talib.MA(data, param1)[-1]
        turnover_value['TVMAX'] = turnover_value.apply(_ma,axis=1)
        return turnover_value['TVMAX']
        
    def TVMA20D(self, data, dependencies=['turnover_value'], max_window=21):
        '''
        This is alpha191_1
        :name: 20日成交金额的移动平均值
        :desc: 20日成交金额的移动平均值(20-day Turnover Value Moving Average)
        :unit:
        :view_dimension:0.01
        '''
        return self._TVMAXD(data, 20)
    
    def TVMA6D(self, data, dependencies=['turnover_value'], max_window=7):
        '''
        This is alpha191_1
        :name: 20日成交金额的移动平均值
        :desc: 20日成交金额的移动平均值(6-day Turnover Value Moving Average)
        '''
        return self._TVMAXD(data, 6)
    
    def TVSD20D(self, data, dependencies=['turnover_value'], max_window=20):
        '''
        This is alpha191_1
        :name: 20日成交金额的标准差
        :desc: 20日成交金额的标准差(20-day Turnover Value STD)
        :unit:
        :view_dimension:0.01
        '''
        return data['turnover_value'].std()
    
    def TVSD6D(self, data, dependencies=['turnover_value'], max_window=6):
        '''
        This is alpha191_1
        :name: 6日成交金额的标准差
        :desc: 6日成交金额的标准差(6-day Turnover Value STD)
        :unit:
        :view_dimension:0.01
        '''
        return data['turnover_value'].std()
        
    def _VEMAXD(self, data, dependencies=['turnover_value']):
        turnover_value = data['turnover_value'].fillna(method='ffill').fillna(0).T
        def _ema(data):
            return talib.EMA(data, param1)[-1]
        turnover_value['EMA'] = turnover_value.apply(_ema,axis=1)
        return turnover_value['EMA']
    
    
    def VEMA10D(self, data, dependencies=['turnover_value'], max_window=11):
        '''
        This is alpha191_1
        :name: 10日成交量的指数移动平均
        :desc: 10 日指数移动均线(12-day Exponential moving average)。取前 N 天的收益和当日的价格，当日价格除以(1+ 当日收益)得到前一日价格，依次计算得到前 N 日价格，并对前 N 日价格计算指数移动平均，即为当日的前复权价移动平均。
        :unit:
        :view_dimension:0.01
        '''
        return self._TVMAXD(data, 10)
    
    def VEMA12D(self, data, dependencies=['turnover_value'], max_window=13):
        '''
        This is alpha191_1
        :name: 12日成交量的指数移动平均
        :desc: 12日指数移动均线(12-day Exponential moving average)。取前 N 天的收益和当日的价格，当日价格除以(1+ 当日收益)得到前一日价格，依次计算得到前 N 日价格，并对前 N 日价格计算指数移动平均，即为当日的前复权价移动平均。
        :unit:
        :view_dimension:0.01
        '''
        return self._TVMAXD(data, 12)
    
    def VEMA26D(self, data, dependencies=['turnover_value'], max_window=26):
        '''
        This is alpha191_1
        :name: 26日成交量的指数移动平均
        :desc: 26日指数移动均线(12-day Exponential moving average)。取前 N 天的收益和当日的价格，当日价格除以(1+ 当日收益)得到前一日价格，依次计算得到前 N 日价格，并对前 N 日价格计算指数移动平均，即为当日的前复权价移动平均。
        :unit:
        :view_dimension:0.01
        '''
        return self._TVMAXD(data, 26)
    
    def VEMA5D(self, data, dependencies=['turnover_value'], max_window=6):
        '''
        This is alpha191_1
        :name: 5日成交量的指数移动平均
        :desc: 5日指数移动均线(12-day Exponential moving average)。取前 N 天的收益和当日的价格，当日价格除以(1+ 当日收益)得到前一日价格，依次计算得到前 N 日价格，并对前 N 日价格计算指数移动平均，即为当日的前复权价移动平均。
        :unit:
        :view_dimension:0.01
        '''
        return self._TVMAXD(data, 5)
    
    def VolDIFF(self, data, dependencies=['turnover_vol'], max_window=27):
        '''
        This is alpha191_1
        :name: VMACD 因子的中间变量
        :desc: VMACD 因子的中间变量
        '''
        turnover_vol = data['turnover_vol'].fillna(method='ffill').fillna(0).T
        def _12ema(data):
            return talib.EMA(data, 13)[-1]
        def _26ema(data):
            return talib.EMA(data, 27)[-1]
        turnover_vol['EMA_12'] = turnover_vol.apply(_12ema,axis=1)
        turnover_vol['EMA_26'] = turnover_vol.apply(_26ema,axis=1)
        return turnover_vol['EMA_12'] - turnover_vol['EMA_26']
    
    def VOL10D(self, data, dependencies=['turn_rate'], max_window=10):
        '''
        This is alpha191_1
        :name: 10 日平均换手率
        :desc: 10 日平均换手率
        :unit:
        :view_dimension:0.01
        '''
        return data['turn_rate'].mean()
    
    def VOL120D(self, data, dependencies=['turn_rate'], max_window=120):
        '''
        This is alpha191_1
        :name: 120 日平均换手率
        :desc: 120 日平均换手率
        :unit:
        :view_dimension:0.01
        '''
        return data['turn_rate'].mean()
    
    def VOL20D(self, data, dependencies=['turn_rate'], max_window=20):
        '''
        This is alpha191_1
        :name: 20 日平均换手率
        :desc: 20 日平均换手率
        :unit:
        :view_dimension:0.01
        '''
        return data['turn_rate'].mean()
    
    def VOL240D(self, data, dependencies=['turn_rate'], max_window=240):
        '''
        This is alpha191_1
        :name: 240 日平均换手率
        :desc: 240 日平均换手率
        :unit:
        :view_dimension:0.01
        '''
        return data['turn_rate'].mean()
    
    def VOL5D(self, data, dependencies=['turn_rate'], max_window=5):
        '''
        This is alpha191_1
        :name: 5 日平均换手率
        :desc: 5 日平均换手率
        :unit:
        :view_dimension:0.01
        '''
        return data['turn_rate'].mean()
    
    def VOL60D(self, data, dependencies=['turn_rate'], max_window=60):
        '''
        This is alpha191_1
        :name: 60 日平均换手率
        :desc: 60 日平均换手率
        :unit:
        :view_dimension:0.01
        '''
        return data['turn_rate'].mean()
    
    
    def _VROCXD(self, data, param1, dependencies=['close_price']):
        close_price = data['close_price'].copy().fillna(method='ffill').fillna(0).T
        def _roc(data):
            return talib.ROC(data, timeperiod=param1)[-1]
        close_price['roc'] = close_price.apply(_roc,axis=1)
        return close_price['roc']
     
    def VROC12D(self, data, dependencies=['close_price'], max_window=13):
        '''
        This is alpha191_1
        :name: 12 日变动速率
        :desc: 12 日变动速率(6-day Price Rate of Change)。是一个动能指标，其以当日的收盘价和 N 天前的收盘价比较，通 过计算股价某一段时间内收盘价变动的比例，应用价格的移动比较来测量价位动量，属于反趋向的指标之一。
        :unit:
        :view_dimension:0.01
        '''
        return self._VROCXD(data, 12)
    
    def VROC6D(self, data, dependencies=['close_price'], max_window=7):
        '''
        This is alpha191_1
        :name: 6 日变动速率
        :desc: 6 日变动速率(6-day Price Rate of Change)。是一个动能指标，其以当日的收盘价和 N 天前的收盘价比较，通 过计算股价某一段时间内收盘价变动的比例，应用价格的移动比较来测量价位动量，属于反趋向的指标之一。
        :unit:
        :view_dimension:0.01
        '''
        return self._VROCXD(data, 6)
    
    def VROC10D(self, data, dependencies=['close_price'], max_window=11):
        '''
        This is alpha191_1
        :name: 10 日变动速率
        :desc: 10 日变动速率(6-day Price Rate of Change)。是一个动能指标，其以当日的收盘价和 N 天前的收盘价比较，通 过计算股价某一段时间内收盘价变动的比例，应用价格的移动比较来测量价位动量，属于反趋向的指标之一。
        :unit:
        :view_dimension:0.01
        '''
        return self._VROCXD(data, 10)
    
    def VROC20D(self, data, dependencies=['close_price'], max_window=21):
        '''
        This is alpha191_1
        :name: 10 日变动速率
        :desc: 10 日变动速率(6-day Price Rate of Change)。是一个动能指标，其以当日的收盘价和 N 天前的收盘价比较，通 过计算股价某一段时间内收盘价变动的比例，应用价格的移动比较来测量价位动量，属于反趋向的指标之一。
        :unit:
        :view_dimension:0.01
        '''
        return self._VROCXD(data, 20)
    
    def _DifVOLXD(self, data, param1, dependencies=['close_price']):
        return data['close_price'].iloc[-param1:].mean() - data['close_price'].mean()
    
    def DifVOL5D(self, data, dependencies=['close_price'], max_window=120):
        '''
        This is alpha191_1
        :name: 相对 5 日相对 120 日平均换手率
        :desc: 相对 5 日相对 120 日平均换手率(Difference between 5-day average turnover rate and 120 -day average turnover rate)。计算方法:DAVOL5 = VOL5 - VOL120
        :unit:
        :view_dimension:0.01
        '''
        return self._DifVOLXD(data, 5)
    
    def DifVOL10D(self, data, dependencies=['close_price'], max_window=120):
        '''
        This is alpha191_1
        :name: 相对 10 日相对 120 日平均换手率
        :desc: 相对 10 日相对 120 日平均换手率(Difference between 5-day average turnover rate and 120 -day average turnover rate)。计算方法:DAVOL5 = VOL10 - VOL120
        :unit:
        :view_dimension:0.01
        '''
        return self._DifVOLXD(data, 10)
    
    def DifVOL20D(self, data, dependencies=['close_price'], max_window=120):
        '''
        This is alpha191_1
        :name: 相对 20 日相对 120 日平均换手率
        :desc: 相对 20 日相对 120 日平均换手率(Difference between 5-day average turnover rate and 120 -day average turnover rate)。计算方法:DAVOL5 = VOL20 - VOL120
        :unit:
        :view_dimension:0.01
        '''
        return self._DifVOLXD(data, 20)
    
    def STOM(self, data, dependencies=['turn_rate'], max_window=21):
        '''
        This is alpha191_1
        :name: 月度换手率对数
        :desc: 月度换手率对数,使用近 1 个月的换手率的累加和的对数
        :unit:
        :view_dimension:0.01
        '''
        turn_rate = data['turn_rate']
        turn_rate_sum = turn_rate.sum()
        return np.log(turn_rate_sum)
    
    def STO3M(self, data, dependencies=['turn_rate'], max_window=63):
        '''
        This is alpha191_1
        :name: 季度度换手率对数
        :desc: 季度换手率对数,使用近 3 个月的换手率的累加和的对数
        :unit:
        :view_dimension:0.01
        '''
        return np.log((np.exp(data['turn_rate']).sum() / 3))
    
    def STO12M(self, data, dependencies=['turn_rate'], max_window=252):
        '''
        This is alpha191_1
        :name: 年换手率对数
        :desc: 年换手率对数,使用近 12 个月的换手率的累加和的对数
        :unit:
        :view_dimension:0.01
        '''
        return np.log((np.exp(data['turn_rate']).sum() / 12))
