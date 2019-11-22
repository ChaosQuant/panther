# -*- coding: utf-8 -*-
import six,pdb
import numpy as np
import pandas as pd
from utilities.singleton import Singleton
import talib

@six.add_metaclass(Singleton)
class FactorPriceVolume(object):
    def __init__(self):
        __str__ = 'factor_price_volume'
        self.name = '量价指标'
        self.factor_type1 = '技术指标因子'
        self.factor_type2 = '量价指标'
        self.description = '通过成交量与股价变动关系分析未来趋势'
        
    
    def _PVTXD(self, data, param1=6, dependencies=['close_price','turnover_vol']):
        close_price = data['close_price']
        turnover_vol = data['turnover_vol']
        prev_close = close_price.shift(param1)
        result = (close_price - prev_close) / prev_close
        return result * turnover_vol
        
    def PVT1D(self, data, dependencies=['close_price','turnover_vol'], max_window=2):
        '''
        This is alpha191_1
        :name: 价量趋势
        :desc: 价量趋势(Price and Volume Trend)指标。把能量变化与价格趋势有机地联系到了一起，从而构成了量价趋势指标。
        :unit:
        :view_dimension:1
        '''
        return self._PVTXD(data, 1).mean()
    
    
    def PVT6D(self, data, dependencies=['close_price','turnover_vol'], max_window=7):
        '''
        This is alpha191_1
        :name: 因子 PVT 的 6 日均值。
        :desc: 因子 PVT 的 6 日均值 (6-day average price and volume trend)。
        :unit:
        :view_dimension:1
        '''
        return self._PVTXD(data, 6).mean()
    
    
    def PVT12D(self, data, dependencies=['close_price','turnover_vol'], max_window=13):
        '''
        This is alpha191_1
        :name: 因子 PVT 的 12 日均值。
        :desc: 因子 PVT 的 12 日均值 (12-day average price and volume trend)。
        :unit:
        :view_dimension:1
        '''
        return self._PVTXD(data, 12).mean()
    
    def _ACDXD(self, data, dependencies=['close_price','lowest_price', 'highest_price']):
        close_price = data['close_price']
        lowest_price = data['lowest_price']
        highest_price = data['highest_price']
        prev_close = data['close_price'].shift(1)
        buy =  close_price - np.minimum(lowest_price, close_price)
        sell =  close_price - np.maximum(highest_price, close_price)
        return buy.sum() + sell.sum()
    
    def ACD6D(self, data, dependencies=['close_price','lowest_price', 'highest_price'], max_window=6):
        '''
        This is alpha191_1
        :name: 6日收集派发指标
        :desc: 6日收集派发指标
        :unit:
        :view_dimension:1
        '''
        return self._ACDXD(data)
    
    def ACD20D(self, data, dependencies=['close_price','lowest_price', 'highest_price'], max_window=20):
        '''
        This is alpha191_1
        :name: 20日收集派发指标。
        :desc: 20日收集派发指标
        :unit:
        :view_dimension:1
        '''
        return self._ACDXD(data)
    
    def WILLR14D(self, data, dependencies=['highest_price','lowest_price', 'close_price'], max_window=15):
        '''
        This is alpha191_1
        :name: 威廉指标
        :desc: 表示是市场处于超买还是超卖状态。
        :unit:
        :view_dimension:1
        '''
        highest_price = data['highest_price']
        lowest_price = data['lowest_price']
        close_price = data['close_price']
        cp = close_price.stack().reset_index().rename(columns={0:'close_price'})
        hp = highest_price.stack().reset_index().rename(columns={0:'highest_price'})
        lp = lowest_price.stack().reset_index().rename(columns={0:'lowest_price'})
        data_sets = lp.merge(cp,on=['security_code','trade_date']).merge(
            hp,on=['security_code','trade_date']).sort_values(
            by=['trade_date','security_code'],ascending=True)
        def _willr(data):
            result = talib.WILLR(data.highest_price.values,
                               data.lowest_price.values,
                               data.close_price, timeperiod=14)
            return result.iloc[-1]
        return data_sets.groupby('security_code').apply(_willr)
    
    def _OBVXD(self, data, dependencies=['turnover_vol','close_price']):
        turnover_vol = data['turnover_vol']
        close_price = data['close_price']
        cp = close_price.stack().reset_index().rename(columns={0:'close_price'})
        vol = turnover_vol.stack().reset_index().rename(columns={0:'turnover_vol'})
        data_sets = cp.merge(vol,on=['security_code','trade_date']).sort_values(
            by=['trade_date','security_code'],ascending=True)
        def _obv(data):
            result = talib.OBV(data.close_price.values,
                               data.turnover_vol.values)
            return result[0]
        return data_sets.groupby('security_code').apply(_obv)
    
    
    def OBV1D(self, data, dependencies=['turnover_vol','close_price'], max_window=1):
        '''
        This is alpha191_1
        :name: 能量潮指标
        :desc: 以股市的成交量变化来衡量股市的推动力，从而研判股价的走势。
        :unit:
        :view_dimension:1
        '''
        return self._OBVXD(data)
    
    def OBV6D(self, data, dependencies=['turnover_vol','close_price'], max_window=6):
        '''
        This is alpha191_1
        :name: 6日能量潮指标
        :desc: 以股市的成交量变化来衡量股市的推动力，从而研判股价的走势。
        :unit:
        :view_dimension:1
        '''
        return self._OBVXD(data)
    
    
    def OBV20D(self, data, dependencies=['turnover_vol','close_price'], max_window=20):
        '''
        This is alpha191_1
        :name: 20日能量潮指标
        :desc: 以股市的成交量变化来衡量股市的推动力，从而研判股价的走势。
        :unit:
        :view_dimension:1
        '''
        return self._OBVXD(data)
