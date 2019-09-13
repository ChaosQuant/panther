# -*- coding: utf-8 -*-
import six,pdb
import numpy as np
import pandas as pd
from utilities.singleton import Singleton
import talib

@six.add_metaclass(Singleton)
class PriceVolume(object):
    def __init__(self):
        __str__ = 'price_volume'
        self.name = '量价指标'
        self.factor_type1 = '技术指标因子'
        self.factor_type2 = '量价指标'
        self.desciption = '通过成交量与股价变动关系分析未来趋势'
        
    
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
        '''
        return self._PVTXD(data, 1).mean()
    
    
    def PVT6D(self, data, dependencies=['close_price','turnover_vol'], max_window=7):
        '''
        This is alpha191_1
        :name: 因子 PVT 的 6 日均值。
        :desc: 因子 PVT 的 6 日均值 (6-day average price and volume trend)。
        '''
        return self._PVTXD(data, 6).mean()
    
    
    def PVT12D(self, data, dependencies=['close_price','turnover_vol'], max_window=13):
        '''
        This is alpha191_1
        :name: 因子 PVT 的 12 日均值。
        :desc: 因子 PVT 的 12 日均值 (12-day average price and volume trend)。
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
        '''
        return self._ACDXD(data)
    
    def ACD20D(self, data, dependencies=['close_price','lowest_price', 'highest_price'], max_window=20):
        '''
        This is alpha191_1
        :name: 20日收集派发指标。
        :desc: 20日收集派发指标
        '''
        return self._ACDXD(data)