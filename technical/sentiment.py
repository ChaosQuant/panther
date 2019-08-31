# -*- coding: utf-8 -*-


@six.add_metaclass(Singleton)
class Sentiment(object):
    def __init__(self):
        __str__ = 'sentiment'
        
    
    def _dm(self, data, param1, dependencies=['lowest_price','highest_price']):
        prev_lowest = data['lowest_price'].shift(param1)
        prev_highest = data['highest_price'].shift(param1)
        condition1 = data['highest_price'] +  data['lowest_price']
        condition2 = prev_lowest + prev_highest
        condition3 =  condition1 - condition2
        dmz = np.maximum(abs(data['highest_price'] - prev_highest),abs(data['lowest_price'] - prev_lowest))\
            if condition3 > 0 else 0
        
        dmf = np.maximum(abs(data['highest_price'] - prev_highest),abs(data['lowest_price'] - prev_lowest))\
            if condition3 < 0 else 0
        return dmz, dmf
        
    def diz(self, data, param1, dependencies=['lowest_price','highest_price']):
        dmz,dmf = self._dm(data, param1)
        diz = dmz.rolling(window=param1, min_periods=param1).sum() / (
                dmz.rolling(window=param1, min_periods=param1).sum() + dmf.rolling(window=param1, min_periods=param1).sum())
        return diz
    
    
    def dif(self, data, param1, dependencies=['lowest_price','highest_price']):
        dmz,dmf = self._dm(data, param1)
        diz = dmf.rolling(window=param1, min_periods=param1).sum() / (
                dmz.rolling(window=param1, min_periods=param1).sum() + dmf.rolling(window=param1, min_periods=param1).sum())
        return dif
        
    def ddi(self, data, param1, dependencies=['lowest_price','highest_price']):
        return self.diz(data, param1) - self.dif(data, param1)
    
    def bear_pw(self, data, param1, dependencies=['lowest_price','close_price']):
        return data['lowest_price'] - data['close_price'].rolling_mean(param1) 
    
    def bull_pw(self, data, param1, dependencies=['highest_price','close_price']):
        return data['highest_price'] - data['close_price'].rolling_mean(param1)
    
    def elder(self, data, param1, dependencies=['highest_price','lowest_price', 'close_price']):
        return (self.bear_pw(data, param1) - self.bull_pw(data, param1)) / data['close_price']
    
    def ar(self, data, param1, dependencies=['highest_price','lowest_price', 'open_price']):
        condition1 = data['highest_price'] - data['open_price']
        condition2 = data['open_price'] - data['lowest_price']
        return condition1.rolling(window=param1, min_periods=param1).sum() /\ 
                condition2.rolling(window=param1, min_periods=param1).sum()
    
    def br(self, data, param1, dependencies=['highest_price','lowest_price', 'close_price']):
        condition1 = data['highest_price'] - data['close_price'].shift(1)
        condition2 = data['close_price'] - data['lowest_price'].shift(1)
        condition3 = np.maximum(condition1, 0)
        condition4 = np.maximum(condition2, 0)
        return condition3.rolling(window=param1, min_periods=param1).sum() /\ 
                condition4.rolling(window=param1, min_periods=param1).sum()
    
    def arbr(self, data, param1, dependencies=['highest_price','lowest_price', 
                                               'close_price','open_price']):
        return self.ar(data, param1) - self.br(data, param1)
    
    def _sm(self, data, param1, dependencies=['highest_price','lowest_price', 
                                               'open_price']):
        prev_open = data['open_price'].shift(param1)
        condition1 = data['open_price'] > prev_open
        condition2 = np.maximum(data['highest_price'] - data['open_price'],
                               data['open_price'] - prev_open)
        condition3 = np.maximum(data['open_price'] - data['lowest_price'],
                               data['open_price'] - prev_open)
        dtm = np.where(condition1, condition2, 0)
        dbm = np.where(condition1, condition3, 0)
        return dtm, dbm
        
    def stm(self, data, param1, dependencies=['highest_price','lowest_price', 
                                               'open_price']):
        dtm,dbm = _sm(data, param1)
        return dtm.rolling(window=param1, min_periods=param1).sum()
    
    def sbm(self, data, param1, dependencies=['highest_price','lowest_price', 
                                               'open_price']):
        dtm,dbm = _sm(data, param1)
        return dbm.rolling(window=param1, min_periods=param1).sum()
    
    def adtm(self, data, param1, dependencies=['highest_price','lowest_price', 
                                               'open_price']):
        stm = self.stm(data, param1)
        sbm = self.sbm(data, param1)
        return (stm - sbm) / np.maximum(stm,sbm)
        
    def atr(self, data, param1, dependencies=['highest_price','lowest_price', 
                                               'close_price']):
        prev_close = data['close_price'].shift(param1)
        condition1 = np.abs(data['highest_price'] - prev_close) 
        condition1 = np.abs(data['lowest_price'] - prev_close)
        tr = np.maximum(np.maximum(data['highest_price'] - data['lowest_price'], condition1),
                        condition2)
        