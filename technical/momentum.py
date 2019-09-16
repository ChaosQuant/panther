# -*- coding: utf-8 -*-
import six,pdb,talib
import numpy as np
from utilities.singleton import Singleton

@six.add_metaclass(Singleton)
class Momentum(object):
    def __init__(self):
        __str__ = 'momentum'
        self.name = '动量指标'
        self.factor_type1 = '技术指标因子'
        self.factor_type2 = '动量指标'
        self.desciption = '主要用于跟踪并预测股价的发展趋势'

    def _TRIXXD(self, data, timeperiod):
        '''
        This is TRIXXD
        :param param1: double
        :return:1.EMA3 = EMA(EMA(EMA(close, N), N), N)；2. TRIX = EMA3(t) / EMA3(t-1) – 1
        '''
        close_price = data['close_price'].copy().fillna(method='ffill').fillna(0).T
        close_price_shift = data['close_price'].copy().fillna(method='ffill').fillna(0).shift(1).T
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

    def PM60D(self, data, dependencies=['close_price'], max_window=62):
        '''
         This is alpha191_1
         :name: 过去 60 天的价格动量
         :desc: 过去 60 天的价格动量=close[i]/close[i-60],注 1：若公司在过去的 6 天内有停牌，停牌日也计算在统计天数内；。注 2：若公司在今天停牌，不计算该因子的值；
        '''
        return self._PMXD(data, 60)

    def PM120D(self, data, dependencies=['close_price'], max_window=122):
        '''
         This is alpha191_1
         :name: 过去 120 天的价格动量
         :desc: 过去 120 天的价格动量=close[i]/close[i-120],注 1：若公司在过去的 120 天内有停牌，停牌日也计算在统计天数内；注 2：若公司在今天停牌，不计算该因子的值；
        '''
        return self._PMXD(data, 120)

    def PM250D(self, data, dependencies=['close_price'], max_window=252):
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

    def PMDif5D60D(self, data, dependencies=['close_price'], max_window=62):
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

    def RCI24D(self, data, dependencies=['close_price'], max_window=26):
        '''
         This is alpha191_1
         :name: 24 日变化率指数
         :desc: 24 日变化率指数（24-day Rate of Change），类似于动力指数。如果价格始终是上升的，则变化率指数始终在 100%线以上，且如果变化速度指数在向上发展时，说明价格上升的速度在加快。公式：RCI[t]=close[t]/close[t-N]
        '''
        return self._PMXD(data, 24)

    def ARC50D(self, data, dependencies=['close_price'], max_window=101):
        '''
         This is alpha191_1
         :name: 变化率指数均值
         :desc: 变化率指数均值 (Average Rate of Change)。股票的价格变化率 RC 指标的均值，用以判断前一段交易周期内股票的平均价格变化率。ARC=EMA(RC,N,1/N),其中RCt=close[t]/close[t-N], N=50, 1/N为指数移动平均加权系数。
        '''
        close_price = data['close_price']
        prev_close = close_price.shift(50)
        rc = close_price / prev_close
        rc = rc.copy().fillna(method='ffill').fillna(0).T
        def _ema(data):
            return talib.EMA(data, 50)[-1]
        return rc.apply(_ema, axis=1)
    
    def APBMA5D(self, data, dependencies=['close_price'], max_window=11):
        '''
         This is alpha191_1
         :name: 绝对偏差移动平均
         :desc: 变化率指数均值 (Average Rate of Change)。股票的价格变化率 RC 指标的均值，用以判断前一段交易周期内股票的平均价格变化率。ARC=EMA(RC,N,1/N),其中RCt=close[t]/close[t-N], N=50, 1/N为指数移动平均加权系数。
        '''
        close_price = data['close_price'].fillna(method='ffill').fillna(0).T
        def _ma(data):
            return talib.MA(data, 5)
        close_price_5ma = close_price.apply(_ma, axis=1)
        result = close_price - close_price_5ma
        return result.apply(_ma, axis=1).T.iloc[-1]

    def MA10Close(self, data, dependencies=['close_price'], max_window=11):
        '''
         This is alpha191_1
         :name: 均线价格比
         :desc: 均线价格比 (10-day moving average to close price ratio)。由于股票的成交价格有响起均线回归的趋势，计算均线价格比可以预测股票在未来周期的运动趋势。MA10Close = MA(close, N) / close
        '''
        close_price = data['close_price'].fillna(method='ffill').fillna(0).T
        def _ma(data):
            return talib.MA(data, 10)
        ma10 = close_price.apply(_ma, axis=1)
        return (ma10 / close_price).T.iloc[-1]

    def _BIASXD(self, data, param1, dependencies=['close_price']):
        close_price = data['close_price'].fillna(method='ffill').fillna(0).T
        def _ma(data):
            return talib.MA(data, param1)[-1]
        close_price_ma = close_price.apply(_ma, axis=1)
        return (close_price.T.iloc[-1] - close_price_ma) / close_price_ma

    def BIAS10D(self, data, dependencies=['close_price'], max_window=11):
        '''
         This is alpha191_1
         :name: 10日乖离率
         :desc: 乖离率 (10-day Bias Ratio/BIAS)，是移动平均原理派生的一项技术指标，表示股价偏离趋向指标斩百分比值。BIAS = (close / MA(close, N) - 1) * 100
        '''
        return self._BIASXD(data, 10)

    def BIAS20D(self, data, dependencies=['close_price'], max_window=21):
        '''
         This is alpha191_1
         :name: 20日乖离率
         :desc: 乖离率 (10-day Bias Ratio/BIAS)，是移动平均原理派生的一项技术指标，表示股价偏离趋向指标斩百分比值。BIAS = (close / MA(close, N) - 1) * 100
        '''
        return self._BIASXD(data, 20)


    def BIAS5D(self, data, dependencies=['close_price'], max_window=6):
        '''
         This is alpha191_1
         :name: 5日乖离率
         :desc: 乖离率 (10-day Bias Ratio/BIAS)，是移动平均原理派生的一项技术指标，表示股价偏离趋向指标斩百分比值。BIAS = (close / MA(close, N) - 1) * 100
        '''
        return self._BIASXD(data, 5)

    def BIAS60D(self, data, dependencies=['close_price'], max_window=62):
        '''
         This is alpha191_1
         :name: 61日乖离率
         :desc: 乖离率 (10-day Bias Ratio/BIAS)，是移动平均原理派生的一项技术指标，表示股价偏离趋向指标斩百分比值。BIAS = (close / MA(close, N) - 1) * 100
        '''
        return self._BIASXD(data, 61)

    def Fiftytwoweekhigh(self, data, dependencies=['close_price'], max_window=252):
        '''
         This is alpha191_1
         :name: 当前价格处于过去1 年股价的位置
         :desc: 当前价格处于过去1 年股价的位置(Price level during the pasted 52 weeks)。
        '''
        close_price = data['close_price']
        min_price = np.min(close_price)
        max_price = np.max(close_price)
        result = (close_price - min_price) / (max_price - min_price)
        return result.iloc[-1]

    def _ChgToXMAvg(self, data, dependencies=['close_price']):
        close_price = data['close_price']
        mean_price = close_price.mean()
        return ((close_price / mean_price) -1).iloc[-1]

    def ChgTo1MAvg(self, data, dependencies=['close_price'], max_window=20):
        '''
         This is alpha191_1
         :name: 当前股价除以过去 1个月股价均值再减 1
         :desc: 当前股价除以过去 1个月股价均值再减 1。公式：close/avg(close,20)-1
        '''
        return self._ChgToXMAvg(data)


    def ChgTo3MAvg(self, data, dependencies=['close_price'], max_window=60):
        '''
         This is alpha191_1
         :name: 当前股价除以过去 3个月股价均值再减 1
         :desc: 当前股价除以过去 3个月股价均值再减 1。公式：close/avg(close,60)-1
        '''
        return self._ChgToXMAvg(data)

    def ChgTo1YAvg(self, data, dependencies=['close_price'], max_window=250):
        '''
         This is alpha191_1
         :name: 当前股价除以过去 1年股价均值再减 1
         :desc: 当前股价除以过去 1年股价均值再减 1。公式：close/avg(close,250)-1
        '''
        return self._ChgToXMAvg(data)

    def DEA(self, data, dependencies=['close_price'], max_window=35):
        '''
         This is alpha191_1
         :name: DEA9D
         :desc: 计算 MACD 因子的中间变量 (Difference in Exponential Average（mediator in calculating MACD))。
        '''
        close_price = data['close_price'].fillna(method='ffill').fillna(0).T
        def _macd(data):
            macd, macdsignal, macdhist = talib.MACD(data, fastperiod=12, slowperiod=26, signalperiod=9)
            return macdsignal[-1]
        return close_price.apply(_macd, axis=1)

    def _EMVXD(self, data, param1 , dependencies=['highest_price','lowest_price','turnover_vol']):
        highest_price = data['highest_price'].fillna(method='ffill').fillna(0)
        lowest_price = data['lowest_price'].fillna(method='ffill').fillna(0)
        perv_highest = highest_price.shift(1)
        perv_lowest = lowest_price.shift(1)
        #(highest + lowest) / 2
        expression1 = (highest_price + lowest_price) / 2
        #(prev_highest + prev_lowest) / 2
        expression2 = (perv_highest + perv_lowest) /2
        #(highest – lowest) / volume
        expression3 = (highest_price  - lowest_price) / (data['turnover_vol'] / 100000000)
        expression4 = (expression1 - expression2) * expression3
        def _ema(data):
            return talib.EMA(data, param1)[-1]
        return expression4.fillna(method='ffill').fillna(0).T.apply(_ema, axis=1)

    def EMV14D(self, data, dependencies=['highest_price','lowest_price','turnover_vol'], max_window=15):
        '''
         This is alpha191_1
         :name: 14日简易波动指标
         :desc: 简易波动指标（14-days Ease of Movement Value）。 EMV 将价格与成交量的变化结合成一个波动指标来反映股价或指数的变动状况。由于股价的变化和成交量的变化都可以引发该指标数值的变动，EMV 实际上也是一个量价合成指标。成交量以亿为单位。
        '''
        return self._EMVXD(data, 14)

    def EMV6D(self, data, dependencies=['highest_price','lowest_price','turnover_vol'], max_window=7):
        '''
         This is alpha191_1
         :name: 6日简易波动指标
         :desc: 简易波动指标（14-days Ease of Movement Value）。 EMV 将价格与成交量的变化结合成一个波动指标来反映股价或指数的变动状况。由于股价的变化和成交量的变化都可以引发该指标数值的变动，EMV 实际上也是一个量价合成指标。成交量以亿为单位。
        '''
        return self._EMVXD(data, 6)
    
    
    def MACD12D26D(self, data, dependencies=['close_price'], max_window=35):
        '''
         This is alpha191_1
         :name: 平滑异同移动平均线
         :desc: 平滑异同移动平均线（Moving Average Convergence Divergence）,又称移动平均聚散指标。
        '''
        close_price = data['close_price'].fillna(method='ffill').fillna(0).T
        def _macd(data):
            macd, macdsignal, macdhist = talib.MACD(data, fastperiod=12, slowperiod=26, signalperiod=9)
            return macd[-1]
        return close_price.apply(_macd, axis=1)

    def MTM10D(self, data, dependencies=['close_price'], max_window=11):
        '''
         This is alpha191_1
         :name: 动量指标
         :desc: 动量指标（Momentom Index）。动量指数以分析股价波动的速度为目的，研究股价在波动过程中各种加速，减速，惯性作用以及股价由静到动或由动转静的现象。
        '''
        return data['close_price'].diff(10).iloc[-1]

    def _EMAXD(self, data, param1, dependencies=['close_price']):
        close_price = data['close_price'].fillna(method='ffill').fillna(0).T
        def _ema(data):
            return talib.EMA(data, param1)[-1]
        return close_price.apply(_ema, axis=1)

    def EMA10D(self, data, dependencies=['close_price'], max_window=11):
        '''
         This is alpha191_1
         :name: 10 日指数移动均线
         :desc: 10 日指数移动均线（10-day Exponential moving average）。取前 N 天的收益和当日的价格，当日价格除以（1+当日收益）得到前一日价格，依次计算得到前 N 日价格，并对前 N 日价格计算指数移动平均，即为当日的前复权价移动平。
        '''
        return self._EMAXD(data, 10)

    def EMA12D(self, data, dependencies=['close_price'], max_window=13):
        '''
         This is alpha191_1
         :name: 12 日指数移动均线
         :desc: 12 日指数移动均线（10-day Exponential moving average）。取前 N 天的收益和当日的价格，当日价格除以（1+当日收益）得到前一日价格，依次计算得到前 N 日价格，并对前 N 日价格计算指数移动平均，即为当日的前复权价移动平。
        '''
        return self._EMAXD(data, 12)

    def EMA120D(self, data, dependencies=['close_price'], max_window=121):
        '''
         This is alpha191_1
         :name: 120 日指数移动均线
         :desc: 120 日指数移动均线（10-day Exponential moving average）。取前 N 天的收益和当日的价格，当日价格除以（1+当日收益）得到前一日价格，依次计算得到前 N 日价格，并对前 N 日价格计算指数移动平均，即为当日的前复权价移动平。
        '''
        return self._EMAXD(data, 120)

    def EMA20D(self, data, dependencies=['close_price'], max_window=21):
        '''
         This is alpha191_1
         :name: 20 日指数移动均线
         :desc: 20 日指数移动均线（10-day Exponential moving average）。取前 N 天的收益和当日的价格，当日价格除以（1+当日收益）得到前一日价格，依次计算得到前 N 日价格，并对前 N 日价格计算指数移动平均，即为当日的前复权价移动平。
        '''
        return self._EMAXD(data, 20)

    def EMA26D(self, data, dependencies=['close_price'], max_window=27):
        '''
         This is alpha191_1
         :name: 26 日指数移动均线
         :desc: 26 日指数移动均线（10-day Exponential moving average）。取前 N 天的收益和当日的价格，当日价格除以（1+当日收益）得到前一日价格，依次计算得到前 N 日价格，并对前 N 日价格计算指数移动平均，即为当日的前复权价移动平。
        '''
        return self._EMAXD(data, 26)

    def EMA5D(self, data, dependencies=['close_price'], max_window=6):
        '''
         This is alpha191_1
         :name: 5 日指数移动均线
         :desc: 5 日指数移动均线（10-day Exponential moving average）。取前 N 天的收益和当日的价格，当日价格除以（1+当日收益）得到前一日价格，依次计算得到前 N 日价格，并对前 N 日价格计算指数移动平均，即为当日的前复权价移动平。
        '''
        return self._EMAXD(data, 5)

    def EMA60D(self, data, dependencies=['close_price'], max_window=61):
        '''
         This is alpha191_1
         :name: 60 日指数移动均线
         :desc: 60 日指数移动均线（10-day Exponential moving average）。取前 N 天的收益和当日的价格，当日价格除以（1+当日收益）得到前一日价格，依次计算得到前 N 日价格，并对前 N 日价格计算指数移动平均，即为当日的前复权价移动平。
        '''
        return self._EMAXD(data, 60)

    def _MAXD(self, data, param1, dependencies=['close_price']):
        close_price = data['close_price'].fillna(method='ffill').fillna(0).T
        def _ma(data):
            return talib.MA(data, param1)[-1]
        return close_price.apply(_ma, axis=1)

    def MA20D(self, data, dependencies=['close_price'], max_window=21):
        '''
         This is alpha191_1
         :name: 20 日移动均线
         :desc: 最近 120交易日的前复权价格的均值
        '''
        return self._MAXD(data, 20)

    def MA120D(self, data, dependencies=['close_price'], max_window=121):
        '''
         This is alpha191_1
         :name: 120 日移动均线
         :desc: 最近 120交易日的前复权价格的均值
        '''
        return self._MAXD(data, 120)

    def MA10D(self, data, dependencies=['close_price'], max_window=11):
        '''
         This is alpha191_1
         :name: 10 日移动均线
         :desc: 最近 10交易日的前复权价格的均值
        '''
        return self._MAXD(data, 10)

    def MA5D(self, data, dependencies=['close_price'], max_window=6):
        '''
         This is alpha191_1
         :name: 5 日移动均线
         :desc: 最近 5交易日的前复权价格的均值
        '''
        return self._MAXD(data, 5)

    def MA60D(self, data, dependencies=['close_price'], max_window=61):
        '''
         This is alpha191_1
         :name: 60 日移动均线
         :desc: 最近 60交易日的前复权价格的均值
        '''
        return self._MAXD(data, 60)

    def BBI(self, data, dependencies=['close_price'], max_window=25):
        '''
         This is alpha191_1
         :name: 多空指数
         :desc: 多空指数（Bull and Bear Index）。是一种将不同日数移动平均线加权平均之后的综合指标，属于均线型指标。
        '''
        return (self._MAXD(data, 3) + self._MAXD(data, 6) + self._MAXD(data, 12) + self._MAXD(data, 24)) / 4

    def _TEMAXD(self, data, param1, dependencies=['close_price']):
        close_price = data['close_price'].fillna(method='ffill').fillna(0).T
        def _tema(data):
            return talib.TEMA(data, param1)[-1]
        return close_price.apply(_tema, axis=1)

    def TEMA10D(self, data, dependencies=['close_price'], max_window=31):
        '''
         This is alpha191_1
         :name: 10 日三重指数移动平均线
         :desc: 10 日三重指数移动平均线（10-day Triple Exponential Moving Average）。取时间 N 内的收盘价分别计算其一至三重指数加权平均
        '''
        return self._TEMAXD(data, 10)

    def TEMA5D(self, data, dependencies=['close_price'], max_window=16):
        '''
         This is alpha191_1
         :name: 5 日三重指数移动平均线
         :desc: 5 日三重指数移动平均线（5-day Triple Exponential Moving Average）。取时间 N 内的收盘价分别计算其一至三重指数加权平均
        '''
        return self._TEMAXD(data, 5)
    
    def _CCIXD(self, data, param1, dependencies=['highest_price','lowest_price', 'close_price']):
        highest_price = data['highest_price']
        lowest_price = data['lowest_price']
        close_price = data['close_price']
        cp = close_price.stack().reset_index().rename(columns={0:'close_price'})
        hp = highest_price.stack().reset_index().rename(columns={0:'highest_price'})
        lp = lowest_price.stack().reset_index().rename(columns={0:'lowest_price'})
        data_sets = lp.merge(cp,on=['security_code','trade_date']).merge(
            hp,on=['security_code','trade_date']).sort_values(
            by=['trade_date','security_code'],ascending=True)
        def _cci(data):
            result = talib.CCI(data.highest_price.values,
                               data.lowest_price.values,
                               data.close_price.values,
                               timeperiod=param1)
            return result[-1]
        return data_sets.groupby('security_code').apply(_cci)
    
    def CCI10D(self, data, dependencies=['highest_price','lowest_price', 'close_price'], max_window=11):
        '''
        This is alpha191_1
        :name: 10 日顺势指标
        :desc: 10 日顺势指标(10-day Commodity Channel Index)，专门测量股价是否已超出常态分布范围。CCI 指标波动于正无 穷大到负无穷大之间，不会出现指标钝化现象，有利于投资者更好地研判行情，特别是那些短期内暴涨暴跌的非常态行情。
        '''
        return self._CCIXD(data, 10)
    
    
    def CCI20D(self, data, dependencies=['highest_price','lowest_price', 'close_price'], max_window=21):
        '''
        This is alpha191_1
        :name: 20 日顺势指标
        :desc: 20 日顺势指标(10-day Commodity Channel Index)，专门测量股价是否已超出常态分布范围。CCI 指标波动于正无 穷大到负无穷大之间，不会出现指标钝化现象，有利于投资者更好地研判行情，特别是那些短期内暴涨暴跌的非常态行情。
        '''
        return self._CCIXD(data, 20)
    
    def CCI5D(self, data, dependencies=['highest_price','lowest_price', 'close_price'], max_window=6):
        '''
        This is alpha191_1
        :name: 5 日顺势指标
        :desc: 5 日顺势指标(10-day Commodity Channel Index)，专门测量股价是否已超出常态分布范围。CCI 指标波动于正无 穷大到负无穷大之间，不会出现指标钝化现象，有利于投资者更好地研判行情，特别是那些短期内暴涨暴跌的非常态行情。
        '''
        return self._CCIXD(data, 5)
    
    def CCI88D(self, data, dependencies=['highest_price','lowest_price', 'close_price'], max_window=91):
        '''
        This is alpha191_1
        :name: 88 日顺势指标
        :desc: 88 日顺势指标(10-day Commodity Channel Index)，专门测量股价是否已超出常态分布范围。CCI 指标波动于正无 穷大到负无穷大之间，不会出现指标钝化现象，有利于投资者更好地研判行情，特别是那些短期内暴涨暴跌的非常态行情。
        '''
        return self._CCIXD(data, 88)
    
    def ADX14D(self, data, dependencies=['highest_price','lowest_price', 'close_price'],max_window=29):
        '''
         This is alpha191_1
         :name: 平均动向指数
         :desc: 平均动向指数 (Average directional index)，DMI 因子的构成部分。
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
        def _adx(data):
            result = talib.ADX(data.highest_price.values,
                               data.lowest_price.values,
                               data.close_price.values,
                               timeperiod=14)
            return result[-1]
        return data_sets.groupby('security_code').apply(_adx)
    
    
    def ADXR14D(self, data, dependencies=['highest_price','lowest_price', 'close_price'],max_window=43):
        '''
         This is alpha191_1
         :name: 相对平均动向指数
         :desc: 相对平均动向指数 (Relative average directional index)，DMI 因子的构成
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
        def _adxr(data):
            result = talib.ADXR(data.highest_price.values,
                               data.lowest_price.values,
                               data.close_price.values,
                               timeperiod=14)
            return result[-1]
        return data_sets.groupby('security_code').apply(_adxr)
    
    def UOS7D14D28D(self, data, dependencies=['highest_price','lowest_price', 'close_price'],max_window=30):
        '''
         This is alpha191_1
         :name: 终极指标
         :desc: 终极指标（Ultimate Oscillator）。现行使用的各种振荡指标，对于周期参数的选择相当敏感。不同市况、不同参数设定的振荡指标，产生的结果截然不同。因此，选择最佳的参数组合，成为使用振荡指标之前最重要的一道手续。
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
        def _ultosc(data):
            result = talib.ULTOSC(data.highest_price.values,
                               data.lowest_price.values,
                               data.close_price.values,
                               timeperiod1=7, timeperiod2=14, timeperiod3=28)
            return result[-1]
        return data_sets.groupby('security_code').apply(_ultosc)
    
    def ChkOsci3D10D(self, data, dependencies=['highest_price','lowest_price', 'close_price',
                                              'turnover_vol'],max_window=11):
        '''
         This is alpha191_1
         :name: 佳庆指标
         :desc: 佳庆指标(Chaikin Oscillator)。该指标基于 AD 曲线的指数移动均线而计算得到。
        '''
        highest_price = data['highest_price']
        lowest_price = data['lowest_price']
        close_price = data['close_price']
        turnover_vol = data['turnover_vol']
        cp = close_price.stack().reset_index().rename(columns={0:'close_price'})
        hp = highest_price.stack().reset_index().rename(columns={0:'highest_price'})
        lp = lowest_price.stack().reset_index().rename(columns={0:'lowest_price'})
        vol = turnover_vol.stack().reset_index().rename(columns={0:'turnover_vol'})
        data_sets = lp.merge(cp,on=['security_code','trade_date']).merge(
            hp,on=['security_code','trade_date']).merge(vol, on=['security_code','trade_date']).sort_values(
            by=['trade_date','security_code'],ascending=True)
        
        def _3adema(data):
            ad = talib.AD(data.highest_price.values,
                               data.lowest_price.values,
                               data.close_price,
                                 data.turnover_vol)
            result = talib.EMA(np.nan_to_num(ad),3)
            return result[-1]
        
        def _10adema(data):
            ad = talib.AD(data.highest_price.values,
                               data.lowest_price.values,
                               data.close_price,
                                 data.turnover_vol)
            result = talib.EMA(np.nan_to_num(ad),10)
            return result[-1]
        return data_sets.groupby('security_code').apply(_3adema) - data_sets.groupby('security_code').apply(_10adema)
    
    
    def ChkVol10D(self, data, dependencies=['highest_price','lowest_price'],max_window=21):
        '''
         This is alpha191_1
         :name: 佳庆离散指标
         :desc: 佳庆离散指标(Chaikin Volatility，简称 CVLT，VCI，CV)，又称“佳庆变异率指数”，是通过测量一段时间内价格 幅度平均值的变化来反映价格的离散程度。
        '''
        highest_price = data['highest_price']
        lowest_price = data['lowest_price']
        hp = highest_price.stack().reset_index().rename(columns={0:'highest_price'})
        lp = lowest_price.stack().reset_index().rename(columns={0:'lowest_price'})
        data_sets = lp.merge(hp,on=['security_code','trade_date']).sort_values(
            by=['trade_date','security_code'],ascending=True)
        
        prev_highest_price = data['highest_price'].shift(10)
        prev_lowest_price = data['lowest_price'].shift(10)
        prev_hp = prev_highest_price.stack().reset_index().rename(columns={0:'highest_price'})
        prev_lp = prev_lowest_price.stack().reset_index().rename(columns={0:'lowest_price'})
        prev_data_sets = prev_lp.merge(prev_hp,on=['security_code','trade_date']).sort_values(
            by=['trade_date','security_code'],ascending=True)
        
        def _10hlema(data):
            result = talib.EMA(data.highest_price.values - data.lowest_price.values, 10)
            return result[-1]
        
        hlema  = data_sets.groupby('security_code').apply(_10hlema)
        prev_hlema  = prev_data_sets.groupby('security_code').apply(_10hlema)
        return 100 * (hlema - prev_hlema) / prev_hlema
    
    def _MA10RegressCoeffX(self, data, param1, dependencies=['close_price']):
        close_price = data['close_price'].copy().fillna(method='ffill').fillna(0).T
        def _ma10(data):
            result = talib.MA(data, 10)
            b = result[-param1:]
            x = np.array([i for i in range(1,param1 + 1)])
            return np.linalg.lstsq(np.reshape(x,(-1,1)),np.reshape(b.values,(-1,1)))[0][0][-1]
        return close_price.apply(_ma10,axis=1)
         
        
    def MA10RegressCoeff12(self, data, dependencies=['close_price'], max_window=24):
        '''
        This is alpha191_1
        :name: 10 日价格平均线 12 日线性回归系数
        :desc: 10 日价格平均线 12 日线性回归系数 (regression coefficient of 10-day moving average (in predicting 12-day moving average))。
        '''
        return self._MA10RegressCoeffX(data, 12)
    
    def MA10RegressCoeff6(self, data, dependencies=['close_price'], max_window=17):
        '''
        This is alpha191_1
        :name: 10 日价格平均线 6 日线性回归系数
        :desc: 10 日价格平均线 6 日线性回归系数 (regression coefficient of 10-day moving average (in predicting 12-day moving average))。
        '''
        return self._MA10RegressCoeffX(data, 6)
    
    def _PLRCXD(self, data, param1, dependencies=['close_price']):
        close_price = data['close_price'].copy().fillna(method='ffill').fillna(0).T
        def _pl(data):
            b = data[-param1:]
            x = np.array([i for i in range(1,param1 + 1)])
            return np.linalg.lstsq(np.reshape(x,(-1,1)),np.reshape(b.values,(-1,1)))[0][0][-1]
        return close_price.apply(_pl,axis=1)
    
    def PLRC6D(self, data, dependencies=['close_price'], max_window=7):
        '''
        This is alpha191_1
        :name: 6日价格线性回归系数
        :desc: 价格线性回归系数(6-day Price Linear Regression Coefficient)
        '''
        return self._PLRCXD(data, 6)
    
    def PLRC12D(self, data, dependencies=['close_price'], max_window=13):
        '''
        This is alpha191_1
        :name: 12日价格线性回归系数
        :desc: 价格线性回归系数(12-day Price Linear Regression Coefficient)
        '''
        return self._PLRCXD(data, 12)
