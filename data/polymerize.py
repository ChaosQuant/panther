# -*- coding: utf-8 -*-

import pdb
import numpy as np
from .fech_data import MarketFactory, ExposureFactory, IndexFactory, IndustryFactory, FactorFactory, SecurityFactory, \
    IndexMarketFactory, FetchEngine


# 提取对应数据，并且进行预处理

# 适配
class Adaptation(object):
    def __init__(self, name):
        self._name = name

    def market(self, data):
        raise NotImplementedError

    def risk_exposure(self, data):
        raise NotImplementedError

    @classmethod
    def create_adaptation(cls, name):
        if name == 'rl':
            return RLAdaptation(name)
        elif name == 'dx':
            return DXAdaptation(name)


class DXAdaptation(Adaptation):
    def __init__(self, name):
        super(DXAdaptation, self).__init__(name)

    def market(self, data):
        return data.rename(columns={'preClosePrice': 'pre_close', 'openPrice': 'open_price',
                                    'highestPrice': 'highest_price', 'lowestPrice': 'lowest_price',
                                    'closePrice': 'close_price', 'turnoverVol': 'turnover_vol',
                                    'turnoverValue': 'turnover_value', 'accumAdjFactor': 'factor',
                                    'vwap': 'vwap', 'negMarketValue': 'neg_mkt_value',
                                    'marketValue': 'mkt_value', 'chgPct': 'chg_pct', 'isOpen': 'is_open',
                                    'PE': 'pe_ttm', 'PE1': 'pe', 'PB': 'pb'})

    def risk_exposure(self, data):
        return data

    def calc_adaptation(self, data):
        return data


class RLAdaptation(Adaptation):
    def __init__(self, name):
        super(RLAdaptation, self).__init__(name)

    def market(self, data):
        return data.rename(columns={'symbol': 'code', 'open': 'open_price', 'close': 'close_price',
                                    'high': 'highest_price', 'low': 'lowest_price', 'volume': 'turnover_vol',
                                    'money': 'turnover_value', 'change_pct': 'chg_pct', 'tot_mkt_cap': 'mkt_value',
                                    'factor': 'factor'})

    def risk_exposure(self, data):
        return data.rename(columns={'symbol': 'code'})

    # 缺少vwap
    def calc_adaptation(self, data):
        data['vwap'] = data['turnover_value'] / data['turnover_vol']
        return data


class DBPolymerize(object):
    def __init__(self, name):
        self._name = name
        self._factory_sets = {
            'market': MarketFactory(FetchEngine.create_engine(name)),
            'exposure': ExposureFactory(FetchEngine.create_engine(name)),
            'index': IndexFactory(FetchEngine.create_engine(name)),
            'industry': IndustryFactory(FetchEngine.create_engine(name)),
            'factor': FactorFactory(FetchEngine.create_engine(name)),
            'security': SecurityFactory(FetchEngine.create_engine(name)),
            'index_market': IndexMarketFactory(FetchEngine.create_engine(name))
        }
        self._adaptation = Adaptation.create_adaptation(name)
     
    def fetch_technical_data(self, begin_date, end_date, freq=None):
        #均值填充Nan
        market_data = self._factory_sets['market'].result(begin_date, end_date, freq)
        market_data['volume'] = np.where(market_data.volume.values == 0, np.nan,
                                         market_data.volume.values)
        market_data['money'] = np.where(market_data.money.values == 0, np.nan,
                                         market_data.money.values)
        market_data['volume'] = market_data['volume'].fillna(method='ffill')
        market_data['money'] = market_data['money'].fillna(method='ffill')
        market_data['open'] = np.where(market_data.open.values == 0, market_data.pre_close.values, market_data.open.values)
        market_data['close'] = np.where(market_data.close.values == 0, market_data.pre_close.values, market_data.close.values)
        market_data['high'] = np.where(market_data.high.values == 0, market_data.pre_close.values, market_data.high.values)
        market_data['low'] = np.where(market_data.low.values == 0, market_data.pre_close.values, market_data.low.values)
        #exposure_data = self._factory_sets['exposure'].result(begin_date, end_date, freq)
        market_data = self._adaptation.market(market_data)

        #exposure_data = self._adaptation.risk_exposure(exposure_data)
        #total_data = market_data.merge(exposure_data, on=['security_code','trade_date'])
        total_data = market_data

        return self._adaptation.calc_adaptation(total_data)

    def fetch_volatility_value_data(self, begin_date, end_date, freq=None):
        security_code_dict = {'000905': '2070000187', '000300': '2070000060'}
        market_data = self._factory_sets['market'].result(begin_date, end_date, freq)
        market_data = self._adaptation.market(market_data)
        index_data = self._factory_sets['index_market'].result([security_code_dict['000300']], begin_date, end_date, freq)
        return self._adaptation.calc_adaptation(market_data), index_data

    def fetch_performance_data(self, benchmark, begin_date, end_date, freq=None):
        #目前只有三个基准，故内码先固定
        security_code_dict = {'000905':'2070000187','000300':'2070000060'}

        sw_industry = ['801010', '801020', '801030', '801040', '801050',
                       '801080', '801110', '801120', '801130', '801150', '801160', '801170',
                       '801180', '801200', '801210', '801230', '801710', '801720', '801730',
                       '801740', '801750', '801760', '801770', '801790', '801880']

        # 对应的行业
        benchmark_industry_data = self._factory_sets['industry'].result(sw_industry, begin_date, end_date, freq).rename(
            columns={'isymbol': 'industry_code', 'iname': 'industry'})
        # 对应的权重
        benchmark_index_data = self._factory_sets['index'].result(benchmark, begin_date, end_date, freq).rename(
            columns={'isymbol': 'index_code', 'iname': 'index_name'})
        benchmark_data = benchmark_industry_data.merge(benchmark_index_data, on=['trade_date', 'symbol'])
        # 读取内码
        benchmark_data['code'] = benchmark_data['symbol'].apply(lambda x: str(x.split('.')[0]))
        security_code = self._factory_sets['security'].result(list(benchmark_data.code)).rename(
            columns={'symbol': 'code'})
        benchmark_data = benchmark_data.merge(security_code, on=['code']).drop(['code', 'symbol'], axis=1)

        index_data = self._factory_sets['index_market'].result(security_code_dict.values(), begin_date, end_date, freq)

        market_data = self._factory_sets['market'].result_code(list(set(security_code.security_code)), begin_date,
                                                               end_date, freq)

        # 读取因子数据
        factor_category = 'FactorReversal'
        factor_name = ['CMO20D', 'KDJK9D']
        factor_data = self._factory_sets['factor'].result(factor_category, begin_date, end_date, factor_name, freq)
        exposure_data = self._factory_sets['exposure'].result(begin_date, end_date, freq)

        cov_data = exposure_data

        return benchmark_data, index_data, market_data, factor_data, exposure_data


