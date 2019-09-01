# -*- coding: utf-8 -*-
import pdb,six,importlib
import pandas as pd
from PyFin.api import makeSchedule,BizDayConventions
from sqlalchemy import create_engine, select, and_, or_
from utilities.singleton import Singleton

#连接句柄
@six.add_metaclass(Singleton)
class SQLEngine(object):
    def __init__(self, url):
        self._engine = create_engine(url)
    
    def sql_engine(self):
        return self._engine


class FetchEngine(object):
    def __init__(self, name, url):
        self._name = name
        self._engine = SQLEngine(url)
    
    @classmethod
    def create_engine(cls, name):
        if name == 'rl':
            return FetchRLEngine
        elif name == 'dx':
            return FetchDXEngine
        
    def base(self, table_name, begin_date, end_date, freq = None):
        if freq is None:
            query = select([table_name]).where(
                and_(table_name.trade_date >= begin_date, table_name.trade_date <= end_date, ))
        else:
            rebalance_dates = makeSchedule(begin_date, end_date, freq, 'china.sse', BizDayConventions.Preceding)
            query = select([table_name]).where(table_name.trade_date.in_(rebalance_dates))
        return pd.read_sql(query, self._engine.sql_engine())
        
        
class FetchRLEngine(FetchEngine):
    def __init__(self):
        super(FetchRLEngine, self).__init__('rl','mysql+mysqlconnector://factor_edit:factor_edit_2019@db1.irongliang.com/vision')
    
    def market(self, begin_date, end_date, freq = None):
        table = importlib.import_module('data.rl_model').Market
        return self.base(table, begin_date, end_date, freq)
    
    def exposure(self, begin_date, end_date, freq = None):
        table = importlib.import_module('data.rl_model').Exposure
        return self.base(table, begin_date, end_date, freq)
    
class FetchDXEngine(FetchEngine):
    def __init__(self):
        super(FetchDXEngine, self).__init__('dx','postgresql+psycopg2://alpha:alpha@180.166.26.82:8889/alpha')
     
    def market(self, begin_date, end_date, freq = None):
        table = importlib.import_module('data.dx_model').Market
        return self.base(table, begin_date, end_date, freq)
    
    def exposure(self, begin_date, end_date, freq = None):
        table = importlib.import_module('data.dx_model').Exposure
        return self.base(table, begin_date, end_date, freq)


class EngineFactory():
    def create_engine(self, engine_class):
        return engine_class()
    
    def result(self, begin_date, end_date):
        raise NotImplementedError

class MarketFactory(EngineFactory):
    def __init__(self, engine_class):
        self._fetch_engine = self.create_engine(engine_class)
    
    def result(self, begin_date, end_date, freq=None):
        return self._fetch_engine.market(begin_date, end_date, freq)
    
class ExposureFactory(EngineFactory):
    def __init__(self, engine_class):
        self._fetch_engine = self.create_engine(engine_class)
        
    def result(self, begin_date, end_date, freq=None):
        return self._fetch_engine.exposure(begin_date, end_date, freq)

if  __name__=="__main__":
    market_factory = ExposureFactory(FetchDXEngine)
    begin_date = '2018-12-01'
    end_date = '2018-12-31'
    print(market_factory.result(begin_date, end_date))
