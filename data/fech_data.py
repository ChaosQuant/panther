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
    
    def market_code(self, sets, begin_date, end_date, freq = None):
        table = importlib.import_module('data.rl_model').Market
        if freq is None:
            query = select([table]).where(
                and_(table.trade_date >= begin_date, table.trade_date <= end_date, 
                    table.security_code.in_(sets)))
        else:
            rebalance_dates = makeSchedule(begin_date, end_date, freq, 'china.sse', BizDayConventions.Preceding)
            query = select([table]).where(and_(table.trade_date.in_(rebalance_dates), 
                    table.security_code.in_(sets)))
        return pd.read_sql(query, self._engine.sql_engine()).drop(['id'], axis=1)
    
    def index_market(self, benchmark, begin_date, end_date, freq = None):
        table = importlib.import_module('data.rl_model').IndexMarket
        if freq is None:
            query = select([table]).where(
                and_(table.trade_date >= begin_date, table.trade_date <= end_date, 
                    table.security_code.in_(benchmark)))
        else:
            rebalance_dates = makeSchedule(begin_date, end_date, freq, 'china.sse', BizDayConventions.Preceding)
            query = select([table]).where(and_(table.trade_date.in_(rebalance_dates),table.security_code.in_(benchmark)))
        return pd.read_sql(query, self._engine.sql_engine()).drop(['id'], axis=1)
        
    def exposure(self, begin_date, end_date, freq = None):
        table = importlib.import_module('data.rl_model').Exposure
        return self.base(table, begin_date, end_date, freq)
    
    def index(self, benchmark, begin_date, end_date, freq = None):
        table = importlib.import_module('data.rl_model').Index
        if freq is None:
            query = select([table]).where(
                and_(table.trade_date >= begin_date, table.trade_date <= end_date, 
                    table.isymbol.in_(benchmark)))
        else:
            rebalance_dates = makeSchedule(begin_date, end_date, freq, 'china.sse', BizDayConventions.Preceding)
            query = select([table]).where(and_(table.trade_date.in_(rebalance_dates), 
                    table.isymbol.in_(benchmark)))
        return pd.read_sql(query, self._engine.sql_engine()).drop(['id'], axis=1)
    
    def industry(self, industry, begin_date, end_date, freq = None):
        table = importlib.import_module('data.rl_model').Industry
        if freq is None:
            query = select([table.trade_date,table.isymbol,
                        table.symbol,table.iname]).where(
                and_(table.trade_date >= begin_date, table.trade_date <= end_date, 
                     table.isymbol.in_(industry)))
        else:
            rebalance_dates = makeSchedule(begin_date, end_date, freq, 'china.sse', BizDayConventions.Preceding)
            query = select([table.trade_date,table.isymbol,
                        table.symbol,table.iname]).where(and_(
                        table.trade_date.in_(rebalance_dates),
                        table.isymbol.in_(industry)))
        return pd.read_sql(query, self._engine.sql_engine())
    
    def security(self, symbol_sets):
        table = importlib.import_module('data.rl_model').GLInternalCode
        query = select([table.security_code,table.symbol]).where(
                and_(table.symbol.in_(symbol_sets)))
        return pd.read_sql(query, self._engine.sql_engine())
        
    def factor(self, factor_category, begin_date, end_date, factor_name=None, freq = None):
        if factor_name is None:
            table = importlib.import_module('data.factor_model').__getattribute__(factor_category)
            return self.base(table, begin_date, end_date, freq)
        else:
            table = importlib.import_module('data.factor_model').__getattribute__(factor_category)
            key_sets = ['id','security_code','trade_date'] + factor_name
            db_columns = []
            for key in key_sets:
                db_columns.append(table.__dict__[key])
            if freq is None:
                query = select(db_columns).where(
                    and_(table.trade_date >= begin_date, table.trade_date <= end_date, ))
            else:
                rebalance_dates = makeSchedule(begin_date, end_date, freq, 'china.sse', BizDayConventions.Preceding)
                query = select(db_columns).where(table.trade_date.in_(rebalance_dates))
            return pd.read_sql(query, self._engine.sql_engine()).drop(['id'], axis=1)
    
    
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
    
    def result_code(self, sets, begin_date, end_date, freq=None):
        return self._fetch_engine.market_code(sets, begin_date, end_date, freq)
        
class ExposureFactory(EngineFactory):
    def __init__(self, engine_class):
        self._fetch_engine = self.create_engine(engine_class)
        
    def result(self, begin_date, end_date, freq=None):
        return self._fetch_engine.exposure(begin_date, end_date, freq)
    
class IndexFactory(EngineFactory):
    def __init__(self, engine_class):
        self._fetch_engine = self.create_engine(engine_class)
        
    def result(self, benchmark, begin_date, end_date, freq=None):
        return self._fetch_engine.index(benchmark, begin_date, end_date, freq)
    
class IndustryFactory(EngineFactory):
    def __init__(self, engine_class):
        self._fetch_engine = self.create_engine(engine_class)
        
    def result(self, benchmark, begin_date, end_date, freq=None):
        return self._fetch_engine.industry(benchmark, begin_date, end_date, freq)
    
class FactorFactory(EngineFactory):
    def __init__(self, engine_class):
        self._fetch_engine = self.create_engine(engine_class)
        
    def result(self, factor_category, begin_date, end_date, factor_name=None, freq=None):
        return self._fetch_engine.factor(factor_category, begin_date, end_date, factor_name, freq)
    
class SecurityFactory(EngineFactory):
    def __init__(self, engine_class):
        self._fetch_engine = self.create_engine(engine_class)
        
    def result(self, symbol_sets):
        return self._fetch_engine.security(symbol_sets)
    
class IndexMarketFactory(EngineFactory):
    def __init__(self, engine_class):
        self._fetch_engine = self.create_engine(engine_class)
    
    def result(self, benchmark, begin_date, end_date, freq=None):
        return self._fetch_engine.index_market(benchmark, begin_date, end_date, freq)
                      
if  __name__=="__main__":
    market_factory = ExposureFactory(FetchDXEngine)
    begin_date = '2018-12-01'
    end_date = '2018-12-31'
    print(market_factory.result(begin_date, end_date))
