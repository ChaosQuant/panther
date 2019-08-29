#!/usr/bin/env python
# coding=utf-8
import threading
import pandas as pd
from client.utillities.trade_date import TradeDate
from client.utillities.sync_util import SyncUtil
import os


class SignletionEngine(object):
    objs = {}
    objs_locker = threading.Lock()

    def __new__(cls, *agrs, **kv):
        if cls in cls.objs:
            return cls.objs[cls]['obj']
        cls.objs_locker.acquire()
        try:
            if cls in cls.objs:## double check locking
                return cls.objs[cls]['obj']
            obj = object.__new__(cls)
            cls.objs[cls] = {'obj': obj,'init':False}
            setattr(cls, '__init__', cls.decorate_init(cls.__init__))
            return cls.objs[cls]['obj']
        finally:
            cls.objs_locker.release()

    @classmethod
    def decorate_init(cls, fn):
        def init_wrap(*args):
            if not cls.objs[cls]['init']:
                fn(*args)
                cls.objs[cls]['init'] = True
            return

        return init_wrap

    def __init__(self):
        self._dir = os.environ['VISION_DIR']

    # entities 要获取的元素
    # filter  删选元素的条件
    def query(self, name, entities=None, filters={}):
        # 将元素和条件组合
        query_sets = {'entities': entities,
                      'filter':filters,
                      'name': name}
        return query_sets
    
    def add_factors(self, key, values, q = None):
        if q is None:
            q = {}
        if not 'filter' in q:
            q['filter'] = {}
        if not 'factors' in q['filter']:
            q['filter']['factors'] = {}
        q['filter']['factors'][key] = values
        return q
        
    def add_filter_trade(self, q, trades_date):
        q['filter']['trades_date'] = trades_date
        return q

    def add_filter_symbol(self, q, symbol_sets):
        q['filter']['symbol'] = symbol_sets
        return q

    def add_filter_isymbol(self, q, isymbol):
        q['filter']['isymbol'] = isymbol
        return q

    def add_filter_industry(self, q, industry):
        q['filter']['industry'] = industry
        return q

    def get_price(self, q):
        file_df_sets = None
        entities = q['entities']
        trades_date = q['filter']['trades_date']
        if 'symbol' in q['filter']:
            symbol_sets = q['filter']['symbol']
        else:
            symbol_sets = []
        name = q['name']
        for trade_date in trades_date:
            file_df = pd.read_csv(self._dir + name.lower() + '/' + str(trade_date).replace('-','') + '.csv',
                                 index_col=0, encoding='utf-8')
            if file_df_sets is None:
                file_df_sets = file_df
            else:
                file_df_sets = file_df_sets.append(file_df)
        #遍历股票
        new_file_df_sets = None
        for symbol in symbol_sets:
            if new_file_df_sets is None:
                new_file_df_sets = file_df_sets[file_df_sets['symbol'] == symbol]
            else:
                new_file_df_sets = new_file_df_sets.append(file_df_sets[file_df_sets['symbol'] == symbol])
        if entities == None:
            return new_file_df_sets
        else:
            return new_file_df_sets[entities]

    def get_industry(self, q, date=None):
        file_df_sets = None
        entities = q['entities']
        trades_date = q['filter']['trades_date']
        industry_sets = q['filter']['industry']
        name = q['name']
        for trade_date in trades_date:
            file_df = pd.read_csv(self._dir + name.lower() + '/' + str(trade_date).replace('-','') + '.csv',
                                     index_col=0,encoding='utf-8')
            if file_df_sets is None:
                file_df_sets = file_df
            else:
                file_df_sets = file_df_sets.append(file_df)
        # file_df_sets = file_df_sets[file_df_sets['isymbol'] == q['filter']['isymbol']]
        # 遍历指数
        new_file_df_sets = None
        for industry in industry_sets:
            if new_file_df_sets is None:
                new_file_df_sets = file_df_sets[file_df_sets['isymbol'] == int(industry)]
            else:
                new_file_df_sets = new_file_df_sets.append(file_df_sets[file_df_sets['isymbol'] == int(industry)])

        # 选取对应元素
        new_file_df_sets = new_file_df_sets[entities]
        return new_file_df_sets

    def get_index(self, q, date = None):
        file_df_sets = None
        entities = q['entities']
        trades_date = q['filter']['trades_date']
        name = q['name']
        for trade_date in trades_date:
            file_df = pd.read_csv(self._dir + name.lower() + '/' + str(trade_date).replace('-','') + '.csv',
                                     index_col=0,encoding='utf-8')
            if file_df_sets is None:
                file_df_sets = file_df
            else:
                file_df_sets = file_df_sets.append(file_df)
        file_df_sets = file_df_sets[file_df_sets['isymbol'] == q['filter']['isymbol']]
        # 选取对应元素
        file_df_sets = file_df_sets[entities]
        return file_df_sets

    def get_report(self, q, date = None):
        file_df_sets = None
        entities = q['entities']
        trades_date = q['filter']['trades_date']
        name = q['name']
        for trade_date in trades_date:
            file_df = pd.read_csv(self._dir + '/report/' + name.lower() + '/' + str(trade_date).replace('-','') + '.csv',
                                     index_col=0,encoding='utf-8')
            if file_df_sets is None:
                file_df_sets = file_df
            else:
                file_df_sets = file_df_sets.append(file_df)
        # 选取对应元素
        file_df_sets = file_df_sets[entities]
        return file_df_sets

    def get_fundamentals(self, q, date=None, stat_date=None):
        file_df_sets = None
        entities = q['entities']
        trades_date = q['filter']['trades_date']
        if 'symbol' in q['filter']:
            universe = q['filter']['symbol']
        else:
            universe = []
        name = q['name']
        for trade_date in trades_date:
            file_df = pd.read_csv(self._dir + name.lower() + '/' + str(trade_date).replace('-', '') + '.csv',
                                  index_col=0, encoding='utf-8')
            if file_df_sets is None:
                file_df_sets = file_df
            else:
                file_df_sets = file_df_sets.append(file_df)
        new_file_df_sets = None
        if len(universe) == 0:
            new_file_df_sets = file_df_sets
        else:
            for symbol in universe:
                if new_file_df_sets is None:
                    new_file_df_sets = file_df_sets[file_df_sets['symbol'] == symbol]
                else:
                    new_file_df_sets = new_file_df_sets.append(file_df_sets[file_df_sets['symbol'] == symbol])

        # 选取对应元素
        new_file_df_sets = new_file_df_sets[entities]
        return new_file_df_sets

    def get_sk_history_price(self, universe, end_date, count, entities=None):
        #读取交易日
        file_df_list = []
        trade_date = TradeDate()
        columns_list = None
        trade_date_sets = trade_date.trade_date_sets_range(end_date, count, 1)
        for trade_date in trade_date_sets:
            file_df = pd.read_csv(self._dir + 'sk_daily_price' + '/' + str(trade_date).replace('-','') + '.csv',
                                  index_col=0, encoding='utf-8')
            columns_list = file_df.columns
            file_df_list = file_df_list + file_df.values.tolist()
        file_df_sets = pd.DataFrame(file_df_list, columns=columns_list)
        new_file_df_sets = None
        if len(universe) == 0:
            new_file_df_sets = file_df_sets
        else:
            for symbol in universe:
                if new_file_df_sets is None:
                    new_file_df_sets = file_df_sets[file_df_sets['symbol'] == symbol]
                else:
                    new_file_df_sets = new_file_df_sets.append(file_df_sets[file_df_sets['symbol'] == symbol])
        if entities == None:
            return new_file_df_sets.drop(['id','ndeals','avg_vol_pd','avg_va_pd'],axis=1)
        else:
            return new_file_df_sets[entities]

    def get_index_history_price(self, universe, end_date, count, entities=None):
        #读取交易日
        file_df_list = []
        trade_date = TradeDate()
        columns_list = None
        trade_date_sets = trade_date.trade_date_sets_range(end_date, count, 1)
        for trade_date in trade_date_sets:
            file_df = pd.read_csv(self._dir + 'index_daily_price/' + '/' + str(trade_date).replace('-','') + '.csv',
                                  index_col=0, encoding='utf-8')
            columns_list = file_df.columns
            file_df_list = file_df_list + file_df.values.tolist()
        file_df_sets = pd.DataFrame(file_df_list,columns = columns_list)
        new_file_df_sets = None
        if len(universe) == 0:
            new_file_df_sets = file_df_sets
        else:
            for symbol in universe:
                if new_file_df_sets is None:
                    new_file_df_sets = file_df_sets[file_df_sets['symbol'] == symbol]
                else:
                    new_file_df_sets = new_file_df_sets.append(file_df_sets[file_df_sets['symbol'] == symbol])
        # if entities == None:
        #    return new_file_df_sets.drop(['id','avg_price','ndeals','open_interest','turnover'],axis=1)
        # else:
        return new_file_df_sets[entities]

    def get_factor_value(self, name, universe, end_date, count, entities=None):
        #读取交易日
        file_df_list = []
        trade_date = TradeDate()
        columns_list = None
        trade_date_sets = trade_date.trade_date_sets_range(end_date, count, 1)
        for trade_date in trade_date_sets:
            file_df = pd.read_csv(self._dir + '/factor/'+ 'factor_'+name + '/' + str(trade_date).replace('-','') + '.csv',
                         index_col=0, encoding='utf-8')
            columns_list = file_df.columns
            file_df_list = file_df_list + file_df.values.tolist()
        file_df_sets = pd.DataFrame(file_df_list,columns = columns_list)
        new_file_df_sets = None
        if len(universe) == 0:
            new_file_df_sets = file_df_sets
        else:
            for symbol in universe:
                if new_file_df_sets is None:
                    new_file_df_sets = file_df_sets[file_df_sets['symbol'] == symbol]
                else:
                    new_file_df_sets = new_file_df_sets.append(file_df_sets[file_df_sets['symbol'] == symbol])
        if entities == None:
            return new_file_df_sets.drop(['id'],axis=1)
        else:
            return new_file_df_sets[entities]
     
    def ttm_fundamental(self, q, year=1):
        if 'factors' in q['filter']:
            factors = q['filter']['factors']
        else:
            return None
        trades_date = q['filter']['trades_date'][0]
        if 'symbol' in q['filter']:
            universe = q['filter']['symbol']
        else:
            universe = []
        sync_util = SyncUtil()
        report_date_list = sync_util.ttm_report_date_by_year(trades_date, year)
        new_fundamental = None
        for report_date in report_date_list:
            #读取指定元素所有数据
            ttm_fundamental = None
            for key, factor in factors.items():
                fundamental_data = get_report(add_filter_trade(query(key,factor),[report_date]))
                fundamental_data.set_index('symbol',inplace=True)
                if ttm_fundamental is None:
                    ttm_fundamental = fundamental_data
                else:
                    ttm_fundamental = pd.merge(fundamental_data, ttm_fundamental, left_index=True, right_index=True)
             # 计算ttm
            if new_fundamental is None:
                new_fundamental = ttm_fundamental
            else:
                ttm_stock_sets = list(set(new_fundamental.index) & set(ttm_fundamental.index))
                new_fundamental.loc[ttm_stock_sets] += ttm_fundamental.loc[ttm_stock_sets]
                new_fundamental = new_fundamental.loc[ttm_stock_sets]
        if len(universe) == 0:
            return new_fundamental
        else:
            return new_fundamental.loc[universe]
    
    def ttm_fundamental_sets(self, q, year=1):
        if 'factors' in q['filter']:
            factors = q['filter']['factors']
        else:
            return None
        trades_date = q['filter']['trades_date'][0]
        if 'symbol' in q['filter']:
            universe = q['filter']['symbol']
        else:
            universe = []
        sync_util = SyncUtil()
        report_date_list = sync_util.ttm_report_date_by_year(trades_date, year)
        new_fundamental = None
        for report_date in report_date_list:
            #读取指定元素所有数据
            ttm_fundamental = None
            for key, factor in factors.items():
                fundamental_data = get_report(add_filter_trade(query(key,factor),[report_date]))
                fundamental_data.set_index('symbol',inplace=True)
                if ttm_fundamental is None:
                    ttm_fundamental = fundamental_data
                else:
                    ttm_fundamental = pd.merge(fundamental_data, ttm_fundamental, left_index=True, right_index=True)
            ttm_fundamental['report_date'] = report_date
            if new_fundamental is None:
                new_fundamental = ttm_fundamental
            else:
                new_fundamental = new_fundamental.append(ttm_fundamental)
        if len(universe) == 0:
            return new_fundamental
        else:
            return new_fundamental.loc[universe]
        
        

query = SignletionEngine().query
get_fundamentals = SignletionEngine().get_fundamentals
add_filter_trade = SignletionEngine().add_filter_trade
add_filter_isymbol = SignletionEngine().add_filter_isymbol
add_filter_symbol = SignletionEngine().add_filter_symbol
add_filter_industry = SignletionEngine().add_filter_industry
get_report = SignletionEngine().get_report
get_index = SignletionEngine().get_index
get_industry = SignletionEngine().get_industry
get_price = SignletionEngine().get_price
get_sk_history_price = SignletionEngine().get_sk_history_price
get_index_history_price = SignletionEngine().get_index_history_price
get_factor_value = SignletionEngine().get_factor_value

ttm_fundamental = SignletionEngine().ttm_fundamental
# latest_fundamental = SignletionEngine().latest_fundamental

ttm_fundamental_sets = SignletionEngine().ttm_fundamental_sets
# latest_fundamental_sets = SignletionEngine().latest_fundamental_sets

if __name__ == "__main__":
    get_fundamentals(add_filter_trade(query(CashFlow.__name__,[CashFlow.symbol,CashFlow.net_operate_cash_flow,
                                                               CashFlow.goods_sale_and_service_render_cash]),
                                      ['2017-01-06']))
