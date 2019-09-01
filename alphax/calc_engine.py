# -*- coding: utf-8 -*-
#针对191，101 量价进行通用计算
import pandas as pd
import pdb,importlib,inspect,time,datetime,json
from PyFin.api import advanceDateByCalendar
from data.polymerize import DBPolymerize
from ultron.cluster.invoke.cache_data import cache_data
from alphax import app

class CalcEngine(object):
    def __init__(self, name, methods=[{'packet':'alphax.alpha191','class':'Alpha191'},
                                    {'packet':'alphax.alpha101','class':'Alpha101'}]):
        self._name= name
        self._methods = methods
        self._INDU_STYLES = ['Bank','RealEstate','Health','Transportation','Mining','NonFerMetal',
                   'HouseApp','LeiService','MachiEquip','BuildDeco','CommeTrade','CONMAT',
                   'Auto','Textile','FoodBever','Electronics','Computer','LightIndus',
                   'Utilities','Telecom','AgriForest','CHEM','Media','IronSteel',
                   'NonBankFinan','ELECEQP','AERODEF']
    
    def _func_sets(self, method):
        #私有函数和保护函数过滤
        return list(filter(lambda x: not x.startswith('_') and callable(getattr(method,x)), dir(method)))
        
    #计算单个最大窗口
    def _method_max_windows(self, packet_name, class_name):
        print(packet_name, class_name)
        class_method = importlib.import_module(packet_name).__getattribute__(class_name)
        alpha_max_window = 0
        func_sets = self._func_sets(class_method)
        for func in func_sets:
            try:
                max_window = inspect.signature(getattr(class_method,func)).parameters['max_window'].default
                alpha_max_window = max_window if max_window > alpha_max_window else alpha_max_window
            except Exception as e:
                print('Error:' + str(e))
        return alpha_max_window
    
    def _maximization_windows(self):
        alpha_max_window = 0
        for method in self._methods:
            max_window = self._method_max_windows(method['packet'],method['class'])
            alpha_max_window = max_window if max_window > alpha_max_window else alpha_max_window
        return alpha_max_window
        
        
    def calc_factor_by_date(self, data, trade_date):
        trade_date_list = list(set(data.trade_date))
        trade_date_list.sort(reverse=False)
        benchmark_factor = data.set_index('trade_date').loc[trade_date_list[-1]][['code','factor']]
        benchmark_factor.rename(columns={'factor':'benchmark_factor'},inplace=True)
        mkt_df = data.merge(benchmark_factor, on=['code'])
        mkt_df = mkt_df.set_index(['trade_date', 'code'])
        mkt_df = mkt_df[mkt_df['turnover_vol'] > 0]
        
        #
        for p in mkt_df.columns:
            if p in ['open_price', 'highest_price', 'lowest_price', 'close_price', 'vwap']:
                mkt_df[p] = mkt_df[p] * mkt_df['factor'] / mkt_df['benchmark_factor']
        indu_dict = {}
        indu_names = self._INDU_STYLES + ['COUNTRY']
        for date in trade_date_list:
            date_indu_df = data[data['trade_date'] == trade_date_list[-1]].set_index('code')[indu_names]
            indu_check_se = date_indu_df.sum(axis=1).sort_values()
            date_indu_df.drop(indu_check_se[indu_check_se < 2].index, inplace=True)
            indu_dict[date] = date_indu_df.sort_index()
        total_data = {}
        for col in mkt_df.columns:
            total_data[col] = mkt_df[col].unstack().sort_index()
        total_data['returns'] = total_data['close_price'].pct_change()
        total_data['indu'] = indu_dict
        return total_data
    
    
    def loadon_data(self, trade_date):
        db_polymerize = DBPolymerize(self._name)
        max_windows = self._maximization_windows()
        begin_date = advanceDateByCalendar('china.sse', trade_date, '-%sb' % (30 - 1))
        total_data = db_polymerize.fetch_data(begin_date, trade_date,'1b')
        return total_data
    
    #计算因子
    def calc_factor(self, packet_name, class_name, mkt_df, trade_date):
        result = None
        class_method = importlib.import_module(packet_name).__getattribute__(class_name)
        alpha_max_window = 0
        func_sets = self._func_sets(class_method)
        for func in func_sets:
            print(func)
            func_method = getattr(class_method,func)
            fun_param = inspect.signature(func_method).parameters
            dependencies = fun_param['dependencies'].default
            max_window = fun_param['max_window'].default
            begin = advanceDateByCalendar('china.sse', trade_date, '-%sb' % (max_window - 1))
            data = {}
            for dep in dependencies:
                if dep not in ['indu']:
                    data[dep] = mkt_df[dep].loc[begin.strftime("%Y-%m-%d"):trade_date]
                else:
                    data['indu'] = mkt_df['indu']
            res = getattr(class_method(),func)(data)
            res = pd.DataFrame(res)
            res.columns=[func]
            res = res.reset_index()
            if result is None:
                result = res
            else:
                result = result.merge(res,on=['code'])
        return result
    
    def local_run(self, trade_date):
        total_data = self.loadon_data(trade_date)
        mkt_df = self.calc_factor_by_date(total_data,trade_date)
        self.calc_factor('alphax.alpha191','Alpha191',mkt_df,trade_date)
        
    def remote_run(self, trade_date):
        pdb.set_trace()
        total_data = self.loadon_data(trade_date)
        #存储数据
        session = str(int(time.time() * 1000000 + datetime.datetime.now().microsecond))
        cache_data.set_cache(session, 'alphax', total_data.to_json(orient='records'))
        distributed_factor.delay(session, json.dumps(self._methods), self._name)
        
    def distributed_factor(self, total_data):
        mkt_df = self.calc_factor_by_date(total_data,trade_date)
        self.calc_factor('alphax.alpha191','Alpha191',mkt_df,trade_date)
        
@app.task    
def distributed_factor(session, trade_date, packet_sets, name):
    calc_engine = CalcEngine(name, packet_sets)
    content = cache_data.get_cache(session, factor_name)
    total_data = json_normalize(json.loads(content))
    calc_engine.distributed_factor(total_data)