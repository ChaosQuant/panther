# -*- coding: utf-8 -*-
import pdb
import json,inspect,math,time
import pandas as pd
from pandas.io.json import json_normalize
import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, select, and_, or_
from PyFin.api import advanceDateByCalendar, bizDatesList
from model import Market,Exposure
from alphax.alpha191 import Alpha191
from alphax.alpha101 import Alpha101
from . import app

def get_alpha_range(factor_name, end_number):
    alpha_max_window = 0
    for number in range(1, int(end_number)+1):
        try:
            max_window = 0
            alpha_fun = eval('Alpha' + str(factor_name) + '().alpha_' + str(number))
            fun_param = inspect.signature(alpha_fun).parameters
            dependencies = fun_param['dependencies'].default
            max_window = fun_param['max_window'].default
            alpha_max_window = max_window if max_window > alpha_max_window else alpha_max_window
        except Exception as e:
            print('Error:' + str(e))
    return alpha_max_window


#提取风格数据
def fetch_dx_risk_sets(engine, end_date, max_window):
    begin_date = advanceDateByCalendar('china.sse', end_date, '-%sb' % (max_window - 1))
    query = select([Exposure]).where(
            and_(Exposure.trade_date >= begin_date, Exposure.trade_date <= end_date))
    risk_df = pd.read_sql(query, engine)
    #时间格式转化
    risk_df['trade_date'] = risk_df['trade_date'].apply(lambda x : x.strftime("%Y-%m-%d"))
    return risk_df
    
#提取基础数据
def fetch_dx_factor_sets(engine, end_date, max_window):
    begin_date = advanceDateByCalendar('china.sse', end_date, '-%sb' % (max_window - 1))
    query = select([Market]).where(
            and_(Market.trade_date >= begin_date, Market.trade_date <= end_date, ))
    mkt_df = pd.read_sql(query, engine)
    mkt_df.rename(columns={'preClosePrice':'pre_close','openPrice':'open_price',
                      'highestPrice':'highest_price','lowestPrice':'lowest_price',
                      'closePrice':'close_price','turnoverVol':'turnover_vol',
                      'turnoverValue':'turnover_value','accumAdjFactor':'accum_adj',
                      'vwap':'vwap','negMarketValue': 'neg_mkt_value',
                      'marketValue': 'mkt_value','chgPct': 'chg_pct','isOpen': 'is_open',
                           'PE': 'pe_ttm',
                           'PE1': 'pe',
                           'PB': 'pb'}, inplace=True)
    #时间格式转化
    mkt_df['trade_date'] = mkt_df['trade_date'].apply(lambda x : x.strftime("%Y-%m-%d"))
    return mkt_df

def update_destdb(destination, destsession, table_name, trade_date, sets):
    sets = sets.where(pd.notnull(sets), None)
    #删除原表
    session = destsession()
    session.execute('''delete from `{0}` where trade_date=\'{1}\''''.format(table_name, trade_date))
    session.commit()
    session.close()
    
    sets.to_sql(name=table_name, con=destination, if_exists='append', index=False)

@app.task(ignore_result=True)
def alpha101_dispatch(task_id, factor_name, end_date, source_db, dest_db):
    INDU_STYLES = ['Bank','RealEstate','Health','Transportation','Mining','NonFerMetal',
                   'HouseApp','LeiService','MachiEquip','BuildDeco','CommeTrade','CONMAT',
                   'Auto','Textile','FoodBever','Electronics','Computer','LightIndus',
                   'Utilities','Telecom','AgriForest','CHEM','Media','IronSteel',
                   'NonBankFinan','ELECEQP','AERODEF']
    start = time.time()
    end_number = 101
    alpha101_max_window = get_alpha_range('101', end_number)
    
    engine = create_engine(source_db)
    destination = sa.create_engine(dest_db)
    destsession = sessionmaker( bind=destination, autocommit=False, autoflush=True)
    
    mkt_df = fetch_dx_factor_sets(engine, end_date, alpha101_max_window)
    #转化为前复权数据 前复权价格 = 除权价*复权因子/基准的复权因子
    #获取基准复权因子
    trade_date_list = list(set(mkt_df.trade_date))
    trade_date_list.sort(reverse=False)
    benchmark_date = trade_date_list[-1]
    benchmark_factor = mkt_df.set_index('trade_date').loc[trade_date_list[-1]][['code','accum_adj']]
    benchmark_factor.rename(columns={'accum_adj':'benchmark_factor'},inplace=True)
    mkt_df = mkt_df.merge(benchmark_factor, on=['code'])
    mkt_df = mkt_df.set_index(['trade_date', 'code'])
    mkt_df = mkt_df[mkt_df['turnover_vol'] > 0]
    for p in mkt_df.columns:
        if p in ['open_price', 'highest_price', 'lowest_price', 'close_price', 'vwap']:
            mkt_df[p] = mkt_df[p] * mkt_df['accum_adj'] / mkt_df['benchmark_factor']
     
    #风格数据
    risk_df = fetch_dx_risk_sets(engine, end_date, alpha101_max_window)
    
    indu_dict = {}
    indu_names = INDU_STYLES + ['COUNTRY']
    date_indu_df = risk_df.set_index('code')[indu_names]
    indu_check_se = date_indu_df.sum(axis=1).sort_values()
    date_indu_df.drop(indu_check_se[indu_check_se < 2].index, inplace=True)
    indu_dict[end_date] = date_indu_df.sort_index()
    total_data = {}
    for col in ['open_price', 'highest_price', 'lowest_price', 'close_price', 
                'vwap', 'turnover_vol','turnover_value',
                'neg_mkt_value','mkt_value',  'pe_ttm', 'pe', 'pb',
                'accum_adj']:
        total_data[col] = mkt_df[col].unstack().sort_index()
    total_data['returns'] = total_data['close_price'].pct_change()
    total_data['indu'] = indu_dict
    result = None
    alpha_num_list = [2, 3, 4, 5, 6, 11, 12, 13, 14, 15, 16, 18, 19, 22, 23, 24,
                  26, 27, 29, 32, 36, 40, 44, 45, 50, 52, 53, 54, 55, 57, 58,
                  59, 62, 66, 67, 69, 72, 73, 74, 75, 76, 80, 81, 82, 83, 84,
                  87, 88, 90, 91, 96, 97, 99]
    for number in range(1, int(end_number)+1):
        if number not in alpha_num_list:
            continue
        max_window = 0
        alpha_fun = eval('Alpha101().alpha_' + str(number))
        fun_param = inspect.signature(alpha_fun).parameters
        dependencies = fun_param['dependencies'].default
        max_window = fun_param['max_window'].default
        begin = advanceDateByCalendar('china.sse', end_date, '-%sb' % (max_window - 1))
        data = {}
        for dep in dependencies:
            data[dep] = total_data[dep].loc[begin.strftime("%Y-%m-%d"):end_date]
        
        alpha_start_time = time.time()
        res = alpha_fun(data)
        content = 'alpha{0}:{1}'.format(number, time.time() - alpha_start_time)
        print(content)
        res = pd.DataFrame(res)
        res.columns=['alpha101_' + str(number)]
        res = res.reset_index()
        print(res.columns)
        if result is None:
            result = res
        else:
            result = result.merge(res,on=['code'])
        
    result.rename(columns={'code':'symbol'},inplace=True)
    ##修改命名
    result['trade_date'] = end_date
    result['symbol'] = result['symbol'].apply(
                lambda x: "{:06d}".format(x) + '.XSHG' if len(str(x))==6 and str(x)[0] in '6' else "{:06d}".format(x)\
                + '.XSHE')
    update_destdb(destination, destsession, 'alpha101', end_date, result)
    end = time.time()
    print('存储数据库用时(s)：', end - start)
    
@app.task(ignore_result=True)
def alpha191_dispatch(task_id, factor_name, end_date, source_db, dest_db):
    start = time.time()
    end_number = 191
    alpha191_max_window = get_alpha_range('191', end_number)
    engine = create_engine(source_db)
    destination = sa.create_engine(dest_db)
    destsession = sessionmaker( bind=destination, autocommit=False, autoflush=True)
    mkt_df = fetch_dx_factor_sets(engine, end_date, alpha191_max_window)
    #转化为前复权数据 前复权价格 = 除权价*复权因子/基准的复权因子
    #获取基准复权因子
    trade_date_list = list(set(mkt_df.trade_date))
    trade_date_list.sort(reverse=False)
    benchmark_date = trade_date_list[-1]
    benchmark_factor = mkt_df.set_index('trade_date').loc[trade_date_list[-1]][['code','accum_adj']]
    benchmark_factor.rename(columns={'accum_adj':'benchmark_factor'},inplace=True)
    mkt_df = mkt_df.merge(benchmark_factor, on=['code'])
    mkt_df = mkt_df.set_index(['trade_date', 'code'])
    mkt_df = mkt_df[mkt_df['turnover_vol'] > 0]
    for p in mkt_df.columns:
        if p in ['open_price', 'highest_price', 'lowest_price', 'close_price', 'vwap']:
            mkt_df[p] = mkt_df[p] * mkt_df['accum_adj'] / mkt_df['benchmark_factor']
    total_data = mkt_df.to_panel()
    result = None
    filter_list = [30]
    pdb.set_trace()
    for number in range(130, int(end_number)+1):
        if number in filter_list:
            continue
        max_window = 0
        alpha_fun = eval('Alpha191().alpha_' + str(number))
        fun_param = inspect.signature(alpha_fun).parameters
        dependencies = fun_param['dependencies'].default
        max_window = fun_param['max_window'].default
        begin = advanceDateByCalendar('china.sse', end_date, '-%sb' % (max_window - 1))
        data = {}
        for dep in dependencies:
            data[dep] = total_data[dep].loc[begin.strftime("%Y-%m-%d"):end_date]
        alpha_start_time = time.time()
        res = alpha_fun(data)
        content = 'alpha{0}:{1}'.format(number, time.time() - alpha_start_time)
        print(content)
        res = pd.DataFrame(res)
        res.columns=['alpha191_' + str(number)]
        res = res.reset_index()
        print(res.columns)
        if result is None:
            result = res
        else:
            result = result.merge(res,on=['code'])
        
    result.rename(columns={'code':'symbol'},inplace=True)
    ##修改命名
    result['trade_date'] = end_date
    result['symbol'] = result['symbol'].apply(
                lambda x: "{:06d}".format(x) + '.XSHG' if len(str(x))==6 and str(x)[0] in '6' else "{:06d}".format(x)\
                + '.XSHE')
    update_destdb(destination, destsession, 'alpha101', end_date, result)
    end = time.time()
    print('存储数据库用时(s)：', end - start)
            