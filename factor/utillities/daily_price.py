#!/usr/bin/env python
# coding=utf-8
import pdb
import sys
sys.path.append("..")
from db.data_engine import DataFactory
from trade.daily_price import DailyPrice as TradeDailyPrice
from mlog import MLog

class DailyPrice(object):

    def __init__(self, db_engine=None):
        self.__db_engine = DataFactory.CreateEngine(1,'DNDS', db_engine)
        
    def get_daily_price(self, symbol_sets, trade_date):
        sql_pe = 'select B.SYMBOL, S.TRADEDATE, S.TCLOSE, S.AVGPRICE, B.LISTDATE from dnds.dbo.TQ_QT_SKDAILYPRICE AS S JOIN dnds.dbo.TQ_SK_BASICINFO as B on S.SECODE = B.SECODE and B.symbol IN(' + symbol_sets + ') and S.TRADEDATE = ' + str(trade_date) + ' ORDER BY S.TRADEDATE DESC'
        daily_price_df = self.__db_engine.get_datasets(sql_pe)
        return daily_price_df.set_index(['SYMBOL'])
    
    @classmethod
    def cn_stock_daily_price(cls, db_engine, symbol_sets, trade_date):
        daily_price_sets = {}
        sql_pe = 'select S.ID, B.SYMBOL, S.TRADEDATE,S.LCLOSE,S.TOPEN, S.TCLOSE,S.THIGH,S.TLOW,S.AVGPRICE,S.VOL,S.AMOUNT,B.LISTDATE from dnds.dbo.TQ_QT_SKDAILYPRICE AS S JOIN dnds.dbo.TQ_SK_BASICINFO as B on S.SECODE = B.SECODE and B.symbol IN(' + symbol_sets + ') and S.TRADEDATE = ' + str(trade_date)
        daily_price_df = db_engine.get_datasets(sql_pe)
        for index in daily_price_df.index:
            data = daily_price_df.loc[index].values
            daily_price = TradeDailyPrice()
            daily_price.df_parser(data)
            daily_price_sets[daily_price.symbol()] = daily_price
        return daily_price_sets
    
    @classmethod
    def cn_future_daily_price(cls, db_engine, symbol, trade_date):
        sql_pe = 'select Q.ID, B.CONTRACTCODE,Q.TRADEDATE,Q.LCLOSE,Q.TOPEN,Q.TCLOSE,Q.THIGH,Q.TLOW,Q.SETTLEPRICE,Q.VOL,Q.AMOUNT from dnds.dbo.TQ_QT_FUTURE AS Q JOIN dnds.dbo.TQ_FT_BASICINFO AS B ON B.SECODE = Q.SECODE WHERE B.CONTRACTCODE = \'' + symbol + '\' and Q.TRADEDATE = ' + str(trade_date)
        print sql_pe
        daily_price_df = db_engine.get_datasets(sql_pe)
        for index in daily_price_df.index:
            data = daily_price_df.loc[index].values
            daily_price = TradeDailyPrice(dtype=2)
            daily_price.df_parser(data)
        return daily_price

    @classmethod
    def cn_future_daily_price_sets(cls, db_engine, symbol, start_date, end_date):
        sql_pe = 'select Q.ID, B.CONTRACTCODE,Q.TRADEDATE,Q.LCLOSE,Q.TOPEN,Q.TCLOSE,Q.THIGH,Q.TLOW,Q.SETTLEPRICE,Q.VOL,Q.AMOUNT from dnds.dbo.TQ_QT_FUTURE AS Q JOIN dnds.dbo.TQ_FT_BASICINFO AS B ON B.SECODE = Q.SECODE WHERE B.CONTRACTCODE = \'' + symbol + '\' and Q.TRADEDATE >= ' + str(start_date) + ' and Q.TRADEDATE <= ' + str(end_date)
        print sql_pe
        daily_price_df = db_engine.get_datasets(sql_pe)
        return daily_price_df

