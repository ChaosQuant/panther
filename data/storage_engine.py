# -*- coding: utf-8 -*-
import six,pdb
import pandas as pd
import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, select, and_, or_
from utilities.singleton import Singleton

@six.add_metaclass(Singleton)
class StorageEngine(object):
    def __init__(self, url):
        self._destination = sa.create_engine(url)
        self._destsession = sessionmaker(bind=self._destination, autocommit=False, autoflush=True)
     
    def update_destdb(self, table_name, trade_date, sets):
        sets = sets.where(pd.notnull(sets), None)
        #删除原表
        session = self._destsession()
        session.execute('''delete from `{0}` where trade_date=\'{1}\''''.format(table_name, trade_date))
        session.commit()
        session.close()
        sets.to_sql(name=table_name, con=self._destination, if_exists='append', index=False)


class PerformanceStorageEngine(object):
    def __init__(self, url):
        self._destination = sa.create_engine(url)
        self._destsession = sessionmaker(bind=self._destination, autocommit=False, autoflush=True)

    def update_destdb(self, table_name, factor_name, sets):
        sets = sets.where(pd.notnull(sets), None)
        # 删除原表
        session = self._destsession()
        session.execute('''delete from `{0}` where factor_name=\'{1}\''''.format(table_name, factor_name))
        session.commit()
        session.close()
        sets.to_sql(name=table_name, con=self._destination, if_exists='append', index=False)


class BenchmarkStorageEngine(object):
    def __init__(self, url):
        self._destination = sa.create_engine(url)
        self._destsession = sessionmaker(bind=self._destination, autocommit=False, autoflush=True)

    def update_destdb(self, table_name, sets):
        sets = sets.where(pd.notnull(sets), None)
        # 删除原表
        session = self._destsession()
        session.execute('''delete from `{0}`'''.format(table_name))
        session.commit()
        session.close()
        sets.to_sql(name=table_name, con=self._destination, if_exists='append', index=False)


class IntegratedStorageEngine(object):
    def __init__(self, url):
        self._destination = sa.create_engine(url)
        self._destsession = sessionmaker(bind=self._destination, autocommit=False, autoflush=True)

    def update_destdb(self, table_name, begin_date, end_date, factor_name, sets):
        sets = sets.where(pd.notnull(sets), None)
        # 删除原表
        session = self._destsession()
        session.execute(
            '''delete from `{0}` where (trade_date between \'{1}\' and \'{2}\' ) and factor_name=\'{3}\''''.format(
                table_name, begin_date, end_date, factor_name)
        )
        session.commit()
        session.close()
        sets.to_sql(name=table_name, con=self._destination, if_exists='append', index=False)


class IntegratedSubStorageEngine(object):
    def __init__(self, url):
        self._destination = sa.create_engine(url)
        self._destsession = sessionmaker(bind=self._destination, autocommit=False, autoflush=True)

    def update_destdb(self, table_name, trade_date, factor_name, benchmark, sets):
        sets = sets.where(pd.notnull(sets), None)
        # 删除原表
        session = self._destsession()
        session.execute('''delete from `{0}` where factor_name=\'{1}\' and trade_date=\'{2}\' 
                        and benchmark=\'{3}\''''.format(table_name,
                                                        factor_name,
                                                        trade_date,
                                                        benchmark))
        session.commit()
        session.close()
        sets.to_sql(name=table_name, con=self._destination, if_exists='append', index=False)