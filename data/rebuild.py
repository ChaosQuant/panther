# -*- coding: utf-8 -*-
import importlib,pdb,inspect
import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker

class Rebuild(object):
    def __init__(self, db_url):
        __str__ = 'rebuild'
        self._destination = sa.create_engine(db_url)
        self._destsession = sessionmaker(bind=self._destination, autocommit=False, autoflush=True)
        
    
    def _func_sets(self, method):
        #私有函数和保护函数过滤
        return list(filter(lambda x: not x.startswith('_') and callable(getattr(method,x)), dir(method)))
    
    def _create_table(self, class_name, func_sets):
        create_sql = """create table `{0}`(
                    `id` varchar(32) NOT NULL,
                    `symbol` varchar(24) NOT NULL,
                    `trade_date` date NOT NULL,""".format(class_name)
        for func in func_sets:
            create_sql += """`{0}` decimal(19,4),
                          """.format(func)
        create_sql += """ PRIMARY KEY(`id`,`trade_date`,`symbol`)
                    )ENGINE=InnoDB DEFAULT CHARSET=utf8;"""
        return create_sql
    
    def _update_factor_info(self, class_name, class_method, session):
        ## update factor_info
        delete_sql = """delete from `{0}` where factor_type='{1}'""".format('factor_info', class_name)
        session.execute(delete_sql)
        update_sql = """insert into `{0}` (`name`,`factor_type`,`type_1`,`type_2`) values('{1}','{2}','{3}','{4}');""".format(
        'factor_info', class_method().name, class_name, class_method().factor_type1, class_method().factor_type2)
        session.execute(update_sql)
    
    def _update_factor_detail(self, class_name, class_method, func_sets, session):
        update_sql = ''
        delete_sql = """delete from `{0}` where factor_type='{1}'""".format('factor_detail', class_name)
        session.execute(delete_sql)
        for func in func_sets:
            try:
                desc = str(str(getattr(class_method,func).__doc__.split('\n')[-2]).split('return:')[-1])
                desc = desc.replace(':','：')
            except:
                desc = None
            update_sql ="""insert into `{0}` (`factor_type`,`factor_name`,`description`) values('{1}','{2}','{3}');""".format(
            'factor_detail', class_name, str(func), desc)
            session.execute(update_sql)
        
    def _build_table(self, class_name, func_sets, session):
        create_sql = self._create_table(class_name, func_sets)
        drop_sql =  """drop table if exists `{0}`""".format(class_name)
        session.execute(drop_sql)
        session.execute(create_sql)
    
    def rebuild_table(self, packet_name, class_name):
        #动态获取对应类
        session = self._destsession()
        class_method = importlib.import_module(packet_name).__getattribute__(class_name)
        func_sets = self._func_sets(class_method)
        self._build_table(class_name, func_sets, session)
        self._update_factor_info(class_name, class_method, session)
        self._update_factor_detail(class_name, class_method, func_sets, session)
        session.commit()
        session.close()