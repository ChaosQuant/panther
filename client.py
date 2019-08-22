# -*- coding: utf-8 -*-
import pdb
import inspect
import pandas as pd
import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker
from alphax.dispatch import alpha191_dispatch


##创建数据库表
def create_table(dest_db, table_name, alpha_number):
    destination = sa.create_engine(dest_db)
    dest_session = sessionmaker(bind=destination, autocommit=False, autoflush=True)
    drop_sql = """drop table if exists `{0}`""".format(table_name)
    create_sql = """create table `{0}`(
                    `id` BIGINT NOT NULL AUTO_INCREMENT,
                    `symbol` varchar(24) NOT NULL,
                    `trade_date` date NOT NULL,\n""".format(table_name)

    for number in range(1, alpha_number+1):
        create_sql += """`alpha191_{0}` decimal(19,4),""".format(number)
      
    create_sql += """ \nPRIMARY KEY(`id`,`trade_date`,`symbol`)
                    )ENGINE=InnoDB DEFAULT CHARSET=utf8;"""
    session = dest_session()
    session.execute(create_sql)
    session.commit()
    session.close()

    
    
if __name__ == "__main__":
    end_date = '2018-12-28'
    alpha191_dispatch(str('15609986886946081'), '191', end_date,
                  source_db='postgresql+psycopg2://alpha:alpha@180.166.26.82:8889/alpha', 
                  dest_db='mysql+mysqlconnector://quanto_edit:quanto_edit_2019@db1.irongliang.com/quanto')