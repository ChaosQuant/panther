# -*- coding: utf-8 -*-
from alphax.calc_engine import CalcEngine
from data.rebuild import Rebuild

if  __name__=="__main__":
    #calc_engine = CalcEngine('dx')
    #print(calc_engine.remote_run('2018-12-28'))
    rebuild = Rebuild('mysql+mysqlconnector://factor_edit:factor_edit_2019@db1.irongliang.com:3306/vision')
    rebuild.rebuild_table('alphax.alpha191','Alpha191')