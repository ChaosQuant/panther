# -*- coding: utf-8 -*-
from alphax.calc_engine import CalcEngine

if  __name__=="__main__":
    calc_engine = CalcEngine('dx')
    print(calc_engine.remote_run('2018-12-28'))