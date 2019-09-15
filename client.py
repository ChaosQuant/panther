# -*- coding: utf-8 -*-
from technical.calc_engine import CalcEngine
from data.rebuild import Rebuild
from PyFin.api import *
import pdb,time
import warnings
warnings.filterwarnings("ignore")

if  __name__=="__main__":
    calc_engine = CalcEngine('rl','mysql+mysqlconnector://factor_edit:factor_edit_2019@db1.irongliang.com/vision?charset=utf8')
    print(calc_engine.local_run('2019-09-11'))