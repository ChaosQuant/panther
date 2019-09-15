# -*- coding: utf-8 -*-
import six,pdb,talib
import numpy as np
from utilities.singleton import Singleton

@six.add_metaclass(Singleton)
class Trend(object):
    def __init__(self):
        __str__ = 'momentum'
        self.name = '动量指标'
        self.factor_type1 = '技术指标因子'
        self.factor_type2 = '动量指标'
        self.desciption = '主要用于跟踪并预测股价的发展趋势'
        
    
            