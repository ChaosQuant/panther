#! /usr/bin/env python
# -*- coding: utf-8 -*-

"""
@version: ??
@author: li
@file: calc_engines.py
@time: 2019-09-23 19:44
"""
import importlib


class CalcEngine(object):
    def __init__(self, name, url, methods=[{'packet':'financial.factor_operation_capacity','class':'FactorOperationCapacity'},]):
        self._name = name
        self._methods = methods
        self._url = url

    def local_run(self, trade_date):
        for method in self._methods:
            class_name = method['packet']
            class_name = class_name.split('.')
            class_method = importlib.import_module(class_name[0] + '.calc_engines.' + class_name[1] + '_cal').__getattribute__('CalcEngine')
            calc_engine = class_method(self._name, self._url, self._methods)
            calc_engine.local_run(trade_date)