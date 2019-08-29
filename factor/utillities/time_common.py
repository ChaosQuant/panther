#!/usr/bin/env python
# coding=utf-8

import time
class TimeCommon(object):
    @classmethod
    def get_end_time(cls, end_date, end_time):
        s = time.mktime(time.strptime(str(end_date) + ' ' + str(end_time), '%Y%m%d %H:%M:%S'))
        return int(s)
