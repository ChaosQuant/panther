# -*- coding: utf-8 -*-

import threading
import logging
import datetime
import os
import sys
sys.path.append("..")

class Singleton(object):
    objs = {}
    objs_locker = threading.Lock()


    def __new__(cls, *args, **kv):
        if cls in cls.objs:
            return cls.objs[cls]['obj']

        cls.objs_locker.acquire()
        try:
            if cls in cls.objs:
                return cls.objs[cls]['obj']
            obj = object.__new__(cls)
            cls.objs[cls] = {'obj':obj, 'init':False}
            setattr(cls, '__init__', cls.decorate_init(cls.__init__))
        finally:
            cls.objs_locker.release()


    @classmethod
    def decorate_init(cls, fn):
        def init_wrap(*args):
            if not cls.objs[cls]['init']:
                fn(*args)
                cls.objs[cls]['init'] = True
            return
        return init_wrap

class MLog():
    """
    classdocs
    """

    @classmethod
    def config(cls, name="logging", level=logging.DEBUG):
        """
        Constructor
        """
        dir = os.path.expandvars('$HOME') + '/MLOG/' + name + '/'
        if not os.path.exists(dir):
            os.makedirs(dir)
        filename = dir + name + '_' + datetime.datetime.now().strftime('%b_%d_%H_%M')+'.log'
        # [Tue Nov 15 11:35:53 2016] [notice] Apache/2.2.15 (Unix) DAV/2 PHP/5.3.3 mod_ssl/2.2.15 OpenSSL/1.0.1e-fips configured -- resuming normal operations
        
        
        format_str = "[%(process)d %(thread)d][%(asctime)s][%(filename)s line:%(lineno)d][%(levelname)s] %(message)s"
        # define a Handler which writes INFO messages or higher to the sys.stderr
        logging.basicConfig(level=level,
                            format=format_str,
                            datefmt='%m-%d %H:%M',
                            filename=filename,
                            filemode='w')
        console = logging.StreamHandler()
        console.setLevel(logging.INFO)
        formatter = logging.Formatter(format_str)
        console.setFormatter(formatter)
        # 将定义好的console日志handler添加到root logger
        logging.getLogger('').addHandler(console)


    @classmethod
    def write(self):
        # logging.error()
        return logging

# mlog = MLog()
