# -*- coding: utf-8 -*-


@six.add_metaclass(Singleton)
class Sentiment(object):
    def __init__(self):
        __str__ = 'sentiment'
        self.name = '情绪指标'
        self.factor_type1 = '技术指标因子'
        self.factor_type2 = '情绪指标'
        self.desciption = '反应市场对标的的买卖情绪'