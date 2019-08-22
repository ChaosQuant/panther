# -*- coding: utf-8 -*-

from ultron.cluster.invoke.app_engine import create_app

app = create_app('alphax',['dispatch.alpha191_dispatch','dispatch.alpha101_dispatch'])
