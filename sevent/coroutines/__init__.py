# -*- coding: utf-8 -*-
# 2020/5/8
# create by: snower

from .future import Future


def run(callback, *args, **kwargs):
    from ..loop import instance
    return instance().run(callback, *args, **kwargs)
