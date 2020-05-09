# -*- coding: utf-8 -*-
# 2020/5/8
# create by: snower


def run(callback, *args, **kwargs):
    from ..loop import instance
    return instance().run(callback, *args, **kwargs)
