# -*- coding: utf-8 -*-
# 2019/5/5
# create by: snower

from collections import deque

cdef class Buffer(object):
    def __init__(self):
        self._buffers = deque()

    cdef write(self, data):
        self._buffers.append(data)