# -*- coding: utf-8 -*-
# 15/1/27
# create by: snower

from collections import deque

class Buffer(object):
    def __init__(self):
        self._buffer = ''
        self._buffers = deque()
        self._len = 0
        self._index = 0

    def write(self, data):
        self._buffers.append(data)
        self._len += len(data)

    def read(self, size):
        if size < 0 or len(self._buffer) - self._index < size:
            self._buffer = self._buffer[self._index:] + "".join(self._buffers)
            self._index, self._buffers = 0, deque()
            if size < 0:
                self._index, self._len = 0, 0
                data, self._buffer = self._buffer, ''
                return data
            if size > len(self._buffer):
                return None
        data = self._buffer[self._index: self._index + size]
        self._index += size
        self._len -= size
        return data

    def __len__(self):
        return self._len

    def __str__(self):
        return self.read(-1)

    def __nonzero__(self):
        return self._len > 0

    def __getitem__(self, index):
        if index >= self._len:
            raise KeyError(index)
        if index >= len(self._buffer):
            self.read(-1)
        return self._buffer[index]

    def __iter__(self):
        return self.read(-1)