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

    def join(self):
        self._buffer = self._buffer[self._index:] + "".join(self._buffers)
        self._index, self._buffers = 0, deque()

    def write(self, data):
        self._buffers.append(data)
        self._len += len(data)

    def read(self, size):
        if size < 0 or len(self._buffer) - self._index < size:
            self.join()
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
        self.join()
        return self._buffer

    def __nonzero__(self):
        return self._len > 0

    def __getitem__(self, index):
        if isinstance(index, int):
            length = index + 1
        elif isinstance(index, slice):
            length = index.stop + 1
        else:
            raise KeyError(index)
        if length > self._len:
            raise IndexError(index)
        if length > len(self._buffer):
            self.join()
        return self._buffer.__getitem__(index)

    def __iter__(self):
        for data in str(self):
            yield data
        raise StopIteration()

    def __contains__(self, item):
        self.join()
        return self._buffer.__contains__(item)

    def __hash__(self):
        self.join()
        self._buffer.__hash__()