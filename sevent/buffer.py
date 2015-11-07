# -*- coding: utf-8 -*-
# 15/1/27
# create by: snower

from collections import deque
from event import EventEmitter
from loop import current

MAX_BUFFER_SIZE = 16 * 1024 * 1024

class Buffer(EventEmitter):
    def __init__(self):
        super(Buffer, self).__init__()

        self._loop = current()
        self._buffer = ''
        self._buffers = deque()
        self._len = 0
        self._index = 0
        self._full = False

    def join(self):
        if len(self._buffer) >= self._len:
            return

        if self._index >= self._len:
            self._buffer = "".join(self._buffers)
        else:
            self._buffer = self._buffer[self._index:] + "".join(self._buffers)
        self._index= 0
        self._buffers.clear()

    def write(self, data):
        self._buffers.append(data)
        self._len += len(data)
        if self._len > MAX_BUFFER_SIZE:
            self._full = True
            self._loop.sync(self.emit, "drain", self)

    def read(self, size):
        if size < 0:
            self.join()
            self._len, data, self._buffer = 0, self._buffer, ''

            if self._full and self._len < MAX_BUFFER_SIZE:
                self._full = False
                self._loop.sync(self.emit, "regain", self)

            return data

        if len(self._buffer) - self._index < size:
            self.join()
            if size > len(self._buffer):
                return None

        data = self._buffer[self._index: self._index + size]
        self._index += size
        self._len -= size

        if self._full and self._len < MAX_BUFFER_SIZE:
            self._full = False
            self._loop.sync(self.emit, "regain", self)

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