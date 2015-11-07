# -*- coding: utf-8 -*-
# 15/1/27
# create by: snower

from cStringIO import StringIO
from collections import deque
from event import EventEmitter
from loop import current

MAX_BUFFER_SIZE = 16 * 1024 * 1024

class Buffer(EventEmitter):
    def __init__(self):
        super(Buffer, self).__init__()

        self._loop = current()
        self._buffer = StringIO('')
        self._buffer_len = 0
        self._buffers = deque()
        self._len = 0
        self._index = 0
        self._full = False

    def join(self):
        if self._buffers:
            if self._index < self._buffer_len:
                self._buffers.appendleft(self._buffer.read())
            data = "".join(self._buffers)
            self._buffer = StringIO(data)
            self._buffer_len = len(data)
            self._index = 0
            self._buffers.clear()

    def write(self, data):
        self._buffers.append(data)
        self._len += len(data)
        if self._len > MAX_BUFFER_SIZE:
            self._full = True
            self._loop.sync(self.emit, "drain", self)

    def read(self, size = -1):
        if size < 0:
            self.join()
            self._index, self._len, data, self._buffer, self._buffer_len = 0, 0, self._buffer.read(), StringIO(''), 0

            if self._full and self._len < MAX_BUFFER_SIZE:
                self._full = False
                self._loop.sync(self.emit, "regain", self)

            return data

        if self._len < size:
            return None

        if self._buffer_len - self._index < size:
            self.join()

        data = self._buffer.read(size)
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
        if self._index > 0:
            return self._buffer.getvalue()[self._index:]
        return self._buffer.getvalue()

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
        if length > self._buffer_len:
            self.join()
        return str(self).__getitem__(index)

    def __iter__(self):
        for data in str(self):
            yield data
        raise StopIteration()

    def __contains__(self, item):
        self.join()
        return str(self).__contains__(item)

    def __hash__(self):
        self.join()
        return str(self).__hash__()