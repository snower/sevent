# -*- coding: utf-8 -*-
# 15/1/27
# create by: snower

from cStringIO import StringIO
from collections import deque
from event import EventEmitter
from loop import current

MAX_BUFFER_SIZE = 1024 * 1024

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
        self._drain_size = MAX_BUFFER_SIZE
        self._regain_size = MAX_BUFFER_SIZE * 0.8

    def join(self):
        if self._buffers:
            if self._index < self._buffer_len:
                self._buffers.appendleft(self._buffer.read())
            if len(self._buffers) > 1:
                data = "".join(self._buffers)
                self._buffers.clear()
            else:
                data = self._buffers.popleft()
            self._buffer = StringIO(data)
            self._buffer_len = len(data)
            self._index = 0

    def write(self, data):
        self._buffers.append(data)
        self._len += len(data)
        if self._len > self._drain_size:
            self._full = True
            self._loop.async(self.emit, "drain", self)

    def read(self, size = -1):
        if self._len <= 0:
            return None
            
        if size < 0:
            self.join()
            self._index, self._len, data, self._buffer, self._buffer_len = 0, 0, self._buffer.read(), StringIO(''), 0

            if self._full and self._len < self._regain_size:
                self._full = False
                self._loop.async(self.emit, "regain", self)

            return data

        if self._len < size:
            return None

        if self._buffer_len - self._index < size:
            self.join()

        data = self._buffer.read(size)
        self._index += size
        self._len -= size

        if self._full and self._len < self._regain_size:
            self._full = False
            self._loop.async(self.emit, "regain", self)

        return data

    def next(self):
        if self._len <= 0:
            return None
            
        if self._buffer_len > 0:
            data = self._buffer.read()
            self._buffer_len, self._index = 0, 0
            return data

        data = self._buffers.popleft()
        self._len -= len(data)
        return data

    def __len__(self):
        return self._len

    def __str__(self):
        if self._index > 0:
            return self._buffer.getvalue()[self._index:] + "".join(self._buffers)
        return "".join(self._buffers)

    def __nonzero__(self):
        return self._len > 0

    def __getitem__(self, index):
        if index == 0:
            return self._buffer.getvalue()[self._index:]
        return self._buffers[index - 1]

    def __iter__(self):
        data = self.next()
        while data:
            yield data
            data = self.next()
        raise StopIteration()

    def __contains__(self, item):
        return str(self).__contains__(item)

    def __hash__(self):
        return str(self).__hash__()
