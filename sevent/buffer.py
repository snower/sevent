# -*- coding: utf-8 -*-
# 15/1/27
# create by: snower

import time
from collections import deque
from event import EventEmitter
from loop import current

MAX_BUFFER_SIZE = 2 * 1024 * 1024

class Buffer(EventEmitter):
    def __init__(self):
        super(Buffer, self).__init__()

        self._loop = current()
        self._buffer = ''
        self._buffer_len = 0
        self._buffers = deque()
        self._len = 0
        self._index = 0
        self._full = False
        self._drain_size = MAX_BUFFER_SIZE
        self._regain_size = MAX_BUFFER_SIZE * 0.5
        self._drain_time = time.time()
        self._regain_time = time.time()

    def join(self):
        if self._buffer_len - self._index < self._len:
            if self._index < self._buffer_len:
                self._buffers.appendleft(self._buffer[self._index:])
            if len(self._buffers) > 1:
                data = "".join(self._buffers)
                self._buffers.clear()
            else:
                data = self._buffers.popleft()
            self._buffer = data
            self._buffer_len = len(data)
            self._index = 0

    def write(self, data):
        if self._buffer_len <= 0:
            self._buffer = data
            self._buffer_len = len(data)
        else:
            self._buffers.append(data)
        self._len += len(data)
        if self._len > self._drain_size:
            self._full = True
            self._drain_time = time.time()
            self._loop.async(self.emit, "drain", self)

    def read(self, size = -1):
        if self._len <= 0:
            return None
            
        if size < 0:
            if self._buffer_len - self._index < self._len:
                self.join()
            if self._index > 0:
                self._buffer = self._buffer[self._index:]
            self._index, self._buffer_len, self._len = 0, 0, 0

            if self._full and self._len < self._regain_size:
                self._full = False
                self._regain_time = time.time()
                if self._regain_time - self._drain_size <= 1:
                    self._drain_size = self._drain_size * 2
                    self._regain_size = self._drain_size * 0.5
                self._loop.async(self.emit, "regain", self)

            return self._buffer

        if self._len < size:
            return None

        if self._buffer_len - self._index < size:
            self.join()

        data = self._buffer[self._index : self._index + size]
        self._index += size
        self._len -= size

        if self._full and self._len < self._regain_size:
            self._full = False
            self._loop.async(self.emit, "regain", self)

        return data

    def next(self):
        if self._len <= 0:
            return None
            
        if self._buffer_len - self._index > 0:
            self._len -= self._buffer_len - self._index
            self._buffer_len, self._index, index = 0, 0, self._index
            return self._buffer[index:]

        data = self._buffers.popleft()
        self._len -= len(data)
        return data

    def more(self, max_size):
        if self._buffer_len - self._index < max_size:
            self.join()
        size = min(max_size, self._buffer_len - self._index)
        data = self._buffer[self._index : self._index + size]
        return data

    def seek(self, size):
        if self._buffer_len - self._index < size:
            return

        self._index += size
        self._len -= size

        if self._full and self._len < self._regain_size:
            self._full = False
            self._loop.async(self.emit, "regain", self)

    def __len__(self):
        return self._len

    def __str__(self):
        return self._buffer[self._index:] + "".join(self._buffers)

    def __nonzero__(self):
        return self._len > 0

    def __getitem__(self, index):
        return str(self).__getitem__(index)

    def __iter__(self):
        while True:
            data = self.next()
            if not data:
                raise StopIteration()
            yield data

    def __contains__(self, item):
        return str(self).__contains__(item)

    def __hash__(self):
        return str(self).__hash__()
