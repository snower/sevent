# -*- coding: utf-8 -*-
# 15/1/27
# create by: snower

import os
import time
from collections import deque
from .event import EventEmitter
from .loop import current
from .utils import ensure_unicode, is_py3

try:
    MAX_BUFFER_SIZE = int(os.environ.get("SEVENT_MAX_BUFFER_SIZE", 4 * 1024 * 1024))
except:
    MAX_BUFFER_SIZE = 4 * 1024 * 1024

class Buffer(EventEmitter):
    def __init__(self, max_buffer_size = None):
        super(Buffer, self).__init__()

        self._loop = current()
        self._buffer = b''
        self._buffer_len = 0
        self._buffers = deque()
        self._len = 0
        self._index = 0
        self._full = False
        self._writting = False
        self._drain_size = max_buffer_size or MAX_BUFFER_SIZE
        self._regain_size = self._drain_size * 0.5
        self._drain_time = time.time()
        self._regain_time = time.time()

    def on_drain(self, callback):
        self.on("drain", callback)

    def on_regain(self, callback):
        self.on("regain", callback)

    def once_drain(self, callback):
        self.once("drain", callback)

    def once_regain(self, callback):
        self.once("regain", callback)

    def do_drain(self):
        self._full = True
        self._drain_time = time.time()
        self.emit("drain", self)

    def do_regain(self):
        self._full = False
        self._regain_time = time.time()
        self.emit("regain", self)

    def join(self):
        if self._buffer_len - self._index < self._len:
            if self._index < self._buffer_len:
                self._buffers.appendleft(self._buffer[self._index:])
            if len(self._buffers) > 1:
                data = b"".join(self._buffers)
                self._buffers.clear()
            else:
                data = self._buffers.popleft()
            self._buffer = data
            self._buffer_len = len(data)
            self._index = 0

    def write(self, data):
        if self._buffer_len > 0:
            self._buffers.append(data)
            self._len += len(data)
        else:
            self._buffer = data
            self._buffer_len = len(data)
            self._len += self._buffer_len

        if not self._full and self._len > self._drain_size:
            self.do_drain()
        return self

    def read(self, size = -1):
        if size < 0:
            if self._buffer_len - self._index < self._len:
                self.join()
                buffer = self._buffer
            elif self._index > 0:
                buffer = self._buffer[self._index:]
            else:
                buffer = self._buffer
            self._index, self._buffer_len, self._buffer, self._len = 0, 0, b'', 0

            if self._full and self._len < self._regain_size:
                self.do_regain()
            return buffer

        if self._len < size:
            return b""

        if self._buffer_len - self._index < size:
            self.join()
            data = self._buffer[:size]
            self._index = size
        else:
            data = self._buffer[self._index: self._index + size]
            self._index += size
        self._len -= size
        
        if self._index >= self._buffer_len:
            if self._len > 0:
                self._buffer = self._buffers.popleft()
                self._index, self._buffer_len = 0, len(self._buffer)
            else:
                self._index, self._buffer_len, self._buffer = 0, 0, b''

        if self._full and self._len < self._regain_size:
            self.do_regain()
        return data

    def next(self):
        if self._index > 0:
            data = self._buffer[self._index:]
            self._len -= self._buffer_len - self._index
        else:
            data = self._buffer
            self._len -= self._buffer_len
        
        if self._len > 0:
            self._buffer = self._buffers.popleft()
            self._index, self._buffer_len = 0, len(self._buffer)
        else:
            self._index, self._buffer_len, self._buffer = 0, 0, b''
        if self._full and self._len < self._regain_size:
            self.do_regain()
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
            self.do_regain()

    def memoryview(self, start = 0, end = None):
        self.join()

        if end:
            return memoryview(self._buffer)[self._index + start: self._index + end]
        return memoryview(self._buffer)[self._index + start:]

    def __len__(self):
        return self._len

    def __str__(self):
        self.join()

        if is_py3:
            return ensure_unicode(self._buffer[self._index:])
        return self._buffer[self._index:]

    def __nonzero__(self):
        return self._len > 0

    def __getitem__(self, index):
        self.join()

        return self._buffer[self._index:].__getitem__(index)

    def __iter__(self):
        while True:
            data = self.next()
            if not data:
                raise StopIteration()
            yield data

    def __contains__(self, item):
        self.join()

        return self._buffer[self._index:].__contains__(item)

    def __hash__(self):
        self.join()

        return self._buffer[self._index:].__hash__()
