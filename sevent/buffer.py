# -*- coding: utf-8 -*-
# 15/1/27
# create by: snower

import os
import time
import logging
from collections import deque
from .event import EventEmitter
from .loop import current
from .utils import ensure_unicode, is_py3

try:
    RECV_BUFFER_SIZE = int(os.environ.get("SEVENT_RECV_BUFFER_SIZE", 0))
except:
    RECV_BUFFER_SIZE = 0

try:
    MAX_BUFFER_SIZE = int(os.environ.get("SEVENT_MAX_BUFFER_SIZE", 4 * 1024 * 1024))
except:
    MAX_BUFFER_SIZE = 4 * 1024 * 1024

try:
    if not os.environ.get("SEVENT_NOUSE_CBUFFER", False):
        from . import cbuffer
        BaseBuffer = cbuffer.Buffer
        if RECV_BUFFER_SIZE:
            try:
                cbuffer.socket_set_recv_size(RECV_BUFFER_SIZE)
            except: pass
    else:
        cbuffer = None
except ImportError:
    cbuffer = None

RECV_BUFFER_SIZE = RECV_BUFFER_SIZE or 8 * 1024 - 64

if cbuffer is None:
    class BaseBuffer(object):
        def __init__(self):
            self._buffer = b''
            self._buffer_odata = None
            self._buffer_len = 0
            self._buffers = deque()
            self._buffers_odata = deque()
            self._len = 0
            self._buffer_index = 0

        def join(self):
            if self._buffer_index > 0 or self._buffer_len - self._buffer_index < self._len:
                if self._buffer_index < self._buffer_len:
                    self._buffers.appendleft(self._buffer[self._buffer_index:])
                if len(self._buffers) > 1:
                    data = b"".join(self._buffers)
                    self._buffers.clear()
                else:
                    data = self._buffers.popleft()
                if self._buffers_odata:
                    self._buffer_odata = self._buffers_odata[-1]
                    self._buffers_odata.clear()
                self._buffer = data
                self._buffer_len = len(data)
                self._buffer_index = 0
            return self._buffer

        def write(self, data, odata = None):
            if self._buffer_len > 0:
                self._buffers.append(data)
                self._buffers_odata.append(odata)
                self._len += len(data)
            else:
                self._buffer = data
                self._buffer_odata = odata
                self._buffer_len = len(data)
                self._len += self._buffer_len
            return self

        def read(self, size=-1):
            if size < 0:
                if self._buffer_len - self._buffer_index < self._len:
                    self.join()
                    buffer = self._buffer
                elif self._buffer_index > 0:
                    buffer = self._buffer[self._buffer_index:]
                else:
                    buffer = self._buffer
                buffer_odata = self._buffer_odata
                self._buffer_index, self._buffer_len, self._buffer, self._buffer_odata, self._len = 0, 0, b'', None, 0
                return (buffer, buffer_odata) if buffer_odata else buffer

            if self._len < size:
                return b""

            if self._buffer_len - self._buffer_index < size:
                self.join()
                data = self._buffer[:size]
                self._buffer_index = size
            else:
                data = self._buffer[self._buffer_index: self._buffer_index + size]
                self._buffer_index += size
            buffer_odata = self._buffer_odata
            self._len -= size

            if self._buffer_index >= self._buffer_len:
                if self._len > 0:
                    self._buffer = self._buffers.popleft()
                    self._buffer_odata = self._buffers_odata.popleft()
                    self._buffer_index, self._buffer_len = 0, len(self._buffer)
                else:
                    self._buffer_index, self._buffer_len, self._buffer, self._buffer_odata = 0, 0, b'', None
            return (data, buffer_odata) if buffer_odata else data

        def next(self):
            if self._buffer_index > 0:
                data = self._buffer[self._buffer_index:]
                self._len -= self._buffer_len - self._buffer_index
            else:
                data = self._buffer
                self._len -= self._buffer_len
            buffer_odata = self._buffer_odata

            if self._len > 0:
                self._buffer = self._buffers.popleft()
                self._buffer_odata = self._buffers_odata.popleft()
                self._buffer_index, self._buffer_len = 0, len(self._buffer)
            else:
                self._buffer_index, self._buffer_len, self._buffer, self._buffer_odata = 0, 0, b'', None
            return (data, buffer_odata) if buffer_odata else data

        def __len__(self):
            return self._len

        def __str__(self):
            buffer = self.join()

            if is_py3:
                return ensure_unicode(buffer)
            return (buffer, self._buffer_odata) if self._buffer_odata else buffer

        def __nonzero__(self):
            return self._len > 0

        def __getitem__(self, index):
            buffer = self.join()
            return (buffer.__getitem__(index), self._buffer_odata) if self._buffer_odata else buffer.__getitem__(index)

        def __hash__(self):
            buffer = self.join()
            return buffer.__hash__()

class Buffer(EventEmitter, BaseBuffer):
    def __init__(self, max_buffer_size = None):
        EventEmitter.__init__(self)
        BaseBuffer.__init__(self)

        self._loop = current()
        self._full = False
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
        try:
            self.emit_drain(self)
        except Exception as e:
            logging.exception("buffer emit drain error:%s", e)

    def do_regain(self):
        self._full = False
        self._regain_time = time.time()
        try:
            self.emit_regain(self)
        except Exception as e:
            logging.exception("buffer emit regain error:%s", e)

    def write(self, data, odata = None):
        if odata is None:
            BaseBuffer.write(self, data)
        else:
            BaseBuffer.write(self, data, odata)

        if not self._full and self._len > self._drain_size:
            self.do_drain()
        return self

    def read(self, size=-1):
        data = BaseBuffer.read(self, size)

        if self._full and self._len < self._regain_size:
            self.do_regain()
        return data

    def next(self):
        data = BaseBuffer.next(self)

        if self._full and self._len < self._regain_size:
            self.do_regain()
        return data

    def __iter__(self):
        while True:
            data = self.next()
            if not data:
                raise StopIteration()
            yield data

    def __contains__(self, item):
        buffer = self.join()

        return buffer.__contains__(item)
