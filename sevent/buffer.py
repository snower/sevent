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
    BUFFER_DRAIN_RATE = max(min(float(os.environ.get("SEVENT_BUFFER_DRAIN_RATE", 0.5)), 0.1), 0.9)
except:
    BUFFER_DRAIN_RATE = 0.5

try:
    RECV_COUNT = int(os.environ.get("SEVENT_RECV_COUNT", 0))
except:
    RECV_COUNT = 0
    
try:
    SEND_COUNT = int(os.environ.get("SEVENT_SEND_COUNT", 0))
except:
    SEND_COUNT = 0
    
try:
    if not os.environ.get("SEVENT_NOUSE_CBUFFER", False):
        from . import cbuffer
        BaseBuffer = cbuffer.Buffer
        if RECV_BUFFER_SIZE:
            try:
                cbuffer.socket_set_recv_size(RECV_BUFFER_SIZE)
            except: pass
        if RECV_COUNT:
            try:
                cbuffer.socket_set_recv_count(RECV_COUNT)
            except: pass
        if SEND_COUNT:
            try:
                cbuffer.socket_set_send_count(SEND_COUNT)
            except: pass
    else:
        cbuffer = None
except ImportError:
    logging.warning("cbuffer is not supported")
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

        def extend(self, o):
            if not isinstance(o, BaseBuffer):
                raise TypeError('not Buffer')

            while o:
                data = o.next()
                if isinstance(data, tuple):
                    self.write(*data)
                else:
                    self.write(data)

        def fetch(self, o, size=-1):
            if not isinstance(o, BaseBuffer):
                raise TypeError('not Buffer')

            data = o.read(size)
            self.write(data)
            return len(data)

        def copyfrom(self, o, size=-1):
            if not isinstance(o, BaseBuffer):
                raise TypeError('not Buffer')

            if size < 0:
                data = o.join()
            else:
                data = o.join()[:size]
            self.write(data)
            return len(data)

        def clear(self):
            self._buffer = b''
            self._buffer_odata = None
            self._buffer_len = 0
            self._buffers = deque()
            self._buffers_odata = deque()
            self._len = 0
            self._buffer_index = 0

        def head(self):
            if not self._buffer_odata:
                return self._buffer
            return (self._buffer, self._buffer_odata)

        def head_data(self):
            return self._buffer_odata

        def last(self):
            if not self._buffers:
                return self.head()

            if not self._buffers_odata[-1]:
                return self._buffers[-1]
            return (self._buffers[-1], self._buffers_odata[-1])

        def last_data(self):
            if not self._buffers_odata:
                return self.head_data()
            return self._buffers_odata[-1]

        def __len__(self):
            return self._len

        def __str__(self):
            buffer = self.join()
            return buffer.__str__()

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
        self._drain_size = int(max_buffer_size or MAX_BUFFER_SIZE)
        self._regain_size = int(self._drain_size * BUFFER_DRAIN_RATE)
        self._drain_time = time.time()
        self._regain_time = time.time()

    @property
    def full(self):
        return self._full

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

    def write(self, data, odata=None):
        if odata is None:
            BaseBuffer.write(self, data)
        else:
            BaseBuffer.write(self, data, odata)

        if self._len > self._drain_size and not self._full:
            self.do_drain()
        return self

    def extend(self, o):
        BaseBuffer.extend(self, o)

        if self._len > self._drain_size and not self._full:
            self.do_drain()

        if o._full and o._len < o._regain_size:
            o.do_regain()
        return o

    def fetch(self, o, size=-1):
        r = BaseBuffer.fetch(self, o, size)

        if self._len > self._drain_size and not self._full:
            self.do_drain()

        if o._full and o._len < o._regain_size:
            o.do_regain()
        return r

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

    def clear(self):
        BaseBuffer.clear(self)

        if self._full and self._len < self._regain_size:
            self.do_regain()

    def link(self, o):
        def do_drain(b):
            if o._full:
                return
            return o.do_drain()

        def do_regain(b):
            if not o._full:
                return
            if o._len > o._regain_size:
                return
            return o.do_regain()

        self.on_drain(do_drain)
        self.on_regain(do_regain)
        return self

    def close(self):
        self.remove_all_listeners()

    def decode(self, *args, **kwargs):
        data = self.join()
        return data.decode(*args, **kwargs)

    def items(self):
        return self._buffers

    def __getitem__(self, item):
        data = self.join()
        return data.__getitem__(item)

    def __iter__(self):
        data = self.join()
        return iter(data)

    def __contains__(self, item):
        data = self.join()

        return data.__contains__(item)

    def __add__(self, other):
        data = self.join()
        return data.__add__(other)

    def __bytes__(self):
        return self.join()

    def __cmp__(self, other):
        data = self.join()
        return data.__cmp__(other)

    def __eq__(self, other):
        data = self.join()
        return data == other

    def __gt__(self, other):
        data = self.join()
        return data > other

    def __lt__(self, other):
        data = self.join()
        return data < other

    def __ge__(self, other):
        data = self.join()
        return data >= other

    def __le__(self, other):
        data = self.join()
        return data <= other

    def __ne__(self, other):
        data = self.join()
        return data != other
