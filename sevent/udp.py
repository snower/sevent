# -*- coding: utf-8 -*-
# 2014/12/28
# create by: snower

import logging
import socket
import errno
from collections import deque
from event import EventEmitter
from loop import instance, MODE_IN, MODE_OUT

STATE_STREAMING = 0x01
STATE_BINDING = 0x02
STATE_CLOSING = 0x04
STATE_CLOSED = 0x06

RECV_BUFSIZE = 4096

class Socket(EventEmitter):
    def __init__(self, loop=None):
        super(Socket, self).__init__()
        self._loop = loop or instance()
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.SOL_UDP)
        self._socket.setblocking(False)
        self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._read_handler = self._loop.add_fd(self._socket, MODE_IN, self._read_cb)
        self._write_handler = None
        self._rbuffers = deque()
        self._wbuffers = deque()
        self._state = STATE_STREAMING

    def __del__(self):
        self.close()

    def end(self):
        if self._state in (STATE_STREAMING, STATE_BINDING):
            if not self._wbuffers:
                self.close()
            else:
                self._state = STATE_CLOSING

    def close(self):
        if self._state == STATE_CLOSED:
            return
        if self._read_handler:
            self._loop.remove_handler(self._read_handler)
        if self._write_handler:
            self._loop.remove_handler(self._write_handler)
        self._socket.close()
        self._state = STATE_CLOSED
        self._loop.sync(self.emit, 'close', self)
        def on_close():
            self.emit('close', self)
            self.remove_all_listeners()
        self._loop.sync(on_close)

    def _error(self, error):
        self._loop.sync(self.emit,'error', self, error)
        logging.error("socket error:%s",error)
        self.close()

    def _read_cb(self):
        if self._state in (STATE_STREAMING, STATE_BINDING):
            self._read()

    def _read(self):
        while True:
            try:
                data, address = self._socket.recvfrom(RECV_BUFSIZE)
                self._rbuffers.append(data)
            except socket.error as e:
                if e.args[0] in (errno.EWOULDBLOCK, errno.EAGAIN):break
                else:
                    return self._error(e)

        if self._rbuffers:
            self._loop.sync(self.emit, 'data', self, ''.join(self._rbuffers))
            self._rbuffers = deque()

    def _write_cb(self):
        if self._state in (STATE_STREAMING, STATE_CLOSING, STATE_BINDING):
            if self._write():
                self._loop.sync(self.emit,'drain', self)

    def _write(self):
        while True:
            address, data = self._wbuffers.popleft()
            try:
                r = self._socket.sendto(data, address)
                if r < len(data):
                    self._wbuffers.appendleft((address, data[r:]))
                    return False
            except socket.error as e:
                if e.args[0] in (errno.EWOULDBLOCK, errno.EAGAIN):
                    self._wbuffers.appendleft((address, data))
                else:
                    self._error(e)
                return False

        if self._write_handler:
            self._loop.remove_handler(self._write_handler)
            self._write_handler = None

        if self._state == STATE_CLOSING:
            self.close()
        return True

    def write(self, address, data):
        if self._state not in (STATE_STREAMING, STATE_BINDING):
            return False
        self._wbuffers.append((address, data))
        if not self._write() or self._write_handler is None:
            self._write_handler = self._loop.add_fd(self._socket, MODE_OUT, self._write_cb)
            if not self._write_handler:
                self._error(Exception("write data add fd error"))
                return False
            return True
        self._loop.sync(self.emit,'drain', self)
        return True

class Server(Socket):
    def __init__(self, loop=None):
        super(Server, self).__init__(loop)

    def bind(self, address):
        self._socket.bind(address)
        self._state = STATE_BINDING

    def __del__(self):
        self.close()