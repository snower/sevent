# -*- coding: utf-8 -*-
# 2014/12/28
# create by: snower

import logging
import socket
import errno
from collections import deque, defaultdict
from .event import EventEmitter
from .loop import instance, MODE_IN, MODE_OUT
from .dns import DNSResolver
from .buffer import Buffer

STATE_STREAMING = 0x01
STATE_BINDING = 0x02
STATE_CLOSING = 0x04
STATE_CLOSED = 0x06

RECV_BUFSIZE = 4096

class Socket(EventEmitter):
    MAX_BUFFER_SIZE = None

    @classmethod
    def config(cls, max_buffer_size, **kwargs):
        cls.MAX_BUFFER_SIZE = max_buffer_size

    def __init__(self, loop=None, dns_resolver=None, max_buffer_size = None):
        super(Socket, self).__init__()
        self._loop = loop or instance()
        self._dns_resolver = dns_resolver
        if self._dns_resolver is None:
            class DNSResolverProxy(object):
                def __getattr__(proxy, name):
                    if isinstance(self._dns_resolver, DNSResolverProxy):
                        self._dns_resolver = DNSResolver.default()
                    return getattr(self._dns_resolver, name)
            self._dns_resolver = DNSResolverProxy()

        self._socket = None
        self._read_handler = None
        self._write_handler = None

        self._create_socket()

        def init_buffer():
            return Buffer(max_buffer_size = max_buffer_size or self.MAX_BUFFER_SIZE)

        self._rbuffers = defaultdict(init_buffer)
        self._wbuffers = deque()
        self._address_cache = {}
        self._state = STATE_STREAMING

    def _create_socket(self):
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.SOL_UDP)
        self._socket.setblocking(False)
        self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._read_handler = self._loop.add_fd(self._socket, MODE_IN, self._read_cb)
        self._write_handler = None

    def __del__(self):
        self.close()

    def on_data(self, callback):
        self.on("data", callback)

    def on_close(self, callback):
        self.on("close", callback)

    def on_error(self, callback):
        self.on("error", callback)

    def on_drain(self, callback):
        self.on("drain", callback)

    def once_data(self, callback):
        self.once("data", callback)

    def once_close(self, callback):
        self.once("close", callback)

    def once_error(self, callback):
        self.once("error", callback)

    def once_drain(self, callback):
        self.once("drain", callback)

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
            self._loop.remove_fd(self._socket, self._read_cb)
            self._read_handler = False
        if self._write_handler:
            self._loop.remove_fd(self._socket, self._write_cb)
            self._write_handler = False
        self._socket.close()
        self._state = STATE_CLOSED
        def on_close():
            self.emit('close', self)
            self.remove_all_listeners()
            self._rbuffers = None
            self._wbuffers = None
            self._address_cache = None
        self._loop.async(on_close)

    def _error(self, error):
        self._loop.async(self.emit, 'error', self, error)
        logging.error("socket error: %s", error)
        self.close()

    def drain(self):
        if self._state in (STATE_STREAMING, STATE_BINDING):
            if self._read_handler:
                self._loop.remove_fd(self._socket, self._read_cb)
                self._read_handler = False

    def regain(self):
        if self._state in (STATE_STREAMING, STATE_BINDING):
            if not self._read_handler:
                self._read_handler = self._loop.add_fd(self._socket, MODE_IN, self._read_cb)

    def _read_cb(self):
        if self._state in (STATE_STREAMING, STATE_BINDING):
            self._read()

    def _read(self):
        recv_addresses = set([])
        while self._read_handler:
            try:
                data, address = self._socket.recvfrom(RECV_BUFSIZE)
                self._rbuffers[address].write(data)
                recv_addresses.add(address)
            except socket.error as e:
                if e.args[0] in (errno.EWOULDBLOCK, errno.EAGAIN):
                    break
                else:
                    return self._error(e)

        for address in list(recv_addresses):
            self._loop.async(self.emit, 'data', self, address, self._rbuffers[address])

        if len(self._rbuffers) > 64:
            for address in self._rbuffers.keys():
                if not self._rbuffers[address]:
                    del self._rbuffers[address]

    def _write_cb(self):
        if self._state in (STATE_STREAMING, STATE_CLOSING, STATE_BINDING):
            if self._write():
                self._loop.async(self.emit, 'drain', self)

    def _write(self):
        while self._wbuffers:
            address, data = self._wbuffers[0]
            if data.__class__ == Buffer:
                try:
                    if data._index > 0:
                        r = self._socket.sendto(memoryview(data._buffer)[data._index:], address)
                    else:
                        r = self._socket.sendto(data._buffer, address)
                    data._index += r
                    data._len -= r

                    if data._index >= data._buffer_len:
                        if data._len > 0:
                            data._buffer = data._buffers.popleft()
                            data._index, data._buffer_len = 0, len(data._buffer)
                        else:
                            data._index, data._buffer_len = 0, 0
                            data._writting = False
                            self._wbuffers.popleft()
                        continue
                    else:
                        return False
                except socket.error as e:
                    if e.args[0] not in (errno.EWOULDBLOCK, errno.EAGAIN):
                        self._error(e)
                    return False
            else:
                try:
                    self._wbuffers.popleft()
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
            self._loop.remove_fd(self._socket, self._write_cb)
            self._write_handler = False

        if self._state == STATE_CLOSING:
            self.close()
        return True

    def write(self, address, data):
        if self._state not in (STATE_STREAMING, STATE_BINDING):
            return False

        if data.__class__ == Buffer:
            if data._writting:
                return False
            data._writting = True

        def do_write(address):
            if self._state not in (STATE_STREAMING, STATE_BINDING):
                return False

            self._wbuffers.append((address, data))
            if not self._write_handler:
                if self._write():
                    self._loop.async(self.emit, 'drain', self)
                    return True
                self._write_handler = self._loop.add_fd(self._socket, MODE_OUT, self._write_cb)
                if not self._write_handler:
                    self._error(Exception("write data add fd error"))
            return False

        if address[0] not in self._address_cache:
            def resolve_callback(hostname, ip):
                if not ip:
                    return self._error(Exception('can not resolve hostname %s' % str(address)))
                self._address_cache[hostname] = ip
                do_write((ip, address[1]))
            self._dns_resolver.resolve(address[0], resolve_callback)
            return False
        else:
            ip = self._address_cache[address[0]]
            return do_write((ip, address[1]))

class Socket6(Socket):
    def _create_socket(self):
        self._socket = socket.socket(socket.AF_INET6, socket.SOCK_DGRAM, socket.SOL_UDP)
        self._socket.setblocking(False)
        self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._read_handler = self._loop.add_fd(self._socket, MODE_IN, self._read_cb)
        self._write_handler = None

class Server(Socket):
    def __init__(self, loop=None):
        super(Server, self).__init__(loop)

    def bind(self, address):
        def do_bind(hostname, ip):
            if not ip:
                self.close()
            elif self._state == STATE_BINDING:
                self._socket.bind((ip, address[1]))

        self._state = STATE_BINDING
        self._dns_resolver.resolve(address[0], do_bind)

    def __del__(self):
        self.close()

class Server6(Server):
    def _create_socket(self):
        self._socket = socket.socket(socket.AF_INET6, socket.SOCK_DGRAM, socket.SOL_UDP)
        self._socket.setblocking(False)
        self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._read_handler = self._loop.add_fd(self._socket, MODE_IN, self._read_cb)
        self._write_handler = None
