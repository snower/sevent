# -*- coding: utf-8 -*-
# 2014/12/28
# create by: snower

import logging
import socket
import errno
from .event import EventEmitter
from .loop import instance, MODE_IN, MODE_OUT
from .dns import DNSResolver
from .buffer import Buffer, cbuffer, RECV_BUFFER_SIZE

STATE_STREAMING = 0x01
STATE_BINDING = 0x02
STATE_CLOSING = 0x04
STATE_CLOSED = 0x06

class Socket(EventEmitter):
    MAX_BUFFER_SIZE = None
    RECV_BUFFER_SIZE = RECV_BUFFER_SIZE

    @classmethod
    def config(cls, max_buffer_size=None, recv_buffer_size=RECV_BUFFER_SIZE, **kwargs):
        cls.MAX_BUFFER_SIZE = max_buffer_size
        cls.RECV_BUFFER_SIZE = recv_buffer_size

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
        self._has_drain_event = False

        self._create_socket()

        self._max_buffer_size = max_buffer_size or self.MAX_BUFFER_SIZE
        self._rbuffers = Buffer(max_buffer_size = self._max_buffer_size)
        self._wbuffers = None
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

    def on(self, event_name, callback):
        super(Socket, self).on(event_name, callback)

        if event_name == "drain":
            self._has_drain_event = True

    def once(self, event_name, callback):
        super(Socket, self).once(event_name, callback)

        if event_name == "drain":
            self._has_drain_event = True

    def remove_listener(self, event_name, callback):
        super(Socket, self).remove_listener(event_name, callback)

        if not self._events[event_name] and not self._events_once[event_name]:
            if event_name == "drain":
                self._has_drain_event = False

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
            if not self._write_handler:
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
            try:
                self.emit_close(self)
            except Exception as e:
                logging.exception("tcp emit close error:%s", e)
            self.remove_all_listeners()
            self._rbuffers = None
            self._wbuffers = None
            self._address_cache = None
        self._loop.add_async(on_close)

    def _error(self, error):
        self._loop.add_async(self.emit_error, self, error)
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
        last_data_len = self._rbuffers._len
        while self._read_handler:
            try:
                data, address = self._socket.recvfrom(self.RECV_BUFFER_SIZE)
                self._rbuffers.write(data, address)
            except socket.error as e:
                if e.args[0] in (errno.EWOULDBLOCK, errno.EAGAIN):
                    break
                else:
                    return self._error(e)

        if last_data_len < self._rbuffers._len:
            self._loop.add_async(self.emit_data, self, self._rbuffers)

    def _write_cb(self):
        if self._state in (STATE_STREAMING, STATE_CLOSING, STATE_BINDING):
            if self._write():
                if self._has_drain_event:
                    self._loop.add_async(self.emit_drain, self)
                if self._write_handler:
                    self._loop.remove_fd(self._socket, self._write_cb)
                    self._write_handler = False

                if self._state == STATE_CLOSING:
                    self.close()

    if cbuffer is None:
        def _write(self):
            while self._wbuffers:
                data = self._wbuffers
                address = self._wbuffers._buffer_odata
                try:
                    if data._buffer_index > 0:
                        r = self._socket.sendto(memoryview(data._buffer)[data._buffer_index:], address)
                    else:
                        r = self._socket.sendto(data._buffer, address)
                    data._buffer_index += r
                    data._len -= r

                    if data._buffer_index >= data._buffer_len:
                        if data._len > 0:
                            data._buffer = data._buffers.popleft()
                            data._buffer_odata = data._buffers_odata.popleft()
                            data._buffer_index, data._buffer_len = 0, len(data._buffer)
                        else:
                            data._buffer_index, data._buffer_len, data._buffer, data._buffer_odata = 0, 0, b'', None
                            if data._full and data._len < data._regain_size:
                                data.do_regain()
                        continue
                    else:
                        if data._full and data._len < data._regain_size:
                            data.do_regain()
                        return False
                except socket.error as e:
                    if e.args[0] not in (errno.EWOULDBLOCK, errno.EAGAIN):
                        self._error(e)
                    if data._full and data._len < data._regain_size:
                        data.do_regain()
                    return False
            return True
    else:
        def _write(self):
            while self._wbuffers:
                data, address = self._wbuffers._buffer
                try:
                    if self._wbuffers._buffer_index > 0:
                        r = self._socket.sendto(memoryview(data)[self._wbuffers._buffer_index:], address)
                    else:
                        r = self._socket.sendto(data, address)
                    if r > 0:
                        self._wbuffers.read(r)

                    if self._wbuffers._buffer_index < self._wbuffers._buffer_len:
                        if self._wbuffers._full and self._wbuffers._len < self._wbuffers._regain_size:
                            self._wbuffers.do_regain()
                        return False
                except socket.error as e:
                    if e.args[0] not in (errno.EWOULDBLOCK, errno.EAGAIN):
                        self._error(e)
                    if self._wbuffers._full and self._wbuffers._len < self._wbuffers._regain_size:
                        self._wbuffers.do_regain()
                    return False

            if self._wbuffers._full and self._wbuffers._len < self._wbuffers._regain_size:
                self._wbuffers.do_regain()
            return True

    def write(self, data):
        if self._state not in (STATE_STREAMING, STATE_BINDING):
            return False

        def do_write():
            if self._state not in (STATE_STREAMING, STATE_BINDING):
                return False

            if not self._write_handler:
                if self._write():
                    if self._has_drain_event:
                        self._loop.add_async(self.emit_drain, self)
                    return True
                self._write_handler = self._loop.add_fd(self._socket, MODE_OUT, self._write_cb)
                if not self._write_handler:
                    self._error(Exception("write data add fd error"))
            return False

        if data.__class__ == Buffer:
            if self._wbuffers != data:
                if not self._wbuffers:
                    self._wbuffers = data
                else:
                    while data:
                        self._wbuffers.write(*data.next())
            return do_write()
        else:
            if self._wbuffers is None:
                self._wbuffers = Buffer(max_buffer_size = self._max_buffer_size)

        data, address = data
        if address[0] not in self._address_cache:
            def resolve_callback(hostname, ip):
                if not ip:
                    return self._error(Exception('can not resolve hostname %s' % str(address)))
                self._address_cache[hostname] = ip
                self._wbuffers.write(data, (ip, address[1]))
                return do_write()
            self._dns_resolver.resolve(address[0], resolve_callback)
            return False
        else:
            ip = self._address_cache[address[0]]
            self._wbuffers.write(data, (ip, address[1]))
            return do_write()

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
