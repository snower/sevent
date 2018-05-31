# -*- coding: utf-8 -*-

import time
import logging
from collections import deque
import socket
import errno
from . import event
from .loop import instance, MODE_IN, MODE_OUT
from .buffer import Buffer
from .dns import DNSResolver

MSG_FASTOPEN = 0x20000000

STATE_INITIALIZED = 0x01
STATE_CONNECTING = 0x02
STATE_STREAMING = 0x04
STATE_LISTENING = 0x08
STATE_CLOSING = 0x10
STATE_CLOSED = 0x20

RECV_BUFSIZE = 4096

class Socket(event.EventEmitter):
    MAX_BUFFER_SIZE = None

    @classmethod
    def config(cls, max_buffer_size, **kwargs):
        cls.MAX_BUFFER_SIZE = max_buffer_size

    def __init__(self, loop=None, socket=None, address=None, dns_resolver = None, max_buffer_size = None):
        super(Socket, self).__init__()
        self._loop =loop or instance()
        self._socket = socket
        self._address = address
        self._dns_resolver = dns_resolver or DNSResolver.default()
        self._connect_handler = False
        self._connect_timeout = 5
        self._connect_timeout_handler = None
        self._read_handler = False
        self._write_handler = False
        self._rbuffers = Buffer(max_buffer_size = max_buffer_size or self.MAX_BUFFER_SIZE)
        self._wbuffers = deque()
        self._state = STATE_INITIALIZED
        self._is_enable_fast_open = False
        self._is_enable_nodelay = False
        self._is_resolve = False

        if self._socket:
            self._state = STATE_STREAMING
            self._socket.setblocking(False)
            self._read_handler = self._loop.add_fd(self._socket, MODE_IN, self._read_cb)

    @property
    def address(self):
        return self._address

    @property
    def socket(self):
        return self._socket

    def __del__(self):
        self.close()

    def on_connect(self, callback):
        self.on("connect", callback)

    def on_data(self, callback):
        self.on("data", callback)

    def on_end(self, callback):
        self.on("end", callback)

    def on_close(self, callback):
        self.on("close", callback)

    def on_error(self, callback):
        self.on("error", callback)

    def on_drain(self, callback):
        self.on("drain", callback)

    def once_connect(self, callback):
        self.once("connect", callback)

    def once_data(self, callback):
        self.once("data", callback)

    def once_end(self, callback):
        self.once("end", callback)

    def once_close(self, callback):
        self.once("close", callback)

    def once_error(self, callback):
        self.once("error", callback)

    def once_drain(self, callback):
        self.once("drain", callback)

    def enable_fast_open(self):
        self._is_enable_fast_open = True

    @property
    def is_enable_fast_open(self):
        return self._is_enable_fast_open

    def enable_nodelay(self):
        if self._socket:
            try:
                self._socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            except Exception as e:
                logging.warning('nodela error: %s', e)
                self._is_enable_nodelay = False
                return
        self._is_enable_nodelay = True

    @property
    def is_enable_nodelay(self):
        return self._is_enable_nodelay

    def end(self):
        if self._state not in (STATE_INITIALIZED, STATE_CONNECTING, STATE_STREAMING):return
        if self._state in (STATE_INITIALIZED, STATE_CONNECTING):
            self.close()
        else:
            if self._wbuffers:
                self._state = STATE_CLOSING
            else:
                self.close()

    def close(self):
        if self._state == STATE_CLOSED:
            return

        if self._state == STATE_CONNECTING and self._connect_handler:
            self._loop.remove_fd(self._socket, self._connect_cb)
            self._connect_handler = False
        elif self._state in (STATE_STREAMING, STATE_CLOSING):
            if self._read_handler:
                self._loop.remove_fd(self._socket, self._read_cb)
                self._read_handler = False
            if self._write_handler:
                self._loop.remove_fd(self._socket, self._write_cb)
                self._write_handler = False
            try:
                self._socket.close()
            except Exception as e:
                logging.error("socket close socket error:%s",e)

        self._state = STATE_CLOSED
        def on_close():
            self.emit('close', self)
            self.remove_all_listeners()
            self._rbuffers = None
            self._wbuffers = None
        self._loop.async(on_close)

    def _error(self, error):
        self._loop.async(self.emit, 'error', self, error)
        self.close()
        logging.error("socket error:%s",error)

    def _connect_cb(self):
        if self._state != STATE_CONNECTING:
            return
        self._loop.remove_fd(self._socket, self._connect_cb)
        self._connect_handler = False

        if self._connect_timeout_handler:
            self._loop.cancel_timeout(self._connect_timeout_handler)
            self._connect_timeout_handler = None

        self._state = STATE_STREAMING
        self._read_handler = self._loop.add_fd(self._socket, MODE_IN, self._read_cb)
        self._rbuffers.on("drain", lambda _ : self.drain())
        self._rbuffers.on("regain", lambda _ : self.regain())
        self._loop.async(self.emit, 'connect', self)

        if self._wbuffers and not self._write_handler:
            self._write_handler = self._loop.add_fd(self._socket, MODE_OUT, self._write_cb)
            if not self._write_handler:
                self._error(Exception("write data add fd error"))

    def connect(self, address, timeout=5):
        if self._state != STATE_INITIALIZED:
            return

        self._connect_timeout = timeout

        def on_timeout_cb():
            if self._state == STATE_CONNECTING:
                if not self._is_enable_fast_open:
                    self._error(Exception("connect time out %s" % str(address)))

        def do_connect(hostname, ip):
            if self._state == STATE_CLOSED:
                return

            if not ip:
                return self._error(Exception('can not resolve hostname %s' % str(address)))

            try:
                addrinfo = socket.getaddrinfo(ip, address[1], 0, 0, socket.SOL_TCP)
                # support both IPv4 and IPv6 addresses
                if addrinfo:
                    addr = addrinfo[0]
                    self._socket = socket.socket(addr[0], addr[1], addr[2])
                    self._socket.setblocking(False)
                    self._address = addr[4]
                    self._is_resolve = True

                    if self._is_enable_fast_open:
                        try:
                            self._socket.setsockopt(socket.SOL_TCP, 23, 5)
                        except Exception as e:
                            logging.warning('fast open error: %s', e)
                            self._is_enable_fast_open = False

                    if self._is_enable_nodelay:
                        try:
                            self._socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                        except Exception as e:
                            logging.warning('nodela error: %s', e)
                            self._is_enable_nodelay = False

                    if self._is_enable_fast_open:

                        if self._wbuffers and not self._connect_handler:
                            self._loop.async(self._connect_and_write)
                    else:
                        self._socket.connect(self._address)
                else:
                    self._error(Exception('can not resolve hostname %s' % str(address)))
                    return
            except socket.error as e:
                if e.args[0] not in (errno.EINPROGRESS, errno.EWOULDBLOCK):
                    self._error(Exception("connect error %s %s" % (str(address), e)))
                    return

            if not self._is_enable_fast_open:
                self._connect_handler = self._loop.add_fd(self._socket, MODE_OUT, self._connect_cb)

        self._dns_resolver.resolve(address[0], do_connect)
        self._connect_timeout_handler = self._loop.timeout(timeout, on_timeout_cb)
        self._state = STATE_CONNECTING

    def drain(self):
        if self._state in (STATE_STREAMING, STATE_CLOSING):
            if self._read_handler:
                self._loop.remove_fd(self._socket, self._read_cb)
                self._read_handler = False

    def regain(self):
        if self._state in (STATE_STREAMING, STATE_CLOSING):
            if not self._read_handler:
                self._read_handler = self._loop.add_fd(self._socket, MODE_IN, self._read_cb)

    def _read_cb(self):
        if self._state not in (STATE_STREAMING, STATE_CLOSING):
            if self._state == STATE_CONNECTING:
                if self._read():
                    self._connect_cb()
                    self._loop.async(self.emit, 'data', self, self._rbuffers)
                else:
                    if self._rbuffers._len:
                        self._connect_cb()
                        self._loop.async(self.emit, 'data', self, self._rbuffers)
                    self._loop.async(self.emit, 'end', self)
                    self.close()
            return

        if self._read():
            self._loop.async(self.emit, 'data', self, self._rbuffers)
        else:
            if self._rbuffers._len:
                self._loop.async(self.emit, 'data', self, self._rbuffers)
            self._loop.async(self.emit, 'end', self)
            if self._state in (STATE_STREAMING, STATE_CLOSING):
                self.close()

    def _read(self):
        last_data_len = self._rbuffers._len
        while self._read_handler:
            try:
                data = self._socket.recv(RECV_BUFSIZE)
                if not data:
                    return False
                self._rbuffers.write(data)
            except socket.error as e:
                if e.args[0] in (errno.EWOULDBLOCK, errno.EAGAIN):
                    break
                else:
                    self._error(e)
                    return False

        return last_data_len < self._rbuffers._len

    def _write_cb(self):
        if self._state not in (STATE_STREAMING, STATE_CLOSING):
            if self._state == STATE_CONNECTING:
                self._connect_cb()
                if self._wbuffers:
                    if self._write():
                        self._loop.async(self.emit, 'drain', self)
            return

        if self._write():
            self._loop.async(self.emit, 'drain', self)

    def _connect_and_write(self):
        if self._wbuffers:
            def on_timeout_cb():
                if self._state == STATE_CONNECTING:
                    if self._is_enable_fast_open:
                        self._error(Exception("connect time out %s:%s" % (self._address[0], self._address[1])))

            if self._connect_timeout_handler:
                self._loop.cancel_timeout(self._connect_timeout_handler)
                self._connect_timeout_handler = None
            self._connect_timeout_handler = self._loop.timeout(self._connect_timeout, on_timeout_cb)

            data = self._wbuffers[0]
            if data.__class__ == Buffer:
                try:
                    if data._index == 0 and data._buffer_len == data._len:
                        r = self._socket.sendto(data._buffer, MSG_FASTOPEN, self.address)
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
                    else:
                        buf_len = data._len
                        buf = data.read(-1)
                        r = self._socket.sendto(buf, MSG_FASTOPEN, self.address)
                        if r >= buf_len:
                            self._wbuffers.popleft()
                        else:
                            data.write(buf[r:])

                    self._connect_handler = self._loop.add_fd(self._socket, MODE_OUT, self._connect_cb)
                except socket.error as e:
                    if e.args[0] == errno.EINPROGRESS:
                        self._connect_handler = self._loop.add_fd(self._socket, MODE_OUT, self._connect_cb)
                        return False
                    elif e.args[0] == errno.ENOTCONN:
                        self._is_enable_fast_open = False
                    self._error(e)
                    return False
            else:
                try:
                    data = b"".join(self._wbuffers)
                    self._wbuffers.clear()
                    r = self._socket.sendto(data, MSG_FASTOPEN, self.address)
                    if r < len(data):
                        self._wbuffers.appendleft(data[r:])
                    self._connect_handler = self._loop.add_fd(self._socket, MODE_OUT, self._connect_cb)
                except socket.error as e:
                    if e.args[0] == errno.EINPROGRESS:
                        self._wbuffers.appendleft(data)
                        self._connect_handler = self._loop.add_fd(self._socket, MODE_OUT, self._connect_cb)
                        return False
                    elif e.args[0] == errno.ENOTCONN:
                        logging.error('fast open not supported on this OS')
                        self._is_enable_fast_open = False
                    self._error(e)
                    return False

    def _write(self):
        while self._wbuffers:
            data = self._wbuffers[0]
            if data.__class__ == Buffer:
                try:
                    if data._index > 0:
                        r = self._socket.send(memoryview(data._buffer)[data._index:])
                    else:
                        r = self._socket.send(data._buffer)
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
                    r = self._socket.send(data)
                    if r < len(data):
                        self._wbuffers.appendleft(data[r:])
                        return False
                except socket.error as e:
                    if e.args[0] in (errno.EWOULDBLOCK, errno.EAGAIN):
                        self._wbuffers.appendleft(data)
                    else:
                        self._error(e)
                    return False

        if self._write_handler:
            self._loop.remove_fd(self._socket, self._write_cb)
            self._write_handler = False
        if self._state == STATE_CLOSING:
            self.close()
        return True

    def write(self, data):
        if data.__class__ == Buffer:
            if data._writting:
                return False
            data._writting = True

        if self._state != STATE_STREAMING:
            if self._state == STATE_CONNECTING:
                self._wbuffers.append(data)
                if self._is_enable_fast_open and self._is_resolve and not self._connect_handler:
                    return self._connect_and_write()
                return False
            assert self._state == STATE_STREAMING, "not connected"

        self._wbuffers.append(data)
        if not self._write_handler:
            if self._write():
                self._loop.async(self.emit, 'drain', self)
                return True
            self._write_handler = self._loop.add_fd(self._socket, MODE_OUT, self._write_cb)
            if not self._write_handler:
                self._error(Exception("write data add fd error"))
        return False


class Socket6(Socket):
    pass


class Server(event.EventEmitter):
    def __init__(self, loop=None, dns_resolver = None):
        super(Server, self).__init__()
        self._loop = loop or instance()
        self._dns_resolver = dns_resolver or DNSResolver.default()
        self._socket = None
        self._state = STATE_INITIALIZED
        self._accept_handler = False
        self._is_enable_fast_open = False
        self._is_reuseaddr = False
        self._is_enable_nodelay = False
        self._is_resolve = False

    def __del__(self):
        self.close()

    def on_connection(self, callback):
        self.on("connection", callback)

    def on_close(self, callback):
        self.on("close", callback)

    def on_error(self, callback):
        self.on("error", callback)

    def enable_fast_open(self):
        self._is_enable_fast_open = True

    def enable_reuseaddr(self):
        self._is_reuseaddr = True

    def enable_nodelay(self):
        self._is_enable_nodelay = True

    @property
    def is_enable_fast_open(self):
        return self._is_enable_fast_open

    @property
    def is_reuseaddr(self):
        return self._is_reuseaddr

    @property
    def is_enable_nodelay(self):
        return self._is_enable_nodelay

    def listen(self, address, backlog=128):
        if self._state != STATE_INITIALIZED:
            return

        def do_listen(hostname, ip):
            if not ip:
                return self._error(Exception('can not resolve hostname %s' % str(address)))

            addrinfo = socket.getaddrinfo(ip, address[1], 0, 0, socket.SOL_TCP)
            if addrinfo:
                addr = addrinfo[0]
                self._socket = socket.socket(addr[0], addr[1], addr[2])
                self._socket.setblocking(False)
                self._is_resolve = True

                if self._is_reuseaddr:
                    try:
                        self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                    except Exception as e:
                        logging.warning('reuseaddr error: %s', e)
                        self._is_reuseaddr = False

                if self._is_enable_fast_open:
                    try:
                        self._socket.setsockopt(socket.SOL_TCP, 23, 5)
                    except Exception as e:
                        logging.warning('fast open error: %s', e)
                        self._is_enable_fast_open = False

                self._socket.bind(addr[4])
                self._accept_handler = self._loop.add_fd(self._socket, MODE_IN, self._accept_cb)
                self._socket.listen(backlog)
            else:
                self._error(Exception('can not resolve hostname %s' % str(address)))

        self._dns_resolver.resolve(address[0], do_listen)
        self._state = STATE_LISTENING

    def _accept_cb(self):
        if self._state != STATE_LISTENING:
            return

        connection, address = self._socket.accept()
        socket = Socket(loop=self._loop, socket=connection, address=address)
        if self._is_enable_nodelay:
            socket.enable_nodelay()
        self._loop.async(self.emit, "connection", self, socket)

    def _error(self, error):
        self._loop.async(self.emit, 'error', self, error)
        self.close()
        logging.error("server error: %s", error)

    def close(self):
        if self._state in (STATE_INITIALIZED, STATE_LISTENING):
            if self._accept_handler:
                self._loop.remove_fd(self._socket, self._accept_cb)
                self._accept_handler = False
            if self._socket is not None:
                try:
                    self._socket.close()
                except Exception as e:
                    logging.error("server close socket error: %s", e)
            self._state = STATE_CLOSED
            def on_close():
                self.emit('close', self)
                self.remove_all_listeners()
            self._loop.async(on_close)


class Server6(Server):
    pass
