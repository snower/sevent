# -*- coding: utf-8 -*-

import socket
import errno
from .utils import is_py3, get_logger
from .event import EventEmitter, null_emit_callback
from .loop import instance, MODE_IN, MODE_OUT
from .buffer import Buffer, BaseBuffer, cbuffer, RECV_BUFFER_SIZE
from .dns import DNSResolver
from .errors import SocketClosed, ResolveError, ConnectTimeout, AddressError, ConnectError

MSG_FASTOPEN = 0x20000000

STATE_INITIALIZED = 0x01
STATE_CONNECTING = 0x02
STATE_STREAMING = 0x04
STATE_LISTENING = 0x08
STATE_CLOSING = 0x10
STATE_CLOSED = 0x20


class Socket(EventEmitter):
    MAX_BUFFER_SIZE = None
    RECV_BUFFER_SIZE = RECV_BUFFER_SIZE

    @classmethod
    def config(cls, max_buffer_size=None, recv_buffer_size=RECV_BUFFER_SIZE, **kwargs):
        cls.MAX_BUFFER_SIZE = max_buffer_size
        cls.RECV_BUFFER_SIZE = recv_buffer_size

    def __init__(self, loop=None, socket=None, address=None, dns_resolver=None, max_buffer_size=None):
        EventEmitter.__init__(self)
        self._loop = loop or instance()
        self._socket = socket
        self._fileno = 0
        self._socket_family = 2
        self._address = address
        self._dns_resolver = dns_resolver or DNSResolver.default()
        self._connect_handler = False
        self._connect_timeout = 5
        self._connect_timeout_handler = None
        self._read_handler = False
        self._write_handler = False
        self._max_buffer_size = max_buffer_size or self.MAX_BUFFER_SIZE
        self._rbuffers = Buffer(max_buffer_size=self._max_buffer_size)
        self._wbuffers = Buffer(max_buffer_size=self._max_buffer_size)
        self._state = STATE_INITIALIZED
        self._is_enable_fast_open = False
        self._is_enable_nodelay = False
        self._is_resolve = False
        self._has_drain_event = False
        self.ignore_write_closed_error = False

        if self._socket:
            self._state = STATE_STREAMING
            try:
                self._fileno = self._socket.fileno()
                self._socket.setblocking(False)
                self._read_handler = self._loop.add_fd(self._fileno, MODE_IN, self._read_cb)

                self._rbuffers.on("drain", lambda _: self.drain())
                self._rbuffers.on("regain", lambda _: self.regain())
            except Exception as e:
                self._loop.add_async(self._error, e)

    @property
    def state(self):
        return self._state

    @property
    def address(self):
        return self._address

    @property
    def socket(self):
        return self._socket

    @property
    def buffer(self):
        return self._rbuffers, self._wbuffers

    def __del__(self):
        self.close()

    def on(self, event_name, callback):
        EventEmitter.on(self, event_name, callback)

        if event_name == "drain":
            self._has_drain_event = True

    def once(self, event_name, callback):
        EventEmitter.once(self, event_name, callback)

        if event_name == "drain":
            self._has_drain_event = True

    def off(self, event_name, callback):
        EventEmitter.off(self, event_name, callback)

        if not self._events[event_name] and not self._events_once[event_name]:
            if event_name == "drain":
                self._has_drain_event = False

    def noce(self, event_name, callback):
        EventEmitter.noce(self, event_name, callback)

        if not self._events[event_name] and not self._events_once[event_name]:
            if event_name == "drain":
                self._has_drain_event = False

    def remove_listener(self, event_name, callback):
        EventEmitter.remove_listener(self, event_name, callback)

        if not self._events[event_name] and not self._events_once[event_name]:
            if event_name == "drain":
                self._has_drain_event = False

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

    def off_connect(self, callback):
        self.on("connect", callback)

    def off_data(self, callback):
        self.on("data", callback)

    def off_end(self, callback):
        self.on("end", callback)

    def off_close(self, callback):
        self.on("close", callback)

    def off_error(self, callback):
        self.on("error", callback)

    def off_drain(self, callback):
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

    def noce_connect(self, callback):
        self.once("connect", callback)

    def noce_data(self, callback):
        self.once("data", callback)

    def noce_end(self, callback):
        self.once("end", callback)

    def noce_close(self, callback):
        self.once("close", callback)

    def noce_error(self, callback):
        self.once("error", callback)

    def noce_drain(self, callback):
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
                get_logger().warning('nodela error: %s', e)
                self._is_enable_nodelay = False
                return
        self._is_enable_nodelay = True

    @property
    def is_enable_nodelay(self):
        return self._is_enable_nodelay

    def end(self):
        if self._state not in (STATE_INITIALIZED, STATE_CONNECTING, STATE_STREAMING):
            return

        if self._state in (STATE_INITIALIZED, STATE_CONNECTING):
            self._loop.add_async(self.close)
        else:
            if self._write_handler:
                self._state = STATE_CLOSING
            else:
                self._loop.add_async(self.close)

    def close(self):
        if self._state == STATE_CLOSED:
            return

        if self._state == STATE_CONNECTING and self._connect_handler:
            try:
                self._loop.remove_fd(self._fileno, self._connect_cb)
            except Exception as e:
                get_logger().error("socket close remove_fd error:%s", e)
            self._connect_handler = False
            if self._connect_timeout_handler:
                self._loop.cancel_timeout(self._connect_timeout_handler)
                self._connect_timeout_handler = None
        elif self._state in (STATE_STREAMING, STATE_CLOSING):
            if self._read_handler:
                try:
                    self._loop.remove_fd(self._fileno, self._read_cb)
                except Exception as e:
                    get_logger().error("socket close remove_fd error:%s", e)
                self._read_handler = False
            if self._write_handler:
                try:
                    self._loop.remove_fd(self._fileno, self._write_cb)
                except Exception as e:
                    get_logger().error("socket close remove_fd error:%s", e)
                self._write_handler = False

        self._state = STATE_CLOSED
        def on_close():
            if self._socket:
                try:
                    self._loop.clear_fd(self._fileno)
                except Exception as e:
                    get_logger().error("server close clear_fd error: %s", e)
                try:
                    self._socket.close()
                except Exception as e:
                    get_logger().error("socket close socket error: %s", e)

            try:
                self.emit_close(self)
            except Exception as e:
                get_logger().exception("tcp emit close error:%s", e)
            self.remove_all_listeners()
            self._rbuffers.close()
            self._wbuffers.close()
            self._rbuffers = None
            self._wbuffers = None
        self._loop.add_async(on_close)

    def _error(self, error):
        self._loop.add_async(self.emit_error, self, error)
        self._loop.add_async(self.close)
        if self.emit_error == null_emit_callback:
            get_logger().error("TCP %s socket %s error: %s", self, self.socket, error)

    def _connect_cb(self):
        if self._state != STATE_CONNECTING:
            return
        try:
            self._loop.remove_fd(self._fileno, self._connect_cb)
        except Exception as e:
            return self._error(e)
        self._connect_handler = False

        if self._connect_timeout_handler:
            self._loop.cancel_timeout(self._connect_timeout_handler)
            self._connect_timeout_handler = None

        self._state = STATE_STREAMING
        try:
            self._read_handler = self._loop.add_fd(self._fileno, MODE_IN, self._read_cb)
        except Exception as e:
            return self._error(e)

        self._rbuffers.on("drain", lambda _: self.drain())
        self._rbuffers.on("regain", lambda _: self.regain())
        self._loop.add_async(self.emit_connect, self)

        if self._wbuffers and not self._write_handler:
            try:
                self._write_handler = self._loop.add_fd(self._fileno, MODE_OUT, self._write_cb)
            except Exception as e:
                return self._error(e)

    def connect(self, address, timeout=5):
        if self._state != STATE_INITIALIZED:
            if self._state == STATE_CLOSED:
                raise SocketClosed()
            return

        self._connect_timeout = timeout

        def on_timeout_cb():
            self._connect_timeout_handler = None
            if self._state == STATE_CONNECTING:
                if not self._is_enable_fast_open:
                    self._error(ConnectTimeout("connect time out %s" % str(address)))

        def do_connect(hostname, ip):
            if self._state == STATE_CLOSED:
                return

            if not ip:
                return self._loop.add_async(self._error, ResolveError('can not resolve hostname %s' % str(address)))

            try:
                addrinfo = socket.getaddrinfo(ip, address[1], 0, 0, socket.SOL_TCP)
                if not addrinfo:
                    return self._loop.add_async(self._error, AddressError('address info unknown %s' % str(address)))

                addr = addrinfo[0]
                self._socket = socket.socket(addr[0], addr[1], addr[2])
                self._fileno = self._socket.fileno()
                self._socket_family = addr[0]
                self._socket.setblocking(False)
                self._address = addr[4]
                self._is_resolve = True

                if self._is_enable_fast_open:
                    try:
                        self._socket.setsockopt(socket.SOL_TCP, 23, 5)
                    except Exception as e:
                        get_logger().warning('fast open error: %s', e)
                        self._is_enable_fast_open = False

                if self._is_enable_nodelay:
                    try:
                        self._socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                    except Exception as e:
                        get_logger().warning('nodela error: %s', e)
                        self._is_enable_nodelay = False

                if self._is_enable_fast_open:
                    if self._wbuffers and not self._connect_handler:
                        self._loop.add_async(self._connect_and_write)
                else:
                    self._connect_handler = self._loop.add_fd(self._fileno, MODE_OUT, self._connect_cb)
                    self._socket.connect(self._address)
            except socket.error as e:
                if e.args[0] not in (errno.EINPROGRESS, errno.EWOULDBLOCK):
                    return self._loop.add_async(self._error, ConnectError(address, e, "connect error %s %s" % (str(address), e)))
            except Exception as e:
                return self._loop.add_async(self._error, e)

        self._dns_resolver.resolve(address[0], do_connect)
        self._connect_timeout_handler = self._loop.add_timeout(timeout, on_timeout_cb)
        self._state = STATE_CONNECTING

    def drain(self):
        if self._state in (STATE_STREAMING, STATE_CLOSING):
            if self._read_handler:
                try:
                    self._loop.remove_fd(self._fileno, self._read_cb)
                except Exception as e:
                    return self._error(e)
                self._read_handler = False

    def regain(self):
        if self._state in (STATE_STREAMING, STATE_CLOSING):
            if not self._read_handler:
                try:
                    self._read_handler = self._loop.add_fd(self._fileno, MODE_IN, self._read_cb)
                except Exception as e:
                    self._error(e)

    def _read_cb(self):
        if self._state == STATE_CONNECTING:
            if self._read():
                self._connect_cb()
                self._loop.add_async(self.emit_data, self, self._rbuffers)
            else:
                if self._rbuffers._len:
                    self._connect_cb()
                    self._loop.add_async(self.emit_data, self, self._rbuffers)
                self._loop.add_async(self.emit_end, self)
                self.close()
            return

        if self._read():
            self._loop.add_async(self.emit_data, self, self._rbuffers)
        else:
            if self._rbuffers._len:
                self._loop.add_async(self.emit_data, self, self._rbuffers)
            self._loop.add_async(self.emit_end, self)
            if self._state in (STATE_STREAMING, STATE_CLOSING):
                self.close()

    if cbuffer is None:
        def _read(self):
            last_data_len = self._rbuffers._len
            while self._read_handler:
                try:
                    data = self._socket.recv(self.RECV_BUFFER_SIZE)
                    if not data:
                        break
                    BaseBuffer.write(self._rbuffers, data)
                except socket.error as e:
                    if e.args[0] in (errno.EWOULDBLOCK, errno.EAGAIN):
                        break
                    else:
                        self._error(e)
                        if self._rbuffers._len > self._rbuffers._drain_size and not self._rbuffers._full:
                            self._rbuffers.do_drain()
                        return False
                except Exception as e:
                    self._error(e)
                    return False
                else:
                    if self._rbuffers._len > self._rbuffers._len:
                        break

            if last_data_len < self._rbuffers._len:
                if self._rbuffers._len > self._rbuffers._drain_size and not self._rbuffers._full:
                    self._rbuffers.do_drain()
                return True
            return False
    else:
        def _read(self):
            try:
                r = self._rbuffers.socket_recv(self._fileno, self._rbuffers._drain_size)
            except Exception as e:
                self._error(e)
                if self._rbuffers._len > self._rbuffers._drain_size and not self._rbuffers._full:
                    self._rbuffers.do_drain()
                return False

            if self._rbuffers._len > self._rbuffers._drain_size and not self._rbuffers._full:
                self._rbuffers.do_drain()
            return r

    def _write_cb(self):
        if self._state == STATE_CONNECTING:
            self._connect_cb()
            if self._wbuffers:
                if self._write():
                    if self._has_drain_event:
                        self._loop.add_async(self.emit_drain, self)
                if self._write_handler:
                    try:
                        self._loop.remove_fd(self._fileno, self._write_cb)
                    except Exception as e:
                        return self._error(e)
                    self._write_handler = False
                if self._state == STATE_CLOSING:
                    self.close()
            return

        if self._write():
            if self._has_drain_event:
                self._loop.add_async(self.emit_drain, self)
            if self._write_handler:
                try:
                    self._loop.remove_fd(self._fileno, self._write_cb)
                except Exception as e:
                    return self._error(e)
                self._write_handler = False
            if self._state == STATE_CLOSING:
                self.close()

    def _connect_and_write(self):
        if self._wbuffers and self._address:
            def on_timeout_cb():
                if self._state == STATE_CONNECTING:
                    if self._is_enable_fast_open:
                        self._error(ConnectTimeout("connect time out %s" % (str(self._address),)))

            if self._connect_timeout_handler:
                self._loop.cancel_timeout(self._connect_timeout_handler)
                self._connect_timeout_handler = None
            self._connect_timeout_handler = self._loop.add_timeout(self._connect_timeout, on_timeout_cb)

            try:
                self._wbuffers.read(self._socket.sendto(self._wbuffers.join(), MSG_FASTOPEN, self._address))
                self._connect_handler = self._loop.add_fd(self._fileno, MODE_OUT, self._connect_cb)
            except socket.error as e:
                if e.args[0] == errno.EINPROGRESS:
                    try:
                        self._connect_handler = self._loop.add_fd(self._fileno, MODE_OUT, self._connect_cb)
                    except Exception as e:
                        self._error(e)
                    return False
                elif e.args[0] == errno.ENOTCONN:
                    get_logger().error('fast open not supported on this OS')
                    self._is_enable_fast_open = False
                self._error(e)
                return False
            except Exception as e:
                self._error(e)
                return False

    if cbuffer is None:
        def _write(self):
            while self._wbuffers:
                data = self._wbuffers
                try:
                    if data._buffer_index > 0:
                        r = self._socket.send(memoryview(data._buffer)[data._buffer_index:])
                    else:
                        r = self._socket.send(data._buffer)
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
                except Exception as e:
                    self._error(e)
                    return False
            return True
    else:
        def _write(self):
            try:
                self._wbuffers.socket_send(self._fileno)
                if self._wbuffers:
                    if self._wbuffers._full and self._wbuffers._len < self._wbuffers._regain_size:
                        self._wbuffers.do_regain()
                    return False
            except Exception as e:
                self._error(e)
                if self._wbuffers._full and self._wbuffers._len < self._wbuffers._regain_size:
                    self._wbuffers.do_regain()
                return False

            if self._wbuffers._full and self._wbuffers._len < self._wbuffers._regain_size:
                self._wbuffers.do_regain()
            return True

    def write(self, data):
        if self._state != STATE_STREAMING:
            if self._state == STATE_CONNECTING:
                if data.__class__ is Buffer:
                    BaseBuffer.extend(self._wbuffers, data)
                    if data._full and data._len < data._regain_size:
                        data.do_regain()
                    if self._wbuffers._len > self._wbuffers._drain_size and not self._wbuffers._full:
                        self._wbuffers.do_drain()
                else:
                    BaseBuffer.write(self._wbuffers, data)

                if self._is_enable_fast_open and self._is_resolve and not self._connect_handler:
                    return self._connect_and_write()
                return False
            if self.ignore_write_closed_error:
                return False
            raise SocketClosed()

        if data.__class__ is Buffer:
            BaseBuffer.extend(self._wbuffers, data)
            if data._full and data._len < data._regain_size:
                data.do_regain()
        else:
            BaseBuffer.write(self._wbuffers, data)

        if not self._write_handler:
            if self._write():
                if self._has_drain_event:
                    self._loop.add_async(self.emit_drain, self)
                return True
            else:
                if self._wbuffers._len > self._wbuffers._drain_size and not self._wbuffers._full:
                    self._wbuffers.do_drain()
            try:
                self._write_handler = self._loop.add_fd(self._fileno, MODE_OUT, self._write_cb)
            except Exception as e:
                self._error(e)
            return False
        if self._wbuffers._len > self._wbuffers._drain_size and not self._wbuffers._full:
            self._wbuffers.do_drain()
        return False

    def link(self, socket):
        if self._state not in (STATE_STREAMING, STATE_CONNECTING):
            raise SocketClosed()
        if socket._state not in (STATE_STREAMING, STATE_CONNECTING):
            raise SocketClosed()

        self.ignore_write_closed_error = True
        socket.ignore_write_closed_error = True
        rbuffer, wbuffer = socket.buffer
        if self._state != STATE_STREAMING:
            if self._is_enable_fast_open and rbuffer:
                self.write(rbuffer)

            def on_connect(s):
                self._wbuffers.link(rbuffer)
                if rbuffer:
                    self.write(rbuffer)
                socket.on_data(lambda s, data: self.write(data))
            self.on_connect(on_connect)

        else:
            self._wbuffers.link(rbuffer)
            if rbuffer:
                self.write(rbuffer)
            socket.on_data(lambda s, data: self.write(data))

        if socket._state != STATE_STREAMING:
            if socket._is_enable_fast_open and self._rbuffers:
                socket.write(self._rbuffers)

            def on_pconnect(s):
                wbuffer.link(self._rbuffers)
                if self._rbuffers:
                    socket.write(self._rbuffers)
                self.on_data(lambda s, data: socket.write(data))
            socket.on_connect(on_pconnect)
        else:
            wbuffer.link(self._rbuffers)
            if self._rbuffers:
                socket.write(self._rbuffers)
            self.on_data(lambda s, data: socket.write(data))

        self.on_close(lambda s: socket.end())
        socket.on_close(lambda s: self.end())


class Server(EventEmitter):
    def __init__(self, loop=None, dns_resolver=None):
        EventEmitter.__init__(self)
        self._loop = loop or instance()
        self._dns_resolver = dns_resolver or DNSResolver.default()
        self._fileno = 0
        self._socket = None
        self._state = STATE_INITIALIZED
        self._accept_handler = False
        self._is_enable_fast_open = False
        self._is_reuseaddr = False
        self._is_enable_nodelay = False
        self._is_resolve = False

    def __del__(self):
        self.close()

    def on_listen(self, callback):
        self.on("listen", callback)

    def on_connection(self, callback):
        self.on("connection", callback)

    def on_close(self, callback):
        self.on("close", callback)

    def on_error(self, callback):
        self.on("error", callback)

    def once_listen(self, callback):
        self.on("listen", callback)

    def once_connection(self, callback):
        self.on("connection", callback)

    def once_close(self, callback):
        self.on("close", callback)

    def once_error(self, callback):
        self.on("error", callback)

    def enable_fast_open(self):
        self._is_enable_fast_open = True

    def enable_reuseaddr(self):
        self._is_reuseaddr = True

    def enable_nodelay(self):
        self._is_enable_nodelay = True

    @property
    def state(self):
        return self._state

    @property
    def socket(self):
        return self._socket

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
            if self._state == STATE_CLOSED:
                raise SocketClosed()
            return

        def do_listen(hostname, ip):
            if not ip:
                return self._loop.add_async(self._error, ResolveError('can not resolve hostname %s' % str(address)))

            try:
                addrinfo = socket.getaddrinfo(ip, address[1], 0, 0, socket.SOL_TCP)
                if not addrinfo:
                    return self._loop.add_async(self._error, AddressError('address info unknown %s' % str(address)))

                addr = addrinfo[0]
                self._socket = socket.socket(addr[0], addr[1], addr[2])
                self._fileno = self._socket.fileno()
                self._socket.setblocking(False)
                self._is_resolve = True

                if self._is_reuseaddr:
                    try:
                        self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                    except Exception as e:
                        get_logger().warning('reuseaddr error: %s', e)
                        self._is_reuseaddr = False

                if self._is_enable_fast_open:
                    try:
                        self._socket.setsockopt(socket.SOL_TCP, 23, 5)
                    except Exception as e:
                        get_logger().warning('fast open error: %s', e)
                        self._is_enable_fast_open = False

                self._socket.bind(addr[4])
                self._accept_handler = self._loop.add_fd(self._fileno, MODE_IN, self._accept_cb)
                self._socket.listen(backlog)
                self._loop.add_async(self.emit_listen, self)
            except Exception as e:
                self._loop.add_async(self._error, e)

        self._dns_resolver.resolve(address[0], do_listen)
        self._state = STATE_LISTENING

    def _accept_cb(self):
        if self._state != STATE_LISTENING:
            return

        connection, address = self._socket.accept()
        socket = Socket(loop=self._loop, socket=connection, address=address)
        if self._is_enable_nodelay:
            socket.enable_nodelay()
        self._loop.add_async(self.emit_connection, self, socket)

    def _error(self, error):
        self._loop.add_async(self.emit_error, self, error)
        self.close()
        get_logger().error("server error: %s", error)

    def close(self):
        if self._state in (STATE_INITIALIZED, STATE_LISTENING):
            if self._accept_handler:
                try:
                    self._loop.remove_fd(self._fileno, self._accept_cb)
                except Exception as e:
                    get_logger().error("server close remove_fd error: %s", e)
                self._accept_handler = False
            if self._socket is not None:
                try:
                    self._loop.clear_fd(self._fileno)
                except Exception as e:
                    get_logger().error("server close clear_fd error: %s", e)
                try:
                    self._socket.close()
                except Exception as e:
                    get_logger().error("server close socket error: %s", e)
            self._state = STATE_CLOSED

            def on_close():
                try:
                    self.emit_close(self)
                except Exception as e:
                    get_logger().exception("tcp server emit close error:%s", e)
                self.remove_all_listeners()
            self._loop.add_async(on_close)


class WarpSocket(Socket):
    def __init__(self, socket=None, loop=None, dns_resolver=None, max_buffer_size=None):
        EventEmitter.__init__(self)
        self._loop = loop or instance()
        self._socket = socket or Socket(loop=self._loop, dns_resolver=dns_resolver,
                                        max_buffer_size=max_buffer_size)
        self._max_buffer_size = max_buffer_size or self.MAX_BUFFER_SIZE
        self._rbuffers = Buffer(max_buffer_size=self._max_buffer_size)
        self._wbuffers = Buffer(max_buffer_size=self._max_buffer_size)
        self._state = self._socket.state

        self._socket.on_connect(self._do_connect)
        self._socket.on_end(self._do_end)
        self._socket.on_close(self._do_close)
        self._socket.on_error(self._do_error)
        self._socket.on_data(lambda _, data: self.read(data))
        self._has_on_drain_event = False

        rbuffers, wbuffers = self._socket.buffer
        self._rbuffers.on_drain(lambda _: rbuffers.do_drain())
        self._rbuffers.on_regain(lambda _: rbuffers.do_regain())
        wbuffers.on_drain(lambda _: self._wbuffers.do_drain())
        wbuffers.on_regain(lambda _: self._wbuffers.do_regain())

    @property
    def state(self):
        return self._socket.state

    @property
    def address(self):
        return self._socket.address

    @property
    def socket(self):
        return self._socket.socket

    @property
    def buffer(self):
        return self._rbuffers, self._wbuffers

    @property
    def is_enable_fast_open(self):
        return self._socket.is_enable_fast_open

    def enable_nodelay(self):
        self._socket.enable_nodelay()

    @property
    def is_enable_nodelay(self):
        return self._socket.is_enable_nodelay

    def on(self, event_name, callback):
        if event_name == "drain" and not self._has_on_drain_event:
            self._socket.on_drain(self._do_drain)
            self._has_on_drain_event = True
        Socket.on(self, event_name, callback)

    def once(self, event_name, callback):
        if event_name == "drain" and not self._has_on_drain_event:
            self._socket.on_drain(self._do_drain)
            self._has_on_drain_event = True
        Socket.once(self, event_name, callback)

    def off(self, event_name, callback):
        Socket.off(self, event_name, callback)
        if event_name == "drain" and not self._has_drain_event and self._has_on_drain_event:
            self._socket.off_drain(self._do_drain)
            self._has_on_drain_event = False

    def noce(self, event_name, callback):
        Socket.noce(self, event_name, callback)
        if event_name == "drain" and not self._has_drain_event and self._has_on_drain_event:
            self._socket.off_drain(self._do_drain)
            self._has_on_drain_event = False

    def remove_listener(self, event_name, callback):
        Socket.remove_listener(self, event_name, callback)
        if event_name == "drain" and not self._has_drain_event and self._has_on_drain_event:
            self._socket.off_drain(self._do_drain)
            self._has_on_drain_event = False

    def end(self):
        self._socket.end()
        self._state = self._socket.state

    def close(self):
        self._socket.close()
        self._state = self._socket.state

    def connect(self, address, timeout=5):
        self._socket.connect(address, timeout)
        self._state = self._socket.state

    def drain(self):
        self._socket.drain()

    def regain(self):
        self._socket.regain()

    def _do_connect(self, socket):
        self._state = STATE_STREAMING
        self.emit_connect(self)

    def _do_end(self, socket):
        self._state = STATE_CLOSING
        self.emit_end(self)

    def _do_close(self, socket):
        self._state = STATE_CLOSED
        try:
            self.emit_close(self)
        finally:
            self.remove_all_listeners()
            self._rbuffers.close()
            self._wbuffers.close()
            self._rbuffers = None
            self._wbuffers = None

    def _do_error(self, socket, error):
        self.emit_error(self, error)

    def _do_drain(self, socket):
        self.emit_drain(self)

    def read(self, data):
        if data.__class__ is Buffer:
            BaseBuffer.extend(self._rbuffers, data)
            if data._full and data._len < data._regain_size:
                data.do_regain()
        else:
            BaseBuffer.write(self._rbuffers, data)
        if self._rbuffers._len > self._rbuffers._drain_size and not self._rbuffers._full:
            self._rbuffers.do_drain()
        self.emit_data(self, self._rbuffers)

    def write(self, data):
        return self._socket.write(data)


class WarpServer(Server):
    def __init__(self, socket=None, loop=None, dns_resolver=None):
        EventEmitter.__init__(self)
        self._loop = loop or instance()
        self._socket = socket or Server(loop=self._loop, dns_resolver=dns_resolver)
        self._state = self._socket.state

        self._socket.on_close(self._do_close)
        self._socket.on_error(self._do_error)
        self._socket.on_listen(self._do_listen)
        self._socket.on_connection(lambda _, s: self.handshake(s))

    def enable_fast_open(self):
        self._socket.enable_fast_open()

    def enable_reuseaddr(self):
        self._socket.enable_reuseaddr()

    def enable_nodelay(self):
        self._socket.enable_nodelay()

    @property
    def state(self):
        return self._socket._state

    @property
    def socket(self):
        return self._socket.socket

    @property
    def is_enable_fast_open(self):
        return self._socket.is_enable_fast_open

    @property
    def is_reuseaddr(self):
        return self._socket.is_reuseaddr

    @property
    def is_enable_nodelay(self):
        return self._socket.is_enable_nodelay

    def listen(self, address, backlog=128):
        self._socket.listen(address, backlog)
        self._state = self._socket.state

    def close(self):
        self._socket.close()
        self._state = self._socket.state

    def _do_close(self, socket):
        self._state = STATE_CLOSED
        try:
            self.emit_close(self)
        finally:
            self.remove_all_listeners()

    def _do_error(self, socket, error):
        self.emit_error(self, error)

    def _do_listen(self, socket):
        self._state = STATE_LISTENING
        self.emit_listen(self)

    def handshake(self, socket):
        max_buffer_size = socket._max_buffer_size if hasattr(socket, "_max_buffer_size") else None
        self.emit_connection(self, WarpSocket(socket, loop=self._loop, max_buffer_size=max_buffer_size))


if is_py3:
    from .coroutines.tcp import warp_coroutine
    Socket, Server, WarpSocket, WarpServer = warp_coroutine(Socket, Server, WarpSocket, WarpServer)