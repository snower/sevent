# -*- coding: utf-8 -*-
# 2022/1/21
# create by: snower

from .utils import is_py3, get_logger
from .event import EventEmitter, null_emit_callback
from .loop import instance
from .buffer import Buffer, BaseBuffer, RECV_BUFFER_SIZE
from .errors import SocketClosed

STATE_INITIALIZED = 0x01
STATE_CONNECTING = 0x02
STATE_STREAMING = 0x04
STATE_LISTENING = 0x08
STATE_CLOSING = 0x10
STATE_CLOSED = 0x20

class PipeSocket(EventEmitter):
    MAX_BUFFER_SIZE = None
    RECV_BUFFER_SIZE = RECV_BUFFER_SIZE

    @classmethod
    def config(cls, max_buffer_size=None, recv_buffer_size=RECV_BUFFER_SIZE, **kwargs):
        cls.MAX_BUFFER_SIZE = max_buffer_size
        cls.RECV_BUFFER_SIZE = recv_buffer_size

    def __init__(self, loop=None, socket=None, address=None, max_buffer_size=None):
        EventEmitter.__init__(self)
        self._loop = loop or instance()
        self._socket = socket
        self._address = address
        self._max_buffer_size = max_buffer_size or self.MAX_BUFFER_SIZE
        self._rbuffers = Buffer(max_buffer_size=self._max_buffer_size)
        self._wbuffers = Buffer(max_buffer_size=self._max_buffer_size)
        self._state = STATE_STREAMING if socket else STATE_INITIALIZED
        self._has_drain_event = False
        self._reading = True
        self.ignore_write_closed_error = False

    @property
    def address(self):
        return self._address

    @property
    def socket(self):
        return self

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
        pass

    @property
    def is_enable_fast_open(self):
        return False

    def enable_nodelay(self):
        pass

    @property
    def is_enable_nodelay(self):
        return True

    def end(self):
        if self._state not in (STATE_INITIALIZED, STATE_CONNECTING, STATE_STREAMING):
            return

        if self._state in (STATE_INITIALIZED, STATE_CONNECTING):
            self._loop.add_async(self.close)
        else:
            if self._wbuffers:
                self._state = STATE_CLOSING
            else:
                self._loop.add_async(self.close)

    def close(self):
        if self._state == STATE_CLOSED:
            return

        self._state = STATE_CLOSED
        def on_close():
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
        if self._socket:
            self._loop.add_async(self._socket.end)

    def _error(self, error):
        self._loop.add_async(self.emit_error, self, error)
        self._loop.add_async(self.close)
        if self.emit_error == null_emit_callback:
            get_logger().error("socket error:%s", error)

    def connect(self, address, timeout=5):
        if self._state != STATE_INITIALIZED:
            if self._state == STATE_CLOSED:
                raise SocketClosed()
            return
        if isinstance(address, (tuple, list)):
            address = "pipe#%s" % (address[1] if len(address) >= 2 else address[-1])
        elif not isinstance(address, str):
            address = "pipe#%s" % address
        self._socket = PipeServer._bind_servers[address]._accept_cb(self, address)
        self._address = (address, id(self))
        self._state = STATE_STREAMING
        self._rbuffers.on("drain", lambda _: self.drain())
        self._rbuffers.on("regain", lambda _: self.regain())
        self._loop.add_async(self.emit_connect, self)

    def drain(self):
        if self._state in (STATE_STREAMING, STATE_CLOSING):
            self._reading = False

    def regain(self):
        if self._state in (STATE_STREAMING, STATE_CLOSING):
            self._reading = True

            BaseBuffer.extend(self._rbuffers, self._socket._wbuffers)
            if self._rbuffers._len > self._rbuffers._drain_size and not self._rbuffers._full:
                self._rbuffers.do_drain()
            self._loop.add_async(self.emit_data, self, self._rbuffers)
            if self._socket._wbuffers._full and self._socket._wbuffers._len < self._socket._wbuffers._regain_size:
                self._socket._wbuffers.do_regain()
            if self._socket._has_drain_event:
                self._loop.add_async(self._socket.emit_drain, self._socket)
            if self._socket._state == STATE_CLOSING:
                self._socket.close()

    def write(self, data):
        if self._state != STATE_STREAMING:
            if self.ignore_write_closed_error:
                return False
            raise SocketClosed()

        if self._socket._reading:
            if data.__class__ == Buffer:
                BaseBuffer.extend(self._socket._rbuffers, data)
            else:
                BaseBuffer.write(self._socket._rbuffers, data)
            if self._socket._rbuffers._len > self._socket._rbuffers._drain_size and not self._socket._rbuffers._full:
                self._socket._rbuffers.do_drain()
            self._loop.add_async(self._socket.emit_data, self._socket, self._socket._rbuffers)
            if self._has_drain_event:
                self._loop.add_async(self.emit_drain, self)
            return True

        if data.__class__ == Buffer:
            BaseBuffer.extend(self._wbuffers, data)
        else:
            BaseBuffer.write(self._wbuffers, data)
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
            if self.is_enable_fast_open and rbuffer:
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
            if socket.is_enable_fast_open and self._rbuffers:
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


class PipeServer(EventEmitter):
    _bind_servers = {}

    def __init__(self, loop=None):
        EventEmitter.__init__(self)
        self._loop = loop or instance()
        self._state = STATE_INITIALIZED
        self._address = None

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
        pass

    def enable_reuseaddr(self):
        pass

    def enable_nodelay(self):
        pass

    @property
    def is_enable_fast_open(self):
        return False

    @property
    def is_reuseaddr(self):
        return False

    @property
    def is_enable_nodelay(self):
        return True

    def listen(self, address, backlog=128):
        if self._state != STATE_INITIALIZED:
            if self._state == STATE_CLOSED:
                raise SocketClosed()
            return
        if isinstance(address, (tuple, list)):
            address = "pipe#%s" % (address[1] if len(address) >= 2 else address[-1])
        elif not isinstance(address, str):
            address = "pipe#%s" % address
        PipeServer._bind_servers[address] = self
        self._address = address
        self._state = STATE_LISTENING

    def _accept_cb(self, socket, address):
        if self._state != STATE_LISTENING:
            return
        socket = PipeSocket(socket=socket, address=(address, id(socket)))
        self._loop.add_async(self.emit_connection, self, socket)
        return socket

    def _error(self, error):
        self._loop.add_async(self.emit_error, self, error)
        self.close()
        get_logger().error("server error: %s", error)

    def close(self):
        if self._state in (STATE_INITIALIZED, STATE_LISTENING):
            if self._address and self._address in PipeServer._bind_servers:
                PipeServer._bind_servers.pop(self._address)
            self._state = STATE_CLOSED

            def on_close():
                try:
                    self.emit_close(self)
                except Exception as e:
                    get_logger().exception("tcp server emit close error:%s", e)
                self.remove_all_listeners()
            self._loop.add_async(on_close)


if is_py3:
    from .coroutines.pipe import warp_coroutine
    PipeSocket, PipeServer = warp_coroutine(PipeSocket, PipeServer)