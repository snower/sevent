# -*- coding: utf-8 -*-
# 2025/2/8
# create by: snower

import ssl
import time

from ..buffer import Buffer, BaseBuffer
from ..errors import SSLConnectError, SSLSocketError, SocketClosed, ConnectTimeout
from ..tcp import WarpSocket, WarpServer, STATE_INITIALIZED, STATE_CONNECTING, STATE_CLOSED


class SSLSocket(WarpSocket):
    _default_context = None

    @classmethod
    def load_default_context(cls):
        if cls._default_context is None:
            cls._default_context = ssl.create_default_context()
            cls._default_context.load_default_certs(ssl.Purpose.SERVER_AUTH)
        return cls._default_context

    def __init__(self, context=None, server_side=False, server_hostname=None, session=None, *args, **kwargs):
        WarpSocket.__init__(self, *args, **kwargs)

        self._context = context or self.load_default_context()
        self._server_side = server_side
        self._handshaked = False
        self._shutdowned = None
        self._incoming = ssl.MemoryBIO()
        self._outgoing = ssl.MemoryBIO()
        self._ssl_bio = self._context.wrap_bio(self._incoming, self._outgoing, server_side, server_hostname, session)
        self._connect_timeout = 5
        self._connect_timestamp = 0
        self._handshake_callback = None
        self._handshake_timeout_handler = None
        self._shutdown_timeout_handler = None

    @property
    def context(self):
        return self._context

    @property
    def sslobj(self):
        return self._ssl_bio

    @property
    def session(self):
        if self._ssl_bio is not None:
            return self._ssl_bio.session
        return None

    @property
    def server_side(self):
        return self._server_side

    @property
    def server_hostname(self):
        if self._ssl_bio is not None:
            return self._ssl_bio.server_hostname
        return None

    def connect(self, address, timeout=5):
        if self._state != STATE_INITIALIZED:
            if self._state == STATE_CLOSED:
                raise SocketClosed()
            return
        self._connect_timeout = timeout
        self._connect_timestamp = time.time()
        WarpSocket.connect(self, address, timeout)

    def close(self):
        if self._state == STATE_CLOSED or self._shutdowned is False:
            return
        if not self._handshaked or self._shutdowned is True:
            self._shutdowned = True
            WarpSocket.close(self)
            return
        def on_timeout_cb():
            self._shutdown_timeout_handler = None
            if self._shutdowned is True:
                return
            self._shutdowned = True
            self._error(ConnectTimeout("ssl shutdown time out %s" % str(self.address)))
        self._shutdown_timeout_handler = self._loop.add_timeout(30, on_timeout_cb)
        self._shutdowned = False
        self.do_shutdown()

    def _do_connect(self, socket):
        if self._state not in (STATE_INITIALIZED, STATE_CONNECTING):
            self._shutdowned = True
            WarpSocket.close(self)
            return
        if self._handshaked:
            WarpSocket._do_connect(self, socket)
            return
        timeout = max(1.0, self._connect_timeout - (time.time() - self._connect_timestamp))
        self.start_handshake(timeout, lambda _: self._loop.add_async(WarpSocket._do_connect, self, self._socket))

    def _do_close(self, socket):
        if self._ssl_bio is None or self._context is None:
            return
        self._incoming = None
        self._outgoing = None
        self._ssl_bio = None
        self._context = None
        self._shutdowned = True
        if self._handshake_timeout_handler:
            self._loop.cancel_timeout(self._handshake_timeout_handler)
            self._handshake_timeout_handler = None
        if self._shutdown_timeout_handler:
            self._loop.cancel_timeout(self._shutdown_timeout_handler)
            self._shutdown_timeout_handler = None
        WarpSocket._do_close(self, socket)

    def start_handshake(self, timeout, handshake_callback):
        self._handshake_callback = handshake_callback
        def on_timeout_cb():
            self._handshake_timeout_handler = None
            if self._handshaked:
                return
            self._error(ConnectTimeout("ssl handshake time out %s" % str(self.address)))
        self._handshake_timeout_handler = self._loop.add_timeout(max(1, timeout), on_timeout_cb)
        self.do_handshake()

    def read(self, data):
        if self._state == STATE_CLOSED:
            return
        if data.__class__ == Buffer:
            self._incoming.write(data.read())
        else:
            self._incoming.write(data)
        if not self._handshaked and self.do_handshake():
            return

        try:
            last_data_len = self._rbuffers._len
            while self._incoming.pending:
                try:
                    chunk = self._ssl_bio.read(self._incoming.pending)
                    if not chunk:
                        break
                    BaseBuffer.write(self._rbuffers, chunk)
                except ssl.SSLWantReadError:
                    if self._outgoing.pending:
                        self.flush()
                    break
                except ssl.SSLWantWriteError:
                    if self._outgoing.pending:
                        self.flush()
                except (ssl.SSLZeroReturnError, ssl.SSLEOFError):
                    break
            if last_data_len < self._rbuffers._len:
                if self._rbuffers._len > self._rbuffers._drain_size and not self._rbuffers._full:
                    self._rbuffers.do_drain()
                self._loop.add_async(self.emit_data, self, self._rbuffers)
        except Exception as e:
            self._shutdowned = True
            self._loop.add_async(self._error, SSLSocketError(str(e)))

    def write(self, data):
        if self._state == STATE_CLOSED:
            return False
        try:
            if not self._handshaked:
                if data.__class__ == Buffer:
                    BaseBuffer.extend(self._wbuffers, data)
                    if data._full and data._len < data._regain_size:
                        data.do_regain()
                else:
                    BaseBuffer.write(self._wbuffers, data)
                if self._wbuffers._len > self._wbuffers._drain_size and not self._wbuffers._full:
                    self._wbuffers.do_drain()
                return False

            if data.__class__ == Buffer:
                data = data.read()
            self._ssl_bio.write(data)
        except Exception as e:
            self._shutdowned = True
            self._loop.add_async(self._error, SSLSocketError(str(e)))
        if self._outgoing.pending:
            return self.flush()
        return True

    def do_handshake(self):
        while True:
            try:
                self._ssl_bio.do_handshake()
                if self._outgoing.pending:
                    self.flush()
                self._handshaked = True
                if self._wbuffers:
                    self._ssl_bio.write(self._wbuffers.read())
                    if self._outgoing.pending:
                        self.flush()
                if self._handshake_timeout_handler:
                    self._loop.cancel_timeout(self._handshake_timeout_handler)
                    self._handshake_timeout_handler = None
                handshake_callback, self._handshake_callback = self._handshake_callback, None
                if handshake_callback is not None:
                    handshake_callback(self)
                return False
            except ssl.SSLWantReadError:
                if self._outgoing.pending:
                    self.flush()
                break
            except ssl.SSLWantWriteError:
                if self._outgoing.pending:
                    self.flush()
            except Exception as e:
                self._shutdowned = True
                self._loop.add_async(self._error, SSLConnectError(self.address, e, "ssl handshake error %s %s" % (str(self.address), e)))
                break
        return True

    def do_shutdown(self):
        if self._shutdowned is True:
            return
        while True:
            try:
                if self._handshaked:
                    self._ssl_bio.unwrap()
                    if self._outgoing.pending:
                        self.flush()
                self._shutdowned = True
                WarpSocket.close(self)
                if self._shutdown_timeout_handler:
                    self._loop.cancel_timeout(self._shutdown_timeout_handler)
                    self._shutdown_timeout_handler = None
                break
            except ssl.SSLWantReadError:
                if self._outgoing.pending:
                    self.flush()
                break
            except ssl.SSLWantWriteError:
                if self._outgoing.pending:
                    self.flush()
            except Exception as e:
                self._shutdowned = True
                self._loop.add_async(self._error, SSLSocketError(str(e)))
                break

    def flush(self):
        data = self._outgoing.read()
        if not data:
            return True
        return WarpSocket.write(self, data)


class SSLServer(WarpServer):
    @classmethod
    def create_server_context(cls):
        context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        context.load_default_certs(ssl.Purpose.CLIENT_AUTH)
        return context

    def __init__(self, context, *args, **kwargs):
        WarpServer.__init__(self, *args, **kwargs)

        self._context = context

    @property
    def context(self):
        return self._context

    def handshake(self, socket):
        max_buffer_size = socket._max_buffer_size if hasattr(socket, "_max_buffer_size") else None
        socket = SSLSocket(context=self._context, server_side=True, socket=socket, loop=self._loop, max_buffer_size=max_buffer_size)
        socket.start_handshake(30, lambda _: self._loop.add_async(self.emit_connection, self, socket))