# -*- coding: utf-8 -*-
# 2025/2/8
# create by: snower

import ssl
import time

from ..buffer import Buffer, BaseBuffer
from ..errors import SSLConnectError, SSLSocketError, SocketClosed, ConnectTimeout
from ..tcp import WarpSocket, WarpServer, STATE_INITIALIZED, STATE_CLOSED


class SSLSocket(WarpSocket):
    _default_context = None

    @classmethod
    def load_default_context(cls):
        if cls._default_context is None:
            cls._default_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            cls._default_context.load_default_certs()
        return cls._default_context

    def __init__(self, context=None, server_hostname=None, session=None, *args, **kwargs):
        WarpSocket.__init__(self, *args, **kwargs)

        self._context = context or self.load_default_context()
        self._handshaked = False
        self._shutdowned = None
        self._incoming = ssl.MemoryBIO()
        self._outgoing = ssl.MemoryBIO()
        self._ssl_bio = self._context.wrap_bio(self._incoming, self._outgoing, False, server_hostname, session)
        self._connect_timeout = 5
        self._connect_timestamp = 0
        self._handshake_timeout_handler = None
        self._shutdown_timeout_handler = None

    def connect(self, address, timeout=5):
        if self._state != STATE_INITIALIZED:
            if self._state == STATE_CLOSED:
                raise SocketClosed()
            return
        self._connect_timeout = timeout
        self._connect_timestamp = time.time()
        WarpSocket.connect(self, address, timeout)

    def close(self):
        if self._state == STATE_CLOSED or self._shutdowned is not None:
            return
        def on_timeout_cb():
            self._shutdown_timeout_handler = None
            if self._shutdowned is True:
                return
            self._error(ConnectTimeout("ssl shutdown time out %s" % str(self.address)))
        self._shutdown_timeout_handler = self._loop.add_timeout(30, on_timeout_cb)
        self._shutdowned = False
        self.do_shutdown()

    def _do_connect(self, socket):
        if self._handshaked:
            WarpSocket._do_connect(self, socket)
            return

        def on_timeout_cb():
            self._handshake_timeout_handler = None
            if self._handshaked:
                return
            self._error(ConnectTimeout("ssl handshake time out %s" % str(self.address)))
        timeout = max(1.0, self._connect_timeout - (time.time() - self._connect_timestamp))
        self._handshake_timeout_handler = self._loop.add_timeout(timeout, on_timeout_cb)
        self.do_handshake()

    def _do_close(self, socket):
        self._incoming = None
        self._outgoing = None
        self._ssl_bio = None
        self._context = None
        WarpSocket._do_close(self, socket)

    def read(self, data):
        if data.__class__ == Buffer:
            self._incoming.write(data.read())
        else:
            self._incoming.write(data)
        if not self._handshaked and self.do_handshake():
            return

        try:
            last_data_len = self._rbuffers._len
            while True:
                try:
                    chunk = self._ssl_bio.read(8192)
                    if not chunk:
                        break
                    BaseBuffer.write(self._rbuffers, chunk)
                except ssl.SSLWantReadError:
                    self.flush()
                    break
                except ssl.SSLWantWriteError:
                    self.flush()
            if last_data_len < self._rbuffers._len:
                if self._rbuffers._len > self._rbuffers._drain_size and not self._rbuffers._full:
                    self._rbuffers.do_drain()
                self._loop.add_async(self.emit_data, self, self._rbuffers)
        except Exception as e:
            self._loop.add_async(self._error, SSLSocketError(str(e)))

    def write(self, data):
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
            self.flush()
        except Exception as e:
            self._loop.add_async(self._error, SSLSocketError(str(e)))

    def do_handshake(self):
        while True:
            try:
                self._ssl_bio.do_handshake()
                self._handshaked = True
                if self._wbuffers:
                    self._ssl_bio.write(self._wbuffers.read())
                    self.flush()
                if self._handshake_timeout_handler:
                    self._loop.cancel_timeout(self._handshake_timeout_handler)
                    self._handshake_timeout_handler = None
                WarpSocket._do_connect(self, self._socket)
                return False
            except ssl.SSLWantReadError:
                self.flush()
                break
            except ssl.SSLWantWriteError:
                self.flush()
            except Exception as e:
                self._loop.add_async(self._error, SSLConnectError(self.address, str(e)))
                break
        return True

    def do_shutdown(self):
        while True:
            try:
                self._ssl_bio.unwrap()
                self._shutdowned = True
                WarpSocket.close(self)
                if self._shutdown_timeout_handler:
                    self._loop.cancel_timeout(self._shutdown_timeout_handler)
                    self._shutdown_timeout_handler = None
                break
            except ssl.SSLWantReadError:
                self.flush()
                break
            except ssl.SSLWantWriteError:
                self.flush()
            except Exception as e:
                self._loop.add_async(self._error, SSLSocketError(str(e)))
                break

    def flush(self):
        data = self._outgoing.read()
        if not data:
            return
        WarpSocket.write(self, data)


class SSLServer(WarpServer):
    def __init__(self, context, *args, **kwargs):
        WarpSocket.__init__(self, *args, **kwargs)

        self._context = context