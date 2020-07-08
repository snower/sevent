# -*- coding: utf-8 -*-
# 2020/5/8
# create by: snower

import greenlet
from ..errors import SocketClosed
from ..event import null_emit_callback

STATE_INITIALIZED = 0x01
STATE_CONNECTING = 0x02
STATE_STREAMING = 0x04
STATE_LISTENING = 0x08
STATE_CLOSING = 0x10
STATE_CLOSED = 0x20


def warp_coroutine(BaseSocket, BaseServer):
    class Socket(BaseSocket):
        async def connectof(self, address, timeout=5):
            assert self.emit_connect == null_emit_callback, "already connecting"

            if self._state != STATE_INITIALIZED:
                if self._state == STATE_CLOSED:
                    raise SocketClosed()
                return

            child_gr = greenlet.getcurrent()
            main = child_gr.parent
            assert main is not None, "must be running in async func"

            emit_connect = self.emit_connect
            emit_close = self.emit_close
            emit_error = self.emit_error

            def on_connect(socket):
                if self.emit_connect != on_connect:
                    return

                self.emit_connect = emit_connect
                self.emit_close = emit_close
                self.emit_error = emit_error
                return child_gr.switch()

            def on_close(socket):
                if self.emit_close != on_close:
                    return

                self.emit_connect = emit_connect
                self.emit_close = emit_close
                self.emit_error = emit_error
                return child_gr.throw(SocketClosed())

            def on_error(socker, e):
                if self.emit_error != on_error:
                    return

                self.emit_connect = emit_connect
                self.emit_close = emit_close
                self.emit_error = emit_error
                return child_gr.throw(e)

            setattr(self, "emit_connect", on_connect)
            setattr(self, "emit_close", on_close)
            setattr(self, "emit_error", on_error)
            self.connect(address, timeout)
            return main.switch()

        async def send(self, data):
            assert self.emit_drain == null_emit_callback, "already sending"

            if self._state == STATE_CLOSED:
                raise SocketClosed()

            if self.write(data):
                return

            child_gr = greenlet.getcurrent()
            main = child_gr.parent
            assert main is not None, "must be running in async func"

            emit_drain = self.emit_drain
            emit_close = self.emit_close
            emit_error = self.emit_error

            def on_drain(socket):
                if self.emit_drain != on_drain:
                    return

                self.emit_drain = emit_drain
                self.emit_close = emit_close
                self.emit_error = emit_error
                self._has_drain_event = False
                return child_gr.switch()

            def on_close(socket):
                if self.emit_close != on_close:
                    return

                self.emit_drain = emit_drain
                self.emit_close = emit_close
                self.emit_error = emit_error
                self._has_drain_event = False
                return child_gr.throw(SocketClosed())

            def on_error(socker, e):
                if self.emit_error != on_error:
                    return

                self.emit_drain = emit_drain
                self.emit_close = emit_close
                self.emit_error = emit_error
                self._has_drain_event = False
                return child_gr.throw(e)

            setattr(self, "emit_drain", on_drain)
            setattr(self, "emit_close", on_close)
            setattr(self, "emit_error", on_error)
            self._has_drain_event = True
            return main.switch()

        async def recv(self, size=0):
            assert self.emit_data == null_emit_callback, "already recving"

            if self._state == STATE_CLOSED:
                raise SocketClosed()

            if self._rbuffers and len(self._rbuffers) >= size:
                return self._rbuffers

            child_gr = greenlet.getcurrent()
            main = child_gr.parent
            assert main is not None, "must be running in async func"

            emit_data = self.emit_data
            emit_close = self.emit_close
            emit_error = self.emit_error

            def on_data(socket, buffer):
                if len(buffer) < size:
                    return

                if self.emit_data != on_data:
                    return

                self.emit_data = emit_data
                self.emit_close = emit_close
                self.emit_error = emit_error
                return child_gr.switch(buffer)

            def on_close(socket):
                if self.emit_close != on_close:
                    return

                self.emit_data = emit_data
                self.emit_close = emit_close
                self.emit_error = emit_error
                return child_gr.throw(SocketClosed())

            def on_error(socker, e):
                if self.emit_error != on_error:
                    return

                self.emit_data = emit_data
                self.emit_close = emit_close
                self.emit_error = emit_error
                return child_gr.throw(e)

            setattr(self, "emit_data", on_data)
            setattr(self, "emit_close", on_close)
            setattr(self, "emit_error", on_error)
            return main.switch()

        async def closeof(self):
            if self._state == STATE_CLOSED:
                return

            child_gr = greenlet.getcurrent()
            main = child_gr.parent
            assert main is not None, "must be running in async func"

            self.on_close(lambda socket: child_gr.switch())
            self.end()
            return main.switch()

        async def linkof(self, socket):
            if self._state == STATE_CLOSED and socket._state == STATE_CLOSED:
                return

            child_gr = greenlet.getcurrent()
            main = child_gr.parent
            assert main is not None, "must be running in async func"

            def do_closed(s):
                if self._state == STATE_CLOSED and socket._state == STATE_CLOSED:
                    child_gr.switch()

            BaseSocket.link(self, socket)
            self.on_close(do_closed)
            socket.on_close(do_closed)
            return main.switch()

    class Server(BaseServer):
        async def accept(self):
            assert self.emit_connection == null_emit_callback, "already accepting"

            if self._state == STATE_CLOSED:
                raise SocketClosed()

            child_gr = greenlet.getcurrent()
            main = child_gr.parent
            assert main is not None, "must be running in async func"

            emit_connection = self.emit_connection
            emit_close = self.emit_close
            emit_error = self.emit_error

            def on_connection(server, connection):
                if self.emit_connection != on_connection:
                    return

                self.emit_connection = emit_connection
                self.emit_close = emit_close
                self.emit_error = emit_error
                return child_gr.switch(connection)

            def on_close(socket):
                if self.emit_close != on_close:
                    return

                self.emit_connection = emit_connection
                self.emit_close = emit_close
                self.emit_error = emit_error
                return child_gr.throw(SocketClosed())

            def on_error(socker, e):
                if self.emit_error != on_error:
                    return

                self.emit_connection = emit_connection
                self.emit_close = emit_close
                self.emit_error = emit_error
                return child_gr.throw(e)

            setattr(self, "emit_connection", on_connection)
            setattr(self, "emit_close", on_close)
            setattr(self, "emit_error", on_error)
            return main.switch()

        async def closeof(self):
            if self._state == STATE_CLOSED:
                return

            child_gr = greenlet.getcurrent()
            main = child_gr.parent
            assert main is not None, "must be running in async func"

            self.on_close(lambda server: child_gr.switch())
            self.close()
            return main.switch()

    return Socket, Server
