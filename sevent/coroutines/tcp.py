# -*- coding: utf-8 -*-
# 2020/5/8
# create by: snower

import greenlet
from ..errors import SocketClosed

STATE_INITIALIZED = 0x01
STATE_CONNECTING = 0x02
STATE_STREAMING = 0x04
STATE_LISTENING = 0x08
STATE_CLOSING = 0x10
STATE_CLOSED = 0x20


def warp_coroutine(BaseSocket, BaseServer):
    class Socket(BaseSocket):
        def connect(self, address, timeout=5):
            child_gr = greenlet.getcurrent()
            main = child_gr.parent
            if main is None:
                return BaseSocket.connect(self, address, timeout)

            async def do_connect():
                def on_connect(socket):
                    self.remove_listener("connect", on_connect)
                    self.remove_listener("close", on_close)
                    return child_gr.switch()

                def on_close(socket):
                    return child_gr.throw(SocketClosed())

                self.on_connect(on_connect)
                self.on_close(on_close)
                BaseSocket.connect(self, address, timeout)
                return main.switch()
            return do_connect()

        async def send(self, data):
            if self.write(data):
                return

            child_gr = greenlet.getcurrent()
            main = child_gr.parent
            assert main is not None, "must be running in async func"

            def on_drain(socket):
                self.remove_listener("close", on_close)
                self.remove_listener("drain", on_drain)
                return child_gr.switch()

            def on_close(socket):
                return child_gr.throw(SocketClosed())

            self.on_drain(on_drain)
            self.on_close(on_close)
            return main.switch()

        async def recv(self, size=0):
            if size <= 0 and self._rbuffers:
                return self._rbuffers

            child_gr = greenlet.getcurrent()
            main = child_gr.parent
            assert main is not None, "must be running in async func"

            def on_data(socket, buffer):
                if len(buffer) < size:
                    return
                self.remove_listener("data", on_data)
                self.remove_listener("close", on_close)
                return child_gr.switch(buffer)

            def on_close(socket):
                return child_gr.throw(SocketClosed())

            self.on_data(on_data)
            self.on_close(on_close)
            return main.switch()

        def close(self):
            if self._state == STATE_CLOSED:
                return

            child_gr = greenlet.getcurrent()
            main = child_gr.parent
            if main is None:
                return BaseSocket.close(self)

            async def do_close():
                self.on_close(lambda socket: child_gr.switch())
                return main.switch()
            return do_close()


    class Server(BaseServer):
        async def accept(self):
            child_gr = greenlet.getcurrent()
            main = child_gr.parent
            assert main is not None, "must be running in async func"

            def on_connection(server, connection):
                self.remove_listener("connection", on_connection)
                self.remove_listener("close", on_close)
                return child_gr.switch(connection)

            def on_close(socket):
                return child_gr.throw(SocketClosed())

            self.on_connection(on_connection)
            self.on_close(on_close)
            return main.switch()

        def close(self):
            if self._state == STATE_CLOSED:
                return

            child_gr = greenlet.getcurrent()
            main = child_gr.parent
            if main is None:
                return BaseServer.close(self)

            async def do_close():
                self.on_close(lambda server: child_gr.switch())
                return main.switch()
            return do_close()

    return Socket, Server