# -*- coding: utf-8 -*-
# 2021/2/1
# create by: snower

import greenlet
from collections import deque
from ..errors import ChainClosed
from ..loop import current

class Chain(object):
    def __init__(self, size=1):
        self._size = size
        self._queue = deque()
        self._queue_size = 0
        self._closed = False
        self._send_waiters = deque()
        self._recv_waiters = deque()

    async def send(self, value):
        if self._closed:
            raise ChainClosed()

        if self._recv_waiters:
            current().add_async(self._recv_waiters.popleft().switch, value)
            return

        self._queue.append(value)
        self._queue_size += 1
        if self._queue_size <= self._size:
            return

        child_gr = greenlet.getcurrent()
        main = child_gr.parent
        assert main is not None, "must be running in async func"
        self._send_waiters.append(child_gr)
        main.switch()

    async def recv(self):
        if self._closed:
            raise ChainClosed()

        if self._queue_size > 0:
            if self._send_waiters:
                current().add_async(self._send_waiters.popleft().switch)
            self._queue_size -= 1
            return self._queue.popleft()

        child_gr = greenlet.getcurrent()
        main = child_gr.parent
        assert main is not None, "must be running in async func"
        self._recv_waiters.append(child_gr)
        return main.switch()

    def close(self):
        if self._closed:
            return

        self._closed = True
        while self._send_waiters:
            current().add_async(self._send_waiters.popleft().throw, ChainClosed())
        while self._recv_waiters:
            current().add_async(self._recv_waiters.popleft().throw, ChainClosed())
        self._queue_size = 0
        self._queue.clear()

    async def closeof(self):
        self.close()

    def __del__(self):
        self.close()