# -*- coding: utf-8 -*-

import select
from ..loop import IOLoop


class EpollLoop(IOLoop):
    def __init__(self):
        super(EpollLoop, self).__init__()
        self._epoll = select.epoll()

        self._poll = self._epoll.poll
        self._add_fd = self._epoll.register
        self._remove_fd = self._epoll.unregister
        self._modify_fd = self._epoll.modify

    def _poll(self, timeout):
        return self._epoll.poll(timeout)

    def _add_fd(self, fd, mode):
        self._epoll.register(fd, mode)

    def _remove_fd(self, fd):
        self._epoll.unregister(fd)

    def _modify_fd(self, fd, mode):
        self._epoll.modify(fd, mode)
