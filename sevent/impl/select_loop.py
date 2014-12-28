# -*- coding: utf-8 -*-
import select
from collections import defaultdict
from ..loop import SSLoop,MODE_NULL,MODE_IN,MODE_OUT,MODE_ERR


class SelectLoop(SSLoop):
    def __init__(self):
        super(SelectLoop, self).__init__()
        self._r_list = set()
        self._w_list = set()
        self._x_list = set()

    def _poll(self, timeout):
        try:
            r, w, x = select.select(self._r_list, self._w_list, self._x_list,timeout)
        except Exception,e:
            return []
        results = defaultdict(lambda: MODE_NULL)
        for p in [(r, MODE_IN), (w, MODE_OUT), (x, MODE_ERR)]:
            for fd in p[0]:
                results[fd] |= p[1]
        return results.items()

    def _add_fd(self, fd, mode):
        if mode & MODE_IN:
            self._r_list.add(fd)
        if mode & MODE_OUT:
            self._w_list.add(fd)
        if mode & MODE_ERR:
            self._x_list.add(fd)

    def _remove_fd(self, fd):
        if fd in self._r_list:
            self._r_list.remove(fd)
        if fd in self._w_list:
            self._w_list.remove(fd)
        if fd in self._x_list:
            self._x_list.remove(fd)

    def _modify_fd(self, fd, mode):
        self._remove_fd(fd)
        self._add_fd(fd, mode)
