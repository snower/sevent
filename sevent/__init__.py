# -*- coding: utf-8 -*-

version = '0.4.7'
version_info = (0, 4, 7)

from .utils import is_py3, set_logger
from .loop import instance, current
from .event import EventEmitter
from . import tcp
from . import udp
from .buffer import Buffer
from .dns import DNSResolver
from . import errors

if is_py3:
    from .coroutines.future import Future
    from .coroutines.chain import Chain
    from . import loop


    def run(callback, *args, **kwargs):
        return loop.instance().run(callback, *args, **kwargs)


    def go(callback, *args, **kwargs):
        if not loop._mul_ioloop:
            return loop._ioloop.go(callback, *args, **kwargs)
        return loop._thread_local._sevent_ioloop.go(callback, *args, **kwargs)


    def sleep(seconds):
        if not loop._mul_ioloop:
            return loop._ioloop.sleep(seconds)
        return loop._thread_local._sevent_ioloop.sleep(seconds)
