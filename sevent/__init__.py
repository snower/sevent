# -*- coding: utf-8 -*-

version = '0.1.9'
version_info = (0, 1, 9)

import sys

from .loop import instance, current
from .event import EventEmitter
from . import tcp
from . import udp
from .buffer import Buffer
from .dns import DNSResolver
from . import errors

if sys.version_info[0] >= 3:
    from .coroutines import run, Future
