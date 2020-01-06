# -*- coding: utf-8 -*-

version = '0.1.0'
version_info = (0,1,0)

from .loop import instance, current
from .event import EventEmitter
from . import tcp
from . import udp
from .buffer import Buffer
from .dns import DNSResolver
