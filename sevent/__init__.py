# -*- coding: utf-8 -*-

version = '0.0.5'
version_info = (0,0,5)

from .loop import instance, current
from .event import EventEmitter
from . import tcp
from . import udp
from .buffer import Buffer
from .dns import DNSResolver
