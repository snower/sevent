# -*- coding: utf-8 -*-

version = '0.0.2'
version_info = (0,0,2)

from loop import instance, current
from event import EventEmitter
import tcp
import udp
from buffer import Buffer
from dns import DNSResolver
