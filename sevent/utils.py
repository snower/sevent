# -*- coding: utf-8 -*-
# 18/5/25
# create by: snower

import sys
import logging

if sys.version_info[0] >= 3:
    is_py3 = True
    unicode_type = str
    byte_type = bytes

    def is_int(v):
        return v.__class__ is int

    iter_range = range
else:
    is_py3 = False
    unicode_type = unicode
    byte_type = str


    def is_int(v):
        return v.__class__ is int or v.__class__ is long

    iter_range = xrange


def ensure_bytes(s):
    if isinstance(s, unicode_type):
        return s.encode("utf-8")
    return s


def ensure_unicode(s):
    if isinstance(s, byte_type):
        return s.decode("utf-8")
    return s

_logger = logging

def set_logger(logger):
    global _logger
    _logger = logger

def get_logger():
    return _logger