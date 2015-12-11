# -*- coding: utf-8 -*-

import logging
from collections import defaultdict


class EventEmitter(object):
    def __init__(self):
        self._events = defaultdict(set)
        self._events_once = defaultdict(set)

    def on(self, event_name, callback):
        self._events[event_name].add(callback)

    def once(self, event_name, callback):
        self._events_once[event_name].add(callback)

    def remove_listener(self, event_name, callback):
        try:
            self._events[event_name].remove(callback)
        except KeyError:pass
        try:
            self._events_once[event_name].remove(callback)
        except KeyError:pass

    def remove_all_listeners(self, event_name=None):
        if event_name is None:
            self._events = defaultdict(set)
            self._events_once = defaultdict(set)
        else:
            self._events[event_name] = set()
            self._events_once[event_name] = set()

    def emit(self, event_name, *args, **kwargs):
        for cb in self._events[event_name]:
            try:
                cb(*args, **kwargs)
            except Exception,e:
                logging.exception('error when calling callback:%s',e)

        callbacks = self._events_once[event_name]
        if callbacks:
            self._events_once[event_name] = set()
            while callbacks:
                cb = callbacks.pop()
                try:
                    cb(*args, **kwargs)
                except Exception,e:
                    logging.exception('error when calling callback:%s',e)
