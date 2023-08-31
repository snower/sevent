# -*- coding: utf-8 -*-

from collections import defaultdict
from .utils import is_py3, get_logger


def null_emit_callback(*args, **kwargs):
    return None


class EventEmitter(object):
    def __init__(self):
        self._events = defaultdict(set)
        self._events_once = defaultdict(set)

    def on(self, event_name, callback):
        event_callbacks = self._events[event_name]
        event_callbacks.add(callback)

        if not self._events_once[event_name] and len(event_callbacks) == 1:
            setattr(self, "emit_" + event_name, callback)
        else:
            setattr(self, "emit_" + event_name, self.emit_callback(event_name))

    def off(self, event_name, callback):
        event_callbacks = self._events[event_name]
        try:
            event_callbacks.remove(callback)
        except KeyError:
            pass

        if not event_callbacks:
            once_event_callbacks = self._events_once[event_name]
            if not once_event_callbacks:
                setattr(self, "emit_" + event_name, null_emit_callback)
            elif len(once_event_callbacks) == 1:
                callback = list(once_event_callbacks)[0]

                def emit_callback(*args, **kwargs):
                    setattr(self, "emit_" + event_name, null_emit_callback)
                    once_event_callbacks.clear()
                    return callback(*args, **kwargs)

                setattr(self, "emit_" + event_name, emit_callback)
            else:
                setattr(self, "emit_" + event_name, self.emit_callback(event_name))
        elif len(event_callbacks) == 1:
            once_event_callbacks = self._events_once[event_name]
            if not once_event_callbacks:
                setattr(self, "emit_" + event_name, list(event_callbacks)[0])
            else:
                setattr(self, "emit_" + event_name, self.emit_callback(event_name))
        else:
            setattr(self, "emit_" + event_name, self.emit_callback(event_name))

    def once(self, event_name, callback):
        once_event_callbacks = self._events_once[event_name]
        once_event_callbacks.add(callback)

        if not self._events[event_name] and len(once_event_callbacks) == 1:
            def emit_callback(*args, **kwargs):
                setattr(self, "emit_" + event_name, null_emit_callback)
                once_event_callbacks.clear()
                return callback(*args, **kwargs)

            setattr(self, "emit_" + event_name, emit_callback)
        else:
            setattr(self, "emit_" + event_name, self.emit_callback(event_name))

    def noce(self, event_name, callback):
        once_event_callbacks = self._events_once[event_name]
        try:
            once_event_callbacks.remove(callback)
        except KeyError:
            pass

        event_callbacks = self._events[event_name]
        if not event_callbacks:
            if not once_event_callbacks:
                setattr(self, "emit_" + event_name, null_emit_callback)
            elif len(once_event_callbacks) == 1:
                callback = list(once_event_callbacks)[0]

                def emit_callback(*args, **kwargs):
                    setattr(self, "emit_" + event_name, null_emit_callback)
                    once_event_callbacks.clear()
                    return callback(*args, **kwargs)

                setattr(self, "emit_" + event_name, emit_callback)
            else:
                setattr(self, "emit_" + event_name, self.emit_callback(event_name))
        elif len(event_callbacks) == 1:
            if not once_event_callbacks:
                setattr(self, "emit_" + event_name, list(event_callbacks)[0])
            else:
                setattr(self, "emit_" + event_name, self.emit_callback(event_name))
        else:
            setattr(self, "emit_" + event_name, self.emit_callback(event_name))

    def remove_listener(self, event_name, callback):
        event_callbacks = self._events[event_name]
        try:
            event_callbacks.remove(callback)
        except KeyError:
            pass

        once_event_callbacks = self._events_once[event_name]
        try:
            once_event_callbacks.remove(callback)
        except KeyError:
            pass

        if not event_callbacks:
            if not once_event_callbacks:
                setattr(self, "emit_" + event_name, null_emit_callback)
            elif len(once_event_callbacks) == 1:
                callback = list(once_event_callbacks)[0]

                def emit_callback(*args, **kwargs):
                    setattr(self, "emit_" + event_name, null_emit_callback)
                    once_event_callbacks.clear()
                    return callback(*args, **kwargs)

                setattr(self, "emit_" + event_name, emit_callback)
            else:
                setattr(self, "emit_" + event_name, self.emit_callback(event_name))
        elif len(event_callbacks) == 1:
            if not once_event_callbacks:
                setattr(self, "emit_" + event_name, list(event_callbacks)[0])
            else:
                setattr(self, "emit_" + event_name, self.emit_callback(event_name))
        else:
            setattr(self, "emit_" + event_name, self.emit_callback(event_name))

    def remove_all_listeners(self, event_name=None):
        if event_name is None:
            for event_name in set(list(self._events.keys()) + list(self._events_once.keys())):
                setattr(self, "emit_" + event_name, null_emit_callback)
            self._events = defaultdict(set)
            self._events_once = defaultdict(set)
        else:
            setattr(self, "emit_" + event_name, null_emit_callback)
            self._events[event_name] = set()
            self._events_once[event_name] = set()

    def emit_callback(self, event_name):
        def _(*args, **kwargs):
            for cb in self._events[event_name]:
                try:
                    cb(*args, **kwargs)
                except Exception as e:
                    if isinstance(e, (KeyboardInterrupt, SystemError)):
                        raise e
                    get_logger().exception('error when calling callback:%s',e)

            callbacks = self._events_once[event_name]
            if callbacks:
                self._events_once[event_name] = set()
                while callbacks:
                    cb = callbacks.pop()
                    try:
                        cb(*args, **kwargs)
                    except Exception as e:
                        if isinstance(e, (KeyboardInterrupt, SystemError)):
                            raise e
                        get_logger().exception('error when calling callback:%s',e)
        return _

    def emit(self, event_name, *args, **kwargs):
        for cb in self._events[event_name]:
            try:
                cb(*args, **kwargs)
            except Exception as e:
                if isinstance(e, (KeyboardInterrupt, SystemError)):
                    raise e
                get_logger().exception('error when calling callback:%s', e)

        callbacks = self._events_once[event_name]
        if callbacks:
            self._events_once[event_name] = set()
            while callbacks:
                cb = callbacks.pop()
                try:
                    cb(*args, **kwargs)
                except Exception as e:
                    if isinstance(e, (KeyboardInterrupt, SystemError)):
                        raise e
                    get_logger().exception('error when calling callback:%s', e)

    def __getattr__(self, item):
        if item[:5] == "emit_":
            event_name = item[5:]
            event_callbacks = self._events[event_name]
            if not event_callbacks:
                once_event_callbacks = self._events_once[event_name]
                if not once_event_callbacks:
                    setattr(self, "emit_" + event_name, null_emit_callback)
                    return null_emit_callback
                if len(once_event_callbacks) == 1:
                    callback = list(once_event_callbacks)[0]

                    def emit_callback(*args, **kwargs):
                        setattr(self, "emit_" + event_name, null_emit_callback)
                        once_event_callbacks.clear()
                        return callback(*args, **kwargs)

                    setattr(self, "emit_" + event_name, emit_callback)
                    return emit_callback
                callback = self.emit_callback(event_name)
                setattr(self, "emit_" + event_name, callback)
                return callback

            if len(event_callbacks) == 1:
                once_event_callbacks = self._events_once[event_name]
                if not once_event_callbacks:
                    callback = list(event_callbacks)[0]
                    setattr(self, "emit_" + event_name, callback)
                    return callback
            callback = self.emit_callback(event_name)
            setattr(self, "emit_" + event_name, callback)
            return callback

        elif item[:3] == "on_":
            return lambda *args, **kwargs: self.on(item[3:], *args, **kwargs)
        return object.__getattribute__(self, item)


if is_py3:
    from .coroutines.event import warp_coroutine
    EventEmitter = warp_coroutine(EventEmitter)
