# -*- coding: utf-8 -*-
# 2020/5/8
# create by: snower

import logging
import greenlet


def warp_coroutine(BaseEventEmitter):
    class EventEmitter(BaseEventEmitter):
        def on(self, event_name, callback):
            if callback.__code__.co_flags & 0x80 == 0:
                return BaseEventEmitter.on(self, event_name, callback)

            def run_async_fuc(*args, **kwargs):
                def run():
                    try:
                        callback(*args, **kwargs).send(None)
                    except StopIteration:
                        pass
                    except Exception as e:
                        logging.exception("error when calling callback:%s", e)
                child_gr = greenlet.greenlet(run)
                return child_gr.switch()
            BaseEventEmitter.on(self, event_name, run_async_fuc)

        def once(self, event_name, callback):
            if callback.__code__.co_flags & 0x80 == 0:
                return BaseEventEmitter.once(self, event_name, callback)

            def run_async_fuc(*args, **kwargs):
                def run():
                    try:
                        callback(*args, **kwargs).send(None)
                    except StopIteration:
                        pass
                    except Exception as e:
                        logging.exception("error when calling callback:%s", e)
                child_gr = greenlet.greenlet(run)
                return child_gr.switch()
            BaseEventEmitter.once(self, event_name, run_async_fuc)

    return EventEmitter
