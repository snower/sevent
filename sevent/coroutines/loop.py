# -*- coding: utf-8 -*-
# 2020/5/8
# create by: snower

import logging
import greenlet


def warp_coroutine(BaseIOLoop):
    class IOLoop(BaseIOLoop):
        def call_async(self, callback, *args, **kwargs):
            if callback.__code__.co_flags & 0x80 == 0:
                return self._handlers.append((callback, args, kwargs))

            def run_async_fuc(*args, **kwargs):
                def run():
                    try:
                        callback(*args, **kwargs).send(None)
                    except StopIteration:
                        pass
                    except Exception as e:
                        logging.exception("loop callback error:%s", e)
                child_gr = greenlet.greenlet(run)
                return child_gr.switch()
            self._handlers.append((run_async_fuc, args, kwargs))

        async def sleep(self, seconds):
            child_gr = greenlet.getcurrent()
            main = child_gr.parent
            assert main is not None, "must be running in async func"
            self.add_timeout(seconds, child_gr.switch)
            return main.switch()

        def run(self, callback, *args, **kwargs):
            if callback.__code__.co_flags & 0x80 == 0:
                def do_run():
                    try:
                        callback(*args, **kwargs)
                    finally:
                        self.stop()
                self.add_async(do_run)
                return self.start()

            async def do_async_run():
                try:
                    await callback(*args, **kwargs)
                finally:
                    self.stop()
            self.call_async(do_async_run)
            return self.start()

    return IOLoop
