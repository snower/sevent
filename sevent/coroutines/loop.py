# -*- coding: utf-8 -*-
# 2020/5/8
# create by: snower

import types
import greenlet
from ..utils import get_logger


def warp_coroutine(BaseIOLoop):
    class IOLoop(BaseIOLoop):
        def call_async(self, callback, *args, **kwargs):
            if isinstance(callback, types.CoroutineType):
                def run_coroutine_fuc(*args, **kwargs):
                    def run_coroutine():
                        try:
                            callback.send(None)
                        except StopIteration:
                            return
                        except Exception as e:
                            get_logger().exception("loop callback error:%s", e)

                    child_gr = greenlet.greenlet(run_coroutine)
                    return child_gr.switch()
                return self._handlers.append((run_coroutine_fuc, args, kwargs))

            if callback.__code__.co_flags & 0x80 == 0:
                return self._handlers.append((callback, args, kwargs))

            def run_async_fuc(*args, **kwargs):
                def run_async():
                    try:
                        callback(*args, **kwargs).send(None)
                    except StopIteration:
                        return
                    except Exception as e:
                        get_logger().exception("loop callback error:%s", e)
                child_gr = greenlet.greenlet(run_async)
                return child_gr.switch()
            return self._handlers.append((run_async_fuc, args, kwargs))

        go = call_async

        async def sleep(self, seconds):
            child_gr = greenlet.getcurrent()
            main = child_gr.parent
            assert main is not None, "must be running in async func"
            self.add_timeout(seconds, child_gr.switch)
            return main.switch()

        def run(self, callback, *args, **kwargs):
            if isinstance(callback, types.CoroutineType):
                async def do_coroutine_run():
                    try:
                        await callback
                    finally:
                        self.stop()
                self.call_async(do_coroutine_run)
                return self.start()

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
