# -*- coding: utf-8 -*-
# 2020/5/8
# create by: snower

import greenlet


def warp_coroutine(BaseDNSResolver):
    class DNSResolver(BaseDNSResolver):
        async def gethostbyname(self, hostname, timeout=None):
            child_gr = greenlet.getcurrent()
            main = child_gr.parent
            assert main is not None, "must be running in async func"
            self.resolve(hostname, lambda hostname, ip: child_gr.switch(ip), timeout)
            return main.switch()

    return DNSResolver
