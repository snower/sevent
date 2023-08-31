# -*- coding: utf-8 -*-
# 15/8/6
# create by: snower

import sys
import os
import time
import socket
import dnslib
from collections import defaultdict
from .loop import instance
from .event import EventEmitter
from .utils import ensure_bytes, is_py3

QTYPE_ANY = 255
QTYPE_A = 1
QTYPE_AAAA = 28
QTYPE_CNAME = 5
QTYPE_NS = 2
QCLASS_IN = 1

STATUS_OPENED = 0
STATUS_CLOSED = 1


class DNSCache(object):
    def __init__(self, loop, default_ttl=60):
        self._loop = loop or instance()
        self.default_ttl = default_ttl
        self._cache = defaultdict(list)
        self._last_resolve_time = time.time()

    def append(self, hostname, rrs):
        rrcs = []
        now = time.time()
        for rrc in self._cache[hostname]:
            if rrc.ttl_expried_time <= now:
                continue
            rrcs.append(rrc)
        self._cache[hostname] = rrcs

        for rr in rrs:
            has_cache = False
            for rrc in self._cache[hostname]:
                if rr == rrc:
                    has_cache = True
                    break
            if has_cache:
                continue
            setattr(rr, "ttl_expried_time", time.time() + (rr.ttl or self.default_ttl))
            self._cache[hostname].append(rr)

        if self._last_resolve_time - now >= 120:
            self._loop.add_async(self.resolve)
            self._last_resolve_time = now

    def get(self, hostname):
        if hostname not in self._cache or not self._cache[hostname]:
            return None, hostname

        now = time.time()
        if self._last_resolve_time - now >= 120:
            self._loop.add_async(self.resolve)
            self._last_resolve_time = now

        for rrc in self._cache[hostname]:
            if rrc.ttl_expried_time <= now:
                continue
            return str(rrc.rdata), hostname
        return None, hostname

    def remove(self, hostname):
        if hostname in self._cache:
            self._cache.pop(hostname)

        now = time.time()
        if self._last_resolve_time - now >= 120:
            self._loop.add_async(self.resolve)
            self._last_resolve_time = now

    def resolve(self):
        now = time.time()
        epried_hostnames = []
        for hostname, rrs in self._cache.items():
            rrcs = []
            for rr in rrs:
                if rr.ttl_expried_time <= now:
                    continue
                rrcs.append(rr)
            if rrcs:
                self._cache[hostname] = rrcs
            else:
                epried_hostnames.append(hostname)

        for hostname in epried_hostnames:
            self.remove(hostname)

    def clear(self):
        self._cache = defaultdict(list)

    def __getitem__(self, hostname):
        return self.get(hostname)[0]

    def __delitem__(self, hostname):
        return self.remove(hostname)

    def __contains__(self, hostname):
        return bool(self.get(hostname)[0])


class DnsQueryState(object):
    def __init__(self, hostname, v4server_count, v6server_count):
        self.hostname = hostname
        self.v4bv4_count = v4server_count
        self.v6bv6_count = v6server_count
        self.v6bv4_count = v4server_count
        self.v4bv6_count = v6server_count
        self.v4bv4_loading_count = 0
        self.v6bv6_loading_count = 0
        self.v6bv4_loading_count = 0
        self.v4bv6_loading_count = 0
        self.v4bv4_done_count = 0
        self.v6bv6_done_count = 0
        self.v6bv4_done_count = 0
        self.v4bv6_done_count = 0
        self.callbacks = []

    def v4bv4_done(self):
        return self.v4bv4_done_count >= self.v4bv4_count

    def v6bv6_done(self):
        return self.v6bv6_done_count >= self.v6bv6_count

    def v6bv4_done(self):
        return self.v6bv4_done_count >= self.v6bv4_count

    def v4bv6_done(self):
        return self.v4bv6_done_count >= self.v4bv6_count

    def done(self):
        return self.v4bv4_done_count >= self.v4bv4_count and self.v6bv6_done_count >= self.v6bv6_count \
               and self.v6bv4_done_count >= self.v6bv4_count and self.v4bv6_done_count >= self.v4bv6_count

    def append(self, callback):
        self.callbacks.append(callback)


class DNSResolver(EventEmitter):
    _instance = None

    @classmethod
    def default(cls):
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def __init__(self, loop=None, servers=None, hosts=None, resolve_timeout=None, resend_timeout=0.5):
        EventEmitter.__init__(self)

        self._loop = loop or instance()
        self._servers = []
        self._server6s = []
        self._hosts = hosts or {}

        self._cache = DNSCache(self._loop)
        self._queue = {}
        self._socket = None
        self._socket6 = None
        self._status = STATUS_OPENED

        if not servers:
            servers = self.parse_resolv()
        for server in servers:
            inet_type = self.is_ip(server)
            if inet_type == socket.AF_INET:
                self._servers.append(server)
            elif inet_type == socket.AF_INET6:
                self._server6s.append(server)

        if not hosts:
            self.parse_hosts()

        self._resolve_timeout = resolve_timeout if resolve_timeout else ((len(self._servers) + len(self._server6s))
                                                                         * resend_timeout + 4)
        self._resend_timeout = resend_timeout

    def on_resolve(self, callback):
        self.on("resolve", callback)

    def off_resolve(self, callback):
        self.on("resolve", callback)

    def once_resolve(self, callback):
        self.once("resolve", callback)

    def noce_resolve(self, callback):
        self.once("resolve", callback)

    def create_socket(self):
        from .udp import Socket
        self._socket = Socket(self._loop)
        self._socket.on_data(self.on_data)
        self._socket.on_close(self.on_close)
        self._socket.on_error(lambda s, e: None)

    def create_socket6(self):
        from .udp import Socket
        self._socket6 = Socket(self._loop)
        self._socket6.on_data(self.on_data6)
        self._socket6.on_close(self.on_close)
        self._socket6.on_error(lambda s, e: None)

    def parse_resolv(self):
        try:
            servers = [server for server in str(os.environ.get("SEVENT_NAMESERVER", '')).split(",")
                       if server and self.is_ip(server)]
            if servers:
                return servers
        except:
            pass

        servers = []
        etc_path = '/etc/resolv.conf'
        if 'WINDIR' in os.environ:
            etc_path = os.environ['WINDIR'] + '/system32/drivers/etc/resolv.conf'
        try:
            with open(etc_path, 'rb') as f:
                content = f.readlines()
                for line in content:
                    line = line.strip()
                    if not line or line[:1] == b'#' or not line.startswith(b'nameserver'):
                        continue

                    if is_py3 and type(line) != str:
                        parts = line.decode("utf-8").split()
                    else:
                        parts = line.split()
                    if len(parts) < 2:
                        continue
                    server = parts[1].strip()
                    if not self.is_ip(server):
                        continue
                    servers.append(server)
        except IOError:
            pass
        if not servers:
            if sys.platform == "win32":
                try:
                    from .win32util import get_dns_info
                    servers = get_dns_info().nameservers
                    if servers:
                        return servers
                except:
                    pass
        return servers or ['8.8.4.4', '8.8.8.8', '114.114.114.114']

    def parse_hosts(self):
        etc_path = '/etc/hosts'
        if 'WINDIR' in os.environ:
            etc_path = os.environ['WINDIR'] + '/system32/drivers/etc/hosts'
        try:
            with open(etc_path, 'rb') as f:
                for line in f.readlines():
                    line = line.strip()
                    if not line or line[:1] == b'#':
                        continue

                    if is_py3 and type(line) != str:
                        parts = line.decode("utf-8").split()
                    else:
                        parts = line.split()
                    if len(parts) < 2:
                        continue
                    ip = parts[0].strip()
                    if not self.is_ip(ip):
                        continue

                    for i in range(1, len(parts)):
                        hostname = parts[i].strip()
                        if not hostname:
                            continue
                        self._hosts[ensure_bytes(hostname)] = ip
        except IOError:
            self._hosts[b'localhost'] = '127.0.0.1'

    def call_callback(self, hostname, ip):
        if hostname not in self._queue:
            return
        query_state = self._queue.pop(hostname)
        hostname = hostname.decode("utf-8") if is_py3 and type(hostname) != str else hostname
        for callback in query_state.callbacks:
            self._loop.add_async(callback, hostname, ip)
        self._loop.add_async(self.emit_resolve, self, hostname, ip)

    def on_data(self, socket, buffer):
        while buffer:
            data, address = buffer.next()
            try:
                answer = dnslib.DNSRecord.parse(data)
                hostname = b".".join(answer.q.qname.label)
                if hostname not in self._queue:
                    continue
                rrs = [rr for rr in answer.rr if rr.rtype == answer.q.qtype]
                query_state = self._queue[hostname]
                if answer.q.qtype == 28:
                    query_state.v6bv4_loading_count -= 1
                    query_state.v6bv4_done_count += 1
                else:
                    query_state.v4bv4_loading_count -= 1
                    query_state.v4bv4_done_count += 1

                if rrs:
                    self._cache.append(hostname, rrs)
                    self.call_callback(hostname, str(rrs[0].rdata))
                elif query_state.done():
                    self.call_callback(hostname, None)
                elif query_state.v4bv4_done() and query_state.v6bv4_loading_count <= 0 \
                        and query_state.v6bv4_done_count <= 0:
                    self.send_req(hostname, query_state, 0, True)
            except Exception:
                pass

    def on_data6(self, socket, buffer):
        while buffer:
            data, address = buffer.next()
            try:
                answer = dnslib.DNSRecord.parse(data)
                hostname = b".".join(answer.q.qname.label)
                if hostname not in self._queue:
                    continue
                rrs = [rr for rr in answer.rr if rr.rtype == answer.q.qtype]
                query_state = self._queue[hostname]
                if answer.q.qtype == 1:
                    query_state.v4bv6_loading_count -= 1
                    query_state.v4bv6_done_count += 1
                else:
                    query_state.v6bv6_loading_count -= 1
                    query_state.v6bv6_done_count += 1

                if rrs:
                    self._cache.append(hostname, rrs)
                    self.call_callback(hostname, str(rrs[0].rdata))
                elif query_state.done():
                    self.call_callback(hostname, None)
                elif query_state.v6bv6_done() and query_state.v4bv6_loading_count <= 0 \
                        and query_state.v4bv6_done_count <= 0:
                    self.send_req6(hostname, query_state, 0, True)
            except Exception:
                pass

    def send_req(self, hostname, query_state, server_index=0, is_query_v6=False):
        if not self._servers:
            return
        servers = self._servers

        question = dnslib.DNSRecord.question(hostname, 'AAAA' if is_query_v6 else 'A')
        if self._socket is None:
            self.create_socket()
        seq_data = bytes(question.pack())
        for _ in range(3):
            if server_index >= len(servers):
                break
            if is_query_v6:
                query_state.v6bv4_loading_count += 1
            else:
                query_state.v4bv4_loading_count += 1
            self._socket.write((seq_data, (servers[server_index], 53)))
            server_index += 1

        def on_timeout():
            if server_index >= len(servers):
                if hostname in self._queue and query_state.v6bv4_loading_count <= 0 \
                        and query_state.v6bv4_done_count <= 0:
                    self.send_req(hostname, query_state, 0, True)
                return
            if hostname in self._queue:
                self.send_req(hostname, query_state, server_index, is_query_v6)
        if server_index >= len(servers) and is_query_v6:
            return
        self._loop.add_timeout(self._resend_timeout, on_timeout)

    def send_req6(self, hostname, query_state, server_index=0, is_query_v4=False):
        if not self._server6s:
            return
        servers = self._server6s

        question = dnslib.DNSRecord.question(hostname, 'A' if is_query_v4 else 'AAAA')
        if self._socket6 is None:
            self.create_socket6()
        seq_data = bytes(question.pack())
        for _ in range(3):
            if server_index >= len(servers):
                break
            if is_query_v4:
                query_state.v4bv6_loading_count += 1
            else:
                query_state.v6bv6_loading_count += 1
            self._socket6.write((seq_data, (servers[server_index], 53)))
            server_index += 1

        def on_timeout():
            if server_index >= len(servers):
                if hostname in self._queue and query_state.v4bv6_loading_count <= 0 \
                        and query_state.v4bv6_done_count <= 0:
                    self.send_req6(hostname, query_state, 0, True)
                return
            if hostname in self._queue:
                self.send_req6(hostname, query_state, server_index, is_query_v4)
        if server_index >= len(servers) and is_query_v4:
            return
        self._loop.add_timeout(self._resend_timeout, on_timeout)

    def resolve(self, hostname, callback, timeout=None):
        if self._status == STATUS_CLOSED:
            return callback(hostname.decode("utf-8") if is_py3 and type(hostname) != str else hostname, None)

        hostname = ensure_bytes(hostname)
        if not hostname:
            return callback(hostname.decode("utf-8") if is_py3 and type(hostname) != str else hostname, None)
        elif self.is_ip(hostname):
            return callback(hostname.decode("utf-8") if is_py3 and type(hostname) != str else hostname, hostname)
        elif hostname in self._hosts:
            return callback(hostname.decode("utf-8") if is_py3 and type(hostname) != str else hostname, self._hosts[hostname])
        elif hostname in self._cache:
            return callback(hostname.decode("utf-8") if is_py3 and type(hostname) != str else hostname, self._cache[hostname])
        else:
            try:
                if hostname not in self._queue:
                    self._queue[hostname] = query_state = DnsQueryState(hostname, len(self._servers), len(self._server6s))
                    query_state.append(callback)
                    self.send_req(hostname, query_state)
                    self.send_req6(hostname, query_state)

                    if hostname in self._queue:
                        def on_timeout():
                            if hostname in self._queue:
                                self.call_callback(hostname, None)
                        self._loop.add_timeout(timeout or self._resolve_timeout, on_timeout)
                else:
                    self._queue[hostname].append(callback)
            except Exception:
                self.call_callback(hostname, None)
        return False

    def flush(self):
        self._cache.clear()

    def on_close(self, socket):
        if self._status == STATUS_CLOSED:
            return

        if self._socket == socket:
            self._socket = None
        else:
            self._socket6 = None

    def close(self):
        if self._status == STATUS_CLOSED:
            return

        self._status = STATUS_CLOSED
        if self._socket:
            self._socket.close()
        if self._socket6:
            self._socket6.close()

        for hostname, query_state in self._queue.items():
            for callback in query_state.callbacks:
                self._loop.add_async(callback, hostname, None)
        self._queue.clear()

    def is_ip(self, address):
        if is_py3 and type(address) != str:
            address = address.decode('utf8')

        try:
            socket.inet_pton(socket.AF_INET, address)
            return socket.AF_INET
        except (TypeError, ValueError, OSError, IOError):
            try:
                socket.inet_pton(socket.AF_INET6, address)
                return socket.AF_INET6
            except (TypeError, ValueError, OSError, IOError):
                return False


if is_py3:
    from .coroutines.dns import warp_coroutine
    DNSResolver = warp_coroutine(DNSResolver)