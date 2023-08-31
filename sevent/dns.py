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
        if not self._cache[hostname]:
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
        self._queue = defaultdict(list)
        self._loading = defaultdict(int)
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
        self._socket6.on_data(self.on_data)
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
        if hostname not in self._queue or not self._queue[hostname]:
            return
        callbacks = self._queue.pop(hostname)
        self._loading.pop(hostname, 0)
        hostname = hostname.decode("utf-8") if is_py3 and type(hostname) != str else hostname
        for callback in callbacks:
            self._loop.add_async(callback, hostname, ip)
        self._loop.add_async(self.emit_resolve, self, hostname, ip)

    def on_data(self, socket, buffer):
        while buffer:
            data, address = buffer.next()
            try:
                answer = dnslib.DNSRecord.parse(data)
                hostname = b".".join(answer.q.qname.label)
                if hostname not in self._loading:
                    continue
                if self._loading[hostname] <= 0:
                    self._loading.pop(hostname, 0)
                    continue
                rrs = [rr for rr in answer.rr if rr.rtype == answer.q.qtype]
                self._loading[hostname] -= 1

                if rrs:
                    self._cache.append(hostname, rrs)
                    self.call_callback(hostname, str(rrs[0].rdata))
                elif self._loading[hostname] <= 0:
                    self.call_callback(hostname, None)
            except Exception:
                pass

    def send_req(self, hostname, server_index=0):
        if not self._servers:
            return
        servers = self._servers

        question = dnslib.DNSRecord.question(hostname, 'A')
        if self._socket is None:
            self.create_socket()
        seq_data = bytes(question.pack())
        for _ in range(3):
            if server_index >= len(servers):
                break
            self._loading[hostname] += 1
            self._socket.write((seq_data, (servers[server_index], 53)))
            server_index += 1

        def on_timeout():
            if server_index >= len(servers):
                if hostname not in self._cache:
                    self.send_req6(hostname, 0, True)
                return
            if hostname not in self._cache:
                self.send_req(hostname, server_index)
        self._loop.add_timeout(self._resend_timeout, on_timeout)

    def send_req6(self, hostname, server_index=0, use_v4servers=False):
        if use_v4servers:
            if not self._servers:
                return
            servers = self._servers
        else:
            if not self._server6s:
                return
            servers = self._server6s

        question = dnslib.DNSRecord.question(hostname, 'AAAA')
        if self._socket6 is None:
            self.create_socket6()
        seq_data = bytes(question.pack())
        for _ in range(3):
            if server_index >= len(servers):
                break
            self._loading[hostname] += 1
            self._socket6.write((seq_data, (servers[server_index], 53)))
            server_index += 1

        def on_timeout():
            if server_index >= len(servers):
                return
            if hostname not in self._cache:
                self.send_req6(hostname, server_index, use_v4servers)
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
                if not self._queue[hostname]:
                    self._queue[hostname].append(callback)
                    self.send_req(hostname)
                    self.send_req6(hostname)

                    if hostname not in self._cache:
                        def on_timeout():
                            if hostname not in self._cache:
                                self.call_callback(hostname, None)
                        self._loop.add_timeout(timeout or self._resolve_timeout, on_timeout)
                else:
                    self._queue[hostname].append(callback)
            except Exception as e:
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

        for hostname, callbacks in self._queue.items():
            for callback in callbacks:
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