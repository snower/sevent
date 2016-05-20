# -*- coding: utf-8 -*-
# 15/8/6
# create by: snower

import os
import time
import re
import struct
import socket
import logging
from collections import defaultdict
from loop import instance
from event import EventEmitter

VALID_HOSTNAME = re.compile(br"(?!-)[A-Z\d-]{1,63}(?<!-)$", re.IGNORECASE)

QTYPE_ANY = 255
QTYPE_A = 1
QTYPE_AAAA = 28
QTYPE_CNAME = 5
QTYPE_NS = 2
QCLASS_IN = 1

STATUS_IPV4 = 0
STATUS_IPV6 = 1

STATUS_OPENED = 0
STATUS_CLOSED = 1

class DNSCache(object):
    def __init__(self, default_ttl = 60):
        self.default_ttl = default_ttl
        self._cache = {}

    def set(self, hostname, answers, type, ttl = None):
        self._cache[hostname] = {
            "answers": answers,
            'type': type,
            "cache_time": time.time(),
            "ttl": ttl or self.default_ttl,
        }

    def get(self, hostname):
        answers =  self._cache.get(hostname)
        if answers:
            if answers["cache_time"] + answers["ttl"] >= time.time():
                return answers["answers"][0][0], answers["type"]
            del self._cache[hostname]
        return None, None

    def remove(self, hostname):
        if hostname in self._cache:
            self._cache.pop(hostname)

    def clear(self):
        self._cache = {}

    def __setitem__(self, hostname, ip):
        return self.set(hostname, [[ip, QTYPE_A, self.default_ttl]], QTYPE_A)

    def __getitem__(self, hostname):
        return self.get(hostname)[0]

    def __delitem__(self, hostname):
        return self.remove(hostname)

    def __contains__(self, hostname):
        return bool(self.get(hostname)[0])

class DNSResponse(object):
    def __init__(self):
        self.hostname = None
        self.questions = []  # each: (addr, type, class)
        self.answers = []  # each: (addr, type, class)

    def __str__(self):
        return '%s: %s' % (self.hostname, str(self.answers))

class DNSResolver(EventEmitter):
    _instance = None

    @classmethod
    def default(cls):
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def __init__(self, loop=None, servers=None, hosts=None):
        super(DNSResolver, self).__init__()

        self._loop = loop or instance()
        self._servers = servers or []
        self._hosts = hosts or {}

        self._cache = DNSCache()
        self._queue = defaultdict(list)
        self._hostname_status = {}
        self._hostname_server_index = {}
        self._socket = None
        self._status = STATUS_OPENED

        if not servers:
            self.parse_resolv()
        if not hosts:
            self.parse_hosts()
        self.create_socket()

    def create_socket(self):
        from udp import Socket
        self._socket = Socket(self._loop)
        self._socket.on("data", self.on_data)
        self._socket.on("close", self.on_close)

    def parse_resolv(self):
        self._servers = []
        try:
            with open('/etc/resolv.conf', 'rb') as f:
                content = f.readlines()
                for line in content:
                    line = line.strip()
                    if line:
                        if line.startswith(b'nameserver'):
                            parts = line.split()
                            if len(parts) >= 2:
                                server = parts[1]
                                if self.is_ip(server) == socket.AF_INET:
                                    if type(server) != str:
                                        server = server.decode('utf8')
                                    self._servers.append(server)
        except IOError:
            pass
        if not self._servers:
            self._servers = ['8.8.4.4', '8.8.8.8']

    def parse_hosts(self):
        etc_path = '/etc/hosts'
        if 'WINDIR' in os.environ:
            etc_path = os.environ['WINDIR'] + '/system32/drivers/etc/hosts'
        try:
            with open(etc_path, 'rb') as f:
                for line in f.readlines():
                    line = line.strip()
                    parts = line.split()
                    if len(parts) >= 2:
                        ip = parts[0]
                        if self.is_ip(ip):
                            for i in range(1, len(parts)):
                                hostname = parts[i]
                                if hostname:
                                    self._hosts[hostname] = ip
        except IOError:
            self._hosts['localhost'] = '127.0.0.1'

    def call_callback(self, hostname, ip):
        callbacks = self._queue[hostname]
        del self._queue[hostname]

        for callback in callbacks:
            callback(hostname, ip)

        self.emit("resolve", self, hostname, ip)

        if hostname in self._hostname_server_index:
            del self._hostname_server_index[hostname]
            
        if hostname in self._hostname_status:
            del self._hostname_status[hostname]

    def on_data(self, socket, address, buffer):
        for data in buffer:
            if address[0] not in self._servers:
                return

            response = self.parse_response(data)
            if response and response.hostname:
                hostname = response.hostname

                answers = []
                for answer in response.answers:
                    if answer[2] in (QTYPE_A, QTYPE_AAAA) and answer[3] == QCLASS_IN:
                        answers.append((answer[1], answer[2], answer[4]))

                hostname_status = self._hostname_status.get(hostname, 0)
                if not answers:
                    if hostname_status == 1:
                        self.send_req(hostname)
                        self._hostname_status[hostname] = 0
                    elif hostname_status == 2:
                        self.call_callback(hostname, self._cache[hostname])
                    else:
                        self._hostname_status[hostname] = 1
                else:
                    ip, type, ttl = answers[0]
                    self._hostname_status[hostname] = 2
                    cip, ctype = self._cache.get(hostname)
                    if not cip or ctype == QTYPE_AAAA:
                        self._cache.set(hostname, answers, type, ttl)
                    if type == QTYPE_A or (hostname_status == 1 and type == QTYPE_AAAA):
                        self.call_callback(hostname, ip)

    def send_req(self, hostname, qtype=None):
        qtype = ([QTYPE_A, QTYPE_AAAA] if qtype is None else [qtype]) if not isinstance(qtype, (list, tuple)) else qtype
        server_index = self._hostname_server_index.get(hostname, -1)
        if server_index + 1 >= len(self._servers):
            self.call_callback(hostname, None)
        else:
            server = self._servers[server_index + 1]
            for qt in qtype:
                req = self.build_request(hostname, qt)
                self._socket.write((server, 53), req)
            self._hostname_server_index[hostname] = server_index + 1

    def resolve(self, hostname, callback, timeout = 5):
        if self._status == STATUS_CLOSED:
            return callback(hostname, None)

        if isinstance(hostname, unicode):
            hostname = hostname.encode('utf8')
        if not hostname:
            callback(hostname, None)

        elif self.is_ip(hostname):
            callback(hostname, hostname)
        elif hostname in self._hosts:
            callback(hostname, self._hosts[hostname])
        elif hostname in self._cache:
            callback(hostname, self._cache[hostname])
        else:
            if not self.is_valid_hostname(hostname):
                callback(hostname, None)
            else:
                callbacks = self._queue[hostname]
                if not callbacks:
                    self.send_req(hostname)
                    if timeout > 0:
                        def on_timeout():
                            if hostname not in self._cache:
                                self.call_callback(hostname, None)
                            elif self._hostname_status.get(hostname, 0) == 2:
                                self.call_callback(hostname, self._cache[hostname])
                        self._loop.timeout(timeout, on_timeout)
                callbacks.append(callback)

    def flush(self):
        self._cache.clear()

    def on_close(self, socket):
        if self._status == STATUS_CLOSED:
            return

        self.create_socket()

    def close(self):
        if self._status == STATUS_CLOSED:
            return

        self._status = STATUS_CLOSED
        self._socket.close()
        self._socket = None

        for hostname, callbacks in self._queue:
            for callback in callbacks:
                callback(hostname, None)

    def build_address(self, address):
        address = address.strip(b'.')
        labels = address.split(b'.')
        results = []
        for label in labels:
            l = len(label)
            if l > 63:
                return None
            results.append(chr(l))
            results.append(label)
        results.append(b'\0')
        return b''.join(results)


    def build_request(self, address, qtype):
        request_id = os.urandom(2)
        header = struct.pack('!BBHHHH', 1, 0, 1, 0, 0, 0)
        addr = self.build_address(address)
        qtype_qclass = struct.pack('!HH', qtype, QCLASS_IN)
        return request_id + header + addr + qtype_qclass


    def parse_ip(self, addrtype, data, length, offset):
        if addrtype == QTYPE_A:
            return socket.inet_ntoa(data[offset:offset + length])
        elif addrtype == QTYPE_AAAA:
            if hasattr(socket, "inet_ntop"):
                return socket.inet_ntop(socket.AF_INET6, data[offset:offset + length])
            else:
                return ""
        elif addrtype in [QTYPE_CNAME, QTYPE_NS]:
            return self.parse_name(data, offset)[1]
        else:
            return data[offset:offset + length]


    def parse_name(self, data, offset):
        p = offset
        labels = []
        l = ord(data[p])
        while l > 0:
            if (l & (128 + 64)) == (128 + 64):
                # pointer
                pointer = struct.unpack('!H', data[p:p + 2])[0]
                pointer &= 0x3FFF
                r = self.parse_name(data, pointer)
                labels.append(r[1])
                p += 2
                # pointer is the end
                return p - offset, b'.'.join(labels)
            else:
                labels.append(data[p + 1:p + 1 + l])
                p += 1 + l
            l = ord(data[p])
        return p - offset + 1, b'.'.join(labels)

    def parse_record(self, data, offset, question=False):
        nlen, name = self.parse_name(data, offset)
        if not question:
            record_type, record_class, record_ttl, record_rdlength = struct.unpack(
                '!HHIH', data[offset + nlen:offset + nlen + 10]
            )
            ip = self.parse_ip(record_type, data, record_rdlength, offset + nlen + 10)
            return nlen + 10 + record_rdlength, \
                (name, ip, record_type, record_class, record_ttl)
        else:
            record_type, record_class = struct.unpack(
                '!HH', data[offset + nlen:offset + nlen + 4]
            )
            return nlen + 4, (name, None, record_type, record_class, None, None)


    def parse_header(self, data):
        if len(data) >= 12:
            header = struct.unpack('!HBBHHHH', data[:12])
            res_id = header[0]
            res_qr = header[1] & 128
            res_tc = header[1] & 2
            res_ra = header[2] & 128
            res_rcode = header[2] & 15
            # assert res_tc == 0
            # assert res_rcode in [0, 3]
            res_qdcount = header[3]
            res_ancount = header[4]
            res_nscount = header[5]
            res_arcount = header[6]
            return (res_id, res_qr, res_tc, res_ra, res_rcode, res_qdcount,
                    res_ancount, res_nscount, res_arcount)
        return None


    def parse_response(self, data):
        try:
            if len(data) >= 12:
                header = self.parse_header(data)
                if not header:
                    return None
                res_id, res_qr, res_tc, res_ra, res_rcode, res_qdcount, \
                    res_ancount, res_nscount, res_arcount = header

                qds = []
                ans = []
                offset = 12
                for i in range(0, res_qdcount):
                    l, r = self.parse_record(data, offset, True)
                    offset += l
                    if r:
                        qds.append(r)
                for i in range(0, res_ancount):
                    l, r = self.parse_record(data, offset)
                    offset += l
                    if r:
                        ans.append(r)
                for i in range(0, res_nscount):
                    l, r = self.parse_record(data, offset)
                    offset += l
                for i in range(0, res_arcount):
                    l, r = self.parse_record(data, offset)
                    offset += l
                response = DNSResponse()
                if qds:
                    response.hostname = qds[0][0]
                response.questions = qds
                response.answers = ans
                return response
        except Exception as e:
            logging.exception("parse dns rsponse error:%s", e)
            return None

    def is_valid_hostname(self, hostname):
        if len(hostname) > 255:
            return False
        if hostname[-1] == b'.':
            hostname = hostname[:-1]
        return all(VALID_HOSTNAME.match(x) for x in hostname.split(b'.'))

    def inet_pton(self, family, addr):
        if family == socket.AF_INET:
            return socket.inet_aton(addr)
        elif family == socket.AF_INET6:
            if '.' in addr:  # a v4 addr
                v4addr = addr[addr.rindex(':') + 1:]
                v4addr = socket.inet_aton(v4addr)
                v4addr = map(lambda x: ('%02X' % ord(x)), v4addr)
                v4addr.insert(2, ':')
                newaddr = addr[:addr.rindex(':') + 1] + ''.join(v4addr)
                return self.inet_pton(family, newaddr)
            dbyts = [0] * 8  # 8 groups
            grps = addr.split(':')
            for i, v in enumerate(grps):
                if v:
                    dbyts[i] = int(v, 16)
                else:
                    for j, w in enumerate(grps[::-1]):
                        if w:
                            dbyts[7 - j] = int(w, 16)
                        else:
                            break
                    break
            return b''.join((chr(i // 256) + chr(i % 256)) for i in dbyts)
        else:
            raise RuntimeError("What family?")

    def is_ip(self, address):
        for family in (socket.AF_INET, socket.AF_INET6):
            try:
                if type(address) != str:
                    address = address.decode('utf8')
                self.inet_pton(family, address)
                return family
            except (TypeError, ValueError, OSError, IOError):
                pass
        return False
