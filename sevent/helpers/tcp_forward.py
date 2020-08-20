# -*- coding: utf-8 -*-
# 2020/7/10
# create by: snower

import time
import random
import traceback
import logging
import argparse
from collections import deque
import sevent

BYTES_MAP = {"B": 1, "K": 1024, "M": 1024*1024, "G": 1024*1024*1024, "T": 1024*1024*1024*1024}

def format_data_len(date_len):
    if date_len < 1024:
        return "%dB" % date_len
    elif date_len < 1024*1024:
        return "%.3fK" % (date_len/1024.0)
    elif date_len < 1024*1024*1024:
        return "%.3fM" % (date_len/(1024.0*1024.0))
    elif date_len < 1024*1024*1024*1024:
        return "%.3fG" % (date_len/(1024.0*1024.0*1024.0))
    return "%.3fT" % (date_len/(1024.0*1024.0*1024.0*1024.0))

def warp_write(conn, status, key):
    origin_write = conn.write
    def _(data):
        status[key] += len(data)
        return origin_write(data)
    return _

def warp_speed_limit_write(conn, status, key):
    conn_id = id(conn)
    origin_write = conn.write
    origin_end = conn.end
    speed_limiter = status["speed_limiter"]
    status["is_end"] = False
    buffer = sevent.buffer.Buffer()

    def warp_end():
        if conn_id not in speed_limiter.buffers:
            return origin_end()
        status["is_end"] = True
    conn.end = warp_end

    def speed_write(data, speed):
        dl = len(data)
        if dl > speed:
            status[key] += buffer.fetch(data, speed)
            try:
                return origin_write(buffer)
            except sevent.tcp.SocketClosed:
                speed_limiter.buffers.pop(conn_id, None)
                if status["is_end"]:
                    status["is_end"] = False
                    sevent.current().add_async(origin_end)
                return False

        if not data:
            speed_limiter.buffers.pop(conn_id, None)
            if status["is_end"]:
                status["is_end"] = False
                sevent.current().add_async(origin_end)
            return True

        status[key] += dl
        try:
            return origin_write(data)
        except sevent.tcp.SocketClosed:
            return False

    def _(data):
        if conn_id in speed_limiter.buffers:
            return
        speed_limiter.buffers[conn_id] = (data, speed_write)
        if not speed_limiter.is_running:
            speed_limiter.is_running = True
            sevent.current().call_async(speed_limiter.loop)

        dl = len(data)
        if speed_limiter.global_speed:
            if speed_limiter.current_speed <= 0:
                return
            speed = min(speed_limiter.current_speed, speed_limiter.speed)
            if dl > speed:
                status[key] += buffer.fetch(data, speed)
                speed_limiter.current_speed -= speed
                return origin_write(buffer)

            status[key] += dl
            speed_limiter.current_speed -= dl
            return origin_write(data)

        if dl > speed_limiter.speed:
            status[key] += buffer.fetch(data, speed_limiter.speed)
            return origin_write(buffer)

        status[key] += dl
        return origin_write(data)
    return _

def warp_delay_write(delayer, warp_write_func):
    def _(conn, status, key):
        origin_write = warp_write_func(conn, status, key)
        buffer = sevent.buffer.Buffer()
        def delay_write(data):
            buffer.write(data)
            try:
                return origin_write(buffer)
            except sevent.tcp.SocketClosed:
                return False

        def __(data):
            if delayer.delay:
                delayer.queues.append((time.time() + delayer.delay, data.read(), delay_write))
            else:
                delayer.queues.append((time.time() + random.randint(*delayer.rdelays) / 1000000.0,
                                       data.read(), delay_write))
            if delayer.is_running:
                return
            delayer.is_running = True
            sevent.current().call_async(delayer.loop)
        return __
    return _

def parse_forward(forwards):
    forward_hosts = []
    for forward in forwards:
        hosts, i = ['0.0.0.0', 0, '127.0.0.1', 0], 0
        forward_info = forward.split(":")
        for f in forward_info:
            if i >= 4:
                break

            if i in (0, 2):
                if not f.isdigit():
                    hosts[i] = f
                    i += 1
                    continue
                hosts[i + 1] = int(f)
                i += 2
                continue
            else:
                if f.isdigit():
                    hosts[i] = int(f)
                    i += 1
                    continue
                hosts[i + 1] = f
                i += 2
                continue
        forward_hosts.append(tuple(hosts))
    return forward_hosts

async def tcp_forward(conns, conn, forward_host, forward_port, status):
    start_time = time.time()
    conn.write, pconn = warp_write(conn, status, "recv_len"), None

    try:
        conn.enable_nodelay()
        pconn = sevent.tcp.Socket()
        pconn.enable_nodelay()
        await pconn.connectof((forward_host, forward_port))
        pconn.write = warp_write(pconn, status, "send_len")
        logging.info("tcp forward connected %s:%d -> %s:%d", conn.address[0], conn.address[1], forward_host, forward_port)
        await pconn.linkof(conn)
    except sevent.errors.SocketClosed:
        pass
    except Exception as e:
        logging.info("tcp forward error %s:%d -> %s:%d %s %.2fms\r%s", conn.address[0], conn.address[1],
                     forward_host, forward_port, e, (time.time() - start_time) * 1000, traceback.format_exc())
        return
    finally:
        conn.close()
        if pconn: pconn.close()
        conns.pop(id(conn), None)

    logging.info("tcp forward closed %s:%d -> %s:%d %s %s %.2fms", conn.address[0], conn.address[1],
                 forward_host, forward_port, format_data_len(status["send_len"]), format_data_len(status["recv_len"]),
                 (time.time() - start_time) * 1000)

async def tcp_forward_server(conns, server, forward_host, forward_port, speed_limiter):
    while True:
        conn = await server.accept()
        status = {"recv_len": 0, "send_len": 0, "last_time": time.time(), "check_recv_len": 0, "check_send_len": 0, "speed_limiter": speed_limiter}
        sevent.current().call_async(tcp_forward, conns, conn, forward_host, forward_port, status)
        conns[id(conn)] = (conn, status)

async def check_timeout(conns, timeout):
    while True:
        try:
            now = time.time()
            for conn_id, (conn, status) in list(conns.items()):
                if status['check_recv_len'] != status['recv_len'] or status['check_send_len'] != status['send_len']:
                    status["check_recv_len"] = status["recv_len"]
                    status["check_send_len"] = status["send_len"]
                    status['last_time'] = now
                    continue

                if now - status['last_time'] >= timeout:
                    conn.close()
                    conns.pop(conn_id, None)
        finally:
            await sevent.current().sleep(30)

class Delayer(object):
    def __init__(self, delay, rdelays):
        self.delay = delay
        self.rdelays = rdelays
        self.is_running = False
        self.queues = deque()

    async def loop(self):
        try:
            while self.queues:
                now = time.time()
                timeout_time, data, callback = self.queues.popleft()
                if timeout_time > now:
                    await sevent.current().sleep(timeout_time - now)
                sevent.current().add_async(callback, data)
        finally:
            self.is_running = False

class SpeedLimiter(object):
    def __init__(self, speed, global_speed):
        self.speed = int(speed) or int(global_speed)
        self.global_speed = int(global_speed)
        self.current_speed = self.global_speed if self.global_speed else 0
        self.buffers = {}
        self.is_running = False
        self.loop = self.loop_global_speed if self.global_speed else self.loop_speed

    async def loop_speed(self):
        try:
            current_timestamp = time.time()
            await sevent.current().sleep(0.1)
            while self.buffers:
                try:
                    for _, (data, callback) in list(self.buffers.items()):
                        sevent.current().add_async(callback, data, self.speed)
                finally:
                    now = time.time()
                    sleep_time = 0.2 - (now - current_timestamp)
                    current_timestamp = now
                    await sevent.current().sleep(sleep_time)
        finally:
            self.is_running = False

    async def loop_global_speed(self):
        try:
            current_timestamp = time.time()
            await sevent.current().sleep(0.1)
            while self.buffers:
                try:
                    avg_speed = int(self.global_speed / len(self.buffers))
                    max_speed, over_speed = min(avg_speed, self.speed), 0
                    speed_buffers = []
                    for conn_id, (data, callback) in list(self.buffers.items()):
                        dl = len(data)
                        if max_speed > dl:
                            speed_buffers.append((data, dl, dl, callback))
                            over_speed += avg_speed - dl
                        else:
                            speed_buffers.append((data, dl, max_speed, callback))
                            over_speed += avg_speed - max_speed

                    for data, dl, speed, callback in speed_buffers:
                        if over_speed > 0 and dl > speed and speed < self.speed:
                            can_speed = min(min(dl, self.speed) - speed, over_speed)
                            speed += can_speed
                            over_speed -= can_speed
                        sevent.current().add_async(callback, data, speed)
                    self.current_speed = over_speed
                finally:
                    now = time.time()
                    sleep_time = 0.2 - (now - current_timestamp)
                    current_timestamp = now
                    await sevent.current().sleep(sleep_time)
        finally:
            self.current_speed = self.global_speed
            self.is_running = False

async def tcp_forward_servers(servers, timeout, speed, global_speed):
    conns, speed_limiter = {}, (SpeedLimiter(speed, global_speed) if speed or global_speed else None)
    for server, (forward_host, forward_port) in servers:
        sevent.current().call_async(tcp_forward_server, conns, server, forward_host, forward_port, speed_limiter)
    await check_timeout(conns, timeout)

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)1.1s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S', filemode='a+')

    parser = argparse.ArgumentParser(description="tcp port forward")
    parser.add_argument('-L', dest='forwards', default=[], action="append", type=str,
                        help='forward host, accept format [[local_bind:]local_port:remote_host:remote_port], support muiti forward args (example: 0.0.0.0:80:127.0.0.1:8088 or 80:127.0.0.1:8088)')
    parser.add_argument('-t', dest='timeout', default=7200, type=int, help='no read/write timeout (default: 7200)')
    parser.add_argument('-s', dest='speed', default=0, type=lambda v: int(float(v[:-1]) * BYTES_MAP[v.upper()[-1]]) \
        if v and v.upper()[-1] in BYTES_MAP else int(float(v)), help='per connection speed limit byte, example: 1024 or 1M (default: 0 is unlimit), available units : B K M G T')
    parser.add_argument('-S', dest='global_speed', default=0, type=lambda v: int(float(v[:-1]) * BYTES_MAP[v.upper()[-1]]) \
        if v and v.upper()[-1] in BYTES_MAP else int(float(v)), help='global speed limit byte, example: 1024 or 1M (default: 0 is unlimit), available units : B K M G T')
    parser.add_argument('-d', dest='delay', default=0, type=lambda v: (float(v.split("-")[0]), float(v.split("-")[-1])) \
        if v and isinstance(v, str) and "-" in v else float(v), help='delay millisecond (default: 0 is not delay, example -d100 or -d100-200), the - between two numbers will be random delay')
    args = parser.parse_args()

    if not args.forwards:
        exit(0)

    forwards = parse_forward(args.forwards)
    if not forwards:
        exit(0)

    if args.delay:
        warp_write = warp_delay_write(Delayer(0 if isinstance(args.delay, tuple) else float(args.delay) / 1000.0,
                                    (int(args.delay[0] * 1000), int(args.delay[1] * 1000)) if isinstance(args.delay, tuple) else None),
                                    warp_speed_limit_write if args.speed or args.global_speed else warp_write)
    elif args.speed or args.global_speed:
        warp_write = warp_speed_limit_write

    forward_servers = []
    for (bind, port, forward_host, forward_port) in forwards:
        server = sevent.tcp.Server()
        server.enable_reuseaddr()
        server.listen((bind, port))
        forward_servers.append((server, (forward_host, forward_port)))
        logging.info("port forward listen %s:%s to %s:%s", bind, port, forward_host, forward_port)

    try:
        sevent.run(tcp_forward_servers, forward_servers, args.timeout,
                   int(args.speed / 10), int(args.global_speed / 10))
    except KeyboardInterrupt:
        exit(0)