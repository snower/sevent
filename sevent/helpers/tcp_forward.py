# -*- coding: utf-8 -*-
# 2020/7/10
# create by: snower

import sys
import time
import struct
import random
import traceback
import logging
import argparse
from collections import deque
import threading
import socket
import sevent
from .utils import create_server, create_socket, config_signal, format_data_len, is_subnet

BYTES_MAP = {"B": 1, "K": 1024, "M": 1024*1024, "G": 1024*1024*1024, "T": 1024*1024*1024*1024}

def host_parse(host):
    hosts, subnet, cs = [], ["", 0], []
    is_brackets, is_subnet = False, False
    for c in host:
        if c == ":":
            if not is_brackets and not is_subnet:
                hosts.append("".join(cs))
                cs = []
            else:
                cs.append(c)
        elif c == "[":
            is_brackets = True
        elif c == "]":
            is_brackets = False
        elif c == "|":
            hosts.append("".join(cs))
            cs = []
            is_subnet = True
        elif c == "/":
            subnet[0] = "".join(cs)
            cs = []
        else:
            cs.append(c)

    if is_subnet:
        if subnet[0]:
            subnet[1] = int("".join(cs) if cs else "32")
        else:
            subnet = ["".join(cs), 32]
    else:
        hosts.append("".join(cs))
        subnet = ["0.0.0.0", 0]

    if len(hosts) == 2:
        if hosts[0].isdigit() and hosts[1].isdigit():
            hosts = [("0.0.0.0", int(hosts[0])), ("127.0.0.1", int(hosts[1]))]
        elif hosts[0] == "":
            hosts = [("0.0.0.0", 0), ("127.0.0.1", int(hosts[1]))]
        else:
            hosts = [("0.0.0.0", 0), (hosts[0]), int(hosts[1])]
    elif len(hosts) == 3:
        if hosts[0].isdigit():
            hosts = [("0.0.0.0", int(hosts[0])), (hosts[1], int(hosts[2]))]
        elif hosts[0] == "":
            hosts = [("0.0.0.0", 0), (hosts[1], int(hosts[2]))]
        elif hosts[2].isdigit():
            hosts = [(hosts[0], int(hosts[1])), ("127.0.0.1", int(hosts[2]))]
        else:
            hosts = [(hosts[0], int(hosts[1])), ("127.0.0.1", 0)]
    elif len(hosts) == 4:
        hosts = [(hosts[0], int(hosts[1])), (hosts[2], int(hosts[3]))]
    else:
        raise ValueError(u"host error %s", host)

    try:
        subnet[0] = struct.unpack("!I", socket.inet_pton(socket.AF_INET, subnet[0]))[0]
        subnet[1] = ~ (0xffffffff >> subnet[1])
    except:
        subnet[0] = (struct.unpack("!QQ", socket.inet_pton(socket.AF_INET6, subnet[0])))
        subnet[1] = (~ (0xffffffffffffffff >> min(subnet[1], 64)), ~ (0xffffffffffffffff >> max(subnet[1] - 64, 0)))
    return hosts, subnet

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
    current_speed_key = "recv_current_speed" if key == "recv_len" else "send_current_speed"
    end_key = "recv_is_end" if key == "recv_len" else "send_is_end"
    status[end_key] = False
    buffer = sevent.buffer.Buffer()

    def warp_end():
        if conn_id not in speed_limiter.buffers:
            return origin_end()
        status[end_key] = True
    conn.end = warp_end

    def speed_write(data, speed):
        dl = len(data)
        if dl > speed:
            if speed <= 0:
                if speed_limiter.current_speed <= 0:
                    return
                speed = min(speed_limiter.current_speed, speed_limiter.speed)
                if dl > speed:
                    status[key] += buffer.fetch(data, speed)
                    speed_limiter.current_speed -= speed
                else:
                    status[key] += dl
                    speed_limiter.current_speed -= dl
            else:
                status[key] += buffer.fetch(data, speed)
            try:
                return origin_write(buffer)
            except sevent.tcp.SocketClosed:
                speed_limiter.buffers.pop(conn_id, None)
                if status[end_key]:
                    status[end_key] = False
                    sevent.current().add_async(origin_end)
                return False

        if not data:
            speed_limiter.buffers.pop(conn_id, None)
            if status[end_key]:
                status[end_key] = False
                sevent.current().add_async(origin_end)
            return True

        status[key] += dl
        try:
            return origin_write(data)
        except sevent.tcp.SocketClosed:
            return False

    def _(data):
        if conn_id in speed_limiter.buffers:
            current_speed = status[current_speed_key]
            if current_speed <= 0:
                return

            if speed_limiter.global_speed:
                if speed_limiter.current_speed <= 0:
                    return

                dl = len(data)
                speed = min(speed_limiter.current_speed, current_speed)
                if dl > speed:
                    status[key] += buffer.fetch(data, speed)
                    speed_limiter.current_speed -= speed
                    status[current_speed_key] -= speed
                    return origin_write(buffer)

                status[key] += dl
                speed_limiter.current_speed -= dl
                status[current_speed_key] -= dl
                return origin_write(data)

            dl = len(data)
            if dl > current_speed:
                status[key] += buffer.fetch(data, current_speed)
                status[current_speed_key] = 0
                return origin_write(buffer)

            status[key] += dl
            status[current_speed_key] -= dl
            return origin_write(data)

        speed_limiter.buffers[conn_id] = (data, speed_write)
        if not speed_limiter.is_running:
            speed_limiter.is_running = True
            sevent.current().call_async(speed_limiter.loop)

        dl = len(data)
        if speed_limiter.global_speed:
            if speed_limiter.current_speed <= 0:
                status[current_speed_key] = 0
                return
            speed = min(speed_limiter.current_speed, speed_limiter.speed)
            if dl > speed:
                status[key] += buffer.fetch(data, speed)
                speed_limiter.current_speed -= speed
                status[current_speed_key] = 0
                return origin_write(buffer)

            status[key] += dl
            speed_limiter.current_speed -= dl
            status[current_speed_key] = speed_limiter.speed - dl
            return origin_write(data)

        if dl > speed_limiter.speed:
            status[key] += buffer.fetch(data, speed_limiter.speed)
            status[current_speed_key] = 0
            return origin_write(buffer)

        status[key] += dl
        status[current_speed_key] = speed_limiter.speed - dl
        return origin_write(data)
    return _

def warp_delay_write(delayer, warp_write_func):
    def _(conn, status, key):
        origin_write = warp_write_func(conn, status, key)
        origin_end = conn.end
        buffer = sevent.buffer.Buffer()
        key = "delay_%s_buffer_len" % id(buffer)
        status[key] = 0
        end_key = "delay_recv_is_end" if key == "recv_len" else "delay_send_is_end"
        status[end_key] = False

        def warp_end():
            if status[key] <= 0:
                return origin_end()
            status[end_key] = True
        conn.end = warp_end

        def delay_write(data, data_len):
            status[key] -= buffer.fetch(data, data_len)
            try:
                return origin_write(buffer)
            except sevent.tcp.SocketClosed:
                return False
            finally:
                if not data and status[end_key]:
                    status[end_key] = False
                    sevent.current().add_async(origin_end)

        def __(data):
            data_len = max(len(data) - status[key], 0)
            if delayer.delay:
                delayer.queues.append((time.time() + delayer.delay, data, data_len, delay_write))
            else:
                delayer.queues.append((time.time() + random.randint(*delayer.rdelays) / 1000000.0,
                                       data, data_len, delay_write))
            status[key] += data_len
            if delayer.is_running:
                return
            delayer.is_running = True
            sevent.current().call_async(delayer.loop)
        return __
    return _

def warp_mirror_write(mirror_host, mirror_header, warp_write_func):
    try:
        mirror_host, subnet = host_parse(mirror_host)
        up_address, down_address = mirror_host[0], mirror_host[1]
        if up_address[0] == "0.0.0.0":
            up_address = ("127.0.0.1", up_address[1])
        if down_address[0] == "0.0.0.0":
            down_address = ("127.0.0.1", down_address[1])
    except ValueError:
        logging.error("mirror address error %s", mirror_host)
        return warp_write_func
    mirror_header = mirror_header.replace("{", "%(").replace("}", ")s")

    def warp_up_conn_write(conn, status, key):
        origin_write = conn.write
        up_buffer = sevent.buffer.Buffer()

        def up_conn_write(data):
            try:
                if "mirror_up_conn" not in status:
                    if up_address == down_address and status.get("mirror_down_conn"):
                        status['mirror_up_conn'] = status.get("mirror_down_conn")
                    else:
                        mirror_conn = create_socket(up_address)
                        mirror_conn.connect(up_address, 5)
                        mirror_conn.on_connect(lambda s: mirror_conn.end() if conn._state == sevent.tcp.STATE_CLOSED else None)
                        mirror_conn.on_data(lambda s, b: b.read())
                        conn.on_close(lambda s: mirror_conn.end() if mirror_conn._state != sevent.tcp.STATE_CONNECTING else None)
                        status['mirror_up_conn'] = mirror_conn
                        try:
                            if mirror_header:
                                up_buffer.write((mirror_header % status["mirror_variables"]).encode("utf-8"))
                        except:
                            pass
                    logging.info("mirror up copy to %s:%s", up_address[0], up_address[1])
                up_buffer.copyfrom(data)
                status['mirror_up_conn'].write(up_buffer)
            except:
                pass
            return origin_write(data)
        return up_conn_write

    def warp_down_conn_write(conn, status, key):
        origin_write = conn.write
        down_buffer = sevent.buffer.Buffer()

        def down_conn_write(data):
            try:
                if "mirror_down_conn" not in status:
                    if up_address == down_address and status.get("mirror_up_conn"):
                        status['mirror_down_conn'] = status.get("mirror_up_conn")
                    else:
                        mirror_conn = create_socket(down_address)
                        mirror_conn.connect(down_address, 5)
                        mirror_conn.on_connect(lambda s: mirror_conn.end() if conn._state == sevent.tcp.STATE_CLOSED else None)
                        mirror_conn.on_data(lambda s, b: b.read())
                        conn.on_close(lambda s: mirror_conn.end() if mirror_conn._state != sevent.tcp.STATE_CONNECTING else None)
                        status['mirror_down_conn'] = mirror_conn
                        try:
                            if mirror_header:
                                down_buffer.write((mirror_header % status["mirror_variables"]).encode("utf-8"))
                        except Exception as e:
                            pass
                    logging.info("mirror down copy to %s:%s", down_address[0], down_address[1])
                down_buffer.copyfrom(data)
                status['mirror_down_conn'].write(down_buffer)
            except:
                pass
            return origin_write(data)
        return down_conn_write

    def _(conn, status, key):
        if mirror_header and "mirror_variables" not in status:
            status["mirror_variables"] = {"conn_id": "", "from_host": "", "from_port": 0, "to_host": "", "to_port": 0}
        if key == "send_len":
            if "mirror_subnet" not in status or status["mirror_subnet"]:
                if up_address and len(up_address) >= 2 and up_address[1] > 0:
                    conn.write = warp_up_conn_write(conn, status, key)
                if mirror_header:
                    status["mirror_variables"]["to_host"] = conn.address[0]
                    status["mirror_variables"]["to_port"] = conn.address[1]
            else:
                status.pop("mirror_variables", None)
        if key == "recv_len":
            if subnet and not is_subnet(conn.address[0], subnet):
                status["mirror_subnet"] = False

            if "mirror_subnet" not in status or status["mirror_subnet"]:
                if down_address and len(down_address) >= 2 and down_address[1] > 0:
                    conn.write = warp_down_conn_write(conn, status, key)
                if mirror_header:
                    status["mirror_variables"]["conn_id"] = id(conn)
                    status["mirror_variables"]["from_host"] = conn.address[0]
                    status["mirror_variables"]["from_port"] = conn.address[1]
            else:
                status.pop("mirror_variables", None)
        return warp_write_func(conn, status, key)
    return _

async def tcp_forward(conns, conn, forward_address, subnet, status):
    start_time = time.time()
    conn.write, pconn = warp_write(conn, status, "recv_len"), None

    try:
        conn.enable_nodelay()
        pconn = create_socket(forward_address)
        await pconn.connectof(forward_address)
        pconn.write = warp_write(pconn, status, "send_len")
        logging.info("tcp forward connected %s:%d -> %s:%d", conn.address[0], conn.address[1], forward_address[0], forward_address[1])
        await pconn.linkof(conn)
    except sevent.errors.SocketClosed:
        pass
    except Exception as e:
        logging.info("tcp forward error %s:%d -> %s:%d %s %.2fms\r%s", conn.address[0], conn.address[1],
                     forward_address[0], forward_address[1], e, (time.time() - start_time) * 1000, traceback.format_exc())
        return
    finally:
        conn.close()
        if pconn: pconn.close()
        conns.pop(id(conn), None)

    logging.info("tcp forward closed %s:%d -> %s:%d %s %s %.2fms", conn.address[0], conn.address[1],
                 forward_address[0], forward_address[1], format_data_len(status["send_len"]), format_data_len(status["recv_len"]),
                 (time.time() - start_time) * 1000)

async def tcp_forward_server(conns, server, forward_hosts, speed_limiter):
    while True:
        conn = await server.accept()
        conn_ip, forward_address, subnet = conn.address[0], None, None
        for a, s in forward_hosts:
            if is_subnet(conn_ip, s):
                forward_address, subnet = a, s
                break

        if not forward_address:
            conn.close()
            logging.info("tcp forward subnet fail %s:%d", conn.address[0], conn.address[1])
            return

        status = {"recv_len": 0, "send_len": 0, "last_time": time.time(), "check_recv_len": 0, "check_send_len": 0, "speed_limiter": speed_limiter}
        sevent.current().call_async(tcp_forward, conns, conn, forward_address, subnet, status)
        conns[id(conn)] = (conn, status)

async def check_timeout(conns, timeout):
    def run_check():
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
                        sevent.current().add_async_safe(conn.close)
                        conns.pop(conn_id, None)
            finally:
                time.sleep(min(float(timeout) / 2.0, 30))

    if timeout > 0:
        check_thread = threading.Thread(target=run_check)
        check_thread.daemon = True
        check_thread.start()
    await sevent.Future()

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
                timeout_time, data, data_len, callback = self.queues.popleft()
                if timeout_time > now:
                    await sevent.current().sleep(timeout_time - now)
                sevent.current().add_async(callback, data, data_len)
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
                    for _, (data, callback) in self.buffers.items():
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
    for server, forward_hosts in servers:
        sevent.current().call_async(tcp_forward_server, conns, server, forward_hosts, speed_limiter)
    await check_timeout(conns, timeout)

def main(argv):
    global warp_write

    parser = argparse.ArgumentParser(description="tcp port forward")
    parser.add_argument('-L', dest='forwards', default=[], action="append", type=str,
                        help='forward host, accept format [[local_bind:]local_port:remote_host:remote_port]|[subnet], support muiti forward args (example: 0.0.0.0:80:127.0.0.1:8088 or 80:192.168.0.2:8088|192.168.0.0/24)')
    parser.add_argument('-t', dest='timeout', default=7200, type=int, help='no read/write timeout (default: 7200)')
    parser.add_argument('-s', dest='speed', default=0, type=lambda v: int(float(v[:-1]) * BYTES_MAP[v.upper()[-1]]) \
        if v and v.upper()[-1] in BYTES_MAP else int(float(v)), help='per connection speed limit byte, example: 1024 or 1M (default: 0 is unlimit), available units : B K M G T')
    parser.add_argument('-S', dest='global_speed', default=0, type=lambda v: int(float(v[:-1]) * BYTES_MAP[v.upper()[-1]]) \
        if v and v.upper()[-1] in BYTES_MAP else int(float(v)), help='global speed limit byte, example: 1024 or 1M (default: 0 is unlimit), available units : B K M G T')
    parser.add_argument('-d', dest='delay', default=0, type=lambda v: (float(v.split("-")[0]), float(v.split("-")[-1])) \
        if v and isinstance(v, str) and "-" in v else float(v), help='delay millisecond (default: 0 is not delay, example -d100 or -d100-200), the - between two numbers will be random delay')
    parser.add_argument('-M', dest='mirror_host', default="", type=str,
                        help='mirror host, accept format [[up_host:]up_port:[down_host]:down_port] (example: 0.0.0.0:80:127.0.0.1:8088 or :127.0.0.1:8088 or 127.0.0.1:8088: or 8088:8088)')
    parser.add_argument('-F', dest='mirror_header', default="", type=str,
                        help='mirror header, accept variables [from_host|from_port|to_host|to_port|conn_id] (example: "{conn_id}-{from_host}:{from_port}->{to_host}:{to_port}\\r\\n")')
    args = parser.parse_args(args=argv)
    config_signal()
    if not args.forwards:
        exit(0)

    forwards = {}
    for forward in args.forwards:
        hosts, subnet = host_parse(forward)
        if hosts[0] not in forwards:
            forwards[hosts[0]] = []
        forwards[hosts[0]].append((hosts[1], subnet))
    if not forwards:
        exit(0)

    if args.speed or args.global_speed:
        warp_write = warp_speed_limit_write

    if args.delay:
        warp_write = warp_delay_write(Delayer(0 if isinstance(args.delay, tuple) else float(args.delay) / 1000.0,
                                    (int(args.delay[0] * 1000), int(args.delay[1] * 1000)) if isinstance(args.delay, tuple) else None),
                                    warp_write)

    if args.mirror_host:
        warp_write = warp_mirror_write(args.mirror_host, args.mirror_header, warp_write)

    forward_servers = []
    for bind_address, forward_hosts in forwards.items():
        server = create_server(bind_address)
        forward_servers.append((server, sorted(forward_hosts,
                                               key=lambda x: x[1][1][0] * 0xffffffffffffffff + x[1][1][1] if isinstance(x[1][1], tuple) else x[1][1],
                                               reverse=True)))
        logging.info("port forward listen %s:%s", bind_address[0], bind_address[1])

    sevent.current().call_async(tcp_forward_servers, forward_servers, args.timeout,
                                int(args.speed / 10), int(args.global_speed / 10))

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)1.1s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S', filemode='a+')
    try:
        main(sys.argv[1:])
        sevent.instance().start()
    except KeyboardInterrupt:
        exit(0)