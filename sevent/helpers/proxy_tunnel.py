# -*- coding: utf-8 -*-
# 2025/9/1
# create by: snower

import argparse
import hashlib
import logging
import os
import struct
import sys
import time
import traceback
from collections import deque

import sevent
from sevent.helpers.simple_proxy import parse_allow_deny_host, parse_allow_deny_host_filename, check_timeout, tcp_proxy, \
    warp_write
from sevent.helpers.utils import config_signal, create_server, create_socket, format_data_len
from ..buffer import Buffer, BaseBuffer
from ..errors import SocketClosed, ConnectError
from ..event import EventEmitter, null_emit_callback
from ..loop import instance
from ..tcp import Socket, STATE_STREAMING, STATE_CLOSED, STATE_CLOSING
from ..utils import get_logger

FRAME_TYPE_AUTH = 0x01
FRAME_TYPE_AUTHED = 0x02
FRAME_TYPE_PING = 0x03
FRAME_TYPE_PONG = 0x04
FRAME_TYPE_OPEN = 0x11
FRAME_TYPE_CLOSING = 0x12
FRAME_TYPE_CLOSED = 0x13
FRAME_TYPE_RESET = 0x14
FRAME_TYPE_DATA = 0x15
FRAME_TYPE_DRAIN = 0x16
FRAME_TYPE_REGAIN = 0x017

FRAME_MAX_SIZE = 2560


class TunnelStream(Socket):
    def __init__(self, loop=None, stream_id=None, tunnel=None, max_buffer_size=None):
        EventEmitter.__init__(self)

        self._loop = loop or instance()
        self._stream_id = stream_id
        self._tunnel = tunnel
        self._max_buffer_size = max_buffer_size or Socket.MAX_BUFFER_SIZE
        self._rbuffers = Buffer(max_buffer_size=self._max_buffer_size)
        self._wbuffers = Buffer(max_buffer_size=self._max_buffer_size)
        self._state = STATE_STREAMING
        self._rbuffers.on("drain", lambda _: self.drain())
        self._rbuffers.on("regain", lambda _: self.regain())
        self._has_drain_event = False
        self._writing = False
        self.ignore_write_closed_error = False
        self._frame_closing = False

    @property
    def address(self):
        return ("tunnel(%d)" % len(self._tunnel._streams), self._stream_id)

    @property
    def socket(self):
        return self

    def enable_fast_open(self):
        pass

    @property
    def is_enable_fast_open(self):
        return False

    def enable_nodelay(self):
        pass

    @property
    def is_enable_nodelay(self):
        return True

    def end(self):
        if self._state != STATE_STREAMING:
            return
        if self._wbuffers:
            self._state = STATE_CLOSING
        else:
            self._loop.add_async(self.close)

    def close(self):
        if self._state == STATE_CLOSED:
            return
        self._state = STATE_CLOSED

        def on_close():
            try:
                if self._frame_closing:
                    self._tunnel.write_frame(self._stream_id, FRAME_TYPE_CLOSED, 0, None)
                    self._tunnel.close_stream(self)
                else:
                    self._tunnel.write_frame(self._stream_id, FRAME_TYPE_CLOSING, 0, None)
            except Exception as e:
                get_logger().exception("TcpTunnelStream write closeing frame error:%s", e)
            try:
                self.emit_close(self)
            except Exception as e:
                get_logger().exception("TcpTunnelStream emit close error:%s", e)
            self.remove_all_listeners()
            self._rbuffers.close()
            self._wbuffers.close()
            self._rbuffers = None
            self._wbuffers = None
        self._loop.add_async(on_close)

    def do_end(self):
        if self._state != STATE_STREAMING:
            self._tunnel.write_frame(self._stream_id, FRAME_TYPE_CLOSED, 0, None)
            self._tunnel.close_stream(self)
            return
        if self._wbuffers:
            self._state = STATE_CLOSING
            self._frame_closing = True
        else:
            self._tunnel.write_frame(self._stream_id, FRAME_TYPE_CLOSED, 0, None)
            self._tunnel.close_stream(self)
            self._loop.add_async(self.do_close)

    def do_close(self):
        if self._state == STATE_CLOSED:
            return
        self._state = STATE_CLOSED

        def on_close():
            try:
                self.emit_close(self)
            except Exception as e:
                get_logger().exception("TcpTunnelStream emit close error:%s", e)
            self.remove_all_listeners()
            self._rbuffers.close()
            self._wbuffers.close()
            self._rbuffers = None
            self._wbuffers = None
        self._loop.add_async(on_close)

    def _error(self, error):
        self._loop.add_async(self.emit_error, self, error)
        self._loop.add_async(self.close)
        if self.emit_error == null_emit_callback:
            get_logger().error("TcpTunnelStream %s socket %s error: %s", self, self.socket, error)

    def drain(self):
        if self._state == STATE_STREAMING:
            self._tunnel.write_frame(self._stream_id, FRAME_TYPE_DRAIN, 0, None)

    def regain(self):
        if self._state == STATE_STREAMING:
            self._tunnel.write_frame(self._stream_id, FRAME_TYPE_REGAIN, 0, None)

    def do_on_drain(self):
        if self._state == STATE_STREAMING:
            self._wbuffers.do_drain()

    def do_on_regain(self):
        if self._state == STATE_STREAMING:
            self._wbuffers.do_regain()

    def do_on_frame(self, frame_type, frame_flag, data):
        if not data:
            return
        BaseBuffer.write(self._rbuffers, data)
        if self._rbuffers._len > self._rbuffers._drain_size and not self._rbuffers._full:
            self._rbuffers.do_drain()
        self._loop.add_async(self.emit_data, self, self._rbuffers)

    def do_write_drain(self):
        if self._state not in (STATE_STREAMING, STATE_CLOSING):
            return
        if self._wbuffers:
            try:
                data = BaseBuffer.read(self._wbuffers, FRAME_MAX_SIZE) if len(self._wbuffers) > FRAME_MAX_SIZE else BaseBuffer.read(self._wbuffers)
                self._tunnel.write_frame(self._stream_id, FRAME_TYPE_DATA, 0, data)
                if self._wbuffers._full and self._wbuffers._len < self._wbuffers._regain_size:
                    self._wbuffers.do_regain()
                if not self._wbuffers and self._has_drain_event:
                    self._loop.add_async(self.emit_drain, self)
            except Exception as e:
                self._writing = False
                self._error(e)
        else:
            self._writing = False
            if self._state == STATE_CLOSING:
                self.close()

    def write(self, data):
        if self._state != STATE_STREAMING:
            if self.ignore_write_closed_error:
                return False
            raise SocketClosed()
        if data.__class__ is Buffer:
            BaseBuffer.extend(self._wbuffers, data)
        else:
            BaseBuffer.write(self._wbuffers, data)
        if self._wbuffers._len > self._wbuffers._drain_size and not self._wbuffers._full:
            self._wbuffers.do_drain()
        if not self._writing:
            if not self._wbuffers:
                return True
            data = BaseBuffer.read(self._wbuffers, FRAME_MAX_SIZE) if len(self._wbuffers) > FRAME_MAX_SIZE else BaseBuffer.read(self._wbuffers)
            self._tunnel.write_frame(self._stream_id, FRAME_TYPE_DATA, 0, data)
            self._writing = True
            if self._wbuffers._full and self._wbuffers._len < self._wbuffers._regain_size:
                self._wbuffers.do_regain()
            if not self._wbuffers:
                if self._has_drain_event:
                    self._loop.add_async(self.emit_drain, self)
                return True
        return False


class TcpTunnel(EventEmitter):
    def __init__(self, loop=None, is_server=False):
        super(TcpTunnel, self).__init__()

        self._loop = loop or instance()
        self._is_server = is_server
        self._socket = None
        self._streams = {}
        self._current_id_index = 1 if is_server else 2
        self._send_queue = deque()
        self._send_waiting_drain = False
        self._send_timestamp = 0
        self._recv_length = 2
        self._recv_waiting_length = True
        self._recv_timestamp = 0
        self._ping_timestamp = time.time()
        self._pong_timestamp = time.time()

    @property
    def address(self):
        if self._socket is None:
            return (None, None)
        return self._socket.address

    def update_socket(self, socket):
        self._socket = socket
        self._send_queue.clear()
        self._send_waiting_drain = False
        self._recv_length = 2
        self._recv_waiting_length = True
        socket.on_data(self.on_data)
        socket.on_close(self.on_close)
        socket.on_drain(self.on_drain)
        self._ping_timestamp = time.time()
        self._pong_timestamp = time.time()
        def do_ping_timeout():
            if self._socket is not socket:
                return
            now = time.time()
            if now - self._recv_timestamp <= 45:
                self._ping_timestamp = self._send_timestamp
                self._pong_timestamp = self._recv_timestamp
                self._loop.add_timeout(20 if self._is_server else 15, do_ping_timeout)
                return
            if now - self._pong_timestamp > 120:
                socket.close()
                return
            try:
                if now - self._ping_timestamp >= 45:
                    self.write_frame(0, FRAME_TYPE_PING, 0, None)
                    self._ping_timestamp = now
            finally:
                self._loop.add_timeout(20 if self._is_server else 15, do_ping_timeout)
        self._loop.add_timeout(20 if self._is_server else 15, do_ping_timeout)

    def open_stream(self):
        if self._socket is None:
            raise ConnectError(None, None)
        while True:
            stream_id = self._current_id_index
            self._current_id_index += 2
            if self._current_id_index > 0xffff:
                self._current_id_index = 1 if self._is_server else 2
            if stream_id not in self._streams:
                break
        stream = TunnelStream(self._loop, stream_id, self)
        self._streams[stream_id] = stream
        self.write_frame(stream_id, FRAME_TYPE_OPEN, 0, None)
        return stream

    def close_stream(self, stream):
        self._streams.pop(stream._stream_id, None)

    def write_frame(self, stream_id, frame_type, frame_flag, data):
        if self._socket is None:
            raise SocketClosed()
        if self._send_waiting_drain:
            self._send_queue.append((stream_id, frame_type, frame_flag, data))
        else:
            if data is not None:
                self._socket.write(struct.pack(">HHBB", len(data) + 4, stream_id, frame_type, frame_flag) + data)
            else:
                self._socket.write(struct.pack(">HHBB", 4, stream_id, frame_type, frame_flag))
            self._send_waiting_drain = True
            self._send_timestamp = time.time()
            if frame_type == FRAME_TYPE_DATA:
                stream = self._streams.get(stream_id)
                if stream is not None:
                    self._loop.add_async(stream.do_write_drain)

    def on_data(self, socket, buffer):
        while len(buffer) >= self._recv_length:
            if self._recv_waiting_length:
                self._recv_waiting_length = False
                self._recv_length, = struct.unpack(">H", buffer.read(2))
            else:
                stream_id, frame_type, frame_flag = struct.unpack(">HBB", buffer.read(4))
                data = buffer.read(self._recv_length - 4) if self._recv_length > 4 else None
                self._recv_length = 2
                self._recv_waiting_length = True
                if stream_id == 0:
                    self.on_system_frame(frame_type, frame_flag, data)
                else:
                    self.on_frame(stream_id, frame_type, frame_flag, data)
                self._recv_timestamp = time.time()

    def on_frame(self, stream_id, frame_type, frame_flag, data):
        if stream_id not in self._streams:
            if self._socket is None:
                return
            if frame_type == FRAME_TYPE_OPEN:
                stream = TunnelStream(self._loop, stream_id, self)
                self._streams[stream_id] = stream
                self._loop.add_async(self.emit_stream, self, stream)
            elif frame_type != FRAME_TYPE_RESET:
                self.write_frame(stream_id, FRAME_TYPE_RESET, 0, None)
            return
        stream = self._streams[stream_id]
        if frame_type == FRAME_TYPE_DATA:
            stream.do_on_frame(frame_type, frame_flag, data)
            return
        if frame_type == FRAME_TYPE_DRAIN:
            stream.do_on_drain()
            return
        if frame_type == FRAME_TYPE_REGAIN:
            stream.do_on_regain()
            return
        if frame_type == FRAME_TYPE_RESET or frame_type == FRAME_TYPE_CLOSED:
            stream.do_close()
            self.close_stream(stream)
            return
        if frame_type == FRAME_TYPE_CLOSING:
            stream.do_end()
            return
        if frame_type == FRAME_TYPE_OPEN:
            self.write_frame(stream_id, FRAME_TYPE_RESET, 0, None)
            stream.do_close()
            self.close_stream(stream)

    def on_system_frame(self, frame_type, frame_flag, data):
        if frame_type == FRAME_TYPE_PING:
            self.write_frame(0, FRAME_TYPE_PONG, 0, None)
            self._pong_timestamp = time.time()
            return

    def on_drain(self, socket):
        if not self._send_queue:
            self._send_waiting_drain = False
            return
        try:
            stream_id, frame_type, frame_flag, data = self._send_queue.popleft()
            if data is not None:
                self._socket.write(struct.pack(">HHBB", len(data) + 4, stream_id, frame_type, frame_flag) + data)
            else:
                self._socket.write(struct.pack(">HHBB", 4, stream_id, frame_type, frame_flag))
            self._send_timestamp = time.time()
            if frame_type == FRAME_TYPE_DATA:
                stream = self._streams.get(stream_id)
                if stream is not None:
                    self._loop.add_async(stream.do_write_drain)
        except Exception as e:
            self._send_waiting_drain = False
            if self._socket is not None:
                self._socket._error(e)

    def on_close(self, socket):
        self._socket = None
        streams = list(self._streams.values())
        self._streams.clear()
        self._send_queue.clear()
        self._send_waiting_drain = False
        self._recv_length = 2
        self._recv_waiting_length = True
        for stream in streams:
            stream.do_close()

def gen_sign_key(key):
    t = struct.pack("I", int(time.time()))
    oncestr = os.urandom(16)
    return t + oncestr + hashlib.md5(t + oncestr + sevent.utils.ensure_bytes(key)).digest()

def check_sign_key(key, sign_key):
    return sign_key[20:] == hashlib.md5(sign_key[:20] + sevent.utils.ensure_bytes(key)).digest()

async def handler_server_conn(tunnel, conn, key):
    def on_timeout():
        if conn is None or tunnel._socket is conn:
            return
        conn.close()
    timeout_handler = sevent.current().add_timeout(15, on_timeout)

    start_time = time.time()
    try:
        logging.info("remote conn connecting %s:%d", conn.address[0], conn.address[1])
        data_length, = struct.unpack(">H", (await conn.recv(2)).read(2))
        data = (await conn.recv(data_length)).read(data_length)
        _, frame_type, frame_flag, sign_key_length = struct.unpack(">HBBH", data[:6])
        sign_key = data[6:6 + sign_key_length]
        if not check_sign_key(key, sign_key):
            await conn.send(struct.pack(">HHBBB", 5, 0, FRAME_TYPE_AUTHED, 0, 1))
            await conn.closeof()
            sevent.current().cancel_timeout(timeout_handler)
            logging.info("remote conn auth fail %s:%d %s", conn.address[0], conn.address[1], sign_key)
            return

        if tunnel._socket is not None:
            await conn.send(struct.pack(">HHBBB", 5, 0, FRAME_TYPE_AUTHED, 0, 2))
            await conn.closeof()
            sevent.current().cancel_timeout(timeout_handler)
            logging.info("remote conn auth fail %s:%d %s", conn.address[0], conn.address[1], sign_key)
            return

        await conn.send(struct.pack(">HHBBB", 5, 0, FRAME_TYPE_AUTHED, 0, 0))
        sevent.current().cancel_timeout(timeout_handler)
        tunnel.update_socket(conn)
        logging.info("remote conn succeed %s:%d", conn.address[0], conn.address[1])
        await conn.join()
    except sevent.errors.SocketClosed:
        pass
    except Exception as e:
        logging.info("remote conn error %s:%d %s %.2fms\r%s", conn.address[0], conn.address[1],
                     e, (time.time() - start_time) * 1000, traceback.format_exc())
        return
    finally:
        if conn is not None:
            conn.close()
    logging.info("remote conn closed %s:%d %.2fms", conn.address[0], conn.address[1], (time.time() - start_time) * 1000)

async def run_tunnel_server(tunnel, listen_host, listen_port, key):
    server = create_server((listen_host, listen_port))
    while True:
        conn = await server.accept()
        sevent.current().call_async(handler_server_conn, tunnel, conn, key)

async def run_tunnel_client(tunnel, connect_host, connect_port, key):
    while True:
        start_time = time.time()
        is_connected, conn = False, None
        try:
            logging.info("local conn connecting -> %s:%d", connect_host, connect_port)
            conn = create_socket((connect_host, connect_port))
            await conn.connectof((connect_host, connect_port))

            def on_timeout():
                if conn is None or tunnel._socket is conn:
                    return
                conn.close()
            timeout_handler = sevent.current().add_timeout(15, on_timeout)

            sign_key = gen_sign_key(key)
            await conn.send(struct.pack("!HHBBH", 6 + len(sign_key), 0, FRAME_TYPE_AUTH, 0, len(sign_key)) + sign_key)
            data_length, = struct.unpack(">H", (await conn.recv(2)).read(2))
            data = (await conn.recv(data_length)).read(data_length)
            _, frame_type, frame_flag, connect_result = struct.unpack(">HBBB", data[:5])
            if connect_result != 0:
                await conn.closeof()
                sevent.current().cancel_timeout(timeout_handler)
                logging.info("local conn fail -> %s:%d %d", connect_host, connect_port, connect_result)
            else:
                sevent.current().cancel_timeout(timeout_handler)
                tunnel.update_socket(conn)
                is_connected = True
                logging.info("local conn succeed -> %s:%d", connect_host, connect_port)
                await conn.join()
        except sevent.errors.SocketClosed:
            logging.info("local conn closed -> %s:%d %.2fms", connect_host, connect_port, (time.time() - start_time) * 1000)
        except Exception as e:
            logging.info("local conn error -> %s:%d %s %.2fms\r%s", connect_host, connect_port,
                         e, (time.time() - start_time) * 1000, traceback.format_exc())
        else:
            logging.info("local conn closed -> %s:%d %.2fms", connect_host, connect_port, (time.time() - start_time) * 1000)
        finally:
            if conn is not None:
                conn.close()
        await sevent.sleep(3 if not is_connected else 0.1)

async def handle_proxy_local(conns, tunnel, conn, status):
    start_time = time.time()
    conn.write, pconn = warp_write(conn, status, "recv_len"), None
    try:
        pconn = tunnel.open_stream()
        pconn.write = warp_write(pconn, status, "send_len")
        logging.info("connected %s:%s -> %s:%s -> %s:%s", conn.address[0], conn.address[1], tunnel.address[0], tunnel.address[1],
                     pconn.address[0], pconn.address[1])
        await conn.linkof(pconn)
    except sevent.errors.SocketClosed:
        pass
    except Exception as e:
        logging.info("error %s:%s -> %s:%s -> %s:%s %s %.2fms\r%s", conn.address[0], conn.address[1],
                     tunnel.address[0], tunnel.address[1], pconn.address[0] if pconn else "", pconn.address[1] if pconn else 0,
                     e, (time.time() - start_time) * 1000, traceback.format_exc())
        return
    finally:
        conn.close()
        if pconn: pconn.close()
        conns.pop(id(conn), None)

    logging.info("closed %s:%s -> %s:%s -> %s:%s %s %s %.2fms", conn.address[0], conn.address[1],
                 tunnel.address[0], tunnel.address[1], pconn.address[0], pconn.address[1], format_data_len(status["send_len"]),
                 format_data_len(status["recv_len"]), (time.time() - start_time) * 1000)

async def run_proxy_local_server(conns, tunnel, bind_host, bind_port):
    server = create_server((bind_host, bind_port))
    while True:
        conn = await server.accept()
        status = {"recv_len": 0, "send_len": 0, "last_time": time.time(), "check_recv_len": 0, "check_send_len": 0}
        sevent.current().call_async(handle_proxy_local, conns, tunnel, conn, status)
        conns[id(conn)] = (conn, status)

def handle_proxy_remote_stream(conns, tunnel, stream, allow_host_names, deny_host_names):
    status = {"recv_len": 0, "send_len": 0, "last_time": time.time(), "check_recv_len": 0, "check_send_len": 0}
    sevent.current().call_async(tcp_proxy, conns, tunnel, stream, status, allow_host_names, deny_host_names)
    conns[id(stream)] = (stream, status)

def main(argv):
    parser = argparse.ArgumentParser(description='Simple bidirectional http and socks5 proxy based on TCP tunnel')
    parser.add_argument('-c', dest='is_client_mode', nargs='?', const=True, default=False, type=bool, help='is client mode (defualt: False)')
    parser.add_argument('-k', dest='key', default='', type=str, help='auth key (defualt: "")')
    parser.add_argument('-b', dest='bind', default="0.0.0.0", help='proxy bind host (default: 0.0.0.0)')
    parser.add_argument('-p', dest='port', default=8088, type=int, help='proxy bind port (default: 8088)')
    parser.add_argument('-r', dest='listen_host', default="0.0.0.0", help='server mode reverse server listen host (default: 0.0.0.0)')
    parser.add_argument('-l', dest='listen_port', default=8078, type=int, help='server mode reverse server listen port (default: 8088)')
    parser.add_argument('-H', dest='connect_host', default="127.0.0.1", help='client mode reverse client connect server host (default: 127.0.0.1)')
    parser.add_argument('-P', dest='connect_port', default=8078, type=int, help='client mode reverse client connect server port (default: 8088)')
    parser.add_argument('-t', dest='timeout', default=7200, type=int, help='no read/write timeout (default: 7200)')
    parser.add_argument('-a', dest='allow_hosts', default=[], action="append", type=str,help='allow forward host name')
    parser.add_argument('-d', dest='deny_hosts', default=[], action="append", type=str, help='deny forward host name')
    parser.add_argument('-A', dest='allow_filename', default="", type=str, help='allow forward host name config filename')
    parser.add_argument('-D', dest='deny_filename', default="", type=str, help='deny forward host name config filename')
    args = parser.parse_args(args=argv)
    config_signal()

    allow_host_names, deny_host_names = [], []
    if args.allow_hosts:
        for allow_host in args.allow_hosts:
            allow_host_names.extend([parse_allow_deny_host(h) for h in allow_host.split(",") if h.strip()])
    if args.deny_hosts:
        for deny_host in args.deny_hosts:
            deny_host_names.extend([parse_allow_deny_host(h) for h in deny_host.split(",") if h.strip()])
    if args.allow_filename:
        allow_host_names.extend(parse_allow_deny_host_filename(args.allow_filename))
    if args.deny_filename:
        deny_host_names.extend(parse_allow_deny_host_filename(args.deny_filename))

    conns = {}
    tunnel = TcpTunnel(is_server=not args.is_client_mode)
    if args.is_client_mode:
        sevent.current().call_async(run_tunnel_client, tunnel, args.connect_host, args.connect_port, args.key)
        logging.info("listen server at %s:%d -> %s:%s", args.bind, args.port, args.connect_host, args.connect_port)
    else:
        sevent.current().call_async(run_tunnel_server, tunnel, args.listen_host, args.listen_port, args.key)
        logging.info("listen server at %s:%d -> %s:%s", args.bind, args.port, args.listen_host, args.listen_port)
    tunnel.on("stream", lambda _, stream: handle_proxy_remote_stream(conns, tunnel, stream,
                                                                     allow_host_names or None, deny_host_names or None))

    sevent.current().call_async(run_proxy_local_server, conns, tunnel, args.bind, args.port)
    sevent.current().call_async(check_timeout, conns, args.timeout)

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)1.1s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S', filemode='a+')
    try:
        main(sys.argv[1:])
        sevent.instance().start()
    except KeyboardInterrupt:
        exit(0)