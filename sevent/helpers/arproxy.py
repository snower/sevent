# -*- coding: utf-8 -*-
# 2020/7/10
# create by: snower

import re
from sevent.helpers.tcp2proxy import *

def do_rewrite_host(forward_host, forward_port, rewrite_hosts):
    def parse_rewrite_host(host_args, rewrite_host, rewrite_args):
        for arg in rewrite_args:
            if arg < 0 or arg >= len(host_args):
                continue
            rewrite_host = rewrite_host.replace("{" + str(arg) + "}", host_args[arg])
        rewrite_host = rewrite_host.split(":")
        if len(rewrite_host) == 2:
            return rewrite_host[0], int(rewrite_host[1])
        return rewrite_host[0], forward_port

    for host, (rewrite_host, rewrite_args) in rewrite_hosts:
        if isinstance(host, str):
            if host == "*":
                return parse_rewrite_host(["%s:%s" % (forward_host, forward_port)], rewrite_host, rewrite_args)
            if forward_port in (80, 443):
                if forward_host == host:
                    return parse_rewrite_host([host], rewrite_host, rewrite_args)
            if ("%s:%s" % (forward_host, forward_port)) == host:
                return parse_rewrite_host([host], rewrite_host, rewrite_args)
        else:
            if forward_port in (80, 443):
                matched = host.match(forward_host)
                if matched:
                    return parse_rewrite_host(matched.groups(), rewrite_host, rewrite_args)
            matched = host.match("%s:%s" % (forward_host, forward_port))
            if matched:
                return parse_rewrite_host(matched.groups(), rewrite_host, rewrite_args)
    return forward_host, forward_port

def check_allow_host(forward_host, forward_port, allow_hosts):
    for host in allow_hosts:
        if isinstance(host, str):
            if host == "*":
                return True
            if forward_port in (80, 443):
                if forward_host == host:
                    return True
            if ("%s:%s" % (forward_host, forward_port)) == host:
                return True
        else:
            if forward_port in (80, 443):
                if host.match(forward_host):
                    return True
            if host.match("%s:%s" % (forward_host, forward_port)):
                return True
    return False

def check_noproxy_host(forward_host, forward_port, noproxy_hosts):
    for host in noproxy_hosts:
        if isinstance(host, str):
            if host == "*":
                return True
            if forward_port in (80, 443):
                if forward_host == host:
                    return True
            if ("%s:%s" % (forward_host, forward_port)) == host:
                return True
        else:
            if forward_port in (80, 443):
                if host.match(forward_host):
                    return True
            if host.match("%s:%s" % (forward_host, forward_port)):
                return True
    return False

async def parse_http_forward(conn, rbuffer):
    while True:
        if b'\r\n\r\n' in rbuffer.join():
            break
        rbuffer.write((await conn.recv()).read())

    data = rbuffer.join()
    data = data[:data.index(b'\r\n\r\n')]
    for header in data.split(b'\r\n'):
        try:
            index = header.index(b':')
        except ValueError:
            continue
        if header[:index].decode("utf-8").strip().lower() != "host":
            continue
        host = header[index + 1:].decode("utf-8").strip().split(":")
        if len(host) != 2:
            return header[index + 1:].decode("utf-8").strip(), 80
        return host[0], int(host[1])
    return "", 0

async def parse_tls_forward(conn, rbuffer):
    head_data = (await conn.recv(43)).read(43)
    rbuffer.write(head_data)

    # session_id
    data = (await conn.recv(1)).read(1)
    rbuffer.write(data)
    if ord(data) > 0:
        data = (await conn.recv(ord(data))).read(ord(data))
        rbuffer.write(data)

    # cipher suites
    data = (await conn.recv(2)).read(2)
    rbuffer.write(data)
    csl, = struct.unpack("!H", data)
    if csl > 0:
        data = (await conn.recv(csl)).read(csl)
        rbuffer.write(data)

    # compression methods
    data = (await conn.recv(1)).read(1)
    rbuffer.write(data)
    if ord(data) > 0:
        data = (await conn.recv(ord(data))).read(ord(data))
        rbuffer.write(data)

    # estensions
    data = (await conn.recv(2)).read(2)
    rbuffer.write(data)
    el, = struct.unpack("!H", data)
    if el > 0:
        data = (await conn.recv(el)).read(el)
        rbuffer.write(data)

        i = 0
        while i < len(data):
            et_item, el_item = struct.unpack("!HH", data[i: i + 4])
            if et_item != 0:
                i += el_item + 4
                continue

            sl, = struct.unpack("!H", data[i+4: i+6])
            j, sdata = 0, data[i+6:i+6+sl]
            while j < len(sdata):
                st_item, sl_item = struct.unpack("!BH", sdata[j: j + 3])
                if st_item != 0:
                    j += sl_item + 3
                    continue

                host = sdata[j + 3: j + 3 + sl_item].decode("utf-8").split(":")
                if len(host) != 2:
                    return sdata[j + 3: j + 3 + sl_item].decode("utf-8"), 443
                return host[0], int(host[1])
    return "", 0

async def none_proxy(conns, conn, proxy_host, proxy_port, remote_host, remote_port, status):
    start_time = time.time()
    conn.write, pconn = warp_write(conn, status, "recv_len"), None

    try:
        conn.enable_nodelay()
        pconn = create_socket((remote_host, remote_port))
        await pconn.connectof((remote_host, remote_port))
        pconn.write = warp_write(pconn, status, "send_len")
        logging.info("none proxy connected %s:%d -> %s:%d", conn.address[0], conn.address[1], remote_host, remote_port)
        await pconn.linkof(conn)
    except sevent.errors.SocketClosed:
        pass
    except Exception as e:
        logging.info("none proxy error %s:%d -> %s:%d %s %.2fms\r%s", conn.address[0], conn.address[1], remote_host, remote_port,
                     e, (time.time() - start_time) * 1000, traceback.format_exc())
        return
    finally:
        conn.close()
        if pconn: pconn.close()
        conns.pop(id(conn), None)

    logging.info("none proxy closed %s:%d -> %s:%d %s %s %.2fms", conn.address[0], conn.address[1], remote_host, remote_port,
                 format_data_len(status["send_len"]), format_data_len(status["recv_len"]), (time.time() - start_time) * 1000)

async def parse_forward(proxy_type, conns, conn, proxy_host, proxy_port,
                        default_forward_host, default_forward_port, default_forward_proxy_type,
                        allow_hosts, noproxy_hosts, rewrite_hosts, status):
    try:
        rbuffer = sevent.Buffer()
        timer = sevent.current().add_timeout(5, lambda: conn.close())
        try:
            data = (await conn.recv()).read()
            rbuffer.write(data)

            if len(data) < 11 or data[:2] != b'\x16\x03' or data[2] not in (1, 2, 3, 4) or data[5] != 0x01 \
                    or data[9] != 0x03 or data[10] not in (1, 2, 3, 4):
                if data[:data.index(b' ')].decode("utf-8").lower() in ("get", "post", "put", "delete", "head", "option") \
                        or b'HTTP' in data:
                    forward_host, forward_port = await parse_http_forward(conn, rbuffer)
                else:
                    forward_host, forward_port = "", 0
            else:
                rbuffer.write(conn.buffer[0].read())
                conn.buffer[0].write(rbuffer.read())
                forward_host, forward_port = await parse_tls_forward(conn, rbuffer)
        except Exception:
            forward_host, forward_port = "", 0
        finally:
            sevent.current().cancel_timeout(timer)

        if not forward_host or not forward_port:
            forward_host, forward_port, proxy_type = default_forward_host, default_forward_port, default_forward_proxy_type
        elif allow_hosts and allow_hosts[0] != "*" and not check_allow_host(forward_host, forward_port, allow_hosts):
            forward_host, forward_port, proxy_type = default_forward_host, default_forward_port, default_forward_proxy_type
        if rewrite_hosts:
            forward_host, forward_port = do_rewrite_host(forward_host, forward_port, rewrite_hosts)
        if not forward_host or not forward_port:
            logging.info("%s proxy unknown closed %s:%d -> %s:%d", proxy_type, conn.address[0], conn.address[1],
                         proxy_host, proxy_port)
            conn.close()
            conns.pop(id(conn), None)
            return

        rbuffer.write(conn.buffer[0].read())
        conn.buffer[0].write(rbuffer.read())
        if proxy_type == "http":
            if check_noproxy_host(forward_host, forward_port, noproxy_hosts):
                proxy_type = "none"
                await none_proxy(conns, conn, proxy_host, proxy_port, forward_host, forward_port, status)
            else:
                await http_proxy(conns, conn, proxy_host, proxy_port, forward_host, forward_port, status)
        elif proxy_type == "socks5":
            if check_noproxy_host(forward_host, forward_port, noproxy_hosts):
                proxy_type = "none"
                await none_proxy(conns, conn, proxy_host, proxy_port, forward_host, forward_port, status)
            else:
                await socks5_proxy(conns, conn, proxy_host, proxy_port, forward_host, forward_port, status)
        else:
            await none_proxy(conns, conn, proxy_host, proxy_port, forward_host, forward_port, status)
    except Exception as e:
        logging.info("%s proxy error %s:%d -> %s:%d %s\r%s", proxy_type, conn.address[0], conn.address[1],
                     proxy_host, proxy_port, e, traceback.format_exc())
        conns.pop(id(conn), None)

async def tcp_accept(server, args):
    proxy_info = args.proxy_host.split(":")
    if len(proxy_info) == 1:
        if not proxy_info[0].isdigit():
            proxy_host, proxy_port = proxy_info[0], 8088
        else:
            proxy_host, proxy_port = "127.0.0.1", int(proxy_info[0])
    else:
        proxy_host, proxy_port = proxy_info[0], int(proxy_info[1])

    default_forward_info = args.default_forward_host.split(":")
    if not default_forward_info or not default_forward_info[0]:
        default_forward_host, default_forward_port = "", 0
    elif len(default_forward_info) == 1:
        if not default_forward_info[0].isdigit():
            default_forward_host, default_forward_port = default_forward_info[0], 8088
        else:
            default_forward_host, default_forward_port = "127.0.0.1", int(default_forward_info[0])
    else:
        default_forward_host, default_forward_port = default_forward_info[0], int(default_forward_info[1])
    allow_hosts = []
    for allow_host in (i for i in args.allow_hosts.split(",") if i.strip()):
        if "*" in allow_host and "*" != allow_host:
            allow_hosts.append(re.compile(allow_host.replace(".", "\.").replace("*", ".+?")))
        else:
            allow_hosts.append(allow_host)
    noproxy_hosts = []
    for noproxy_host in (i for i in args.noproxy_hosts.split(",") if i.strip()):
        if "*" in noproxy_host and "*" != noproxy_host:
            noproxy_hosts.append(re.compile(noproxy_host.replace(".", "\.").replace("*", ".+?")))
        else:
            noproxy_hosts.append(noproxy_host)
    rewrite_hosts = []
    for rewrite_host in (i for i in args.rewrite_hosts.split(",") if i.strip()):
        rewrite_host = [i for i in rewrite_host.split("=") if i.strip()]
        if len(rewrite_host) != 2:
            continue
        rewrite_hosts.append((
            re.compile("^" + rewrite_host[0].replace(".", "\.").replace("*", "(.+?)") + "$") if "*" in rewrite_host[0] and "*" != rewrite_host[0] else rewrite_host[0],
            (
                rewrite_host[1],
                tuple((int(i[1:-1]) for i in re.findall("(\{\d+?\})", rewrite_host[1])))
            )
        ))

    logging.info("use %s proxy %s:%d default forward to %s:%d", args.proxy_type, proxy_host, proxy_port,
                 default_forward_host, default_forward_port)
    conns = {}
    sevent.current().call_async(check_timeout, conns, args.timeout)
    while True:
        conn = await server.accept()
        status = {"recv_len": 0, "send_len": 0, "last_time": time.time(), "check_recv_len": 0, "check_send_len": 0}
        sevent.current().call_async(parse_forward, args.proxy_type, conns, conn, proxy_host, proxy_port,
                                    default_forward_host, default_forward_port, args.default_forward_proxy_type,
                                    allow_hosts, noproxy_hosts, rewrite_hosts, status)
        conns[id(conn)] = (conn, status)

def main(argv):
    parser = argparse.ArgumentParser(description='auto parse servername (support http, TLS) forword to remote host from none, http or socks5 proxy')
    parser.add_argument('-b', dest='bind', default="0.0.0.0", help='local bind host (default: 0.0.0.0)')
    parser.add_argument('-p', dest='port', default=8088, type=int, help='local bind port (default: 8088)')
    parser.add_argument('-t', dest='timeout', default=7200, type=int, help='no read/write timeout (default: 7200)')
    parser.add_argument('-T', dest='proxy_type', default="http", choices=("none", "http", "socks5"),
                        help='proxy type (default: http)')
    parser.add_argument('-P', dest='proxy_host', default="127.0.0.1:8088",
                        help='proxy host, accept format [proxy_host:proxy_port] (default: 127.0.0.1:8088)')
    parser.add_argument('-f', dest='default_forward_host', default="",
                        help='default remote forward host , accept format [remote_host:remote_port] (default: )')
    parser.add_argument('-x', dest='default_forward_proxy_type', default="http", choices=("none", "http", "socks5"),
                        help='default remote forward  proxy type (default: http)')
    parser.add_argument('-H', dest='allow_hosts', default="*",
                        help='allow hosts, accept format [host,*host,host*] (default: *)')
    parser.add_argument('-N', dest='noproxy_hosts', default="",
                        help='noproxy hosts, accept format [host,*host,host*] (default: )')
    parser.add_argument('-R', dest='rewrite_hosts', default="",
                        help='rewrite hosts, accept format [host=rhost,*host={1}rhost,host*=rhost{1}] (default: )')
    args = parser.parse_args(args=argv)
    config_signal()
    server = create_server((args.bind, args.port))
    logging.info("listen server at %s:%d", args.bind, args.port)
    sevent.current().call_async(tcp_accept, server, args)

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)1.1s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S', filemode='a+')
    try:
        main(sys.argv[1:])
        sevent.instance().start()
    except KeyboardInterrupt:
        exit(0)