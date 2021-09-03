# sevent

The highest performance event loop.

# Examples

 ### Simple Http Client
 
```python
import sevent

def on_data(s, data):
    print(data.decode("utf-8"))

s = sevent.tcp.Socket()
s.on_data(on_data)
s.on_close(lambda s: sevent.current().stop())
s.connect(('www.google.com', 80))
s.write(b'GET / HTTP/1.1\r\nHost: www.google.com\r\nConnection: Close\r\nUser-Agent: curl/7.58.0\r\nAccept: */*\r\n\r\n')

sevent.instance().start()
```

```python
import sevent

async def http_test():
    s = sevent.tcp.Socket()
    await s.connectof(('www.google.com', 80))
    await s.send(b'GET / HTTP/1.1\r\nHost: www.google.com\r\nConnection: Close\r\nUser-Agent: curl/7.58.0\r\nAccept: */*\r\n\r\n')

    data = b''
    while True:
        try:
            data += (await s.recv()).read()
        except sevent.tcp.SocketClosed:
            break
    print(data.decode("utf-8"))
    await s.closeof()

sevent.run(http_test)
```

### Simple TCP Port Forward

```python
import sys
import sevent

def on_connection(server, conn):
    pconn = sevent.tcp.Socket()
    pconn.connect((sys.argv[2], int(sys.argv[3])))
    conn.link(pconn)

server = sevent.tcp.Server()
server.on_connection(on_connection)
server.listen(("0.0.0.0", int(sys.argv[1])))
sevent.instance().start()
```

```python
import sys
import sevent

async def tcp_port_forward_server():
    server = sevent.tcp.Server()
    server.listen(("0.0.0.0", int(sys.argv[1])))

    while True:
        conn = await server.accept()
        pconn = sevent.tcp.Socket()
        pconn.connect((sys.argv[2], int(sys.argv[3])))
        conn.link(pconn)

sevent.run(tcp_port_forward_server)
```

# License

sevent uses the MIT license, see LICENSE file for the details.