# sevent

The highest performance event loop.

# Examples

 ### Simple Http Client
 
```python
import sevent

def on_data(s, data):
    print(data.read())

s = sevent.tcp.Socket()
s.on_data(on_data)
s.on_close(lambda s: sevent.current().stop())
s.connect(('www.google.com', 80))
s.write('GET / HTTP/1.0\r\nHost: www.google.com\r\nConnection: Close\r\n\r\n')

sevent.current().start()
```

### Simple TCP Port Forward

```python
import sys
from sevent import tcp
from sevent import current

def on_connection(server, conn):
    pconn = tcp.Socket()
    conn.link(pconn)
    pconn.connect((sys.argv[2], int(sys.argv[3])))

server = tcp.Server()
server.on_connection(on_connection)
server.listen(("0.0.0.0", int(sys.argv[1])))
current().start()
```

# License

sevent uses the MIT license, see LICENSE file for the details.