from select import epoll, EPOLLIN, EPOLLERR, EPOLLHUP
from socket import AF_INET, SOCK_STREAM, create_connection, socket

def parse_address(address):
    split = address.split(":")
    host = "".join([s for s in split if s != split[-1]])
    port = int(split[-1])
    return (host, port)

class Dispatcher():

    def __init__(self, port, verbose=False):
        print("Setting up the Dispatcher")
        self.port = port
        self.verbose = verbose
        self.listener = socket(AF_INET,  SOCK_STREAM)
        self.listener.bind(("", self.port))
        self.listener.listen(5)
        
        self.connections = {}
        
        self.epoll = epoll()
        self.epoll.register(self.listener.fileno(), EPOLLIN | EPOLLERR)

        print("Listening on port %d" % port)

    def accept(self):
        c,_ = self.listener.accept()
        self.connections[c.fileno()] = c
        self.epoll.register(c.fileno(), EPOLLIN | EPOLLERR | EPOLLHUP)
        print("Accepted connection on FD %d" % c.fileno())
        return c.fileno()
    
    def connect(self, host):
        c = create_connection(parse_address(host))
        self.connections[c.fileno()] = c
        self.epoll.register(c.fileno(), EPOLLIN | EPOLLERR | EPOLLHUP)
        return c.fileno()

    def disconnect(self, fd):
        if fd not in self.connections.keys():
            return
        self.epoll.unregister(fd)
        del self.connections[fd]

    def kill(self):
        for c in self.connections.values():
            c.close()
        self.listener.close()
        self.connnections = {}

    def message(self, fd, msg):
        if fd not in self.connections.keys():
            print("Attempted to send message to bad FD %d." % fd)
            return
        self.connections[fd].sendall(bytearray(msg, "utf-8"))

    def poll(self, timeout=0.0010, buffer_size=256):
        events = self.epoll.poll(timeout)
        ret = []
        for fd, event in events:
            if fd == self.listener.fileno():
                if event == EPOLLIN:
                    nfd = self.accept()
                    ret.append((nfd, "CONNECTED"))
                    continue
                else:
                    print("Something bad happened to the listening socket. BRB dying.")
                    self.kill()
                    return
            if self.connections[fd] == None:
                print("Received event on invalid FD %d. BRB dying." % fd)
                self.kill()
                return
            else:
                if event == EPOLLIN:
                    c = self.connections[fd]
                    msg = c.recv(buffer_size)
                    if not msg:
                        print("FD %d disconnected." % fd)
                        self.disconnect(fd)
                        ret.append((fd, "DISCONNECTED"))
                        continue
                    if self.verbose:
                        print("Received message from FD %d: %s" % (fd, msg))
                    try:
                        clean = msg.decode("utf-8")
                        ret.append((fd, clean))
                    except UnicodeDecodeError:
                        print("Got a strange value from FD %d, so it's getting disconnected." % fd)
                        self.disconnect(fd)
                        ret.append((fd, "DISCONNECTED"))
                    continue
                if event == EPOLLERR:
                    print("Encountered an error on FD %d. Disconnecting it." % fd)
                    self.disconnect(fd)
                    ret.append((fd, "DISCONNECTED"))
                    continue
                if event == EPOLLHUP:
                    print("FD %d hung-up on us. Disconnecting it." % fd)
                    self.disconnect(fd)
                    ret.append((fd, "DISCONNECTED"))
                    continue
        return ret

if __name__ is "__main__":
    print("A rooster ought not be running this. Try 'server.py' instead.")
