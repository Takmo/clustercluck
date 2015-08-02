from select import epoll, EPOLLIN, EPOLLERR, EPOLLHUP
import socket

class Dispatcher():

    def __init__(self, port, verbose=False):
        print("Setting up the Dispatcher")
        self.port = port
        self.verbose = verbose
        self.listener = socket.socket(socket.AF_INET,  socket.SOCK_STREAM)
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
    
    def broadcast(self, msg_json):
        if self.verbose:
            print("Broadcasting message: %s" % msg_json)
        for c in self.connections.values():
            c.sendall(msg_json)

    def disconnect(self, fd):
        self.epoll.unregister(fd)
        del self.connections[fd]

    def kill(self):
        for _,c in self.connections:
            c.close()
        self.listener.close()
        self.connnections = {}

    def poll(self, timeout=0.0010, buffer_size=256):
        events = self.epoll.poll(timeout)
        for fd, event in events:
            if fd == self.listener.fileno():
                if event == EPOLLIN:
                    self.accept()
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
                    msg_json = c.recv(buffer_size)
                    if not msg_json:
                        print("FD %d disconnected." % fd)
                        self.disconnect(fd)
                        continue
                    print("Received message from FD %d: %s" % (fd, msg_json))
                    self.broadcast(msg_json)
                    # whatever logic here
                    continue
                if event == EPOLLERR:
                    print("Encountered an error on FD %d. Disconnecting it." % fd)
                    self.disconnect(fd)
                    continue
                if event == EPOLLHUP:
                    print("FD %d hung-up on us. Disconnecting it." % fd)
                    self.disconnect(fd)
                    continue

if __name__ is "__main__":
    print("A rooster ought not be running this. Try 'clustercluck' instead.")
