from dispatcher import Dispatcher
from time import time, sleep

import json

class Hen:

    def __init__(self, host, dispatcher):
        # this is the address that others can connect to
        self.host = host

        # the dispatcher we use to send messages
        self.dispatcher = dispatcher

        # stores the information for the leader
        self.leaderfd = -1 # I AM LEADER NAO
        self.leaderhost = self.host

        # the current term; important for RAFT
        self.term = 0
        self.latest_id = 0
        self.last_heartbeat = time()

        # for followers, this is a map from host address to FD
        # for leaders, map from host address to dict containing
        # the "fd" and "unacked" messages, which is a dict
        # mapping message number to JSON msg for resending
        # should always include self
        self.nodes = {}

    #
    # Leader Methods
    #

    def broadcast(self, msg):
        # set the term and ID, but assume message is otherwise prepared
        msg["term"] = self.term
        msg["id"] = self.latest_id
        msg["host"] = self.host
        message = json.dumps(msg)
        # log in unacknowledged messages
        for node in self.nodes:
            if node == self.host:
                # don't broadcast to ourselves
                continue
            self.nodes[node]["unacked"][self.latest_id] = message
            # but don't send yet - need to send other messages first
        # increment latest ID
        self.latest_id += 1

    def handle_leader(self, fd, msg):
        if msg["message"] == "HATCH":
            self.hatch(fd, msg["host"])
            return
        # everything else is basically just checking acks
        for n in self.nodes:
            node = self.nodes[n]
            if node["fd"] == fd:
                for id in node["unacked"]:
                    if id == msg["id"]:
                        del node["unacked"][id]
                        return
        # man that looks cool ^.^

    def hatch(self, fd, host):
        self.nodes[host] = {"fd": fd, "unacked": {}}
        msg = {"message": "UPDATE", "nodes": self.nodes}
        self.broadcast(msg)

    def pluck(self, fd):
        changed = False
        for n in self.nodes:
            node = self.nodes[n]
            if node["fd"] == fd:
                del self.nodes[n]
                changed = True
                break
        if not changed:
            return
        msg = {"message": "UPDATE", "nodes": self.nodes}
        self.broadcast(msg)

    def send(self):
        pluckfds = []
        for n in self.nodes:
            node = self.nodes[n]
            if len(node["unacked"]) > 10:
                # disconnect if it hasn't acked 10+ messages
                pluckfds.append(node["fd"])
            if len(node["unacked"]) > 0:
                # send the oldest message first
                msg = json.dumps(list(node["unacked"].values())[0])
                self.dispatcher.message(node["fd"], msg)
        for fd in pluckfds:
            self.pluck(fd)
            self.dispatcher.disconnect(fd)

    #
    # Follower Methods
    #

    def campaign(self):
        pass # TODO

    def handle_follower(self, fd, msg):
        if fd == self.leaderfd:
            # a message from our fearless leader!
            if msg["message"] == "CLUCK":
                self.cluck()
            if msg["message"] == "HEARTBEAT":
                pass # we just have to ack
            if msg["message"] == "UPDATE":
                self.update(msg["nodes"])
            # ack the message
            msg["message"] = True
            self.dispatcher.message(self.leaderfd, json.dumps(msg))
        else:
            if msg["message"] == "ELECTME":
                self.vote(fd, msg)
            elif msg["message"] == "LEADER":
                # if msg term is greater than ours, new leader
                if msg["term"] > self.term:
                    self.term = msg["term"]
                    self.leaderhost = msg["host"]
                    self.leaderfd = fd
                    ack = True
                else:
                    ack = False
                msg["message"] = ack
                self.dispatcher.message(fd, json.dumps(msg))
            else:
                error = self.resp(msg["term"], msg["id"], "NOT LEADER")
                error["leader"] = self.leaderhost
                self.dispatcher.message(fd, json.dumps(error))

    def kingme(self):
        pass # TODO become leader

    def update(self, nodes):
        self.nodes = {}
        for node in nodes:
            self.nodes[node] = -1
        # the -1 FD means it's not connected

    def vote(self, fd, msg):
        if msg["term"] <= self.term:
            error = self.resp(self.term, msg["id"], False)
            self.dispatcher.message(fd, json.dumps(error))
        else:
            self.term = msg["term"]
            okay = self.resp(self.term, msg["id"], True)
            self.dispatcher.message(fd, json.dumps(okay))

    #
    # Common Methods
    #

    def cluck(self):
        pass # TODO screw with logs

    def handle_message(self, fd, message):
        try:
            msg = json.loads(message)
        except (TypeError,ValueError):
            print("Could not load JSON from FD %d, disconnecting." % fd)
            self.dispatcher.message(fd, json.dumps(self.resp(-1, "", "INVALID JSON")))
            if self.leaderfd == -1:
                self.pluck(fd)
            self.dispatcher.disconnect(fd)
            return
        if self.leaderfd == -1:
            self.handle_leader(fd, msg)
        else:
            self.handle_follower(fd, msg)

    def peck(self):
        for fd, msg in self.dispatcher.poll():
            if msg == "CONNECTED":
                pass # have to handle this case
            elif msg == "DISCONNECTED":
                # if the leader disconnected, start a new campaign
                if fd == self.leaderfd:
                    self.campaign()
            else:
                self.handle_message(fd, msg)

        if self.leaderfd == -1:
            # time for heartbeat?
            if time() - self.last_heartbeat > 1:
                # heartbeat every half a second
                print("HEARTBEAT")
                self.broadcast({"message": "HEARTBEAT"})
                self.last_heartbeat = time()
            # send messages
            self.send()

    def resp(self, term, id, response):
        return {"term" : term, "id" : id, "message" : response}

if __name__ == "__main__":
    dsp = Dispatcher(9001)
    hen = Hen("localhost:9001", dsp)
    try:
        while True:
            hen.peck()
            sleep(0.05)
    except KeyboardInterrupt:
        pass
    dsp.kill()
    print("Bye!")
