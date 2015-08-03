from argparse import ArgumentParser
from dispatcher import Dispatcher, parse_address
from time import time, sleep

import json

class RaftState:

    def resp(self, term, id, response):
        return {"term" : term, "id" : id, "message" : response}

class LeaderState(RaftState):

    def __init__(self, host, nodes, dispatcher, term=0, latest_id=0):
        # set our host - never changes
        self.host = host

        # same with dispatcher
        self.dispatcher = dispatcher

        # because this always comes either initially or from ElectionState
        # this will already be set up. remember that nodes here hold address,
        # fd, and unacknowledged messages. In FollowState they just hold addresses.
        self.nodes = nodes

        # set term, latest_id, and heartbeat time
        self.term = term
        self.latest_id = 0
        self.last_heartbeat = time()

    def broadcast(self, msg):
        # send out a message to everyone but us
        # assume that the message was prepared, then just add term, id, and host
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
    
    def chrip(self, fd, msg):
        # can return this or FollowState()
        # this method probably needs lots of work, should definitely handle
        # responses to a new election, but maybe shouldn't vote?
        if msg["message"] == "HATCH":
            self.hatch(fd, msg["host"])
            return self
        # everything else is basically just checking acks
        for n in self.nodes:
            node = self.nodes[n]
            if node["fd"] == fd:
                for id in node["unacked"]:
                    if id == msg["id"]:
                        del node["unacked"][id]
                        return self
        # man that looks cool ^.^
        return self

    def hatch(self, fd, host):
        self.nodes[host] = {"fd": fd, "unacked": {}}
        msg = {"message": "UPDATE", "nodes": list(self.nodes.keys())}
        self.broadcast(msg)
        print("A new Hen at %s has hatched!" % host)

    def peck(self):
        # time for heartbeat?
        if time() - self.last_heartbeat > 1:
            # heartbeat every half a second
            self.broadcast({"message": "HEARTBEAT"})
            self.last_heartbeat = time()
        # send messages
        self.send()

    def pluck(self, fd):
        changed = False
        for n in self.nodes:
            node = self.nodes[n]
            if node["fd"] == fd:
                del self.nodes[n]
                msg = {"message": "UPDATE", "nodes": list(self.nodes.keys())}
                self.broadcast(msg)
                print("Hen at %s was plucked!" % n)
                break;

    def send(self):
        pluckfds = []
        for n in self.nodes:
            node = self.nodes[n]
            if n == self.host:
                # we never have to send to ourselves, but we should
                # be in self.nodes
                continue
            if len(node["unacked"]) > 10:
                # disconnect if it hasn't acked 10+ messages
                pluckfds.append(node["fd"])
            if len(node["unacked"]) > 0:
                # send the oldest message first
                msg = (list(node["unacked"].values())[0])
                self.dispatcher.message(node["fd"], msg)
        for fd in pluckfds:
            self.pluck(fd)
            self.dispatcher.disconnect(fd)

class ElectionState(RaftState):

    def __init__(self, host, nodes, dispatcher):
        pass

    def campaign_all(self):
        # send a request to all nodes
        pass
    
    def handle_message(self, fd, msg):
        # can return this, FollowState(), or LeaderState()
        pass

#TODO If the leader dies or times out, the first node to notice should
#TODO delete the leader's presence. Then all nodes receiving a campaign
#TODO request should do the same. The leader will have to ask to rejoin.

class FollowState(RaftState):

    def __init__(self, host, dispatcher, remote_host, term=0):
        # set our basic things
        self.host = host
        self.term = termm
        self.dispatcher = dispatcher

    def chrip(self, fd, msg):
        print("Received: %s" % msg)
        if fd == self.leaderfd:
            # a message from our fearless leader!
            self.term = msg["term"]
            self.latest_id = msg["id"]
            if msg["message"] == "CLUCK":
                self.cluck()
            if msg["message"] == "HEARTBEAT":
                pass # we just have to ack
            if msg["message"] == "UPDATE":
                self.update(msg["nodes"])
            # ack the message
            msg["message"] = True
            self.dispatcher.message(self.leaderfd, json.dumps(msg))
            print("Responded: %s" % json.dumps(msg))
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

    def connect(self, host):
        # TODO make this handle reconnects to correct host
        self.leaderfd = self.dispatcher.connect(host)
        self.leaderhost = host
        msg = {"host" : self.host, "message": "HATCH"}
        self.dispatcher.message(self.leaderfd, json.dumps(msg))
        # if we connected to the wrong host, it will respond with
        # the actual leader, who we should connect to

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

class Hen:

    def __init__(self, host, dispatcher, remote_host=""):
        if remote_host == "":
            self.state = LeaderState(host, {host: None}, dispatcher)
        else:
            self.state = FollowState(host, dispatcher, remote_host)

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
                continue # have to handle this case
            if msg == "DISCONNECTED":
                # if the leader disconnected, start a new campaign
                #if fd == self.leaderfd:
                    #self.campaign()
                continue
            self.state.chirp(fd, msg)

        # leader stuff
        if self.state.peck != None:
            self.state.peck()

if __name__ == "__main__":
    parser = ArgumentParser(description = "Great clucking software. Distributed, even.")
    parser.add_argument("--host", dest="host", type=str, nargs=1, required=True, help="Local host/port.")
    parser.add_argument("--connect", dest="connect", type=str, nargs=1, required=False, default="",
            help="Remote host/port if not leader.")
    args = parser.parse_args()

    _, port = parse_address(args.host[0])
    dsp = Dispatcher(port)
    if args.connect:
        hen = Hen(args.host[0], dsp, args.connect[0])
    else:
        hen = Hen(args.host[0], dsp)

    try:
        while True:
            hen.peck()
            sleep(0.05)
    except KeyboardInterrupt:
        pass
    dsp.kill()
    print("Bye!")
