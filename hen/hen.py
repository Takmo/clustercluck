from dispatcher import Dispatcher
from time import sleep

import json
import uuid

class Hen:

    def __init__(self, host, dispatcher):
        self.host = host # this is the address others should connect to
        self.dispatcher = dispatcher
        self.leaderfd = -1 # I AM LEADER NAO
        self.leaderhost = self.host
        self.term = 0

    def campaign(self):
        pass # TODO

    def handle_follower(self, fd, msg):
        if fd == self.leaderfd:
            # a message from our fearless leader!
            pass # TODO
        else:
            if msg["message"] == "ELECTME":
                self.vote(fd, msg)
            else:
                error = self.resp(msg["term"], msg["uuid"], "NOT LEADER")
                error["leader"] = self.leaderhost
                self.dispatcher.message(fd, json.dumps(error))

    def handle_leader(self, fd, msg):
        pass # TODO leaderly things

    def handle_message(self, fd, message):
        try:
            msg = json.loads(message)
        except (TypeError,ValueError):
            print("Could not load JSON from FD %d, disconnecting." % fd)
            self.dispatcher.message(fd, json.dumps(self.resp(-1, "", "INVALID JSON")))
            self.pluck(fd)
            return
        if self.leaderfd == -1:
            self.handle_leader(fd, msg)
        else:
            self.handle_follower(fd, msg)

    def hatch(self, fd, host):
        pass # TODO add connection as RAFT node

    def peck(self):
        for fd, msg in self.dispatcher.poll():
            if msg == "CONNECTED":
                pass # have to handle this case
            elif msg == "DISCONNECTED":
                if self.leaderfd == -1:
                    self.pluck(fd)
                elif fd == self.leaderfd:
                    self.campaign()
            else:
                self.handle_message(fd, msg)

    def pluck(self, fd):
        # TODO make this cleaner for RAFT nodes disconnecting
        self.dispatcher.disconnect(fd)

    def resp(self, term, uuid, response):
        return {"term" : term, "uuid" : uuid, "message" : response}

    def vote(self, fd, msg):
        if msg["term"] <= self.term:
            error = self.resp(self.term, msg["uuid"], False)
            self.dispatcher.message(fd, json.dumps(error))
        else:
            self.term = msg["term"]
            okay = self.resp(self.term, msg["uuid"], True)
            self.dispatcher.message(fd, json.dumps(okay))

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
