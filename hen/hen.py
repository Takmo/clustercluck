from dispatcher import Dispatcher
from time import sleep

import json
import uuid

class Hen:

    def __init__(self, dispatcher):
        self.dispatcher = dispatcher
        self.leaderfd = -1 # I AM LEADER NAO

    def campaign(self):
        pass # TODO

    def error(self, uuid, response):
        return {"uuid" : uuid, "response" : response}

    def handle_message(self, fd, message):
        try:
            msg = json.loads(message)
        except (TypeError,ValueError):
            print("Could not load JSON from FD %d, disconnecting." % fd)
            self.dispatcher.message(fd, json.dumps(self.error("", "INVALID JSON")))
            self.dispatcher.disconnect(fd) # TODO - handle DC of nodes
        # TODO other things lol

    def peck(self):
        for fd, msg in self.dispatcher.poll():
            if msg == "CONNECTED":
                pass # dispatcher handled connect; we don't have to
            elif msg == "DISCONNECTED":
                if self.leaderfd == -1:
                    continue # TODO we're the leader, process a 'pluck' event
                elif fd == self.leaderfd:
                    continue # TODO leader DC'd, we should start a campaign
            else:
                self.handle_message(fd, msg)

if __name__ == "__main__":
    dsp = Dispatcher(9001)
    hen = Hen(dsp)
    try:
        while True:
            hen.peck()
            sleep(0.05)
    except KeyboardInterrupt:
        pass
    dsp.kill()
    print("Bye!")
