# See README.md for licensing information. Or don't. I don't really care.

from RaftState import RaftState

from random import randint
from time import time

# Note: The heartbeat timer needs to be random for each RaftFollower.
# The timer for each node will be a random number of MS between these.

# In milliseconds...
heartbeat_min = 4000
heartbeat_max = 8000

class RaftFollower(RaftState):

    # The RaftFollower accepts new entries from the leader and either
    # adds them or rejects them, letting the leader know that there
    # is an inconsistency. The RaftFollower also responds to elections
    # and will initiate a new election if their heartbeat timer expires.

    # Constructor
    def __init__(self, dispatcher, host, remote_host, commitlog = []):
        # First call the parent constructor.
        super().__init__(commitlog)

        # Now set our settings.
        self.dispatcher = dispatcher
        self.host = host

        # Finally connect to remote_host.
        self.connect(remote_host)

    # Handle messages from the leader.
    def chirp(self, fd, msg):
        # TODO handle elections.
        # All other types of messages must come from the leader.
        if fd != self.leader_df:
            return

        # Handle heartbeat.
        if msg['type'] is 'heartbeat':
            self.last_heartbeat = time() * 1000
            return

        # Handle a new entry.
        if msg['type'] is 'entry':
            msg['resp'] = self.append(msg)
            self.dispatcher.message(self.leader_fd, json.dumps(msg))

    # Establish a connection with a leader.
    def connect(self, remote_host):
        # Establish connection.
        self.leader_fd = self.dispatcher.connect(remote_host)
        self.leader_host = remote_host

        # Set the heartbeat timeout.
        self.timeout = randint(heartbeat_min, heartbeat_max)
        self.last_heartbeat = time() * 1000
    
    # Tick.
    def peck(self):
        # Basically just handle heartbeat. TODO
        return
