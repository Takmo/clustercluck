# See README.md for licensing information. Or don't. I don't really care.

from RaftState import RaftState

class RaftLeader(RaftState):

    # The RaftLeader is responsible for sending new entries to all RaftFollowers.
    # In addition to new entries being sent, the leader also sends out heartbeats
    # to all followers to confirm that it is alive. Appended entries require
    # confirmation. Heartbeats do not.

    def __init__(self, dispatcher, host, followers = [], commitlog = []):
        # First call the parent constructor.
        super().__init__(commitlog)

        # Set up our networking things.
        self.host = host
        self.dispatcher = dispatcher

        # Set up our list of followers. Each follower is a dict containing
        # the fd, host, last response time, and last acknowledged entry.
        # If transitioning from ElectionState, this will already be filled.
        self.followers = followers

        # These are the queues for nodes being added or removed from the
        # current cluster configuration. Same as `self.followers`.
        self.hatch_queue = []
        self.pluck_queue = []

        # Log the current time for heartbeats.
        self.last_heartbeat = time()

    def send_messages(self):
        for follower in self.followers:
            # TODO: don't send messages constantly, time instead
            last_id = follower['last_id']

            # We might not have to send anything.
            if last_id == self.get_latest_entry()['id']:
                # We don't need to send anything because we're up-to-date.
                continue
            
            # Grab the base messages.
            old_msg = self.get_entry_at(last_id)
            new_msg = self.get_entry_at(last_id + 1)

            # Format the message itself.
            new_msg['old_term'] = old_msg['term']
            new_msg['old_id'] = old_msg['id']
            new_msg['type'] = 'entry'

            # Append our information.
            new_msg['host'] = self.host

            # Send the message
            self.dispatcher.message(follower['fd'], json.dumps(new_msg))
        # Done!
    
    def chirp(self, fd, msg):
        # Basically let's just check (n)acks.
        # TODO: in the future, this should also respond to elections and transition.
        for follower in self.followers:
            # Cull out noise.
            if node['fd'] != fd:
                # Not the correct follower.
                continue
            if msg['type'] != 'entry':
                # Weird, they didn't respond to an entry...
                continue
            if msg['id'] != (follower['last_id'] + 1):
                # Weird, the (n)ack is unexpected. Ignore.
                continue

            # Actually (n)ack messages.
            if msg['resp'] == True:
                # They accepted the message, time to send the next one.
                follower['last_id'] += 1
            if msg['resp'] == False:
                # They didn't accept the message. We'll try one earlier.
                follower['last_id'] -= 1
        # Done!

    def hatch(self, fd, host):
        # TODO: manage configuration changes
        self.hatch_queue.append({'host': host, 'fd': fd})

    # Send a heartbeat message to all followers.
    def heartbeat(self):
        for follower in self.followers:
            send = json.dumps({'host': host, 'type': 'heartbeat'})
            self.dispatcher.message(follower['fd'], send)

    def peck(self):
        # Possibly send a heartbeat.
        if time() - self.last_heartbeat > 1:
            self.heartbeat()
            self.last_heartbeat = time()
        # Send outstanding messages.
        self.send()

    def pluck(self, fd):
        # TODO: manage configuration changes
        for follower in self.followers:
            if follower['fd'] == fd:
                self.pluck_queue.append(follower)
                break
        # Done!

