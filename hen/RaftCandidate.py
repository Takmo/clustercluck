# See README.md for licensing information. Or don't. I don't really care.

from RaftState import RaftState

class RaftCandidate(RaftState):

    # Election stuff is entirely TODO right now.

    # Constructor
    def __init__(self, commitlog):
        # Call superclass constructor.
        super().__init__(commitlog)
