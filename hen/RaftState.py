# See README.md for licensing information. Or don't. I don't really care.

class RaftState:

    # One of the most important details of the RaftState superclass is
    # to handle the commitlog, which will be an important detail for
    # all subclasses.

    # The `self.commitlog` member is a list of dicts containing each
    # entry. Each entry contains the id, term, and value associated with
    # an entry in the log.

    # Constructor: Either pass a commitlog or start empty.
    def __init__(self, commitlog = []):
        self.commitlog = commitlog

    # Attempt to append a log entry.
    def append(self, entry):
        # Find our version of the previous entry.
        match = self.get_entry_at(entry['old_id'])

        # If we don't have the previous entry, wait for it.
        if match == None:
            return False

        # If the previous entry is in our system, clear everything after and append.
        if entry['old_term'] == match['term'] and entry['old_id'] == match['id']:
            self.clear_after(latest['id'])
            self.commitlog.append({'term': entry['term'], 'id': entry['id'], 'entry': entry['entry']})
            return True
        return False

    # Clear all entries after a given ID.
    def clear_after(self, id):
        if get_latest_entry()['id'] == id:
            return
        newcommitlog = []
        for entry in self.commitlog:
            if entry['id'] <= id:
                newcommitlog.append(entry)
        self.commitlog = newcommitlog

    # Get the entry at an id.
    def get_entry_at(self, id):
        if id < 0:
            return {'term': -1, 'id': -1, 'entry': ''}
        if id < len(self.commitlog):
            return self.commitlog[id]
        return None

