from states import ReplicaState
from lockhandler import LockHandler

class Replica:
    def __init__(self):
        self._state = ReplicaState.FOLLOWER
        self._currentTerm = 0
        self._log = []
        self._commitIndex = 0
        self._lastApplied = 0
        self._nextIndex = []
        self._matchIndex = []

        self.lockHandler = LockHandler(7)

    def __str__(self):
        return ""
