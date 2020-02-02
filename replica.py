import logging
from dotenv import load_dotenv
from os import getenv
from random import randint

from states import ReplicaState
from lockhandler import LockHandler

class Replica:
    MIN_ELECTION_TIMEOUT_ENV_VAR_NAME = "RANDOM_TIMEOUT_MIN_MS"
    MAX_ELECTION_TIMEOUT_ENV_VAR_NAME = "RANDOM_TIMEOUT_MAX_MS"

    def __init__(self):
        load_dotenv()

        self._state = ReplicaState.FOLLOWER
        self._currentTerm = 0
        self._log = []
        self._commitIndex = 0
        self._lastApplied = 0
        self._nextIndex = []
        self._matchIndex = []
        self._timeout = getElectionTimeout()

        self.lockHandler = LockHandler(7)

    def __str__(self):
        return ""

    def _getElectionTimeout(self):
        minTimeMS = getenv(Replica.MIN_ELECTION_TIMEOUT_ENV_VAR_NAME)
        maxTimeMS = getenv(Replica.MAX_ELECTION_TIMEOUT_ENV_VAR_NAME)

        if not minTimeMS:
            raise ValueError("Attempted to read value from nonexistent enviornemnt variable {Replica.MIN_ELECTION_TIMEOUT_ENV_VAR_NAME}")
        if not maxTimeMS:
            raise ValueError("Attempted to read value from nonexistent enviornemnt variable {Replica.MAX_ELECTION_TIMEOUT_ENV_VAR_NAME}")

        return randint(int(minTimeMS), int(maxTimeMS))
