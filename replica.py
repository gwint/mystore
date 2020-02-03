#!/usr/bin/python3

import sys

sys.path.append('gen-py')

import logging
from dotenv import load_dotenv
from os import getenv
from random import randint
from socket import gethostname, gethostbyname
from time import sleep
from threading import Thread

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

from locknames import LockNames
from states import ReplicaState
from lockhandler import LockHandler
from replicaservice import ReplicaService
from replicaservice.ttypes import Ballot, Response, Entry

class Replica:
    MIN_ELECTION_TIMEOUT_ENV_VAR_NAME = "RANDOM_TIMEOUT_MIN_MS"
    MAX_ELECTION_TIMEOUT_ENV_VAR_NAME = "RANDOM_TIMEOUT_MAX_MS"
    CLUSTER_MEMBERSHIP_FILE_ENV_VAR_NAME = "CLUSTER_MEMBERSHIP_FILE"

    def __init__(self, port):
        load_dotenv()

        self._replicaToReplicaCommPort = port

        self._state = ReplicaState.FOLLOWER
        self._currentTerm = 0
        self._log = []
        self._commitIndex = 0
        self._lastApplied = 0
        self._nextIndex = []
        self._matchIndex = []
        self._timeout = self._getElectionTimeout()
        self._timeLeft = self._timeout

        self._clusterMembership = self._getClusterMembership()

        self._lockHandler = LockHandler(9)

        Thread(target=self._timer).start()

    def requestVote(self,
                    term,
                    candidateID,
                    lastLogIndex,
                    lastLogTerm):
        pass

    def appendEntry(self,
                    term,
                    leaderID,
                    prevLogIndex,
                    prevLogTerm,
                    entry,
                    leaderCommit):
        pass

    def _getElectionTimeout(self):
        minTimeMS = getenv(Replica.MIN_ELECTION_TIMEOUT_ENV_VAR_NAME)
        maxTimeMS = getenv(Replica.MAX_ELECTION_TIMEOUT_ENV_VAR_NAME)

        if not minTimeMS:
            raise ValueError("Attempted to read value from nonexistent enviornemnt variable {Replica.MIN_ELECTION_TIMEOUT_ENV_VAR_NAME}")
        if not maxTimeMS:
            raise ValueError("Attempted to read value from nonexistent enviornemnt variable {Replica.MAX_ELECTION_TIMEOUT_ENV_VAR_NAME}")

        return randint(int(minTimeMS), int(maxTimeMS))

    def _getClusterMembership(self):
        membership = set()
        membershipFile = getenv(Replica.CLUSTER_MEMBERSHIP_FILE_ENV_VAR_NAME)

        with open(membershipFile, 'r') as membershipFileObj:
            line = membershipFileObj.readline()
            while line:
                host, port = line.strip().split(':')
                try:
                    membership.add((host, int(port)))
                except ValueError:
                    raise ValueError(f'All ports in {Replica.CLUSTER_MEMBERSHIP_FILE_ENV_VAR_NAME} must be integer values.  {port} is not.')

                line = membershipFileObj.readline()

        localIP = gethostbyname(gethostname())
        membership.remove((localIP, self._replicaToReplicaCommPort))

        return membership

    def _timer(self):
        while True:
            self._lockHandler.acquireLocks(LockNames.TIMER_LOCK)

            if self._timeLeft == 0:
                print("Time has expired!")
                self._lockHandler.releaseLocks(LockNames.TIMER_LOCK)
                break

            self._timeLeft -= 1
            self._lockHandler.releaseLocks(LockNames.TIMER_LOCK)

            sleep(0.001)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Incorrect usage: try ./replica.py <port number>")
        sys.exit(1)

    portStr = sys.argv[1]

    try:
        portToUse = int(portStr)
        print(f'Running on port {portToUse}')
        replica = Replica(portToUse)

        processor = ReplicaService.Processor(replica)
        transport = TSocket.TServerSocket(host='127.0.0.1', port=portToUse)
        tfactory = TTransport.TBufferedTransportFactory()
        pfactory = TBinaryProtocol.TBinaryProtocolFactory()

        server = TServer.TSimpleServer(processor, transport, tfactory, pfactory)
        server.serve()

    except ValueError:
        raise ValueError(f'The provided port number ({portStr}) must contain only digits')
