#!/usr/bin/python3

import sys

sys.path.append('gen-py')

import logging
from dotenv import load_dotenv
from os import getenv
from random import randint
from socket import gethostname, gethostbyname, timeout
from time import sleep
from threading import Thread
from os import _exit

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

from locknames import LockNames
from states import ReplicaState
from lockhandler import LockHandler
from replicaservice import ReplicaService
from replicaservice.ttypes import Ballot, AppendEntryResponse, Entry, ID, GetResponse, PutResponse

class Replica:
    MIN_ELECTION_TIMEOUT_ENV_VAR_NAME = "RANDOM_TIMEOUT_MIN_MS"
    MAX_ELECTION_TIMEOUT_ENV_VAR_NAME = "RANDOM_TIMEOUT_MAX_MS"
    CLUSTER_MEMBERSHIP_FILE_ENV_VAR_NAME = "CLUSTER_MEMBERSHIP_FILE"
    HEARTBEAT_TICK_ENV_VAR_NAME = "HEARTBEAT_TICK_MS"
    RPC_TIMEOUT_ENV_VAR_NAME = "RPC_TIMEOUT_MS"

    def __init__(self, port):
        load_dotenv()

        self._replicaToReplicaCommPort = port

        self._state = ReplicaState.FOLLOWER
        self._currentTerm = 0
        self._log = [self._getEmptyLogEntry()]
        self._commitIndex = 0
        self._lastApplied = 0
        self._nextIndex = {}
        self._matchIndex = {}
        self._timeout = self._getElectionTimeout()
        self._timeLeft = self._timeout
        self._heartbeatTick = int(getenv(Replica.HEARTBEAT_TICK_ENV_VAR_NAME))
        self._myID = ("127.0.0.1", port)
        self._votedFor = ()
        self._leader = ()
        self._map = {}
        self._clientResponseCache = {}
        self._numServersReplicatedOn = 0

        self._clusterMembership = self._getClusterMembership()

        self._logger = logging.getLogger(f'{self._myID}_logger')
        handler = logging.FileHandler(f'{self._myID[0]}:{self._myID[1]}.log')
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        handler.setFormatter(formatter)
        self._logger.addHandler(handler)
        self._logger.setLevel(logging.DEBUG)

        self._lockHandler = LockHandler(12)

        Thread(target=self._timer).start()
        Thread(target=self._heartbeatSender).start()

    def requestVote(self, \
                    term, \
                    candidateID, \
                    lastLogIndex, \
                    lastLogTerm):

        ballotTerm = -1
        voteGranted = False

        self._lockHandler.acquireLocks(LockNames.CURR_TERM_LOCK, \
                                       LockNames.LOG_LOCK, \
                                       LockNames.STATE_LOCK, \
                                       LockNames.VOTED_FOR_LOCK)

        self._logger.debug(f'{self._state} {(candidateID.hostname, candidateID.port)} is requesting my vote.')

        if term > self._currentTerm:
            self._state = ReplicaState.FOLLOWER
            self._currentTerm = term
            self._votedFor = ()

        ballotTerm = self._currentTerm

        if (not self._votedFor or self._votedFor == (candidateID.hostname, candidateID.port)) and \
                self._isAtLeastAsUpToDateAs(len(self._log), \
                                            self._log[-1].term, \
                                            lastLogIndex, \
                                            lastLogTerm):
            self._logger.debug(f'Granted vote to {(candidateID.hostname, candidateID.port)}')
            voteGranted = True
            self._votedFor = (candidateID.hostname, candidateID.port)

        self._lockHandler.releaseLocks(LockNames.CURR_TERM_LOCK, \
                                       LockNames.LOG_LOCK, \
                                       LockNames.STATE_LOCK, \
                                       LockNames.VOTED_FOR_LOCK)

        ballot = Ballot()
        ballot.voteGranted = voteGranted
        ballot.term = ballotTerm

        return ballot

    def appendEntry(self, \
                    term, \
                    leaderID, \
                    prevLogIndex, \
                    prevLogTerm, \
                    entry, \
                    leaderCommit):

        response = AppendEntryResponse()
        response.status = True

        self._lockHandler.acquireLocks(LockNames.CURR_TERM_LOCK, \
                                       LockNames.LOG_LOCK, \
                                       LockNames.STATE_LOCK, \
                                       LockNames.COMMIT_INDEX_LOCK,
                                       LockNames.LEADER_LOCK)

        if term < self._currentTerm or prevLogTerm >= len(self._log) or \
                                self._log[prevLogIndex].term != prevLogTerm:
            response.status = False
            response.term = max(term, self._currentTerm)
            self._currentTerm = max(term, self._currentTerm)

            self._lockHandler.releaseLocks(LockNames.CURR_TERM_LOCK, \
                                           LockNames.LOG_LOCK, \
                                           LockNames.STATE_LOCK, \
                                           LockNames.COMMIT_INDEX_LOCK, \
                                           LockNames.LEADER_LOCK)
            return response

        self._state = ReplicaState.FOLLOWER

        self._leader = (leaderID.hostname, leaderID.port)

        self._currentTerm = max(term, self._currentTerm)

        self._logger.debug(f'{self._state} ({leaderID.hostname}:{leaderID.port}) is appending an entry to my log.')

        self._timeLeft = self._timeout

        response.term = self._currentTerm

        self._lockHandler.releaseLocks(LockNames.CURR_TERM_LOCK, \
                                       LockNames.LOG_LOCK, \
                                       LockNames.STATE_LOCK, \
                                       LockNames.COMMIT_INDEX_LOCK, \
                                       LockNames.LEADER_LOCK)

        return response

    def kill(self):
        self._logger.debug(f'{self._myID[0]}:{self._myID[1]} is now dying')
        _exit(0)

    def get(self, key, requestNumber):
        response = GetResponse(success=True)

        self._lockHandler.acquireLocks(LockNames.STATE_LOCK, \
                                       LockNames.LEADER_LOCK, \
                                       LockNames.CURR_TERM_LOCK, \
                                       LockNames.LOG_LOCK, \
                                       LockNames.COMMIT_INDEX_LOCK, \
                                       LockNames.VOTED_FOR_LOCK)

        self._logger.debug(f'{self._state} ({self._myID[0]}:{self._myID[1]}) now attempting to retrieve value associated with {key}')

        if self._state != ReplicaState.LEADER:
            self._logger.debug(f'{self._state} Was contacted to resolve a GET but am not the leader, redirected to ({self._leader[0] if self._leader else ""}:{self._leader[1] if self._leader else ""})')
            response.success = False
            response.leaderID = None
            if self._leader:
                response.leaderID = ID(self._leader[0], self._leader[1])

            self._lockHandler.releaseLocks(LockNames.STATE_LOCK, \
                                           LockNames.LEADER_LOCK, \
                                           LockNames.CURR_TERM_LOCK, \
                                           LockNames.LOG_LOCK, \
                                           LockNames.COMMIT_INDEX_LOCK, \
                                           LockNames.VOTED_FOR_LOCK)

            return response


        for host, port in self._clusterMembership:
            transport = TSocket.TSocket(host, port)
            transport = TTransport.TBufferedTransport(transport)

            try:
                transport.open()
                protocol = TBinaryProtocol.TBinaryProtocol(transport)
                client = ReplicaService.Client(protocol)

                self._logger.debug(f'{self._state} Now sending a heartbeat to ({host}:{port})')

                response = client.appendEntry( \
                                  self._currentTerm, \
                                  self._getID(self._myID[0], self._myID[1]), \
                                  len(self._log)-1, \
                                  self._log[-1].term, \
                                  None, \
                                  self._commitIndex)

                if response.term > self._currentTerm:
                    self._state = ReplicaState.FOLLOWER
                    self._currentTerm = response.term
                    self._votedFor = ()
                    response.success = False
                    break

            except TTransport.TTransportException:
                self._logger.debug(f'Error while attempting to send an empty appendEntry request to ({host}:{port})')

        self._lockHandler.releaseLocks(LockNames.STATE_LOCK, \
                                       LockNames.LEADER_LOCK, \
                                       LockNames.CURR_TERM_LOCK, \
                                       LockNames.LOG_LOCK, \
                                       LockNames.COMMIT_INDEX_LOCK, \
                                       LockNames.VOTED_FOR_LOCK)

        return response

    def put(self, key, value, clientIdentifier, requestIdentifier):
        self._lockHandler.acquireLocks(LockNames.STATE_LOCK, \
                                       LockNames.LEADER_LOCK, \
                                       LockNames.CURR_TERM_LOCK, \
                                       LockNames.LOG_LOCK, \
                                       LockNames.COMMIT_INDEX_LOCK, \
                                       LockNames.VOTED_FOR_LOCK, \
                                       LockNames.REPLICATION_AMOUNT_LOCK)

        response = PutResponse(success=True)
        self._logger.debug(f'{self._state} ({self._myID[0]}:{self._myID[1]}) now attempting to associate {value} with {key} ({key} => {value})')

        if self._state != ReplicaState.LEADER:
            self._logger.debug(f'{self._state} Was contacted to resolve a PUT but am not the leader, redirected to ({self._leader[0] if self._leader else ""}:{self._leader[1] if self._leader else ""})')
            response.success = False
            if self._leader:
                response.leaderID = ID(self._leader[0], self._leader[1])

            self._lockHandler.releaseLocks(LockNames.STATE_LOCK, \
                                           LockNames.LEADER_LOCK, \
                                           LockNames.CURR_TERM_LOCK, \
                                           LockNames.LOG_LOCK, \
                                           LockNames.COMMIT_INDEX_LOCK, \
                                           LockNames.VOTED_FOR_LOCK, \
                                           LockNames.REPLICATION_AMOUNT_LOCK)

            return response

        newLogEntry = Entry(key, value, self._currentTerm, clientIdentifier, requestIdentifier)
        self._log.append(newLogEntry)

        for host, port in self._clusterMembership:
            transport = TSocket.TSocket(host, port)
            transport = TTransport.TBufferedTransport(transport)
            transport.open()
            protocol = TBinaryProtocol.TBinaryProtocol(transport)
            client = ReplicaService.Client(protocol)

            response = client.appendEntry(self._currentTerm, \
                                          self._leader, \
                                          len(self._log)-2, \
                                          self._log[-2].term, \
                                          newLogEntry, \
                                          self._commitIndex)

            if response.term > self._currentTerm:
                self._currentTerm = response.term
                self._state = ReplicaState.FOLLOWER
                self._votedFor = ()
                response.success = False
                break

            if not response.success:
                ## Try to send again
                pass

        response.leaderID = ID(self._leader[0], self._leader[1])

        self._lockHandler.releaseLocks(LockNames.STATE_LOCK, \
                                       LockNames.LEADER_LOCK, \
                                       LockNames.CURR_TERM_LOCK, \
                                       LockNames.LOG_LOCK, \
                                       LockNames.COMMIT_INDEX_LOCK, \
                                       LockNames.VOTED_FOR_LOCK, \
                                       LockNames.REPLICATION_AMOUNT_LOCK)

        return response

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

        membership.remove(self._myID)

        return membership

    def _getID(self, host, port):
        newID = ID()
        newID.hostname = host
        newID.port = port

        return newID

    def _getEmptyLogEntry(self):
        emptyLogEntry = Entry()
        emptyLogEntry.key = -1
        emptyLogEntry.value = -1
        emptyLogEntry.term = -1

        return emptyLogEntry

    def _isAtLeastAsUpToDateAs(self, \
                               myLastLogIndex, \
                               myLastLogTerm, \
                               otherLastLogIndex, \
                               otherLastLogTerm):

        return (myLastLogTerm > otherLastLogTerm) or \
                (myLastLogTerm == otherLastLogTerm and \
                        myLastLogIndex >= otherLastLogIndex)

    def _timer(self):
        sleep(3)

        while True:
            self._lockHandler.acquireLocks(LockNames.STATE_LOCK, \
                                           LockNames.LOG_LOCK, \
                                           LockNames.CURR_TERM_LOCK, \
                                           LockNames.TIMER_LOCK, \
                                           LockNames.COMMIT_INDEX_LOCK, \
                                           LockNames.VOTED_FOR_LOCK, \
                                           LockNames.LEADER_LOCK, \
                                           LockNames.NEXT_INDEX_LOCK, \
                                           LockNames.MATCH_INDEX_LOCK)

            if self._state == ReplicaState.LEADER:
                self._lockHandler.releaseLocks(LockNames.STATE_LOCK, \
                                               LockNames.LOG_LOCK, \
                                               LockNames.CURR_TERM_LOCK, \
                                               LockNames.TIMER_LOCK, \
                                               LockNames.COMMIT_INDEX_LOCK, \
                                               LockNames.VOTED_FOR_LOCK, \
                                               LockNames.LEADER_LOCK, \
                                               LockNames.NEXT_INDEX_LOCK, \
                                               LockNames.MATCH_INDEX_LOCK)
                sleep(0.3)
                continue

            if self._timeLeft == 0:
                self._logger.debug(f'{self._state} Time has expired!')

                votesReceived = 1
                self._state = ReplicaState.CANDIDATE
                self._votedFor = (self._myID[0], self._myID[1])
                self._timeLeft = self._timeout
                self._currentTerm += 1

                for host, port in self._clusterMembership:
                    self._logger.debug(f'Now requesting vote from {host}:{port}')
                    transport = TSocket.TSocket(host, port)
                    transport.setTimeout(int(getenv(Replica.RPC_TIMEOUT_ENV_VAR_NAME)) / 1000)
                    transport = TTransport.TBufferedTransport(transport)

                    try:
                        transport.open()
                        protocol = TBinaryProtocol.TBinaryProtocol(transport)
                        client = ReplicaService.Client(protocol)

                        leaderID = self._getID(self._myID[0], self._myID[1])

                        ballot = client.requestVote(self._currentTerm, \
                                                    leaderID, \
                                                    len(self._log), \
                                                    self._log[-1].term)

                        votesReceived += (1 if ballot.voteGranted else 0)

                    except TTransport.TTransportException:
                        self._logger.debug(f'Error while attempting to request a vote from replica at ({host}:{port})')
                    except timeout:
                        self._logger.debug(f'Timeout occurred while attempting request vote from ({host}:{port})')

                    self._logger.debug(f'{votesReceived} votes have been received during this election')

                    if votesReceived >= ((len(self._clusterMembership)+1) // 2) + 1:
                        self._state = ReplicaState.LEADER
                        self._leader = self._myID

                        for host, port in self._clusterMembership:
                            self._nextIndex[(host,port)] = len(self._log)
                            self._matchIndex[(host,port)] = 0

                            transport = TSocket.TSocket(host, port)
                            transport.setTimeout(int(getenv(Replica.RPC_TIMEOUT_ENV_VAR_NAME)) / 1000)
                            transport = TTransport.TBufferedTransport(transport)

                            try:
                                transport.open()
                                protocol = TBinaryProtocol.TBinaryProtocol(transport)
                                client = ReplicaService.Client(protocol)

                                response = client.appendEntry( \
                                     self._currentTerm, \
                                     self._getID(self._myID[0], self._myID[1]), \
                                     len(self._log)-1, \
                                     self._log[-1].term, \
                                     None, \
                                     self._commitIndex)

                                if response.term > self._currentTerm:
                                    self._state = ReplicaState.FOLLOWER
                                    self._currentTerm = response.term
                                    self._votedFor = ()

                            except TTransport.TTransportException:
                                self._logger.debug(f'Error while attempting to send an empty appendEntry request to replica at ({host}:{port})')
                            except timeout:
                                self._logger.debug(f'Timeout experienced while attempting to assert control over ({host}:{port})')

                        self._logger.debug("I have asserted control of the cluster!")
                        break

                self._timeLeft = self._timeout

            self._timeLeft -= 1

            self._lockHandler.releaseLocks(LockNames.STATE_LOCK, \
                                           LockNames.LOG_LOCK, \
                                           LockNames.CURR_TERM_LOCK, \
                                           LockNames.TIMER_LOCK, \
                                           LockNames.COMMIT_INDEX_LOCK, \
                                           LockNames.VOTED_FOR_LOCK, \
                                           LockNames.LEADER_LOCK, \
                                           LockNames.NEXT_INDEX_LOCK, \
                                           LockNames.MATCH_INDEX_LOCK)

            sleep(0.001)

    def _heartbeatSender(self):
        while True:
            self._lockHandler.acquireLocks(LockNames.CURR_TERM_LOCK, \
                                           LockNames.LOG_LOCK, \
                                           LockNames.STATE_LOCK, \
                                           LockNames.COMMIT_INDEX_LOCK, \
                                           LockNames.VOTED_FOR_LOCK)

            if self._state != ReplicaState.LEADER:
                self._lockHandler.releaseLocks(LockNames.CURR_TERM_LOCK, \
                                               LockNames.LOG_LOCK, \
                                               LockNames.STATE_LOCK, \
                                               LockNames.COMMIT_INDEX_LOCK, \
                                               LockNames.VOTED_FOR_LOCK)
                sleep(0.5)
                continue

            for host, port in self._clusterMembership:
                transport = TSocket.TSocket(host, port)
                transport = TTransport.TBufferedTransport(transport)

                try:
                    transport.open()
                    protocol = TBinaryProtocol.TBinaryProtocol(transport)
                    client = ReplicaService.Client(protocol)

                    self._logger.debug(f'{self._state} Now sending a heartbeat to ({host}:{port})')

                    response = client.appendEntry( \
                                  self._currentTerm, \
                                  self._getID(self._myID[0], self._myID[1]), \
                                  len(self._log)-1, \
                                  self._log[-1].term, \
                                  None, \
                                  self._commitIndex)

                    if response.term > self._currentTerm:
                        self._state = ReplicaState.FOLLOWER
                        self._currentTerm = response.term
                        self._votedFor = ()

                except TTransport.TTransportException:
                    self._logger.debug(f'Error while attempting to send an empty appendEntry request to ({host}:{port})')


            self._lockHandler.releaseLocks(LockNames.CURR_TERM_LOCK, \
                                           LockNames.LOG_LOCK, \
                                           LockNames.STATE_LOCK, \
                                           LockNames.COMMIT_INDEX_LOCK, \
                                           LockNames.VOTED_FOR_LOCK)

            sleep(self._heartbeatTick / 1000)

if __name__ == "__main__":

    if len(sys.argv) != 2:
        print("Incorrect usage: try ./replica.py <port number>")
        sys.exit(1)

    portStr = sys.argv[1]

    try:
        portToUse = int(portStr)
        print(f'Running on port {portToUse}')
        replica = Replica(portToUse)

        transport = TSocket.TServerSocket(host='127.0.0.1', port=portToUse)
        tfactory = TTransport.TBufferedTransportFactory()
        pfactory = TBinaryProtocol.TBinaryProtocolFactory()

        processor = ReplicaService.Processor(replica)

        server = TServer.TThreadPoolServer(processor, transport, tfactory, pfactory)

        server.serve()

    except ValueError:
        raise ValueError(f'The provided port number ({portStr}) must contain only digits')
