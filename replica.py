#!/usr/bin/python3

import sys

sys.path.append('gen-py')

import logging
from dotenv import load_dotenv
from os import getenv
from random import randint
from socket import gethostname, gethostbyname, timeout
from time import sleep
from threading import Thread, get_ident, Condition
from os import _exit
from queue import Queue

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

from locknames import LockNames
from states import ReplicaState
from lockhandler import LockHandler
from replicaservice import ReplicaService
from replicaservice.ttypes import Ballot, AppendEntryResponse, Entry, ID, GetResponse, PutResponse

def areAMajorityGreaterThanOrEqual(numLst, num):
    numForMajority = (len(numLst) // 2) + 1
    numGreaterThanOrEqual = 0
    for currNum in numLst:
        if currNum >= num:
            numGreaterThanOrEqual += 1

    return numGreaterThanOrEqual >= numForMajority

class Job:
    def __init__(self, entryPosition=None, targetHost=None, targetPort=None):
        self.entryPosition = entryPosition
        self.targetHost = targetHost
        self.targetPort = targetPort

class ReplicaFormatter(logging.Formatter):
    def __init__(self, fmt, replica):
        self._replica = replica
        super(ReplicaFormatter, self).__init__(fmt)

    def format(self, record):
        record.state = self._replica.getState()
        record.term = self._replica.getTerm()
        record.log = self._replica.getLog()
        record.map = self._replica.getMap()
        record.commitIndex = self._replica.getCommitIndex()
        record.lastApplied = self._replica.getLastApplied()
        record.matchIndex = self._replica.getMatchIndex()

        return super(ReplicaFormatter, self).format(record)

class Replica:
    MIN_ELECTION_TIMEOUT_ENV_VAR_NAME = "RANDOM_TIMEOUT_MIN_MS"
    MAX_ELECTION_TIMEOUT_ENV_VAR_NAME = "RANDOM_TIMEOUT_MAX_MS"
    CLUSTER_MEMBERSHIP_FILE_ENV_VAR_NAME = "CLUSTER_MEMBERSHIP_FILE"
    HEARTBEAT_TICK_ENV_VAR_NAME = "HEARTBEAT_TICK_MS"
    RPC_TIMEOUT_ENV_VAR_NAME = "RPC_TIMEOUT_MS"
    RPC_RETRY_TIMEOUT_MIN_ENV_VAR_NAME = "MIN_RPC_RETRY_TIMEOUT"

    def __init__(self, port):
        load_dotenv()

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
        self._myID = (gethostbyname(gethostname()), port)
        self._votedFor = ()
        self._leader = ()
        self._map = {}
        self._clientResponseCache = {}
        self._currentRequestBeingServiced = None
        self._jobsToRetry = Queue()
        self._noopIndex = None
        self._hasOperationStarted = False

        self._clusterMembership = self._getClusterMembership()

        self._logger = logging.getLogger(f'{self._myID}_logger')
        handler = logging.FileHandler(f'{self._myID[0]}:{self._myID[1]}.log')
        formatter = ReplicaFormatter('%(term)s %(state)s %(asctime)s %(message)s %(log)s CI: %(commitIndex)s LA: %(lastApplied)s %(map)s %(matchIndex)s', self)
        handler.setFormatter(formatter)
        self._logger.addHandler(handler)
        self._logger.setLevel(logging.DEBUG)

        self._lockHandler = LockHandler(13)
        self._lockHandler.lockAll()

        Thread(target=self._timer).start()
        Thread(target=self._heartbeatSender).start()
        Thread(target=self._retryRequest).start()

    def getState(self):
        return self._state

    def getTerm(self):
        return self._currentTerm

    def getLog(self):
        printableLog = []
        for entry in self._log:
            printableLog.append((entry.key, entry.value, entry.term, entry.clientIdentifier, entry.requestIdentifier))

        return str(printableLog)

    def getMap(self):
        return str(self._map)

    def getCommitIndex(self):
        return str(self._commitIndex)

    def getLastApplied(self):
        return str(self._lastApplied)

    def getMatchIndex(self):
        return str(self._matchIndex)

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

        self._logger.debug(f'{(candidateID.hostname, candidateID.port)} is requesting my vote.')

        if term > self._currentTerm:
            self._state = ReplicaState.FOLLOWER
            self._currentTerm = term
            self._votedFor = ()

        ballotTerm = self._currentTerm

        if (not self._votedFor or self._votedFor == (candidateID.hostname, candidateID.port)) and \
                self._isAtLeastAsUpToDateAs(lastLogIndex, \
                                            lastLogTerm, \
                                            len(self._log)-1, \
                                            self._log[-1].term):
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

        response = AppendEntryResponse(success=True)

        self._lockHandler.acquireLocks(LockNames.CURR_TERM_LOCK, \
                                       LockNames.LOG_LOCK, \
                                       LockNames.STATE_LOCK, \
                                       LockNames.LAST_APPLIED_LOCK,
                                       LockNames.LEADER_LOCK, \
                                       LockNames.MAP_LOCK, \
                                       LockNames.TIMER_LOCK, \
                                       LockNames.COMMIT_INDEX_LOCK)

        self._logger.debug(f'({leaderID.hostname}:{leaderID.port}) is appending an entry ({entry}) to my log.')

        if term >= self._currentTerm:
            self._timeLeft = self._timeout

        assert prevLogIndex >= 0
        assert len(self._log) > 0

        if (term < self._currentTerm) or \
                                (prevLogIndex >= len(self._log)) or \
                                (self._log[prevLogIndex].term != prevLogTerm):
            self._logger.debug(f'Rejecting appendEntry request from ({leaderID.hostname}:{leaderID.port}); leaderterm={term}, myterm={self._currentTerm}, prevLogIndex={prevLogIndex}')
            response.success = False
            response.term = max(term, self._currentTerm)
            self._currentTerm = max(term, self._currentTerm)

            self._lockHandler.releaseLocks(LockNames.CURR_TERM_LOCK, \
                                           LockNames.LOG_LOCK, \
                                           LockNames.STATE_LOCK, \
                                           LockNames.LAST_APPLIED_LOCK, \
                                           LockNames.LEADER_LOCK, \
                                           LockNames.MAP_LOCK, \
                                           LockNames.TIMER_LOCK, \
                                           LockNames.COMMIT_INDEX_LOCK)
            return response

        self._state = ReplicaState.FOLLOWER

        indicesToRemove = []
        for i in range(len(self._log)-1, prevLogIndex, -1):
            indicesToRemove.append(i)

        if indicesToRemove:
            self._logger.debug(f'Now removing entries at indices {indicesToRemove} from log to bring in line with leader')
            for _ in indicesToRemove:
                self._log.pop()

        def applyEntry(entry):
            if not (entry):
                return

            self._map[entry.key] = entry.value

        if entry:
            self._logger.debug(f'Now appending entry ({entry}) to the log')
            self._log.append(entry)

        if leaderCommit > self._commitIndex:
            self._commitIndex = min(leaderCommit, len(self._log)-1)

        if self._commitIndex > self._lastApplied:
            self._logger.debug(f'Now applying log entry ({self._log[self._lastApplied+1]}) to state machine')
            self._lastApplied += 1
            applyEntry(self._log[self._lastApplied])

        self._leader = (leaderID.hostname, leaderID.port)

        self._currentTerm = max(term, self._currentTerm)

        response.term = self._currentTerm

        self._lockHandler.releaseLocks(LockNames.CURR_TERM_LOCK, \
                                       LockNames.LOG_LOCK, \
                                       LockNames.STATE_LOCK, \
                                       LockNames.LAST_APPLIED_LOCK, \
                                       LockNames.LEADER_LOCK, \
                                       LockNames.MAP_LOCK, \
                                       LockNames.TIMER_LOCK, \
                                       LockNames.COMMIT_INDEX_LOCK)

        return response

    def kill(self):
        self._logger.debug(f'{self._myID[0]}:{self._myID[1]} is now dying')
        _exit(0)

    def getInformation(self):
        return { "endpoint": f'{self._myID[0]}:{str(self._myID[1])}', \
                 "role": f'{str(self._state)}', \
                 "term": f'{self._currentTerm}', \
                 "index": f'{len(self._log)}' }

    def start(self):
        print("starting operation...")
        if not self._hasOperationStarted:
            self._hasOperationStarted = True
            self._lockHandler.unlockAll()

    def get(self, key, clientIdentifier, requestNumber):
        response = GetResponse(success=True)

        self._lockHandler.acquireLocks(LockNames.STATE_LOCK, \
                                       LockNames.LEADER_LOCK, \
                                       LockNames.CURR_TERM_LOCK, \
                                       LockNames.LOG_LOCK, \
                                       LockNames.COMMIT_INDEX_LOCK, \
                                       LockNames.VOTED_FOR_LOCK, \
                                       LockNames.MATCH_INDEX_LOCK, \
                                       LockNames.LATEST_NO_OP_LOG_INDEX)

        self._logger.debug(f'({self._myID[0]}:{self._myID[1]}) now attempting to retrieve value associated with {key}')

        if self._state != ReplicaState.LEADER:
            self._logger.debug(f'Was contacted to resolve a GET but am not the leader, redirected to ({self._leader[0] if self._leader else ""}:{self._leader[1] if self._leader else ""})')
            response.success = False
            response.leaderID = None
            if self._leader:
                response.leaderID = ID(self._leader[0], self._leader[1])

            self._lockHandler.releaseLocks(LockNames.STATE_LOCK, \
                                           LockNames.LEADER_LOCK, \
                                           LockNames.CURR_TERM_LOCK, \
                                           LockNames.LOG_LOCK, \
                                           LockNames.COMMIT_INDEX_LOCK, \
                                           LockNames.VOTED_FOR_LOCK, \
                                           LockNames.MATCH_INDEX_LOCK, \
                                           LockNames.LATEST_NO_OP_LOG_INDEX)

            return response

        numReplicasSuccessfullyContacted = 1

        for host, port in self._clusterMembership:
            transport = TSocket.TSocket(host, port)
            transport.setTimeout(int(getenv(Replica.RPC_TIMEOUT_ENV_VAR_NAME)))
            transport = TTransport.TBufferedTransport(transport)

            try:
                transport.open()
                try:
                    protocol = TBinaryProtocol.TBinaryProtocol(transport)
                    client = ReplicaService.Client(protocol)

                    self._logger.debug(f'Now sending a heartbeat to ({host}:{port})')

                    appendEntryResponse = client.appendEntry( \
                                      self._currentTerm, \
                                      self._getID(self._myID[0], self._myID[1]), \
                                      len(self._log)-1, \
                                      self._log[-1].term, \
                                      None, \
                                      self._commitIndex)

                    if appendEntryResponse.term > self._currentTerm:
                        self._state = ReplicaState.FOLLOWER
                        self._currentTerm = appendEntryResponse.term
                        self._votedFor = ()
                        response.success = False

                        self._lockHandler.releaseLocks(LockNames.STATE_LOCK, \
                                                       LockNames.LEADER_LOCK, \
                                                       LockNames.CURR_TERM_LOCK, \
                                                       LockNames.LOG_LOCK, \
                                                       LockNames.COMMIT_INDEX_LOCK, \
                                                       LockNames.VOTED_FOR_LOCK, \
                                                       LockNames.MATCH_INDEX_LOCK, \
                                                       LockNames.LATEST_NO_OP_LOG_INDEX)

                        return response

                    if not appendEntryResponse.success:
                        response.success = False

                        self._lockHandler.releaseLocks(LockNames.STATE_LOCK, \
                                                       LockNames.LEADER_LOCK, \
                                                       LockNames.CURR_TERM_LOCK, \
                                                       LockNames.LOG_LOCK, \
                                                       LockNames.COMMIT_INDEX_LOCK, \
                                                       LockNames.VOTED_FOR_LOCK, \
                                                       LockNames.MATCH_INDEX_LOCK, \
                                                       LockNames.LATEST_NO_OP_LOG_INDEX)

                        return response

                    numReplicasSuccessfullyContacted += 1

                except TTransport.TTransportException as e:
                    if isinstance(e.inner, timeout):
                        self._logger.debug(f'Timeout occurred while attempting to send heartbeat to replica at ({host}:{port})')
                    else:
                        self._logger.debug(f'Error while attempting to send a heartbeat to replica at ({host}:{port}): {str(e)}')

            except TTransport.TTransportException as e:
                self._logger.debug(f'Error while attempting to open a connection to send an empty appendEntry request to ({host}:{port})')

        if numReplicasSuccessfullyContacted < ((len(self._clusterMembership)+1) // 2) + 1:
            response.success = False

            self._lockHandler.releaseLocks(LockNames.STATE_LOCK, \
                                           LockNames.LEADER_LOCK, \
                                           LockNames.CURR_TERM_LOCK, \
                                           LockNames.LOG_LOCK, \
                                           LockNames.COMMIT_INDEX_LOCK, \
                                           LockNames.VOTED_FOR_LOCK, \
                                           LockNames.MATCH_INDEX_LOCK, \
                                           LockNames.LATEST_NO_OP_LOG_INDEX)

            return response

        if areAMajorityGreaterThanOrEqual(list(self._matchIndex.values()) + [len(self._log)-1], self._noopIndex):
            response.value = self._map.get(key.strip(), "")

        self._lockHandler.releaseLocks(LockNames.STATE_LOCK, \
                                       LockNames.LEADER_LOCK, \
                                       LockNames.CURR_TERM_LOCK, \
                                       LockNames.LOG_LOCK, \
                                       LockNames.COMMIT_INDEX_LOCK, \
                                       LockNames.VOTED_FOR_LOCK, \
                                       LockNames.MATCH_INDEX_LOCK, \
                                       LockNames.LATEST_NO_OP_LOG_INDEX)

        return response

    def put(self, key, value, clientIdentifier, requestIdentifier):
        self._lockHandler.acquireLocks(LockNames.STATE_LOCK, \
                                       LockNames.LEADER_LOCK, \
                                       LockNames.CURR_TERM_LOCK, \
                                       LockNames.LOG_LOCK, \
                                       LockNames.COMMIT_INDEX_LOCK, \
                                       LockNames.VOTED_FOR_LOCK, \
                                       LockNames.NEXT_INDEX_LOCK, \
                                       LockNames.LAST_APPLIED_LOCK, \
                                       LockNames.MATCH_INDEX_LOCK)

        response = PutResponse(success=True)
        self._logger.debug(f'({self._myID[0]}:{self._myID[1]}) now attempting to associate {value} with {key} ({key} => {value})')

        if self._state != ReplicaState.LEADER:
            self._logger.debug(f'Was contacted to resolve a PUT but am not the leader, redirected to ({self._leader[0] if self._leader else ""}:{self._leader[1] if self._leader else ""})')
            response.success = False
            if self._leader:
                response.leaderID = ID(self._leader[0], self._leader[1])

            self._lockHandler.releaseLocks(LockNames.STATE_LOCK, \
                                           LockNames.LEADER_LOCK, \
                                           LockNames.CURR_TERM_LOCK, \
                                           LockNames.LOG_LOCK, \
                                           LockNames.COMMIT_INDEX_LOCK, \
                                           LockNames.VOTED_FOR_LOCK, \
                                           LockNames.NEXT_INDEX_LOCK, \
                                           LockNames.LAST_APPLIED_LOCK, \
                                           LockNames.MATCH_INDEX_LOCK)

            return response

        if requestIdentifier == self._currentRequestBeingServiced:
            self._logger.debug(f'Continuing servicing of request {requestIdentifier}')

            relevantEntryIndex = 0
            for entryIndex in range(len(self._log)):
                if self._log[entryIndex].requestIdentifier == requestIdentifier:
                    relevantEntryIndex = entryIndex
                    break

            assert relevantEntryIndex != 0

            entryIsFromCurrentTerm = (self._log[relevantEntryIndex].term == self._currentTerm)

            response.success = entryIsFromCurrentTerm and \
                         areAMajorityGreaterThanOrEqual(list(self._matchIndex.values()) + [len(self._log)-1], relevantEntryIndex)

            self._logger.debug(f'Is this entry from this term? = {entryIsFromCurrentTerm}')
            self._logger.debug(f'Has the entry been successfully replicated on a majority of replicas {areAMajorityGreaterThanOrEqual(list(self._matchIndex.values()) + [len(self._log)-1], relevantEntryIndex)}')

            self._lockHandler.releaseLocks(LockNames.STATE_LOCK, \
                                           LockNames.LEADER_LOCK, \
                                           LockNames.CURR_TERM_LOCK, \
                                           LockNames.LOG_LOCK, \
                                           LockNames.COMMIT_INDEX_LOCK, \
                                           LockNames.VOTED_FOR_LOCK, \
                                           LockNames.NEXT_INDEX_LOCK, \
                                           LockNames.LAST_APPLIED_LOCK, \
                                           LockNames.MATCH_INDEX_LOCK)

            return response

        newLogEntry = Entry(key, value, self._currentTerm, clientIdentifier, requestIdentifier)
        self._log.append(newLogEntry)

        self._currentRequestBeingServiced = requestIdentifier

        numServersReplicatedOn = 1

        for host, port in self._clusterMembership:
            transport = TSocket.TSocket(host, port)
            transport.setTimeout(int(getenv(Replica.RPC_TIMEOUT_ENV_VAR_NAME)))
            transport = TTransport.TBufferedTransport(transport)

            try:
                transport.open()
            except TTransport.TTransportException as e:
                self._logger.debug(f'Error while attempting to append an entry to replica at ({host}:{port}): {str(e)}')
                continue

            protocol = TBinaryProtocol.TBinaryProtocol(transport)
            client = ReplicaService.Client(protocol)

            try:
                self._logger.debug(f'Now sending an appendEntry request to ({host}:{port}); may take a while...')
                appendEntryResponse = client.appendEntry( \
                                            self._currentTerm, \
                                            ID(self._myID[0], self._myID[1]), \
                                            len(self._log)-2, \
                                            self._log[-2].term, \
                                            newLogEntry, \
                                            self._commitIndex)

                if appendEntryResponse.term > self._currentTerm:
                    self._currentTerm = appendEntryResponse.term
                    self._state = ReplicaState.FOLLOWER
                    self._votedFor = ()
                    response.success = False

                    self._lockHandler.releaseLocks( \
                                           LockNames.STATE_LOCK, \
                                           LockNames.LEADER_LOCK, \
                                           LockNames.CURR_TERM_LOCK, \
                                           LockNames.LOG_LOCK, \
                                           LockNames.COMMIT_INDEX_LOCK, \
                                           LockNames.VOTED_FOR_LOCK, \
                                           LockNames.NEXT_INDEX_LOCK, \
                                           LockNames.LAST_APPLIED_LOCK, \
                                           LockNames.MATCH_INDEX_LOCK)

                    return response

                if not appendEntryResponse.success:
                    self._logger.debug(f'AppendEntryRequest directed to ({host}:{port}) failed due to log
 inconsistency: Reducing next index value from {self._nextIndex[(host,port)]} to {self._nextIndex[(host,port)]-1}')
                    self._nextIndex[(host,port)] = max(1, self._nextIndex[(host,port)]-1)
                else:
                    self._logger.debug(f'Entry successfully replicated on ({host}:{port}: Now increasing
 replication amount from {numServersReplicatedOn} to {numServersReplicatedOn+1})')
                    self._matchIndex[(host,port)] = self._nextIndex[(host,port)]
                    self._nextIndex[(host,port)] += 1
                    numServersReplicatedOn += 1

            except TTransport.TTransportException as e:
                if isinstance(e.inner, timeout):
                    self._logger.debug(f'Timeout occurred while attempting to append entry to replica at ({host}:{port})')
                    self._jobsToRetry.put(Job(len(self._log)-1, host, port))
                    continue

                raise e

        response.leaderID = ID(self._leader[0], self._leader[1])

        if numServersReplicatedOn < ((len(self._clusterMembership) + 1) // 2) + 1:
            self._logger.debug(f'Entry unsuccessfully replicated on a majority of servers: replication amount = {numServersReplicatedOn} / {(len(self._clusterMembership) + 1 // 2) + 1}')
            response.success = False
        else:
            self._logger.debug(f'Entry successfully replicated on a majority of servers and writing mapping ({key} => {value}) to the state machine')

            self._map[key] = value
            self._commitIndex = len(self._log)-1
            self._lastApplied = len(self._log)-1

        self._lockHandler.releaseLocks(LockNames.STATE_LOCK, \
                                       LockNames.LEADER_LOCK, \
                                       LockNames.CURR_TERM_LOCK, \
                                       LockNames.LOG_LOCK, \
                                       LockNames.COMMIT_INDEX_LOCK, \
                                       LockNames.VOTED_FOR_LOCK, \
                                       LockNames.NEXT_INDEX_LOCK, \
                                       LockNames.LAST_APPLIED_LOCK, \
                                       LockNames.MATCH_INDEX_LOCK)

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
        emptyLogEntry.key = ""
        emptyLogEntry.value = ""
        emptyLogEntry.term = -1

        return emptyLogEntry

    def _isAtLeastAsUpToDateAs(self, \
                               otherLastLogIndex, \
                               otherLastLogTerm, \
                               myLastLogIndex, \
                               myLastLogTerm):

        return (otherLastLogTerm > myLastLogTerm) or \
                (otherLastLogTerm == myLastLogTerm and \
                        otherLastLogIndex >= myLastLogIndex)

    def _retryRequest(self):
        maxTimeToSpendRetryingMS = int(getenv(Replica.MIN_ELECTION_TIMEOUT_ENV_VAR_NAME))

        while True:
            while self._jobsToRetry.empty():
                sleep(0.01)

            job = self._jobsToRetry.get()

            timeSpentOnCurrentRetryMS = 0
            timeoutMS = int(getenv(Replica.RPC_RETRY_TIMEOUT_MIN_ENV_VAR_NAME))

            while timeSpentOnCurrentRetryMS < (0.8 * maxTimeToSpendRetryingMS):
                sleep(timeoutMS // 1000)

                self._lockHandler.acquireLocks(LockNames.STATE_LOCK, \
                                               LockNames.LEADER_LOCK, \
                                               LockNames.CURR_TERM_LOCK, \
                                               LockNames.VOTED_FOR_LOCK, \
                                               LockNames.NEXT_INDEX_LOCK, \
                                               LockNames.MATCH_INDEX_LOCK, \
                                               LockNames.LOG_LOCK)

                entry = self._log[job.entryPosition]

                if self._state != ReplicaState.LEADER:
                    self._jobsToRetry = Queue()
                    self._lockHandler.releaseLocks(LockNames.STATE_LOCK, \
                                                   LockNames.LEADER_LOCK, \
                                                   LockNames.CURR_TERM_LOCK, \
                                                   LockNames.VOTED_FOR_LOCK, \
                                                   LockNames.NEXT_INDEX_LOCK, \
                                                   LockNames.MATCH_INDEX_LOCK, \
                                                   LockNames.LOG_LOCK)

                    break

                print(f'Retrying appendEntry request ({entry}) after {timeoutMS} ms...')

                transport = TSocket.TSocket(job.targetHost, job.targetPort)
                transport.setTimeout(timeoutMS)
                transport = TTransport.TBufferedTransport(transport)

                try:
                    transport.open()
                except TTransport.TTransportException as e:
                    self._logger.debug(f'Error while attempting to retry appending an entry to replica at ({job.targetHost}:{job.targetPort}): {str(e)}')

                protocol = TBinaryProtocol.TBinaryProtocol(transport)
                client = ReplicaService.Client(protocol)

                try:
                    self._logger.debug(f'Now retrying appendEntry request to ({job.targetHost}:{job.targetPort}); may take a while...')
                    appendEntryResponse = client.appendEntry( \
                                                self._currentTerm, \
                                                ID(self._myID[0], self._myID[1]), \
                                                job.entryPosition-1, \
                                                self._log[job.entryPosition-1].term, \
                                                entry, \
                                                self._commitIndex)

                    if appendEntryResponse.term > self._currentTerm:
                        self._currentTerm = appendEntryResponse.term
                        self._state = ReplicaState.FOLLOWER
                        self._votedFor = ()
                        self._jobsToRetry = Queue()
                        self._lockHandler.releaseLocks(LockNames.STATE_LOCK, \
                                                       LockNames.LEADER_LOCK, \
                                                       LockNames.CURR_TERM_LOCK, \
                                                       LockNames.VOTED_FOR_LOCK, \
                                                       LockNames.NEXT_INDEX_LOCK, \
                                                       LockNames.MATCH_INDEX_LOCK, \
                                                       LockNames.LOG_LOCK)

                        break

                    if not appendEntryResponse.success:
                        self._logger.debug(f'AppendEntryRequest retry directed to ({job.targetHost}:{job.targetPort})
 failed due to log inconsistency: THIS SHOULD NEVER HAPPEN!')
                        self._lockHandler.releaseLocks(LockNames.STATE_LOCK, \
                                                       LockNames.LEADER_LOCK, \
                                                       LockNames.CURR_TERM_LOCK, \
                                                       LockNames.VOTED_FOR_LOCK, \
                                                       LockNames.NEXT_INDEX_LOCK, \
                                                       LockNames.MATCH_INDEX_LOCK, \
                                                       LockNames.LOG_LOCK)
                    else:
                        self._logger.debug(f'Entry successfully replicated on ({job.targetHost}:{job.targetPort})
 during retry: Now increasing nextIndex value from {self._nextIndex[(job.targetHost,job.targetPort)]} to
 {self._nextIndex[(job.targetHost,job.targetPort)]+1}')
                        self._matchIndex[(job.targetHost,job.targetPort)] =
 self._nextIndex[(job.targetHost,job.targetPort)]
                        self._nextIndex[(job.targetHost,job.targetPort)] += 1
                        self._lockHandler.releaseLocks(LockNames.STATE_LOCK, \
                                                       LockNames.LEADER_LOCK, \
                                                       LockNames.CURR_TERM_LOCK, \
                                                       LockNames.VOTED_FOR_LOCK, \
                                                       LockNames.NEXT_INDEX_LOCK, \
                                                       LockNames.MATCH_INDEX_LOCK, \
                                                       LockNames.LOG_LOCK)
                        break

                except TTransport.TTransportException as e:
                    if isinstance(e.inner, timeout):
                        self._logger.debug(f'Timeout occurred while attempting to retry appending entry to replica at ({job.targetHost}:{job.targetPort})')
                        self._lockHandler.releaseLocks(LockNames.STATE_LOCK, \
                                                       LockNames.LEADER_LOCK, \
                                                       LockNames.CURR_TERM_LOCK, \
                                                       LockNames.VOTED_FOR_LOCK, \
                                                       LockNames.NEXT_INDEX_LOCK, \
                                                       LockNames.MATCH_INDEX_LOCK, \
                                                       LockNames.LOG_LOCK)
                    else:
                        self._logger.debug(f'Unexpected exception occurred while attempting to retry appending entry to replica at ({job.targetHost}:{job.targetPort}): {str(e)}')
                        self._lockHandler.releaseLocks(LockNames.STATE_LOCK, \
                                                       LockNames.LEADER_LOCK, \
                                                       LockNames.CURR_TERM_LOCK, \
                                                       LockNames.VOTED_FOR_LOCK, \
                                                       LockNames.NEXT_INDEX_LOCK, \
                                                       LockNames.MATCH_INDEX_LOCK, \
                                                       LockNames.LOG_LOCK)

                timeSpentOnCurrentRetryMS += timeoutMS
                timeoutMS *= 1.5

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
                self._logger.debug(f'Time has expired!')

                votesReceived = 1
                self._state = ReplicaState.CANDIDATE
                self._votedFor = (self._myID[0], self._myID[1])
                self._timeLeft = self._timeout
                self._currentTerm += 1

                for host, port in self._clusterMembership:
                    self._logger.debug(f'Now requesting vote from {host}:{port}')
                    transport = TSocket.TSocket(host, port)
                    transport.setTimeout(int(getenv(Replica.RPC_TIMEOUT_ENV_VAR_NAME)))
                    transport = TTransport.TBufferedTransport(transport)

                    try:
                        transport.open()

                        try:
                            protocol = TBinaryProtocol.TBinaryProtocol(transport)
                            client = ReplicaService.Client(protocol)

                            leaderID = self._getID(self._myID[0], self._myID[1])

                            ballot = client.requestVote(self._currentTerm, \
                                                        leaderID, \
                                                        len(self._log), \
                                                        self._log[-1].term)

                            votesReceived += (1 if ballot.voteGranted else 0)

                        except TTransport.TTransportException as e:
                            if isinstance(e.inner, timeout):
                                self._logger.debug(f'Timeout occurred while requesting vote from ({host}:{port})')
                            else:
                                self._logger.debug(f'Error while attempting to request a vote from replica at ({host}:{port}): {str(e)}')

                    except TTransport.TTransportException as e:
                        self._logger.debug(f'Error while attempting to open a connection to request a vote from replica at ({host}:{port}): {str(e)}')

                    self._logger.debug(f'{votesReceived} votes have been received during this election')

                    if votesReceived >= ((len(self._clusterMembership)+1) // 2) + 1:
                        self._state = ReplicaState.LEADER
                        self._leader = self._myID

                        self._noopIndex = len(self._log)
                        noopEntry = Entry(key="", \
                                          value="", \
                                          term=self._currentTerm)

                        self._log.append(noopEntry)

                        for host, port in self._clusterMembership:
                            self._nextIndex[(host,port)] = len(self._log)-1
                            self._matchIndex[(host,port)] = 0

                            transport = TSocket.TSocket(host, port)
                            transport.setTimeout(int(getenv(Replica.RPC_TIMEOUT_ENV_VAR_NAME)))
                            transport = TTransport.TBufferedTransport(transport)

                            try:
                                transport.open()

                                try:
                                    protocol = TBinaryProtocol.TBinaryProtocol(transport)
                                    client = ReplicaService.Client(protocol)

                                    appendEntryResponse = client.appendEntry( \
                                                        self._currentTerm, \
                                                        self._getID(self._myID[0], self._myID[1]), \
                                                        len(self._log)-2, \
                                                        self._log[-2].term, \
                                                        noopEntry, \
                                                        self._commitIndex)

                                    if appendEntryResponse.term > self._currentTerm:
                                        self._state = ReplicaState.FOLLOWER
                                        self._currentTerm = appendEntryResponse.term
                                        self._votedFor = ()

                                    if not appendEntryResponse.success:
                                        self._logger.debug(f'AppendEntryRequest directed to ({host}:{port}) failed due to log inconsistency: Reducing nextIndex value from {self._nextIndex[(host,port)]} to {max(1, self._nextIndex[(host,port)]-1)}')
                                        self._nextIndex[(host,port)] = max(1, self._nextIndex[(host,port)]-1)
                                    else:
                                        self._logger.debug(f'AppendEntryRequest containing no-op directed to ({host}:{port}) successful: Increasing next index value from {self._nextIndex[(host,port)]} to {self._nextIndex[(host,port)]+1}')
                                        self._matchIndex[(host,port)] = self._nextIndex[(host,port)]
                                        self._nextIndex[(host,port)] += 1

                                except TTransport.TTransportException as e:
                                    if isinstance(e.inner, timeout):
                                        self._logger.debug(f'Timeout experienced while attempting to assert control over replica at ({host}:{port})')
                                        self._jobsToRetry.put(Job(len(self._log)-1, host, port))
                                    else:
                                        self._logger.debug(f'Error while attempting to assert control over replica at ({host}:{port}): {str(e)}')

                            except TTransport.TTransportException as e:
                                self._logger.debug(f'Error while attempting to establish connection to assert control over replica at ({host}:{port}): {str(e)}')

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
                                           LockNames.VOTED_FOR_LOCK, \
                                           LockNames.NEXT_INDEX_LOCK, \
                                           LockNames.MATCH_INDEX_LOCK)

            if self._state != ReplicaState.LEADER:
                self._lockHandler.releaseLocks(LockNames.CURR_TERM_LOCK, \
                                               LockNames.LOG_LOCK, \
                                               LockNames.STATE_LOCK, \
                                               LockNames.COMMIT_INDEX_LOCK, \
                                               LockNames.VOTED_FOR_LOCK, \
                                               LockNames.NEXT_INDEX_LOCK, \
                                               LockNames.MATCH_INDEX_LOCK)
                sleep(0.5)
                continue

            old = self._commitIndex
            self._commitIndex = self._findUpdatedCommitIndex()
            if old != self._commitIndex:
                self._logger.debug(f'Commit Index changed from {old} to {self._commitIndex}: {self._matchIndex}')

            if self._commitIndex > self._lastApplied:
                def applyEntry(entry):
                    if not entry:
                        return
                    self._map[entry.key] = entry.value

                self._logger.debug(f'Now Applying log entry ({self._log[self._lastApplied+1]}) to state machine')
                self._lastApplied += 1
                applyEntry(self._log[self._lastApplied])

            for host, port in self._clusterMembership:
                transport = TSocket.TSocket(host, port)
                transport.setTimeout(int(getenv(Replica.RPC_TIMEOUT_ENV_VAR_NAME)))
                transport = TTransport.TBufferedTransport(transport)

                entryToSend = None
                prevLogIndex = len(self._log)-1
                prevLogTerm = self._log[-1].term
                if self._nextIndex[(host, port)] < len(self._log):
                    logIndexToSend = self._nextIndex[(host, port)]
                    entryToSend = self._log[logIndexToSend]
                    prevLogIndex = logIndexToSend-1
                    prevLogTerm = self._log[prevLogIndex].term

                try:
                    transport.open()

                    try:
                        protocol = TBinaryProtocol.TBinaryProtocol(transport)
                        client = ReplicaService.Client(protocol)

                        appendEntryResponse = client.appendEntry( \
                                      self._currentTerm, \
                                      self._getID(self._myID[0], self._myID[1]), \
                                      prevLogIndex, \
                                      prevLogTerm, \
                                      entryToSend, \
                                      self._commitIndex)

                        if appendEntryResponse.term > self._currentTerm:
                            self._state = ReplicaState.FOLLOWER
                            self._currentTerm = appendEntryResponse.term
                            self._votedFor = ()
                            break

                        if not appendEntryResponse.success:
                            self._logger.debug(f'AppendEntryRequest directed to ({host}:{port}) failed due to log inconsistency: Reducing next index value from {self._nextIndex[(host,port)]} to {self._nextIndex[(host,port)]-1}')
                            self._nextIndex[(host,port)] = max(0, self._nextIndex[(host,port)] - 1)
                        elif entryToSend:
                            self._logger.debug(f'AppendEntryRequest directed to ({host}:{port}) successful: Increasing next index value from {self._nextIndex[(host,port)]} to {self._nextIndex[(host,port)]+1}')
                            self._matchIndex[(host,port)] = self._nextIndex[(host,port)]
                            self._nextIndex[(host,port)] += 1
                        else:
                            self._logger.debug(f'AppendEntryRequest (heartbeat) directed to ({host}:{port}) successful')

                    except TTransport.TTransportException as e:
                        if isinstance(e.inner, timeout):
                            self._logger.debug(f'Timeout experienced while sending heartbeat to ({host}:{port})')
                            if entryToSend:
                                self._jobsToRetry.put(Job(self._nextIndex[(host, port)], host, port))
                        else:
                            self._logger.debug(f'Error while attempting to send an appendEntry request to ({host}:{port}) from heartbeatSender: {str(e)}')

                except TTransport.TTransportException as e:
                    self._logger.debug(f'Error while attempting to establish a connection to send an appendEntry request to ({host}:{port}) from heartbeatSender: {str(e)}')

            self._lockHandler.releaseLocks(LockNames.CURR_TERM_LOCK, \
                                           LockNames.LOG_LOCK, \
                                           LockNames.STATE_LOCK, \
                                           LockNames.COMMIT_INDEX_LOCK, \
                                           LockNames.VOTED_FOR_LOCK, \
                                           LockNames.NEXT_INDEX_LOCK, \
                                           LockNames.MATCH_INDEX_LOCK)

            sleep(self._heartbeatTick / 1000)

    def _findUpdatedCommitIndex(self):
        possibleNewCommitIndex = len(self._log)-1
        indices = list(self._matchIndex.values()) + [len(self._log)-1]
        while possibleNewCommitIndex > self._commitIndex:
            self._logger.debug(f'possibleNewCommitIndex: {possibleNewCommitIndex}; {indices}; {areAMajorityGreaterThanOrEqual(indices, possibleNewCommitIndex)}')
            if areAMajorityGreaterThanOrEqual(indices, possibleNewCommitIndex) and \
                                   self._log[possibleNewCommitIndex].term == self._currentTerm:
                return possibleNewCommitIndex

            possibleNewCommitIndex -= 1

        return possibleNewCommitIndex

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Incorrect usage: try ./replica.py <host> <port number>")
        sys.exit(1)

    portStr = sys.argv[1]

    try:
        portToUse = int(portStr)
        replica = Replica(portToUse)

        transport = TSocket.TServerSocket(host="0.0.0.0", port=portToUse)
        tfactory = TTransport.TBufferedTransportFactory()
        pfactory = TBinaryProtocol.TBinaryProtocolFactory()

        processor = ReplicaService.Processor(replica)

        server = TServer.TThreadedServer(processor, transport, tfactory, pfactory)

        server.serve()

    except ValueError:
        raise ValueError(f'The provided port number ({portStr}) must contain only digits')
