#include <iostream>
#include <string>
#include <cstdio>
#include <cstdlib>
#include <limits>
#include <fstream>
#include <sstream>
#include <chrono>
#include <stdlib.h>
#include <cassert>

#include "replica.hpp"
#include "lockhandler.hpp"
#include "locknames.hpp"
#include "states.hpp"

#include "dotenv.h"
#include "spdlog/spdlog.h"
#include "spdlog/sinks/basic_file_sink.h"
#include "spdlog/pattern_formatter.h"

#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TTransport.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TThreadedServer.h>

#include "gen-cpp/replicaservice_types.h"
#include "gen-cpp/ReplicaService.h"

using apache::thrift::transport::TTransportException;

bool
areAMajorityGreaterThanOrEqual(std::vector<int> numLst, int num) {
    unsigned int numForMajority = (numLst.size() / 2) + 1;
    unsigned int numGreaterThanOrEqual = 0;
    for(const int& currNum : numLst) {
        if(currNum >= num) {
            ++numGreaterThanOrEqual;
        }
    }

    return numGreaterThanOrEqual >= numForMajority;
}

const char* Replica::MIN_ELECTION_TIMEOUT_ENV_VAR_NAME = "RANDOM_TIMEOUT_MIN_MS";
const char* Replica::MAX_ELECTION_TIMEOUT_ENV_VAR_NAME = "RANDOM_TIMEOUT_MAX_MS";
const char* Replica::CLUSTER_MEMBERSHIP_FILE_ENV_VAR_NAME = "CLUSTER_MEMBERSHIP_FILE";
const char* Replica::HEARTBEAT_TICK_ENV_VAR_NAME = "HEARTBEAT_TICK_MS";
const char* Replica::RPC_TIMEOUT_ENV_VAR_NAME = "RPC_TIMEOUT_MS";
const char* Replica::RPC_RETRY_TIMEOUT_MIN_ENV_VAR_NAME = "MIN_RPC_RETRY_TIMEOUT";

Replica::Replica(unsigned int port) : state(ReplicaState::FOLLOWER),
                                      currentTerm(0),
                                      commitIndex(0),
                                      lastApplied(0),
                                      timeout(Replica::getElectionTimeout()),
                                      votedFor(Replica::getNullID()),
                                      leader(Replica::getNullID()),
                                      currentRequestBeingServiced(std::numeric_limits<unsigned int>::max()),
                                      hasOperationStarted(false),
                                      clusterMembership(Replica::getClusterMembership()),
                                      lockHandler(13),
                                      noopIndex(0) {

    this->timeLeft = this->timeout;
    this->heartbeatTick = atoi(dotenv::env[Replica::HEARTBEAT_TICK_ENV_VAR_NAME].c_str());

    this->log.push_back(Replica::getEmptyLogEntry());

    char hostBuffer[256];
    gethostname(hostBuffer, sizeof(hostBuffer));
    hostent* hostEntry = gethostbyname(hostBuffer);
    char* ip = inet_ntoa(*((struct in_addr*)
                           hostEntry->h_addr_list[0]));

    this->myID.hostname = std::string(ip);
    this->myID.port = port;

    auto it = this->clusterMembership.begin();
    for(; it != this->clusterMembership.end(); ++it) {
        if(this->myID == *it) {
            break;
        }
    }

    this->clusterMembership.erase(it);

    this->lockHandler.lockAll();

    spdlog::set_pattern("[%H:%M:%S:%e] %v");

    std::stringstream logFileNameStream;
    logFileNameStream << this->myID.hostname << ":" << this->myID.port << ".log";
    this->logger = spdlog::basic_logger_mt("file_logger", logFileNameStream.str());
    spdlog::flush_on(spdlog::level::info);
    spdlog::set_default_logger(this->logger);

    this->timerThr = std::thread(&Replica::timer, this);
    this->heartbeatSenderThr = std::thread(&Replica::heartbeatSender, this);
    this->retryThr = std::thread(&Replica::retryRequest, this);
}

void
Replica::requestVote(Ballot& _return, const int32_t term, const ID& candidateID, const int32_t lastLogIndex, const int32_t lastLogTerm) {
    int ballotTerm = -1;
    bool voteGranted = false;

    this->lockHandler.acquireLocks(LockName::CURR_TERM_LOCK,
                                   LockName::LOG_LOCK,
                                   LockName::STATE_LOCK,
                                   LockName::VOTED_FOR_LOCK);

    std::stringstream msg;
    msg << candidateID << " is requesting my vote.";
    this->logMsg(msg.str());

    if(term > this->currentTerm) {
        this->state = ReplicaState::FOLLOWER;
        this->currentTerm = term;
        this->votedFor = Replica::getNullID();
    }

    ballotTerm = this->currentTerm;

    if((this->votedFor == candidateID || Replica::isANullID(this->votedFor)) &&
                this->isAtLeastAsUpToDateAs(lastLogIndex,
                                            lastLogTerm,
                                            this->log.size()-1,
                                            this->log.back().term)) {
        msg.str("");
        msg << "Granted vote to " << candidateID;
        this->logMsg(msg.str());
        voteGranted = true;
        this->votedFor = candidateID;
    }

    this->lockHandler.releaseLocks(LockName::CURR_TERM_LOCK,
                                   LockName::LOG_LOCK,
                                   LockName::STATE_LOCK,
                                   LockName::VOTED_FOR_LOCK);

    _return.voteGranted = voteGranted;
    _return.term = ballotTerm;
}

void
Replica::appendEntry(AppendEntryResponse& _return, const int32_t term, const ID& leaderID, const int32_t prevLogIndex, const int32_t prevLogTerm, const Entry& entry, const int32_t leaderCommit) {

    _return.success = true;

    this->lockHandler.acquireLocks(LockName::CURR_TERM_LOCK,
                                   LockName::LOG_LOCK,
                                   LockName::STATE_LOCK,
                                   LockName::LAST_APPLIED_LOCK,
                                   LockName::LEADER_LOCK,
                                   LockName::MAP_LOCK,
                                   LockName::TIMER_LOCK,
                                   LockName::COMMIT_INDEX_LOCK);

    std::stringstream msg;
    msg <<  leaderID << " is appending " << entry << " to my log.";
    this->logMsg(msg.str());

    if(term >= this->currentTerm) {
        this->timeLeft = this->timeout;
    }

    assert(prevLogIndex >= 0);
    assert(this->log.size() > 0);

    if((term < this->currentTerm) || (prevLogIndex >= this->log.size()) ||
                                (this->log[prevLogIndex].term != prevLogTerm)) {
        std::stringstream msg;
        msg << "Rejecting appendEntry request from (" << leaderID << "; leaderterm=" << term << ", myterm=" << this->currentTerm << ", prevLogIndex=" << prevLogIndex << ")";
        this->logMsg(msg.str());

        _return.success = false;
        _return.term = std::max(term, this->currentTerm);
        this->currentTerm = std::max(term, this->currentTerm);

        this->lockHandler.releaseLocks(LockName::CURR_TERM_LOCK,
                                       LockName::LOG_LOCK,
                                       LockName::STATE_LOCK,
                                       LockName::LAST_APPLIED_LOCK,
                                       LockName::LEADER_LOCK,
                                       LockName::MAP_LOCK,
                                       LockName::TIMER_LOCK,
                                       LockName::COMMIT_INDEX_LOCK);

        return;
    }

    this->state = ReplicaState::FOLLOWER;

    unsigned int numEntriesToRemove = 0;
    for(unsigned int i = this->log.size()-1; i > prevLogIndex; --i) {
        ++numEntriesToRemove;
    }

    if(numEntriesToRemove > 0) {
        std::stringstream msg;
        msg << "Now removing " << numEntriesToRemove << " from the back of the log to bring it in line with the leader";
        this->logMsg(msg.str());

        for(unsigned int i = 0; i < numEntriesToRemove; ++i) {
            this->log.pop_back();
        }
    }

    if(!Replica::isAnEmptyEntry(entry)) {
        std::stringstream msg;
        msg << "Now appending " << entry << " to the log";
        this->logMsg(msg.str());

        this->log.push_back(entry);
    }

    if(leaderCommit > this->commitIndex) {
        assert(this->log.size() > 0);
        this->commitIndex = std::min(leaderCommit, (int) this->log.size()-1);
    }

    auto applyEntry = [&](const Entry& entry) {
        if(!Replica::isAnEmptyEntry(entry)) {
            this->stateMachine[entry.key] = entry.value;
        }
    };

    if(this->commitIndex > this->lastApplied) {
        std::stringstream msg;
        msg << "Now applying log entry " << this->log[this->lastApplied+1] << " to state machine";
        this->logMsg(msg.str());

        ++this->lastApplied;
        applyEntry(this->log[this->lastApplied]);
    }

    this->leader = leaderID;
    this->currentTerm = std::max(term, this->currentTerm);
    _return.term = this->currentTerm;

    this->lockHandler.releaseLocks(LockName::CURR_TERM_LOCK,
                                   LockName::LOG_LOCK,
                                   LockName::STATE_LOCK,
                                   LockName::LAST_APPLIED_LOCK,
                                   LockName::LEADER_LOCK,
                                   LockName::MAP_LOCK,
                                   LockName::TIMER_LOCK,
                                   LockName::COMMIT_INDEX_LOCK);
}

void
Replica::get(GetResponse& _return, const std::string& key, const std::string& clientIdentifier, const int32_t requestIdentifier) {
    _return.success = true;

    this->lockHandler.acquireLocks(LockName::STATE_LOCK,
                                   LockName::LEADER_LOCK,
                                   LockName::CURR_TERM_LOCK,
                                   LockName::LOG_LOCK,
                                   LockName::COMMIT_INDEX_LOCK,
                                   LockName::VOTED_FOR_LOCK,
                                   LockName::MATCH_INDEX_LOCK,
                                   LockName::LATEST_NO_OP_LOG_INDEX);

    std::stringstream msg;
    msg << this->myID << " now attempting to retrieve value associated with " << key;
    this->logMsg(msg.str());

    if(this->state != ReplicaState::LEADER) {
        std::stringstream msg;
        msg << "Was contacted to resolve a GET but am not the leader, redirected to " << this->leader;
        this->logMsg(msg.str());

        _return.success = false;
        _return.leaderID = Replica::getNullID();
        if(!Replica::isANullID(this->leader)) {
            _return.leaderID = this->leader;
        }

        this->lockHandler.releaseLocks(LockName::STATE_LOCK,
                                       LockName::LEADER_LOCK,
                                       LockName::CURR_TERM_LOCK,
                                       LockName::LOG_LOCK,
                                       LockName::COMMIT_INDEX_LOCK,
                                       LockName::VOTED_FOR_LOCK,
                                       LockName::MATCH_INDEX_LOCK,
                                       LockName::LATEST_NO_OP_LOG_INDEX);

        return;
    }

    unsigned int numReplicasSuccessfullyContacted = 1;

    for(const auto &id : this->clusterMembership) {
        std::shared_ptr<apache::thrift::transport::TSocket> socket(new apache::thrift::transport::TSocket(id.hostname, id.port));
        socket->setConnTimeout(atoi(dotenv::env[Replica::RPC_TIMEOUT_ENV_VAR_NAME].c_str()));
        socket->setSendTimeout(atoi(dotenv::env[Replica::RPC_TIMEOUT_ENV_VAR_NAME].c_str()));
        socket->setRecvTimeout(atoi(dotenv::env[Replica::RPC_TIMEOUT_ENV_VAR_NAME].c_str()));
        std::shared_ptr<apache::thrift::transport::TTransport> transport(new apache::thrift::transport::TBufferedTransport(socket));
        std::shared_ptr<apache::thrift::protocol::TProtocol> protocol(new apache::thrift::protocol::TBinaryProtocol(transport));
        ReplicaServiceClient client(protocol);

        try {
            transport->open();

            try {
                std::stringstream msg;
                msg << "Now sending a heartbeat to " << id;
                this->logMsg(msg.str());

                AppendEntryResponse appendEntryResponse;

                client.appendEntry(appendEntryResponse,
                                   this->currentTerm,
                                   this->myID,
                                   this->log.size()-1,
                                   this->log.back().term,
                                   Replica::getEmptyLogEntry(),
                                   this->commitIndex);

                msg.str("");
                msg << "Was AppendEntryRequest successful: " << appendEntryResponse.success;
                this->logMsg(msg.str());

                if(appendEntryResponse.term > this->currentTerm) {
                    this->state = ReplicaState::FOLLOWER;
                    this->currentTerm = appendEntryResponse.term;
                    this->votedFor = Replica::getNullID();
                    _return.success = false;

                    std::stringstream msg;
                    msg << "Early exit: Larger term encountered";
                    this->logMsg(msg.str());

                    this->lockHandler.releaseLocks(LockName::STATE_LOCK,
                                                   LockName::LEADER_LOCK,
                                                   LockName::CURR_TERM_LOCK,
                                                   LockName::LOG_LOCK,
                                                   LockName::COMMIT_INDEX_LOCK,
                                                   LockName::VOTED_FOR_LOCK,
                                                   LockName::MATCH_INDEX_LOCK,
                                                   LockName::LATEST_NO_OP_LOG_INDEX);

                    return;
                }

                if(appendEntryResponse.success == false) {
                    _return.success = false;

                    std::stringstream msg;
                    msg << "Early exit: appendEntryResponse not successful";
                    this->logMsg(msg.str());

                    this->lockHandler.releaseLocks(LockName::STATE_LOCK,
                                                   LockName::LEADER_LOCK,
                                                   LockName::CURR_TERM_LOCK,
                                                   LockName::LOG_LOCK,
                                                   LockName::COMMIT_INDEX_LOCK,
                                                   LockName::VOTED_FOR_LOCK,
                                                   LockName::MATCH_INDEX_LOCK,
                                                   LockName::LATEST_NO_OP_LOG_INDEX);

                    return;
                }

                ++numReplicasSuccessfullyContacted;
            }
            catch(TTransportException& e) {
                if(e.getType() == TTransportException::TTransportExceptionType::TIMED_OUT) {
                    std::stringstream msg;
                    msg << "Timeout occurred while attempting to send a heartbeat to replica at " << id;
                    this->logMsg(msg.str());
                }
                else {
                    std::stringstream msg;
                    msg << "Error while attempting to send a heartbeat to replica at " << id;
                    this->logMsg(msg.str());
                }
            }
        }
        catch(TTransportException& e) {
            msg.str("");
            msg << "Error while attempting to open a connection to send an empty AppendEntry request to replica at " << id << ":" << e.getType();
            this->logMsg(msg.str());
        }
    }

    if(numReplicasSuccessfullyContacted < ((this->clusterMembership.size()+1) / 2) + 1) {
        _return.success = false;

        std::stringstream msg;
        msg << "Early exit: replication level needed not reached";
        this->logMsg(msg.str());

        this->lockHandler.releaseLocks(LockName::STATE_LOCK,
                                       LockName::LEADER_LOCK,
                                       LockName::CURR_TERM_LOCK,
                                       LockName::LOG_LOCK,
                                       LockName::COMMIT_INDEX_LOCK,
                                       LockName::VOTED_FOR_LOCK,
                                       LockName::MATCH_INDEX_LOCK,
                                       LockName::LATEST_NO_OP_LOG_INDEX);

        return;
    }

    std::vector<int> matchIndices;
    for(auto pair : this->matchIndex) {
        matchIndices.push_back(pair.second);
    }
    matchIndices.push_back(this->log.size()-1);

    if(areAMajorityGreaterThanOrEqual(matchIndices, this->noopIndex)) {
        _return.value = this->stateMachine.at(key);
    }

    this->lockHandler.releaseLocks(LockName::STATE_LOCK,
                                   LockName::LEADER_LOCK,
                                   LockName::CURR_TERM_LOCK,
                                   LockName::LOG_LOCK,
                                   LockName::COMMIT_INDEX_LOCK,
                                   LockName::VOTED_FOR_LOCK,
                                   LockName::MATCH_INDEX_LOCK,
                                   LockName::LATEST_NO_OP_LOG_INDEX);

}

void
Replica::put(PutResponse& _return, const std::string& key, const std::string& value, const std::string& clientIdentifier, const int32_t requestIdentifier) {
    this->lockHandler.acquireLocks(LockName::STATE_LOCK,
                                   LockName::LEADER_LOCK,
                                   LockName::CURR_TERM_LOCK,
                                   LockName::LOG_LOCK,
                                   LockName::COMMIT_INDEX_LOCK,
                                   LockName::VOTED_FOR_LOCK,
                                   LockName::NEXT_INDEX_LOCK,
                                   LockName::LAST_APPLIED_LOCK,
                                   LockName::MATCH_INDEX_LOCK);

    _return.success = true;

    std::stringstream msg;
    msg << this->myID << " now attempting to associate " << key << " with " << value;
    this->logMsg(msg.str());

    if(this->state != ReplicaState::LEADER) {
        std::stringstream msg;
        msg << "Was contacted to resolve a PUT but am not the leader, redirected to " << this-> leader;
        this->logMsg(msg.str());

        _return.success = false;
        if(!Replica::isANullID(this->leader)) {
            _return.leaderID = this->leader;
        }

        this->lockHandler.releaseLocks(LockName::STATE_LOCK,
                                       LockName::LEADER_LOCK,
                                       LockName::CURR_TERM_LOCK,
                                       LockName::LOG_LOCK,
                                       LockName::COMMIT_INDEX_LOCK,
                                       LockName::VOTED_FOR_LOCK,
                                       LockName::NEXT_INDEX_LOCK,
                                       LockName::LAST_APPLIED_LOCK,
                                       LockName::MATCH_INDEX_LOCK);

        return;
    }

    if(requestIdentifier == this->currentRequestBeingServiced) {
        std::stringstream msg;
        msg << "Continuing servicing of request " << requestIdentifier;
        this->logMsg(msg.str());

        unsigned int relevantEntryIndex = 0;
        for(unsigned int entryIndex = 0; entryIndex < this->log.size(); ++entryIndex) {
            if(this->log[entryIndex].requestIdentifier == requestIdentifier) {
                relevantEntryIndex = entryIndex;
                break;
            }
        }

        assert(relevantEntryIndex != 0);

        bool entryIsFromCurrentTerm = (this->log[relevantEntryIndex].term == this->currentTerm);

        std::vector<int> matchIndices;
        for(auto pair : this->matchIndex) {
            matchIndices.push_back(pair.second);
        }
        matchIndices.push_back(this->log.size()-1);

        _return.success = entryIsFromCurrentTerm &&
                                        areAMajorityGreaterThanOrEqual(matchIndices, relevantEntryIndex);

        msg.str("");
        msg << "Is this entry from this term? = " << entryIsFromCurrentTerm;
        this->logMsg(msg.str());

        msg.str("");
        msg << "Has the entry been successfully replicated on a majority of replicas " <<
                                 areAMajorityGreaterThanOrEqual(matchIndices, relevantEntryIndex);
        this->logMsg(msg.str());

        this->lockHandler.releaseLocks(LockName::STATE_LOCK,
                                       LockName::LEADER_LOCK,
                                       LockName::CURR_TERM_LOCK,
                                       LockName::LOG_LOCK,
                                       LockName::COMMIT_INDEX_LOCK,
                                       LockName::VOTED_FOR_LOCK,
                                       LockName::NEXT_INDEX_LOCK,
                                       LockName::LAST_APPLIED_LOCK,
                                       LockName::MATCH_INDEX_LOCK);

        return;
    }

    Entry newLogEntry;
    newLogEntry.key = key;
    newLogEntry.value = value;
    newLogEntry.term = this->currentTerm;
    newLogEntry.clientIdentifier = clientIdentifier;
    newLogEntry.requestIdentifier = requestIdentifier;
    this->log.push_back(newLogEntry);

    this->currentRequestBeingServiced = requestIdentifier;

    unsigned int numServersReplicatedOn = 1;

    for(const auto &id : this->clusterMembership) {
        std::shared_ptr<apache::thrift::transport::TSocket> socket(new apache::thrift::transport::TSocket(id.hostname, id.port));
        socket->setConnTimeout(atoi(dotenv::env[Replica::RPC_TIMEOUT_ENV_VAR_NAME].c_str()));
        socket->setSendTimeout(atoi(dotenv::env[Replica::RPC_TIMEOUT_ENV_VAR_NAME].c_str()));
        socket->setRecvTimeout(atoi(dotenv::env[Replica::RPC_TIMEOUT_ENV_VAR_NAME].c_str()));
        std::shared_ptr<apache::thrift::transport::TTransport> transport(new apache::thrift::transport::TBufferedTransport(socket));
        std::shared_ptr<apache::thrift::protocol::TProtocol> protocol(new apache::thrift::protocol::TBinaryProtocol(transport));
        ReplicaServiceClient client(protocol);

        try {
            transport->open();
        }
        catch(TTransportException& e) {
            std::stringstream msg;
            msg << "Error while opening a connection to append an entry to replica at " << id << ":" << e.getType();
            this->logMsg(msg.str());
            continue;
        }

        try {
            std::stringstream msg;
            msg << "Now sending an AppendEntry request to " << id << "; may take a while...";
            this->logMsg(msg.str());

            AppendEntryResponse appendEntryResponse;
            client.appendEntry(appendEntryResponse,
                               this->currentTerm,
                               this->myID,
                               this->log.size()-2,
                               this->log[this->log.size()-2].term,
                               newLogEntry,
                               this->commitIndex);

            if(appendEntryResponse.term > this->currentTerm) {
                this->currentTerm = appendEntryResponse.term;
                this->state = ReplicaState::FOLLOWER;
                this->votedFor = Replica::getNullID();
                _return.success = false;

                this->lockHandler.releaseLocks(LockName::STATE_LOCK,
                                               LockName::LEADER_LOCK,
                                               LockName::CURR_TERM_LOCK,
                                               LockName::LOG_LOCK,
                                               LockName::COMMIT_INDEX_LOCK,
                                               LockName::VOTED_FOR_LOCK,
                                               LockName::NEXT_INDEX_LOCK,
                                               LockName::LAST_APPLIED_LOCK,
                                               LockName::MATCH_INDEX_LOCK);

                return;
            }

            if(appendEntryResponse.success == false) {
                std::stringstream msg;
                msg << "AppendEntryRequest directed to " << id << " failed due to log inconsistency: Reducing next index value from " << this->nextIndex[id];
                this->logMsg(msg.str());

                assert(this->nextIndex[id] > 0);
                this->nextIndex[id] = std::max(1, this->nextIndex[id]-1);
            }
            else {
                std::stringstream msg;
                msg << "Entry successfully replicated on " << id << ": Now increasing replication aount from " <<
                        numServersReplicatedOn << " to " << (numServersReplicatedOn+1);
                this->logMsg(msg.str());

                this->matchIndex[id] = this->nextIndex[id];
                ++this->nextIndex[id];
                ++numServersReplicatedOn;
            }
        }
        catch(TTransportException& e) {
            if(e.getType() == TTransportException::TTransportExceptionType::TIMED_OUT) {
                std::stringstream msg;
                msg << "Timeout occurred while attempting to append entry to replica at " << id;
                this->logMsg(msg.str());

                Job retryJob = {this->log.size()-1, id.hostname, id.port};
                this->jobsToRetry.push(retryJob);
                continue;
            }

            throw e;
        }
    }

    _return.leaderID = this->leader;

    if(numServersReplicatedOn < ((this->clusterMembership.size() + 1) / 2) + 1) {
        std::stringstream msg;
        msg << "Entry unsuccessfully replicated on a majority of servers: replication amount = " <<
             numServersReplicatedOn  << "/" <<  ((this->clusterMembership.size() + 1 / 2) + 1);
        this->logMsg(msg.str());
        _return.success = false;
    }
    else {
        std::stringstream msg;
        msg << "Entry successfully replicated on a majority of servers and writing mapping " << key << " => " <<
                value << " to the state machine";
        this->logMsg(msg.str());

        this->stateMachine[key] = value;
        this->commitIndex = this->log.size()-1;
        this->lastApplied = this->log.size()-1;
    }

    this->lockHandler.releaseLocks(LockName::STATE_LOCK,
                                   LockName::LEADER_LOCK,
                                   LockName::CURR_TERM_LOCK,
                                   LockName::LOG_LOCK,
                                   LockName::COMMIT_INDEX_LOCK,
                                   LockName::VOTED_FOR_LOCK,
                                   LockName::NEXT_INDEX_LOCK,
                                   LockName::LAST_APPLIED_LOCK,
                                   LockName::MATCH_INDEX_LOCK);

    return;
}

void
Replica::kill() {
    std::stringstream idStream;
    idStream << this->myID.hostname;
    idStream << ":" << this->myID.port;
    idStream << " is now dying";

    this->logMsg(idStream.str());
    exit(0);
}

void
Replica::getInformation(std::map<std::string, std::string> & _return) {
    std::stringstream roleStream;
    std::stringstream termStream;
    std::stringstream indexStream;
    std::stringstream endpointStream;

    roleStream << this->state;
    termStream << this->currentTerm;
    indexStream << this->log.size();
    endpointStream << this->myID.hostname << ":" << this->myID.port;

    _return["endpoint"] = endpointStream.str();
    _return["role"] = roleStream.str();
    _return["term"] = termStream.str();
    _return["index"] = indexStream.str();
}

void
Replica::start() {
    std::cout << "starting operation...\n";
    if(!this->hasOperationStarted) {
        this->hasOperationStarted = true;
        this->lockHandler.unlockAll();
    }
}

void
Replica::timer() {
    std::this_thread::sleep_for(std::chrono::seconds(3));

    while(true) {
        this->lockHandler.acquireLocks(LockName::STATE_LOCK,
                                       LockName::LOG_LOCK,
                                       LockName::CURR_TERM_LOCK,
                                       LockName::TIMER_LOCK,
                                       LockName::COMMIT_INDEX_LOCK,
                                       LockName::VOTED_FOR_LOCK,
                                       LockName::LEADER_LOCK,
                                       LockName::NEXT_INDEX_LOCK,
                                       LockName::MATCH_INDEX_LOCK);

        if(this->state == ReplicaState::LEADER) {
            this->lockHandler.releaseLocks(LockName::STATE_LOCK,
                                           LockName::LOG_LOCK,
                                           LockName::CURR_TERM_LOCK,
                                           LockName::TIMER_LOCK,
                                           LockName::COMMIT_INDEX_LOCK,
                                           LockName::VOTED_FOR_LOCK,
                                           LockName::LEADER_LOCK,
                                           LockName::NEXT_INDEX_LOCK,
                                           LockName::MATCH_INDEX_LOCK);

            std::this_thread::sleep_for(std::chrono::milliseconds(300));
            continue;
        }

        if(this->timeLeft == 0) {
            this->logMsg("Time has expired!");

            unsigned int votesReceived = 1;
            this->state = ReplicaState::CANDIDATE;
            this->votedFor = this->myID;
            this->timeLeft = this->timeout;
            ++(this->currentTerm);

            for(auto const& id : this->clusterMembership) {
                std::stringstream msg;
                msg << "Now requesting vote from ";
                msg << id;
                this->logMsg(msg.str());
                std::shared_ptr<apache::thrift::transport::TSocket> socket(new apache::thrift::transport::TSocket(id.hostname, id.port));
                socket->setConnTimeout(atoi(dotenv::env[Replica::RPC_TIMEOUT_ENV_VAR_NAME].c_str()));
                socket->setSendTimeout(atoi(dotenv::env[Replica::RPC_TIMEOUT_ENV_VAR_NAME].c_str()));
                socket->setRecvTimeout(atoi(dotenv::env[Replica::RPC_TIMEOUT_ENV_VAR_NAME].c_str()));
                std::shared_ptr<apache::thrift::transport::TTransport> transport(new apache::thrift::transport::TBufferedTransport(socket));
                std::shared_ptr<apache::thrift::protocol::TProtocol> protocol(new apache::thrift::protocol::TBinaryProtocol(transport));
                ReplicaServiceClient client(protocol);

                try {
                    transport->open();

                    try {
                        Ballot ballot;

                        client.requestVote(ballot,
                                           this->currentTerm,
                                           this->myID,
                                           this->log.size(),
                                           this->log.back().term);

                        if(ballot.voteGranted) {
                            ++votesReceived;
                        }
                    }
                    catch(apache::thrift::transport::TTransportException& e) {
                        if(e.getType() == TTransportException::TTransportExceptionType::TIMED_OUT) {
                            msg.str("");
                            msg << "Timeout occurred while requesting a vote from " << id;
                            this->logMsg(msg.str());
                        }
                        else {
                            msg.str("");
                            msg << "Error while attempting to request a vote from " << id;
                            this->logMsg(msg.str());
                        }
                    }
                }
                catch(apache::thrift::transport::TTransportException& e) {
                    msg.str("");
                    msg << "Error while attempting to open a connection to request a vote from replica at " << id << ":" << e.getType();
                    this->logMsg(msg.str());
                }

                msg.str("");
                msg << votesReceived << " votes have been received during this election";
                this->logMsg(msg.str());

                if(votesReceived >= ((this->clusterMembership.size()+1) / 2) + 1) {
                    this->state = ReplicaState::LEADER;
                    this->leader = this->myID;
                    this->noopIndex = this->log.size();
                    Entry noopEntry;
                    noopEntry.key = "";
                    noopEntry.value = "";
                    noopEntry.term = this->currentTerm;
                    noopEntry.clientIdentifier = "";
                    noopEntry.requestIdentifier = 0;

                    this->log.push_back(noopEntry);

                    for(auto const& id : this->clusterMembership) {
                        this->nextIndex[id] = this->log.size()-1;
                        this->matchIndex[id] = 0;

                        std::shared_ptr<apache::thrift::transport::TSocket> socket(new apache::thrift::transport::TSocket(id.hostname, id.port));
                        socket->setConnTimeout(atoi(dotenv::env[Replica::RPC_TIMEOUT_ENV_VAR_NAME].c_str()));
                        socket->setSendTimeout(atoi(dotenv::env[Replica::RPC_TIMEOUT_ENV_VAR_NAME].c_str()));
                        socket->setRecvTimeout(atoi(dotenv::env[Replica::RPC_TIMEOUT_ENV_VAR_NAME].c_str()));
                        std::shared_ptr<apache::thrift::transport::TTransport> transport(new apache::thrift::transport::TBufferedTransport(socket));
                        std::shared_ptr<apache::thrift::protocol::TProtocol> protocol(new apache::thrift::protocol::TBinaryProtocol(transport));
                        ReplicaServiceClient client(protocol);

                        try {
                            transport->open();

                            try {
                                AppendEntryResponse appendEntryResponse;
                                auto it = this->log.end() - 2;
                                client.appendEntry(appendEntryResponse,
                                                   this->currentTerm,
                                                   this->myID,
                                                   this->log.size()-2,
                                                   it->term,
                                                   noopEntry,
                                                   this->commitIndex);

                                if(appendEntryResponse.term > this->currentTerm) {
                                    this->state = ReplicaState::FOLLOWER;
                                    this->currentTerm = appendEntryResponse.term;
                                    this->votedFor = Replica::getNullID();
                                }

                                if(appendEntryResponse.success == false) {
                                    msg.str("");
                                    msg << "AppendEntryRequest directed to " << id << " failed due to log inconsistency: Reducing nextIndex value from " << this->nextIndex[id];
                                    this->logMsg(msg.str());
                                    int possibleNewNextIndex = this->nextIndex[id]-1;
                                    this->nextIndex[id] = std::max(1, possibleNewNextIndex);
                                }
                                else {
                                    msg.str("");
                                    msg << "AppendEntryRequest containing no-op directed to " << id << " successful: Increasing nextIndex value from " << this->nextIndex[id];
                                    this->logMsg(msg.str());
                                    this->matchIndex[id] = this->nextIndex[id];
                                    ++this->nextIndex[id];
                                }
                            }
                            catch(TTransportException& e) {
                                if(e.getType() == TTransportException::TTransportExceptionType::TIMED_OUT) {
                                    msg.str("");
                                    msg << "Timeout occurred while asserting control of the replica at " << id;
                                    this->logMsg(msg.str());
                                }
                                else {
                                    msg.str("");
                                    msg << "Error while attempting to assert control of the replica at " << id;
                                    this->logMsg(msg.str());
                                }
                            }
                        }
                        catch(TTransportException& e) {
                            msg.str("");
                            msg << "Error while attempting to open a connection to assert control over the replica at " << id << ":" << e.getType();
                            this->logMsg(msg.str());
                        }
                    }
                    this->logMsg("I have asserted control of the cluster!");
                    break;
                }
            }

            this->timeLeft = this->timeout;
        }
        --this->timeLeft;

        this->lockHandler.releaseLocks(LockName::STATE_LOCK,
                                       LockName::LOG_LOCK,
                                       LockName::CURR_TERM_LOCK,
                                       LockName::TIMER_LOCK,
                                       LockName::COMMIT_INDEX_LOCK,
                                       LockName::VOTED_FOR_LOCK,
                                       LockName::LEADER_LOCK,
                                       LockName::NEXT_INDEX_LOCK,
                                       LockName::MATCH_INDEX_LOCK);

        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
}

void
Replica::heartbeatSender() {
    while(true) {
        this->lockHandler.acquireLocks(LockName::CURR_TERM_LOCK,
                                       LockName::LOG_LOCK,
                                       LockName::STATE_LOCK,
                                       LockName::COMMIT_INDEX_LOCK,
                                       LockName::VOTED_FOR_LOCK,
                                       LockName::NEXT_INDEX_LOCK,
                                       LockName::MATCH_INDEX_LOCK);

        if(this->state != ReplicaState::LEADER) {
            this->lockHandler.releaseLocks(LockName::CURR_TERM_LOCK,
                                           LockName::LOG_LOCK,
                                           LockName::STATE_LOCK,
                                           LockName::COMMIT_INDEX_LOCK,
                                           LockName::VOTED_FOR_LOCK,
                                           LockName::NEXT_INDEX_LOCK,
                                           LockName::MATCH_INDEX_LOCK);

            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            continue;
        }

        unsigned int old = this->commitIndex;
        this->commitIndex = this->findUpdatedCommitIndex();
        if(old != this->commitIndex) {
            std::stringstream msg;
            msg << "Commit index changed from " << old << " to " << this->commitIndex;
            this->logMsg(msg.str());
        }

        if(this->commitIndex > this->lastApplied) {
            auto applyEntry = [&](const Entry& entry) {
                if(!Replica::isAnEmptyEntry(entry)) {
                    this->stateMachine[entry.key] = entry.value;
                }
            };

            std::stringstream msg;
            msg << "Now applying log entry " << this->log[this->lastApplied+1] << " to state machine";
            this->logMsg(msg.str());

            ++this->lastApplied;
            applyEntry(this->log[this->lastApplied]);
        }

        for(const ID& id : this->clusterMembership) {
            std::shared_ptr<apache::thrift::transport::TSocket> socket(new apache::thrift::transport::TSocket(id.hostname, id.port));
            socket->setConnTimeout(atoi(dotenv::env[Replica::RPC_TIMEOUT_ENV_VAR_NAME].c_str()));
            socket->setSendTimeout(atoi(dotenv::env[Replica::RPC_TIMEOUT_ENV_VAR_NAME].c_str()));
            socket->setRecvTimeout(atoi(dotenv::env[Replica::RPC_TIMEOUT_ENV_VAR_NAME].c_str()));

            std::shared_ptr<apache::thrift::transport::TTransport> transport(new apache::thrift::transport::TBufferedTransport(socket));
            std::shared_ptr<apache::thrift::protocol::TProtocol> protocol(new apache::thrift::protocol::TBinaryProtocol(transport));
            ReplicaServiceClient client(protocol);

            Entry entryToSend = Replica::getEmptyLogEntry();
            unsigned int prevLogIndex = this->log.size()-1;
            unsigned int prevLogTerm = this->log.back().term;
            if(this->nextIndex[id] < this->log.size()) {
                unsigned int logIndexToSend = this->nextIndex[id];
                entryToSend = this->log[logIndexToSend];
                prevLogIndex = logIndexToSend-1;
                prevLogTerm = this->log[prevLogIndex].term;
            }

            try {
                transport->open();

                try {
                    AppendEntryResponse appendEntryResponse;
                    client.appendEntry(appendEntryResponse,
                                       this->currentTerm,
                                       this->myID,
                                       prevLogIndex,
                                       prevLogTerm,
                                       entryToSend,
                                       this->commitIndex);

                    if(appendEntryResponse.term > this->currentTerm) {
                        this->state = ReplicaState::FOLLOWER;
                        this->currentTerm = appendEntryResponse.term;
                        this->votedFor = Replica::getNullID();
                        break;
                    }

                    std::stringstream msg;

                    if(appendEntryResponse.success == false) {
                        msg.str("");
                        msg << "AppendEntryRequest directed to " << id << " failed due to log inconsistency: Reducing nextIndex value from " << this->nextIndex[id];
                        this->logMsg(msg.str());
                        int possibleNewNextIndex = this->nextIndex[id]-1;
                        this->nextIndex[id] = std::max(0, possibleNewNextIndex);
                    }
                    else if(!Replica::isAnEmptyEntry(entryToSend)) {
                        msg.str("");
                        msg << "AppendEntryRequest directed to " << id << " successful: Increasing nextIndex value from " << this->nextIndex[id];
                        this->logMsg(msg.str());
                        this->matchIndex[id] = this->nextIndex[id];
                        ++this->nextIndex[id];
                    }
                    else {
                        msg.str("");
                        msg << "AppendEntryRequest (heartbeat) directed to " << id << " successful";
                        this->logMsg(msg.str());
                    }
                }

                catch(TTransportException& e) {
                    std::stringstream msg;
                    if(e.getType() == TTransportException::TTransportExceptionType::TIMED_OUT) {
                        msg.str("");
                        msg << "Timeout occurred while sending a heartbeat to the replica at " << id;
                        this->logMsg(msg.str());
                    }
                    else {
                        msg.str("");
                        msg << "Error while attempting to send an AppendEntryRequest to the replica at " << id;
                        this->logMsg(msg.str());
                    }
                }
            }
            catch(TTransportException& e) {
                std::stringstream msg;
                msg << "Error while attempting to establish a connection to send an appendEntry request to " << id << " from heartbeat sender: " << e.getType();
                this->logMsg(msg.str());
            }
        }

        this->lockHandler.releaseLocks(LockName::CURR_TERM_LOCK,
                                       LockName::LOG_LOCK,
                                       LockName::STATE_LOCK,
                                       LockName::COMMIT_INDEX_LOCK,
                                       LockName::VOTED_FOR_LOCK,
                                       LockName::NEXT_INDEX_LOCK,
                                       LockName::MATCH_INDEX_LOCK);

        std::this_thread::sleep_for(std::chrono::milliseconds(this->heartbeatTick));
    }
}

void
Replica::retryRequest() {
    unsigned int maxTimeToSpendRetryingMS = atoi(dotenv::env[Replica::MIN_ELECTION_TIMEOUT_ENV_VAR_NAME].c_str());

    while(true) {
        while(this->jobsToRetry.empty()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }

        Job job = this->jobsToRetry.front();
        this->jobsToRetry.pop();

        unsigned int timeSpentOnCurrentRetryMS = 0;
        unsigned int timeoutMS = atoi(dotenv::env[Replica::RPC_RETRY_TIMEOUT_MIN_ENV_VAR_NAME].c_str());

        while(timeSpentOnCurrentRetryMS < (0.8 * maxTimeToSpendRetryingMS)) {
            std::this_thread::sleep_for(std::chrono::milliseconds(timeoutMS));

            this->lockHandler.acquireLocks(LockName::STATE_LOCK,
                                           LockName::LEADER_LOCK,
                                           LockName::CURR_TERM_LOCK,
                                           LockName::VOTED_FOR_LOCK,
                                           LockName::NEXT_INDEX_LOCK,
                                           LockName::MATCH_INDEX_LOCK,
                                           LockName::LOG_LOCK);

            Entry entry = this->log.at(job.entryPosition);

            if(this->state != ReplicaState::LEADER) {
                std::queue<Job> empty;
                std::swap(this->jobsToRetry, empty);

                this->lockHandler.releaseLocks(LockName::STATE_LOCK,
                                               LockName::LEADER_LOCK,
                                               LockName::CURR_TERM_LOCK,
                                               LockName::VOTED_FOR_LOCK,
                                               LockName::NEXT_INDEX_LOCK,
                                               LockName::MATCH_INDEX_LOCK,
                                               LockName::LOG_LOCK);

                break;
            }

            std::stringstream msg;
            msg << "Retrying AppendEntryRequest " << entry << " after " << timeoutMS << " ms.";
            this->logMsg(msg.str());

            ID targetID;
            targetID.hostname = job.targetHost;
            targetID.port = job.targetPort;

            std::shared_ptr<apache::thrift::transport::TSocket> socket(new apache::thrift::transport::TSocket(targetID.hostname, targetID.port));
            socket->setConnTimeout(atoi(dotenv::env[Replica::RPC_TIMEOUT_ENV_VAR_NAME].c_str()));
            socket->setSendTimeout(atoi(dotenv::env[Replica::RPC_TIMEOUT_ENV_VAR_NAME].c_str()));
            socket->setRecvTimeout(atoi(dotenv::env[Replica::RPC_TIMEOUT_ENV_VAR_NAME].c_str()));

            std::shared_ptr<apache::thrift::transport::TTransport> transport(new apache::thrift::transport::TBufferedTransport(socket));
            std::shared_ptr<apache::thrift::protocol::TProtocol> protocol(new apache::thrift::protocol::TBinaryProtocol(transport));
            ReplicaServiceClient client(protocol);

            try {
                transport->open();
            }
            catch(TTransportException& e) {
                std::stringstream msg;
                msg << "Error while attempting to open a connecton to retry an AppendEntryRequest to the replica at " << targetID;
                this->logMsg(msg.str());
            }

            try {
                AppendEntryResponse appendEntryResponse;
                client.appendEntry(appendEntryResponse,
                                   this->currentTerm,
                                   this->myID,
                                   job.entryPosition-1,
                                   this->log.at(job.entryPosition-1).term,
                                   entry,
                                   this->commitIndex);

                if(appendEntryResponse.term > this->currentTerm) {
                    this->currentTerm = appendEntryResponse.term;
                    this->state = ReplicaState::FOLLOWER;
                    this->votedFor = Replica::getNullID();
                    std::queue<Job> empty;
                    std::swap(this->jobsToRetry, empty);

                    this->lockHandler.releaseLocks(LockName::STATE_LOCK,
                                                   LockName::LEADER_LOCK,
                                                   LockName::CURR_TERM_LOCK,
                                                   LockName::VOTED_FOR_LOCK,
                                                   LockName::NEXT_INDEX_LOCK,
                                                   LockName::MATCH_INDEX_LOCK,
                                                   LockName::LOG_LOCK);

                    break;
                }

                if(appendEntryResponse.success == false) {
                    std::stringstream msg;
                    msg << "AppendEntryRequest retry to " << targetID << " failed due to log inconsistency: THIS SHOULD NEVER HAPPEN!";
                    this->logMsg(msg.str());

                    this->lockHandler.releaseLocks(LockName::STATE_LOCK,
                                                   LockName::LEADER_LOCK,
                                                   LockName::CURR_TERM_LOCK,
                                                   LockName::VOTED_FOR_LOCK,
                                                   LockName::NEXT_INDEX_LOCK,
                                                   LockName::MATCH_INDEX_LOCK,
                                                   LockName::LOG_LOCK);
                }
                else {
                    std::stringstream msg;
                    msg << "Entry successfully replicated on " << targetID  << " during retry: Now increasing nextIndex value from " <<
                                this->nextIndex.at(targetID) << " to " << (this->nextIndex.at(targetID)+1);
                    this->logMsg(msg.str());

                    this->matchIndex.at(targetID) = this->nextIndex.at(targetID);
                    ++this->nextIndex.at(targetID);

                    this->lockHandler.releaseLocks(LockName::STATE_LOCK,
                                                   LockName::LEADER_LOCK,
                                                   LockName::CURR_TERM_LOCK,
                                                   LockName::VOTED_FOR_LOCK,
                                                   LockName::NEXT_INDEX_LOCK,
                                                   LockName::MATCH_INDEX_LOCK,
                                                   LockName::LOG_LOCK);
                    break;
                }
            }
            catch(TTransportException& e) {
                std::stringstream msg;
                if(e.getType() == TTransportException::TTransportExceptionType::TIMED_OUT) {
                    msg.str("");
                    msg << "Timeout occurred while retrying an AppendEntryRequest to the replica at " << targetID;
                    this->logMsg(msg.str());
                }
                else {
                    msg.str("");
                    msg << "Non-Timeout error while attempting to retry an AppendEntryRequest to the replica at " << targetID;
                    this->logMsg(msg.str());
                }
            }

            timeSpentOnCurrentRetryMS += timeoutMS;
            timeoutMS = ((double) timeoutMS) * 1.5;
        }
    }
}

Entry
Replica::getEmptyLogEntry() {
    Entry emptyLogEntry;
    emptyLogEntry.key = "";
    emptyLogEntry.value = "";
    emptyLogEntry.term = -1;
    emptyLogEntry.clientIdentifier = "";
    emptyLogEntry.requestIdentifier = 0;

    return emptyLogEntry;
}

bool
Replica::isAnEmptyEntry(const Entry& entry) {
    return entry.key == "" &&
           entry.value == "" &&
           entry.term == -1 &&
           entry.clientIdentifier == "" &&
           entry.requestIdentifier == 0;
}

unsigned int
Replica::getElectionTimeout() {
    unsigned int minTimeMS = atoi(dotenv::env[Replica::MIN_ELECTION_TIMEOUT_ENV_VAR_NAME].c_str());
    unsigned int maxTimeMS = atoi(dotenv::env[Replica::MAX_ELECTION_TIMEOUT_ENV_VAR_NAME].c_str());

    srand(time(0));

    return (rand() % (maxTimeMS - minTimeMS)) + minTimeMS;
}

std::vector<ID>
Replica::getClusterMembership() {
    std::vector<ID> membership;
    std::ifstream infile("cluster.membership");
    std::string line;
    while(std::getline(infile, line)) {
        std::stringstream ss(line);
        std::string host;
        std::string portStr;

        getline(ss, host, ':');
        getline(ss, portStr, ':');

        ID id;
        id.hostname = host;
        id.port = atoi(portStr.c_str());

        membership.push_back(id);
    }

    return membership;
}

bool
Replica::isAtLeastAsUpToDateAs(unsigned int otherLastLogIndex,
                               unsigned int otherLastLogTerm,
                               unsigned int myLastLogIndex,
                               unsigned int myLastLogTerm) {

    return (otherLastLogTerm > myLastLogTerm) || \
                (otherLastLogTerm == myLastLogTerm && \
                        otherLastLogIndex >= myLastLogIndex);
}

void
Replica::logMsg(std::string message) {
    std::stringstream stateStream;
    std::stringstream logStream;
    std::stringstream stateMachineStream;
    std::stringstream termStream;

    stateStream << this->state;
    logStream << this->log;
    stateMachineStream << this->stateMachine;
    termStream << this->currentTerm;

    std::stringstream displayStr;
    displayStr << "{0} T={1} " << message << " {2} {3}";

    this->logger->info(displayStr.str(),
                       stateStream.str(),
                       termStream.str(),
                       logStream.str(),
                       stateMachineStream.str());
}

unsigned int
Replica::findUpdatedCommitIndex() {
    int possibleNewCommitIndex = this->log.size()-1;
    std::vector<int> indices;
    for(auto const& mapping : this->matchIndex) {
        indices.push_back(mapping.second);
    }
    indices.push_back(this->log.size()-1);

    while(possibleNewCommitIndex > this->commitIndex) {
        if(areAMajorityGreaterThanOrEqual(indices, possibleNewCommitIndex) &&
                      this->log[possibleNewCommitIndex].term == this->currentTerm) {
            return possibleNewCommitIndex;
        }

        --possibleNewCommitIndex;
    }

    return possibleNewCommitIndex;
}

ID
Replica::getNullID() {
    ID nullID;
    nullID.hostname = "";
    nullID.port = 0;

    return nullID;
}

bool
Replica::isANullID(const ID& id) {
    return id.hostname == "" && id.port == 0;
}

bool
ID::operator<(const ID& other) const {
    std::stringstream id1, id2;
    id1 << this->hostname << ":" << this->port;
    id2 << other.hostname << ":" << other.port;

    return id1.str() < id2.str();
}

int
main(int argc, char** argv) {
    if(argc != 2) {
        std::cerr << "Incorrect Usage: Try ./MyStore <port-number>\n";
    }

    unsigned int portToUse = atoi(argv[1]);

    std::shared_ptr<Replica> handler(new Replica(portToUse));
    std::shared_ptr<apache::thrift::TProcessor> processor(new ReplicaServiceProcessor(handler));
    std::shared_ptr<apache::thrift::transport::TServerTransport> serverTransport(new apache::thrift::transport::TServerSocket(portToUse));
    std::shared_ptr<apache::thrift::transport::TTransportFactory> transportFactory(new apache::thrift::transport::TBufferedTransportFactory());
    std::shared_ptr<apache::thrift::protocol::TProtocolFactory> protocolFactory(new apache::thrift::protocol::TBinaryProtocolFactory());

    apache::thrift::server::TThreadedServer server(processor, serverTransport, transportFactory, protocolFactory);
    server.serve();

    return 0;
}
