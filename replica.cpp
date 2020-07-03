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
#include <fstream>
#include <unordered_set>

#include "replica.hpp"
#include "lockhandler.hpp"
#include "locknames.hpp"

#include "dotenv.h"
#include "spdlog/spdlog.h"
#include "spdlog/sinks/basic_file_sink.h"
#include "spdlog/pattern_formatter.h"
#include "argparse.hpp"

#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TTransport.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TThreadedServer.h>

#include "gen-cpp/replicaservice_types.h"
#include "gen-cpp/ReplicaService.h"

#define NDEBUG

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
const char* Replica::SNAPSHOT_FILE_ENV_VAR_NAME = "SNAPSHOT_FILE";
const char* Replica::MAX_ALLOWED_LOG_SIZE_ENV_VAR_NAME = "MAX_ALLOWED_LOG_SIZE";

Replica::Replica(unsigned int port, const std::vector<std::string>& clusterSocketAddrs) : state(ReplicaState::FOLLOWER),
                                                                                          currentTerm(0),
                                                                                          commitIndex(0),
                                                                                          lastApplied(0),
                                                                                          timeout(Replica::getElectionTimeout()),
                                                                                          votedFor(Replica::getNullID()),
                                                                                          leader(Replica::getNullID()),
                                                                                          currentRequestBeingServiced(std::numeric_limits<unsigned int>::max()),
                                                                                          hasOperationStarted(false),
                                                                                          clusterMembership(Replica::getMemberIDs(clusterSocketAddrs)),
                                                                                          lockHandler(16),
                                                                                          noopIndex(0),
											  willingToVote(false) {

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

    spdlog::set_pattern("[%H:%M:%S:%e] %v");

    std::stringstream logFileNameStream;
    logFileNameStream << this->myID.hostname << ":" << this->myID.port << ".log";
    this->logger = spdlog::basic_logger_mt("file_logger", logFileNameStream.str());
    spdlog::flush_on(spdlog::level::info);
    spdlog::set_default_logger(this->logger);

    this->currentSnapshot.lastIncludedIndex = 0;
    this->currentSnapshot.lastIncludedTerm = -1;

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
                                   LockName::VOTED_FOR_LOCK,
                                   LockName::SNAPSHOT_LOCK);

    #ifndef NDEBUG
    std::stringstream msg;
    msg << candidateID << " is requesting my vote.";
    this->logMsg(msg.str());
    #endif

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
        #ifndef NDEBUG
        msg.str("");
        msg << "Granted vote to " << candidateID;
        this->logMsg(msg.str());
        #endif

        voteGranted = true;
        this->votedFor = candidateID;
    }

    this->lockHandler.releaseLocks(LockName::CURR_TERM_LOCK,
                                   LockName::LOG_LOCK,
                                   LockName::STATE_LOCK,
                                   LockName::VOTED_FOR_LOCK,
                                   LockName::SNAPSHOT_LOCK);

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
                                   LockName::COMMIT_INDEX_LOCK,
                                   LockName::SNAPSHOT_LOCK,
                                   LockName::CLUSTER_MEMBERSHIP_LOCK);

    #ifndef NDEBUG
    std::stringstream msg;
    msg <<  leaderID << " is appending " << entry << " to my log.";
    this->logMsg(msg.str());
    #endif

    if(term >= this->currentTerm) {
        this->timeLeft = this->timeout;
    }

    assert(prevLogIndex >= 0);
    assert(this->log.size() > 0);

    if((term < this->currentTerm) || ((unsigned) prevLogIndex >= this->log.size()) ||
                                                    (prevLogTerm != this->log.at(prevLogIndex).term)) {

        #ifndef NDEBUG
        msg.str("");
        msg << "Rejecting appendEntry request from (" << leaderID << "; leaderterm=" << term << ", myterm=" << this->currentTerm << ", prevLogIndex=" << prevLogIndex << ")";
        this->logMsg(msg.str());
        #endif

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
                                       LockName::COMMIT_INDEX_LOCK,
                                       LockName::SNAPSHOT_LOCK,
                                       LockName::CLUSTER_MEMBERSHIP_LOCK);

        return;
    }

    this->state = ReplicaState::FOLLOWER;

    unsigned int numEntriesToRemove = 0;
    for(int i = this->log.size()-1; i > prevLogIndex; --i) {
        ++numEntriesToRemove;
    }

    if(numEntriesToRemove > 0) {
        #ifndef NDEBUG
        msg.str("");
        msg << "Now removing " << numEntriesToRemove << " from the back of the log to bring it in line with the leader";
        this->logMsg(msg.str());
        #endif

        for(unsigned int i = 0; i < numEntriesToRemove; ++i) {
            this->log.pop_back();
        }
    }

    if(entry.type != EntryType::EMPTY_ENTRY) {
        #ifndef NDEBUG
        msg.str("");
        msg << "Now appending " << entry << " to the log";
        this->logMsg(msg.str());
        #endif

        this->log.push_back(entry);

        if(entry.type == EntryType::CONFIG_CHANGE_ENTRY) {
            this->nonVotingMembers = entry.nonVotingMembers;
            this->clusterMembership = entry.newConfiguration;
        }

        #ifndef NDEBUG
        if(this->log.size() >= (unsigned) atoi(dotenv::env[Replica::MAX_ALLOWED_LOG_SIZE_ENV_VAR_NAME].c_str())) {
            this->currentSnapshot = this->getSnapshot();
        }
        #endif
    }

    if(leaderCommit > this->commitIndex) {
        assert(this->log.size() > 0);
        this->commitIndex = std::min(leaderCommit, (int) this->log.size()-1);
    }

    auto applyEntry = [&](const Entry& entry) {
        if(entry.type == EntryType::SET_MAPPING_ENTRY) {
            if(this->stateMachine.find(entry.key) == this->stateMachine.end()) {
                std::vector<std::string> values(1, entry.value);
                this->stateMachine[entry.key] = values;
            }
            else {
                this->stateMachine[entry.key].push_back(entry.value);
            }
        }
        else if(entry.type == EntryType::DEL_MAPPING_ENTRY) {
            if(this->stateMachine.find(entry.key) != this->stateMachine.end()) {
                this->stateMachine.erase(entry.key);
            }
        }
    };

    if(this->commitIndex > this->lastApplied) {
        #ifndef NDEBUG
        msg.str("");
        msg << "Now applying log entry " << this->log.at(this->lastApplied+1) << " to state machine";
        this->logMsg(msg.str());
        #endif

        ++this->lastApplied;
        applyEntry(this->log.at(this->lastApplied));
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
                                   LockName::COMMIT_INDEX_LOCK,
                                   LockName::SNAPSHOT_LOCK,
                                   LockName::CLUSTER_MEMBERSHIP_LOCK);
}

void
Replica::get(GetResponse& _return, const std::string& key, const std::string& clientIdentifier, const int32_t requestIdentifier, const int32_t numPastMappings) {
    _return.success = true;

    this->lockHandler.acquireLocks(LockName::STATE_LOCK,
                                   LockName::LEADER_LOCK,
                                   LockName::CURR_TERM_LOCK,
                                   LockName::LOG_LOCK,
                                   LockName::COMMIT_INDEX_LOCK,
                                   LockName::VOTED_FOR_LOCK,
                                   LockName::MATCH_INDEX_LOCK,
                                   LockName::LATEST_NO_OP_LOG_INDEX,
                                   LockName::SNAPSHOT_LOCK,
                                   LockName::CLUSTER_MEMBERSHIP_LOCK);

    #ifndef NDEBUG
    std::stringstream msg;
    msg << this->myID << " now attempting to retrieve value associated with " << key;
    this->logMsg(msg.str());
    #endif

    if(this->state != ReplicaState::LEADER) {
        #ifndef NDEBUG
        std::stringstream msg;
        msg << "Was contacted to resolve a GET but am not the leader, redirected to " << this->leader;
        this->logMsg(msg.str());
        #endif

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
                                       LockName::LATEST_NO_OP_LOG_INDEX,
                                       LockName::SNAPSHOT_LOCK,
                                       LockName::CLUSTER_MEMBERSHIP_LOCK);

        return;
    }

    unsigned int numReplicasSuccessfullyContacted = 1;

    for(const auto &id : this->clusterMembership) {
        if(id == this->myID) {
            continue;
        }

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
                #ifndef NDEBUG
                msg.str("");
                msg << "Now sending a heartbeat to " << id;
                this->logMsg(msg.str());
                #endif

                AppendEntryResponse appendEntryResponse;

                client.appendEntry(appendEntryResponse,
                                   this->currentTerm,
                                   this->myID,
                                   this->log.size()-1,
                                   this->log.back().term,
                                   Replica::getEmptyLogEntry(),
                                   this->commitIndex);

                #ifndef NDEBUG
                msg.str("");
                msg << "Was AppendEntryRequest successful: " << appendEntryResponse.success;
                this->logMsg(msg.str());
                #endif

                if(appendEntryResponse.term > this->currentTerm) {
                    this->state = ReplicaState::FOLLOWER;
                    this->currentTerm = appendEntryResponse.term;
                    this->votedFor = Replica::getNullID();
                    _return.success = false;

                    #ifndef NDEBUG
                    msg.str("");
                    msg << "Early exit: Larger term encountered";
                    this->logMsg(msg.str());
                    #endif

                    this->lockHandler.releaseLocks(LockName::STATE_LOCK,
                                                   LockName::LEADER_LOCK,
                                                   LockName::CURR_TERM_LOCK,
                                                   LockName::LOG_LOCK,
                                                   LockName::COMMIT_INDEX_LOCK,
                                                   LockName::VOTED_FOR_LOCK,
                                                   LockName::MATCH_INDEX_LOCK,
                                                   LockName::LATEST_NO_OP_LOG_INDEX,
                                                   LockName::SNAPSHOT_LOCK,
                                                   LockName::CLUSTER_MEMBERSHIP_LOCK);

                    return;
                }

                if(!appendEntryResponse.success) {
                    _return.success = false;

                    #ifndef NDEBUG
                    msg.str("");
                    msg << "Early exit: appendEntryResponse not successful";
                    this->logMsg(msg.str());
                    #endif

                    this->lockHandler.releaseLocks(LockName::STATE_LOCK,
                                                   LockName::LEADER_LOCK,
                                                   LockName::CURR_TERM_LOCK,
                                                   LockName::LOG_LOCK,
                                                   LockName::COMMIT_INDEX_LOCK,
                                                   LockName::VOTED_FOR_LOCK,
                                                   LockName::MATCH_INDEX_LOCK,
                                                   LockName::LATEST_NO_OP_LOG_INDEX,
                                                   LockName::SNAPSHOT_LOCK,
                                                   LockName::CLUSTER_MEMBERSHIP_LOCK);

                    return;
                }

                if(this->nonVotingMembers.find(id) == this->nonVotingMembers.end()) {
                    ++numReplicasSuccessfullyContacted;
                }
            }
            catch(TTransportException& e) {
                #ifndef NDEBUG
                if(e.getType() == TTransportException::TTransportExceptionType::TIMED_OUT) {
                    msg.str("");
                    msg << "Timeout occurred while attempting to send a heartbeat to replica at " << id;
                    this->logMsg(msg.str());
                }
                else {
                    msg.str("");
                    msg << "Error while attempting to send a heartbeat to replica at " << id;
                    this->logMsg(msg.str());
                }
                #endif
            }
        }
        catch(TTransportException& e) {
            #ifndef NDEBUG
            msg.str("");
            msg << "Error while attempting to open a connection to send an empty AppendEntry request to replica at " << id << ":" << e.getType();
            this->logMsg(msg.str());
            #endif
        }
    }

    if(numReplicasSuccessfullyContacted < (this->clusterMembership.size() / 2) + 1) {
        _return.success = false;

        #ifndef NDEBUG
        msg.str("");
        msg << "Early exit: replication level needed not reached";
        this->logMsg(msg.str());
        #endif

        this->lockHandler.releaseLocks(LockName::STATE_LOCK,
                                       LockName::LEADER_LOCK,
                                       LockName::CURR_TERM_LOCK,
                                       LockName::LOG_LOCK,
                                       LockName::COMMIT_INDEX_LOCK,
                                       LockName::VOTED_FOR_LOCK,
                                       LockName::MATCH_INDEX_LOCK,
                                       LockName::LATEST_NO_OP_LOG_INDEX,
                                       LockName::SNAPSHOT_LOCK,
                                       LockName::CLUSTER_MEMBERSHIP_LOCK);

        return;
    }

    std::vector<int> matchIndices;
    for(auto pair : this->matchIndex) {
        matchIndices.push_back(pair.second);
    }
    matchIndices.push_back(this->log.size()-1);

    if(areAMajorityGreaterThanOrEqual(matchIndices, this->noopIndex)) {
        if(this->stateMachine.find(key) != this->stateMachine.end()) {
            _return.values.clear();
            auto pastMappingIter = this->stateMachine.at(key).end() - 1;
            _return.values.push_back(*pastMappingIter);
            int numPastMappingsProvided = 1;
            while(pastMappingIter != this->stateMachine.at(key).begin() && numPastMappingsProvided < numPastMappings+1) {
                --pastMappingIter;
                ++numPastMappingsProvided;
                _return.values.push_back(*pastMappingIter);
            }
        }
        else {
            _return.values.push_back("");
        }
    }

    this->lockHandler.releaseLocks(LockName::STATE_LOCK,
                                   LockName::LEADER_LOCK,
                                   LockName::CURR_TERM_LOCK,
                                   LockName::LOG_LOCK,
                                   LockName::COMMIT_INDEX_LOCK,
                                   LockName::VOTED_FOR_LOCK,
                                   LockName::MATCH_INDEX_LOCK,
                                   LockName::LATEST_NO_OP_LOG_INDEX,
                                   LockName::SNAPSHOT_LOCK,
                                   LockName::CLUSTER_MEMBERSHIP_LOCK);

}

void
Replica::deletekey(DelResponse& _return, const std::string& key, const std::string& clientIdentifier, const int32_t requestIdentifier) {
    this->lockHandler.acquireLocks(LockName::STATE_LOCK,
                                   LockName::LEADER_LOCK,
                                   LockName::CURR_TERM_LOCK,
                                   LockName::LOG_LOCK,
                                   LockName::COMMIT_INDEX_LOCK,
                                   LockName::VOTED_FOR_LOCK,
                                   LockName::NEXT_INDEX_LOCK,
                                   LockName::LAST_APPLIED_LOCK,
                                   LockName::MATCH_INDEX_LOCK,
                                   LockName::SNAPSHOT_LOCK,
                                   LockName::CLUSTER_MEMBERSHIP_LOCK);

    _return.success = true;

    #ifndef NDEBUG
    std::stringstream msg;
    msg << this->myID << " now attempting to delete " << key;
    this->logMsg(msg.str());
    #endif

    if(this->state != ReplicaState::LEADER) {
        #ifndef NDEBUG
        msg.str("");
        msg << "Was contacted to resolve a DEL but am not the leader, redirected to " << this-> leader;
        this->logMsg(msg.str());
        #endif

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
                                       LockName::MATCH_INDEX_LOCK,
                                       LockName::SNAPSHOT_LOCK,
                                       LockName::CLUSTER_MEMBERSHIP_LOCK);

        return;
    }

    if(requestIdentifier == this->currentRequestBeingServiced) {
        #ifndef NDEBUG
        msg.str("");
        msg << "Continuing servicing of request " << requestIdentifier;
        this->logMsg(msg.str());
        #endif

        unsigned int relevantEntryIndex = 0;
        for(unsigned int entryIndex = 0; entryIndex < this->log.size(); ++entryIndex) {
            if(this->log.at(entryIndex).requestIdentifier == requestIdentifier) {
                relevantEntryIndex = entryIndex;
                break;
            }
        }

        assert(relevantEntryIndex != 0);

        bool entryIsFromCurrentTerm = (this->log.at(relevantEntryIndex).term == this->currentTerm);

        std::vector<int> matchIndices;
        for(auto pair : this->matchIndex) {
            matchIndices.push_back(pair.second);
        }
        matchIndices.push_back(this->log.size()-1);

        _return.success = entryIsFromCurrentTerm &&
                                        areAMajorityGreaterThanOrEqual(matchIndices, relevantEntryIndex);

        #ifndef NDEBUG
        msg.str("");
        msg << "Is this entry from this term? = " << entryIsFromCurrentTerm;
        this->logMsg(msg.str());

        msg.str("");
        msg << "Has the entry been successfully replicated on a majority of replicas " <<
                                 areAMajorityGreaterThanOrEqual(matchIndices, relevantEntryIndex);
        this->logMsg(msg.str());
        #endif

        this->lockHandler.releaseLocks(LockName::STATE_LOCK,
                                       LockName::LEADER_LOCK,
                                       LockName::CURR_TERM_LOCK,
                                       LockName::LOG_LOCK,
                                       LockName::COMMIT_INDEX_LOCK,
                                       LockName::VOTED_FOR_LOCK,
                                       LockName::NEXT_INDEX_LOCK,
                                       LockName::LAST_APPLIED_LOCK,
                                       LockName::MATCH_INDEX_LOCK,
                                       LockName::SNAPSHOT_LOCK,
                                       LockName::CLUSTER_MEMBERSHIP_LOCK);

        return;
    }

    Entry newLogEntry;
    newLogEntry.type = EntryType::DEL_MAPPING_ENTRY;
    newLogEntry.key = key;
    newLogEntry.term = this->currentTerm;
    newLogEntry.clientIdentifier = clientIdentifier;
    newLogEntry.requestIdentifier = requestIdentifier;
    this->log.push_back(newLogEntry);

    #ifndef NDEBUG
    if(this->log.size() >= (unsigned) atoi(dotenv::env[Replica::MAX_ALLOWED_LOG_SIZE_ENV_VAR_NAME].c_str())) {
        this->currentSnapshot = this->getSnapshot();
    }
    #endif

    this->currentRequestBeingServiced = requestIdentifier;

    unsigned int numServersReplicatedOn = 1;

    for(const auto &id : this->clusterMembership) {
        if(id == this->myID) {
            continue;
        }

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
            #ifndef NDEBUG
            msg.str("");
            msg << "Error while opening a connection to append an entry to replica at " << id << ":" << e.getType();
            this->logMsg(msg.str());
            #endif
            continue;
        }

        try {
            #ifndef NDEBUG
            msg.str("");
            msg << "Now sending an AppendEntry request to " << id << "; may take a while...";
            this->logMsg(msg.str());
            #endif

            AppendEntryResponse appendEntryResponse;
            client.appendEntry(appendEntryResponse,
                               this->currentTerm,
                               this->myID,
                               this->log.size()-2,
                               this->log.at(this->log.size()-2).term,
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
                                               LockName::MATCH_INDEX_LOCK,
                                               LockName::SNAPSHOT_LOCK,
                                               LockName::CLUSTER_MEMBERSHIP_LOCK);

                return;
            }

            if(!appendEntryResponse.success) {
                #ifndef NDEBUG
                msg.str("");
                msg << "AppendEntryRequest directed to " << id << " failed due to log inconsistency: Reducing next index value from " << this->nextIndex.at(id);
                this->logMsg(msg.str());
                #endif

                assert(this->nextIndex.at(id) > 0);
                this->nextIndex.at(id) = std::max(1, this->nextIndex.at(id)-1);
            }
            else {
                #ifndef NDEBUG
                msg.str("");
                msg << "Entry successfully replicated on " << id << ": Now increasing replication aount from " <<
                        numServersReplicatedOn << " to " << (numServersReplicatedOn+1);
                this->logMsg(msg.str());
                #endif

                this->matchIndex[id] = this->nextIndex.at(id);
                ++this->nextIndex.at(id);

                if(this->nonVotingMembers.find(id) == this->nonVotingMembers.end()) {
                    ++numServersReplicatedOn;
                }
            }
        }
        catch(TTransportException& e) {
            if(e.getType() == TTransportException::TTransportExceptionType::TIMED_OUT) {
                #ifndef NDEBUG
                msg.str("");
                msg << "Timeout occurred while attempting to append entry to replica at " << id;
                this->logMsg(msg.str());
                #endif

                Job retryJob = {(int) this->log.size()-1, id.hostname, id.port};
                this->jobsToRetry.push(retryJob);
                continue;
            }

            throw e;
        }
    }

    _return.leaderID = this->leader;

    if(numServersReplicatedOn < (this->clusterMembership.size() / 2) + 1) {
        #ifndef NDEBUG
        msg.str("");
        msg << "Entry unsuccessfully replicated on a majority of servers: replication amount = " <<
             numServersReplicatedOn  << "/" <<  ((this->clusterMembership.size() + 1 / 2) + 1);
        this->logMsg(msg.str());
        #endif

        _return.success = false;
    }
    else {
        #ifndef NDEBUG
        msg.str("");
        msg << "Entry successfully replicated on a majority of servers and removing mapping with key: " << key;
        this->logMsg(msg.str());
        #endif

        if(this->stateMachine.find(key) != this->stateMachine.end()) {
            this->stateMachine.erase(key);
        }

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
                                   LockName::MATCH_INDEX_LOCK,
                                   LockName::SNAPSHOT_LOCK,
                                   LockName::CLUSTER_MEMBERSHIP_LOCK);

    return;
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
                                   LockName::MATCH_INDEX_LOCK,
                                   LockName::SNAPSHOT_LOCK,
                                   LockName::CLUSTER_MEMBERSHIP_LOCK);

    _return.success = true;

    #ifndef NDEBUG
    std::stringstream msg;
    msg << this->myID << " now attempting to associate " << key << " with " << value;
    this->logMsg(msg.str());
    #endif

    if(this->state != ReplicaState::LEADER) {
        #ifndef NDEBUG
        msg.str("");
        msg << "Was contacted to resolve a PUT but am not the leader, redirected to " << this-> leader;
        this->logMsg(msg.str());
        #endif

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
                                       LockName::MATCH_INDEX_LOCK,
                                       LockName::SNAPSHOT_LOCK,
                                       LockName::CLUSTER_MEMBERSHIP_LOCK);

        return;
    }

    if(requestIdentifier == this->currentRequestBeingServiced) {
        #ifndef NDEBUG
        msg.str("");
        msg << "Continuing servicing of request " << requestIdentifier;
        this->logMsg(msg.str());
        #endif

        unsigned int relevantEntryIndex = 0;
        for(unsigned int entryIndex = 0; entryIndex < this->log.size(); ++entryIndex) {
            if(this->log.at(entryIndex).requestIdentifier == requestIdentifier) {
                relevantEntryIndex = entryIndex;
                break;
            }
        }

        assert(relevantEntryIndex != 0);

        bool entryIsFromCurrentTerm = (this->log.at(relevantEntryIndex).term == this->currentTerm);

        std::vector<int> matchIndices;
        for(auto pair : this->matchIndex) {
            matchIndices.push_back(pair.second);
        }
        matchIndices.push_back(this->log.size()-1);

        _return.success = entryIsFromCurrentTerm &&
                                        areAMajorityGreaterThanOrEqual(matchIndices, relevantEntryIndex);

        #ifndef NDEBUG
        msg.str("");
        msg << "Is this entry from this term? = " << entryIsFromCurrentTerm;
        this->logMsg(msg.str());

        msg.str("");
        msg << "Has the entry been successfully replicated on a majority of replicas " <<
                                 areAMajorityGreaterThanOrEqual(matchIndices, relevantEntryIndex);
        this->logMsg(msg.str());
        #endif

        this->lockHandler.releaseLocks(LockName::STATE_LOCK,
                                       LockName::LEADER_LOCK,
                                       LockName::CURR_TERM_LOCK,
                                       LockName::LOG_LOCK,
                                       LockName::COMMIT_INDEX_LOCK,
                                       LockName::VOTED_FOR_LOCK,
                                       LockName::NEXT_INDEX_LOCK,
                                       LockName::LAST_APPLIED_LOCK,
                                       LockName::MATCH_INDEX_LOCK,
                                       LockName::SNAPSHOT_LOCK,
                                       LockName::CLUSTER_MEMBERSHIP_LOCK);

        return;
    }

    Entry newLogEntry;
    newLogEntry.type = EntryType::SET_MAPPING_ENTRY;
    newLogEntry.key = key;
    newLogEntry.value = value;
    newLogEntry.term = this->currentTerm;
    newLogEntry.clientIdentifier = clientIdentifier;
    newLogEntry.requestIdentifier = requestIdentifier;
    this->log.push_back(newLogEntry);

    #ifndef NDEBUG
    if(this->log.size() >= (unsigned) atoi(dotenv::env[Replica::MAX_ALLOWED_LOG_SIZE_ENV_VAR_NAME].c_str())) {
        this->currentSnapshot = this->getSnapshot();
    }
    #endif

    this->currentRequestBeingServiced = requestIdentifier;

    unsigned int numServersReplicatedOn = 1;

    for(const auto &id : this->clusterMembership) {
        if(id == this->myID) {
            continue;
        }

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
            #ifndef NDEBUG
            msg.str("");
            msg << "Error while opening a connection to append an entry to replica at " << id << ":" << e.getType();
            this->logMsg(msg.str());
            #endif
            continue;
        }

        try {
            #ifndef NDEBUG
            msg.str("");
            msg << "Now sending an AppendEntry request to " << id << "; may take a while...";
            this->logMsg(msg.str());
            #endif

            AppendEntryResponse appendEntryResponse;
            client.appendEntry(appendEntryResponse,
                               this->currentTerm,
                               this->myID,
                               this->log.size()-2,
                               this->log.at(this->log.size()-2).term,
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
                                               LockName::MATCH_INDEX_LOCK,
                                               LockName::SNAPSHOT_LOCK,
                                               LockName::CLUSTER_MEMBERSHIP_LOCK);

                return;
            }

            if(!appendEntryResponse.success) {
                #ifndef NDEBUG
                msg.str("");
                msg << "AppendEntryRequest directed to " << id << " failed due to log inconsistency: Reducing next index value from " << this->nextIndex.at(id);
                this->logMsg(msg.str());
                #endif

                assert(this->nextIndex.at(id) > 0);
                this->nextIndex.at(id) = std::max(1, this->nextIndex.at(id)-1);
            }
            else {
                #ifndef NDEBUG
                msg.str("");
                msg << "Entry successfully replicated on " << id << ": Now increasing replication aount from " <<
                        numServersReplicatedOn << " to " << (numServersReplicatedOn+1);
                this->logMsg(msg.str());
                #endif

                this->matchIndex[id] = this->nextIndex.at(id);
                ++this->nextIndex.at(id);

                if(this->nonVotingMembers.find(id) == this->nonVotingMembers.end()) {
                    ++numServersReplicatedOn;
                }
            }
        }
        catch(TTransportException& e) {
            if(e.getType() == TTransportException::TTransportExceptionType::TIMED_OUT) {
                #ifndef NDEBUG
                msg.str("");
                msg << "Timeout occurred while attempting to append entry to replica at " << id;
                this->logMsg(msg.str());
                #endif

                Job retryJob = {(int) this->log.size()-1, id.hostname, id.port};
                this->jobsToRetry.push(retryJob);
                continue;
            }

            throw e;
        }
    }

    _return.leaderID = this->leader;

    if(numServersReplicatedOn < ((this->clusterMembership.size()-this->nonVotingMembers.size()) / 2) + 1) {
        #ifndef NDEBUG
        msg.str("");
        msg << "Entry unsuccessfully replicated on a majority of servers: replication amount = " <<
             numServersReplicatedOn  << "/" <<  ((this->clusterMembership.size() + 1 / 2) + 1);
        this->logMsg(msg.str());
        #endif

        _return.success = false;
    }
    else {
        #ifndef NDEBUG
        msg.str("");
        msg << "Entry successfully replicated on a majority of servers and writing mapping " << key << " => " <<
                value << " to the state machine";
        this->logMsg(msg.str());
        #endif

        if(this->stateMachine.find(key) == this->stateMachine.end()) {
            std::vector<std::string> values(1, value);
            this->stateMachine[key] = values;
        }
        else if(this->stateMachine.at(key).back() != value){
            this->stateMachine[key].push_back(value);
        }

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
                                   LockName::MATCH_INDEX_LOCK,
                                   LockName::SNAPSHOT_LOCK,
                                   LockName::CLUSTER_MEMBERSHIP_LOCK);

    return;
}

void
Replica::kill() {
    std::stringstream idStream;
    idStream << this->myID.hostname;
    idStream << ":" << this->myID.port;
    idStream << " is now dying";

    #ifndef NDEBUG
    this->logMsg(idStream.str());
    #endif

    exit(0);
}

void
Replica::getInformationHelper(GetInformationHelperResponse & _return, const int32_t term) {
    _return.success = true;

    if(term < this->currentTerm) {
        _return.success = false;
        _return.term = this->currentTerm;

        return;
    }

    std::stringstream roleStream;
    std::stringstream termStream;
    std::stringstream indexStream;
    std::stringstream endpointStream;

    roleStream << this->state;
    termStream << this->currentTerm;
    indexStream << this->log.size();
    endpointStream << this->myID.hostname << ":" << this->myID.port;

    _return.replicaInformation["endpoint"] = endpointStream.str();
    _return.replicaInformation["role"] = roleStream.str();
    _return.replicaInformation["term"] = termStream.str();
    _return.replicaInformation["index"] = indexStream.str();
}

void
Replica::getInformation(GetInformationResponse& _return) {
    this->lockHandler.acquireLocks(LockName::STATE_LOCK,
                                   LockName::CURR_TERM_LOCK,
                                   LockName::CLUSTER_MEMBERSHIP_LOCK,
                                   LockName::LEADER_LOCK);

    _return.success = true;

    if(this->state != ReplicaState::LEADER) {
        _return.success = false;
        if(!Replica::isANullID(this->leader)) {
            _return.leaderID = this->leader;
        }

        this->lockHandler.releaseLocks(LockName::STATE_LOCK,
                                       LockName::CURR_TERM_LOCK,
                                       LockName::CLUSTER_MEMBERSHIP_LOCK,
                                       LockName::LEADER_LOCK);

        return;
    }

    // Add leader information to _return
    std::stringstream roleStream;
    std::stringstream termStream;
    std::stringstream indexStream;
    std::stringstream endpointStream;

    roleStream << this->state;
    termStream << this->currentTerm;
    indexStream << this->log.size();
    endpointStream << this->myID.hostname << ":" << this->myID.port;

    _return.clusterInformation[this->myID]["endpoint"] = endpointStream.str();
    _return.clusterInformation[this->myID]["role"] = roleStream.str();
    _return.clusterInformation[this->myID]["term"] = termStream.str();
    _return.clusterInformation[this->myID]["index"] = indexStream.str();

    for(const ID& id : this->clusterMembership) {
        if(id == this->myID) {
            continue;
        }

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
            #ifndef NDEBUG
            msg.str("");
            msg << "Error while opening a connection to get information from replica at " << id << ":" << e.getType();
            this->logMsg(msg.str());
            #endif

            _return.clusterInformation[id]["endpoint"] = "N/A";
            _return.clusterInformation[id]["role"] = "N/A";
            _return.clusterInformation[id]["term"] = "N/A";
            _return.clusterInformation[id]["index"] = "N/A";

            continue;
        }

        try {
            #ifndef NDEBUG
            msg.str("");
            msg << "Now sending a getInformationHelper request to " << id << "; may take a while...";
            this->logMsg(msg.str());
            #endif

            GetInformationHelperResponse getInformationHelperResponse;
            client.getInformationHelper(getInformationHelperResponse, this->currentTerm);

            if(getInformationHelperResponse.term > this->currentTerm) {
                this->currentTerm = getInformationHelperResponse.term;
                this->state = ReplicaState::FOLLOWER;
                this->votedFor = Replica::getNullID();
                _return.success = false;

                this->lockHandler.releaseLocks(LockName::STATE_LOCK,
                                               LockName::LEADER_LOCK,
                                               LockName::CURR_TERM_LOCK,
                                               LockName::CLUSTER_MEMBERSHIP_LOCK);

                return;
            }

            assert(getInformationHelperResponse.success);

            _return.clusterInformation[id] = getInformationHelperResponse.replicaInformation;
        }
        catch(TTransportException& e) {
            _return.clusterInformation[id]["endpoint"] = "N/A";
            _return.clusterInformation[id]["role"] = "N/A";
            _return.clusterInformation[id]["term"] = "N/A";
            _return.clusterInformation[id]["index"] = "N/A";
        }
    }

    this->lockHandler.releaseLocks(LockName::STATE_LOCK,
                                   LockName::CURR_TERM_LOCK,
                                   LockName::CLUSTER_MEMBERSHIP_LOCK,
                                   LockName::LEADER_LOCK);
}

void
Replica::timer() {
    std::this_thread::sleep_for(std::chrono::seconds(3));

    std::stringstream msg;

    while(true) {
        this->lockHandler.acquireLocks(LockName::STATE_LOCK,
                                       LockName::LOG_LOCK,
                                       LockName::CURR_TERM_LOCK,
                                       LockName::TIMER_LOCK,
                                       LockName::COMMIT_INDEX_LOCK,
                                       LockName::VOTED_FOR_LOCK,
                                       LockName::LEADER_LOCK,
                                       LockName::NEXT_INDEX_LOCK,
                                       LockName::MATCH_INDEX_LOCK,
                                       LockName::SNAPSHOT_LOCK,
                                       LockName::CLUSTER_MEMBERSHIP_LOCK);

        if(this->state == ReplicaState::LEADER) {
            this->lockHandler.releaseLocks(LockName::STATE_LOCK,
                                           LockName::LOG_LOCK,
                                           LockName::CURR_TERM_LOCK,
                                           LockName::TIMER_LOCK,
                                           LockName::COMMIT_INDEX_LOCK,
                                           LockName::VOTED_FOR_LOCK,
                                           LockName::LEADER_LOCK,
                                           LockName::NEXT_INDEX_LOCK,
                                           LockName::MATCH_INDEX_LOCK,
                                           LockName::SNAPSHOT_LOCK,
                                           LockName::CLUSTER_MEMBERSHIP_LOCK);

            std::this_thread::sleep_for(std::chrono::milliseconds(300));
            continue;
        }

        if(this->timeLeft == 0) {
            #ifndef NDEBUG
            this->logMsg("Time has expired!");
            #endif

            unsigned int votesReceived = 1;
            this->state = ReplicaState::CANDIDATE;
            this->votedFor = this->myID;
            this->timeLeft = this->timeout;
            ++(this->currentTerm);

            for(auto const& id : this->clusterMembership) {
                if(id == this->myID) {
                    continue;
                }

                #ifndef NDEBUG
                msg.str("");
                msg << "Now requesting vote from ";
                msg << id;
                this->logMsg(msg.str());
                #endif

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
                        #ifndef NDEBUG
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
                        #endif
                    }
                }
                catch(apache::thrift::transport::TTransportException& e) {
                    #ifndef NDEBUG
                    msg.str("");
                    msg << "Error while attempting to open a connection to request a vote from replica at " << id << ":" << e.getType();
                    this->logMsg(msg.str());
                    #endif
                }

                #ifndef NDEBUG
                msg.str("");
                msg << votesReceived << " votes have been received during this election";
                this->logMsg(msg.str());
                #endif

                if(votesReceived >= (this->clusterMembership.size() / 2) + 1) {
                    this->state = ReplicaState::LEADER;
                    this->leader = this->myID;
                    this->noopIndex = this->log.size();
                    Entry noopEntry;
                    noopEntry.type = EntryType::NOOP_ENTRY;
                    noopEntry.key = "";
                    noopEntry.value = "";
                    noopEntry.term = this->currentTerm;
                    noopEntry.clientIdentifier = "";
                    noopEntry.requestIdentifier = std::numeric_limits<int>::max();

                    this->log.push_back(noopEntry);

                    for(auto const& id : this->clusterMembership) {
                        if(id == this->myID) {
                            continue;
                        }

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

                                if(!appendEntryResponse.success) {
                                    #ifndef NDEBUG
                                    msg.str("");
                                    msg << "AppendEntryRequest directed to " << id << " failed due to log inconsistency: Reducing nextIndex value from " << this->nextIndex.at(id);
                                    this->logMsg(msg.str());
                                    #endif

                                    int possibleNewNextIndex = this->nextIndex.at(id)-1;
                                    this->nextIndex[id] = std::max(1, possibleNewNextIndex);
                                }
                                else {
                                    #ifndef NDEBUG
                                    msg.str("");
                                    msg << "AppendEntryRequest containing no-op directed to " << id << " successful: Increasing nextIndex value from " << this->nextIndex.at(id);
                                    this->logMsg(msg.str());
                                    #endif

                                    this->matchIndex[id] = this->nextIndex.at(id);
                                    ++this->nextIndex.at(id);
                                }
                            }
                            catch(TTransportException& e) {
                                #ifndef NDEBUG
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
                                #endif
                            }
                        }
                        catch(TTransportException& e) {
                            #ifndef NDEBUG
                            msg.str("");
                            msg << "Error while attempting to open a connection to assert control over the replica at " << id << ":" << e.getType();
                            this->logMsg(msg.str());
                            #endif
                        }
                    }

                    #ifndef NDEBUG
                    this->logMsg("I have asserted control of the cluster!");
                    #endif

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
                                       LockName::MATCH_INDEX_LOCK,
                                       LockName::SNAPSHOT_LOCK,
                                       LockName::CLUSTER_MEMBERSHIP_LOCK);

        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
}

void
Replica::heartbeatSender() {
    std::stringstream msg;

    while(true) {
        this->lockHandler.acquireLocks(LockName::CURR_TERM_LOCK,
                                       LockName::LOG_LOCK,
                                       LockName::STATE_LOCK,
                                       LockName::COMMIT_INDEX_LOCK,
                                       LockName::VOTED_FOR_LOCK,
                                       LockName::NEXT_INDEX_LOCK,
                                       LockName::MATCH_INDEX_LOCK,
                                       LockName::SNAPSHOT_LOCK,
                                       LockName::CLUSTER_MEMBERSHIP_LOCK);

        if(this->state != ReplicaState::LEADER) {
            this->lockHandler.releaseLocks(LockName::CURR_TERM_LOCK,
                                           LockName::LOG_LOCK,
                                           LockName::STATE_LOCK,
                                           LockName::COMMIT_INDEX_LOCK,
                                           LockName::VOTED_FOR_LOCK,
                                           LockName::NEXT_INDEX_LOCK,
                                           LockName::MATCH_INDEX_LOCK,
                                           LockName::SNAPSHOT_LOCK,
                                           LockName::CLUSTER_MEMBERSHIP_LOCK);

            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            continue;
        }

        int old = this->commitIndex;
        this->commitIndex = this->findUpdatedCommitIndex();
        if(old != this->commitIndex) {
            #ifndef NDEBUG
            msg.str("");
            msg << "Commit index changed from " << old << " to " << this->commitIndex;
            this->logMsg(msg.str());
            #endif
        }

        if(this->commitIndex > this->lastApplied) {
            auto applyEntry = [&](const Entry& entry) {
                if(entry.type == EntryType::SET_MAPPING_ENTRY) {
                    if(this->stateMachine.find(entry.key) == this->stateMachine.end()) {
                        std::vector<std::string> values(1, entry.value);
                        this->stateMachine[entry.key] = values;
                    }
                    else {
                        this->stateMachine[entry.key].push_back(entry.value);
                    }
                }
                else if(entry.type == EntryType::DEL_MAPPING_ENTRY) {
                    if(this->stateMachine.find(entry.key) != this->stateMachine.end()) {
                        this->stateMachine.erase(entry.key);
                    }
                }
            };

            #ifndef NDEBUG
            msg.str("");
            msg << "Now applying log entry " << this->log.at(this->lastApplied+1) << " to state machine";
            this->logMsg(msg.str());
            #endif

            ++this->lastApplied;
            applyEntry(this->log.at(this->lastApplied));
        }

        for(const ID& id : this->clusterMembership) {
            if(id == this->myID) {
                continue;
            }

            std::shared_ptr<apache::thrift::transport::TSocket> socket(new apache::thrift::transport::TSocket(id.hostname, id.port));
            socket->setConnTimeout(atoi(dotenv::env[Replica::RPC_TIMEOUT_ENV_VAR_NAME].c_str()));
            socket->setSendTimeout(atoi(dotenv::env[Replica::RPC_TIMEOUT_ENV_VAR_NAME].c_str()));
            socket->setRecvTimeout(atoi(dotenv::env[Replica::RPC_TIMEOUT_ENV_VAR_NAME].c_str()));

            std::shared_ptr<apache::thrift::transport::TTransport> transport(new apache::thrift::transport::TBufferedTransport(socket));
            std::shared_ptr<apache::thrift::protocol::TProtocol> protocol(new apache::thrift::protocol::TBinaryProtocol(transport));
            ReplicaServiceClient client(protocol);

            Entry entryToSend = Replica::getEmptyLogEntry();
            entryToSend.type = EntryType::EMPTY_ENTRY;
            unsigned int prevLogIndex = this->log.size()-1;
            unsigned int prevLogTerm = this->log.back().term;
            if(this->nextIndex.at(id) < (int) this->log.size()) {
                unsigned int logIndexToSend = this->nextIndex.at(id);
                entryToSend = this->log.at(logIndexToSend);
                prevLogIndex = logIndexToSend-1;
                prevLogTerm = this->log.at(prevLogIndex).term;
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


                    if(!appendEntryResponse.success) {
                        #ifndef NDEBUG
                        msg.str("");
                        msg << "AppendEntryRequest directed to " << id << " failed due to log inconsistency: Reducing nextIndex value from " << this->nextIndex.at(id);
                        this->logMsg(msg.str());
                        #endif

                        int possibleNewNextIndex = this->nextIndex.at(id)-1;
                        this->nextIndex[id] = std::max(0, possibleNewNextIndex);
                    }
                    else if(entryToSend.type != EntryType::EMPTY_ENTRY) {
                        #ifndef NDEBUG
                        msg.str("");
                        msg << "AppendEntryRequest directed to " << id << " successful: Increasing nextIndex value from " << this->nextIndex.at(id);
                        this->logMsg(msg.str());
                        #endif

                        this->matchIndex[id] = this->nextIndex.at(id);
                        ++this->nextIndex.at(id);
                    }
                    else {
                        #ifndef NDEBUG
                        msg.str("");
                        msg << "AppendEntryRequest (heartbeat) directed to " << id << " successful";
                        this->logMsg(msg.str());
                        #endif
                    }
                }

                catch(TTransportException& e) {
                    #ifndef NDEBUG
                    msg.str("");
                    if(e.getType() == TTransportException::TTransportExceptionType::TIMED_OUT) {
                        msg << "Timeout occurred while sending a heartbeat to the replica at " << id;
                        this->logMsg(msg.str());
                    }
                    else {
                        msg << "Error while attempting to send an AppendEntryRequest to the replica at " << id;
                        this->logMsg(msg.str());
                    }
                    #endif
                }
            }
            catch(TTransportException& e) {
                #ifndef NDEBUG
                msg.str("");
                msg << "Error while attempting to establish a connection to send an appendEntry request to " << id << " from heartbeat sender: " << e.getType();
                this->logMsg(msg.str());
                #endif
            }
        }

        this->lockHandler.releaseLocks(LockName::CURR_TERM_LOCK,
                                       LockName::LOG_LOCK,
                                       LockName::STATE_LOCK,
                                       LockName::COMMIT_INDEX_LOCK,
                                       LockName::VOTED_FOR_LOCK,
                                       LockName::NEXT_INDEX_LOCK,
                                       LockName::MATCH_INDEX_LOCK,
                                       LockName::SNAPSHOT_LOCK,
                                       LockName::CLUSTER_MEMBERSHIP_LOCK);

        std::this_thread::sleep_for(std::chrono::milliseconds(this->heartbeatTick));
    }
}

void
Replica::retryRequest() {
    unsigned int maxTimeToSpendRetryingMS = atoi(dotenv::env[Replica::MIN_ELECTION_TIMEOUT_ENV_VAR_NAME].c_str());

    std::stringstream msg;

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
                                           LockName::LOG_LOCK,
                                           LockName::SNAPSHOT_LOCK,
                                           LockName::CLUSTER_MEMBERSHIP_LOCK);

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
                                               LockName::LOG_LOCK,
                                               LockName::SNAPSHOT_LOCK,
                                               LockName::CLUSTER_MEMBERSHIP_LOCK);

                break;
            }

            #ifndef NDEBUG
            msg.str("");
            msg << "Retrying AppendEntryRequest " << entry << " after " << timeoutMS << " ms.";
            this->logMsg(msg.str());
            #endif

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
                #ifndef NDEBUG
                msg.str("");
                msg << "Error while attempting to open a connecton to retry an AppendEntryRequest to the replica at " << targetID;
                this->logMsg(msg.str());
                #endif
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
                                                   LockName::LOG_LOCK,
                                                   LockName::SNAPSHOT_LOCK,
                                                   LockName::CLUSTER_MEMBERSHIP_LOCK);

                    break;
                }

                if(!appendEntryResponse.success) {
                    #ifndef NDEBUG
                    msg.str("");
                    msg << "AppendEntryRequest retry to " << targetID << " failed due to log inconsistency: THIS SHOULD NEVER HAPPEN!";
                    this->logMsg(msg.str());
                    #endif

                    this->lockHandler.releaseLocks(LockName::STATE_LOCK,
                                                   LockName::LEADER_LOCK,
                                                   LockName::CURR_TERM_LOCK,
                                                   LockName::VOTED_FOR_LOCK,
                                                   LockName::NEXT_INDEX_LOCK,
                                                   LockName::MATCH_INDEX_LOCK,
                                                   LockName::LOG_LOCK,
                                                   LockName::SNAPSHOT_LOCK,
                                                   LockName::CLUSTER_MEMBERSHIP_LOCK);
                }
                else {
                    #ifndef NDEBUG
                    msg.str("");
                    msg << "Entry successfully replicated on " << targetID  << " during retry: Now increasing nextIndex value from " <<
                                this->nextIndex.at(targetID) << " to " << (this->nextIndex.at(targetID)+1);
                    this->logMsg(msg.str());
                    #endif

                    this->matchIndex.at(targetID) = this->nextIndex.at(targetID);
                    ++this->nextIndex.at(targetID);

                    this->lockHandler.releaseLocks(LockName::STATE_LOCK,
                                                   LockName::LEADER_LOCK,
                                                   LockName::CURR_TERM_LOCK,
                                                   LockName::VOTED_FOR_LOCK,
                                                   LockName::NEXT_INDEX_LOCK,
                                                   LockName::MATCH_INDEX_LOCK,
                                                   LockName::LOG_LOCK,
                                                   LockName::SNAPSHOT_LOCK,
                                                   LockName::CLUSTER_MEMBERSHIP_LOCK);
                    break;
                }
            }
            catch(TTransportException& e) {
                #ifndef NDEBUG
                msg.str("");
                if(e.getType() == TTransportException::TTransportExceptionType::TIMED_OUT) {
                    msg << "Timeout occurred while retrying an AppendEntryRequest to the replica at " << targetID;
                    this->logMsg(msg.str());
                }
                else {
                    msg << "Non-Timeout error while attempting to retry an AppendEntryRequest to the replica at " << targetID;
                    this->logMsg(msg.str());
                }
                #endif
            }

            timeSpentOnCurrentRetryMS += timeoutMS;
            timeoutMS = ((double) timeoutMS) * 1.5;
        }
    }
}

int32_t
Replica::installSnapshot(const int32_t leaderTerm, const ID& leaderID, const int32_t lastIncludedIndex, const int32_t lastIncludedTerm, const int32_t offset, const std::string& data, const bool done) {
    this->lockHandler.acquireLocks(LockName::CURR_TERM_LOCK,
                                   LockName::TIMER_LOCK,
                                   LockName::LOG_LOCK,
                                   LockName::SNAPSHOT_LOCK);

    int termToReturn = std::max(this->currentTerm, leaderTerm);

    if(this->currentTerm > leaderTerm) {
        this->lockHandler.releaseLocks(LockName::CURR_TERM_LOCK,
                                       LockName::TIMER_LOCK,
                                       LockName::LOG_LOCK,
                                       LockName::SNAPSHOT_LOCK);
        return termToReturn;
    }

    this->currentTerm = termToReturn;

    this->timeLeft = this->timeout;

    std::stringstream compactionFileNameStream;
    compactionFileNameStream << dotenv::env[Replica::SNAPSHOT_FILE_ENV_VAR_NAME] << "-" << this->myID.hostname << ":" << this->myID.port;
    std::string compactionFileName = compactionFileNameStream.str();

    std::ofstream compactionFileStream;
    if(offset == 0) {
        compactionFileStream.open(compactionFileName.c_str(), std::ofstream::binary);
    }
    else {
        compactionFileStream.open(compactionFileName.c_str(), std::ofstream::binary | std::ofstream::app);
    }

    compactionFileStream.seekp(offset, std::ios_base::beg);

    const char* bytes = data.c_str();
    for(unsigned int i = 0; i < data.size(); ++i) {
        compactionFileStream << bytes[i];
    }
    compactionFileStream.close();

    if(!done) {
        this->lockHandler.releaseLocks(LockName::CURR_TERM_LOCK,
                                       LockName::TIMER_LOCK,
                                       LockName::LOG_LOCK,
                                       LockName::SNAPSHOT_LOCK);
        return termToReturn;
    }


    for(unsigned int i = 0; i < this->log.size(); ++i) {
    }

    /*
    std::ifstream compactionFileStream(compactionFileName.c_str());
    compactionFileStream >> this->currentSnapshot;
    compactionFileStream.close();
    */

    this->lockHandler.releaseLocks(LockName::CURR_TERM_LOCK,
                                   LockName::TIMER_LOCK,
                                   LockName::LOG_LOCK,
                                   LockName::SNAPSHOT_LOCK);

    return termToReturn;
}

bool
Replica::addNewConfiguration(const std::vector<ID>& newConfiguration, const std::string& clientIdentifier, const int32_t requestIdentifier) {
    this->lockHandler.acquireLocks(LockName::LOG_LOCK,
                                   LockName::CURR_TERM_LOCK,
                                   LockName::STATE_LOCK,
                                   LockName::COMMIT_INDEX_LOCK,
                                   LockName::CLUSTER_MEMBERSHIP_LOCK,
                                   LockName::NEXT_INDEX_LOCK,
                                   LockName::MATCH_INDEX_LOCK);

    if(this->state != ReplicaState::LEADER) {
        return false;
    }

    if(requestIdentifier == this->currentRequestBeingServiced) {
        #ifndef NDEBUG
        msg.str("");
        msg << "Continuing servicing of request " << requestIdentifier;
        this->logMsg(msg.str());
        #endif

        unsigned int relevantEntryIndex = 0;
        for(unsigned int entryIndex = 0; entryIndex < this->log.size(); ++entryIndex) {
            if(this->log.at(entryIndex).requestIdentifier == requestIdentifier) {
                relevantEntryIndex = entryIndex;
                break;
            }
        }

        assert(relevantEntryIndex != 0);

        bool entryIsFromCurrentTerm = (this->log.at(relevantEntryIndex).term == this->currentTerm);

        std::vector<int> matchIndices;
        for(auto pair : this->matchIndex) {
            matchIndices.push_back(pair.second);
        }
        matchIndices.push_back(this->log.size()-1);

        #ifndef NDEBUG
        msg.str("");
        msg << "Is this entry from this term? = " << entryIsFromCurrentTerm;
        this->logMsg(msg.str());

        msg.str("");
        msg << "Has the entry been successfully replicated on a majority of replicas " <<
                                 areAMajorityGreaterThanOrEqual(matchIndices, relevantEntryIndex);
        this->logMsg(msg.str());
        #endif

        this->lockHandler.releaseLocks(LockName::LOG_LOCK,
                                       LockName::CURR_TERM_LOCK,
                                       LockName::STATE_LOCK,
                                       LockName::COMMIT_INDEX_LOCK,
                                       LockName::CLUSTER_MEMBERSHIP_LOCK,
                                       LockName::NEXT_INDEX_LOCK,
                                       LockName::MATCH_INDEX_LOCK);

        return entryIsFromCurrentTerm && areAMajorityGreaterThanOrEqual(matchIndices, relevantEntryIndex);
    }

    this->currentRequestBeingServiced = requestIdentifier;

    std::set<ID> oldConfigurationIDs(this->clusterMembership.begin(), this->clusterMembership.end());
    std::set<ID> newConfigurationIDs(newConfiguration.begin(), newConfiguration.end());

    this->clusterMembership.clear();

    set_union(oldConfigurationIDs.begin(), oldConfigurationIDs.end(),
              newConfigurationIDs.begin(), newConfigurationIDs.end(),
              std::inserter(this->clusterMembership, this->clusterMembership.begin()));

    Entry newConfigurationEntry;
    newConfigurationEntry.type = EntryType::CONFIG_CHANGE_ENTRY;
    newConfigurationEntry.newConfiguration = this->clusterMembership;
    newConfigurationEntry.term = this->currentTerm;

    for(const ID& id : newConfigurationIDs) {
        if(oldConfigurationIDs.find(id) == oldConfigurationIDs.end()) {
            newConfigurationEntry.nonVotingMembers.insert(id);
        }

        if(this->nextIndex.find(id) == this->nextIndex.end()) {
            assert(this->matchIndex.find(id) == this->matchIndex.end());
            this->nextIndex[id] = this->log.size()-1;
            this->matchIndex[id] = 0;
        }
    }

    this->log.push_back(newConfigurationEntry);

    int replicationAmount = 0;

    for(const ID& id : this->clusterMembership) {
        if(id == this->myID) {
            continue;
        }

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
                client.appendEntry(appendEntryResponse,
                                   this->currentTerm,
                                   this->myID,
                                   this->log.size()-2,
                                   this->log.at(this->log.size()-2).term,
                                   newConfigurationEntry,
                                   this->commitIndex);

                if(appendEntryResponse.term > this->currentTerm) {
                    this->state = ReplicaState::FOLLOWER;
                    this->currentTerm = appendEntryResponse.term;
                    this->votedFor = Replica::getNullID();
                    break;
                }

                if(!appendEntryResponse.success) {
                    this->currentTerm = appendEntryResponse.term;
                    this->state = ReplicaState::FOLLOWER;
                    this->votedFor = Replica::getNullID();
                }
                else {
                    ++replicationAmount;
                }
            }
            catch(TTransportException& e) {
            }
        }
        catch(TTransportException& e) {
        }
    }

/*
    if(replicationAmount >= ((this->clusterMembership.size() + 1) / 2) + 1) {
        Entry newConfigurationEntry;
        newConfigurationEntry.type = EntryType::CONFIG_CHANGE_ENTRY;
        newConfigurationEntry.newConfiguration = newConfiguration;
        newConfigurationEntry.term = this->currentTerm;

        this->clusterMembership = newConfiguration;

        int replicationAmount = 0;
        for(const ID& id : this->clusterMembership) {
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
                    client.appendEntry(appendEntryResponse,
                                       this->currentTerm,
                                       this->myID,
                                       this->log.size()-2,
                                       this->log.at(this->log.size()-2).term,
                                       newConfigurationEntry,
                                       this->commitIndex);

                    if(appendEntryResponse.term > this->currentTerm) {
                        this->state = ReplicaState::FOLLOWER;
                        this->currentTerm = appendEntryResponse.term;
                        this->votedFor = Replica::getNullID();
                        break;
                    }

                    if(!appendEntryResponse.success) {
                        this->currentTerm = appendEntryResponse.term;
                        this->state = ReplicaState::FOLLOWER;
                        this->votedFor = Replica::getNullID();
                    }
                    else {
                        ++replicationAmount;
                    }
                }
                catch(TTransportException& e) {
                }
            }
            catch(TTransportException& e) {
            }
        }

        if(replicationAmount >= ((this->clusterMembership.size() + 1) / 2) + 1) {
            for(const ID& id : oldConfigurationIDs) {
                if(newConfigurationIDs.find(id) == newConfigurationIDs.end()) {
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
                            client.kill();
                        }
                        catch(TTransportException& e) {
                        }
                    }
                    catch(TTransportException& e) {
                    }
                }
            }

            this->clusterMembership = newConfiguration;

            std::ofstream membershipFileObj("cluster.membership");
            for(const ID& id : this->clusterMembership) {
                membershipFileObj << id.hostname << ":" << id.port << "\n";
            }
        }
    }
*/

    this->lockHandler.releaseLocks(LockName::LOG_LOCK,
                                   LockName::CURR_TERM_LOCK,
                                   LockName::STATE_LOCK,
                                   LockName::COMMIT_INDEX_LOCK,
                                   LockName::CLUSTER_MEMBERSHIP_LOCK,
                                   LockName::NEXT_INDEX_LOCK,
                                   LockName::MATCH_INDEX_LOCK);

    return true;
}

Entry
Replica::getEmptyLogEntry() {
    Entry emptyLogEntry;
    emptyLogEntry.type = EntryType::EMPTY_ENTRY;
    emptyLogEntry.key = "";
    emptyLogEntry.value = "";
    emptyLogEntry.term = -1;
    emptyLogEntry.clientIdentifier = "";
    emptyLogEntry.requestIdentifier = std::numeric_limits<int>::max();

    return emptyLogEntry;
}

unsigned int
Replica::getElectionTimeout() {
    unsigned int minTimeMS = atoi(dotenv::env[Replica::MIN_ELECTION_TIMEOUT_ENV_VAR_NAME].c_str());
    unsigned int maxTimeMS = atoi(dotenv::env[Replica::MAX_ELECTION_TIMEOUT_ENV_VAR_NAME].c_str());

    srand(time(0));

    return (rand() % (maxTimeMS - minTimeMS)) + minTimeMS;
}

std::vector<ID>
Replica::getMemberIDs(const std::vector<std::string>& socketAddrs) {
    std::vector<ID> membership;

    for(const std::string& addr : socketAddrs) {
        std::stringstream ss(addr);
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
                      this->log.at(possibleNewCommitIndex).term == this->currentTerm) {
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

Snapshot
Replica::getSnapshot() {
    Snapshot newSnapshot;

    newSnapshot.lastIncludedIndex = this->log.size()-1 + this->currentSnapshot.lastIncludedIndex;
    newSnapshot.lastIncludedTerm = this->log.back().term;
    for(auto const& mapping : this->stateMachine) {
        newSnapshot.mappings.push_back(mapping);
    }

    #ifndef NDEBUG
    std::stringstream msg;
    msg << "Taking snapshot: index=" << newSnapshot.lastIncludedIndex << ", term=" <<
                newSnapshot.lastIncludedTerm << ", " << newSnapshot.mappings;
    this->logMsg(msg.str());
    #endif

    return newSnapshot;
}

bool
ID::operator<(const ID& other) const {
    std::stringstream id1, id2;
    id1 << this->hostname << ":" << this->port;
    id2 << other.hostname << ":" << other.port;

    return id1.str() < id2.str();
}

int
main(int argc, const char** argv) {
    ArgumentParser parser;

    parser.addArgument("--listeningport", 1, false);
    parser.addArgument("--clustermembership", '+', false);

    parser.parse(argc, argv);

    unsigned int portToUse = atoi(parser.retrieve<std::string>("listeningport").c_str());
    std::vector<std::string> clusterMembers = parser.retrieve<std::vector<std::string>>("clustermembership");

    std::shared_ptr<Replica> handler(new Replica(portToUse, clusterMembers));
    std::shared_ptr<apache::thrift::TProcessor> processor(new ReplicaServiceProcessor(handler));
    std::shared_ptr<apache::thrift::transport::TServerTransport> serverTransport(new apache::thrift::transport::TServerSocket(portToUse));
    std::shared_ptr<apache::thrift::transport::TTransportFactory> transportFactory(new apache::thrift::transport::TBufferedTransportFactory());
    std::shared_ptr<apache::thrift::protocol::TProtocolFactory> protocolFactory(new apache::thrift::protocol::TBinaryProtocolFactory());

    apache::thrift::server::TThreadedServer server(processor, serverTransport, transportFactory, protocolFactory);
    server.serve();

    return 0;
}
