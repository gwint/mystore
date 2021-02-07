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
#include <stdexcept>

#include "replica.hpp"
#include "lockhandler.hpp"
#include "locknames.hpp"
#include "utils.hpp"

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

#include "replicaservice_types.h"
#include "ReplicaService.h"

using apache::thrift::transport::TTransportException;

#ifndef NDEBUG
#define LOG_INFO(msgStream) {\
    std::stringstream msg;\
    msg << msgStream;\
    this->logMsg(msg.str());\
}
#else
#define LOG_INFO(msgStream)
#endif
    

Replica::Replica(unsigned int port, const std::vector<std::string>& clusterSocketAddrs) : state(ReplicaState::FOLLOWER),
          currentTerm(0),
          commitIndex(0),
          lastApplied(0),
          timeout(getElectionTimeout()),
          votedFor(getNullID()),
          leader(getNullID()),
          currentRequestBeingServiced(std::numeric_limits<unsigned int>::max()),
          hasOperationStarted(false),
          clusterMembership(getMemberIDs(clusterSocketAddrs)),
          lockHandler(16),
          noopIndex(0),
	  willingToVote(true) {

    this->timeLeft = this->timeout;
    this->heartbeatTick = atoi(dotenv::env[HEARTBEAT_TICK_ENV_VAR_NAME].c_str());

    this->log.push_back(getEmptyLogEntry());

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

    this->lockHandler.acquireLocks({LockName::CURR_TERM_LOCK,
                                    LockName::LOG_LOCK,
                                    LockName::STATE_LOCK,
                                    LockName::VOTED_FOR_LOCK,
                                    LockName::SNAPSHOT_LOCK,
                                    LockName::WILLING_TO_VOTE_LOCK});

    LOG_INFO(candidateID << " is requesting my vote.");

    if(term > this->currentTerm) {
        this->state = ReplicaState::FOLLOWER;
        this->currentTerm = term;
        this->votedFor = getNullID();
    }

    ballotTerm = this->currentTerm;

    if((this->votedFor == candidateID || isANullID(this->votedFor)) &&
                this->isAtLeastAsUpToDateAs(lastLogIndex,
                                            lastLogTerm,
                                            this->log.size()-1,
                                            this->log.back().term) && this->willingToVote) {
        LOG_INFO("Granted vote to " << candidateID);
        voteGranted = true;
        this->votedFor = candidateID;
    }

    this->lockHandler.releaseLocks({LockName::CURR_TERM_LOCK,
                                    LockName::LOG_LOCK,
                                    LockName::STATE_LOCK,
                                    LockName::VOTED_FOR_LOCK,
                                    LockName::SNAPSHOT_LOCK,
                                    LockName::WILLING_TO_VOTE_LOCK});

    _return.voteGranted = voteGranted;
    _return.term = ballotTerm;
}

void
Replica::appendEntry(AppendEntryResponse& _return, const int32_t term, const ID& leaderID, const int32_t prevLogIndex, const int32_t prevLogTerm, const Entry& entry, const int32_t leaderCommit) {

    _return.success = true;

    this->lockHandler.acquireLocks({LockName::CURR_TERM_LOCK,
                                    LockName::LOG_LOCK,
                                    LockName::STATE_LOCK,
                                    LockName::LAST_APPLIED_LOCK,
                                    LockName::LEADER_LOCK,
                                    LockName::MAP_LOCK,
                                    LockName::TIMER_LOCK,
                                    LockName::COMMIT_INDEX_LOCK,
                                    LockName::SNAPSHOT_LOCK,
                                    LockName::CLUSTER_MEMBERSHIP_LOCK,
                                    LockName::WILLING_TO_VOTE_LOCK});

    LOG_INFO(leaderID << " is appending " << entry << " to my log.");

    if(term >= this->currentTerm) {
        this->timeLeft = this->timeout;
	this->willingToVote = false;
    }

    assert(prevLogIndex >= 0);
    assert(this->log.size() > 0);

    if((term < this->currentTerm) || ((unsigned) prevLogIndex >= this->log.size()) ||
                                                    (prevLogTerm != this->log.at(prevLogIndex).term)) {

        LOG_INFO("Rejecting appendEntry request from (" << leaderID << "; leaderterm=" << term << ", myterm=" << this->currentTerm << ", prevLogIndex=" << prevLogIndex << ")");

        _return.success = false;
        _return.term = std::max(term, this->currentTerm);
        this->currentTerm = std::max(term, this->currentTerm);

        this->lockHandler.releaseLocks({LockName::CURR_TERM_LOCK,
                                        LockName::LOG_LOCK,
                                        LockName::STATE_LOCK,
                                        LockName::LAST_APPLIED_LOCK,
                                        LockName::LEADER_LOCK,
                                        LockName::MAP_LOCK,
                                        LockName::TIMER_LOCK,
                                        LockName::COMMIT_INDEX_LOCK,
                                        LockName::SNAPSHOT_LOCK,
                                        LockName::CLUSTER_MEMBERSHIP_LOCK,
                                        LockName::WILLING_TO_VOTE_LOCK});

        return;
    }

    this->state = ReplicaState::FOLLOWER;

    unsigned int numEntriesToRemove = 0;
    for(int i = this->log.size()-1; i > prevLogIndex; --i) {
        ++numEntriesToRemove;
    }

    if(numEntriesToRemove > 0) {
        LOG_INFO("Now removing " << numEntriesToRemove << " from the back of the log to bring it in line with the leader");

        for(unsigned int i = 0; i < numEntriesToRemove; ++i) {
            this->log.pop_back();
        }
    }

    if(entry.type != EntryType::EMPTY_ENTRY) {
        LOG_INFO("Now appending " << entry << " to the log");

        this->log.push_back(entry);

        if(entry.type == EntryType::CONFIG_CHANGE_ENTRY) {
            this->nonVotingMembers = entry.nonVotingMembers;
            this->clusterMembership = entry.newConfiguration;
        }

        if(this->log.size() >= (unsigned) atoi(dotenv::env[MAX_ALLOWED_LOG_SIZE_ENV_VAR_NAME].c_str())) {
            this->currentSnapshot = this->getSnapshot();
        }
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
        LOG_INFO("Now applying log entry " << this->log.at(this->lastApplied+1) << " to state machine");

        ++this->lastApplied;
        applyEntry(this->log.at(this->lastApplied));
    }

    this->leader = leaderID;
    this->currentTerm = std::max(term, this->currentTerm);
    _return.term = this->currentTerm;

    this->lockHandler.releaseLocks({LockName::CURR_TERM_LOCK,
                                    LockName::LOG_LOCK,
                                    LockName::STATE_LOCK,
                                    LockName::LAST_APPLIED_LOCK,
                                    LockName::LEADER_LOCK,
                                    LockName::MAP_LOCK,
                                    LockName::TIMER_LOCK,
                                    LockName::COMMIT_INDEX_LOCK,
                                    LockName::SNAPSHOT_LOCK,
                                    LockName::CLUSTER_MEMBERSHIP_LOCK,
                                    LockName::WILLING_TO_VOTE_LOCK});
}

ReplicaServiceClient
Replica::getServiceClient(const std::string& hostname, int port) {
    std::shared_ptr<apache::thrift::transport::TSocket> socket(new apache::thrift::transport::TSocket(hostname, port));
    socket->setConnTimeout(atoi(dotenv::env[RPC_TIMEOUT_ENV_VAR_NAME].c_str()));
    socket->setSendTimeout(atoi(dotenv::env[RPC_TIMEOUT_ENV_VAR_NAME].c_str()));
    socket->setRecvTimeout(atoi(dotenv::env[RPC_TIMEOUT_ENV_VAR_NAME].c_str()));
    std::shared_ptr<apache::thrift::transport::TTransport> transport(new apache::thrift::transport::TBufferedTransport(socket));
    std::shared_ptr<apache::thrift::protocol::TProtocol> protocol(new apache::thrift::protocol::TBinaryProtocol(transport));
    ReplicaServiceClient client(protocol);

    try {
        transport->open();
    }
    catch(TTransportException& e) {
        LOG_INFO("Error while attempting to open a connection to replica at (" << hostname << ":" << port << "):" << e.getType());
    }

    return client;
}


void
Replica::get(GetResponse& _return, const std::string& key, const std::string& clientIdentifier, const int32_t requestIdentifier, const int32_t numPastMappings) {
    _return.success = true;

    this->lockHandler.acquireLocks({LockName::STATE_LOCK,
                                    LockName::LEADER_LOCK,
                                    LockName::CURR_TERM_LOCK,
                                    LockName::LOG_LOCK,
                                    LockName::COMMIT_INDEX_LOCK,
                                    LockName::VOTED_FOR_LOCK,
                                    LockName::MATCH_INDEX_LOCK,
                                    LockName::LATEST_NO_OP_LOG_INDEX,
                                    LockName::SNAPSHOT_LOCK,
                                    LockName::CLUSTER_MEMBERSHIP_LOCK});

    LOG_INFO(this->myID << " now attempting to retrieve value associated with " << key);

    if(this->state != ReplicaState::LEADER) {
        LOG_INFO("Was contacted to resolve a GET but am not the leader, redirected to " << this->leader);

        _return.success = false;
        _return.leaderID = getNullID();
        if(!isANullID(this->leader)) {
            _return.leaderID = this->leader;
        }

        this->lockHandler.releaseLocks({LockName::STATE_LOCK,
                                        LockName::LEADER_LOCK,
                                        LockName::CURR_TERM_LOCK,
                                        LockName::LOG_LOCK,
                                        LockName::COMMIT_INDEX_LOCK,
                                        LockName::VOTED_FOR_LOCK,
                                        LockName::MATCH_INDEX_LOCK,
                                        LockName::LATEST_NO_OP_LOG_INDEX,
                                        LockName::SNAPSHOT_LOCK,
                                        LockName::CLUSTER_MEMBERSHIP_LOCK});

        return;
    }

    unsigned int numReplicasSuccessfullyContacted = 1;

    for(const auto &id : this->clusterMembership) {
        if(id == this->myID) {
            continue;
        }

        try {
            ReplicaServiceClient client = getServiceClient(id.hostname, id.port);

            LOG_INFO("Now sending a heartbeat to " << id);

            AppendEntryResponse appendEntryResponse;

            client.appendEntry(appendEntryResponse,
                               this->currentTerm,
                               this->myID,
                               this->log.size()-1,
                               this->log.back().term,
                               getEmptyLogEntry(),
                               this->commitIndex);

            LOG_INFO("Was AppendEntryRequest successful: " << appendEntryResponse.success);

            if(appendEntryResponse.term > this->currentTerm) {
                this->state = ReplicaState::FOLLOWER;
                this->currentTerm = appendEntryResponse.term;
                this->votedFor = getNullID();
                _return.success = false;

                LOG_INFO("Early exit: Larger term encountered");

                this->lockHandler.releaseLocks({LockName::STATE_LOCK,
                                                LockName::LEADER_LOCK,
                                                LockName::CURR_TERM_LOCK,
                                                LockName::LOG_LOCK,
                                                LockName::COMMIT_INDEX_LOCK,
                                                LockName::VOTED_FOR_LOCK,
                                                LockName::MATCH_INDEX_LOCK,
                                                LockName::LATEST_NO_OP_LOG_INDEX,
                                                LockName::SNAPSHOT_LOCK,
                                                LockName::CLUSTER_MEMBERSHIP_LOCK});

                return;
            }

            if(!appendEntryResponse.success) {
                _return.success = false;

                LOG_INFO("Early exit: appendEntryResponse not successful");

                this->lockHandler.releaseLocks({LockName::STATE_LOCK,
                                                LockName::LEADER_LOCK,
                                                LockName::CURR_TERM_LOCK,
                                                LockName::LOG_LOCK,
                                                LockName::COMMIT_INDEX_LOCK,
                                                LockName::VOTED_FOR_LOCK,
                                                LockName::MATCH_INDEX_LOCK,
                                                LockName::LATEST_NO_OP_LOG_INDEX,
                                                LockName::SNAPSHOT_LOCK,
                                                LockName::CLUSTER_MEMBERSHIP_LOCK});

                return;
            }

            if(this->nonVotingMembers.find(id) == this->nonVotingMembers.end()) {
                ++numReplicasSuccessfullyContacted;
            }
        }
        catch(TTransportException& e) {
            if(e.getType() == TTransportException::TTransportExceptionType::TIMED_OUT) {
                LOG_INFO("Timeout occurred while attempting to send a heartbeat to replica at " << id);
            }
            else {
                LOG_INFO("Error while attempting to send a heartbeat to replica at " << id);
            }
        }
    }

    if(numReplicasSuccessfullyContacted < (this->clusterMembership.size() / 2) + 1) {
        _return.success = false;

        LOG_INFO("Early exit: replication level needed not reached");

        this->lockHandler.releaseLocks({LockName::STATE_LOCK,
                                        LockName::LEADER_LOCK,
                                        LockName::CURR_TERM_LOCK,
                                        LockName::LOG_LOCK,
                                        LockName::COMMIT_INDEX_LOCK,
                                        LockName::VOTED_FOR_LOCK,
                                        LockName::MATCH_INDEX_LOCK,
                                        LockName::LATEST_NO_OP_LOG_INDEX,
                                        LockName::SNAPSHOT_LOCK,
                                        LockName::CLUSTER_MEMBERSHIP_LOCK});

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

    this->lockHandler.releaseLocks({LockName::STATE_LOCK,
                                    LockName::LEADER_LOCK,
                                    LockName::CURR_TERM_LOCK,
                                    LockName::LOG_LOCK,
                                    LockName::COMMIT_INDEX_LOCK,
                                    LockName::VOTED_FOR_LOCK,
                                    LockName::MATCH_INDEX_LOCK,
                                    LockName::LATEST_NO_OP_LOG_INDEX,
                                    LockName::SNAPSHOT_LOCK,
                                    LockName::CLUSTER_MEMBERSHIP_LOCK});

}

void
Replica::deletekey(DelResponse& _return, const std::string& key, const std::string& clientIdentifier, const int32_t requestIdentifier) {
    this->lockHandler.acquireLocks({LockName::STATE_LOCK,
                                    LockName::LEADER_LOCK,
                                    LockName::CURR_TERM_LOCK,
                                    LockName::LOG_LOCK,
                                    LockName::COMMIT_INDEX_LOCK,
                                    LockName::VOTED_FOR_LOCK,
                                    LockName::NEXT_INDEX_LOCK,
                                    LockName::LAST_APPLIED_LOCK,
                                    LockName::MATCH_INDEX_LOCK,
                                    LockName::SNAPSHOT_LOCK,
                                    LockName::CLUSTER_MEMBERSHIP_LOCK});

    _return.success = true;

    LOG_INFO(this->myID << " now attempting to delete " << key);

    if(this->state != ReplicaState::LEADER) {
        LOG_INFO("Was contacted to resolve a DEL but am not the leader, redirected to " << this-> leader);

        _return.success = false;
        if(!isANullID(this->leader)) {
            _return.leaderID = this->leader;
        }

        this->lockHandler.releaseLocks({LockName::STATE_LOCK,
                                        LockName::LEADER_LOCK,
                                        LockName::CURR_TERM_LOCK,
                                        LockName::LOG_LOCK,
                                        LockName::COMMIT_INDEX_LOCK,
                                        LockName::VOTED_FOR_LOCK,
                                        LockName::NEXT_INDEX_LOCK,
                                        LockName::LAST_APPLIED_LOCK,
                                        LockName::MATCH_INDEX_LOCK,
                                        LockName::SNAPSHOT_LOCK,
                                        LockName::CLUSTER_MEMBERSHIP_LOCK});

        return;
    }

    if(requestIdentifier == this->currentRequestBeingServiced) {
        LOG_INFO("Continuing servicing of request " << requestIdentifier);

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

        LOG_INFO("Is this entry from this term? = " << entryIsFromCurrentTerm);
        LOG_INFO("Has the entry been successfully replicated on a majority of replicas " << areAMajorityGreaterThanOrEqual(matchIndices, relevantEntryIndex));

        this->lockHandler.releaseLocks({LockName::STATE_LOCK,
                                        LockName::LEADER_LOCK,
                                        LockName::CURR_TERM_LOCK,
                                        LockName::LOG_LOCK,
                                        LockName::COMMIT_INDEX_LOCK,
                                        LockName::VOTED_FOR_LOCK,
                                        LockName::NEXT_INDEX_LOCK,
                                        LockName::LAST_APPLIED_LOCK,
                                        LockName::MATCH_INDEX_LOCK,
                                        LockName::SNAPSHOT_LOCK,
                                        LockName::CLUSTER_MEMBERSHIP_LOCK});

        return;
    }

    Entry newLogEntry;
    newLogEntry.type = EntryType::DEL_MAPPING_ENTRY;
    newLogEntry.key = key;
    newLogEntry.term = this->currentTerm;
    newLogEntry.clientIdentifier = clientIdentifier;
    newLogEntry.requestIdentifier = requestIdentifier;
    this->log.push_back(newLogEntry);

    if(this->log.size() >= (unsigned) atoi(dotenv::env[MAX_ALLOWED_LOG_SIZE_ENV_VAR_NAME].c_str())) {
        this->currentSnapshot = this->getSnapshot();
    }

    this->currentRequestBeingServiced = requestIdentifier;

    unsigned int numServersReplicatedOn = 1;

    for(const auto &id : this->clusterMembership) {
        if(id == this->myID) {
            continue;
        }

        try {
            ReplicaServiceClient client = getServiceClient(id.hostname, id.port);

            LOG_INFO("Now sending an AppendEntry request to " << id << "; may take a while...");

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
                this->votedFor = getNullID();
                _return.success = false;

                this->lockHandler.releaseLocks({LockName::STATE_LOCK,
                                                LockName::LEADER_LOCK,
                                                LockName::CURR_TERM_LOCK,
                                                LockName::LOG_LOCK,
                                                LockName::COMMIT_INDEX_LOCK,
                                                LockName::VOTED_FOR_LOCK,
                                                LockName::NEXT_INDEX_LOCK,
                                                LockName::LAST_APPLIED_LOCK,
                                                LockName::MATCH_INDEX_LOCK,
                                                LockName::SNAPSHOT_LOCK,
                                                LockName::CLUSTER_MEMBERSHIP_LOCK});

                return;
            }

            if(!appendEntryResponse.success) {
                LOG_INFO("AppendEntryRequest directed to " << id << " failed due to log inconsistency: Reducing next index value from " << this->nextIndex.at(id));

                assert(this->nextIndex.at(id) > 0);
                this->nextIndex.at(id) = std::max(1, this->nextIndex.at(id)-1);
            }
            else {
                LOG_INFO("Entry successfully replicated on " << id << ": Now increasing replication aount from " << numServersReplicatedOn << " to " << (numServersReplicatedOn+1));

                this->matchIndex[id] = this->nextIndex.at(id);
                ++this->nextIndex.at(id);

                if(this->nonVotingMembers.find(id) == this->nonVotingMembers.end()) {
                    ++numServersReplicatedOn;
                }
            }
        }
        catch(TTransportException& e) {
            if(e.getType() == TTransportException::TTransportExceptionType::TIMED_OUT) {
                LOG_INFO("Timeout occurred while attempting to append entry to replica at " << id);

                Job retryJob = {(int) this->log.size()-1, id.hostname, id.port};
                this->jobsToRetry.push(retryJob);
                continue;
            }

            throw e;
        }
    }

    _return.leaderID = this->leader;

    if(numServersReplicatedOn < (this->clusterMembership.size() / 2) + 1) {
        LOG_INFO("Entry unsuccessfully replicated on a majority of servers: replication amount = " << numServersReplicatedOn  << "/" <<  ((this->clusterMembership.size() + 1 / 2) + 1));

        _return.success = false;
    }
    else {
        LOG_INFO("Entry successfully replicated on a majority of servers and removing mapping with key: " << key);

        if(this->stateMachine.find(key) != this->stateMachine.end()) {
            this->stateMachine.erase(key);
        }

        this->commitIndex = this->log.size()-1;
        this->lastApplied = this->log.size()-1;
    }

    this->lockHandler.releaseLocks({LockName::STATE_LOCK,
                                    LockName::LEADER_LOCK,
                                    LockName::CURR_TERM_LOCK,
                                    LockName::LOG_LOCK,
                                    LockName::COMMIT_INDEX_LOCK,
                                    LockName::VOTED_FOR_LOCK,
                                    LockName::NEXT_INDEX_LOCK,
                                    LockName::LAST_APPLIED_LOCK,
                                    LockName::MATCH_INDEX_LOCK,
                                    LockName::SNAPSHOT_LOCK,
                                    LockName::CLUSTER_MEMBERSHIP_LOCK});

    return;
}

void
Replica::put(PutResponse& _return, const std::string& key, const std::string& value, const std::string& clientIdentifier, const int32_t requestIdentifier) {
    this->lockHandler.acquireLocks({LockName::STATE_LOCK,
                                    LockName::LEADER_LOCK,
                                    LockName::CURR_TERM_LOCK,
                                    LockName::LOG_LOCK,
                                    LockName::COMMIT_INDEX_LOCK,
                                    LockName::VOTED_FOR_LOCK,
                                    LockName::NEXT_INDEX_LOCK,
                                    LockName::LAST_APPLIED_LOCK,
                                    LockName::MATCH_INDEX_LOCK,
                                    LockName::SNAPSHOT_LOCK,
                                    LockName::CLUSTER_MEMBERSHIP_LOCK});

    _return.success = true;

    LOG_INFO(this->myID << " now attempting to associate " << key << " with " << value);

    if(this->state != ReplicaState::LEADER) {
        LOG_INFO("Was contacted to resolve a PUT but am not the leader, redirected to " << this-> leader);

        _return.success = false;
        if(!isANullID(this->leader)) {
            _return.leaderID = this->leader;
        }

        this->lockHandler.releaseLocks({LockName::STATE_LOCK,
                                        LockName::LEADER_LOCK,
                                        LockName::CURR_TERM_LOCK,
                                        LockName::LOG_LOCK,
                                        LockName::COMMIT_INDEX_LOCK,
                                        LockName::VOTED_FOR_LOCK,
                                        LockName::NEXT_INDEX_LOCK,
                                        LockName::LAST_APPLIED_LOCK,
                                        LockName::MATCH_INDEX_LOCK,
                                        LockName::SNAPSHOT_LOCK,
                                        LockName::CLUSTER_MEMBERSHIP_LOCK});

        return;
    }

    if(requestIdentifier == this->currentRequestBeingServiced) {
        LOG_INFO("Continuing servicing of request " << requestIdentifier);

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

        LOG_INFO("Is this entry from this term? = " << entryIsFromCurrentTerm);
        LOG_INFO("Has the entry been successfully replicated on a majority of replicas " << areAMajorityGreaterThanOrEqual(matchIndices, relevantEntryIndex));

        this->lockHandler.releaseLocks({LockName::STATE_LOCK,
                                        LockName::LEADER_LOCK,
                                        LockName::CURR_TERM_LOCK,
                                        LockName::LOG_LOCK,
                                        LockName::COMMIT_INDEX_LOCK,
                                        LockName::VOTED_FOR_LOCK,
                                        LockName::NEXT_INDEX_LOCK,
                                        LockName::LAST_APPLIED_LOCK,
                                        LockName::MATCH_INDEX_LOCK,
                                        LockName::SNAPSHOT_LOCK,
                                        LockName::CLUSTER_MEMBERSHIP_LOCK});

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

    if(this->log.size() >= (unsigned) atoi(dotenv::env[MAX_ALLOWED_LOG_SIZE_ENV_VAR_NAME].c_str())) {
        this->currentSnapshot = this->getSnapshot();
    }

    this->currentRequestBeingServiced = requestIdentifier;

    unsigned int numServersReplicatedOn = 1;

    for(const auto &id : this->clusterMembership) {
        if(id == this->myID) {
            continue;
        }

        ReplicaServiceClient client = getServiceClient(id.hostname, id.port);

        try {
            LOG_INFO("Now sending an AppendEntry request to " << id << "; may take a while...");

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
                this->votedFor = getNullID();
                _return.success = false;

                this->lockHandler.releaseLocks({LockName::STATE_LOCK,
                                                LockName::LEADER_LOCK,
                                                LockName::CURR_TERM_LOCK,
                                                LockName::LOG_LOCK,
                                                LockName::COMMIT_INDEX_LOCK,
                                                LockName::VOTED_FOR_LOCK,
                                                LockName::NEXT_INDEX_LOCK,
                                                LockName::LAST_APPLIED_LOCK,
                                                LockName::MATCH_INDEX_LOCK,
                                                LockName::SNAPSHOT_LOCK,
                                                LockName::CLUSTER_MEMBERSHIP_LOCK});

                return;
            }

            if(!appendEntryResponse.success) {
                LOG_INFO("AppendEntryRequest directed to " << id << " failed due to log inconsistency: Reducing next index value from " << this->nextIndex.at(id));

                assert(this->nextIndex.at(id) > 0);
                this->nextIndex.at(id) = std::max(1, this->nextIndex.at(id)-1);
            }
            else {
                LOG_INFO("Entry successfully replicated on " << id << ": Now increasing replication aount from " << numServersReplicatedOn << " to " << (numServersReplicatedOn+1));

                this->matchIndex[id] = this->nextIndex.at(id);
                ++this->nextIndex.at(id);

                if(this->nonVotingMembers.find(id) == this->nonVotingMembers.end()) {
                    ++numServersReplicatedOn;
                }
            }
        }
        catch(TTransportException& e) {
            if(e.getType() == TTransportException::TTransportExceptionType::TIMED_OUT) {
                LOG_INFO("Timeout occurred while attempting to append entry to replica at " << id);

                Job retryJob = {(int) this->log.size()-1, id.hostname, id.port};
                this->jobsToRetry.push(retryJob);
                continue;
            }

            throw e;
        }
    }

    _return.leaderID = this->leader;

    if(numServersReplicatedOn < ((this->clusterMembership.size()-this->nonVotingMembers.size()) / 2) + 1) {
        LOG_INFO("Entry unsuccessfully replicated on a majority of servers: replication amount = " << numServersReplicatedOn  << "/" <<  ((this->clusterMembership.size() + 1 / 2) + 1));

        _return.success = false;
    }
    else {
        LOG_INFO("Entry successfully replicated on a majority of servers and writing mapping " << key << " => " << value << " to the state machine");

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

    this->lockHandler.releaseLocks({LockName::STATE_LOCK,
                                    LockName::LEADER_LOCK,
                                    LockName::CURR_TERM_LOCK,
                                    LockName::LOG_LOCK,
                                    LockName::COMMIT_INDEX_LOCK,
                                    LockName::VOTED_FOR_LOCK,
                                    LockName::NEXT_INDEX_LOCK,
                                    LockName::LAST_APPLIED_LOCK,
                                    LockName::MATCH_INDEX_LOCK,
                                    LockName::SNAPSHOT_LOCK,
                                    LockName::CLUSTER_MEMBERSHIP_LOCK});

    return;
}

void
Replica::kill() {
    std::stringstream idStream;
    idStream << this->myID.hostname;
    idStream << ":" << this->myID.port;
    idStream << " is now dying";

    LOG_INFO(idStream.str());

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
    this->lockHandler.acquireLocks({LockName::STATE_LOCK,
                                    LockName::CURR_TERM_LOCK,
                                    LockName::CLUSTER_MEMBERSHIP_LOCK,
                                    LockName::LEADER_LOCK});

    _return.success = true;

    if(this->state != ReplicaState::LEADER) {
        _return.success = false;
        if(!isANullID(this->leader)) {
            _return.leaderID = this->leader;
        }

        this->lockHandler.releaseLocks({LockName::STATE_LOCK,
                                        LockName::CURR_TERM_LOCK,
                                        LockName::CLUSTER_MEMBERSHIP_LOCK,
                                        LockName::LEADER_LOCK});

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

        try {
            ReplicaServiceClient client = getServiceClient(id.hostname, id.port);

            LOG_INFO("Now sending a getInformationHelper request to " << id << "; may take a while...");

            GetInformationHelperResponse getInformationHelperResponse;
            client.getInformationHelper(getInformationHelperResponse, this->currentTerm);

            if(getInformationHelperResponse.term > this->currentTerm) {
                this->currentTerm = getInformationHelperResponse.term;
                this->state = ReplicaState::FOLLOWER;
                this->votedFor = getNullID();
                _return.success = false;

                this->lockHandler.releaseLocks({LockName::STATE_LOCK,
                                                LockName::LEADER_LOCK,
                                                LockName::CURR_TERM_LOCK,
                                                LockName::CLUSTER_MEMBERSHIP_LOCK});

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

    this->lockHandler.releaseLocks({LockName::STATE_LOCK,
                                    LockName::CURR_TERM_LOCK,
                                    LockName::CLUSTER_MEMBERSHIP_LOCK,
                                    LockName::LEADER_LOCK});
}

void
Replica::timer() {
    std::this_thread::sleep_for(std::chrono::seconds(3));

    unsigned int minTimeoutMS = atoi(dotenv::env[MIN_ELECTION_TIMEOUT_ENV_VAR_NAME].c_str());
    
    std::stringstream msg;

    while(true) {
        this->lockHandler.acquireLocks({LockName::STATE_LOCK,
                                        LockName::LOG_LOCK,
                                        LockName::CURR_TERM_LOCK,
                                        LockName::TIMER_LOCK,
                                        LockName::COMMIT_INDEX_LOCK,
                                        LockName::VOTED_FOR_LOCK,
                                        LockName::LEADER_LOCK,
                                        LockName::NEXT_INDEX_LOCK,
                                        LockName::MATCH_INDEX_LOCK,
                                        LockName::SNAPSHOT_LOCK,
                                        LockName::CLUSTER_MEMBERSHIP_LOCK,
                                        LockName::WILLING_TO_VOTE_LOCK});

        if(this->state == ReplicaState::LEADER) {
            this->lockHandler.releaseLocks({LockName::STATE_LOCK,
                                            LockName::LOG_LOCK,
                                            LockName::CURR_TERM_LOCK,
                                            LockName::TIMER_LOCK,
                                            LockName::COMMIT_INDEX_LOCK,
                                            LockName::VOTED_FOR_LOCK,
                                            LockName::LEADER_LOCK,
                                            LockName::NEXT_INDEX_LOCK,
                                            LockName::MATCH_INDEX_LOCK,
                                            LockName::SNAPSHOT_LOCK,
                                            LockName::CLUSTER_MEMBERSHIP_LOCK,
                                            LockName::WILLING_TO_VOTE_LOCK});

            std::this_thread::sleep_for(std::chrono::milliseconds(300));
            continue;
        }

        if(this->timeLeft == 0) {
            LOG_INFO("Time has expired!");

            unsigned int votesReceived = 1;
            this->state = ReplicaState::CANDIDATE;
            this->votedFor = this->myID;
            this->timeLeft = this->timeout;
            ++(this->currentTerm);

            for(auto const& id : this->clusterMembership) {
                if(id == this->myID) {
                    continue;
                }

                LOG_INFO("Now requesting vote from " << id);

                try {
                    ReplicaServiceClient client = getServiceClient(id.hostname, id.port);

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
                        LOG_INFO("Timeout occurred while requesting a vote from " << id);
                    }
                    else {
                        LOG_INFO("Error while attempting to request a vote from " << id);
                    }
                }

                LOG_INFO(votesReceived << " votes have been received during this election");

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

                        try {
                            ReplicaServiceClient client = getServiceClient(id.hostname, id.port);

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
                                this->votedFor = getNullID();
                            }

                            if(!appendEntryResponse.success) {
                                LOG_INFO("AppendEntryRequest directed to " << id << " failed due to log inconsistency: Reducing nextIndex value from " << this->nextIndex.at(id));

                                int possibleNewNextIndex = this->nextIndex.at(id)-1;
                                this->nextIndex[id] = std::max(1, possibleNewNextIndex);
                            }
                            else {
                                LOG_INFO("AppendEntryRequest containing no-op directed to " << id << " successful: Increasing nextIndex value from " << this->nextIndex.at(id));

                                this->matchIndex[id] = this->nextIndex.at(id);
                                ++this->nextIndex.at(id);
                            }
                        }
                        catch(TTransportException& e) {
                            if(e.getType() == TTransportException::TTransportExceptionType::TIMED_OUT) {
                                LOG_INFO("Timeout occurred while asserting control of the replica at " << id);
                            }
                            else {
                                LOG_INFO("Error while attempting to assert control of the replica at " << id);
                            }
                        }
                    }

                    LOG_INFO("I have asserted control of the cluster!");

                    break;
                }
            }

            this->timeLeft = this->timeout;
        }
        --this->timeLeft;

	if((unsigned) this->timeout - this->timeLeft == minTimeoutMS) {
	    this->willingToVote = true;
	} 

        this->lockHandler.releaseLocks({LockName::STATE_LOCK,
                                        LockName::LOG_LOCK,
                                        LockName::CURR_TERM_LOCK,
                                        LockName::TIMER_LOCK,
                                        LockName::COMMIT_INDEX_LOCK,
                                        LockName::VOTED_FOR_LOCK,
                                        LockName::LEADER_LOCK,
                                        LockName::NEXT_INDEX_LOCK,
                                        LockName::MATCH_INDEX_LOCK,
                                        LockName::SNAPSHOT_LOCK,
                                        LockName::CLUSTER_MEMBERSHIP_LOCK,
                                        LockName::WILLING_TO_VOTE_LOCK});

        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
}

void
Replica::heartbeatSender() {
    std::stringstream msg;

    while(true) {
        this->lockHandler.acquireLocks({LockName::CURR_TERM_LOCK,
                                        LockName::LOG_LOCK,
                                        LockName::STATE_LOCK,
                                        LockName::COMMIT_INDEX_LOCK,
                                        LockName::VOTED_FOR_LOCK,
                                        LockName::NEXT_INDEX_LOCK,
                                        LockName::MATCH_INDEX_LOCK,
                                        LockName::SNAPSHOT_LOCK,
                                        LockName::CLUSTER_MEMBERSHIP_LOCK});

        if(this->state != ReplicaState::LEADER) {
            this->lockHandler.releaseLocks({LockName::CURR_TERM_LOCK,
                                            LockName::LOG_LOCK,
                                            LockName::STATE_LOCK,
                                            LockName::COMMIT_INDEX_LOCK,
                                            LockName::VOTED_FOR_LOCK,
                                            LockName::NEXT_INDEX_LOCK,
                                            LockName::MATCH_INDEX_LOCK,
                                            LockName::SNAPSHOT_LOCK,
                                            LockName::CLUSTER_MEMBERSHIP_LOCK});

            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            continue;
        }

        int old = this->commitIndex;
        this->commitIndex = this->findUpdatedCommitIndex();
        if(old != this->commitIndex) {
            LOG_INFO("Commit index changed from " << old << " to " << this->commitIndex);
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

            LOG_INFO("Now applying log entry " << this->log.at(this->lastApplied+1) << " to state machine");

            ++this->lastApplied;
            applyEntry(this->log.at(this->lastApplied));
        }

        for(const ID& id : this->clusterMembership) {
            if(id == this->myID) {
                continue;
            }

            Entry entryToSend = getEmptyLogEntry();
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
                ReplicaServiceClient client = getServiceClient(id.hostname, id.port);

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
                    this->votedFor = getNullID();
                    break;
                }


                if(!appendEntryResponse.success) {
                    LOG_INFO("AppendEntryRequest directed to " << id << " failed due to log inconsistency: Reducing nextIndex value from " << this->nextIndex.at(id));

                    int possibleNewNextIndex = this->nextIndex.at(id)-1;
                    this->nextIndex[id] = std::max(0, possibleNewNextIndex);
                }
                else if(entryToSend.type != EntryType::EMPTY_ENTRY) {
                    LOG_INFO("AppendEntryRequest directed to " << id << " successful: Increasing nextIndex value from " << this->nextIndex.at(id));

                    this->matchIndex[id] = this->nextIndex.at(id);
                    ++this->nextIndex.at(id);
                }
                else {
                    LOG_INFO("AppendEntryRequest (heartbeat) directed to " << id << " successful");
                }
            }
            catch(TTransportException& e) {
                if(e.getType() == TTransportException::TTransportExceptionType::TIMED_OUT) {
                    LOG_INFO("Timeout occurred while sending a heartbeat to the replica at " << id);
                }
                else {
                    LOG_INFO("Error while attempting to send an AppendEntryRequest to the replica at " << id);
                }
            }
        }

        this->lockHandler.releaseLocks({LockName::CURR_TERM_LOCK,
                                        LockName::LOG_LOCK,
                                        LockName::STATE_LOCK,
                                        LockName::COMMIT_INDEX_LOCK,
                                        LockName::VOTED_FOR_LOCK,
                                        LockName::NEXT_INDEX_LOCK,
                                        LockName::MATCH_INDEX_LOCK,
                                        LockName::SNAPSHOT_LOCK,
                                        LockName::CLUSTER_MEMBERSHIP_LOCK});

        std::this_thread::sleep_for(std::chrono::milliseconds(this->heartbeatTick));
    }
}

void
Replica::retryRequest() {
    unsigned int maxTimeToSpendRetryingMS = atoi(dotenv::env[MIN_ELECTION_TIMEOUT_ENV_VAR_NAME].c_str());

    std::stringstream msg;

    while(true) {
        while(this->jobsToRetry.empty()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }

        Job job = this->jobsToRetry.front();
        this->jobsToRetry.pop();

        unsigned int timeSpentOnCurrentRetryMS = 0;
        unsigned int timeoutMS = atoi(dotenv::env[RPC_RETRY_TIMEOUT_MIN_ENV_VAR_NAME].c_str());

        while(timeSpentOnCurrentRetryMS < (0.8 * maxTimeToSpendRetryingMS)) {
            std::this_thread::sleep_for(std::chrono::milliseconds(timeoutMS));

            this->lockHandler.acquireLocks({LockName::STATE_LOCK,
                                            LockName::LEADER_LOCK,
                                            LockName::CURR_TERM_LOCK,
                                            LockName::VOTED_FOR_LOCK,
                                            LockName::NEXT_INDEX_LOCK,
                                            LockName::MATCH_INDEX_LOCK,
                                            LockName::LOG_LOCK,
                                            LockName::SNAPSHOT_LOCK,
                                            LockName::CLUSTER_MEMBERSHIP_LOCK});

            Entry entry = this->log.at(job.entryPosition);

            if(this->state != ReplicaState::LEADER) {
                std::queue<Job> empty;
                std::swap(this->jobsToRetry, empty);

                this->lockHandler.releaseLocks({LockName::STATE_LOCK,
                                                LockName::LEADER_LOCK,
                                                LockName::CURR_TERM_LOCK,
                                                LockName::VOTED_FOR_LOCK,
                                                LockName::NEXT_INDEX_LOCK,
                                                LockName::MATCH_INDEX_LOCK,
                                                LockName::LOG_LOCK,
                                                LockName::SNAPSHOT_LOCK,
                                                LockName::CLUSTER_MEMBERSHIP_LOCK});

                break;
            }

            LOG_INFO("Retrying AppendEntryRequest " << entry << " after " << timeoutMS << " ms.");

            ID targetID;
            targetID.hostname = job.targetHost;
            targetID.port = job.targetPort;

            try {
                ReplicaServiceClient client = getServiceClient(job.targetHost, job.targetPort);

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
                    this->votedFor = getNullID();
                    std::queue<Job> empty;
                    std::swap(this->jobsToRetry, empty);

                    this->lockHandler.releaseLocks({LockName::STATE_LOCK,
                                                    LockName::LEADER_LOCK,
                                                    LockName::CURR_TERM_LOCK,
                                                    LockName::VOTED_FOR_LOCK,
                                                    LockName::NEXT_INDEX_LOCK,
                                                    LockName::MATCH_INDEX_LOCK,
                                                    LockName::LOG_LOCK,
                                                    LockName::SNAPSHOT_LOCK,
                                                    LockName::CLUSTER_MEMBERSHIP_LOCK});

                    break;
                }

                if(!appendEntryResponse.success) {
                    LOG_INFO("AppendEntryRequest retry to " << targetID << " failed due to log inconsistency: THIS SHOULD NEVER HAPPEN!");

                    this->lockHandler.releaseLocks({LockName::STATE_LOCK,
                                                    LockName::LEADER_LOCK,
                                                    LockName::CURR_TERM_LOCK,
                                                    LockName::VOTED_FOR_LOCK,
                                                    LockName::NEXT_INDEX_LOCK,
                                                    LockName::MATCH_INDEX_LOCK,
                                                    LockName::LOG_LOCK,
                                                    LockName::SNAPSHOT_LOCK,
                                                    LockName::CLUSTER_MEMBERSHIP_LOCK});
                }
                else {
                    LOG_INFO("Entry successfully replicated on " << targetID  << " during retry: Now increasing nextIndex value from " << this->nextIndex.at(targetID) << " to " << (this->nextIndex.at(targetID)+1));

                    this->matchIndex.at(targetID) = this->nextIndex.at(targetID);
                    ++this->nextIndex.at(targetID);

                    this->lockHandler.releaseLocks({LockName::STATE_LOCK,
                                                    LockName::LEADER_LOCK,
                                                    LockName::CURR_TERM_LOCK,
                                                    LockName::VOTED_FOR_LOCK,
                                                    LockName::NEXT_INDEX_LOCK,
                                                    LockName::MATCH_INDEX_LOCK,
                                                    LockName::LOG_LOCK,
                                                    LockName::SNAPSHOT_LOCK,
                                                    LockName::CLUSTER_MEMBERSHIP_LOCK});
                    break;
                }
            }
            catch(TTransportException& e) {
                if(e.getType() == TTransportException::TTransportExceptionType::TIMED_OUT) {
                    LOG_INFO("Timeout occurred while retrying an AppendEntryRequest to the replica at " << targetID);
                }
                else {
                    LOG_INFO("Non-Timeout error while attempting to retry an AppendEntryRequest to the replica at " << targetID);
                }
            }

            timeSpentOnCurrentRetryMS += timeoutMS;
            timeoutMS = ((double) timeoutMS) * 1.5;
        }
    }
}

int32_t
Replica::installSnapshot(const int32_t leaderTerm, const ID& leaderID, const int32_t lastIncludedIndex, const int32_t lastIncludedTerm, const int32_t offset, const std::string& data, const bool done) {
    this->lockHandler.acquireLocks({LockName::CURR_TERM_LOCK,
                                    LockName::TIMER_LOCK,
                                    LockName::LOG_LOCK,
                                    LockName::SNAPSHOT_LOCK});

    int termToReturn = std::max(this->currentTerm, leaderTerm);

    if(this->currentTerm > leaderTerm) {
        this->lockHandler.releaseLocks({LockName::CURR_TERM_LOCK,
                                        LockName::TIMER_LOCK,
                                        LockName::LOG_LOCK,
                                        LockName::SNAPSHOT_LOCK});
        return termToReturn;
    }

    this->currentTerm = termToReturn;

    this->timeLeft = this->timeout;

    std::stringstream compactionFileNameStream;
    compactionFileNameStream << dotenv::env[SNAPSHOT_FILE_ENV_VAR_NAME] << "-" << this->myID.hostname << ":" << this->myID.port;
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
        this->lockHandler.releaseLocks({LockName::CURR_TERM_LOCK,
                                        LockName::TIMER_LOCK,
                                        LockName::LOG_LOCK,
                                        LockName::SNAPSHOT_LOCK});
        return termToReturn;
    }


    for(unsigned int i = 0; i < this->log.size(); ++i) {
    }

    /*
    std::ifstream compactionFileStream(compactionFileName.c_str());
    compactionFileStream >> this->currentSnapshot;
    compactionFileStream.close();
    */

    this->lockHandler.releaseLocks({LockName::CURR_TERM_LOCK,
                                    LockName::TIMER_LOCK,
                                    LockName::LOG_LOCK,
                                    LockName::SNAPSHOT_LOCK});

    return termToReturn;
}

void
Replica::addNewConfiguration(AddConfigResponse& _return, const std::vector<ID>& newConfiguration, const std::string& clientIdentifier, const int32_t requestIdentifier) {
    this->lockHandler.acquireLocks({LockName::LOG_LOCK,
                                    LockName::CURR_TERM_LOCK,
                                    LockName::STATE_LOCK,
                                    LockName::COMMIT_INDEX_LOCK,
                                    LockName::CLUSTER_MEMBERSHIP_LOCK,
                                    LockName::NEXT_INDEX_LOCK,
                                    LockName::MATCH_INDEX_LOCK});

    if(this->state != ReplicaState::LEADER) {
        //return false;
    }

    if(requestIdentifier == this->currentRequestBeingServiced) {
        std::stringstream msg;
        LOG_INFO("Continuing servicing of request " << requestIdentifier);

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

        LOG_INFO("Is this entry from this term? = " << entryIsFromCurrentTerm);
        LOG_INFO("Has the entry been successfully replicated on a majority of replicas " << areAMajorityGreaterThanOrEqual(matchIndices, relevantEntryIndex));

        this->lockHandler.releaseLocks({LockName::LOG_LOCK,
                                        LockName::CURR_TERM_LOCK,
                                        LockName::STATE_LOCK,
                                        LockName::COMMIT_INDEX_LOCK,
                                        LockName::CLUSTER_MEMBERSHIP_LOCK,
                                        LockName::NEXT_INDEX_LOCK,
                                        LockName::MATCH_INDEX_LOCK});

        //return entryIsFromCurrentTerm && areAMajorityGreaterThanOrEqual(matchIndices, relevantEntryIndex);
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

    this->nonVotingMembers = newConfigurationEntry.nonVotingMembers;

    this->log.push_back(newConfigurationEntry);

    int replicationAmount = 0;

    for(const ID& id : this->clusterMembership) {
        if(id == this->myID) {
            continue;
        }

        try {
            ReplicaServiceClient client = getServiceClient(id.hostname, id.port);

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
                this->votedFor = getNullID();
                break;
            }

            if(!appendEntryResponse.success) {
                this->currentTerm = appendEntryResponse.term;
                this->state = ReplicaState::FOLLOWER;
                this->votedFor = getNullID();
            }
            else {
                ++replicationAmount;
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
                        this->votedFor = getNullID();
                        break;
                    }

                    if(!appendEntryResponse.success) {
                        this->currentTerm = appendEntryResponse.term;
                        this->state = ReplicaState::FOLLOWER;
                        this->votedFor = getNullID(); }
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

    this->lockHandler.releaseLocks({LockName::LOG_LOCK,
                                    LockName::CURR_TERM_LOCK,
                                    LockName::STATE_LOCK,
                                    LockName::COMMIT_INDEX_LOCK,
                                    LockName::CLUSTER_MEMBERSHIP_LOCK,
                                    LockName::NEXT_INDEX_LOCK,
                                    LockName::MATCH_INDEX_LOCK});

    //return true;
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

Snapshot
Replica::getSnapshot() {
    Snapshot newSnapshot;

    newSnapshot.lastIncludedIndex = this->log.size()-1 + this->currentSnapshot.lastIncludedIndex;
    newSnapshot.lastIncludedTerm = this->log.back().term;
    for(auto const& mapping : this->stateMachine) {
        newSnapshot.mappings.push_back(mapping);
    }

    return newSnapshot;
}

bool
ID::operator<(const ID& other) const {
    std::stringstream id1, id2;
    id1 << this->hostname << ":" << this->port;
    id2 << other.hostname << ":" << other.port;

    return id1.str() < id2.str();
}

std::ostream&
operator<<(std::ostream& os, const std::unordered_map<std::string, std::vector<std::string>>& stateMachine) {
    os << "[";
    unsigned int count = 0;
    for(auto it = stateMachine.begin(); it != stateMachine.end(); ++it) {
        os << it->first << "=>" << it->second.back();
        if(count < stateMachine.size()-1) {
            os << ", ";
        }
        ++count;
    }
    os << "]";

    return os;
}

std::ostream&
operator<<(std::ostream& os, const std::vector<Entry>& log) {
    os << "[";
    for(unsigned int i = 0; i < log.size(); ++i) {
        os << log[i];

        if(i < log.size()-1) {
            os << ", ";
        }
    }
    os << "]";

    return os;
}

std::ostream&
operator<<(std::ostream& os, const ReplicaState& state) {
    switch(state) {
        case ReplicaState::LEADER:
            os << "LEADER";
            break;
        case ReplicaState::CANDIDATE:
            os << "CANDIDATE";
            break;
        case ReplicaState::FOLLOWER:
            os << "FOLLOWER";
            break;
    };

    return os;
}

std::ostream&
operator<<(std::ostream& os, const Snapshot& snapshot) {
    os << snapshot.lastIncludedIndex << snapshot.lastIncludedTerm;

    for(auto const& mapping : snapshot.mappings) {
        os << mapping.first << '\n' << mapping.second.back() << '\n';
    }

    return os;
}

std::istream&
operator>>(std::istream& is, Snapshot& snapshot) {
    int lastIncludedIndex,
        lastIncludedTerm;

    is >> lastIncludedIndex >> lastIncludedTerm;

    snapshot.lastIncludedIndex = lastIncludedIndex;
    snapshot.lastIncludedTerm = lastIncludedTerm;

    /*
    while(is) {
        std::string key, value;
        is >> key >> value;

        std::stack<std::string>> valueStack;
        valueStack.push(value);

        snapshot.mappings.push_back({key, value});
    }
    */

    return is;
}
