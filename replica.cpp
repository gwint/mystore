#include <iostream>
#include <string>
#include <cstdio>
#include <cstdlib>
#include <limits>
#include <fstream>
#include <sstream>

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

bool
areAMajorityGreaterThanOrEqual(std::vector<unsigned int> numLst, unsigned int num) {
    unsigned int numForMajority = (numLst.size() / 2) + 1;
    unsigned int numGreaterThanOrEqual = 0;
    for(const unsigned int& currNum : numLst) {
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
                                      lockHandler(13) {

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

    this->lockHandler.lockAll();

    spdlog::set_pattern("[%H:%M:%S] %v");

    std::stringstream logFileNameStream;
    logFileNameStream << this->myID.hostname << ":" << this->myID.port << ".log";
    this->logger = spdlog::basic_logger_mt("file_logger", logFileNameStream.str());
}

void
Replica::requestVote(Ballot& _return, const int32_t term, const ID& candidateID, const int32_t lastLogIndex, const int32_t lastLogTerm) {
    printf("requestVote\n");
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
    this->logMsg("");
}

void
Replica::get(GetResponse& _return, const std::string& key, const std::string& clientIdentifier, const int32_t requestIdentifier) {
    printf("get\n");
}

void
Replica::put(PutResponse& _return, const std::string& key, const std::string& value, const std::string& clientIdentifier, const int32_t requestIdentifier) {
    printf("put\n");
}

void
Replica::kill() {
    std::stringstream idStream;
    idStream << this->myID.hostname;
    idStream << ":" << this->myID.port;
    idStream << " is now dying";

    std::cout << idStream.str() << std::endl;

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

Entry
Replica::getEmptyLogEntry() {
    Entry emptyLogEntry;
    emptyLogEntry.key = "";
    emptyLogEntry.value = "";
    emptyLogEntry.term = -1;

    return emptyLogEntry;
}

unsigned int
Replica::getElectionTimeout() {
    unsigned int minTimeMS = atoi(dotenv::env[Replica::MIN_ELECTION_TIMEOUT_ENV_VAR_NAME].c_str());
    unsigned int maxTimeMS = atoi(dotenv::env[Replica::MAX_ELECTION_TIMEOUT_ENV_VAR_NAME].c_str());

    srand(100);

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

    stateStream << this->state;
    logStream << this->log;
    stateMachineStream << this->stateMachine;

    std::stringstream displayStr;
    displayStr << message << " {0} {1} {2}";

    this->logger->info(displayStr.str(),
                       stateStream.str(),
                       logStream.str(),
                       stateMachineStream.str());
}

unsigned int
Replica::findUpdatedCommitIndex() {
    unsigned int possibleNewCommitIndex = this->log.size()-1;
    std::vector<unsigned int> indices;
    for(auto const& mapping : this->matchIndex) {
        indices.push_back(mapping.second);
    }
    indices.push_back(this->log.size()-1);

    while(possibleNewCommitIndex > this->commitIndex) {
        if(areAMajorityGreaterThanOrEqual(indices, possibleNewCommitIndex) &&
                      (unsigned)this->log[possibleNewCommitIndex].term == this->currentTerm) {
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

int main(int argc, char** argv) {
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
