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
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TTransport.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TThreadedServer.h>

#include "gen-cpp/replicaservice_types.h"

auto& test = dotenv::env.load_dotenv();

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
                                      currentRequestBeingServiced(std::numeric_limits<unsigned int>::max()),
                                      hasOperationStarted(false),
                                      clusterMembership(Replica::getClusterMembership()),
                                      lockHandler(13) {
    this->timeLeft = this->timeout;
    this->heartbeatTick = atoi(dotenv::env[Replica::HEARTBEAT_TICK_ENV_VAR_NAME].c_str());

    char hostBuffer[256];
    gethostname(hostBuffer, sizeof(hostBuffer));
    hostent* hostEntry = gethostbyname(hostBuffer);
    char* ip = inet_ntoa(*((struct in_addr*)
                           hostEntry->h_addr_list[0]));

    this->myID.hostname = std::string(ip);
    this->myID.port = port;

    this->lockHandler.lockAll();
}

void
Replica::requestVote(Ballot& _return, const int32_t term, const ID& candidateID, const int32_t lastLogIndex, const int32_t lastLogTerm) {
    printf("requestVote\n");
}

void
Replica::appendEntry(AppendEntryResponse& _return, const int32_t term, const ID& leaderID, const int32_t prevLogIndex, const int32_t prevLogTerm, const Entry& entry, const int32_t leaderCommit) {
    printf("appendEntry\n");
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
    printf("kill\n");
}

void
Replica::getInformation(std::map<std::string, std::string> & _return) {
    printf("getInformation\n");
}

void
Replica::start() {
    printf("start\n");
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

int main() {
    Replica replica(5000);

    return 0;
}
