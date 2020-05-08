#ifndef REPLICA_H
#define REPLICA_H

#include <unordered_map>
#include <utility>
#include <queue>
#include <thread>

#include "states.hpp"
#include "lockhandler.hpp"

#include "gen-cpp/replicaservice_types.h"
#include "gen-cpp/ReplicaService.h"

#include "spdlog/spdlog.h"
#include "spdlog/sinks/basic_file_sink.h"
#include "spdlog/pattern_formatter.h"

class Replica;

struct Job {
    Job(int entryPositionIn,
        std::string targetHostIn,
        unsigned int targetPortIn) :
                     entryPosition(entryPositionIn),
                     targetHost(targetHostIn),
                     targetPort(targetPortIn) {}

    int entryPosition;
    std::string targetHost;
    unsigned int targetPort;
};

class Replica : virtual public ReplicaServiceIf {

    private:
        ReplicaState state;
        unsigned int currentTerm;
        unsigned int commitIndex;
        unsigned int lastApplied;
        std::vector<Entry> log;
        std::map<ID, unsigned int> nextIndex;
        std::map<ID, unsigned int> matchIndex;
        unsigned int timeout;
        unsigned int timeLeft;
        unsigned int heartbeatTick;
        ID myID;
        ID votedFor;
        ID leader;
        std::unordered_map<std::string, std::string> stateMachine;
        unsigned int currentRequestBeingServiced;
        std::queue<Job> jobsToRetry;
        bool hasOperationStarted;
        std::vector<ID> clusterMembership;
        LockHandler lockHandler;
        std::shared_ptr<spdlog::logger> logger;
        unsigned int noopIndex;

        static Entry getEmptyLogEntry();
        static unsigned int getElectionTimeout();
        static std::vector<ID> getClusterMembership();
        static ID getNullID();

        bool isAtLeastAsUpToDateAs(unsigned int,
                                   unsigned int,
                                   unsigned int,
                                   unsigned int);

        unsigned int findUpdatedCommitIndex();

        void logMsg(std::string);

        static const char* MIN_ELECTION_TIMEOUT_ENV_VAR_NAME;
        static const char* MAX_ELECTION_TIMEOUT_ENV_VAR_NAME;
        static const char* CLUSTER_MEMBERSHIP_FILE_ENV_VAR_NAME;
        static const char* HEARTBEAT_TICK_ENV_VAR_NAME;
        static const char* RPC_TIMEOUT_ENV_VAR_NAME;
        static const char* RPC_RETRY_TIMEOUT_MIN_ENV_VAR_NAME;

    public:
        Replica(unsigned int);

        void requestVote(Ballot&, const int32_t, const ID&, const int32_t, const int32_t);
        void appendEntry(AppendEntryResponse&, const int32_t, const ID&, const int32_t, const int32_t, const Entry&, const int32_t);
        void get(GetResponse&, const std::string&, const std::string&, const int32_t);
        void put(PutResponse&, const std::string&, const std::string&, const std::string&, const int32_t);
        void kill();
        void start();
        void getInformation(std::map<std::string, std::string> &);

        void timer(int);
};

std::ostream&
operator<<(std::ostream& os, const std::unordered_map<std::string, std::string>& stateMachine) {
    os << "[";
    for(auto const& mapping : stateMachine) {
        os << mapping.first << "=>" << mapping.second << ", ";
    }
    os << "]";

    return os;
}

std::ostream&
operator<<(std::ostream& os, const std::vector<Entry>& log) {
    os << "[";
    for(unsigned int i = 0; i < log.size(); ++i) {
        os << log[i];

        if(i < log.size()) {
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

#endif
