#ifndef REPLICA_H
#define REPLICA_H

#include <unordered_map>
#include <utility>
#include <queue>

#include "states.hpp"
#include "lockhandler.hpp"

#include "gen-cpp/replicaservice_types.h"
#include "gen-cpp/ReplicaService.h"

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

        Entry getEmptyLogEntry();
        static unsigned int getElectionTimeout();
        static std::vector<ID> getClusterMembership();

        bool isAtLeastAsUpToDateAs(unsigned int,
                                   unsigned int,
                                   unsigned int,
                                   unsigned int);

        static const char* MIN_ELECTION_TIMEOUT_ENV_VAR_NAME;
        static const char* MAX_ELECTION_TIMEOUT_ENV_VAR_NAME;
        static const char* CLUSTER_MEMBERSHIP_FILE_ENV_VAR_NAME;
        static const char* HEARTBEAT_TICK_ENV_VAR_NAME;
        static const char* RPC_TIMEOUT_ENV_VAR_NAME;
        static const char* RPC_RETRY_TIMEOUT_MIN_ENV_VAR_NAME;

    public:
        Replica(unsigned int);

        ReplicaState getState() const;
        unsigned int getTerm() const;
        std::vector<Entry> getLog() const;
        std::unordered_map<std::string, std::string>getStateMachine() const;
        unsigned int getCommitIndex() const;
        unsigned int getLastApplied() const;
        std::map<ID, unsigned int> getMatchIndex() const;
        std::map<ID, unsigned int> getNextIndex() const;

        void requestVote(Ballot&, const int32_t, const ID&, const int32_t, const int32_t);
        void appendEntry(AppendEntryResponse&, const int32_t, const ID&, const int32_t, const int32_t, const Entry&, const int32_t);
        void get(GetResponse&, const std::string&, const std::string&, const int32_t);
        void put(PutResponse&, const std::string&, const std::string&, const std::string&, const int32_t);
        void kill();
        void start();
        void getInformation(std::map<std::string, std::string> &);
};

#endif
