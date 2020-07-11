#ifndef REPLICA_H
#define REPLICA_H

#include <unordered_map>
#include <set>
#include <utility>
#include <queue>
#include <stack>
#include <thread>

#include "lockhandler.hpp"

#include "gen-cpp/replicaservice_types.h"
#include "gen-cpp/ReplicaService.h"

#include "spdlog/spdlog.h"
#include "spdlog/sinks/basic_file_sink.h"
#include "spdlog/pattern_formatter.h"

class Replica;

struct Job {
    int entryPosition;
    std::string targetHost;
    int targetPort;
};

struct Snapshot {
    int lastIncludedIndex;
    int lastIncludedTerm;
    std::vector<std::pair<std::string, std::vector<std::string>>> mappings;
};

enum ReplicaState {
    LEADER,
    FOLLOWER,
    CANDIDATE
};

class Replica : virtual public ReplicaServiceIf {
    private:
        ReplicaState state;
        int currentTerm;
        int commitIndex;
        int lastApplied;
        std::vector<Entry> log;
        std::map<ID, int> nextIndex;
        std::map<ID, int> matchIndex;
        int timeout;
        int timeLeft;
        int heartbeatTick;
        ID myID;
        ID votedFor;
        ID leader;
        std::unordered_map<std::string, std::vector<std::string>> stateMachine;
        int currentRequestBeingServiced;
        std::queue<Job> jobsToRetry;
        bool hasOperationStarted;
        std::vector<ID> clusterMembership;
        std::set<ID> nonVotingMembers;
        LockHandler lockHandler;
        std::shared_ptr<spdlog::logger> logger;
        int noopIndex;
        std::thread timerThr;
        std::thread heartbeatSenderThr;
        std::thread retryThr;
        Snapshot currentSnapshot;
	bool willingToVote;

        static Entry getEmptyLogEntry();
        static unsigned int getElectionTimeout();
        static std::vector<ID> getMemberIDs(const std::vector<std::string>&);
        static ID getNullID();
        static bool isANullID(const ID&);

        bool isAtLeastAsUpToDateAs(unsigned int,
                                   unsigned int,
                                   unsigned int,
                                   unsigned int);

        unsigned int findUpdatedCommitIndex();

        void logMsg(std::string);

        Snapshot getSnapshot();

        static const char* MIN_ELECTION_TIMEOUT_ENV_VAR_NAME;
        static const char* MAX_ELECTION_TIMEOUT_ENV_VAR_NAME;
        static const char* CLUSTER_MEMBERSHIP_FILE_ENV_VAR_NAME;
        static const char* HEARTBEAT_TICK_ENV_VAR_NAME;
        static const char* RPC_TIMEOUT_ENV_VAR_NAME;
        static const char* RPC_RETRY_TIMEOUT_MIN_ENV_VAR_NAME;
        static const char* SNAPSHOT_FILE_ENV_VAR_NAME;
        static const char* MAX_ALLOWED_LOG_SIZE_ENV_VAR_NAME;

    public:
        Replica(unsigned int, const std::vector<std::string>&);

        void requestVote(Ballot&, const int32_t, const ID&, const int32_t, const int32_t);
        void appendEntry(AppendEntryResponse&, const int32_t, const ID&, const int32_t, const int32_t, const Entry&, const int32_t);
        void get(GetResponse&, const std::string&, const std::string&, const int32_t, const int32_t);
        void put(PutResponse&, const std::string&, const std::string&, const std::string&, const int32_t);
        void kill();
        void getInformationHelper(GetInformationHelperResponse &, const int32_t);
        void getInformation(GetInformationResponse &);
        int32_t installSnapshot(const int32_t, const ID&, const int32_t, const int32_t, const int32_t, const std::string&, const bool);
        bool addNewConfiguration(const std::vector<ID> &, const std::string&, const int32_t);
        void deletekey(DelResponse&, const std::string&, const std::string&, const int32_t);

        void timer();
        void heartbeatSender();
        void retryRequest();
};

std::ostream&
operator<<(std::ostream&, const std::unordered_map<std::string, std::vector<std::string>>&);

std::ostream&
operator<<(std::ostream&, const std::vector<Entry>&);

std::ostream&
operator<<(std::ostream&, const ReplicaState&);

std::ostream&
operator<<(std::ostream&, const Snapshot&);

std::istream&
operator>>(std::istream&, Snapshot&);

std::ostream&
operator<<(std::ostream&, std::vector<std::pair<std::string, std::string>>&);

#endif
