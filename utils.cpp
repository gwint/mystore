#include <stdlib.h>
#include <sstream>
#include <iostream>

#include "dotenv.h"

#include "constants.hpp"
#include "utils.hpp"

/*
Entry
getEmptyLogEntry() {
    Entry emptyLogEntry;
    emptyLogEntry.type = EntryType::EMPTY_ENTRY;
    emptyLogEntry.key = "";
    emptyLogEntry.value = "";
    emptyLogEntry.term = -1;
    emptyLogEntry.clientIdentifier = "";
    emptyLogEntry.requestIdentifier = std::numeric_limits<int>::max();

    return emptyLogEntry;
}
*/

unsigned int
getElectionTimeout() {
    unsigned int minTimeMS = atoi(dotenv::env[MIN_ELECTION_TIMEOUT_ENV_VAR_NAME].c_str());
    unsigned int maxTimeMS = atoi(dotenv::env[MAX_ELECTION_TIMEOUT_ENV_VAR_NAME].c_str());

    std::cout << minTimeMS << "\n";
    std::cout << maxTimeMS << "\n";

    srand(time(0));

    return (rand() % (maxTimeMS - minTimeMS)) + minTimeMS;
}

/*
std::vector<ID>
getMemberIDs(const std::vector<std::string>& socketAddrs) {
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
*/

/*
ID
getNullID() {
    ID nullID;
    nullID.hostname = "";
    nullID.port = 0;

    return nullID;
}

bool
isANullID(const ID& id) {
    return id.hostname == "" && id.port == 0;
}
*/

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
