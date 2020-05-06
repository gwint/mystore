#include <iostream>
#include <string>

#include "replica.hpp"
#include "lockhandler.hpp"
#include "locknames.hpp"
#include "states.hpp"

#include "spdlog/spdlog.h"
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

struct Job {
    Job(int entryPositionIn,
        std::string targetHostIn,
        unsigned int targetPortIn) : entryPosition(entryPositionIn), targetHost(targetHostIn), targetPort(targetPortIn) {}

    int entryPosition;
    std::string targetHost;
    unsigned int targetPort;
};

int main() {
    std::cout << "Now running..." << '\n';

    return 0;
}
