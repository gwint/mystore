#include <iostream>

#include "replica.hpp"
#include "lockhandler.hpp"
#include "locknames.hpp"
#include "states.hpp"

#include <thrift/transport/TSocket.h>
#include <thrift/transport/TTransport.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TThreadedServer.h>

//#include "gen-cpp/replicaservice_types.h"

int main() {
    std::cout << "Now running..." << '\n';

    return 0;
}
