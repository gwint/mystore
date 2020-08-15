#include "replica.hpp"

#include "argparse.hpp"

#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TTransport.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TThreadedServer.h>

int
main(int argc, const char** argv) {
    #ifndef NDEBUG
    std::cout << "The debug flag is set!" << std::endl;
    #endif

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
