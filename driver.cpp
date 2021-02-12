#include "replica.hpp"

#include "argparse.hpp"

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

    return 0;
}
