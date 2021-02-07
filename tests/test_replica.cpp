#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <stdexcept>
#include <vector>
#include <algorithm>

#include "replica.hpp"
#include "utils.hpp"
#include "constants.hpp"

#include "replicaservice_types.h"
#include "ReplicaService.h"

#include "dotenv.h"

#include <pthread.h>

TEST(OperatorTests, ReplicaStateCoutOperatorTest) {
    std::streambuf* oldCoutStreamBuf = std::cout.rdbuf();
    std::ostringstream strCout;
    std::cout.rdbuf(strCout.rdbuf());

    std::cout << ReplicaState::FOLLOWER;
    ASSERT_EQ("FOLLOWER", strCout.str());
    strCout.str("");
    strCout.clear();

    std::cout << ReplicaState::LEADER;
    ASSERT_EQ("LEADER", strCout.str());

    strCout.str("");
    strCout.clear();

    std::cout << ReplicaState::CANDIDATE;
    ASSERT_EQ("CANDIDATE", strCout.str());

    std::cout.rdbuf(oldCoutStreamBuf);

}

TEST(OperatorTests, StateMachineCoutOperatorTest) {

    std::streambuf* oldCoutStreamBuf = std::cout.rdbuf();
    std::ostringstream strCout;
    std::cout.rdbuf(strCout.rdbuf());

    std::unordered_map<std::string, std::vector<std::string>> testStateMachine;
    testStateMachine["a"] = std::vector<std::string>(1, "b");
    testStateMachine["b"] = std::vector<std::string>(1, "c");
    testStateMachine["c"] = std::vector<std::string>();
    testStateMachine["c"].push_back("d");
    testStateMachine["c"].push_back("e");
    
    std::cout << testStateMachine;

    std::string expectedOutput = "[a=>b, b=>c, c=>e]";
    std::string capturedStr = strCout.str();

    ASSERT_TRUE(capturedStr.find("a=>b") != std::string::npos &&
                capturedStr.find("b=>c") != std::string::npos && 
                capturedStr.find("c=>e") != std::string::npos &&
                std::count(capturedStr.begin(), capturedStr.end(), ',') == 2 &&
                std::count(capturedStr.begin(), capturedStr.end(), '=') == 3 &&
                std::count(capturedStr.begin(), capturedStr.end(), '>') == 3);

    std::cout.rdbuf(oldCoutStreamBuf);
}

TEST(OperatorTests, LogCoutOperatorTest) {

    std::streambuf* oldCoutStreamBuf = std::cout.rdbuf();
    std::ostringstream strCout;
    std::cout.rdbuf(strCout.rdbuf());

    std::vector<Entry> log;
    Entry entry1;
    entry1.type = EntryType::EMPTY_ENTRY;
    log.push_back(entry1);
    
    std::cout << log;

    std::string expectedOutput = "[Entry(type=EMPTY_ENTRY, key=, value=, term=0, clientIdentifier=, requestIdentifier=0, newConfiguration=[], nonVotingMembers={})]";
    std::string capturedStr = strCout.str();

    ASSERT_EQ(expectedOutput, capturedStr);

    std::cout.rdbuf(oldCoutStreamBuf);
}

TEST(HelperFunctionTests, TestElectionTimeoutsInRange) {
    for(int i = 0; i < 10000; ++i) {
        unsigned int timeout = getElectionTimeout();
        ASSERT_TRUE(timeout >= atoi(dotenv::env[MIN_ELECTION_TIMEOUT_ENV_VAR_NAME].c_str()) &&
                    timeout <= atoi(dotenv::env[MAX_ELECTION_TIMEOUT_ENV_VAR_NAME].c_str())); 
    }
}

TEST(HelperFunctionTests, TestGetEmptyLogEntry) {
    Entry entry = getEmptyLogEntry();

    ASSERT_EQ(entry.type, EntryType::EMPTY_ENTRY);
    ASSERT_EQ(entry.key, "");
    ASSERT_EQ(entry.value, "");
    ASSERT_EQ(entry.term, -1);
    ASSERT_EQ(entry.clientIdentifier, "");
    ASSERT_EQ(entry.requestIdentifier, std::numeric_limits<int>::max());
}

TEST(HelperFunctionTests, TestGetMemberIDs) {
    std::vector<std::string> socketAddrs = {"127.0.1.1:5000", "127.0.1.1:5001", "127.0.1.1:5002"};
    std::vector<ID> ids = getMemberIDs(socketAddrs);

    ID id1;
    id1.hostname = "127.0.1.1";
    id1.port = 5000;
    ID id2;
    id2.hostname = "127.0.1.1";
    id2.port = 5001;
    ID id3;
    id3.hostname = "127.0.1.1";
    id3.port = 5002;

    ASSERT_EQ(socketAddrs.size(), ids.size());
    ASSERT_EQ(id1, ids.at(0));
    ASSERT_EQ(id2, ids.at(1));
    ASSERT_EQ(id3, ids.at(2));
}

TEST(HelperFunctionTests, TestGetNullID) {
    ID id = getNullID();

    ASSERT_EQ(id.hostname, "");
    ASSERT_EQ(id.port, 0);
}

TEST(HelperFunctionTests, TestIsANullID) {
    ID emptyID = ID();
    emptyID.hostname = "";
    emptyID.port = 0;

    ASSERT_TRUE(isANullID(emptyID));
}

TEST(HelperFunctionTests, TestAreAMajorityGreaterThanOrEqual) {
    std::vector<int> nums = {2, 4, 6, 8};

    ASSERT_TRUE(areAMajorityGreaterThanOrEqual(nums, 4));
    ASSERT_FALSE(areAMajorityGreaterThanOrEqual(nums, 8));
}

TEST(RPCTests, TestKill) {
    ASSERT_TRUE(false);
}

TEST(RPCTests, TestRequestVote) {
    ASSERT_TRUE(false);
}

TEST(RPCTests, TestAppendEntry) {
    ASSERT_TRUE(false);
}

int
main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
