#include <gtest/gtest.h>
#include <stdexcept>
#include <vector>
#include <algorithm>

#include "replica.hpp"
#include "lockhandler.hpp"

#include <pthread.h>

TEST(LockHandlerTests, ConfirmMutexCreationTest) {
    LockHandler lockhandler(1);
    std::vector<pthread_mutex_t>& locks = lockhandler.getLocks();
    int numLocks = locks.size(); 
    ASSERT_EQ(numLocks, 1);
}

TEST(LockHandlerTests, ConfirmNonnegativeLockCountsAllowedTest) {
    LockHandler lockHandler(1); 
    ASSERT_EQ(true, true);
}

TEST(LockHandlerTests, ConfirmNegativeLockCountsNotAllowedTest) {
    ASSERT_THROW(LockHandler(-1), std::invalid_argument);
}

TEST(LockHandlerTests, AcquireSingleLockTest) {
    LockHandler lockhandler(5);
    std::vector<pthread_mutex_t>& locks = lockhandler.getLocks();
    lockhandler.acquireLocks({LockName::CURR_TERM_LOCK});

    ASSERT_NE(0, pthread_mutex_trylock(&locks[1]));
}

TEST(LockHandlerTests, AcquireMultipleLockTest) {
    LockHandler lockhandler(5);
    std::vector<pthread_mutex_t>& locks = lockhandler.getLocks();
    lockhandler.acquireLocks({LockName::CURR_TERM_LOCK, LockName::COMMIT_INDEX_LOCK});

    ASSERT_NE(0, pthread_mutex_trylock(&locks[1]));
    ASSERT_NE(0, pthread_mutex_trylock(&locks[3]));
}

TEST(LockHandlerTests, ReleaseSingleLockTest) {
    
    LockHandler lockhandler(5);
    std::vector<pthread_mutex_t>& locks = lockhandler.getLocks();
    lockhandler.acquireLocks({LockName::CURR_TERM_LOCK});

    ASSERT_NE(0, pthread_mutex_trylock(&locks[1]));

    lockhandler.releaseLocks({LockName::CURR_TERM_LOCK});

    locks = lockhandler.getLocks();

    ASSERT_EQ(0, pthread_mutex_trylock(&locks[1]));
}

TEST(LockHandlerTests, ReleaseMultipleLockTest) {
    
    LockHandler lockhandler(5);
    std::vector<pthread_mutex_t>& locks = lockhandler.getLocks();
    lockhandler.acquireLocks({LockName::CURR_TERM_LOCK, LockName::COMMIT_INDEX_LOCK});

    ASSERT_NE(0, pthread_mutex_trylock(&locks[1]));
    ASSERT_NE(0, pthread_mutex_trylock(&locks[3]));

    lockhandler.releaseLocks({LockName::CURR_TERM_LOCK, LockName::COMMIT_INDEX_LOCK});

    locks = lockhandler.getLocks();

    ASSERT_EQ(0, pthread_mutex_trylock(&locks[1]));
    ASSERT_EQ(0, pthread_mutex_trylock(&locks[3]));
}

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

int
main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
