#include <gtest/gtest.h>
#include <stdexcept>

#include "lockhandler.hpp"

#include <pthread.h>

TEST(LockHandlerTests, ConfirmMutexCreationTest) {
    LockHandler lockhandler(1);
    int numLocks = lockhandler.getNumLocks(); 
    ASSERT_EQ(numLocks, 1);
}

TEST(LockHandlerTests, ConfirmNonnegativeLockCountsAllowedTest) {
    LockHandler lockHandler(1); 
    ASSERT_EQ(true, true);
}

TEST(LockHandlerTests, ConfirmNegativeLockCountsNotAllowedTest) {
    ASSERT_THROW(LockHandler(-1), std::invalid_argument);
}

TEST(LockHandlerTests, LockAcquireTest) {
    LockHandler lockhandler(5);
    pthread_mutex_t* locks = lockhandler.getLocks();
    lockhandler.acquireLocks(LockName::CURR_TERM_LOCK, LockName::COMMIT_INDEX_LOCK);

    for(int i = 0; i < 5; ++i) {
        if(i == 1 || i == 3) {
            ASSERT_NE(0, pthread_mutex_trylock(&locks[i]));
        } 
    }
}


TEST(LockHandlerTests, LockReleaseTest) {
    ASSERT_THROW(LockHandler(1), std::invalid_argument);
}


int
main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
