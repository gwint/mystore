#include <gtest/gtest.h>
#include <stdexcept>
#include <vector>

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
    lockhandler.acquireLocks(LockName::CURR_TERM_LOCK);

    ASSERT_NE(0, pthread_mutex_trylock(&locks[1]));
}

TEST(LockHandlerTests, AcquireMultipleLockTest) {
    LockHandler lockhandler(5);
    std::vector<pthread_mutex_t>& locks = lockhandler.getLocks();
    lockhandler.acquireLocks(LockName::CURR_TERM_LOCK, LockName::COMMIT_INDEX_LOCK);

    ASSERT_NE(0, pthread_mutex_trylock(&locks[1]));
    ASSERT_NE(0, pthread_mutex_trylock(&locks[3]));
}

TEST(LockHandlerTests, ReleaseSingleLockTest) {
    
    LockHandler lockhandler(5);
    std::vector<pthread_mutex_t>& locks = lockhandler.getLocks();
    lockhandler.acquireLocks(LockName::CURR_TERM_LOCK);

    ASSERT_NE(0, pthread_mutex_trylock(&locks[1]));

    lockhandler.releaseLocks(LockName::CURR_TERM_LOCK);

    locks = lockhandler.getLocks();

    ASSERT_EQ(0, pthread_mutex_trylock(&locks[1]));
}

TEST(LockHandlerTests, ReleaseMultipleLockTest) {
    
    LockHandler lockhandler(5);
    std::vector<pthread_mutex_t>& locks = lockhandler.getLocks();
    lockhandler.acquireLocks(LockName::CURR_TERM_LOCK, LockName::COMMIT_INDEX_LOCK);

    ASSERT_NE(0, pthread_mutex_trylock(&locks[1]));
    ASSERT_NE(0, pthread_mutex_trylock(&locks[3]));

    lockhandler.releaseLocks(LockName::CURR_TERM_LOCK, LockName::COMMIT_INDEX_LOCK);

    locks = lockhandler.getLocks();

    ASSERT_EQ(0, pthread_mutex_trylock(&locks[1]));
    ASSERT_EQ(0, pthread_mutex_trylock(&locks[3]));
}

int
main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
