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

int
main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
