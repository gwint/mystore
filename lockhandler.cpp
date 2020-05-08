#include <algorithm>

#include "lockhandler.hpp"
#include "locknames.hpp"

#include <pthread.h>

LockHandler::LockHandler(unsigned int numLocksIn) : numLocks(numLocksIn) {
    this->locks = new pthread_mutex_t[numLocksIn];

    for(unsigned int i = 0; i < numLocksIn; ++i) {
        pthread_mutex_init(&this->locks[i], NULL);
    }
}

LockHandler::~LockHandler() {
    delete[] this->locks;
}

/*
template <typename T, typename... Types>
void
LockHandler::releaseLocks(T lock, Types... rest) {
    std::vector<LockName> locksToRelease;
    LockHandler::collect(&locksToRelease, lock, rest...);

    std::sort(locksToRelease.begin(), locksToRelease.end());
    std::reverse(locksToRelease.begin(), locksToRelease.end());

    for(LockName lockName : locksToRelease) {
        pthread_mutex_unlock(&this->locks[lockName]);
    }
}
*/

void
LockHandler::lockAll() {
    for(unsigned int i = 0; i < this->numLocks; ++i) {
        pthread_mutex_lock(&this->locks[i]);
    }
}

void
LockHandler::unlockAll() {
    for(unsigned int i = 0; i < this->numLocks; ++i) {
        pthread_mutex_unlock(&this->locks[i]);
    }
}
