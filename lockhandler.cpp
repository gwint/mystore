#include <algorithm>
#include <iostream>
#include <stdexcept>

#include "lockhandler.hpp"
#include "locknames.hpp"
#include <vector>
#include <pthread.h>

LockHandler::LockHandler(int numLocksIn) {
    if(numLocksIn <= 0) {
        throw std::invalid_argument("The number of locks this lockhandler is responsible for must be non-negative");
    }

    for(unsigned int i = 0; i < numLocksIn; ++i) {
        this->mutexes.push_back(pthread_mutex_t());
        pthread_mutex_init(&this->mutexes.back(), NULL);
    }
}

std::vector<pthread_mutex_t>&
LockHandler::getLocks() {
    return this->mutexes;
}

void
LockHandler::acquireLocks(std::initializer_list<LockName> lockNames) {
    std::vector<LockName> locksToAcquire(lockNames.begin(), lockNames.end());

    std::sort(locksToAcquire.begin(), locksToAcquire.end());

    for(LockName lockName : locksToAcquire) {
        pthread_mutex_lock(&this->mutexes[lockName]);
    }
}

void
LockHandler::releaseLocks(std::initializer_list<LockName> lockNames) {
    std::vector<LockName> locksToRelease(lockNames.begin(), lockNames.end());

    std::sort(locksToRelease.begin(), locksToRelease.end());
    std::reverse(locksToRelease.begin(), locksToRelease.end());

    for(LockName lockName : locksToRelease) {
        pthread_mutex_unlock(&this->mutexes[lockName]);
    }
}
