#include "lockhandler.hpp"

#include <pthread.h>

LockHandler::LockHandler(unsigned int numLocksIn) : numLocks(numLocksIn) {
    this->locks = new pthread_mutex_t[numLocksIn];

    for(int i = 0; i < numLocksIn; ++i) {
        pthread_mutex_init(&this->locks[i], NULL);
    }
}

LockHandler::~LockHandler() {
    delete[] this->locks;
}

template <typename T, typename... Types>
void LockHandler::acquireLocks(T lock, Types... rest) {
}

void LockHandler::acquireLocks() {
}

template <typename T, typename... Types>
void LockHandler::releaseLocks(T lock, Types... rest) {
}

void LockHandler::releaseLocks() {
}
