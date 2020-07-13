#include <algorithm>
#include <iostream>
#include <stdexcept>

#include "lockhandler.hpp"
#include "locknames.hpp"

#include <pthread.h>

LockHandler::LockHandler(int numLocksIn) : numLocks(numLocksIn) {
    if(numLocksIn <= 0) {
        throw std::invalid_argument("The number of locks this lockhandler is responsible for must be non-negative");
    }

    this->locks = new pthread_mutex_t[numLocksIn];

    for(unsigned int i = 0; i < numLocksIn; ++i) {
        pthread_mutex_init(&this->locks[i], NULL);
    }
}

LockHandler::~LockHandler() {
    delete[] this->locks;
}

int
LockHandler::getNumLocks() {
    return this->numLocks;
}

pthread_mutex_t*
LockHandler::getLocks() {
    return this->locks;
}
