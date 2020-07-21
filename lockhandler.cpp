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
    }

    for(unsigned int i = 0; i < this->mutexes.size(); ++i) {
        pthread_mutex_init(&this->mutexes.at(i), NULL);
    }
}

std::vector<pthread_mutex_t>&
LockHandler::getLocks() {
    return this->mutexes;
}
