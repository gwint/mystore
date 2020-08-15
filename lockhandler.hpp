#ifndef LOCK_HANDLER_H
#define LOCK_HANDLER_H

#include <pthread.h>
#include <vector>
#include <algorithm>
#include <initializer_list>

#include "locknames.hpp"

class LockHandler {
    private:
        std::vector<pthread_mutex_t> mutexes;

    public:
        LockHandler(int);
        void acquireLocks(std::initializer_list<LockName>);
        void releaseLocks(std::initializer_list<LockName>);
        std::vector<pthread_mutex_t>& getLocks();
};

#endif
