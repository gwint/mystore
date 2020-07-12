#ifndef LOCK_HANDLER_H
#define LOCK_HANDLER_H

#include <pthread.h>
#include <vector>
#include <algorithm>

#include "locknames.hpp"

class LockHandler {
    private:
        int numLocks;
        pthread_mutex_t* locks;

    public:
        LockHandler(int);
        ~LockHandler();
        template <typename T, typename... Types> void acquireLocks(T, Types...);
        template <typename T, typename... Types> void releaseLocks(T, Types...);
        void lockAll();
        void unlockAll();
        template <typename T> static void collect(std::vector<T>&, T);
        template <typename T, typename ... Ts> static void collect(std::vector<T>&, T, Ts...);
        int getNumLocks();
};

template <typename T, typename... Types>
void
LockHandler::acquireLocks(T lock, Types... rest) {
    std::vector<LockName> locksToAcquire;
    LockHandler::collect(locksToAcquire, lock, rest...);

    std::sort(locksToAcquire.begin(), locksToAcquire.end());

    for(LockName lockName : locksToAcquire) {
        pthread_mutex_lock(&this->locks[lockName]);
    }
}

template <typename T, typename... Types>
void
LockHandler::releaseLocks(T lock, Types... rest) {
    std::vector<LockName> locksToRelease;
    LockHandler::collect(locksToRelease, lock, rest...);

    std::sort(locksToRelease.begin(), locksToRelease.end());
    std::reverse(locksToRelease.begin(), locksToRelease.end());

    for(LockName lockName : locksToRelease) {
        pthread_mutex_unlock(&this->locks[lockName]);
    }
}

template <typename T, typename ... Ts>
void LockHandler::collect(std::vector<T>& lst, T first, Ts ... rest) {
    lst.push_back(first);
    LockHandler::collect(lst, rest...);
}

template <typename T>
void LockHandler::collect(std::vector<T>& lst, T only) {
    lst.push_back(only);
}

#endif
