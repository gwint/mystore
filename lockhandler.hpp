#ifndef LOCK_HANDLER_H
#define LOCK_HANDLER_H

#include <pthread.h>
#include <vector>

class LockHandler {
    private:
        unsigned int numLocks;
        pthread_mutex_t* locks;

    public:
        LockHandler(unsigned int);
        ~LockHandler();
        template <typename T, typename... Types> void acquireLocks(T, Types...);
        void acquireLocks();
        template <typename T, typename... Types> void releaseLocks(T, Types...);
        void releaseLocks();
};


#endif
