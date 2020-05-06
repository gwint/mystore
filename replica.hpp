#ifndef REPLICA_H
#define REPLICA_H

#include "states.hpp"

class Replica {
    private:
        ReplicaState state;
        unsigned int currentTerm;
        //std::vector<

    public:
        Replica(unsigned int);
        ReplicaState getState() const;
        unsigned int getTerm() const;
};

#endif
