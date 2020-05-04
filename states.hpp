#ifndef STATES_H
#define STATES_H

#include <iostream>

enum ReplicaState {
    LEADER,
    FOLLOWER,
    CANDIDATE
};

std::ostream& operator<<(std::ostream& os, ReplicaState& state) {
    switch(state) {
        case LEADER:
            os << "LEADER";
            break;
        case FOLLOWER:
            os << "FOLLOWER";
            break;
        case CANDIDATE:
            os << "CANDIDATE";
            break;
    };

    return os;
}

#endif
