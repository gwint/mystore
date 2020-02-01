from enum import Enum

class ReplicaState(Enum):
    LEADER = 1
    FOLLOWER = 2
    CANDIDATE = 3
