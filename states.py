from enum import IntEnum

class ReplicaState(IntEnum):
    LEADER = 1
    FOLLOWER = 2
    CANDIDATE = 3

    def __str__(self):
        intValue = int(self)

        if intValue == 1:
            return "LEADER"
        elif intValue == 2:
            return "FOLLOWER"
        else:
            return "CANDIDATE"
