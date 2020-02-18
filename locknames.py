from enum import IntEnum

class LockNames(IntEnum):
    STATE_LOCK = 0
    CURR_TERM_LOCK = 1
    LOG_LOCK = 2
    COMMIT_INDEX_LOCK = 3
    LAST_APPLIED_LOCK = 4
    NEXT_INDEX_LOCK = 5
    MATCH_INDEX_LOCK = 6
    TIMER_LOCK = 7
    VOTED_FOR_LOCK = 8
    LEADER_LOCK = 9
    MAP_LOCK = 10
