#ifndef LOCKNAMES_H
#define LOCKNAMES_H

enum LockName {
    STATE_LOCK,
    CURR_TERM_LOCK,
    LOG_LOCK,
    COMMIT_INDEX_LOCK,
    LAST_APPLIED_LOCK,
    NEXT_INDEX_LOCK,
    MATCH_INDEX_LOCK,
    TIMER_LOCK,
    VOTED_FOR_LOCK,
    LEADER_LOCK,
    MAP_LOCK,
    CURRENT_REQUEST_ID_LOCK,
    LATEST_NO_OP_LOG_INDEX,
    SNAPSHOT_LOCK
};

#endif
