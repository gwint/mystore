/**
 *
 *  bool        Boolean, one byte
 *  i8 (byte)   Signed 8-bit integer
 *  i16         Signed 16-bit integer
 *  i32         Signed 32-bit integer
 *  i64         Signed 64-bit integer
 *  double      64-bit floating point value
 *  string      String
 *  binary      Blob (byte array)
 *  map<t1,t2>  Map from one type to another
 *  list<t1>    Ordered list of one type
 *  set<t1>     Set of unique elements of one type
 *
 */

struct Entry {
    1: i32 key
    2: i32 value
    3: i32 term
}

struct Response {
    1: bool status,
    2: i32 term,
    3: i32 prev_log_index,
    4: i32 number_of_entries_added
}

struct Ballot {
    1: bool status,
    2: i32 term
}

service Replica {

    Ballot request_vote(1:i32 term,
                        2:i32 candidate_id,
                        3:i32 last_log_index,
                        4:i32 last_log_term),

    Response append_entry(1:i32 term,
                          2:i32 leader_id,
                          3:i32 prev_log_index,
                          4:i32 prev_log_term,
                          5:Entry entry,
                          6:i32 leader_commit)
}
