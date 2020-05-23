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
    1: string key,
    2: string value,
    3: i32 term,
    4: string clientIdentifier,
    5: i32 requestIdentifier
}

struct AppendEntryResponse {
    1: bool success,
    2: i32 term,
    3: i32 prevLogIndex,
    4: i32 numberOfEntriesAdded
}

struct Ballot {
    1: bool voteGranted,
    2: i32 term
}

struct ID {
    1: string hostname,
    2: i32 port
}

struct PutResponse {
    1: i32 success,
    2: ID leaderID
}

struct GetResponse {
    1: bool success,
    2: string value,
    3: ID leaderID
}

service ReplicaService {

    Ballot requestVote(1:i32 term,
                       2:ID candidateID,
                       3:i32 lastLogIndex,
                       4:i32 lastLogTerm),

    AppendEntryResponse appendEntry(1:i32 term,
                         2:ID leaderID,
                         3:i32 prevLogIndex,
                         4:i32 prevLogTerm,
                         5:Entry entry,
                         6:i32 leaderCommit),

    GetResponse get(1:string key,
                    2:string clientIdentifier,
                    3:i32 requestIdentifier),

    PutResponse put(1:string key,
                    2:string value,
                    3:string clientIdentifier,
                    4:i32 requestIdentifier),

    oneway void kill(),

    map<string, string> getInformation(),

    oneway void start(),

    i32 installSnapshot(1:i32 leaderTerm,
                        2:ID leaderID,
                        3:i32 lastIncludedIndex,
                        4:i32 lastIncludedTerm,
                        5:i32 offset,
                        6:binary data,
                        7:bool done)
}
