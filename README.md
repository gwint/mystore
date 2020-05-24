MyStore is a distributed key-value store for string storage.  It is a decentralized
system that consists of processes, called nodes, running on serveral machines.  MyStore
is written in C++ and utilizes the Apache Thrift for remote procedure calls.  The RAFT
consensus algorithm is used to manage the replicated log that holds the mappings stored
within the system.  See the below resources for more information about RAFT:

- [In Search of an Understandable Consensus Algorithm](https://raft.github.io/raft.pdf)
- [Raft Visualization](https://raft.github.io/)

## Requirements

- C++ >= 14
- cmake >= 3.17.2
- Python >= 3.7 (used for command line tool)
- Apache Thrift version >= 0.13.0
- A Github Account (to pull in external projects used for logging and .env file reading during build)

## Installation

```
$ git clone https://github.com/gwint/mystore.git
$ cd mystore
$ mkdir build
$ cd build
$ cmake ..
$ make
```
Repeat these steps on each machine that will be apart of the MyStore cluster.

## Determine Cluster Membership

cluster.membership must exist to successfully operate a MyStore cluster.  Each line
in the file must have the form <ip-address>:<port #>.  For example, the following

    127.0.1.1:5000
    127.0.1.1:5001
    127.0.1.1:5002

would be the contents of cluster.membership for a cluster consisting of three replicas,
all running locally on ports 5000, 5001, and 5002, respectively.

## Start up and interact with a MyStore cluster

While inside the mystore directory, runnning ./build/MyStore <port-number> will
start a MyStore node listening on a port represented by <port-number>.  Once this has been done on
all machines whose information is included in cluster.membership, a command line tool can be used to
start the cluster's operation and store/retrieve mappings from the cluster.  For example, the following commands
will start the cluster, store a key-value pair within the cluster, and then retrieve the value associated with
the stored key:

```
$ ./mystore start
$ ./mystore put akey aval
$ ./mystore get akey
```

