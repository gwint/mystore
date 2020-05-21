MyStore is a distributed key-value store for string storage.  It is a decentralized
system that consists of processes, called nodes, running on serveral machines.  MyStore
is written in C++ and utilizes the Apache Thrift for remote procedure calls.  The RAFT
consensus algorithm is used to manage the replicated log that holds the mappings stored
within the system.

## Why a Distributed Key-Value Store?

In short: they provide a reliable way to store important information and allow
that information to be accessed by applications of varied type and scale.
etcd is one open-source solution and it serves as the inspiration behind this
project.

## What is Raft?

Raft is a consensus algorithm that is designed to be easy to understand. It's
decomposed into relatively independent subproblems, and it cleanly
addresses all major pieces needed for practical systems.  This project
includes a distributed key-value store whose mappings are kept consistent across
a set of relicas using the Raft consensus algorithm and a series of threads
used to both perform writes and request snapshots of the mapppings present
at any given moment.

- [In Search of an Understandable Consensus Algorithm](https://raft.github.io/raft.pdf)
- [Raft Visualization](https://raft.github.io/)

## Requirements

- C++ >= 14
- cmake >= 3.17.2
- Python >= 3.7
- A Github Account (to pull in external projects for logging and .env file reading during build)

## Installation

```
$ git clone https://github.com/gwint/distributed-key-value-store.git
$ cd distributed-key-value-store
$ mkdir build
$ cd build
$ cmake ..
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
