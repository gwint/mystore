<h1 align="center">
    <br>
    <img src="https://raft.github.io/logo/annie-solo.png" alt="Raft logo" width="200">
    <br>
    Raft - An understandable consensus algorithm
    <br>
    <br>
</h1>

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

### Where is consensus needed?

A consensus algorithm is needed in any setting where multiple copies of
something need to stay in agreement with one another.  NoSQL databases, which
can utilize replication for increased fault tolerance, provide one venue where
consensus is required.  MongoDB, for example, manages database replicas in a
manner mirrors the RAFT consensus algorithm.  Like RAFT, MongoDB seperates
its a replica set into a single "primary", who is responsible for accepting
client requests, and a set of "secondaries" which are updated by the primary.
Also, like RAFT, each mananages a log of operations to perform.  See [this](https://docs.mongodb.com/manual/replication/) for
more information on how MongoDB handles replication.

## Additional Raft Resources

- [In Search of an Understandable Consensus Algorithm](https://raft.github.io/raft.pdf)
- [Raft Visualization](https://raft.github.io/)
