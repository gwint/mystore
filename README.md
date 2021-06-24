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
- For use with the command line tool:
	- The most recent version of the following Python modules: thrift, python-dotenv
- Apache Thrift version >= 0.13.0
- A Github Account (to pull in external projects used for logging and .env file reading during build)

## Installation

```
$ git clone https://github.com/gwint/mystore.git
$ Add location of libthrift-0.14.2.so to LD_LIBRARY_PATH (On ubuntu 20.10, /usr/local/lib was the path I used)
$ cd mystore
$ mkdir build
$ cd build
$ cmake ..
$ make
```

## Start Up
Start an instance with ./build/MyStore --listeningport <port-number> --clustermembership <ipaddr:port> ... <ipaddr:port>, where <ipaddr:port> is provided for every node in the cluster, ipaddr is an ip address, and port is a port number

Example: ./build/MyStore --listeningport 5000 --clustermembership 127.0.1.1:5000 127.0.1.1:5001 127.0.1.1:5002

## Interact with a MyStore cluster

Once all instances are started, the mystore command line tool can be used to retrieve and store key-value pairs.

```
$ ./mystore members -table 127.0.1.1:5000 127.0.1.1:5001 127.0.1.1:5002
$ ./mystore put akey aval 127.0.1.1:5000 127.0.1.1:5001 127.0.1.1:5002
$ ./mystore get akey 127.0.1.1:5000 127.0.1.1:5001 127.0.1.1:5002
$ ./mystore put akey aval2 127.0.1.1:5000 127.0.1.1:5001 127.0.1.1:5002
$ ./mystore get akey -rev 1 127.0.1.1:5000 127.0.1.1:5001 127.0.1.1:5002
```

See ./mystore -h for a listing of all available commands.
