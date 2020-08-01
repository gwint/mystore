FROM ubuntu:bionic

RUN apt-get update && apt-get install -y git cmake autoconf libtool build-essential pkg-config automake bison flex g++ libboost-all-dev libevent-dev libssl-dev make thrift-compiler wget

RUN wget http://apache.mirrors.hoobly.com/thrift/0.13.0/thrift-0.13.0.tar.gz && tar -xvf thrift-0.13.0.tar.gz

WORKDIR thrift-0.13.0

RUN ./bootstrap.sh && ./configure && make && make install

ENV LD_LIBRARY_PATH=/thrift-0.13.0/lib/cpp/.libs/

WORKDIR ../mystore

RUN mkdir tests

COPY ./*.cpp ./*.hpp CMakeLists.txt CMakeLists.txt.in  ./
COPY tests/* tests/

WORKDIR build

RUN cmake .. && make
