FROM thrift-base 

WORKDIR ../mystore

RUN mkdir tests

COPY ./*.cpp ./*.hpp CMakeLists.txt CMakeLists.txt.in  ./
COPY tests/* tests/

WORKDIR build

RUN cmake .. && make

ARG port

EXPOSE $port 

WORKDIR ..
