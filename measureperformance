#!/usr/bin/python3

import argparse
import sys
from dotenv import load_dotenv
from os import getenv
import time
import subprocess
import os

NUM_READS_ENV_VAR_NAME = "NUM_READS_FOR_PERFORMANCE_TEST"
NUM_WRITES_ENV_VAR_NAME = "NUM_WRITES_FOR_PERFORMANCE_TEST"

def getMyStoreTimePerRead():
    numReadsToPerform = int(getenv(NUM_READS_ENV_VAR_NAME))
    totalTimeSpent = 0

    for key in range(numReadsToPerform):
        response = os.system(f'./mystore put {key} val')

        startTime = time.time()
        response = os.system(f'./mystore get {key}')
        totalTimeSpent += (time.time() - startTime)

    return totalTimeSpent / numReadsToPerform

def getEtcdTimePerRead():
    numReadsToPerform = int(getenv(NUM_READS_ENV_VAR_NAME))
    totalTimeSpent = 0

    for key in range(numReadsToPerform):
        response = os.system(f'~/etcd/bin/etcdctl put {key} val')

        startTime = time.time()
        response = os.system(f'~/etcd/bin/etcdctl get {key}')
        totalTimeSpent += (time.time() - startTime)

    return totalTimeSpent / numReadsToPerform

def getMyStoreTimePerWrite():
    numWritesToPerform = int(getenv(NUM_WRITES_ENV_VAR_NAME))
    totalTimeSpent = 0

    for key in range(numWritesToPerform):
        startTime = time.time()
        response = os.system(f'./mystore put {key} val')
        totalTimeSpent += (time.time() - startTime)

    return totalTimeSpent / numWritesToPerform

def getEtcdTimePerWrite():
    numWritesToPerform = int(getenv(NUM_WRITES_ENV_VAR_NAME))
    totalTimeSpent = 0

    for key in range(numWritesToPerform):
        startTime = time.time()
        response = os.system(f'~/etcd/bin/etcdctl put {key} val')
        totalTimeSpent += (time.time() - startTime)

    return totalTimeSpent / numWritesToPerform

if __name__ == "__main__":
    load_dotenv()

    readOptionName = "-reads"
    writeOptionName = "-writes"
    etcdOptionName = "-etcd"
    mystoreOptionName = "-mystore"

    parser = argparse.ArgumentParser(prog=f'{sys.argv[0]}')
    parser.add_argument(readOptionName, action="store_true")
    parser.add_argument(writeOptionName, action="store_true")
    parser.add_argument(etcdOptionName, action="store_true")
    parser.add_argument(mystoreOptionName, action="store_true")

    args = parser.parse_args()

    if not (args.reads or args.writes):
        print("You must specify at least one of [-reads, -writes]")
        sys.exit(1)
    if not (args.mystore or args.etcd):
        print("You must specify at least one of [-mystore, -etcd]")
        sys.exit(1)

    callMap = {args.reads: {args.mystore: getMyStoreTimePerRead, \
                            args.etcd: getEtcdTimePerRead}, \
               args.writes: {args.mystore: getMyStoreTimePerWrite, \
                             args.etcd: getEtcdTimePerWrite}}

    timePerOp = callMap[True][True]()

    print(f'Time Per Operation = {timePerOp} seconds')
