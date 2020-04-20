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

def getTimePerRead(numReadsToPeform):
    print("measuring read performance...")
    totalTimeSpent = 0

    for key in range(numReadsToPerform):
        response = os.system(f'./mystore put {key} val')

        startTime = time.time()
        response = os.system(f'./mystore get {key}')
        totalTimeSpent += (time.time() - startTime)

    return totalTimeSpent / numReadsToPerform


def getTimePerWrite(numWritesToPerform):
    print("measuring write performance...")
    totalTimeSpent = 0

    for key in range(numWritesToPerform):
        startTime = time.time()
        response = os.system(f'./mystore put {key} val')
        totalTimeSpent += (time.time() - startTime)

    return totalTimeSpent / numWritesToPerform

if __name__ == "__main__":
    load_dotenv()

    parser = argparse.ArgumentParser(prog=f'{sys.argv[0]}')
    parser.add_argument("-reads", action="store_true")
    parser.add_argument("-writes", action="store_true")

    args = parser.parse_args()

    if not (args.reads or args.writes):
        print("You must specify at least one of [-reads, -writes]")
        sys.exit(1)
    if args.reads:
        numReadsToPerform = int(getenv(NUM_READS_ENV_VAR_NAME))
        timePerRead = getTimePerRead(numReadsToPerform)
        print(f'Time Per Read = {timePerRead} seconds / read')
    if args.writes:
        numWritesToPerform = int(getenv(NUM_WRITES_ENV_VAR_NAME))
        timePerWrite = getTimePerWrite(numWritesToPerform)
        print(f'Time Per Write = {timePerWrite} seconds / write')
