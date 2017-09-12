#!/usr/bin/python
import os
import sys
from operator import itemgetter
from Logger import MyLogger
import logging
from datetime import datetime
import grp
import pwd
import traceback
import subprocess
from sets import Set

def extractFiles(inputFile1, inputFile2, logger):

    inputFile1List = []
    inputFile2List = []
    with open(inputFile1, "r") as f:
        for line in f:
            if(line[-1] == '\n'):
                line = line[:-1]
            inputFile1List.append(line)

    with open(inputFile2, "r") as f:
        for line in f:
            if(line[-1] == '\n'):
                line = line[:-1]
            inputFile2List.append(line)
    s1 = Set(inputFile1List)
    s2 = Set(inputFile2List)

    diff = s1.difference(s2)

    diff = list(diff)
    fileHandle = open("diff_result.txt", "w")
    for f in diff:
        fileHandle.write("{0}\n".format(f))

    fileHandle.close()

    logger.info("{0} files have left".format(len(diff)))


    







inputFile1 = "linn_old_2.txt"
inputFile2 = "linn_new.txt"

logger = MyLogger("Tool-Logger", logging.INFO).getLogger()

extractFiles(inputFile1, inputFile2, logger)

