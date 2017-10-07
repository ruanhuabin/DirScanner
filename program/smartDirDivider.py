#!/usr/bin/python
import os
import sys
from operator import itemgetter
from Logger import MyLogger
import logging
from datetime import datetime
import traceback
import argparse
from time import gmtime, strftime
import errno    
import os
import time
import math
def getCurrDateTime():
    """
    Get current date in specify format: YYYY-mm-dd-hh:mm:ss
    """
    currDate = str(datetime.now())
    dt = currDate.split()
    date = dt[0]
    time = dt[1].split('.')[0]
    currDateTime = date + '-' + time

    return currDateTime

logTime = getCurrDateTime()
logFile = "scaner_(" + logTime + ").log"
logger = MyLogger(logFileName = logFile).getLogger()
fileHandler = logger.handlers[1]
logFileName = fileHandler.baseFilename
"""
if old log file is exist, we trunk it into a empty log file 
"""
open(logFileName, "w")

def initDirGroups(processNum):
    dirGroups = {}
    for i in xrange(processNum):
        dirGroups["process_" + str(i)] = [0]

    return dirGroups

def assignToDirGroups(dirGroups, maxFileNum, dirName, fileNum):
    for(k, currSize) in dirGroups.iteritems():
        currGroupSize = dirGroups[k][0]
        if(currGroupSize < maxFileNum):
            dirGroups[k].append((dirName, fileNum))
            dirGroups[k][0] = dirGroups[k][0] + fileNum
            break


def divGroups(inputFile, outputFile, processNum):
    lineNum = 0
    dir2FileNumList = []
    with open(inputFile) as f:
        for line in f:
            lineNum = lineNum + 1
            if(line[-1] == '\n'):
                line = line[0:-1]

            #(dirPath, fileNum) = line.split()
            (fileNum, dirPath) = line.split(None, 1)
            dir2FileNumList.append((dirPath, fileNum))

            
    totalFileNum = sum([int(v[1]) for v in dir2FileNumList])
    logger.info("Total file num is: " + str(totalFileNum))
    fileNumPerProcess =int( math.ceil(float(totalFileNum) / float( processNum)))

    logger.info("File number for every process: " + str(fileNumPerProcess))
    dirGroups = initDirGroups(processNum)


    for item in dir2FileNumList:
        dirName = item[0]
        fileNum = int(item[1])
        assignToDirGroups(dirGroups, fileNumPerProcess, dirName, fileNum)


    fileHandle = open(outputFile, "w")
    fileHandle.write("Process Num: {0}, total file num: {1}, average file num for each process: {2}\n\n".format(processNum, totalFileNum, fileNumPerProcess))
    for(k, v) in dirGroups.iteritems():
        fileHandle.write("\n" + k + "\n")
        fileHandle.write("File number to process:" + str(v[0]) +  "\n")
        for item in v[1:]:
            fileHandle.write(str(item) + "\n")
    fileHandle.close()

    process2DirsList = []
    for(k, v) in dirGroups.iteritems():
        dirs = [ d[0] for d in v[1:]]
        item = (k, dirs)
        process2DirsList.append(item)

    finalProcess2DirsList = process2DirsList#sorted(process2DirsList, key=lambda x: x[0])
    return finalProcess2DirsList


        




if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="This program is used to make a balance division of dirs for process processing")
    parser.add_argument("-i", "--input_file", action="store", help="a file contains a list of 2-tuple of directory and number of regular file", required=True)
    parser.add_argument("-o", "--output", action="store", help="file for saving output result, default name is groups.txt", default='groups.txt')
    parser.add_argument("-n", "--process_num", action="store", type=int, help="Process number to be run", default=8)
    args = parser.parse_args()

    inputFile = args.input_file
    outputFile = args.output
    processNum = args.process_num
    logger.info("Input file is: " + inputFile)
    logger.info("Output file is: " + outputFile)
    logger.info("Process number is: " + str(processNum))


    process2DirsList = divGroups(inputFile, outputFile, processNum)
    for item in process2DirsList:
        logger.info("{0} ==> {1}".format(item[0], item[1]))
    logger.info("Done Successfully")
