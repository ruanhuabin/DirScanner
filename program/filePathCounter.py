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


def calcuDir2RegularFileNum(pathFile, outputFile, depthLevel):
    logger.info("Input file is: " + pathFile)
    logger.info("Output file is: " + outputFile)


    filePathCounterDict = {}
    lineNum = 0
    lineNumSkipped = 0
    with open(pathFile) as f:
        for line in f:
            lineNum = lineNum + 1
            filePath = line
            if(line[-1] == '\n'):
                filePath = line

            dirPath = os.path.dirname(filePath)
            dirs = dirPath.split('/')
            """
            delete first element since it always an empty element
            """
            del dirs[0]

            dirNum = len(dirs)
            if(dirNum < depthLevel):
                lineNumSkipped = lineNumSkipped + 1
                continue
            
            #dirKey = "/" + dirs[0] + "/" + dirs[1] + "/" + dirs[2]
            dirKey = "/"
            for i in xrange(0, depthLevel):
                dirKey = dirKey +  dirs[i] + "/"
            
            if(filePathCounterDict.has_key(dirKey)):
                filePathCounterDict[dirKey] = filePathCounterDict[dirKey] + 1
            else:
                filePathCounterDict[dirKey] = 1
            
            if(lineNum % 100000 == 0):
                logger.info("{0} lines finish processing".format(lineNum))

    
    logger.info("{0} lines finish processing".format(lineNum))
    logger.info("{0} files's directory depth is less than {1}".format(lineNumSkipped, depthLevel))


    filePathCounterList = [(k, v) for (k, v) in filePathCounterDict.iteritems()] 
    finalFilePathCounter = sorted(filePathCounterList, key = lambda x:x[1], reverse=True)
    logger.info(finalFilePathCounter)
    fileHandle = open(outputFile, "w")
    for item in finalFilePathCounter:
        #line = "%-100s%30d\n"%(item[0], item[1])
        line = "%-30d%100s\n"%(item[1], item[0])
        fileHandle.write(line)
    fileHandle.close()


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="This program is used to count the regular file in level N directory based on input file path list file")
    parser.add_argument("-i", "--path_file", action="store", help="a file path list that contains the regular file path", required=True)
    parser.add_argument("-o", "--output", action="store", help="file for saving output result, default name is result.txt", default='result.txt')
    parser.add_argument("-d", "--depth", action="store", type=int, help="directory depth for counter, default is 3", default=3)
    args = parser.parse_args()
    inputFile = args.path_file
    outputFile = args.output
    dirDepth = args.depth

    calcuDir2RegularFileNum(inputFile, outputFile, dirDepth)
    exit()

    
    logger.info("Input file is: " + inputFile)
    logger.info("Output file is: " + outputFile)


    filePathCounterDict = {}
    lineNum = 0
    lineNumSkipped = 0
    with open(inputFile) as f:
        for line in f:
            lineNum = lineNum + 1
            filePath = line
            if(line[-1] == '\n'):
                filePath = line

            dirPath = os.path.dirname(filePath)
            dirs = dirPath.split('/')
            """
            delete first element since it always an empty element
            """
            del dirs[0]

            dirNum = len(dirs)
            if(dirNum < dirDepth):
                lineNumSkipped = lineNumSkipped + 1
                continue
            
            #dirKey = "/" + dirs[0] + "/" + dirs[1] + "/" + dirs[2]
            dirKey = "/"
            for i in xrange(0, dirDepth):
                dirKey = dirKey +  dirs[i] + "/"
            
            if(filePathCounterDict.has_key(dirKey)):
                filePathCounterDict[dirKey] = filePathCounterDict[dirKey] + 1
            else:
                filePathCounterDict[dirKey] = 1
            
            if(lineNum % 100000 == 0):
                logger.info("{0} lines finish processing".format(lineNum))

    
    logger.info("{0} lines finish processing".format(lineNum))
    logger.info("{0} files's directory depth is less than {1}".format(lineNumSkipped, dirDepth))


    filePathCounterList = [(k, v) for (k, v) in filePathCounterDict.iteritems()] 
    finalFilePathCounter = sorted(filePathCounterList, key = lambda x:x[1], reverse=True)
    logger.info(finalFilePathCounter)
    fileHandle = open(outputFile, "w")
    for item in finalFilePathCounter:
        line = "%-100s%30d\n"%(item[0], item[1])
        fileHandle.write(line)
    fileHandle.close()


    
    logger.info("Done Successfully")
