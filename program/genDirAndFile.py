#!/bin/python

import sys
import os
import errno    
from Logger import MyLogger
import logging
from datetime import datetime
from time import gmtime, strftime
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

def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc:  # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise
logTime = getCurrDateTime()
logFile = "genDirFile_(" + logTime + ").log"
logger = MyLogger(logFileName = logFile).getLogger()
fileHandler = logger.handlers[1]
logFileName = fileHandler.baseFilename
"""
if old log file is exist, we trunk it into a empty log file 
"""
open(logFileName, "w")



dirListFile = "dirs.txt"
rootDir = "./"
if(len(sys.argv) == 2):
    dirListFile = sys.argv[1]
if(len(sys.argv) == 3):
    dirListFile = sys.argv[1]
    rootDir = sys.argv[2]

if(rootDir[-1] != "/"):
    rootDir = rootDir + "/"

lineNum = 0
fileIndex = 0
with open(dirListFile) as f:
    for line in f:
        line = line[0:-1]
        
        dirPath = rootDir + line
        mkdir_p(dirPath)
        lineNum = lineNum + 1
                
        """
        Generating 500 files for each directory
        """
        for i in xrange(200):
           fileIndex = fileIndex + 1
           fileName = "{0}/newFile_{1}".format(dirPath, fileIndex)
           fileHandle = open(fileName, "w")
           fileHandle.write("\nabcdefg")
           fileHandle.close()


        if(lineNum % 10 == 0):
            logger.info("{0} directories {1} files finish generating".format(lineNum, fileIndex))

        
logger.info("{0} directories {1} files finish generating".format(lineNum, fileIndex))
