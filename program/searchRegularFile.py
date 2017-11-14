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
import argparse
from time import gmtime, strftime
import errno    
import os
import multiprocessing
import time
import glob,os.path
from random import shuffle
from smartDirScanner import searchRegularFile, mkdir, getCurrDateTime
logger = MyLogger().getLogger()

if __name__ == "__main__":

    logger.info("Start to analysis directory, current time is: {0}".format(getCurrDateTime()))
    baseTime = getCurrDateTime()
    parser = argparse.ArgumentParser(description="This program is used to search all regular files in directorys that listed in dir file ")
    parser.add_argument("-i", "--dir_file_name", action="store", help="The file that contains the dir list", required=True)
    parser.add_argument("-o", "--output_dir", action="store", help="The directory that used for output result file", required=True)
    parser.add_argument("-p", "--process_name", action="store", help="The process name", required=True)
    args = parser.parse_args()

    dirFileName = args.dir_file_name
    processName = args.process_name
    outputDir = args.output_dir
    if(outputDir[-1] != '/'):
        outputDir = outputDir + "/"
    logger.info("Dir file name: {0}, process name: {1}, output dir: {2}".format(dirFileName, processName, outputDir))
    
    mkdir(outputDir)

    dirs = []
    with open(dirFileName) as f:
        for line in f:
            if(line[-1] == '\n'):
                line = line[0:-1]
            dirs.append(line)

    searchRegularFile(dirs, outputDir, processName) 
 

