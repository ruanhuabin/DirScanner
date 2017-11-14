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
from smartDirScanner import searchRegularFile, mkdir, getCurrDateTime, getStatInfo
logger = MyLogger().getLogger()

if __name__ == "__main__":

    logger.info("Start to analysis directory, current time is: {0}".format(getCurrDateTime()))
    baseTime = getCurrDateTime()
    parser = argparse.ArgumentParser(description="This program is used to stat file that was listed in path_list_[n].txt file")
    parser.add_argument("-i", "--path_list_file", action="store", help="The file that contains the path list", required=True)
    parser.add_argument("-o", "--output_stat_file_name", action="store", help="The file name that used for save the stat result", required=True)
    parser.add_argument("-p", "--process_name", action="store", help="The process name", required=True)
    args = parser.parse_args()

    pathListFileName = args.path_list_file
    processName = args.process_name
    outputFileName = args.output_stat_file_name
    logger.info("path list file name: {0}, process name: {1}, output dir: {2}".format(pathListFileName, processName, outputFileName))
    
    group = []
    with open(pathListFileName) as f:
        for line in f:
            if(line[-1] == '\n'):
                line = line[0:-1]
            group.append(line)

    getStatInfo(group, outputFileName, outputFileName + ".invalid", processName)

