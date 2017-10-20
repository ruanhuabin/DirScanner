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
import math
from random import shuffle

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

"""
File for save directory that is not allowed accessing by root
"""
exceptFileHandle = open("exception.log", "a")

def getLevel12Dirs(inputDir):
    """
    Obtain all dirs with the depth == 2, similar to use linux command find with -maxdepth to 2
    """
    if(inputDir[-1] != '/'):
        inputDir = inputDir + '/'

    """
    Obtain all level one dirs
    """
    fileDepth1 = glob.glob(inputDir + '/*')
    dirsDepth1 = filter(lambda f: os.path.isdir(f), fileDepth1);

    fileDepth1Hidden = glob.glob(inputDir + '/.*')
    dirsDepth1Hidden = filter(lambda f: os.path.isdir(f), fileDepth1Hidden);

    dirsDepth1 = dirsDepth1 + dirsDepth1Hidden

    """
    Remove dirs that is s symbol link
    """
    dirsDepth1Final = filter(lambda f: os.path.islink(f) == False, dirsDepth1)
    symbolLinkDirs = filter(lambda f: os.path.islink(f) == True, dirsDepth1)

    """
    Obtain all level two dirs
    """
    fileDepth2 = glob.glob(inputDir + '/*/*')
    dirsDepth2 = filter(lambda f: os.path.isdir(f) and os.path.islink(f) == False, fileDepth2);

    fileDepth2Hidden1 = glob.glob(inputDir + '/.*/*')
    dirsDepth2Hidden1 = filter(lambda f: os.path.isdir(f) and os.path.islink(f) == False, fileDepth2Hidden1);

    fileDepth2Hidden2 = glob.glob(inputDir + '/.*/.*')
    dirsDepth2Hidden2 = filter(lambda f: os.path.isdir(f) and os.path.islink(f) == False, fileDepth2Hidden2);

    fileDepth2Hidden3 = glob.glob(inputDir + '/*/.*')
    dirsDepth2Hidden3 = filter(lambda f: os.path.isdir(f) and os.path.islink(f) == False,  fileDepth2Hidden3);


    dirsDepth2 = dirsDepth2 + dirsDepth2Hidden1 + dirsDepth2Hidden2 + dirsDepth2Hidden3

    """
    Remove dirs that are located in symbol link dirs
    """
    dirsDepth2Final = []
    for item in dirsDepth2:
        dirName = os.path.dirname(item)
        if(dirName not in symbolLinkDirs):
            dirsDepth2Final.append(item)

    return (dirsDepth1Final, dirsDepth2Final)


def getLevel12RegularFiles(inputDir):
    """
    Obtain all regular files with the depth == 1 or == 2, similar to use linux command find with -maxdepth to 2
    """

    if(inputDir[-1] != '/'):
        inputDir = inputDir + '/'

    """
    Obtain all level one regular files 
    """
    fileDepth1 = glob.glob(inputDir + '/*')
    regular1Files = filter(lambda f: os.path.isfile(f) and os.path.islink(f) == False, fileDepth1);
    
    """
    Obtain all level one regular files 
    """
    fileDepth2 = glob.glob(inputDir + '/*/*')
    regular2Files = filter(lambda f: os.path.isfile(f) and os.path.islink(f) == False, fileDepth2);

    """
    Obtain all hidden regular files
    """
    fileDepth1Hidden = glob.glob(inputDir + '/.*')
    regular1FilesHidden = filter(lambda f: os.path.isfile(f) and os.path.islink(f) == False, fileDepth1Hidden);
    
    fileDepth2Hidden1 = glob.glob(inputDir + '/.*/*')
    regular2FilesHidden1 = filter(lambda f: os.path.isfile(f) and os.path.islink(f) == False, fileDepth2Hidden1);
    

    fileDepth2Hidden2 = glob.glob(inputDir + '/.*/.*')
    regular2FilesHidden2 = filter(lambda f: os.path.isfile(f) and os.path.islink(f) == False, fileDepth2Hidden2);

    fileDepth2Hidden3 = glob.glob(inputDir + '/*/.*')
    regular2FilesHidden3 = filter(lambda f: os.path.isfile(f) and os.path.islink(f) == False, fileDepth2Hidden3);

    regularFiles = regular1Files + regular2Files + regular1FilesHidden + regular2FilesHidden1 + regular2FilesHidden2 + regular2FilesHidden3;

    return regularFiles

def divDirsToGroups(dirs, groupNum = 1):

    """
    Divide all dir in dirs into groupNum groups 
    """
    groups = []
    for i in xrange(0, groupNum):
        groups.append([])

    totalDirsNum = len(dirs)
    groupSize = 1
    if(totalDirsNum > groupNum):
        if(totalDirsNum % groupNum == 0):
            groupSize = totalDirsNum / groupNum
        else:
            groupSize = totalDirsNum / groupNum + 1

    logger.debug("Group size is: {0}".format(groupSize))

    j = 0
    for i in xrange(0, totalDirsNum):
        groups[j].append(dirs[i])

        j = j + 1
        if(j == groupNum):
            j = 0

    return groups

   
def walkErrorHandle(oserror):
    logger.info("An Error happen")
    fileHandle = open("exc.log", 'a')
    traceback.print_exc(file=fileHandle)
    fileHandle.flush()
    fileHandle.close()
    
def searchRegularFile(dirs, outputDir, processName):
    """
    This function make a process called processName to search all the regular files recursively in directory in dirs
    """
    if(outputDir[-1] != '/'):
        outputDir = outputDir + "/"
    lineNum = 0
    fileHandle = open(outputDir + processName + ".txt", "w")



    symbolLinkFileNum = 0
    for item in dirs:
        for (root, currDirs, files) in os.walk(item, onerror=walkErrorHandle):
            """
            Firstly, we should remove symbol link dirs
            !!!No use: since os.walk will not follow the symbol link dir by default
            but the glob.glob will follow the symbol link
            """
            #for item in currDirs:
                #fullPath = root + "/" + item
                #if(os.path.islink(fullPath) == True):
                    #currDirs.remove(item)
            
            for f in files:
                filepath = os.path.join(root, f)
                if(os.path.islink(filepath) == True):
                    symbolLinkFileNum = symbolLinkFileNum + 1
                    if(symbolLinkFileNum % 1000 == 0):
                        logger.warn("{0}: {1} symbol link files found, current last one is: {2}".format(processName, symbolLinkFileNum, filepath))
                    continue
                try:
                    fileHandle.write("{0}\n".format(filepath))
                    lineNum = lineNum + 1
                    fileHandle.flush()
                except Exception as exception:
                    logger.error("File writing exception: {0}:{1}".format(filepath, str(exception)))
                    traceback.print_exc(file=sys.stdout)

                if(lineNum % 100000 == 0):
                    logger.info("{0}: {1} files have been found".format(processName, lineNum))

    fileHandle.close()
    logger.info("Process: {0} has finished searching directory: {1}".format(processName, str(dirs)))

def mkdir(path):
    try:
        os.makedirs(path)
    except OSError as exc: 
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise

def getTimeGap(time1, time2):
    """
    This function is used to get the time gap between two time variable.
    @input:The input time format is like: 2017-08-09-12:34:56
    @output:The item in result list is like: "200 days, 12:12:56"
    """
    FMT = '%Y-%m-%d-%H:%M:%S'
    tdelta = datetime.strptime(time2, FMT) - datetime.strptime(time1, FMT)    
    tdelta = str(tdelta)    
    items = tdelta.split(',')
    
    if(len(items) > 1):    
        return items
    else:
        return ['0 day', items[0]]

def sizeof_fmt(num, suffix='B'):
    for unit in ['','K','M','G','T','P','E','Z']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)



def getDayGap(t1, t2):
    timeGap = getTimeGap(t1, t2)
    dayGap = timeGap[0]
    dayGap = dayGap.split()[0]
    dayGap = int(dayGap)
    return dayGap


def genUserFileAccessPeriodInfo(statFileName, periodThreshold, outputDir="./"):
    """
    This function is used to gen information about how many files each user 
    had not accessed for more|less than periodThreshold days. 
    The result will be used to notify user to delete data with long time unaccess.
    @input: statFileName, a file contains file meta info
            periodThreshold, a day number used as classify basis.
            outputDir, a directory used to ouput the result file
    @output: a number of files contains the user file access period information
    """
    if(outputDir[-1] != "/"):
        outputDir = outputDir + "/"

    """
    Start to collect user -> file access period information, save result in user2FTS:
    key = userName, value = (fileName, accessTime, fileSize)
    """
    user2FTS = {}
    lineNum = 0
    with open(statFileName) as f:
        for line in f:
            line       = line[0:-1]
            metaInfo   = line.split('#')
            accessTime = metaInfo[1]
            fileName   = metaInfo[3]
            userName   = metaInfo[4]
            fileSize   = int(metaInfo[-1])
            #logger.info("({0}, {1}, {2}, {3})".format(accessTime, fileName, userName, fileSize))
            if(user2FTS.has_key(userName)):
                user2FTS[userName].append((fileName, accessTime, fileSize))
            else:
                user2FTS[userName] = [(fileName, accessTime, fileSize)]

            lineNum = lineNum + 1
            if(lineNum % 100000 == 0):
                logger.info("{0} lines finish processing".format(lineNum))

    logger.info("{0} lines finish processing".format(lineNum))

    """
    Split info in user2FTS to user2FG, user2FS, user2NSG, user2NSS based on periodThreshold classification
    user2FG  : user --> file path list,those files last access time is >= periodThreshold days
    user2NSG : user--> file total number, file total size,files last access time is >=  periodThreshold days
    user2FGS : user --> file path list,those files last access time is < periodThreshold days
    user2NSS : user--> file total number, file total size, files last access time is < periodThreshold days
    """
    user2FG  = {}
    user2FS  = {}
    user2NSG = {}
    user2NSS = {}
    baseTime = getCurrDateTime()
    lineNum = 0
    logger.info("baseTime is : {0}".format(baseTime))
    for(k, v) in user2FTS.iteritems():
        for vv in v:
            fileName   = vv[0]
            accessTime = vv[1]
            fileSize   = vv[2]
            dayGap     = getDayGap(accessTime, baseTime)
            """
            Put file path to user2FG if dayGap >= periodThreshold, else put it to user2FS
            """
            if(int(dayGap) >= int(periodThreshold)):
                if(user2FG.has_key(k)):
                    user2FG[k].append((fileName, fileSize))
                else:
                    user2FG[k] = [(fileName, fileSize)]
            else:
                if(user2FS.has_key(k)):
                    user2FS[k].append((fileName, fileSize))
                else:
                    user2FS[k] = [(fileName, fileSize)]
            lineNum = lineNum + 1
            if(lineNum % 100000 == 0):
                logger.info("{0} lines finish processing".format(lineNum))
    
    logger.info("{0} lines finish processing".format(lineNum))


    #logger.info("user2FG: " + str(user2FG))
    lineNum = 0
    for(k, v) in user2FG.iteritems():
        fileNum     = len(v)
        totalSize   = sum([vv[1] for vv in v])
        totalSize   = sizeof_fmt(totalSize)
        user2NSG[k] = (fileNum, totalSize)
        lineNum = lineNum + 1
        if(lineNum % 100000 == 0):
            logger.info("{0} lines finish processing".format(lineNum))
    
    logger.info("{0} lines finish processing".format(lineNum))

    lineNum = 0
    #logger.info("user2FS: " + str(user2FS))
    for(k, v) in user2FS.iteritems():
        fileNum     = len(v)
        totalSize   = sum([vv[1] for vv in v])
        totalSize   = sizeof_fmt(totalSize)
        user2NSS[k] = (fileNum, totalSize)
        lineNum = lineNum + 1
        if(lineNum % 100000 == 0):
            logger.info("{0} lines finish processing".format(lineNum))
    
    logger.info("{0} lines finish processing".format(lineNum))
 
    
    for(userName, v)in user2NSG.iteritems():
        outputFileName = outputDir + "user_" + userName + "_{0}+.txt".format(periodThreshold)
        fileHandle = open(outputFileName, "w")
        fileHandle.write("User ID:{0}\n".format(userName))
        fileHandle.write("File number  not accessed for more than {0} days: {1}\n".format(periodThreshold, v[0]))
        fileHandle.write("File total Size: {0}\n".format(v[1]))
        fileHandle.write("File list: \n".format(v[1]))
        fileHandle.close()
    for(userName, v)in user2NSS.iteritems():
        outputFileName = outputDir + "user_" + userName + "_{0}-.txt".format(periodThreshold)
        fileHandle = open(outputFileName, "w")
        fileHandle.write("User ID:{0}\n".format(userName))
        fileHandle.write("File number accessed within {0} days: {1}\n".format(periodThreshold, v[0]))
        fileHandle.write("File total Size: {0}\n".format(v[1]))
        fileHandle.write("File list: \n".format(v[1]))
        fileHandle.close()

    
    for(userName, v) in user2FG.iteritems():
        outputFileName = outputDir + "user_" + userName + "_{0}+.txt".format(periodThreshold)
        fileHandle = open(outputFileName, "a")
        for vv in v:
            fileHandle.write("{0}\n".format(vv[0]))
        fileHandle.close()

    for(userName, v) in user2FS.iteritems():
        outputFileName = outputDir + "user_" + userName + "_{0}-.txt".format(periodThreshold)
        fileHandle = open(outputFileName, "a")
        for vv in v:
            fileHandle.write("{0}\n".format(vv[0]))
        fileHandle.close()


def genGroupFileAccessPeriodInfo(statFileName, periodThreshold, outputDir="./"):
    """
    This function is used to gen information about how many files each user 
    had not accessed for more|less than periodThreshold days. 
    The result will be used to notify user to delete data with long time unaccess.
    @input: statFileName, a file contains file meta info
            periodThreshold, a day number used as classify basis.
            outputDir, a directory used to ouput the result file
    @output: a number of files contains the user file access period information
    """
    if(outputDir[-1] != "/"):
        outputDir = outputDir + "/"

    """
    Start to collect user -> file access period information, save result in user2FTS:
    key = userName, value = (fileName, accessTime, fileSize)
    """
    user2FTS = {}
    lineNum = 0
    with open(statFileName) as f:
        for line in f:
            line       = line[0:-1]
            metaInfo   = line.split('#')
            accessTime = metaInfo[1]
            fileName   = metaInfo[3]
            groupName   = metaInfo[5]
            fileSize   = int(metaInfo[-1])
            #logger.info("({0}, {1}, {2}, {3})".format(accessTime, fileName, groupName, fileSize))
            if(user2FTS.has_key(groupName)):
                user2FTS[groupName].append((fileName, accessTime, fileSize))
            else:
                user2FTS[groupName] = [(fileName, accessTime, fileSize)]

            lineNum = lineNum + 1
            if(lineNum % 100000 == 0):
                logger.info("{0} lines finish processing".format(lineNum))

    logger.info("{0} lines finish processing".format(lineNum))

    """
    Split info in user2FTS to user2FG, user2FS, user2NSG, user2NSS based on periodThreshold classification
    user2FG  : user --> file path list,those files last access time is >= periodThreshold days
    user2NSG : user--> file total number, file total size,files last access time is >=  periodThreshold days
    user2FGS : user --> file path list,those files last access time is < periodThreshold days
    user2NSS : user--> file total number, file total size, files last access time is < periodThreshold days
    """
    user2FG  = {}
    user2FS  = {}
    user2NSG = {}
    user2NSS = {}
    baseTime = getCurrDateTime()
    lineNum = 0
    logger.info("baseTime is : {0}".format(baseTime))
    for(k, v) in user2FTS.iteritems():
        for vv in v:
            fileName   = vv[0]
            accessTime = vv[1]
            fileSize   = vv[2]
            dayGap     = getDayGap(accessTime, baseTime)
            """
            Put file path to user2FG if dayGap >= periodThreshold, else put it to user2FS
            """
            if(int(dayGap) >= int(periodThreshold)):
                if(user2FG.has_key(k)):
                    user2FG[k].append((fileName, fileSize))
                else:
                    user2FG[k] = [(fileName, fileSize)]
            else:
                if(user2FS.has_key(k)):
                    user2FS[k].append((fileName, fileSize))
                else:
                    user2FS[k] = [(fileName, fileSize)]
            lineNum = lineNum + 1
            if(lineNum % 100000 == 0):
                logger.info("{0} lines finish processing".format(lineNum))
    
    logger.info("{0} lines finish processing".format(lineNum))


    #logger.info("user2FG: " + str(user2FG))
    lineNum = 0
    for(k, v) in user2FG.iteritems():
        fileNum     = len(v)
        totalSize   = sum([vv[1] for vv in v])
        totalSize   = sizeof_fmt(totalSize)
        user2NSG[k] = (fileNum, totalSize)
        lineNum = lineNum + 1
        if(lineNum % 100000 == 0):
            logger.info("{0} lines finish processing".format(lineNum))
    
    logger.info("{0} lines finish processing".format(lineNum))

    lineNum = 0
    #logger.info("user2FS: " + str(user2FS))
    for(k, v) in user2FS.iteritems():
        fileNum     = len(v)
        totalSize   = sum([vv[1] for vv in v])
        totalSize   = sizeof_fmt(totalSize)
        user2NSS[k] = (fileNum, totalSize)
        lineNum = lineNum + 1
        if(lineNum % 100000 == 0):
            logger.info("{0} lines finish processing".format(lineNum))
    
    logger.info("{0} lines finish processing".format(lineNum))
 
    
    for(groupName, v)in user2NSG.iteritems():
        outputFileName = outputDir + "group_" + groupName + "_{0}+.txt".format(periodThreshold)
        fileHandle = open(outputFileName, "w")
        fileHandle.write("Group ID:{0}\n".format(groupName))
        fileHandle.write("File number  not accessed for more than {0} days: {1}\n".format(periodThreshold, v[0]))
        fileHandle.write("File total Size: {0}\n".format(v[1]))
        fileHandle.write("File list: \n".format(v[1]))
        fileHandle.close()
    for(groupName, v)in user2NSS.iteritems():
        outputFileName = outputDir + "group_" + groupName + "_{0}-.txt".format(periodThreshold)
        fileHandle = open(outputFileName, "w")
        fileHandle.write("Group ID:{0}\n".format(groupName))
        fileHandle.write("File number accessed within {0} days: {1}\n".format(periodThreshold, v[0]))
        fileHandle.write("File total Size: {0}\n".format(v[1]))
        fileHandle.write("File list: \n".format(v[1]))
        fileHandle.close()

    
    for(groupName, v) in user2FG.iteritems():
        outputFileName = outputDir + "group_" + groupName + "_{0}+.txt".format(periodThreshold)
        fileHandle = open(outputFileName, "a")
        for vv in v:
            fileHandle.write("{0}\n".format(vv[0]))
        fileHandle.close()

    for(groupName, v) in user2FS.iteritems():
        outputFileName = outputDir + "group_" + groupName + "_{0}-.txt".format(periodThreshold)
        fileHandle = open(outputFileName, "a")
        for vv in v:
            fileHandle.write("{0}\n".format(vv[0]))
        fileHandle.close()



def addToDict(dictData, item):
    k = item[0]
    v1 = item[1]
    v2 = item[2]
    
    if(dictData.has_key(k)):
        dictData[k].append((v1, v2))
    else:
        dictData[k] = [(v1, v2)]


#Calculate time gap for each file type in different time period
def calculate_time_gap(ftGapDetail):

    tmGapTable = ["000-030", "030-060", "060-090", "090-120", "120-150", "150-180", "180-210", "210-240", "240-270", "270-300", "300-330", "330-360", 
                  "360-390", "390-420", "420-450", "450-480", "480-510", "510-540", "540-570", "570-600", "600-630", "630-660", "660-690", "690-720", "720-"]    
    #(key, value) in ftGapNew is like (".mrc", [("30 days", [file size in bytes]), ("65 days", [file size in bytes]) ...]) 
    ftGapNew = {}
    for(k,v) in ftGapDetail.iteritems():
        ext = k
        for vv in v:
            dayGap = vv[0].split(' ')[0]
            dayGap = int(dayGap)
            fileSize = vv[1]
        
            addToDict(ftGapNew, (ext, dayGap, fileSize))
        
    tmGapPeriod = {}
    tmGapMaxIndex = len(tmGapTable) - 1
    for(k,v) in ftGapNew.iteritems():

        for vv in v:
            tmIndex = vv[0] / 30
            fileSize = vv[1]
            
            if(tmIndex > tmGapMaxIndex):
                tmIndex = tmGapMaxIndex
            
            
            newKey = k + "(" + tmGapTable[tmIndex] + ")"
            if(tmGapPeriod.has_key(newKey)):
                currValue = tmGapPeriod[newKey]
                currCnt = currValue[0] + 1
                currSize = currValue[1] + fileSize
                currValue = (currCnt, currSize)
                tmGapPeriod[newKey] = currValue
            else:
                tmGapPeriod[newKey] =  (1, fileSize)
                
    """
    (key, value) in ftGapNew is like:{"[file extention]"==>[(dayGap, "this file size"), (dayGap, "this file size")], .....}
    (key value) in tmpGapPeriod is like: {".mrc(090-120)" ==> (120, 55555)}, means that there 120 files that were accessed in 90~120 days, these files total size is 55555 bytes 
    """
    return (ftGapNew, tmGapPeriod)
            
            
def genTypeTypePeriod2NumSize(inputFileName, outputTypeTimePeriod2FileSizeFileName, outputType2FileSizeFileName, baseTime):
    """
    Calculate each file's time gap between last access time and baseTime, here baseTime is specify by user.
    Also generate each file type's total number and corresponding type
    item format in inputFileName is like: [modifyTime]#[accessTime]#[changeTime]#[filepath]#owner#group#filesize       
    """
    lineNum = 0    
    fileTotalSize = 0
    """
    (key, value) in ftGapDetail is like("suffix of file, like .mrc", ["time gap result like "xxx days, hh:mm:ss"])
    """
    ftGapDetail = {}
    with open(inputFileName) as f:
        for line in f: 
            lineNum = lineNum + 1
            items = line.split('#')            
            fileLastAccessTime = items[1]
            fname = items[3]
            fileSize = items[-1]
            if(fileSize[-1] == '\n'):
                fileSize = fileSize[:-1]
            
            fileSize = int(fileSize)
            fileTotalSize = fileTotalSize + fileSize
            
            timeGap = getTimeGap(fileLastAccessTime, baseTime)
            (name, ext) = os.path.splitext(fname)
            
            if(len(ext) > 0 and ext[-1] == '\n'):
                ext = ext[:-1]
            if(ftGapDetail.has_key(ext)):                
                ftGapDetail[ext].append((timeGap[0], fileSize))
            else:
                ftGapDetail[ext] = [(timeGap[0], fileSize)]
            
            
            if(lineNum % 100000 == 0):
                logger.info("{0} lines have been finished processing".format(lineNum))
                
            
    logger.info("{0} lines have been finished processing".format(lineNum))
            
    (ftGapNew, tmGapPeriod) = calculate_time_gap(ftGapDetail)
    
    
    tmGapPeriodList = tmGapPeriod.iteritems()
    sortedGapPeriodList = sorted(tmGapPeriodList, key=lambda x:x[0], reverse = True)
    
    f = open(outputTypeTimePeriod2FileSizeFileName, "w")
    for item in sortedGapPeriodList:
        f.write("{0}#{1}#{2}\n".format(item[0], item[1][0], sizeof_fmt(item[1][1])))
    
    f.write("Total File Size: {0}\n".format(sizeof_fmt(fileTotalSize)))
    f.close()

    
    f = open(outputType2FileSizeFileName, "w")
    unSortedFTGapNew = []
    for (k, v) in ftGapNew.iteritems():
        item = (k, len(v), sum(vv[1] for vv in v))
        unSortedFTGapNew.append(item)
    
    sortedFTGapNew = sorted(unSortedFTGapNew, key = lambda x: x[2], reverse = True)
     
    for item in sortedFTGapNew:
        f.write("{0}#{1}#{2}\n".format(item[0], item[1], sizeof_fmt(item[2])))

    f.write("Total File Size: {0}\n".format(sizeof_fmt(fileTotalSize)))
    f.close()


def gen_user_grp_usage(filename, outputUser2UsedSizeFile, outputGrp2UsedSizeFile, outputGrp2UsersFile):
    """
    Analysis how many space each user and each group used
    """
    lineNum = 0    
    fileTotalSize = 0
    
    """
    (key, value) format is like: ("grpname", "{("username1"), ("number of files", "bytes used")} ")
    """
    grpUserUsage = {}
    userUsage = {}
    userGrp = {}
    grpUser = {}
    with open(filename) as f:
        for line in f: 
            lineNum = lineNum + 1
            items = line.split('#')
            fileSize = items[-1]
            grp = items[-2]            
            username = items[-3]
            userGrp[username] = grp
            
            if(fileSize[-1] == '\n'):
                fileSize = fileSize[:-1]
            
            fileSize = int(fileSize)
            fileTotalSize = fileTotalSize + fileSize
            
            if(userUsage.has_key(username)):
                currValue = userUsage[username]                             
                fileCnt = currValue[0] + 1
                bytesUsed = currValue[1] + fileSize
                
                currValue = (fileCnt, bytesUsed)
                userUsage[username] = currValue
            else:
                userUsage[username] = (1, fileSize)
            
            if(lineNum % 100000 == 0):
                logger.info("{0} lines finish processing".format(lineNum))
    
    for (username, usage) in userUsage.iteritems():
        #add user info to grp            
        if(grpUserUsage.has_key(userGrp[username]) ):
            grpUserUsage[userGrp[username]].append((username, userUsage[username][0], userUsage[username][1]))
        else:
            grpUserUsage[userGrp[username]] = [(username, userUsage[username][0], userUsage[username][1])]
            
    for(username, grp) in userGrp.iteritems():
        if(grpUser.has_key(grp)):
            grpUser[grp].append(username)
        else:
            grpUser[grp] = [username]
        
    logger.info("{0} lines finish processing".format(lineNum))
    fileHandle = open(outputUser2UsedSizeFile, "w")
    sortedUserUsage = sorted(userUsage.iteritems(), key = lambda x: x[1][1], reverse=True)   
    for item in sortedUserUsage:
        fileHandle.write("{0}#{1}#{2}\n".format(item[0], item[1][0], sizeof_fmt(item[1][1])))
    fileHandle.close()
    
    grpUserUsageList = []
    fileHandle = open(outputGrp2UsedSizeFile, "w")
    for (k, v) in grpUserUsage.iteritems():
        item = (k, len(v), sum(vv[2] for vv in v))
        grpUserUsageList.append(item)
    
    sortedGrpUserUsage = sorted(grpUserUsageList, key=lambda x: x[2], reverse = True)

    for item in sortedGrpUserUsage:
        fileHandle.write("{0}#{1}#{2}\n".format(item[0], item[1], sizeof_fmt(item[2])))
        
    fileHandle.close()
    
    fileHandle = open(outputGrp2UsersFile, "w")
    sortedGrpUser = sorted(grpUser.iteritems(), key=lambda x: x[0])
    for item in sortedGrpUser:
        fileHandle.write(str(item) + "\n")
    
    fileHandle.close()

def findAllRegularFile(dirGroups, outputDir, fileNamePrefix, processNum = 1):

    pool = multiprocessing.Pool(processes=processNum)
    result = []
    for i in xrange(processNum):
        processName = (fileNamePrefix + "_%d") %(i)
        result.append(pool.apply_async(searchRegularFile, (dirGroups[i], outputDir, processName)))
    pool.close()
    pool.join()

    #cnt = 0
    #for res in result:
        #print cnt, ":",  res.get()
        #cnt = cnt + 1

def getStatInfo(pathGroup, statResultFile, invalidResultFile, processName):
    """
    This function will be run by each process
    """
    lineNum = 0
    timeFileInfo = {}
    fileNotExist = []

    for fname in pathGroup:
        lineNum = lineNum + 1
        #Be carefull: this check is must, otherwise the file will not be found
        if(fname[-1] == '\n'):
            fname = fname[0:-1]
        if(lineNum % 10000 == 0):
            logger.info("{0}: {1} lines finish processing".format(processName, lineNum))

        """
        We only handle the file that is really exist, some file may no longer exist when this script runs, e.g:file may be  deleted by user
        """
        if(os.path.isfile(fname) == True):
            try:
                statInfo = os.stat(fname)
                modifyTime = datetime.fromtimestamp(statInfo.st_mtime).strftime('%Y-%m-%d-%H:%M:%S')
                accessTime = datetime.fromtimestamp(statInfo.st_atime).strftime('%Y-%m-%d-%H:%M:%S')
                changeTime = datetime.fromtimestamp(statInfo.st_ctime).strftime('%Y-%m-%d-%H:%M:%S')
                fileSize = str(statInfo.st_size)
                #Following two lines may raise exceptions
                owner = pwd.getpwuid(statInfo.st_uid)[0]
                group = grp.getgrgid(statInfo.st_gid)[0]

                timeFileInfo[fname] = (modifyTime, accessTime, changeTime, owner, group, fileSize)
            except Exception as exception:
                logger.info("{0}:File with exception: {1}, set it owner to {2}, grp to {3}".format(processName, fname, "unknow_user_" + str(statInfo.st_uid), "unknow_grp_" + str(statInfo.st_gid)))
                logger.warn("{0}:{1}:Exception Detail: {2}".format(processName, fname, str(exception)))
                traceback.print_exc(file=sys.stdout)
                timeFileInfo[fname] = (modifyTime, accessTime, changeTime, "unknow_user_" + str(statInfo.st_uid), "unknow_grp_" + str(statInfo.st_gid), fileSize)
        else:
            logger.error("{0}:File is not file: {1}".format(processName, fname))
            fileNotExist.append(fname)


    logger.info("{0}:{1} lines finish processing".format(processName, lineNum))


    fileHandle = open(statResultFile, "w")
    
    cnt = 0
    logger.info("{0}: Start to write stat result info to file:{1}".format(processName, statResultFile))
    for(fname, values) in timeFileInfo.iteritems():
        cnt = cnt + 1
        lineToWrite = values[0] + "#" + values[1] + "#" + values[2] + "#" + fname + "#" + values[3] + "#" + values[4] + "#" + values[5] +  "\n"
        fileHandle.write(lineToWrite)
    logger.info("{0}: End to write stat result info to file:{1}".format(processName, statResultFile))
    fileHandle.close()

    fileHandle = open(invalidResultFile, "w")
    logger.info("{0}: start to write invalid result info to file:{1}".format(processName, invalidResultFile))
    for f in fileNotExist:
        fileHandle.write("{0}\n".format(f))
    fileHandle.close()
    logger.info("{0}: End to write stat result info to file:{1}".format(processName, invalidResultFile))
    return (timeFileInfo, fileNotExist)


def statAllRegularFile(pathGroups, outputDir, fileNamePrefix, processNum = 1):

    pool = multiprocessing.Pool(processes=processNum)
    result = []
    for i in xrange(processNum):
        processName = (fileNamePrefix + "_%d") %(i)
        outputStatFile = outputDir + "stat_{0}".format(i) + ".txt"
        invalidStatFileName = outputDir + fileNamePrefix + "_{0}".format(i) + "_stat_invalid.txt"
        result.append(pool.apply_async(getStatInfo, (pathGroups[i], outputStatFile, invalidStatFileName, fileNamePrefix + "_{0}".format(i))))
    pool.close()
    pool.join()

def mergePathFiles(outputDir, fileNamePrefix, processNum, level12RegularFiles, outputFilePathName):

    allPaths = []
    for item in level12RegularFiles:
        allPaths.append(item)

    for i in xrange(0,processNum):
        fileName = outputDir + fileNamePrefix + "_{0}".format(i) + ".txt"
        with open(fileName) as f:
            for line in f: 
                allPaths.append(line)

        f.close()

    fileHandle = open(outputFilePathName, "w")
    for item in allPaths:
        if(item[-1] != '\n'):
            fileHandle.write(item + "\n")
        else:
            fileHandle.write(item)

        fileHandle.flush()

    fileHandle.close()

    return allPaths


def mergeStatFiles(statFileDir, fileNamePrefix,  processNum, statFileName):

    allStatInfo = []
    for i in xrange(0, processNum):
        fileName = statFileDir + fileNamePrefix + "_{0}".format(i) + ".txt"
        with open(fileName) as f:
            for line in f:
                allStatInfo.append(line)

        f.close()
  
    fileHandle = open(statFileName, "w")
    for item in allStatInfo:
        if(item[-1] != '\n'):
            fileHandle.write(item + "\n")
        else:
            fileHandle.write(item)

    fileHandle.close()

def divAllPathsToGroups(allPaths, groupNum):
    
    """
    Divide all dir in dirs into groupNum groups 
    """
    groups = []
    for i in xrange(0, groupNum):
        groups.append([])

    totalPathNum = len(allPaths)
    groupSize = 1
    if(totalPathNum > groupNum):
        if(totalPathNum % groupNum == 0):
            groupSize = totalPathNum / groupNum
        else:
            groupSize = totalPathNum / groupNum + 1

    logger.debug("Group size is: {0}".format(groupSize))

    j = 0
    for i in xrange(0, totalPathNum):
        groups[j].append(allPaths[i])

        j = j + 1
        if(j == groupNum):
            j = 0

    return groups

def genFileSizeRangeDict(classifySize=500, classifyNum=5):
   """
   Gen a range dict like rangeDict["0~~500MB"] to save the info that have  file size in range 0~~500MB
   """   
   rangeDict = {}
   for i in xrange(0, classifyNum - 1):
       """
       Note: if you put following two list out of for loop, then you will only get one path list and type list, all the 3rd element in rangeDict will be the same
       I pay two hours to find this logic error.
       """
       pathList = []
       typeList = []
       userList = []
       userGrpList = []
       low = "%5d"%(i * classifySize)
       high = "%5d"%(i * classifySize + classifySize)
       key = str(low) + "~~" + str(high)
       rangeDict[key] = [0,0,typeList,pathList,userList, userGrpList]

   key = "%5d"%((classifyNum - 1) * classifySize)
   key = str(key) + "+"
   """
   The value of rangeDict is define as a 4-tuple: [fileTotalSize, fileTotalNum, [filePathList],[type#num#size] ]
   """
   lastTypeList = []
   lastPathList = []
   lastUserList = []
   lastUserGrpList = []
   rangeDict[key] = [0,0,lastTypeList,lastPathList, lastUserList, lastUserGrpList]

   return rangeDict

def getRangeKey(rangeKeysList, fileSize, classifySize):
    """
    Get the file size in corresponding range
    """
    fileSizeInMB = fileSize / 1024 / 1024
    index = fileSizeInMB / classifySize

    rangeKeyNum = len(rangeKeysList)
    if(index < rangeKeyNum):
        return rangeKeysList[index]
    else:
        return rangeKeysList[-1]


def getTypeCnt(pathListInfo):
    """
    Get type 2 count info in path list
    """
    typeCntDict = {}
    lineNum = 0
    for item in pathListInfo:
        fname = item[0]
        if(fname[-1] == "\n"):
            fname = fname[0:-1]


        (name, ext) = os.path.splitext(fname)
        if(len(ext) == 0):
            """
            Here we use '$     $' to indicate file without suffix
            """
            ext = "$     $"

        if(typeCntDict.has_key(ext)):
            typeCntDict[ext] = typeCntDict[ext] + 1
        else:
            typeCntDict[ext] = 1

        lineNum = lineNum + 1
        if(lineNum % 100000 == 0):
            logger.info("{0} lines finish processing".format(lineNum))
    
    logger.info("{0} lines finish processing".format(lineNum))

    """
    Organize info like type->cnt
    """
    typeCntList = []
    for (key, value) in typeCntDict.iteritems():
        item = key + "->" + str(value)
        typeCntList.append(item)

    return sorted(typeCntList)

def getUserCntSize(pathListInfo):
    userCntSizeList = []
    userCntSizeDict = {}
    lineNum = 0
    for item in pathListInfo:
        fileSize = item[1]
        userName = item[2]
        userGrpName = item[3]

        if(userCntSizeDict.has_key(userName) == True):
            userCntSizeDict[userName][0] = userCntSizeDict[userName][0] + fileSize
            userCntSizeDict[userName][1] = userCntSizeDict[userName][1] + 1
        else:
            userCntSizeDict[userName] = [fileSize, 1]

        lineNum = lineNum + 1
        if(lineNum % 100000 == 0):
            logger.info("{0} lines finish processing".format(lineNum))

    logger.info("{0} lines finish processing".format(lineNum))
    for(k, v) in userCntSizeDict.iteritems():
        userCntSizeList.append((k, v[1], v[0]))
    
    userCntSizeListSorted = sorted(userCntSizeList, key=lambda x: x[2], reverse = True)
    userCntSizeListFinal = []

    for item in userCntSizeListSorted:
        userCntSizeListFinal.append((item[0], item[1], sizeof_fmt(item[2])))

    return userCntSizeListFinal

def getUserGrpCntSize(pathListInfo):
    userGrpCntSizeList = []
    userGrpCntSizeDict = {}
    lineNum = 0
    for item in pathListInfo:
        fileSize = item[1]
        userName = item[2]
        userGrpName = item[3]

        if(userGrpCntSizeDict.has_key(userGrpName) == True):
            userGrpCntSizeDict[userGrpName][0] = userGrpCntSizeDict[userGrpName][0] + fileSize
            userGrpCntSizeDict[userGrpName][1] = userGrpCntSizeDict[userGrpName][1] + 1
        else:
            userGrpCntSizeDict[userGrpName] = [fileSize, 1]
        lineNum = lineNum + 1
        if(lineNum % 100000 == 0):
            logger.info("{0} lines finish processing".format(lineNum))


    logger.info("{0} lines finish processing".format(lineNum))
    for(k, v) in userGrpCntSizeDict.iteritems():
        userGrpCntSizeList.append((k, v[1], v[0]))

    userGrpCntSizeListSorted = sorted(userGrpCntSizeList, key=lambda x: x[2], reverse = True)
    userGrpCntSizeListFinal = []

    for item in userGrpCntSizeListSorted:
        userGrpCntSizeListFinal.append((item[0], item[1], sizeof_fmt(item[2])))

    return userGrpCntSizeListFinal

def genFileSizeRangeDistribution(statInfoFile, classifySize=500, classifyNum=5, fileNameFlag="", outputDir="./"):
    lineNum = 0    
    fileTotalSize = 0

    fileSizeRangeDict = genFileSizeRangeDict(classifySize,classifyNum)
    rangeKeys = fileSizeRangeDict.keys()
    rangeKeys = sorted(rangeKeys)
    with open(statInfoFile) as f:
        for line in f: 
            lineNum = lineNum + 1
            items = line.split('#')            
            fileLastAccessTime = items[1]
            fname = items[3]
            fileSize = items[-1]
            userGrpName = items[-2]
            userName = items[-3]
            if(fileSize[-1] == '\n'):
                fileSize = fileSize[:-1]
            
            fileSize = int(fileSize)
            fileTotalSize = fileTotalSize + fileSize

            rangeKey = getRangeKey(rangeKeys, fileSize, classifySize)
            fileSizeRangeDict[rangeKey][0] = fileSizeRangeDict[rangeKey][0] + fileSize
            fileSizeRangeDict[rangeKey][1] = fileSizeRangeDict[rangeKey][1] + 1
            fileSizeRangeDict[rangeKey][3].append((fname, fileSize, userName, userGrpName))
            if(lineNum % 100000 == 0):
                logger.info("{0} lines finisned processing".format(lineNum))
            

    logger.info("{0} lines finisned processing".format(lineNum))
    for rangeKey in rangeKeys:
        fileSizeRangeDict[rangeKey][0] = sizeof_fmt(fileSizeRangeDict[rangeKey][0]) 
        fileSizeRangeDict[rangeKey][2] = getTypeCnt(fileSizeRangeDict[rangeKey][3])
        fileSizeRangeDict[rangeKey][4] = getUserCntSize(fileSizeRangeDict[rangeKey][3])
        fileSizeRangeDict[rangeKey][5] = getUserGrpCntSize(fileSizeRangeDict[rangeKey][3])

    """
    Output rangeDict result to file
    """
    if(outputDir[-1] != '/'):
        outputDir = outputDir + "/"
    for (key, value) in fileSizeRangeDict.iteritems():
        newKey = key.replace(' ', '')
        outputFilename = outputDir + newKey + fileNameFlag + ".txt"
        fileHandle = open(outputFilename, "w")
        fileHandle.write("Total Size: " + str(value[0]) + "\n")
        fileHandle.write("Total File Num: " + str(value[1]) + "\n")
        fileHandle.write("---------------------User 2 Cnt Num-----------------\n")
        for item in value[4]:
            fileHandle.write("{0}  #  {1}  #  {2}\n".format(item[0], item[1], item[2]))
        fileHandle.write("---------------------Grp 2 Cnt Num-----------------\n")
        for item in value[5]:
            fileHandle.write("{0}  #  {1}  #  {2}\n".format(item[0], item[1], item[2]))
        fileHandle.write("---------------------Type 2 Num-----------------\n")
        for item in value[2]:
            fileHandle.write(item + "\n")
        fileHandle.write("---------------------File List------------------\n")
        for item in value[3]:
            filePath = item[0]
            fileHandle.write(filePath + "\n")
        fileHandle.close()

    return fileSizeRangeDict
def calDir2RegularFileNum(pathFile, outputFile, depthLevel = 3):

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
        line = "%-30d%100s\n"%(item[1], item[0])
        fileHandle.write(line)
    fileHandle.close()

    return finalFilePathCounter

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


def divGroups(dir2FileNumList, outputFile, processNum):
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

    dirGroupsList = []
    for(k, v) in dirGroups.iteritems():
        dirs = [ d[0] for d in v[1:]]
        dirGroupsList.append(dirs)

    return dirGroupsList

def makeBalanceDirGroups(historyFilePathListFile, tmpOutputDir, processNum, level2Dirs):
    if(tmpOutputDir[-1] == "/"):
        tmpOutputDir = tmpOutputDir + "/"
    dir2RegularFileNumResultFile = tmpOutputDir + "dir2FileNum.txt"
    dir2RegularFileNumList = calDir2RegularFileNum(historyFilePathListFile, dir2RegularFileNumResultFile, 3)
    """
    Here we should remove dir in dir2RegularFileNumList that not in level2Dirs
    """
    logger.debug("Before removing dir not in level2Dirs, dir2RegularFileNumList len is {0}, dirs are: {1} ".format(len(dir2RegularFileNumList), str(dir2RegularFileNumList)) )
    finalDir2RegularFileNumList = []
    logger.info("dir2RegularFileNumList = " + str(dir2RegularFileNumList))
    for item in dir2RegularFileNumList:
        dirName = item[0][0:-1]
        if(dirName in level2Dirs):
            finalDir2RegularFileNumList.append(item)

    logger.debug("After removing dir not in level2Dirs, dir2RegularFileNumList len is {0}, dirs are: {1} ".format(len(finalDir2RegularFileNumList), str(finalDir2RegularFileNumList)) )
    process2DirsResultFile = tmpOutputDir + "process2Dirs.txt"
    dirGroups = divGroups(finalDir2RegularFileNumList, process2DirsResultFile, processNum)
    
    """
    Here we still need to add dir that in level2Dirs but not in finalDir2RegularFileNumList to finalDirGroups
    """
    logger.info("finalDir2RegularFileNumList = " + str(finalDir2RegularFileNumList))

    dirs1 = [v[0][0:-1] for v in finalDir2RegularFileNumList]
    logger.debug("dirs1: len:{0}, value:{1} ".format(len(dirs1), dirs1))
    dirs1 = set(dirs1)
    dirs2 = set(level2Dirs)
    logger.debug("dirs2: len:{0}, value:{1} ".format(len(list(dirs2)), list(dirs2)))
    newDirs = dirs2 - dirs1
    newDirs = list(newDirs)
    logger.debug("newDirs: len:{0}, value:{1} ".format(len(newDirs), newDirs))
    newDirsGroups = divDirsToGroups(newDirs, processNum)

    logger.debug("New Dir Group is: " + str(newDirsGroups))
    logger.debug("Start to assign new dir group to dirGroups")
    groupNum = len(newDirsGroups)
    for i in xrange(0, groupNum):
        dirGroups[groupNum - i - 1] = dirGroups[groupNum -i - 1] + newDirsGroups[i]
    logger.debug("End to assign new dir group to dirGroups")
    return dirGroups

def getHistoryAllPath(historyAllPathFile):
    historyAllPaths = []
    with open(historyAllPathFile) as f:
        for line in f:
            if(line[-1] == "\n"):
                filePath = line[0:-1]
            else:
                filePath = line
            historyAllPaths.append(filePath)
        f.close()
    return historyAllPaths

def getHistoryStatInfo(historyStatFile):
    histStatInfoDict = {}
    with open(historyStatFile) as f:
        for line in f:
            if(line[-1] == "\n"):
                line = line[0:-1]
            try:
                (m, a, c, f, u, g, s) = line.split('#')
            except Exception as exception:
                logger.error("Split stat info string error, there maybe too many # in stat info string:{0}:{1}".format(line, str(exception)))
                traceback.print_exc(file=sys.stdout)

            histStatInfoDict[f] = (m, a, c, u, g, s)

    return histStatInfoDict

def statFile(fname):
    if(os.path.isfile(fname) == True):
        try:
            statInfo = os.stat(fname)
            modifyTime = datetime.fromtimestamp(statInfo.st_mtime).strftime('%Y-%m-%d-%H:%M:%S')
            accessTime = datetime.fromtimestamp(statInfo.st_atime).strftime('%Y-%m-%d-%H:%M:%S')
            changeTime = datetime.fromtimestamp(statInfo.st_ctime).strftime('%Y-%m-%d-%H:%M:%S')
            fileSize = str(statInfo.st_size)
            #Following two lines may raise exceptions
            owner = pwd.getpwuid(statInfo.st_uid)[0]
            group = grp.getgrgid(statInfo.st_gid)[0]

            fileStatInfo = (modifyTime, accessTime, changeTime, owner, group, fileSize)
        except Exception as exception:
            fileStatInfo = (modifyTime, accessTime, changeTime, "unknow_user_" + str(statInfo.st_uid), "unknow_grp_" + str(statInfo.st_gid), fileSize)
            logger.warn("{0}:File with exception: {1}, set it owner to {2}, grp to {3}".format(processName, fname, "unknow_user_" + str(statInfo.st_uid), "unknow_grp_" + str(statInfo.st_gid)))
            logger.warn("{0}:{1}:Exception Detail: {2}".format(processName, fname, str(exception)))
            traceback.print_exc(file=sys.stdout)
    else:
        logger.error("File is not file: {0}".format(fname))

    return fileStatInfo

def mergeWithHistoryStatFile(historyStatFile, increasingFileList, reducingFileList, outputDir):

    finalStatFile= outputDir + "all_stat_result_final.txt"
    histStatInfoDict = getHistoryStatInfo(historyStatFile)

    reducingFileStatInfo = []
    reducingFileName = outputDir + "all_reducing_stat_result.txt"
    for (index, fn) in enumerate(reducingFileList):
        if(histStatInfoDict.has_key(fn)):
            v = histStatInfoDict[fn]
            reducingFileInfo = v[0] + "#" + v[1] + "#" + v[2] + "#" + fn + "#" +v[3] + "#" + v[4] + "#" + v[5]
            reducingFileStatInfo.append(reducingFileInfo)
            del histStatInfoDict[fn]
        if((index + 1) % 10000 == 0):
            logger.info("{0} lines finish processing".format(index + 1))


    for (index, fn) in enumerate(increasingFileList):
        statInfo = statFile(fn)
        histStatInfoDict[fn] = statInfo

        if((index + 1) % 10000 == 0):
            logger.info("{0} lines finish processing".format(index + 1))

    fileHandle = open(finalStatFile, "w")
    for k, v in histStatInfoDict.iteritems():
        item = v[0] + "#" + v[1] + "#" + v[2] + "#" + k + "#" +v[3] + "#" + v[4] + "#" + v[5]
        fileHandle.write(item + "\n")
    fileHandle.close()

    fileHandle = open(reducingFileName, "w")
    for item in reducingFileStatInfo:
        fileHandle.write(item + "\n")
    fileHandle.close()

    return (finalStatFile, reducingFileName)

def genResultFiles(outputDir, statFileName, fileUniqID, baseTime, periodThreshold):
    typeTypePeriod2NumSizeFile = outputDir + "type_type_period_2_num_size{0}.txt".format(fileUniqID)
    type2NumSizeFile = outputDir + "type_2_num_size{0}.txt".format(fileUniqID)
    genTypeTypePeriod2NumSize(statFileName, typeTypePeriod2NumSizeFile, type2NumSizeFile, baseTime) 
    logger.info("Gen type 2 num and size file [{0}] and type type period 2 num and size file [{1}] success".format(type2NumSizeFile, typeTypePeriod2NumSizeFile))

    user2UsageFile = outputDir + "user_usage{0}.txt".format(fileUniqID)
    grp2UsageFile = outputDir + "group_usage{0}.txt".format(fileUniqID)
    grp2UserFile = outputDir + "group_2_user{0}.txt".format(fileUniqID)
    gen_user_grp_usage(statFileName, user2UsageFile, grp2UsageFile, grp2UserFile)
    logger.info("Gen user to usage file, group 2 usage file ,group 2 user file success, file names are [{0}, {1}, {2}]".format(user2UsageFile, grp2UsageFile, grp2UserFile))
    
    genFileSizeRangeDistribution(statFileName, classifySize, classifyNum, fileUniqID, outputDir)
    logger.info("Gen file size distribution result files success")

    genUserFileAccessPeriodInfo(statFileName, periodThreshold, outputDir)
    genGroupFileAccessPeriodInfo(statFileName, periodThreshold, outputDir)


def genFinalResultFiles(outputDir, outputStatFileName, baseTime, periodThreshold):
    genResultFiles(outputDir, outputStatFileName, "_total", baseTime, periodThreshold)
    
def genIncreasingResultFiles(outputDir, increasingStatFileName, baseTime):
    genResultFiles(outputDir, increasingStatFileName, "_increasing", baseTime);

def genReducingResultFiles(outputDir, reducingStatFileName, baseTime):
    genResultFiles(outputDir, reducingStatFileName, "_reducing", baseTime)
    
if __name__ == "__main__":

    #genUserFileAccessPeriodInfo("../testdata/stat10.txt", 100, "./tmp/")
    #exit()

    logger.info("start to analysis directory, current time is: {0}".format(getCurrDateTime()))
    basetime =getCurrDateTime()
    parser = argparse.ArgumentParser(description="this program is used to scan all regular file in a dirctory and make some statistic operations")
    parser.add_argument("-i", "--dir_to_search", action="store", help="the directory to search", required=False)
    parser.add_argument("-s", "--stat_file_name", action="store", help="name of the linux stat command result file, default[all_stat_result.txt]", default="all_stat_result.txt")
    parser.add_argument("-d", "--output_dir", action="store", help="the directory to save the output result file, default[./result]", default="./result/")
    parser.add_argument("-m", "--tmp_dir", action="store", help="the tmpory directory for saving intermediat result file,default[./tmp/]", default="./tmp/")
    parser.add_argument("-p", "--filename_prefix", action="store", help="the prefix string for the file path file name, default[file_path]", default="file_path")
    parser.add_argument("-t", "--base_time", action="store", help="the datetime to compare with the last access time, format is: yyyy-mm-dd-hh:mm:ss, default is system current time",default=basetime) 
    parser.add_argument("-n", "--process_num", action="store", type=int, help="the process number to run, default is 8", default=8)
    parser.add_argument("-r", "--is_shuffle", action="store", type=int, help="whether to shuffle the dirs depth that >= 2 so as to make a possible blance for  process loading ,default is 0 indicating not to shuffle ", choices=[0,1], default=0)
    parser.add_argument("-e", "--exclude_dirs", action="append", help="the directories that do not need to be searched, default is []", default=[])
    parser.add_argument("-o", "--operation", action="store", help="operation to execute", choices=['all', 'gen_file_distribution'], default='all')
    parser.add_argument("-c", "--classify_size", action="store", type=int, help="size in mb used to classify the result stat info, default is 1024mb", default=1024)
    parser.add_argument("-g", "--classify_num", action="store", type=int, help="num of classes to generate, default is to generate 2 classes", default=2)
    parser.add_argument("-x", "--is_used_mpi", action="store", type=int, help="whether to usd mpi process to run, default is 0 indicating not to use mpi", choices=[0,1], default=0)
    parser.add_argument("-y", "--period_threshold", action="store", type=int, help="int period value used to gen user file period info, default is 100 days", default=100)
    parser.add_argument("-b", "--previous_path_file", action="store", help="a file path list file that is genenerated in privious scanning, this file is used for a purpose of balance scanning ", default="None")
    parser.add_argument("-z", "--previous_stat_file", action="store", help="a stat file that is genenerated in privious scanning, this file is used for a purpose of increasing stat operation ", default="None")
    args = parser.parse_args()
    logger.info("directory to search:" + str(args.dir_to_search))
    logger.info("output dir is: " + args.output_dir)
    logger.info("directories that should not be searched: " + str(args.exclude_dirs))
    logger.info("date used as base time " + args.base_time)
    logger.info("operation to execute: " + args.operation)
    logger.info("intermediate result file output dir is: " + args.tmp_dir)
    logger.info("process number to run {0}".format(args.process_num))
    if(args.is_used_mpi == 1):
        logger.info("is used mpi: yes")
    else:
        logger.info("is used mpi: no")

    if(args.previous_path_file != "None"):
        logger.info("is used balance scanning: yes, history file path list file is: " + args.previous_path_file)
    else:
        logger.info("is used balance scanning: no, start a new scanning")
    op = args.operation
    baseTime = args.base_time
    intermediateResultDir = args.tmp_dir
    fileNamePrefix = args.filename_prefix
    outputDir = args.output_dir
    processNum = args.process_num
    dirToSearch = args.dir_to_search
    isShuffle = args.is_shuffle
    classifyNum = args.classify_num
    classifySize = args.classify_size
    isUsedMPI = args.is_used_mpi
    previousFilePathFile = args.previous_path_file
    previousStatFile = args.previous_stat_file
    periodThreshold = args.period_threshold

    if(previousFilePathFile != "None" and previousStatFile == "None"):
        logger.error("History path file and stat file must be specified simutaneously, using -b and -z to specify!!!")
        exit()

    if(previousFilePathFile == "None" and previousStatFile != "None"):
        logger.error("History path file and stat file must be specified simutaneously, using -b and -z to specify!!!")
        exit()

    """
    Append slash to the end of outputDir and intermediateResultDir
    """
    if(outputDir[-1] != '/'):
        outputDir = outputDir + "/"
    if(intermediateResultDir[-1] != '/'):
        intermediateResultDir = intermediateResultDir + "/"
    mkdir(outputDir)
    mkdir(intermediateResultDir)

    (level1Dirs, level2Dirs) = getLevel12Dirs(dirToSearch)

    """
    here we write the level2Dirs to file so that we can use mpi process the search the regular file
    """
    logger.info("start to write level 2 dirs to file level2Dirs.txt")
    fileHandle = open("level2Dirs.txt", "w");
    for item in level2Dirs:
        fileHandle.write(item + "\n");
    fileHandle.close()
    logger.info("end to write level 2 dirs to file level2Dirs.txt")

    level12RegularFiles = getLevel12RegularFiles(dirToSearch)
    logger.info("regular files in level 1&2 directory: " + str(level12RegularFiles))

    if(isShuffle == 1):
        logger.info("dir depth that >= 2 before shuffle: " + str(level2Dirs))
        shuffle(level2Dirs)
        logger.info("dir depth that >= 2 after shuffle: " + str(level2Dirs))

    dirGroups = divDirsToGroups(level2Dirs, processNum)  
    """
    if we have previous scanned file path list file, then we should make a balance dir dividing
    """
    if(previousFilePathFile != "None"):
        logger.info("start to make a balance dir dividing based on previous scanned file: " + previousFilePathFile)
        dirGroups = makeBalanceDirGroups(previousFilePathFile, intermediateResultDir, processNum, level2Dirs)
        logger.info("end to make a balance dir dividing based on previous scanned file: " + previousFilePathFile)
    logger.info("number of leve2dirs: " + str(len(level2Dirs)))
    logger.info("dir groups result:")
    dirgroupresultfile = intermediateResultDir + "dirgroup.txt"

    fileHandle = open(dirgroupresultfile, "w")
    index = 0
    for item in dirGroups:
        logger.info(item)
        fileHandle.write("process {0} dir list: len: {1}\n\n".format(index, len(item)))
        for v in item:
            fileHandle.write(v + "\n")
        
        fileHandle.write("\n")
        index = index + 1

    fileHandle.close()

    if(isUsedMPI == 1):
        """
        write dir group to each file so that each mpi process can read its dir file
        we save all dirs file and file path files to the intermediate result dir
        """
        for i in xrange(processNum):
            group = dirGroups[i]
            fname = intermediateResultDir + "dirs_" + str(i) + ".txt"
            fileHandle = open(fname, "w")
            for item in group:
                fileHandle.write(item + "\n")
            fileHandle.close()

        """
        mpiexec -n 180 -f machinefile ./genfilepath ./tmp ./result
        """
        mpiCommandGenFilePath = "mpiexec -f ./machinefile -n " + str(processNum) + " ./genfilepath" + " " + intermediateResultDir + " " + intermediateResultDir + " " + fileNamePrefix 

        logger.info("mpiCommandGenFilePath = " + mpiCommandGenFilePath)

        """
        here we use mpi program to find all regular files by a number of process.
        """
        os.system(mpiCommandGenFilePath)

    else:
        logger.info("start to find all regular files in directory that depth is > 2")
        findAllRegularFile(dirGroups, intermediateResultDir,  fileNamePrefix, processNum) 
        logger.info("end to find all regular files in directory that depth is > 2")

    logger.info("sleep for 2 seconds for merging file path files")
    time.sleep(2)
    logger.info("start to merge file path files")
    outputPathFileName = outputDir + "all_file_path_result.txt"
    allpaths = mergePathFiles(intermediateResultDir, fileNamePrefix, processNum, level12RegularFiles, outputPathFileName)
    logger.info("end to merge file path files")

    """
    we append the regular files in level 1 and 2 dir to the first process file path file
    """
    fileHandle = open(intermediateResultDir + fileNamePrefix + "_0.txt", "a+")
    for item in level12RegularFiles:
        fileHandle.write(item + "\n")
    fileHandle.close()


    logger.info("sleep 2 seconds and then to stat file ")
    time.sleep(2)
    """
    here we get the reducing file list and increasing file list
    """
    historyAllPaths = set()
    if(previousFilePathFile != "None"):
        historyAllPaths = getHistoryAllPath(previousFilePathFile)
        historyAllPaths = set(historyAllPaths)

    currAllPaths = []
    for item in allpaths:
        if(item[-1] == "\n"):
            currAllPaths.append(item[0:-1])
        else:
            currAllPaths.append(item)

    currAllPaths = set(currAllPaths)
    reducingFileList = list(historyAllPaths - currAllPaths)
    increasingFileList = list(currAllPaths - historyAllPaths)

    reducingfilename = intermediateResultDir + "reducingfile.txt"
    increasingfilename = intermediateResultDir + "increasingfile.txt"
    fileHandle1 = open(reducingfilename, "w")
    fileHandle2 = open(increasingfilename, "w")

    fileHandle1.write("reducing file num: {0}\n\n".format(len(reducingFileList)))
    for item in reducingFileList:
        fileHandle1.write(item + "\n")

    fileHandle1.close()

    fileHandle2.write("increasing file num: {0}\n\n".format(len(increasingFileList)))
    for item in increasingFileList:
        fileHandle2.write(item + "\n")
    fileHandle2.close()
    """
    end to get the increasing file list and reducing file list
    """
    #pathGroups = divallpathstogroups(allpaths, processNum)
    """
    get the path groups for stat operation based on the increasing file list
    """
    pathGroups = divAllPathsToGroups(increasingFileList, processNum)
    
    if(isUsedMPI == 1):
        """
        we write item in pathGroups to each file ,so that for every mpi process to do stat operation
        """
        for i in xrange(processNum):
            group = pathGroups[i]
            fname = intermediateResultDir + "path_list_" + str(i) + ".txt"
            fileHandle = open(fname, "w")
            for item in group:
                if(item[-1] == '\n'):
                    fileHandle.write(item)
                else:
                    fileHandle.write(item + "\n")

            fileHandle.close()

        """
        mpiexec -f machinefile -n 24 ./statfilepath tmp tmp huabin
        """
        mpicommandstatfilepath = "mpiexec -f ./machinefile -n " + str(processNum) + " ./statfilepath" + " " + intermediateResultDir + " " + intermediateResultDir + " " + fileNamePrefix 

        logger.info("mpicommandstatfilepath = " + mpicommandstatfilepath)

        """
        here we use mpi program to stat all regular files by a number of process.
        """
        os.system(mpicommandstatfilepath)
    else:
        statAllRegularFile(pathGroups, intermediateResultDir, fileNamePrefix, processNum)

    outputStatFileName = outputDir + args.stat_file_name 
    mergeStatFiles(intermediateResultDir, "stat", processNum, outputStatFileName)
    """
    mergeStatFiles merges the stat info that are increased, so here outputStatFileName is pointed to the file that contains the increased file info
    """
    increasingStatFileName = outputStatFileName
    logger.info("sleep 1 second then to generate various result file")
    time.sleep(1)
    """
    here we need to update stat result file based on increasingFileList and historystatinfo
    """
    if(previousFilePathFile != "None" and previousStatFile != "None"):
        (outputStatFileName, reducingStatFileName) = mergeWithHistoryStatFile(previousStatFile, increasingFileList, reducingFileList, outputDir)
        genIncreasingResultFiles(outputDir, increasingStatFileName, baseTime)
        genReducingResultFiles(outputDir, reducingStatFileName, baseTime)

    genFinalResultFiles(outputDir, outputStatFileName, baseTime, periodThreshold)
    exceptFileHandle.close()
