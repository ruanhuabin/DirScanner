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
import argparse
from time import gmtime, strftime
import errno    
import os
import multiprocessing
import time
import glob,os.path
from random import shuffle
logger = MyLogger().getLogger()
fileHandler = logger.handlers[1]
logFileName = fileHandler.baseFilename
"""
if old log file is exist, we trunk it into a empty log file 
"""
open(logFileName, "w")

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
    Obtain all regular files with the depth == 1, similar to use linux command find with -maxdepth to 2
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
    #logger = MyLogger().getLogger()
    

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

    logger.info("Group size is: {0}".format(groupSize))

    j = 0
    for i in xrange(0, totalDirsNum):
        groups[j].append(dirs[i])

        j = j + 1
        if(j == groupNum):
            j = 0

    return groups

   
def searchRegularFile(dirs, outputDir, processName):
    """
    This function make a process called processName to search all the regular files recursively in directory in dirs
    """
    if(outputDir[-1] != '/'):
        outputDir = outputDir + "/"
    lineNum = 0
    fileHandle = open(outputDir + processName + ".txt", "w")
    #fileHandle.flush()
    #fileHandle.close()

    """
    we should sleep for some time so that the os.walk can find the output file
    No used!!!
    """
    #time.sleep(3)
    #fileHandle = open(outputDir + processName + ".txt", "a")
    for item in dirs:
        for (root, currDirs, files) in os.walk(item):
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
                    logger.warn("{0}: {1} is a symbol link, ignored".format(processName, filepath))
                    continue
                try:
                    fileHandle.write("{0}\n".format(filepath))
                    lineNum = lineNum + 1
                    fileHandle.flush()
                except Exception as exception:
                    logger.error("File with exception: {0}:{1}".format(filepath, str(exception)))
                    traceback.print_exc(file=sys.stdout)

                if(lineNum % 10000 == 0):
                    logger.info("{0}: {1} files have been found".format(processName, lineNum))

    fileHandle.close()

    logger.info("Process: {0} has finished searching directory: {1}".format(processName, str(dirs)))
   # return "done by process: " + processName



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
                
            
    
    #(key, value) in ftGapNew is like:{"[file extention]"==>[(dayGap, "this file size"), (dayGap, "this file size")], .....}
    #(key value) in tmpGapPeriod is like: {".mrc(090-120)" ==> (120, 55555)}, means that there 120 files that were accessed in 90~120 days, these files total size is 55555 bytes 
    return (ftGapNew, tmGapPeriod)
            
            
#Calculate each file's time gap between last access time and baseTime, here baseTime is specify by user.
#Also generate each file type's total number and corresponding type
#item format in inputFileName is like: [modifyTime]#[accessTime]#[changeTime]#[filepath]#owner#group#filesize       
def genTypeTypePeriod2NumSize(inputFileName, outputTypeTimePeriod2FileSizeFileName, outputType2FileSizeFileName, baseTime):
    #logger = MyLogger().getLogger()
    lineNum = 0    
    fileTotalSize = 0
    #(key, value) in ftGapDetail is like("suffix of file, like .mrc", ["time gap result like "xxx days, hh:mm:ss"])
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
    #for(k,v) in tmGapPeriod.iteritems():
    
    f = open(outputTypeTimePeriod2FileSizeFileName, "w")
    logger.info("Start to write typetimeperiod -> file size info in file: {0}".format(outputTypeTimePeriod2FileSizeFileName))
    for item in sortedGapPeriodList:
        #print("{0} ----> {1}---->{2}".format(item[0], item[1][0], sizeof_fmt(item[1][1])))
        f.write("{0}#{1}#{2}\n".format(item[0], item[1][0], sizeof_fmt(item[1][1])))
    
    f.write("Total File Size: {0}\n".format(sizeof_fmt(fileTotalSize)))

    f.close()

    logger.info("End to write typetimeperiod  -> file size info in file: {0}".format(outputTypeTimePeriod2FileSizeFileName))
    #print("-----------------------------------------------\n")
    
    f = open(outputType2FileSizeFileName, "w")
    #Sort by the length of second element in item, second element in each item is a list of day gap info 
    #sortedFTGapNew = sorted(ftGapNew.iteritems(), key=lambda x:len(x[1]), reverse = True)
    #print("ftGapNew = {0}".format(ftGapNew))
  
    unSortedFTGapNew = []
    for (k, v) in ftGapNew.iteritems():
        item = (k, len(v), sum(vv[1] for vv in v))
        unSortedFTGapNew.append(item)
    
    #print("unSortedFTGapNew = {0}".format(unSortedFTGapNew))    
    sortedFTGapNew = sorted(unSortedFTGapNew, key = lambda x: x[2], reverse = True)
  
     
    logger.info("Start to write type -> file size info in file: {0}".format(outputType2FileSizeFileName))
    for item in sortedFTGapNew:
       #print("{0} ----> {1}--->{2}".format(item[0], item[1], sizeof_fmt(item[2])))
        f.write("{0}#{1}#{2}\n".format(item[0], item[1], sizeof_fmt(item[2])))

    f.write("Total File Size: {0}\n".format(sizeof_fmt(fileTotalSize)))
    f.close()
    logger.info("End to write type -> file size info in file: {0}".format(outputType2FileSizeFileName))


#Analysis how many space each user and each group used
def gen_user_grp_usage(filename, outputUser2UsedSizeFile, outputGrp2UsedSizeFile, outputGrp2UsersFile):
    #logger = MyLogger().getLogger()
    lineNum = 0    
    fileTotalSize = 0
    
    #(key, value) format is like: ("grpname", "{("username1"), ("number of files", "bytes used")} ")
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
                logger.info("{0} lines have been finished processing".format(lineNum))
    
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
        
    logger.info("{0} lines have been finished processing".format(lineNum))
    fileHandle = open(outputUser2UsedSizeFile, "w")
    sortedUserUsage = sorted(userUsage.iteritems(), key = lambda x: x[1][1], reverse=True)   
    logger.info("Start to write user space usage info to file {0}".format(outputUser2UsedSizeFile))
    for item in sortedUserUsage:
        logger.info("UserName = {0}, FileCnt = {1}, TotalSize = {2}".format(item[0], item[1][0], sizeof_fmt(item[1][1])))
        fileHandle.write("{0}#{1}#{2}\n".format(item[0], item[1][0], sizeof_fmt(item[1][1])))
    fileHandle.close()
    
    logger.info("End to write user space usage info to file {0}".format(outputUser2UsedSizeFile))
    
    grpUserUsageList = []
    fileHandle = open(outputGrp2UsedSizeFile, "w")
    for (k, v) in grpUserUsage.iteritems():
        #logger.info("grpName = {0}, user num = {1}, user usage = {2}, total spaced used = {3}".format(k, len(v), v, sizeof_fmt(sum(vv[2] for vv in v))))
        item = (k, len(v), sum(vv[2] for vv in v))
        grpUserUsageList.append(item)
        #fileHandle.write("{0}#{1}#{2}\n".format(k, len(v), sizeof_fmt(sum(vv[2] for vv in v))))
    
    sortedGrpUserUsage = sorted(grpUserUsageList, key=lambda x: x[2], reverse = True)
    #print("sortedGrpUserUsage = {0}".format(sortedGrpUserUsage))    

    logger.info("Start to write group space usage info to file {0}".format(outputGrp2UsedSizeFile))
    for item in sortedGrpUserUsage:
        #logger.info("grpName = {0}, user num = {1}, total spaced used = {2}".format(item[0], item[1], sizeof_fmt(item[2])))
        fileHandle.write("{0}#{1}#{2}\n".format(item[0], item[1], sizeof_fmt(item[2])))
        
    fileHandle.close()
    logger.info("End to write group space usage info to file {0}".format(outputGrp2UsedSizeFile))
    
    fileHandle = open(outputGrp2UsersFile, "w")
    sortedGrpUser = sorted(grpUser.iteritems(), key=lambda x: x[0])
    logger.info("Start to write group to users info to file {0}".format(outputGrp2UsersFile))
    for item in sortedGrpUser:
        #logger.info("{0} ---> {1}".format(item[0], item[1]))
        fileHandle.write(str(item) + "\n")
    
    fileHandle.close()
    logger.info("End to write group to users info to file {0}".format(outputGrp2UsersFile))

#Find all regular file in specify dir "dirFullName", and save the result to file "pathListFullFileName"
#Output file format is like:
# /path/to/file1
# /path/to/file2
def genFilePathList(dirFullName, pathListFullFileName, excludeDirs, logger):

    if(dirFullName[-1] != '/'):
        dirFullName = dirFullName + "/"
    lineNum = 0
    fileHandle = open(pathListFullFileName, "w")
    for (root, dirs, files) in os.walk(dirFullName):
        for item in excludeDirs:
            (parent, dirname) = os.path.split(item)
            if(root[:-1] == parent and dirname in dirs):
                dirs.remove(dirname)
        logger.info("Find regular file in directory: {0}".format(root))
        for f in files:
            filepath = os.path.join(root, f)
            if(os.path.islink(filepath) == True):
                logger.warn("{0} is a symbol link, ignored".format(filepath))
                continue
            #logger.info("f = {0}".format(filepath))
            fileHandle.write("{0}\n".format(filepath))
            lineNum = lineNum + 1
            if(lineNum % 10000 == 0):
                logger.info("{0} regular files have been found".format(lineNum))

    logger.info("{0} regular files have been found".format(lineNum))
    fileHandle.close()


#input file format is like:
# /path/to/file1
# /path/to/file2
def getStatInfo(filepathListFile, statResultFile, invalidResultFile,  logger):
    logger.info("Start to get stat info")
    lineNum = 0
    timeFileInfo = {}
    fileNotExist = []
    with open(filepathListFile) as f:
        for line in f:
            fname = line 
            lineNum = lineNum + 1
            
            #Be carefull: this check is must, otherwise the file will not be found
            if(fname[-1] == '\n'):
                fname = fname[0:-1]
            if(lineNum % 10000 == 0):
                logger.info("{0} lines have been finished processing".format(lineNum))

            #We only handle the file that is really exist, some file may no longer exist when this script runs, e.g:file may be  deleted by user
            if(os.path.isfile(fname) == True):
                #modifyTime = os.path.getmtime(fname)
                #accessTime = os.path.getatime(fname)
                #changeTime = os.path.getctime(fname)
                #modifyTime = datetime.fromtimestamp(modifyTime).strftime('%Y-%m-%d-%H:%M:%S')
                #accessTime = datetime.fromtimestamp(accessTime).strftime('%Y-%m-%d-%H:%M:%S')
                #changeTime = datetime.fromtimestamp(changeTime).strftime('%Y-%m-%d-%H:%M:%S')
                #timeFileInfo[fname] = (modifyTime, accessTime, changeTime)
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
                    logger.warn("File with exception: {0}, set it owner to {1}, grp to {2}".format(fname, "unknow_user_" + str(statInfo.st_uid), "unknow_grp_" + str(statInfo.st_gid)))
                    logger.warn("{0}:Exception Detail: {1}".format(fname, str(exception)))
                    traceback.print_exc(file=sys.stdout)
                    timeFileInfo[fname] = (modifyTime, accessTime, changeTime, "unknow_user_" + str(statInfo.st_uid), "unknow_grp_" + str(statInfo.st_gid), fileSize)
            else:
                fileNotExist.append(fname)


    logger.info("{0} lines have been finished processing".format(lineNum))

    fileHandle = open(statResultFile, "w")
    cnt = 0
    logger.info("Start to write stat result info to file:{0}".format(statResultFile))
    for(fname, values) in timeFileInfo.iteritems():
        cnt = cnt + 1
        lineToWrite = values[0] + "#" + values[1] + "#" + values[2] + "#" + fname + "#" + values[3] + "#" + values[4] + "#" + values[5] +  "\n"
        fileHandle.write(lineToWrite)

    logger.info("End to write stat result info to file:{0}".format(statResultFile))
    fileHandle.close()

    fileHandle = open(invalidResultFile, "w")
    logger.info("start to write invalid result info to file:{0}".format(invalidResultFile))
    for f in fileNotExist:
        fileHandle.write("{0}\n".format(f))

    logger.info("End to write stat result info to file:{0}".format(invalidResultFile))

    return (timeFileInfo, fileNotExist)

def getStatInfo2(filepathListFile, statResultFile, invalidResultFile):

    #logger = MyLogger().getLogger()
    logger.info("Start to get stat info in function getStatInfo()")
    lineNum = 0
    timeFileInfo = {}
    fileNotExist = []
    with open(filepathListFile) as f:
        for line in f:
            fname = line 
            lineNum = lineNum + 1
            
            #Be carefull: this check is must, otherwise the file will not be found
            if(fname[-1] == '\n'):
                fname = fname[0:-1]
            if(lineNum % 10000 == 0):
                logger.info("{0} lines have been finished processing".format(lineNum))

            #We only handle the file that is really exist, some file may no longer exist when this script runs, e.g:file may be  deleted by user
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
                    logger.info("File with exception: {0}, set it owner to {1}, grp to {2}".format(fname, "unknow_user_" + str(statInfo.st_uid), "unknow_grp_" + str(statInfo.st_gid)))
                    logger.warn("{0}:Exception Detail: {1}".format(fname, str(exception)))
                    traceback.print_exc(file=sys.stdout)
                    timeFileInfo[fname] = (modifyTime, accessTime, changeTime, "unknow_user_" + str(statInfo.st_uid), "unknow_grp_" + str(statInfo.st_gid), fileSize)
            else:
                fileNotExist.append(fname)


    logger.info("{0} lines have been finished processing".format(lineNum))


    fileHandle = open(statResultFile, "w")
    
    cnt = 0
    logger.info("Start to write stat result info to file:{0}".format(statResultFile))
    for(fname, values) in timeFileInfo.iteritems():
        cnt = cnt + 1
        lineToWrite = values[0] + "#" + values[1] + "#" + values[2] + "#" + fname + "#" + values[3] + "#" + values[4] + "#" + values[5] +  "\n"
        fileHandle.write(lineToWrite)

    logger.info("End to write stat result info to file:{0}".format(statResultFile))
    fileHandle.close()

    fileHandle = open(invalidResultFile, "w")
    logger.info("start to write invalid result info to file:{0}".format(invalidResultFile))
    for f in fileNotExist:
        fileHandle.write("{0}\n".format(f))

    logger.info("End to write stat result info to file:{0}".format(invalidResultFile))

    return (timeFileInfo, fileNotExist)


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

def statAllRegularFile(pathFiles, outputDir, fileNamePrefix, processNum = 1):

    pool = multiprocessing.Pool(processes=processNum)
    result = []
    for i in xrange(processNum):
        processName = (fileNamePrefix + "_%d") %(i)
        outputStatFile = outputDir + fileNamePrefix + "_{0}".format(i) + "_stat.txt"
        invalidStatFileName = outputDir + fileNamePrefix + "_{0}".format(i) + "_stat_invalid.txt"
        result.append(pool.apply_async(getStatInfo2, (pathFiles[i], outputStatFile, invalidStatFileName)))
    pool.close()
    pool.join()



def getStatInfo3(pathGroup, statResultFile, invalidResultFile, processName):

    #logger = MyLogger().getLogger()
    logger.info("{0}: Start to get file stat info".format(processName))
    lineNum = 0
    timeFileInfo = {}
    fileNotExist = []
    #with open(filepathListFile) as f:
    #for line in f:
    for fname in pathGroup:
        lineNum = lineNum + 1
        
        #Be carefull: this check is must, otherwise the file will not be found
        if(fname[-1] == '\n'):
            fname = fname[0:-1]
        if(lineNum % 10000 == 0):
            logger.info("{0}: {1} lines have been finished processing".format(processName, lineNum))

        #We only handle the file that is really exist, some file may no longer exist when this script runs, e.g:file may be  deleted by user
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


    logger.info("{0}:{1} lines have been finished processing".format(processName, lineNum))


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

    logger.info("{0}: End to write stat result info to file:{1}".format(processName, invalidResultFile))

    return (timeFileInfo, fileNotExist)


def statAllRegularFile3(pathGroups, outputDir, fileNamePrefix, processNum = 1):

    pool = multiprocessing.Pool(processes=processNum)
    result = []
    for i in xrange(processNum):
        processName = (fileNamePrefix + "_%d") %(i)
        #outputStatFile = outputDir + fileNamePrefix + "_{0}".format(i) + "_stat.txt"
        outputStatFile = outputDir + "stat_{0}".format(i) + ".txt"
        invalidStatFileName = outputDir + fileNamePrefix + "_{0}".format(i) + "_stat_invalid.txt"
        result.append(pool.apply_async(getStatInfo3, (pathGroups[i], outputStatFile, invalidStatFileName, fileNamePrefix + "_{0}".format(i))))
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


def mergeStatFiles(outputDir, fileNamePrefix,  processNum, statFileName):


    allStatInfo = []
    for i in xrange(0, processNum):
        fileName = outputDir + fileNamePrefix + "_{0}".format(i) + ".txt"
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

    logger.info("Group size is: {0}".format(groupSize))

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
       low = "%5d"%(i * classifySize)
       high = "%5d"%(i * classifySize + classifySize)
       key = str(low) + "~~" + str(high)
       rangeDict[key] = [0,0,typeList,pathList]

   key = "%5d"%((classifyNum - 1) * classifySize)
   key = str(key) + "+"
   """
   The value of rangeDict is define as a 4-tuple: [fileTotalSize, fileTotalNum, [filePathList],[type#num#size] ]
   """
   lastTypeList = []
   lastPathList = []
   rangeDict[key] = [0,0,lastTypeList,lastPathList]
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


def getTypeCnt(pathList):
    """
    Get type 2 count info in path list
    """
    typeCntDict = {}
    for fname in pathList:
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
    

    """
    Organize info like type->cnt
    """
    typeCntList = []
    for (key, value) in typeCntDict.iteritems():
        item = key + "->" + str(value)
        typeCntList.append(item)

    return sorted(typeCntList)






def genFileSizeRangeDistribution(statInfoFile, classifySize=500, classifyNum=5, outputDir="./"):
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
            if(fileSize[-1] == '\n'):
                fileSize = fileSize[:-1]
            
            fileSize = int(fileSize)
            fileTotalSize = fileTotalSize + fileSize
            (name, ext) = os.path.splitext(fname)
            
            if(len(ext) > 0 and ext[-1] == '\n'):
                ext = ext[:-1]

            rangeKey = getRangeKey(rangeKeys, fileSize, classifySize)
            fileSizeRangeDict[rangeKey][0] = fileSizeRangeDict[rangeKey][0] + fileSize
            fileSizeRangeDict[rangeKey][1] = fileSizeRangeDict[rangeKey][1] + 1
            fileSizeRangeDict[rangeKey][3].append(fname)
            if(lineNum % 10000 == 0):
                logger.info("{0} lines finisned processing".format(lineNum))
            

    logger.info("{0} lines finisned processing".format(lineNum))
    for rangeKey in rangeKeys:
        fileSizeRangeDict[rangeKey][0] = sizeof_fmt(fileSizeRangeDict[rangeKey][0]) 
        fileSizeRangeDict[rangeKey][2] = getTypeCnt(fileSizeRangeDict[rangeKey][3])

    """
    Output rangeDict result to file
    """
    if(outputDir[-1] != '/'):
        outputDir = outputDir + "/"
    for (key, value) in fileSizeRangeDict.iteritems():
        newKey = key.replace(' ', '')
        outputFilename = outputDir + newKey + ".txt"
        fileHandle = open(outputFilename, "w")
        fileHandle.write("Total Size: " + str(value[0]) + "\n")
        fileHandle.write("Total File Num: " + str(value[1]) + "\n")
        fileHandle.write("---------------------Type 2 Num-----------------\n")
        for item in value[2]:
            fileHandle.write(item + "\n")
        fileHandle.write("---------------------File List------------------\n")
        for item in value[3]:
            fileHandle.write(item + "\n")
        fileHandle.close()

    return fileSizeRangeDict

if __name__ == "__main__":

    logger.info("Start to analysis directory, current time is: {0}".format(getCurrDateTime()))
    baseTime = getCurrDateTime()
    parser = argparse.ArgumentParser(description="This program is used to scan all regular file in a dirctory and make some statistic operations")
    parser.add_argument("-i", "--dir_to_search", action="store", help="The directory to search", required=False)
    parser.add_argument("-s", "--stat_file_name", action="store", help="Name of the linux stat command result file", default="all_stat_result.txt")
    parser.add_argument("-d", "--output_dir", action="store", help="The directory to save the output result file", default="/tmp/")
    parser.add_argument("-m", "--tmp_dir", action="store", help="The tmpory directory for saving intermediat result file", default="./tmp/")
    parser.add_argument("-p", "--filename_prefix", action="store", help="The prefix string for the file path file name", default="file_path")
    parser.add_argument("-t", "--base_time", action="store", help="The datetime to compare with the last access time, format is: yyyy-mm-dd-hh:mm:ss",default=baseTime) 
    parser.add_argument("-n", "--process_num", action="store", type=int, help="The process number to run", default=8)
    parser.add_argument("-r", "--is_shuffle", action="store", type=int, help="Whether to shuffle the dirs depth that >= 2 so as to blance the process load ", default=0)
    parser.add_argument("-e", "--exclude_dirs", action="append", help="The directories that do not need to be searched", default=[])
    parser.add_argument("-o", "--operation", action="store", help="operation to execute", choices=['all', 'gen_file_path_only','gen_stat_file','gen_type_typeperiod_2_num_size', 'gen_user_grp_2_num_size'], default='all')
    parser.add_argument("-c", "--classify_size", action="store", type=int, help="Size in MB used to classify the result stat info", default=500)
    parser.add_argument("-g", "--classify_num", action="store", type=int, help="Num of classify to generate", default=5)
    parser.add_argument("-x", "--is_used_mpi", action="store", type=int, help="Num of classify to generate", default=0)
    args = parser.parse_args()
    logger.info("directory to search:" + str(args.dir_to_search))
    logger.info("output dir is: " + args.output_dir)
    logger.info("directories that should not be searched: " + str(args.exclude_dirs))
    logger.info("date used as base time " + args.base_time)
    logger.info("operation to execute: " + args.operation)
    logger.info("Intermediate result file output dir is: " + args.tmp_dir)
    logger.info("process number to run {0}".format(args.process_num))
    logger.info("Is used MPI: ".format(args.is_used_mpi))

    op = args.operation
    intermediateResultDir = args.tmp_dir
    fileNamePrefix = args.filename_prefix
    outputDir = args.output_dir
    processNum = args.process_num
    dirToSearch = args.dir_to_search
    isShuffle = args.is_shuffle
    classifyNum = args.classify_num
    classifySize = args.classify_size
    isUsedMPI = args.is_used_mpi

    if(outputDir != "/tmp/"):
        mkdir(args.output_dir)
    if(outputDir[-1] != '/'):
        outputDir = outputDir + "/"
    if(intermediateResultDir[-1] != '/'):
        intermediateResultDir = intermediateResultDir + "/"
    mkdir(intermediateResultDir)
    (level1Dirs, level2Dirs) = getLevel12Dirs(dirToSearch)

    """
    Here we write the level2Dirs to file so that we can use mpi process the search the regular file
    """
    logger.info("Start to write level 2 dirs to file level2Dirs.txt")
    fileHandle = open("level2Dirs.txt", "w");
    for item in level2Dirs:
        fileHandle.write(item + "\n");
    fileHandle.close()
    logger.info("End to write level 2 dirs to file level2Dirs.txt")

    level12RegularFiles = getLevel12RegularFiles(dirToSearch)
    logger.info("Regular files in level 1&2 directory: " + str(level12RegularFiles))

    if(isShuffle == 1):
        logger.info("Dir depth that >= 2 before shuffle: " + str(level2Dirs))
        shuffle(level2Dirs)
        logger.info("Dir depth that >= 2 after shuffle: " + str(level2Dirs))

    dirGroups = divDirsToGroups(level2Dirs, processNum)  
    logger.info("Number of leve2Dirs: " + str(len(level2Dirs)))
    logger.info("Dir groups result:")
    for item in dirGroups:
        print(item)



    if(isUsedMPI == 1):
        """
        Write dir group to each file so that each mpi process can read its dir file
        We save all dirs file and file path files to the intermediate result dir
        """
        for i in xrange(processNum):
            group = dirGroups[i]
            fname = intermediateResultDir + "dirs_" + str(i) + ".txt"
            fileHandle = open(fname, "w")
            for item in group:
                fileHandle.write(item + "\n")
            fileHandle.close()

        """
        mpiexec -n 180 -f machinefile ./genFilePath ./tmp ./result
        """
        mpiCommandGenFilePath = "mpiexec -f ./machinefile -n " + str(processNum) + " ./genFilePath" + " " + intermediateResultDir + " " + intermediateResultDir + " " + fileNamePrefix 

        logger.info("mpiCommandGenFilePath = " + mpiCommandGenFilePath)

        """
        Here we use mpi program to find all regular files by a number of process.
        """
        os.system(mpiCommandGenFilePath)

    else:
        logger.info("Start to find all regular files in directory that depth is > 2")
        findAllRegularFile(dirGroups, intermediateResultDir,  fileNamePrefix, processNum) 
        logger.info("End to find all regular files in directory that depth is > 2")

    logger.info("Start to find all regular files in directory that depth is 1 & 2")
    level12RegularFiles = getLevel12RegularFiles(dirToSearch)
    logger.info("End to find all regular files in directory that depth is 1 & 2")
    logger.info("Regular files in level 1&2 directory: {0}".format(level12RegularFiles))
    logger.info("Num of regular files in level 1&2: {0}".format(len(level12RegularFiles)))
    logger.info("Sleep for 5 seconds for merging file path files")
    time.sleep(5)
    logger.info("Start to merge file path files")
    outputPathFileName = outputDir + "all_file_path_result.txt"
    allPaths = mergePathFiles(intermediateResultDir, fileNamePrefix, processNum, level12RegularFiles, outputPathFileName)
    logger.info("End to merge file path files")

    """
    we append the regular files in level 1 and 2 dir to the first process file path file
    """
    fileHandle = open(intermediateResultDir + fileNamePrefix + "_0.txt", "a+")
    for item in level12RegularFiles:
        fileHandle.write(item + "\n")
    fileHandle.close()


    logger.info("Sleep 3 seconds and then to stat file ")
    time.sleep(3)
    #pathFiles = []
    #for i in xrange(0, processNum):
    #    pathFile = intermediateResultDir + fileNamePrefix + "_{0}".format(i) + ".txt"
    #    pathFiles.append(pathFile)

    #statAllRegularFile(pathFiles, intermediateResultDir, fileNamePrefix, processNum)
    pathGroups = divAllPathsToGroups(allPaths, processNum)

    if(isUsedMPI == 1):
        """
        We write item in pathGroups to each file ,so that for every mpi process to do stat operation
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
        mpiexec -f machinefile -n 24 ./statFilePath tmp tmp huabin
        """
        mpiCommandStatFilePath = "mpiexec -f ./machinefile -n " + str(processNum) + " ./statFilePath" + " " + intermediateResultDir + " " + intermediateResultDir + " " + fileNamePrefix 

        logger.info("mpiCommandStatFilePath = " + mpiCommandStatFilePath)

        """
        Here we use mpi program to stat all regular files by a number of process.
        """
        os.system(mpiCommandStatFilePath)
    else:
        statAllRegularFile3(pathGroups, intermediateResultDir, fileNamePrefix, processNum)

    outputStatFileName = outputDir + args.stat_file_name 
    mergeStatFiles(intermediateResultDir, "stat", processNum, outputStatFileName)
    time.sleep(3)
    logger.info("start to gen type -> num and size result")
    baseTime = args.base_time
    typeTypePeriod2NumSizeFile = outputDir + "type_type_period_2_num_size.txt"
    type2NumSizeFile = outputDir + "type_2_num_size.txt"
    genTypeTypePeriod2NumSize(outputStatFileName, typeTypePeriod2NumSizeFile, type2NumSizeFile, baseTime) 
    logger.info("End to gen type -> num and size result")
    logger.info("Start to gen user group usage result")
    user2UsageFile = outputDir + "user_usage.txt"
    grp2UsageFile = outputDir + "group_usage.txt"
    grp2UserFile = outputDir + "group_2_user.txt"
    gen_user_grp_usage(outputStatFileName, user2UsageFile, grp2UsageFile, grp2UserFile)
    logger.info("End to gen user group usage result")
    
    logger.info("Start to gen file size distribution info")
    genFileSizeRangeDistribution(outputStatFileName, classifySize, classifyNum, outputDir)
    logger.info("End to gen file size distribution info")

