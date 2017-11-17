#!/bin/bash - 
#===============================================================================
#
#          FILE:  run.sh
# 
#         USAGE:  ./run.sh 
# 
#   DESCRIPTION:  
# 
#       OPTIONS:  ---
#  REQUIREMENTS:  ---
#          BUGS:  ---
#         NOTES:  ---
#        AUTHOR: Ruan Huabin <ruanhuabin@gmail.com>
#       COMPANY: Tsinghua University
#       CREATED: 10/07/2017 03:14:11 PM CST
#      REVISION:  ---
#===============================================================================

set -o nounset                              # Treat unset variables as an error
rm -rf scaner*.log

if [ -d result3 ]
then
    echo "Directory [result2] is exist, start to delete"
    rm -rf result3/*.txt
fi

if [ -d tmp3 ]
then
    echo "Directory [tmp2] is exist, start to delete"
    rm -rf tmp3/*.txt
fi

export PATH=/Share/home/hbruan/DirScanner/mpich/bin:$PATH
export LD_LIBRARY_PATH=/Share/home/hbruan/DirScanner/mpich/lib
python smartDirScanner.py -i $1 -n 24 -x 1 -d ./20171117Result -m 20171117Tmp
