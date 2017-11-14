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

if [ -d result2 ]
then
    echo "Directory [result2] is exist, start to delete"
    rm -rf result2/*.txt
fi

if [ -d tmp2 ]
then
    echo "Directory [tmp2] is exist, start to delete"
    rm -rf tmp2/*.txt
fi
python smartDirScanner.py -i ../ShareEM/ -n 20 -x 1 -d result2 -m tmp2
