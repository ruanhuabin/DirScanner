#!/bin/bash - 
#===============================================================================
#
#          FILE:  runGenDir.sh
# 
#         USAGE:  ./runGenDir.sh 
# 
#   DESCRIPTION:  
# 
#       OPTIONS:  ---
#  REQUIREMENTS:  ---
#          BUGS:  ---
#         NOTES:  ---
#        AUTHOR: Ruan Huabin <ruanhuabin@gmail.com>
#       COMPANY: Tsinghua University
#       CREATED: 11/14/2017 02:10:29 PM CST
#      REVISION:  ---
#===============================================================================

set -o nounset                              # Treat unset variables as an error
set -e
set -x
python genDirAndFile.py dirs.txt ./EM
