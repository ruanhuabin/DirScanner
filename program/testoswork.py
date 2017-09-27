import os
import sys
from os.path import join, getsize

import traceback
def myerrorfun(OSError):

    print "hello:", os.error.strerror, str(os.error.filename), OSError.message
    fileHandle = open("exception.log", "a")
    traceback.print_exc(file=fileHandle)
    
    fileHandle.close()

for root, dirs, files in os.walk(sys.argv[1], onerror=myerrorfun):
    #print root, "consumes",
    #print sum([getsize(join(root, name)) for name in files]),
    #print "bytes in", len(files), "non-directory files"
    for name in files:
        print join(root, name)

