def sizeof_fmt(num, suffix='B'):
    for unit in ['','K','M','G','T','P','E','Z']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)


def calSize(fileName):
    totalSize = 0
    with open(fileName) as f:
        for line in f:
            size = int(line[0:-1])
            totalSize = totalSize + size

    return (sizeof_fmt(totalSize), totalSize)
import sys
(totalSize, numSize) = calSize(sys.argv[1])
print "total size: ", totalSize, numSize
