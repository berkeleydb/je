#!/bin/sh

# Runs MeasureDiskOrderedScan -- see its source code for details.
#
# Meant to be run from the JEHOME directory.  Creates a JEHOME/tmp directory
# to contain the data.  Must be run as root in order to execute the command to
# clear the file system cache.
#
# This script populates the data and runs a single performance comparison.  It
# is meant to be modified as needed.

JEHOME="."
CP="${JEHOME}/build/lib/je.jar:${JEHOME}/build/test/standalone/classes"
ARGS="MeasureDiskOrderedScan -nRecords 5000000 -jeCacheSize 500000000"
CMD="java -cp $CP -Xms1g -Xmx1g $ARGS"

# Clear directory and Populate
#rm -rf tmp && mkdir tmp
#sync; echo 3 > /proc/sys/vm/drop_caches
#$CMD -action Populate

# DirtyReadScan
#sync; echo 3 > /proc/sys/vm/drop_caches
#$CMD -action DirtyReadScan

# DiskOrderedScan
sync; echo 3 > /proc/sys/vm/drop_caches
$CMD -action DiskOrderedScan
