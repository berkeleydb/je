/*-
 * Copyright (C) 2002, 2017, Oracle and/or its affiliates. All rights reserved.
 *
 * This file was distributed by Oracle as part of a version of Oracle Berkeley
 * DB Java Edition made available at:
 *
 * http://www.oracle.com/technetwork/database/database-technologies/berkeleydb/downloads/index.html
 *
 * Please see the LICENSE file included in the top-level directory of the
 * appropriate version of Oracle Berkeley DB Java Edition for a copy of the
 * license and additional information.
 */

package com.sleepycat.je.log;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.utilint.VLSN;

/**
 * This is a debugging utility which implements the unadvertised DbPrintLog -vd
 * option, which displays VLSN distribution in a log. Here's a sample of the
 * output. This is used to analyze log cleaner barrier behavior.
 *
 * ... 3 files
 * file 0xb6 numRepRecords = 9 firstVLSN = 1,093,392 lastVLSN = 1,093,400
 * file 0xb7 numRepRecords = 4 firstVLSN = 1,093,401 lastVLSN = 1,093,404
 * ... 3 files
 * file 0xbb numRepRecords = 1 firstVLSN = 1,093,405 lastVLSN = 1,093,405
 * file 0xbc numRepRecords = 1 firstVLSN = 1,093,406 lastVLSN = 1,093,406
 * ... 1 files
 * file 0xbe numRepRecords = 1 firstVLSN = 1,093,407 lastVLSN = 1,093,407
 * file 0xbf numRepRecords = 2 firstVLSN = 1,093,408 lastVLSN = 1,093,409
 * file 0xc0 numRepRecords = 7 firstVLSN = 1,093,410 lastVLSN = 1,093,416
 * ... 0 files at end
 * First file: 0x0
 * Last file: 0xc0
 */
public class VLSNDistributionReader extends DumpFileReader {

    private final Map<Long,PerFileInfo> countByFile;
    private PerFileInfo info;
    private final Long[] allFileNums;
    private int fileNumIndex;

    /**
     * Create this reader to start at a given LSN.
     */
    public VLSNDistributionReader(EnvironmentImpl envImpl,
                                  int readBufferSize,
                                  long startLsn,
                                  long finishLsn,
                                  long endOfFileLsn,
                                  boolean verbose,
                                  boolean forwards)
        throws DatabaseException {

        super(envImpl, readBufferSize, startLsn, finishLsn, endOfFileLsn,
              null /* all entryTypes */, 
              null /* all dbIds */,
              null /* all txnIds */,
              verbose,
              true, /*repEntriesOnly*/
              forwards);
        countByFile = new HashMap<Long,PerFileInfo>();
        allFileNums = fileManager.getAllFileNumbers();
        fileNumIndex = 0;
    }

    /**
     * Count the number of vlsns in the file, along with the first and last
     * vlsn. Display this when the log reader moves to a new file. .
     */
    @Override
    protected boolean processEntry(ByteBuffer entryBuffer) {
        VLSN currentVLSN = currentEntryHeader.getVLSN();
        long currentFile = window.currentFileNum();

        if (info == null) {
            info = new PerFileInfo(currentFile);
            countByFile.put(currentFile, info);
        } else if (!info.isFileSame(currentFile)) {
            /* 
             * We've flipped to a new file. We'd like to print the number
             * of files between the one targeted by this info to give a sense
             * for how many are inbetween. For example, if the log has file
             * 4, 5, 6, and only 6 has a vlsn, we should print 
             *  ... 2 files
             *  file 0x6: ...
             */
            info.display();

            /* Set up a new file. */
            info = new PerFileInfo(currentFile);
            countByFile.put(currentFile, info);
        }

        info.increment(currentVLSN);

        int nextEntryPosition = 
            entryBuffer.position() + currentEntryHeader.getItemSize();
        entryBuffer.position(nextEntryPosition);

        return true;
    }

    @Override
    public void summarize(boolean csvFormat) {
        if (info != null) {
            info.display();
        }

        System.err.println( "... " +
                            (allFileNums.length - fileNumIndex) +
                            " files at end");
        
        System.err.println("First file: 0x" + 
                           Long.toHexString(fileManager.getFirstFileNum()));
        System.err.println("Last file: 0x" + 
                           Long.toHexString(fileManager.getLastFileNum()));
    }

    /**
     * Tracks per-file statistics.
     */
    private class PerFileInfo {
        private final long fileNum;
        private VLSN firstVLSNInFile;
        private VLSN lastVLSNInFile;
        private int count;

        PerFileInfo(long fileNum) {
            this.fileNum = fileNum;
        }

        public boolean isFileSame(long currentFile) {
           return fileNum == currentFile;
        }

        void increment(VLSN currentVLSN) {
            count++;
            if (firstVLSNInFile == null) {
                firstVLSNInFile = currentVLSN;
            }
            lastVLSNInFile = currentVLSN;
        }

        @Override
        public String toString() {
            return "file 0x" + Long.toHexString(fileNum) +
                " numRepRecords = " +  count +
                " firstVLSN = " + firstVLSNInFile +
                " lastVLSN = " + lastVLSNInFile;
        }

        void display() {
            int inbetweenCount = 0;
            while (fileNumIndex < allFileNums.length) {
                long whichFile = allFileNums[fileNumIndex];

                if (whichFile > fileNum) {
                    break;
                }
                fileNumIndex++;
                inbetweenCount++;
            }
            
            if (inbetweenCount > 1) {
                System.err.println("... " + (inbetweenCount -1) + " files");
            }
            System.err.println(this);
        }
    }
}
