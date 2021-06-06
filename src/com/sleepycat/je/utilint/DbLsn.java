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

package com.sleepycat.je.utilint;

import java.io.File;
import java.util.Arrays;

import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.log.FileManager;
import com.sleepycat.je.tree.TreeUtils;

/**
 * DbLsn is a class that operates on Log Sequence Numbers (LSNs). An LSN is a
 * long comprised of a file number (32b) and offset within that file (32b)
 * which references a unique record in the database environment log.  While
 * LSNs are represented as long's, we operate on them using an abstraction and
 * return longs from these methods so that we don't have to worry about the
 * lack of unsigned quantities.
 */
public class DbLsn {
    static final long INT_MASK = 0xFFFFFFFFL;

    public static final long MAX_FILE_OFFSET = 0xFFFFFFFFL;

    /* Signifies a transient LSN. */
    private static final long MAX_FILE_NUM = 0xFFFFFFFFL;

    public static final long NULL_LSN = -1;

    private DbLsn() {
    }

    public static long makeLsn(long fileNumber, long fileOffset) {
        return fileOffset & INT_MASK |
            ((fileNumber & INT_MASK) << 32);
    }

    /**
     * This flavor of makeLsn is used when the file offset has been stored
     * in 32 bits, as is done in the VLSNBucket.
     */
    public static long makeLsn(long fileNumber, int fileOffset) {
        return fileOffset & INT_MASK |
            ((fileNumber & INT_MASK) << 32);
    }

    /**
     * For transient LSNs we use the MAX_FILE_NUM and the ascending sequence of
     * offsets.
     */
    public static long makeTransientLsn(long fileOffset) {
        return makeLsn(DbLsn.MAX_FILE_NUM, fileOffset);
    }

    /**
     * A transient LSN is defined as one with a file number of MAX_FILE_NUM.
     */
    public static boolean isTransient(long lsn) {
        return getFileNumber(lsn) == MAX_FILE_NUM;
    }

    public static boolean isTransientOrNull(long lsn) {
        return lsn == NULL_LSN || isTransient(lsn);
    }

    public static long longToLsn(Long lsn) {
        if (lsn == null) {
            return NULL_LSN;
        }

        return lsn.longValue();
    }

    /**
     * Return the file number for this DbLsn.
     * @return the number for this DbLsn.
     */
    public static long getFileNumber(long lsn) {
        return (lsn >> 32) & INT_MASK;
    }

    /**
     * Return the file offset for this DbLsn.
     * @return the offset for this DbLsn.
     */
    public static long getFileOffset(long lsn) {
        return (lsn & INT_MASK);
    }

    /*
     * The file offset is really an unsigned int. If we are using the
     * file offset as a value, we must be careful to manipulate it as a long
     * in order not to lose the last bit of data. If we are only storing
     * the file offset, we can treat it as an Integer in order to save
     * 32 bits of space.
     */
    public static int getFileOffsetAsInt(long lsn) {
        return (int) getFileOffset(lsn);
    }

    public static long convertIntFileOffsetToLong(int storedLsn) {
        return storedLsn & 0xffffffffL;
    }

    private static int compareLong(long l1, long l2) {
        if (l1 < l2) {
            return -1;
        } else if (l1 > l2) {
            return 1;
        } else {
            return 0;
        }
    }

    public static int compareTo(long lsn1, long lsn2) {
        if (lsn1 == NULL_LSN ||
            lsn2 == NULL_LSN) {
            throw EnvironmentFailureException.unexpectedState
                ("NULL_LSN lsn1=" + getNoFormatString(lsn1) + 
                 " lsn2=" + getNoFormatString(lsn2));
        }

        long fileNumber1 = getFileNumber(lsn1);
        long fileNumber2 = getFileNumber(lsn2);
        if (fileNumber1 == fileNumber2) {
            return compareLong(getFileOffset(lsn1), getFileOffset(lsn2));
        }
        return compareLong(fileNumber1, fileNumber2);
    }

    public static String toString(long lsn) {
        return "<DbLsn val=\"0x" +
            Long.toHexString(getFileNumber(lsn)) +
            "/0x" +
            Long.toHexString(getFileOffset(lsn)) +
            "\"/>";
    }

    public static String getNoFormatString(long lsn) {
        return "0x" + Long.toHexString(getFileNumber(lsn)) + "/0x" +
            Long.toHexString(getFileOffset(lsn));
    }

    public static String dumpString(long lsn, int nSpaces) {
        StringBuilder sb = new StringBuilder();
        sb.append(TreeUtils.indent(nSpaces));
        sb.append(toString(lsn));
        return sb.toString();
    }

    /**
     * Return the logsize in bytes between these two LSNs. This is an
     * approximation; the logs might actually be a little more or less in
     * size. This assumes that no log files have been cleaned.
     */
    public static long getNoCleaningDistance(long thisLsn,
                                             long otherLsn,
                                             long logFileSize) {
        long diff = 0;

        assert thisLsn != NULL_LSN;
        /* First figure out how many files lay between the two. */
        long myFile = getFileNumber(thisLsn);
        if (otherLsn == NULL_LSN) {
            otherLsn = 0;
        }
        long otherFile = getFileNumber(otherLsn);
        if (myFile == otherFile) {
            diff = Math.abs(getFileOffset(thisLsn) - getFileOffset(otherLsn));
        } else if (myFile > otherFile) {
            diff = calcDiff(myFile - otherFile,
                            logFileSize, thisLsn, otherLsn);
        } else {
            diff = calcDiff(otherFile - myFile,
                            logFileSize, otherLsn, thisLsn);
        }
        return diff;
    }

    /**
     * Return the logsize in bytes between these two LSNs. This is an
     * approximation; the logs might actually be a little more or less in
     * size. This assumes that log files might have been cleaned.
     */
    public static long getWithCleaningDistance(long thisLsn,
                                               long otherLsn,
                                               long logFileSize,
                                               FileManager fileManager) {
        long diff = 0;

        assert thisLsn != NULL_LSN;
        /* First figure out how many files lay between the two. */
        long myFile = getFileNumber(thisLsn);
        if (otherLsn == NULL_LSN) {
            otherLsn = 0;
        }
        long otherFile = getFileNumber(otherLsn);
        if (myFile == otherFile) {
            diff = Math.abs(getFileOffset(thisLsn) - getFileOffset(otherLsn));
        } else {
            /* Figure out how many files lie between. */
            Long[] fileNums = fileManager.getAllFileNumbers();
            int myFileIdx = Arrays.binarySearch(fileNums,
                                                Long.valueOf(myFile));
            int otherFileIdx =
                Arrays.binarySearch(fileNums, Long.valueOf(otherFile));
            if (myFileIdx > otherFileIdx) {
                diff = calcDiff(myFileIdx - otherFileIdx,
                                logFileSize, thisLsn, otherLsn);
            } else {
                diff = calcDiff(otherFileIdx - myFileIdx,
                                logFileSize, otherLsn, thisLsn);
            }
        }
        return diff;
    }

    private static long calcDiff(long fileDistance,
                                 long logFileSize,
                                 long laterLsn,
                                 long earlierLsn) {
        long diff = fileDistance * logFileSize;
        diff += getFileOffset(laterLsn);
        diff -= getFileOffset(earlierLsn);
        return diff;
    }

    /**
     * Returns the number of bytes between two LSNs, counting the true size of
     * each intermediate file. Assumes that all files in the LSN range are
     * currently protected from cleaner deletion, e.g., during recovery. Uses
     * File.length and does not perturb the FileManager's file handle cache.
     */
    public static long getTrueDistance(final long thisLsn,
                                       final long otherLsn,
                                       final FileManager fileManager) {

        final long lsn1;
        final long lsn2;

        if (compareTo(thisLsn, otherLsn) < 0) {
            lsn1 = thisLsn;
            lsn2 = otherLsn;
        } else {
            lsn1 = otherLsn;
            lsn2 = thisLsn;
        }

        final long file1 = getFileNumber(lsn1);
        final long file2 = getFileNumber(lsn2);

        long dist = getFileOffset(lsn2) - getFileOffset(lsn1);

        if (file1 == file2) {
            return dist;
        }

        final Long[] fileNums = fileManager.getAllFileNumbers();

        final int idx1 = Arrays.binarySearch(fileNums, file1);
        final int idx2 = Arrays.binarySearch(fileNums, file2);

        /*
         * File2 has already been counted, and we've already subtracted the
         * offset of file1. Add lengths of file1 to file2-1.
         */
        for (int i = idx1; i < idx2; i += 1) {
            final String path = fileManager.getFullFileName(fileNums[i]);
            dist += new File(path).length();
        }

        return dist;
    }
}
