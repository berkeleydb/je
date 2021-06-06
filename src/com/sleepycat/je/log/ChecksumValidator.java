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
import java.util.zip.Checksum;

import com.sleepycat.je.utilint.Adler32;
import com.sleepycat.je.utilint.DbLsn;

/**
 * Checksum validator is used to check checksums on log entries.
 */
public class ChecksumValidator {
    private static final boolean DEBUG = false;

    private Checksum cksum;

    public ChecksumValidator() {
        cksum = Adler32.makeChecksum();
    }

    public void reset() {
        cksum.reset();
    }

    /**
     * Add this byte buffer to the checksum. Assume the byte buffer is already
     * positioned at the data.
     * @param buf target buffer
     * @param length of data
     */
    public void update(ByteBuffer buf, int length)
        throws ChecksumException {

        if (buf == null) {
            throw new ChecksumException(
                "null buffer given to checksum validation, probably " +
                 " result of 0's in log file, len=" + length);
        }

        int bufStart = buf.position();

        if (DEBUG) {
            System.out.println("bufStart = " + bufStart +
                               " length = " + length);
        }

        update(buf.array(), bufStart + buf.arrayOffset(), length);
    }

    public void update(byte[] buf, int offset, int length) {
        cksum.update(buf, offset, length);
    }

    void validate(long expectedChecksum, long lsn)
        throws ChecksumException {

        if (expectedChecksum != cksum.getValue()) {
            throw new ChecksumException
                ("Location " + DbLsn.getNoFormatString(lsn) +
                 " expected " + expectedChecksum + " got " + cksum.getValue());
        }
    }

    public void validate(long expectedChecksum, long fileNum, long fileOffset)
        throws ChecksumException {

        if (expectedChecksum != cksum.getValue()) {
            long problemLsn = DbLsn.makeLsn(fileNum, fileOffset);

            throw new ChecksumException
                ("Location " + DbLsn.getNoFormatString(problemLsn) +
                 " expected " + expectedChecksum + " got " +
                 cksum.getValue());
        }
    }
}
