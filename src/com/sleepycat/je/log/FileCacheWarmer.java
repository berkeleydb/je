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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;

import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.LoggerUtils;

/**
 * Warm-up the file system cache during startup, for some portion of the log
 * that is not being read by recovery.
 *
 * See EnvironmentConfig.LOG_FILE_WARM_UP_SIZE (until we publish it, this is
 * in EnvironmentParams).
 */
public class FileCacheWarmer extends Thread {

    private final EnvironmentImpl envImpl;
    private final long recoveryStartLsn;
    private final long endOfLogLsn;
    private final int warmUpSize;
    private final int bufSize;
    private volatile boolean stop;

    FileCacheWarmer(final EnvironmentImpl envImpl,
                    final long recoveryStartLsn,
                    final long endOfLogLsn,
                    final int warmUpSize,
                    final int bufSize) {
        this.envImpl = envImpl;
        this.recoveryStartLsn = recoveryStartLsn;
        this.endOfLogLsn = endOfLogLsn;
        this.warmUpSize = warmUpSize;
        this.bufSize = bufSize;
        stop = false;
    }

    /**
     * Stops this thread. At most one read will occur after calling this
     * method, and then the thread will exit.
     */
    void shutdown() {
        stop = true;
    }

    @Override
    public void run() {
        try {
            doRun();
        } catch (Throwable e) {

            /*
             * Log error as SEVERE but do not invalidate environment since it
             * perfectly usable.
             */
            LoggerUtils.traceAndLogException(
                envImpl, FileCacheWarmer.class.getName(), "run",
                "Unable to warm file system cache due to exception", e);

        } finally {
            /* Ensure that this thread can be GC'd after it stops. */
            envImpl.getFileManager().clearFileCacheWarmer();
        }
    }

    private void doRun()
        throws Throwable {

        final FileManager fm = envImpl.getFileManager();

        final long ONE_MB = 1L << 20;

        long remaining = (warmUpSize * ONE_MB) -
            DbLsn.getTrueDistance(recoveryStartLsn, endOfLogLsn, fm);

        if (remaining <= 0) {
            return;
        }

        // System.out.println("FileCacheWarmer start " + remaining);

        final byte[] buf = new byte[bufSize];

        long fileNum = DbLsn.getFileNumber(recoveryStartLsn);
        long fileOff = DbLsn.getFileOffset(recoveryStartLsn);

        String filePath = fm.getFullFileName(fileNum);
        File file = new File(filePath);
        RandomAccessFile raf = null;

        try {
            raf = new RandomAccessFile(file, "r");

            while (!stop && remaining > 0) {

                if (fileOff <= 0) {
                    raf.close();
                    raf = null;

                    while (!stop) {
                        final Long nextFileNum = fm.getFollowingFileNum(
                            fileNum, false /*forward*/);

                        if (nextFileNum == null) {
                            return;
                        }

                        fileNum = nextFileNum;
                        filePath = fm.getFullFileName(fileNum);
                        file = new File(filePath);
                        try {
                            raf = new RandomAccessFile(file, "r");
                        } catch (FileNotFoundException e) {
                            continue;
                        }
                        fileOff = raf.length();
                        break;
                    }
                }

                final long pos = Math.max(0L, fileOff - bufSize);
                raf.seek(pos);

                final int bytes = (int) (fileOff - pos);
                final int read = raf.read(buf, 0, bytes);

                if (read != bytes) {
                    throw new IllegalStateException(
                        "Requested " + bytes + " bytes but read " + read);
                }

                remaining -= bytes;
                fileOff = pos;
            }

            raf.close();
            raf = null;

        } finally {

            // System.out.println(
            // "FileCacheWarmer finish " + remaining + " " + stop);

            if (raf != null) {
                try {
                    raf.close();
                } catch (Exception e) {
                    /* Ignore this. Another exception is in flight. */
                }
            }
        }
    }
}
