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
package com.sleepycat.je.rep.stream;

import java.io.IOException;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.cleaner.FileProtector;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.rep.impl.node.NameIdPair;
import com.sleepycat.je.rep.vlsn.VLSNIndex;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.VLSN;

/**
 * Implementation of a master node acting as a FeederSource. The
 * MasterFeederSource is stateful, because it keeps its own FeederReader which
 * acts as a cursor or scanner across the log files, so it can only be used by
 * a single Feeder.
 */
public class MasterFeederSource implements FeederSource {

    private final FeederReader feederReader;

    /* Protects files being read from being deleted. See FileProtector. */
    private final FileProtector.ProtectedFileRange protectedFileRange;

    public MasterFeederSource(EnvironmentImpl envImpl,
                              VLSNIndex vlsnIndex,
                              NameIdPair replicaNameIdPair,
                              VLSN startVLSN)
        throws DatabaseException, IOException {

        int readBufferSize =
            envImpl.getConfigManager().getInt
            (EnvironmentParams.LOG_ITERATOR_READ_SIZE);

        feederReader = new FeederReader(envImpl,
                                        vlsnIndex,
                                        DbLsn.NULL_LSN, // startLsn
                                        readBufferSize);

        long startLsn = feederReader.initScan(startVLSN);

        /*
         * Syncup currently protects all files in the entire VLSNIndex range.
         * This allows us to safely protect files from the matchpoint onward.
         */
        protectedFileRange = envImpl.getFileProtector().protectFileRange(
            FileProtector.FEEDER_NAME + "-" + replicaNameIdPair,
            DbLsn.getFileNumber(startLsn),
            true /*protectVlsnIndex*/);
    }

    /**
     * Must be called to allow deletion of files protected by this feeder.
     */
    @Override
    public void shutdown(EnvironmentImpl envImpl) {
        envImpl.getFileProtector().removeFileProtection(protectedFileRange);
    }

    /*
     * @see com.sleepycat.je.rep.stream.FeederSource#getLogRecord
     * (com.sleepycat.je.utilint.VLSN, int)
     */
    @Override
    public OutputWireRecord getWireRecord(VLSN vlsn, int waitTime)
        throws DatabaseException, InterruptedException, IOException {

        try {
            OutputWireRecord record =
                feederReader.scanForwards(vlsn, waitTime);

            if (record == null) {
                return null;
            }

            /*
             * Advance the protected file range when we advance to a new file,
             * to allow deletion of older files. Use getRangeStart (which is
             * not synchronized) to check whether the file has advanced, before
             * calling advanceRange (which is synchronized). This check must be
             * inexpensive and non-blocking.
             */
            long lastFile = feederReader.getLastFile(record);
            if (lastFile > protectedFileRange.getRangeStart()) {
                protectedFileRange.advanceRange(lastFile);
            }

            return record;
        } catch (DatabaseException e) {
            /* Add more information */
            e.addErrorMessage
                ("MasterFeederSource fetching vlsn=" + vlsn +
                 " waitTime=" + waitTime);
            throw e;
        }
    }

    @Override
    public String dumpState() {
        return feederReader.dumpState();
    }
}
