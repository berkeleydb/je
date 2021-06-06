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

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Durability;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.Durability.ReplicaAckPolicy;
import com.sleepycat.je.Durability.SyncPolicy;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.FileReader;
import com.sleepycat.je.log.LogEntryHeader;
import com.sleepycat.je.log.entry.LogEntry;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicationConfig;
import com.sleepycat.je.rep.UnknownMasterException;
import com.sleepycat.je.rep.impl.RepParams;
import com.sleepycat.je.rep.impl.node.cbvlsn.LocalCBVLSNUpdater;
import com.sleepycat.je.rep.utilint.RepTestUtils;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.VLSN;

public class VLSNTestUtils {
    private static final String GROUP_NAME = "repGroup";
    private static final String NODE_NAME = "n8";
    private static final String TEST_HOST = "localhost";
    private static final Integer MAX_DISTANCE = 1000;

    /*
     * If -DlongTimeout is true, then this test will run with very long
     * timeouts, to make interactive debugging easier.
     */
    private static final boolean longTimeout =
        Boolean.getBoolean("longTimeout");

    /**
     * Create a replicated environment and populate the log.
     */
    public static ReplicatedEnvironment setupLog(File envHome,
                                                 int bucketStride,
                                                 int bucketMaxMappings,
                                                 LogPopulator populator)
        throws UnknownMasterException, DatabaseException {

        /* 
         * Create a single replicator. We're only interested in the log
         * on this node, so the durability for the replica is a no-op.
         */
        Durability syncDurability = new Durability(SyncPolicy.SYNC,
                                                   SyncPolicy.NO_SYNC,
                                                   ReplicaAckPolicy.NONE);

        ReplicatedEnvironment rep = 
            makeReplicator(envHome,
                           syncDurability, 1000 /* logfilelen */,
                           bucketStride,
                           bucketMaxMappings);

        ReplicatedEnvironment.State joinState = rep.getState();
        if (!joinState.equals(ReplicatedEnvironment.State.MASTER)) {
            throw new IllegalStateException("bad state " + joinState);
        }

        populator.populateLog(rep);

        return rep;
    }

    public static ReplicatedEnvironment 
        makeReplicator(File envHome,
                       Durability durability,
                       long fileLen,
                       int bucketStride,
                       int bucketMaxMappings)
        throws DatabaseException {

        /*
         * Configure the environment with a specific durability and log file
         * length.
         */
        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        envConfig.setTransactional(true);
        envConfig.setDurability(durability);

        /* 
         * Disable anything that might asynchronously write the log and
         * interfere with this test's notion of what data should be present.
         */
        envConfig.setConfigParam(EnvironmentConfig.ENV_RUN_CLEANER,
                                 "false");
        envConfig.setConfigParam(EnvironmentConfig.ENV_RUN_CHECKPOINTER,
                                 "false");
        LocalCBVLSNUpdater.setSuppressGroupDBUpdates(true);

        DbInternal.disableParameterValidation(envConfig);
        envConfig.setConfigParam("je.log.fileMax", Long.toString(fileLen));

        int port = Integer.parseInt(RepParams.DEFAULT_PORT.getDefault());
        String hostName = TEST_HOST + ":" + port;
        ReplicationConfig repConfig = 
            new ReplicationConfig(GROUP_NAME, NODE_NAME, hostName);
        repConfig.setConfigParam
            (RepParams.ENV_SETUP_TIMEOUT.getName(), "60 s");
        repConfig.setConfigParam
            (ReplicationConfig.ENV_CONSISTENCY_TIMEOUT, "60 s");
        repConfig.setConfigParam("je.rep.vlsn.stride",
                                 Integer.toString(bucketStride));
        repConfig.setConfigParam("je.rep.vlsn.mappings",
                                 Integer.toString(bucketMaxMappings));
        repConfig.setConfigParam("je.rep.vlsn.distance",
                                  MAX_DISTANCE.toString());

        repConfig.setHelperHosts(hostName);

        /*
         * If -DlongTimeout is true, then this test will run with very
         * long timeouts, to make interactive debugging easier.
         */
        if (longTimeout) {
            RepTestUtils.setLongTimeouts(repConfig);
        }

        ReplicatedEnvironment rep =
            new ReplicatedEnvironment(envHome, repConfig, envConfig);
        return rep;
    }

    /**
     * Scan the log using a reader that checks every entry and use it to
     * verify that a FeederReader picks up the right replicated entries.
     * Also check that the FeederReader did actually make skip to the lsn
     * positions provided by the mapping.
     * @throws InterruptedException
     */
    public static ArrayList<CheckWireRecord>
        collectExpectedData(CheckReader checker)

        throws DatabaseException, InterruptedException {

        ArrayList<CheckWireRecord> expected = new ArrayList<CheckWireRecord>();

        /* Read every replicated log entry with the checker. */
        while (checker.readNextEntry()) {
            CheckWireRecord w = checker.getWireRecord();
            expected.add(w);
        }
        return expected;
    }

    /* This scans all log entries and picks out the replicated ones. */
    public static class CheckReader extends FileReader {

        private CheckWireRecord wireRecord;
        public long nScanned;

        public CheckReader(EnvironmentImpl envImpl)
            throws DatabaseException {

            super(envImpl, 
                  1000,            // readBufferSize
                  true,            // forward
                  DbLsn.NULL_LSN,  // startLsn
                  null,            // singleFileNumber
                  DbLsn.NULL_LSN,  // endOfFileLsn
                  DbLsn.NULL_LSN); // finishLsn
        }

        /** Return true if this entry is replicated. */
        @Override
        protected boolean isTargetEntry() {
            nScanned++;
            return entryIsReplicated();
        }

        @Override
        protected boolean processEntry(ByteBuffer entryBuffer)
                throws DatabaseException {

            ByteBuffer buffer = entryBuffer.slice();
            buffer.limit(currentEntryHeader.getItemSize());
            wireRecord = new CheckWireRecord(envImpl, getLastLsn(),
                                             currentEntryHeader.getType(),
                                             currentEntryHeader.getVersion(),
                                             currentEntryHeader.getItemSize(),
                                             currentEntryHeader.getVLSN(),
                                             buffer);

            entryBuffer.position(entryBuffer.position() +
                                 currentEntryHeader.getItemSize());
            return true;
        }

        CheckWireRecord getWireRecord() {
            return wireRecord;
        }
    }

    /**
     * A CheckWireRecord contains an OutputWireRecord read from the log. It
     * also adds the lsn of that WireRecord and instantiates the log entry
     * right away because we know we'll need it for test purposes.
     */
    public static class CheckWireRecord extends OutputWireRecord {
        public long lsn;
        private final LogEntry logEntry;

        CheckWireRecord(EnvironmentImpl envImpl,
                        long lsn,
                        byte entryType,
                        int entryVersion,
                        int itemSize,
                        VLSN vlsn,
                        ByteBuffer entryBuffer)
            throws DatabaseException {

            super(envImpl,
                  new LogEntryHeader(entryType, entryVersion, itemSize, vlsn),
                  entryBuffer);
            this.lsn = lsn;
            this.logEntry = instantiateEntry(envImpl, entryBuffer);
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("lsn=").append(DbLsn.getNoFormatString(lsn));
            sb.append(" ").append(header);
            sb.append(" ").append(logEntry);
            return sb.toString();
        }

        /**
         * @return true if this CheckWireRecord has the exact physical same
         * contents as the OutputWireRecord.  Must be called before the
         * entryBuffer that backs this OutputWireRecord is reused.
         * @throws DatabaseException
         */
        public boolean exactMatch(OutputWireRecord feederRecord)
            throws DatabaseException {

            if (!header.logicalEqualsIgnoreVersion(feederRecord.header)) {
                return false;
            }

            LogEntry feederEntry = feederRecord.instantiateEntry
                (envImpl, feederRecord.entryBuffer);
            StringBuilder sb = new StringBuilder();
            feederEntry.dumpEntry(sb, true);
            String feederString = sb.toString();

            sb = new StringBuilder();
            logEntry.dumpEntry(sb, true);
            String myEntryString = sb.toString();

            return myEntryString.equals(feederString);
        }
    }

    /**
     * Tests can customize how they populate a log.
     */
    public interface LogPopulator {

        /* Put the desired data into this environment. */
        public void populateLog(ReplicatedEnvironment rep);
    }
}
