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

package com.sleepycat.je.logversion;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.HashSet;
import java.util.Set;
import javax.transaction.xa.XAResource;

import com.sleepycat.je.CheckpointConfig;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.JEVersion;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.XAEnvironment;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.LogUtils.XidImpl;
import com.sleepycat.je.log.ReplicationContext;
import com.sleepycat.je.log.TestUtilLogReader;
import com.sleepycat.je.log.entry.EmptyLogEntry;
import com.sleepycat.je.log.entry.LogEntry;
import com.sleepycat.je.log.entry.MatchpointLogEntry;
import com.sleepycat.je.log.entry.SingleItemEntry;
import com.sleepycat.je.txn.RollbackEnd;
import com.sleepycat.je.txn.RollbackStart;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.Matchpoint;
import com.sleepycat.je.utilint.VLSN;
import com.sleepycat.utilint.StringUtils;

/**
 * This standalone command line program generates log files named je-x.y.z.jdb
 * and je-x.y.z.txt, where x.y.z is the version of JE used to run the program.
 * This program needs to be run for the current version of JE when we release
 * a new major version of JE.  It does not need to be run again for older
 * versions of JE, unless it is changed to generate new types of log entries
 * and we need to verify those log entries for all versions of JE.  In that
 * case the LogEntryVersionTest may also need to be changed.
 *
 * <p>Run this program with the desired version of JE in the classpath and pass
 * a home directory as the single command line argument.  After running this
 * program move the je-x.y.z.* files to the directory of this source package.
 * When adding je-x.y.z.jdb to CVS make sure to use -kb since it is a binary
 * file.</p>
 *
 * <p>This program can be run using the logversiondata ant target.</p>
 *
 * @see LogEntryVersionTest
 */
public class MakeLogEntryVersionData {

    /* Minimum child entries per BIN. */
    private static int N_ENTRIES = 4;

    private MakeLogEntryVersionData() {
    }

    public static void main(String[] args)
        throws Exception {

        if (args.length != 1) {
            throw new Exception("Home directory arg is required.");
        }

        File homeDir = new File(args[0]);
        File logFile = new File(homeDir, TestUtils.LOG_FILE_NAME);
        File renamedLogFile = new File(homeDir, "je-" +
            JEVersion.CURRENT_VERSION.getNumericVersionString() + ".jdb");
        File summaryFile = new File(homeDir, "je-" +
            JEVersion.CURRENT_VERSION.getNumericVersionString() + ".txt");

        if (logFile.exists()) {
            throw new Exception("Home directory (" + homeDir +
                                ") must be empty of log files.");
        }

        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        DbInternal.disableParameterValidation(envConfig);
        envConfig.setAllowCreate(true);
        envConfig.setTransactional(true);
        /* Make as small a log as possible to save space in CVS. */
        envConfig.setConfigParam
            (EnvironmentParams.ENV_RUN_INCOMPRESSOR.getName(), "false");
        envConfig.setConfigParam
            (EnvironmentParams.ENV_RUN_CLEANER.getName(), "false");
        envConfig.setConfigParam
            (EnvironmentParams.ENV_RUN_EVICTOR.getName(), "false");
        envConfig.setConfigParam
            (EnvironmentParams.ENV_RUN_CHECKPOINTER.getName(), "false");
        /* Use a 100 MB log file size to ensure only one file is written. */
        envConfig.setConfigParam(EnvironmentParams.LOG_FILE_MAX.getName(),
                                 Integer.toString(100 * (1 << 20)));
        /* Force BIN-delta. */
        envConfig.setConfigParam
            (EnvironmentParams.BIN_DELTA_PERCENT.getName(),
             Integer.toString(75));
        /* Ensure that we create two BINs with N_ENTRIES LNs. */
        envConfig.setConfigParam
            (EnvironmentParams.NODE_MAX.getName(),
             Integer.toString(N_ENTRIES));

        CheckpointConfig forceCheckpoint = new CheckpointConfig();
        forceCheckpoint.setForce(true);

        XAEnvironment env = new XAEnvironment(homeDir, envConfig);

        /* 
         * Make two shadow database. Database 1 is transactional and has
         * aborts, database 2 is not transactional.
         */
        for (int i = 0; i < 2; i += 1) {
            boolean transactional = (i == 0);
            String dbName = transactional ? Utils.DB1_NAME : Utils.DB2_NAME;

            DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setAllowCreate(true);
            dbConfig.setTransactional(transactional);
            dbConfig.setSortedDuplicates(true);
            Database db = env.openDatabase(null, dbName, dbConfig);

            Transaction txn = null;
            if (transactional) {
                txn = env.beginTransaction(null, null);
            }

            for (int j = 0; j < N_ENTRIES; j += 1) {
                db.put(txn, Utils.entry(j), Utils.entry(0));
            }
            db.put(txn, Utils.entry(0), Utils.entry(1));
            /* Update. */
            db.put(txn, Utils.entry(0), Utils.entry(1));

            /* Must checkpoint to generate BIN-deltas. */
            env.checkpoint(forceCheckpoint);

            /* Delete everything but the last LN to cause IN deletion. */
            for (int j = 0; j < N_ENTRIES - 1; j += 1) {
                db.delete(txn, Utils.entry(j));
            }

            if (transactional) {
                txn.abort();
            }

            db.close();
        }

        /* Compress twice to delete DBIN, DIN, BIN, IN. */
        env.compress();
        env.compress();

        /* DB2 was not aborted and will contain: {3, 0} */
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(false);
        dbConfig.setReadOnly(true);
        dbConfig.setSortedDuplicates(true);
        Database db = env.openDatabase(null, Utils.DB2_NAME, dbConfig);
        Cursor cursor = db.openCursor(null, null);
        try {
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry data = new DatabaseEntry();
            OperationStatus status = cursor.getFirst(key, data, null);
            if (status != OperationStatus.SUCCESS) {
                throw new Exception("Expected SUCCESS but got: " + status);
            }
            if (Utils.value(key) != 3 || Utils.value(data) != 0) {
                throw new Exception("Expected {3,0} but got: {" +
                                    Utils.value(key) + ',' +
                                    Utils.value(data) + '}');
            }
        } finally {
            cursor.close();
        }
        db.close();

        /* 
         * Make database 3, which is transactional and has some explicit 
         * transaction commit record.
         */
        dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setTransactional(true);
        Transaction txn = env.beginTransaction(null, null);
        db = env.openDatabase(null, Utils.DB3_NAME, dbConfig);
        OperationStatus status = db.put(txn, Utils.entry(99), Utils.entry(79));
        assert status == OperationStatus.SUCCESS: "status=" + status;
        db.close();
        txn.commit();
        
        /*
         * Generate an XA txn Prepare. The transaction must be non-empty in
         * order to actually log the Prepare.
         */
        XidImpl xid =
            new XidImpl(1, StringUtils.toUTF8("MakeLogEntryVersionData"), null);
        env.start(xid, XAResource.TMNOFLAGS);
        /* Re-write the existing {3,0} record. */
        dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(false);
        dbConfig.setReadOnly(false);
        dbConfig.setTransactional(true);
        dbConfig.setSortedDuplicates(true);
        db = env.openDatabase(null, Utils.DB2_NAME, dbConfig);
        db.put(null, Utils.entry(3), Utils.entry(0));
        db.close();
        env.prepare(xid);
        env.rollback(xid);

        EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);

        /* Log a RollbackStart entry. */
        LogEntry entry = 
            SingleItemEntry.create(LogEntryType.LOG_ROLLBACK_START, 
                                   new RollbackStart(VLSN.NULL_VLSN,
                                                     DbLsn.NULL_LSN,
                                                     new HashSet<Long>()));
        envImpl.getLogManager().log(entry, ReplicationContext.NO_REPLICATE);

        /* Log a RollbackEnd entry. */
        entry = SingleItemEntry.create(LogEntryType.LOG_ROLLBACK_END,
                                       new RollbackEnd(DbLsn.NULL_LSN, 
                                                       DbLsn.NULL_LSN));
        envImpl.getLogManager().log(entry, ReplicationContext.NO_REPLICATE);

        /* Log a Matchpoint entry. */
        entry = new MatchpointLogEntry(new Matchpoint(1));
        envImpl.getLogManager().log(entry, ReplicationContext.NO_REPLICATE);

        /* Log an ImmutableFile entry. */
        entry = SingleItemEntry.create(LogEntryType.LOG_IMMUTABLE_FILE,
                                       new EmptyLogEntry());
        envImpl.getLogManager().log(entry, ReplicationContext.NO_REPLICATE);

        env.close();

        /*
         * Get the set of all log entry types we expect to output.  We exclude
         * several types:
         * - MapLN_TX: MapLN (non-transactional) is now used instead.
         * - INDelete: root compression is no longer used.
         * - LN, LN_TX: deprecated and replaced by LN_INS/UPD/DEL, etc.
         * - DelDupLN, DelDupLN_TX, DupCountLN, DupCountLN_TX, DIN, DBIN,
         *   DupBINDelta, INDupDelete: deprecated, dup tree is no longer used.
         */
        Set<LogEntryType> expectedTypes = LogEntryType.getAllTypes();
        expectedTypes.remove(LogEntryType.LOG_MAPLN_TRANSACTIONAL);
        expectedTypes.remove(LogEntryType.LOG_IN_DELETE_INFO);
        expectedTypes.remove(LogEntryType.LOG_OLD_LN); // LN
        expectedTypes.remove(LogEntryType.LOG_OLD_LN_TRANSACTIONAL); // LN_TX
        expectedTypes.remove(LogEntryType.LOG_DEL_DUPLN);
        expectedTypes.remove(LogEntryType.LOG_DEL_DUPLN_TRANSACTIONAL);
        expectedTypes.remove(LogEntryType.LOG_DUPCOUNTLN);
        expectedTypes.remove(LogEntryType.LOG_DUPCOUNTLN_TRANSACTIONAL);
        expectedTypes.remove(LogEntryType.LOG_DIN);
        expectedTypes.remove(LogEntryType.LOG_DBIN);
        expectedTypes.remove(LogEntryType.LOG_OLD_DUP_BIN_DELTA);
        expectedTypes.remove(LogEntryType.LOG_IN_DUPDELETE_INFO);
        expectedTypes.remove(LogEntryType.LOG_OLD_BIN_DELTA);

        /* Open read-only and write all LogEntryType names to a text file. */
        envConfig.setReadOnly(true);
        Environment env2 = new Environment(homeDir, envConfig);
        PrintWriter writer = new PrintWriter
            (new BufferedOutputStream(new FileOutputStream(summaryFile)));
        TestUtilLogReader reader = new TestUtilLogReader
            (DbInternal.getNonNullEnvImpl(env2));
        while (reader.readNextEntry()) {
            LogEntryType type = reader.getEntryType();
            writer.println(type.toStringNoVersion() + '/' +
                           reader.getEntryVersion());
            expectedTypes.remove(type);
        }
        writer.close();
        env2.close();

        if (expectedTypes.size() > 0) {
            throw new Exception("Types not output: " + expectedTypes);
        }

        if (!logFile.exists()) {
            throw new Exception("What happened to: " + logFile);
        }

        if (!logFile.renameTo(renamedLogFile)) {
            throw new Exception
                ("Could not rename: " + logFile + " to " + renamedLogFile);
        }

        System.out.println("Created: " + renamedLogFile);
        System.out.println("Created: " + summaryFile);
    }
}
