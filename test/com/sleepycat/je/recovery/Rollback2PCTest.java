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

package com.sleepycat.je.recovery;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.File;

import javax.transaction.xa.XAException;

import org.junit.Test;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.je.CheckpointConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.XAEnvironment;
import com.sleepycat.je.log.LogUtils.XidImpl;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;
import com.sleepycat.utilint.StringUtils;

public class Rollback2PCTest extends TestBase {
    private final File envHome;

    public Rollback2PCTest() {
        envHome = SharedTestUtils.getTestDir();
    }

    /**
     * Test that getXATransaction does not return a prepared txn.
     */
    @Test
    public void testSR16375()
        throws DatabaseException, XAException {

            /* Setup environment. */
        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setTransactional(true);
        envConfig.setAllowCreate(true);
        XAEnvironment xaEnv = new XAEnvironment(envHome, envConfig);

        /* Setup database. */
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(true);
        dbConfig.setAllowCreate(true);
        Database db = xaEnv.openDatabase(null, "foo", dbConfig);

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        IntegerBinding.intToEntry(1, key);

        /*
         * Start an XA transaction and add a record.  Then crash the
         * environment.
         */
        XidImpl xid = new XidImpl(1, StringUtils.toUTF8("FooTxn"), null);
        Transaction preCrashTxn = xaEnv.beginTransaction(null, null);
        xaEnv.setXATransaction(xid, preCrashTxn);
        IntegerBinding.intToEntry(99, data);
        assertEquals(OperationStatus.SUCCESS, db.put(preCrashTxn, key, data));
        db.close();
        xaEnv.prepare(xid);
        xaEnv.sync();

        /* Crash */
        DbInternal.getNonNullEnvImpl(xaEnv).abnormalClose();
        xaEnv = null;

        /* Recover */
        envConfig.setAllowCreate(false);
        xaEnv = new XAEnvironment(envHome, envConfig);

        /* Ensure that getXATransaction returns null. */
        Transaction resumedTxn = xaEnv.getXATransaction(xid);
        assertNull(resumedTxn);

        /* Rollback. */
        xaEnv.rollback(xid);
        DbInternal.getNonNullEnvImpl(xaEnv).abnormalClose();
    }

    /**
     * Verifies a bug fix to a problem that occurs when aborting a prepared txn
     * after recovery.  During recovery, we were counting the old version of an
     * LN as obsolete when replaying the prepared txn LN.  But if that txn
     * aborts later, the old version becomes active.  The fix is to use inexact
     * counting.  [#17022]
     */
    @Test
    public void testLogCleanAfterRollbackPrepared()
        throws DatabaseException, XAException {

        /*
         * Setup environment.
         *
         * We intentionally do not disable the checkpointer daemon to add
         * variability to the test.  This variability found a checkpointer bug
         * in the past.  [#20270]
         */
        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setTransactional(true);
        envConfig.setAllowCreate(true);
        envConfig.setConfigParam(EnvironmentConfig.ENV_RUN_CLEANER,
                                 "false");
        envConfig.setConfigParam(EnvironmentConfig.CLEANER_MIN_UTILIZATION,
                                 "90");
        XAEnvironment xaEnv = new XAEnvironment(envHome, envConfig);

        /* Setup database. */
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(true);
        dbConfig.setAllowCreate(true);
        Database db = xaEnv.openDatabase(null, "foo", dbConfig);

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        IntegerBinding.intToEntry(1, key);
        IntegerBinding.intToEntry(99, data);
        assertEquals(OperationStatus.SUCCESS, db.put(null, key, data));
        DbInternal.getNonNullEnvImpl(xaEnv).forceLogFileFlip();
        DbInternal.getNonNullEnvImpl(xaEnv).forceLogFileFlip();
        DbInternal.getNonNullEnvImpl(xaEnv).forceLogFileFlip();

        /*
         * Start an XA transaction and add a record.  Then crash the
         * environment.
         */
        XidImpl xid = new XidImpl(1, StringUtils.toUTF8("FooTxn"), null);
        Transaction preCrashTxn = xaEnv.beginTransaction(null, null);
        xaEnv.setXATransaction(xid, preCrashTxn);
        IntegerBinding.intToEntry(100, data);
        assertEquals(OperationStatus.SUCCESS, db.put(preCrashTxn, key, data));
        db.close();
        xaEnv.prepare(xid);
        DbInternal.getNonNullEnvImpl(xaEnv).getLogManager().flushSync();

        /* Crash */
        DbInternal.getNonNullEnvImpl(xaEnv).abnormalClose();
        xaEnv = null;

        /* Recover */
        envConfig.setAllowCreate(false);
        xaEnv = new XAEnvironment(envHome, envConfig);

        /* Rollback. */
        xaEnv.rollback(xid);
        
        /* Force log cleaning. */
        CheckpointConfig force = new CheckpointConfig();
        force.setForce(true);
        xaEnv.checkpoint(force);
        xaEnv.cleanLog();
        xaEnv.checkpoint(force);

        /* Close and re-open, ensure we can read the original record. */
        xaEnv.close();
        xaEnv = new XAEnvironment(envHome, envConfig);
        db = xaEnv.openDatabase(null, "foo", dbConfig);
        /* Before the fix, the get() caused a LogFileNotFound. */
        assertEquals(OperationStatus.SUCCESS, db.get(null, key, data, null));
        /* BEGIN debugging code. */
        if (99 != IntegerBinding.entryToInt(data)) {
            String entryTypes = null;
            String txnIds = null;
            long startLsn = DbLsn.NULL_LSN;
            long endLsn = DbLsn.NULL_LSN;
            boolean verbose = true;
            boolean stats = false;
            boolean csvFormat = false;
            boolean repEntriesOnly = false;
            boolean forwards = true;
            String customDumpReaderClass = null;
            new com.sleepycat.je.util.DbPrintLog().dump
                (envHome, entryTypes, txnIds, startLsn, endLsn,
                 verbose, stats, repEntriesOnly, csvFormat, forwards, false,
                 customDumpReaderClass);
        }
        /* END debugging code. */
        assertEquals(99, IntegerBinding.entryToInt(data));
        db.close();
        xaEnv.close();
    }
}
