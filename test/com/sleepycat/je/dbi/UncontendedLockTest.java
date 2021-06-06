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

package com.sleepycat.je.dbi;

import static com.sleepycat.je.txn.LockStatDefinition.LOCK_READ_LOCKS;
import static com.sleepycat.je.txn.LockStatDefinition.LOCK_WRITE_LOCKS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import java.io.File;

import org.junit.After;
import org.junit.Test;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentStats;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.util.DualTestCase;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.je.utilint.StatGroup;
import com.sleepycat.util.test.SharedTestUtils;

/**
 * Checks that optimized uncontended locks (see CursorImpl.lockLN) are taken
 * during delete/update operations for a non-duplicates DB.  For a duplicates
 * DB, ensure that uncontended locks are not taken, since this is not fully
 * implemented and would be unreliable.
 */
public class UncontendedLockTest extends DualTestCase {

    private static final StatsConfig CLEAR_STATS;
    static {
        CLEAR_STATS = new StatsConfig();
        CLEAR_STATS.setClear(true);
    }
    private final File envHome;
    private Environment env;
    private Database db;
    private boolean isSerializable;

    public UncontendedLockTest() {
        envHome = SharedTestUtils.getTestDir();
    }

    @After
    public void tearDown() 
        throws Exception {

        try {
            super.tearDown();
        } catch (Throwable e) {
            System.out.println("tearDown: " + e);
        }
        env = null;
        db = null;
    }

    @Test
    public void testUncontended() {
        doUncontended(false);
    }

    @Test
    public void testUncontendedDups() {
        doUncontended(true);
    }

    private void doUncontended(boolean dups) {
        open(dups);
        env.getStats(CLEAR_STATS);

        /*
         * NOTE: Number of contended and dup DB requests are expected based on
         * testing, not on any specific requirement.  If we can reduce these
         * over time, great, just update the test.
         *
         * The critical thing is that the number of requests and write locks is
         * exactly one for the non-dup, non-contended case.
         */

        /* Insert */
        Transaction txn = env.beginTransaction(null, null);
        writeData(dups, txn, false /*update*/);
        checkLocks(
            txn,
            dups ? 2 : 1 /*nRequests*/,
            dups ? 2 : 1 /*nWriteLocks*/);
        txn.commit();

        /* Update */
        txn = env.beginTransaction(null, null);
        writeData(dups, txn, true /*update*/);
        checkLocks(
            txn,
            dups ? 2 : 1 /*nRequests*/,
            dups ? 2 : 1 /*nWriteLocks*/);
        txn.commit();

        /* Delete */
        txn = env.beginTransaction(null, null);
        deleteData(dups, txn);
        checkLocks(
            txn,
            dups ? 3 : (isSerializable ? 3 : 1) /*nRequests*/,
            dups ? 2 : (isSerializable ? 2 : 1) /*nWriteLocks*/);
        txn.commit();

        close();
    }

    @Test
    public void testContended() {
        doContended(false);
    }

    @Test
    public void testContendedDups() {
        doContended(true);
    }

    private void doContended(boolean dups) {
        open(dups);
        env.getStats(CLEAR_STATS);

        /*
         * NOTE: Number of contended and dup DB requests are expected based on
         * testing, not on any specific requirement.  If we can reduce these
         * over time, great, just update the test.
         */

        /* Insert - no way to have contention on a new slot. */
        writeData(dups, null, false /*update*/);

        /* Simulate contended locking by reading first. */

        /* Update */
        Transaction txn = env.beginTransaction(null, null);
        readData(dups, txn);
        writeData(dups, txn, true /*update*/);
        checkLocks(
            txn,
            dups ? (isSerializable ? 6 : 6) : (isSerializable ? 4 : 4)
            /*nRequests*/,
            dups ? 3 : 2 /*nWriteLocks*/);
        txn.commit();

        /* Delete */
        txn = env.beginTransaction(null, null);
        readData(dups, txn);
        deleteData(dups, txn);
        checkLocks(
            txn,
            dups ? 4 : (isSerializable ? 4 : 3) /*nRequests*/,
            dups ? 2 : 2 /*nWriteLocks*/);
        txn.commit();

        close();
    }

    private void checkLocks(Transaction txn, int nRequests, int nWriteLocks) {
        final EnvironmentStats envStats = env.getStats(CLEAR_STATS);
        assertEquals(nRequests, envStats.getNRequests());
        assertEquals(0, envStats.getNWaits());
        StatGroup lockerStats = DbInternal.getLocker(txn).collectStats();
        assertEquals(0, lockerStats.getInt(LOCK_READ_LOCKS));
        assertEquals(nWriteLocks, lockerStats.getInt(LOCK_WRITE_LOCKS));
    }

    private void open(boolean dups) {
        final EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setTransactional(true);
        envConfig.setAllowCreate(true);
        /* Don't run daemons, so they don't lock unexpectedly. */
        envConfig.setConfigParam(
            EnvironmentConfig.ENV_RUN_CHECKPOINTER, "false");
        envConfig.setConfigParam(
            EnvironmentConfig.ENV_RUN_CLEANER, "false");
        envConfig.setConfigParam(
            EnvironmentConfig.ENV_RUN_EVICTOR, "false");
        envConfig.setConfigParam(
            EnvironmentConfig.ENV_RUN_IN_COMPRESSOR, "false");
        envConfig.setConfigParam(EnvironmentConfig.VERIFY_BTREE, "false");
        env = create(envHome, envConfig);

        isSerializable = env.getConfig().getTxnSerializableIsolation();

        final DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(true);
        dbConfig.setAllowCreate(true);
        dbConfig.setSortedDuplicates(dups);
        db = env.openDatabase(null, "foo", dbConfig);
    }

    private void close() {
        db.close();
        close(env);
    }

    private void writeData(boolean dups, Transaction txn, boolean update) {
        final DatabaseEntry key = new DatabaseEntry();
        final DatabaseEntry data = new DatabaseEntry();
        IntegerBinding.intToEntry(1, key);
        IntegerBinding.intToEntry(1, data);
        OperationStatus status;
        /* putNoOverwrite not used for dups because it does extra locking. */
        if (update || dups) {
            status = db.put(txn, key, data);
        } else {
            status = db.putNoOverwrite(txn, key, data);
        }
        assertSame(OperationStatus.SUCCESS, status);
        if (!dups) {
            return;
        }
        IntegerBinding.intToEntry(2, data);
        if (update) {
            status = db.put(txn, key, data);
        } else {
            status = db.putNoDupData(txn, key, data);
        }
        assertSame(OperationStatus.SUCCESS, status);
    }

    private void deleteData(boolean dups, Transaction txn) {
        final DatabaseEntry key = new DatabaseEntry();
        IntegerBinding.intToEntry(1, key);
        if (!dups) {
            final OperationStatus status = db.delete(txn, key);
            assertSame(OperationStatus.SUCCESS, status);
            return;
        }
        final DatabaseEntry data = new DatabaseEntry();
        IntegerBinding.intToEntry(2, data);
        final Cursor cursor = db.openCursor(txn, null);
        OperationStatus status = cursor.getSearchBoth(key, data, null);
        assertSame(OperationStatus.SUCCESS, status);
        status = cursor.delete();
        assertSame(OperationStatus.SUCCESS, status);
        cursor.close();
    }

    private void readData(boolean dups, Transaction txn) {
        final DatabaseEntry key = new DatabaseEntry();
        IntegerBinding.intToEntry(1, key);
        if (!dups) {
            final OperationStatus status =
                db.get(txn, key, new DatabaseEntry(), null);
            assertSame(OperationStatus.SUCCESS, status);
            return;
        }
        final DatabaseEntry data = new DatabaseEntry();
        IntegerBinding.intToEntry(2, data);
        final OperationStatus status = db.getSearchBoth(txn, key, data, null);
        assertSame(OperationStatus.SUCCESS, status);
    }
}
