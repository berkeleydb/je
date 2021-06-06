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

package com.sleepycat.je;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;

import org.junit.Test;

import com.sleepycat.je.util.DualTestCase;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.util.test.SharedTestUtils;

/**
 * BDB's transactional DDL operations (database creation, truncation,
 * remove and rename) need special support through what we call "handle" locks.
 *
 * When a database is created, a write lock is taken. When the creation 
 * transaction is committed, that write lock should be turned into a read lock
 * and should be transferred to the database handle.
 *
 * Note that when this test is run in HA mode, environment creation results in
 * a different number of outstanding locks. And example of a HA specific lock
 * is that taken for the RepGroupDb, which holds replication group information.
 * Because of that, this test takes care to check for a relative number of 
 * locks, rather than an absolute number.
 */
public class DbHandleLockTest extends DualTestCase {
    private File envHome;
    private Environment env;

    public DbHandleLockTest() {
        envHome = SharedTestUtils.getTestDir();
    }

    public void openEnv() {
        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setTransactional(true);
        envConfig.setAllowCreate(true);
        env = create(envHome, envConfig);
    }

    @Test
    public void testOpenHandle()
        throws Throwable {

        try {
            EnvironmentConfig envConfig = TestUtils.initEnvConfig();
            envConfig.setTransactional(true);
            envConfig.setAllowCreate(true);
            envConfig.setConfigParam(EnvironmentConfig.VERIFY_BTREE, "false");
            env = create(envHome, envConfig);
            Transaction txnA =
                env.beginTransaction(null, TransactionConfig.DEFAULT);
            DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setTransactional(true);
            dbConfig.setAllowCreate(true);
           
            LockStats oldLockStat = env.getLockStats(null);
             
            Database db = env.openDatabase(txnA, "foo", dbConfig);

            /*
             * At this point, we expect a write lock on the NameLN by txnA, and
             * a read lock on the NameLN by the handle locker.
             */
            LockStats lockStat = env.getLockStats(null);
            assertEquals(oldLockStat.getNTotalLocks() + 1, 
                    lockStat.getNTotalLocks());
            assertEquals(oldLockStat.getNWriteLocks() + 1, 
                    lockStat.getNWriteLocks());
            assertEquals(oldLockStat.getNReadLocks() + 1, 
                    lockStat.getNReadLocks());

            txnA.commit();

            lockStat = env.getLockStats(null);
            assertEquals(oldLockStat.getNTotalLocks() + 1, 
                    lockStat.getNTotalLocks());
            assertEquals(oldLockStat.getNWriteLocks(), 
                    lockStat.getNWriteLocks());
            assertEquals(oldLockStat.getNReadLocks() + 1, 
                    lockStat.getNReadLocks());

            /* Updating the root from another txn should be possible. */
            insertData(10, db);
            db.close();

            lockStat = env.getLockStats(null);
            assertEquals(oldLockStat.getNTotalLocks(), 
                    lockStat.getNTotalLocks());
            assertEquals(oldLockStat.getNWriteLocks(), 
                    lockStat.getNWriteLocks());
            assertEquals(oldLockStat.getNReadLocks(), 
                    lockStat.getNReadLocks());
            close(env);
            env = null;
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }

    @Test
    public void testSR12068()
        throws Throwable {

        try {
            openEnv();
            Transaction txnA =
                env.beginTransaction(null, TransactionConfig.DEFAULT);

            DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setTransactional(true);
            dbConfig.setAllowCreate(true);
            Database db = env.openDatabase(txnA, "foo", dbConfig);
            db.close();

            dbConfig.setExclusiveCreate(true);
            try {
                db = env.openDatabase(txnA, "foo", dbConfig);
                fail("should throw database exeception");
            } catch (DatabaseException DE) {
                /* expected Database already exists. */
            }
            dbConfig.setAllowCreate(false);
            dbConfig.setExclusiveCreate(false);
            db = env.openDatabase(txnA, "foo", dbConfig);
            db.close();
            txnA.commit();
            txnA = env.beginTransaction(null, TransactionConfig.DEFAULT);
            env.removeDatabase(txnA, "foo");
            txnA.commit();
            close(env);
            env = null;
        } catch (Throwable T) {
            T.printStackTrace();
            throw T;
        }
    }

    private void insertData(int numRecs, Database db)
        throws Throwable {

        for (int i = 0; i < numRecs; i++) {
            DatabaseEntry key = new DatabaseEntry(TestUtils.getTestArray(i));
            DatabaseEntry data = new DatabaseEntry(TestUtils.getTestArray(i));
            assertEquals(OperationStatus.SUCCESS,
                    db.put(null, key, data));
        }
    }

    /**
     * Ensures that handle locks are released on the old LSN when a NameLN is
     * migrated by the cleaner. [#20617]
     */
    @Test
    public void testReleaseHandleLocks() {

        final EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setTransactional(true);
        envConfig.setAllowCreate(true);
        envConfig.setConfigParam(EnvironmentConfig.LOG_FILE_MAX, "1000000");
        envConfig.setConfigParam(EnvironmentConfig.VERIFY_BTREE, "false");
        env = create(envHome, envConfig);

        final int nDbs = 500;
        final int dataSize = 100000;
        final Database[] handles = new Database[nDbs];

        final DatabaseEntry key = new DatabaseEntry(new byte[1]);
        final DatabaseEntry data = new DatabaseEntry(new byte[dataSize]);

        for (int i = 0; i < nDbs; i += 1) {
            final DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setTransactional(true);
            dbConfig.setAllowCreate(true);
            handles[i] = env.openDatabase(null, "db" + i, dbConfig);
            final Database db = handles[i];
            OperationStatus status = db.put(null, key, data);
            assertSame(OperationStatus.SUCCESS, status);
            if (i < nDbs - 100) {
                status = db.delete(null, key);
                assertSame(OperationStatus.SUCCESS, status);
            }
        }
        env.cleanLog();
        env.checkpoint(new CheckpointConfig().setForce(true));

        final EnvironmentStats stats = env.getStats(null);
        assertTrue(String.valueOf(stats.getNReadLocks()),
                   stats.getNReadLocks() < (nDbs * 2));
        for (final Database db : handles) {
            db.close();
        }
        close(env);
    }
}
