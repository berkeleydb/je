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

package com.sleepycat.je.txn;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.junit.Test;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockConflictException;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.DbTree;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.junit.JUnitThread;
import com.sleepycat.je.util.DualTestCase;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.util.test.SharedTestUtils;

public class LockManagerTest extends DualTestCase {

    private Locker txn1;
    private Locker txn2;
    private Locker txn3;
    private Locker txn4;
    private Long nid;
    private AtomicInteger sequence;

    private Environment env;
    private final File envHome;

    public LockManagerTest() {
        envHome =  SharedTestUtils.getTestDir();
    }

    @Before
    public void setUp()
        throws Exception {

        super.setUp();
        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setConfigParam(EnvironmentParams.NODE_MAX.getName(), "6");
        envConfig.setConfigParam(EnvironmentParams.N_LOCK_TABLES.getName(),
                                 "11");
        envConfig.setAllowCreate(true);
        envConfig.setTransactional(true);
        env = create(envHome, envConfig);

        nid = new Long(1);
        sequence = new AtomicInteger(0);
    }

    private void initLockers() 
        throws DatabaseException {

        EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);

        txn1 = BasicLocker.createBasicLocker(envImpl);
        txn2 = BasicLocker.createBasicLocker(envImpl);
        txn3 = BasicLocker.createBasicLocker(envImpl);
        txn4 = BasicLocker.createBasicLocker(envImpl);
    }

    private void initTxns(TransactionConfig config, EnvironmentImpl envImpl) 
        throws DatabaseException {

        txn1 = Txn.createLocalTxn(envImpl, config);
        txn2 = Txn.createLocalTxn(envImpl, config);
        txn3 = Txn.createLocalTxn(envImpl, config);
        txn4 = Txn.createLocalTxn(envImpl, config);
    }

    private void closeEnv() 
        throws DatabaseException {

        txn1.operationEnd();
        txn2.operationEnd();
        txn3.operationEnd();
        txn4.operationEnd();

        close(env);
    }

    /*
     * SR15926 showed a bug where node IDs that are > 0x80000000 produce
     * negative lock table indexes becuase of the modulo arithmetic in
     * LockManager.getLockTableIndex().
     *
     * Since node IDs are no longer used for locking, this test is somewhat
     * outdated.  However, it is still a good idea to check that we can lock
     * an LSN value with the sign bit set.
     */
    @Test
    public void testSR15926LargeNodeIds()
        throws Exception {

        initLockers();

        final LockManager lockManager = DbInternal.getNonNullEnvImpl(env).
            getTxnManager().getLockManager();

        try {
            lockManager.lock(0x80000000L, txn1, LockType.WRITE,
                             0, false, false, null);
        } catch (Exception e) {
            fail("shouldn't get exception " + e);
        } finally {
            closeEnv();
        }
    }

    @Test
    public void testNegatives()
        throws Exception {

        initLockers();

        final LockManager lockManager = DbInternal.getNonNullEnvImpl(env).
            getTxnManager().getLockManager();

        try {
            assertFalse(lockManager.isOwner(nid, txn1, LockType.READ));
            assertFalse(lockManager.isOwner(nid, txn1, LockType.WRITE));
            assertFalse(lockManager.isLocked(nid));
            assertFalse(lockManager.isWaiter(nid, txn1));
            lockManager.lock(1, txn1, LockType.READ, 0, false, false, null);

            /* already holds this lock */
            assertEquals(LockGrantType.EXISTING,
                         lockManager.lock(1, txn1, LockType.READ, 0,
                         false, false, null));
            assertFalse(lockManager.isOwner(nid, txn2, LockType.READ));
            assertFalse(lockManager.isOwner(nid, txn2, LockType.WRITE));
            assertTrue(lockManager.isLocked(nid));
            assertTrue(lockManager.nOwners(new Long(2)) == -1);
            assertTrue(lockManager.nWaiters(new Long(2)) == -1);

            /* lock 2 doesn't exist, shouldn't affect any the existing lock */
            lockManager.release(2L, txn1);
            txn1.removeLock(2L);
            assertTrue(lockManager.isLocked(nid));

            /* txn2 is not the owner, shouldn't release lock 1. */
            lockManager.release(1L, txn2);
            txn2.removeLock(1L);
            assertTrue(lockManager.isLocked(nid));
            assertTrue(lockManager.isOwner(nid, txn1, LockType.READ));
            assertTrue(lockManager.nOwners(nid) == 1);

            /* Now really release. */
            lockManager.release(1L, txn1);
            txn1.removeLock(1L);
            assertFalse(lockManager.isLocked(nid));
            assertFalse(lockManager.isOwner(nid, txn1, LockType.READ));
            assertFalse(lockManager.nOwners(nid) == 1);

            lockManager.lock(1, txn1, LockType.WRITE, 0, false, false, null);
            /* holds write and subsequent request for READ is ok */
            lockManager.lock(1, txn1, LockType.READ, 0, false, false, null);
            /* already holds this lock */
            assertTrue(lockManager.lock(1, txn1, LockType.WRITE,
                        0, false, false, null) ==
                    LockGrantType.EXISTING);
            assertFalse(lockManager.isWaiter(nid, txn1));
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            closeEnv();
        }
    }

    /**
     * Acquire three read locks and make sure that they share nicely.
     */
    @Test
    public void testMultipleReaders()
        throws Throwable {

        initLockers();

        final LockManager lockManager = DbInternal.getNonNullEnvImpl(env).
            getTxnManager().getLockManager();

        JUnitThread tester1 =
            new JUnitThread("testMultipleReaders1") {
                public void testBody() {
                    try {
                        lockManager.lock(1, txn1, LockType.READ, 0, 
                                false, false, null);
                        assertTrue
                            (lockManager.isOwner(nid, txn1, LockType.READ));
                        sequence.incrementAndGet();
                        while (sequence.get() < 3) {
                            Thread.yield();
                        }
                        lockManager.release(1L, txn1);
                        txn1.removeLock(1L);
                    } catch (DatabaseException DBE) {
                        DBE.printStackTrace();
                        fail("caught DatabaseException " + DBE);
                    }
                }
            };

        JUnitThread tester2 =
            new JUnitThread("testMultipleReaders2") {
                public void testBody() {
                    try {
                        lockManager.lock(1, txn2, LockType.READ, 0,
                                false, false, null);
                        assertTrue
                            (lockManager.isOwner(nid, txn2, LockType.READ));
                        sequence.incrementAndGet();
                        while (sequence.get() < 3) {
                            Thread.yield();
                        }
                        lockManager.release(1L, txn2);
                        txn2.removeLock(1L);
                    } catch (DatabaseException DBE) {
                        DBE.printStackTrace();
                        fail("caught DatabaseException " + DBE);
                    }
                }
            };

        JUnitThread tester3 =
            new JUnitThread("testMultipleReaders3") {
                public void testBody() {
                    try {
                        lockManager.lock(1, txn3, LockType.READ, 0,
                                false, false, null);
                        assertTrue
                            (lockManager.isOwner(nid, txn3, LockType.READ));
                        sequence.incrementAndGet();
                        while (sequence.get() < 3) {
                            Thread.yield();
                        }
                        lockManager.release(1L, txn3);
                        txn3.removeLock(1L);
                    } catch (DatabaseException DBE) {
                        DBE.printStackTrace();
                        fail("caught DatabaseException " + DBE);
                    }
                }
            };

        tester1.start();
        tester2.start();
        tester3.start();
        tester1.finishTest();
        tester2.finishTest();
        tester3.finishTest();
        closeEnv();
    }

    /**
     * Grab two read locks, hold them, and make sure that a write lock
     * waits for them to be released.
     */
    @Test
    public void testMultipleReadersSingleWrite1()
        throws Throwable {

        initLockers();

        final LockManager lockManager = DbInternal.getNonNullEnvImpl(env).
            getTxnManager().getLockManager();

        JUnitThread tester1 =
            new JUnitThread("testMultipleReaders1") {
                public void testBody() {
                    try {
                        lockManager.lock(1, txn1, LockType.READ, 0,
                                false, false, null);
                        assertTrue
                            (lockManager.isOwner(nid, txn1, LockType.READ));
                        while (lockManager.nWaiters(nid) < 1) {
                            Thread.yield();
                        }
                        assertTrue(lockManager.isWaiter(nid, txn3));
                        assertFalse(lockManager.isWaiter(nid, txn1));
                        lockManager.release(1L, txn1);
                        txn1.removeLock(1L);
                        assertFalse
                            (lockManager.isOwner(nid, txn1, LockType.READ));
                    } catch (DatabaseException DBE) {
                        DBE.printStackTrace();
                        fail("caught DatabaseException " + DBE);
                    }
                }
            };

        JUnitThread tester2 =
            new JUnitThread("testMultipleReaders2") {
                public void testBody() {
                    try {
                        lockManager.lock(1, txn2, LockType.READ, 0,
                                false, false, null);
                        assertTrue
                            (lockManager.isOwner(nid, txn2, LockType.READ));
                        while (lockManager.nWaiters(nid) < 1) {
                            Thread.yield();
                        }
                        assertTrue(lockManager.isWaiter(nid, txn3));
                        lockManager.release(1L, txn2);
                        txn2.removeLock(1L);
                        assertFalse
                            (lockManager.isOwner(nid, txn2, LockType.READ));
                    } catch (DatabaseException DBE) {
                        DBE.printStackTrace();
                        fail("caught DatabaseException " + DBE);
                    }
                }
            };

        JUnitThread tester3 =
            new JUnitThread("testMultipleReaders3") {
                public void testBody() {
                    try {
                        while (lockManager.nOwners(nid) < 2) {
                            Thread.yield();
                        }
                        lockManager.lock(1, txn3, LockType.WRITE, 0,
                                false, false, null);
                        assertTrue
                            (lockManager.isOwner(nid, txn3, LockType.WRITE));
                    } catch (DatabaseException DBE) {
                        DBE.printStackTrace();
                        fail("caught DatabaseException " + DBE);
                    }
                }
            };

        tester1.start();
        tester2.start();
        tester3.start();
        tester1.finishTest();
        tester2.finishTest();
        tester3.finishTest();
        closeEnv();
    }

    /**
     * Acquire two read locks, put a write locker behind the two
     * read lockers, and then queue a read locker behind the writer.
     * Ensure that the third reader is not granted until the writer
     * releases the lock.
     */
    @Test
    public void testMultipleReadersSingleWrite2()
        throws Throwable {

        initLockers();

        final LockManager lockManager = DbInternal.getNonNullEnvImpl(env).
            getTxnManager().getLockManager();

        JUnitThread tester1 =
            new JUnitThread("testMultipleReaders1") {
                public void testBody() {
                    try {
                        lockManager.lock(1, txn1, LockType.READ, 0,
                                false, false, null);
                        assertTrue
                            (lockManager.isOwner(nid, txn1, LockType.READ));
                        while (lockManager.nWaiters(nid) < 2) {
                            Thread.yield();
                        }
                        lockManager.release(1L, txn1);
                        txn1.removeLock(1L);
                    } catch (DatabaseException DBE) {
                        DBE.printStackTrace();
                        fail("caught DatabaseException " + DBE);
                    }
                }
            };

        JUnitThread tester2 =
            new JUnitThread("testMultipleReaders2") {
                public void testBody() {
                    try {
                        lockManager.lock(1, txn2, LockType.READ, 0,
                                false, false, null);
                        assertTrue
                            (lockManager.isOwner(nid, txn2, LockType.READ));
                        while (lockManager.nWaiters(nid) < 2) {
                            Thread.yield();
                        }
                        lockManager.release(1L, txn2);
                        txn2.removeLock(1L);
                    } catch (DatabaseException DBE) {
                        DBE.printStackTrace();
                        fail("caught DatabaseException " + DBE);
                    }
                }
            };

        JUnitThread tester3 =
            new JUnitThread("testMultipleReaders3") {
                public void testBody() {
                    try {
                        while (lockManager.nOwners(nid) < 2) {
                            Thread.yield();
                        }
                        lockManager.lock(1, txn3, LockType.WRITE, 0,
                                false, false, null);
                        while (lockManager.nWaiters(nid) < 1) {
                            Thread.yield();
                        }
                        assertTrue
                            (lockManager.isOwner(nid, txn3, LockType.WRITE));
                        lockManager.release(1L, txn3);
                        txn3.removeLock(1L);
                    } catch (DatabaseException DBE) {
                        DBE.printStackTrace();
                        fail("caught DatabaseException " + DBE);
                    }
                }
            };

        JUnitThread tester4 =
            new JUnitThread("testMultipleReaders4") {
                public void testBody() {
                    try {
                        while (lockManager.nWaiters(nid) < 1) {
                            Thread.yield();
                        }
                        lockManager.lock(1, txn4, LockType.READ, 0,
                                false, false, null);
                        assertTrue
                            (lockManager.isOwner(nid, txn4, LockType.READ));
                        lockManager.release(1L, txn4);
                        txn4.removeLock(1L);
                    } catch (DatabaseException DBE) {
                        DBE.printStackTrace();
                        fail("caught DatabaseException " + DBE);
                    }
                }
            };

        tester1.start();
        tester2.start();
        tester3.start();
        tester4.start();
               tester1.finishTest();
        tester2.finishTest();
        tester3.finishTest();
        tester4.finishTest();
        closeEnv();
    }

    /**
     * Acquire two read locks for two transactions, then request a write
     * lock for a third transaction.  Then request a write lock for one
     * of the first transactions that already has a read lock (i.e.
     * request an upgrade lock).  Make sure it butts in front of the
     * existing wait lock.
     */
    @Test
    public void testUpgradeLock()
        throws Throwable {

        initLockers();

        final LockManager lockManager = DbInternal.getNonNullEnvImpl(env).
            getTxnManager().getLockManager();

        JUnitThread tester1 =
            new JUnitThread("testUpgradeLock1") {
                public void testBody() {
                    try {
                        lockManager.lock(1, txn1, LockType.READ, 0,
                                         false, false, null);
                        assertTrue
                            (lockManager.isOwner(nid, txn1, LockType.READ));
                        while (lockManager.nWaiters(nid) < 2) {
                            Thread.yield();
                        }
                        lockManager.release(1L, txn1);
                        txn1.removeLock(1L);
                    } catch (DatabaseException DBE) {
                        DBE.printStackTrace();
                        fail("caught DatabaseException " + DBE);
                    }
                }
            };

        JUnitThread tester2 =
            new JUnitThread("testUpgradeLock2") {
                public void testBody() {
                    try {
                        lockManager.lock(1, txn2, LockType.READ, 0,
                                         false, false, null);
                        assertTrue
                            (lockManager.isOwner(nid, txn2, LockType.READ));
                        while (lockManager.nWaiters(nid) < 1) {
                            Thread.yield();
                        }
                        lockManager.lock(1, txn2, LockType.WRITE, 0,
                                         false, false, null);
                        assertTrue(lockManager.nWaiters(nid) == 1);
                        lockManager.release(1L, txn2);
                        txn2.removeLock(1L);
                    } catch (DatabaseException DBE) {
                        DBE.printStackTrace();
                        fail("caught DatabaseException " + DBE);
                    }
                }
            };

        JUnitThread tester3 =
            new JUnitThread("testUpgradeLock3") {
                public void testBody() {
                    try {
                        while (lockManager.nOwners(nid) < 2) {
                            Thread.yield();
                        }
                        lockManager.lock(1, txn3, LockType.WRITE, 0,
                                         false, false, null);
                        assertTrue
                            (lockManager.isOwner(nid, txn3, LockType.WRITE));
                        lockManager.release(1L, txn3);
                        txn3.removeLock(1L);
                    } catch (DatabaseException DBE) {
                        DBE.printStackTrace();
                        fail("caught DatabaseException " + DBE);
                    }
                }
            };

        tester1.start();
        tester2.start();
        tester3.start();
        tester1.finishTest();
        tester2.finishTest();
        tester3.finishTest();
        closeEnv();
    }

    /**
     * Acquire a read lock, then request a write lock for a second
     * transaction in non-blocking mode.  Make sure it fails.
     */
    @Test
    public void testNonBlockingLock1()
        throws Throwable {

        initLockers();

        final LockManager lockManager = DbInternal.getNonNullEnvImpl(env).
            getTxnManager().getLockManager();

        JUnitThread tester1 =
            new JUnitThread("testNonBlocking1") {
                public void testBody() {
                    try {
                        lockManager.lock(1, txn1, LockType.READ, 0,
                                         false, false, null);
                        assertTrue
                            (lockManager.isOwner(nid, txn1, LockType.READ));
                        while (sequence.get() < 1) {
                            Thread.yield();
                        }
                        lockManager.release(1L, txn1);
                        txn1.removeLock(1L);
                    } catch (DatabaseException DBE) {
                        DBE.printStackTrace();
                        fail("caught DatabaseException " + DBE);
                    }
                }
            };

        JUnitThread tester2 =
            new JUnitThread("testNonBlocking2") {
                public void testBody() {
                    try {
                        /* wait for tester1 */
                        while (lockManager.nOwners(nid) < 1) {
                            Thread.yield();
                        }
                        LockGrantType grant = lockManager.lock
                            (1, txn2, LockType.WRITE, 0, true, false, null);
                        assertSame(LockGrantType.DENIED, grant);
                        assertFalse
                            (lockManager.isOwner(nid, txn2, LockType.WRITE));
                        assertFalse
                            (lockManager.isOwner(nid, txn2, LockType.READ));
                        assertTrue(lockManager.nWaiters(nid) == 0);
                        assertTrue(lockManager.nOwners(nid) == 1);
                        sequence.incrementAndGet();
                        /* wait for tester1 to release the lock */
                        while (lockManager.nOwners(nid) > 0) {
                            Thread.yield();
                        }
                        assertTrue
                            (lockManager.lock(1, txn2, LockType.WRITE, 0,
                                              false, false, null) ==
                             LockGrantType.NEW);
                        assertTrue
                            (lockManager.isOwner(nid, txn2, LockType.WRITE));
                        assertTrue(lockManager.nWaiters(nid) == 0);
                        assertTrue(lockManager.nOwners(nid) == 1);
                        lockManager.release(1L, txn2);
                        txn2.removeLock(1L);
                    } catch (DatabaseException DBE) {
                        DBE.printStackTrace();
                        fail("caught DatabaseException " + DBE);
                    }
                }
            };

        tester1.start();
        tester2.start();
        tester1.finishTest();
        tester2.finishTest();
        closeEnv();
    }

    /**
     * Acquire a write lock, then request a read lock for a second
     * transaction in non-blocking mode.  Make sure it fails.
     */
    @Test
    public void testNonBlockingLock2()
        throws Throwable {

        initLockers();

        final LockManager lockManager = DbInternal.getNonNullEnvImpl(env).
            getTxnManager().getLockManager();

        JUnitThread tester1 =
            new JUnitThread("testNonBlocking1") {
                public void testBody() {
                    try {
                        lockManager.lock(1, txn1, LockType.WRITE, 0,
                                         false, false, null);
                        assertTrue
                            (lockManager.isOwner(nid, txn1, LockType.WRITE));
                        sequence.incrementAndGet();
                        while (sequence.get() < 2) {
                            Thread.yield();
                        }
                        lockManager.release(1L, txn1);
                        txn1.removeLock(1L);
                    } catch (DatabaseException DBE) {
                        DBE.printStackTrace();
                        fail("caught DatabaseException " + DBE);
                    }
                }
            };

        JUnitThread tester2 =
            new JUnitThread("testNonBlocking2") {
                public void testBody() {
                    try {
                        /* wait for tester1 */
                        while (sequence.get() < 1) {
                            Thread.yield();
                        }
                        LockGrantType grant = lockManager.lock
                            (1, txn2, LockType.READ, 0, true, false, null);
                        assertSame(LockGrantType.DENIED, grant);
                        assertFalse
                            (lockManager.isOwner(nid, txn2, LockType.READ));
                        assertFalse
                            (lockManager.isOwner(nid, txn2, LockType.WRITE));
                        assertTrue(lockManager.nWaiters(nid) == 0);
                        assertTrue(lockManager.nOwners(nid) == 1);
                        sequence.incrementAndGet();
                        /* wait for tester1 to release the lock */
                        while (lockManager.nOwners(nid) > 0) {
                            Thread.yield();
                        }
                        assertTrue
                            (lockManager.lock(1, txn2, LockType.READ, 0,
                                              false, false, null) ==
                             LockGrantType.NEW);
                        assertTrue
                            (lockManager.isOwner(nid, txn2, LockType.READ));
                        assertFalse
                            (lockManager.isOwner(nid, txn2, LockType.WRITE));
                        assertTrue(lockManager.nWaiters(nid) == 0);
                        assertTrue(lockManager.nOwners(nid) == 1);
                        lockManager.release(1L, txn2);
                        txn2.removeLock(1L);
                    } catch (DatabaseException DBE) {
                        DBE.printStackTrace();
                        fail("caught DatabaseException " + DBE);
                    }
                }
            };

        tester1.start();
        tester2.start();
        tester1.finishTest();
        tester2.finishTest();
        closeEnv();
    }

    /**
     * Acquire a write lock, then request a read lock for a second
     * transaction in blocking mode.  Make sure it waits.
     */
    @Test
    public void testWaitingLock()
        throws Throwable {

        initLockers();

        final LockManager lockManager = DbInternal.getNonNullEnvImpl(env).
            getTxnManager().getLockManager();

        JUnitThread tester1 =
            new JUnitThread("testBlocking1") {
                public void testBody() {
                    try {
                        lockManager.lock(1, txn1, LockType.WRITE, 0,
                                         false, false, null);
                        assertTrue
                            (lockManager.isOwner(nid, txn1, LockType.WRITE));
                        sequence.incrementAndGet();
                        while (sequence.get() < 2) {
                            Thread.yield();
                        }
                        lockManager.release(1L, txn1);
                        txn1.removeLock(1L);
                    } catch (DatabaseException DBE) {
                        DBE.printStackTrace();
                        fail("caught DatabaseException " + DBE);
                    }
                }
            };

        JUnitThread tester2 =
            new JUnitThread("testBlocking2") {
                public void testBody() {
                    try {
                        /* wait for tester1 */
                        while (sequence.get() < 1) {
                            Thread.yield();
                        }
                        try {
                            lockManager.lock(1, txn2, LockType.READ, 500,
                                             false, false, null);
                            fail("didn't time out");
                        } catch (LockConflictException e) {
                            assertTrue
                                (TestUtils.skipVersion(e).startsWith("Lock "));
                        }
                        assertFalse
                            (lockManager.isOwner(nid, txn2, LockType.READ));
                        assertFalse
                            (lockManager.isOwner(nid, txn2, LockType.WRITE));
                        assertTrue(lockManager.nWaiters(nid) == 0);
                        assertTrue(lockManager.nOwners(nid) == 1);
                        sequence.incrementAndGet();
                        /* wait for tester1 to release the lock */
                        while (lockManager.nOwners(nid) > 0) {
                            Thread.yield();
                        }
                        assertTrue
                            (lockManager.lock(1, txn2, LockType.READ, 0,
                                              false, false, null) ==
                             LockGrantType.NEW);
                        assertTrue
                            (lockManager.isOwner(nid, txn2, LockType.READ));
                        assertFalse
                            (lockManager.isOwner(nid, txn2, LockType.WRITE));
                        assertTrue(lockManager.nWaiters(nid) == 0);
                        assertTrue(lockManager.nOwners(nid) == 1);
                        lockManager.release(1L, txn2);
                        txn2.removeLock(1L);
                    } catch (DatabaseException DBE) {
                        DBE.printStackTrace();
                        fail("caught DatabaseException " + DBE);
                    }
                }
            };

        tester1.start();
        tester2.start();
        tester1.finishTest();
        tester2.finishTest();
        closeEnv();
    }

    /**
     * Test that LockConflictException has the correct owners and waiters when
     * it is thrown due to a timeout.
     *
     * Create five threads, the first two of which take a read lock and the
     * second two of which try for a write lock backed up behind the two
     * read locks.  Then have a fifth thread try for a read lock which backs
     * up behind all of them.  The first two threads (read lockers) are owners
     * and the second two threads are waiters.  When the fifth thread catches
     * the LockConflictException make sure that it contains the txn ids for the
     * two readers in the owners array and the txn ids for the two writers
     * in the waiters array.
     */
    @Test
    public void testLockConflictInfo()
        throws Throwable {

        EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);
        final LockManager lockManager = 
            envImpl.getTxnManager().getLockManager();
        TransactionConfig config = new TransactionConfig();

        initTxns(config, envImpl);

        final Txn txn5 = Txn.createLocalTxn(envImpl, config);

        sequence.set(0);
        JUnitThread tester1 =
            new JUnitThread("testMultipleReaders1") {
                public void testBody() {
                    try {
                        lockManager.lock(1, txn1, LockType.READ, 0,
                                         false, false, null);
                        assertTrue
                            (lockManager.isOwner(nid, txn1, LockType.READ));
                        while (sequence.get() < 1) {
                            Thread.yield();
                        }
                        lockManager.release(1L, txn1);
                        txn1.removeLock(1L);
                    } catch (DatabaseException DBE) {
                        DBE.printStackTrace();
                        fail("caught DatabaseException " + DBE);
                    }
                }
            };

        JUnitThread tester2 =
            new JUnitThread("testMultipleReaders2") {
                public void testBody() {
                    try {
                        lockManager.lock(1, txn2, LockType.READ, 0,
                                         false, false, null);
                        assertTrue
                            (lockManager.isOwner(nid, txn2, LockType.READ));
                        while (sequence.get() < 1) {
                            Thread.yield();
                        }
                        lockManager.release(1L, txn2);
                        txn2.removeLock(1L);
                    } catch (DatabaseException DBE) {
                        DBE.printStackTrace();
                        fail("caught DatabaseException " + DBE);
                    }
                }
            };

        JUnitThread tester3 =
            new JUnitThread("testMultipleReaders3") {
                public void testBody() {
                    try {
                        while (lockManager.nOwners(nid) < 2) {
                            Thread.yield();
                        }
                        lockManager.lock(1, txn3, LockType.WRITE, 0,
                                         false, false, null);
                        while (sequence.get() < 1) {
                            Thread.yield();
                        }
                        assertTrue
                            (lockManager.isOwner(nid, txn3, LockType.WRITE));
                        lockManager.release(1L, txn3);
                        txn3.removeLock(1L);
                    } catch (DatabaseException DBE) {
                        DBE.printStackTrace();
                        fail("caught DatabaseException " + DBE);
                    }
                }
            };

        JUnitThread tester4 =
            new JUnitThread("testMultipleReaders4") {
                public void testBody() {
                    try {
                        while (lockManager.nOwners(nid) < 2) {
                            Thread.yield();
                        }
                        lockManager.lock(1, txn4, LockType.WRITE, 0,
                                         false, false, null);
                        while (sequence.get() < 1) {
                            Thread.yield();
                        }
                        assertTrue
                            (lockManager.isOwner(nid, txn4, LockType.WRITE));
                        lockManager.release(1L, txn4);
                        txn4.removeLock(1L);
                    } catch (DatabaseException DBE) {
                        DBE.printStackTrace();
                        fail("caught DatabaseException " + DBE);
                    }
                }
            };

        JUnitThread tester5 =
            new JUnitThread("testMultipleReaders5") {
                public void testBody() {
                    try {
                        while (lockManager.nWaiters(nid) < 2) {
                            Thread.yield();
                        }
                        lockManager.lock(1, txn5, LockType.READ, 900,
                                         false, false, null);
                        fail("expected LockConflictException");
                    } catch (LockConflictException e) {

                        long[] owners = e.getOwnerTxnIds();
                        long[] waiters = e.getWaiterTxnIds();

                        assertTrue((owners[0] == txn1.getId() &&
                                    owners[1] == txn2.getId()) ||
                                   (owners[1] == txn1.getId() &&
                                    owners[0] == txn2.getId()));

                        assertTrue((waiters[0] == txn3.getId() &&
                                    waiters[1] == txn4.getId()) ||
                                   (waiters[1] == txn3.getId() &&
                                    waiters[0] == txn4.getId()));

                    } catch (DatabaseException DBE) {
                        fail("expected LockConflictException");
                        DBE.printStackTrace(System.out);
                    }
                    sequence.set(1);
                }
            };

        tester1.start();
        tester2.start();
        tester3.start();
        tester4.start();
        tester5.start();
        tester1.finishTest();
        tester2.finishTest();
        tester3.finishTest();
        tester4.finishTest();
        tester5.finishTest();
        ((Txn) txn1).abort(false);
        ((Txn) txn2).abort(false);
        ((Txn) txn3).abort(false);
        ((Txn) txn4).abort(false);
        txn5.abort(false);
        closeEnv();
    }

    /**
     * Test lock stealing.
     *
     * Create five threads, with the first two taking a read lock and the
     * second two trying for a write lock backed up behind the two read locks.
     * Then have a fifth importunate thread try for a write lock which will
     * flush the first two owners.  The first two threads (read lockers) are
     * owners and the second two threads are waiters.  When the importunate
     * thread steals the lock, make sure that the other two owners become
     * onlyAbortable.
     */
    @Test
    public void testImportunateTxn1()
        throws Throwable {

        EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);

        /* LockPreemptedException is thrown only if replicated. */
        if (!envImpl.isReplicated()) {
            close(env);
            return;
        }

        /*
         * Use an arbitrary DatabaseImpl so that the Txn.lock method can be
         * called below with a non-null database param.  Although this is a
         * LockManager test, lock preemption requires calling Txn.lock.
         */
        final DatabaseImpl dbImpl = 
            envImpl.getDbTree().getDb(DbTree.NAME_DB_ID);

        final LockManager lockManager = 
            envImpl.getTxnManager().getLockManager();
        TransactionConfig config = new TransactionConfig();

        initTxns(config, envImpl);
        
        final Txn txn5 = Txn.createLocalTxn(envImpl, config);
        txn5.setImportunate(true);

        sequence.set(0);
        JUnitThread tester1 =
            new JUnitThread("testImportunateTxn1.1") {
                public void testBody() {
                    try {
                        txn1.setLockTimeout(0);
                        txn1.lock(1, LockType.READ, false, dbImpl);
                        assertTrue
                            (lockManager.isOwner(nid, txn1, LockType.READ));
                        while (sequence.get() < 1) {
                            Thread.yield();
                        }
                        assertTrue(txn1.isPreempted());
                        sequence.incrementAndGet();
                    } catch (DatabaseException DBE) {
                        DBE.printStackTrace();
                        fail("caught DatabaseException " + DBE);
                    }
                }
            };

        JUnitThread tester2 =
            new JUnitThread("testImportunateTxn1.2") {
                public void testBody() {
                    try {
                        txn2.setLockTimeout(0);
                        txn2.lock(1, LockType.READ, false, dbImpl);
                        assertTrue
                            (lockManager.isOwner(nid, txn2, LockType.READ));
                        while (sequence.get() < 1) {
                            Thread.yield();
                        }
                        assertTrue(txn1.isPreempted());
                        sequence.incrementAndGet();
                    } catch (DatabaseException DBE) {
                        DBE.printStackTrace();
                        fail("caught DatabaseException " + DBE);
                    }
                }
            };

        JUnitThread tester3 =
            new JUnitThread("testImportunateTxn1.3") {
                public void testBody() {
                    try {
                        while (lockManager.nOwners(nid) < 2) {
                            Thread.yield();
                        }
                        txn3.setLockTimeout(0);
                        txn3.lock(1, LockType.WRITE, false, dbImpl);
                        while (sequence.get() < 1) {
                            Thread.yield();
                        }
                        assertTrue
                            (lockManager.isOwner(nid, txn3, LockType.WRITE));
                        lockManager.release(1L, txn3);
                        txn3.removeLock(1L);
                    } catch (DatabaseException DBE) {
                        DBE.printStackTrace();
                        fail("caught DatabaseException " + DBE);
                    }
                }
            };

        JUnitThread tester4 =
            new JUnitThread("testImportunateTxn1.4") {
                public void testBody() {
                    try {
                        while (lockManager.nOwners(nid) < 2) {
                            Thread.yield();
                        }
                        txn4.setLockTimeout(0);
                        txn4.lock(1, LockType.WRITE, false, dbImpl);
                        while (sequence.get() < 1) {
                            Thread.yield();
                        }
                        assertTrue
                            (lockManager.isOwner(nid, txn4, LockType.WRITE));
                        lockManager.release(1L, txn4);
                        txn4.removeLock(1L);
                    } catch (DatabaseException DBE) {
                        DBE.printStackTrace();
                        fail("caught DatabaseException " + DBE);
                    }
                }
            };

        JUnitThread tester5 =
            new JUnitThread("testImportunateTxn1.5") {
                public void testBody() {
                    try {
                        while (lockManager.nWaiters(nid) < 1) {
                            Thread.yield();
                        }
                        txn5.setImportunate(true);
                        txn5.setLockTimeout(900);
                        txn5.lock(1, LockType.WRITE, false, dbImpl);
                        sequence.set(1);
                        while (sequence.get() < 3) {
                            Thread.yield();
                        }
                        lockManager.release(1L, txn5);
                        txn5.removeLock(1L);
                    } catch (DatabaseException DBE) {
                        DBE.printStackTrace(System.out);
                        fail("unexpected DatabaseException");
                    }
                }
            };

        tester1.start();
        tester2.start();
        tester3.start();
        tester4.start();
        tester5.start();
        tester1.finishTest();
        tester2.finishTest();
        tester3.finishTest();
        tester4.finishTest();
        tester5.finishTest();
        ((Txn) txn1).abort(false);
        ((Txn) txn2).abort(false);
        ((Txn) txn3).abort(false);
        ((Txn) txn4).abort(false);
        txn5.abort(false);
        closeEnv();
    }

    /**
     * Test lock stealing.
     *
     * Create five threads, with the first two taking a read lock and the
     * second two trying for a write lock backed up behind the two read locks.
     * Then have a fifth importunate thread take a read lock and try for a
     * write lock (upgrade) which will flush the first two read lock owners.
     * The first two threads (read lockers) are owners and the second two
     * threads are waiters.  When the importunate thread steals the lock, make
     * sure that the other two owners become onlyAbortable.
     */
    @Test
    public void testImportunateTxn2()
        throws Throwable {

        EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);

        /* LockPreemptedException is thrown only if replicated. */
        if (!envImpl.isReplicated()) {
            close(env);
            return;
        }

        /*
         * Use an arbitrary DatabaseImpl so that the Txn.lock method can be
         * called below with a non-null database param.  Although this is a
         * LockManager test, lock preemption requires calling Txn.lock.
         */
        final DatabaseImpl dbImpl = 
            envImpl.getDbTree().getDb(DbTree.NAME_DB_ID);

        final LockManager lockManager = 
            envImpl.getTxnManager().getLockManager();
        TransactionConfig config = new TransactionConfig();

        initTxns(config, envImpl);

        final Txn txn5 = Txn.createLocalTxn(envImpl, config);
        txn5.setImportunate(true);

        sequence.set(0);
        JUnitThread tester1 =
            new JUnitThread("testImportunateTxn1.1") {
                public void testBody() {
                    try {
                        txn1.setLockTimeout(0);
                        txn1.lock(1, LockType.READ, false, dbImpl);
                        assertTrue
                            (lockManager.isOwner(nid, txn1, LockType.READ));
                        while (sequence.get() < 1) {
                            Thread.yield();
                        }
                        assertTrue(txn1.isPreempted());
                        sequence.incrementAndGet();
                    } catch (DatabaseException DBE) {
                        DBE.printStackTrace();
                        fail("caught DatabaseException " + DBE);
                    }
                }
            };

        JUnitThread tester2 =
            new JUnitThread("testImportunateTxn1.2") {
                public void testBody() {
                    try {
                        txn2.setLockTimeout(0);
                        txn2.lock(1, LockType.READ, false, dbImpl);
                        assertTrue
                            (lockManager.isOwner(nid, txn2, LockType.READ));
                        while (sequence.get() < 1) {
                            Thread.yield();
                        }
                        assertTrue(txn2.isPreempted());
                        sequence.incrementAndGet();
                    } catch (DatabaseException DBE) {
                        DBE.printStackTrace();
                        fail("caught DatabaseException " + DBE);
                    }
                }
            };

        JUnitThread tester3 =
            new JUnitThread("testImportunateTxn1.3") {
                public void testBody() {
                    try {
                        while (lockManager.nOwners(nid) < 3) {
                            Thread.yield();
                        }
                        txn3.setLockTimeout(0);
                        txn3.lock(1, LockType.WRITE, false, dbImpl);
                        while (sequence.get() < 1) {
                            Thread.yield();
                        }
                        assertTrue
                            (lockManager.isOwner(nid, txn3, LockType.WRITE));
                        lockManager.release(1L, txn3);
                        txn3.removeLock(1L);
                    } catch (DatabaseException DBE) {
                        DBE.printStackTrace();
                        fail("caught DatabaseException " + DBE);
                    }
                }
            };

        JUnitThread tester4 =
            new JUnitThread("testImportunateTxn1.4") {
                public void testBody() {
                    try {
                        while (lockManager.nOwners(nid) < 3) {
                            Thread.yield();
                        }
                        txn4.setLockTimeout(0);
                        txn4.lock(1, LockType.WRITE, false, dbImpl);
                        while (sequence.get() < 1) {
                            Thread.yield();
                        }
                        assertTrue
                            (lockManager.isOwner(nid, txn4, LockType.WRITE));
                        lockManager.release(1L, txn4);
                        txn4.removeLock(1L);
                    } catch (DatabaseException DBE) {
                        DBE.printStackTrace();
                        fail("caught DatabaseException " + DBE);
                    }
                }
            };

        JUnitThread tester5 =
            new JUnitThread("testImportunateTxn1.5") {
                public void testBody() {
                    try {
                        txn5.setLockTimeout(0);
                        txn5.lock(1, LockType.READ, false, dbImpl);
                        while (lockManager.nWaiters(nid) < 1) {
                            Thread.yield();
                        }
                        txn5.setImportunate(true);
                        txn5.setLockTimeout(900);
                        txn5.lock(1, LockType.WRITE, false, dbImpl);
                        sequence.set(1);
                        while (sequence.get() < 3) {
                            Thread.yield();
                        }
                        lockManager.release(1L, txn5);
                        txn5.removeLock(1L);
                    } catch (DatabaseException DBE) {
                        DBE.printStackTrace(System.out);
                        fail("unexpected DatabaseException");
                    }
                }
            };

        tester1.start();
        tester2.start();
        tester3.start();
        tester4.start();
        tester5.start();
        tester1.finishTest();
        tester2.finishTest();
        tester3.finishTest();
        tester4.finishTest();
        tester5.finishTest();
        ((Txn) txn1).abort(false);
        ((Txn) txn2).abort(false);
        ((Txn) txn3).abort(false);
        ((Txn) txn4).abort(false);
        txn5.abort(false);
        closeEnv();
    }
}
