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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.dbi.MemoryBudget;
import com.sleepycat.je.util.DualTestCase;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.util.test.SharedTestUtils;

public class LockTest extends DualTestCase {
    private Environment env;
    private final File envHome;

    public LockTest() {
        envHome =  SharedTestUtils.getTestDir();
    }

    @Before
    public void setUp()
        throws Exception {

        super.setUp();
        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setConfigParam(EnvironmentParams.NODE_MAX.getName(), "6");
        envConfig.setAllowCreate(true);
        envConfig.setTransactional(true);
        env = create(envHome, envConfig);
    }

    @Test
    public void testLockConflicts()
        throws Exception {

        EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);

        Locker txn1 = BasicLocker.createBasicLocker(envImpl);
        Locker txn2 = BasicLocker.createBasicLocker(envImpl);
        Locker txn3 = BasicLocker.createBasicLocker(envImpl);

        MemoryBudget mb = envImpl.getMemoryBudget();
        try {

            /*
             * Start fresh. Ask for a read lock from txn1 twice,
             * should only be one owner. Then add multiple
             * would-be-writers as waiters.
             */
            Lock lock = new LockImpl();
            assertEquals(LockGrantType.NEW,
                         lock.lock(LockType.READ, txn1, false, false, mb, 0).
                         lockGrant);
            assertEquals(LockGrantType.EXISTING,
                         lock.lock(LockType.READ, txn1, false, false, mb, 0).
                         lockGrant);

            /* txn1 has a READ lock. */
            assertEquals(1, lock.nOwners());
            assertEquals(0, lock.nWaiters());

            /* txn2 asks for a read lock, gets it. */
            assertEquals(LockGrantType.NEW,
                         lock.lock(LockType.READ, txn2, false, false, mb, 0).
                         lockGrant);

            /* txn1 asks for WRITE, must wait */
            assertEquals(LockGrantType.WAIT_PROMOTION,
                         lock.lock(LockType.WRITE, txn1, false, false, mb, 0).
                         lockGrant);

            /* txn2 write request must wait */
            assertEquals(LockGrantType.WAIT_PROMOTION,
                         lock.lock(LockType.WRITE, txn2, false, false, mb, 0).
                         lockGrant);

            /* Two read locks, two write waiters */
            assertEquals(2, lock.nOwners());
            assertEquals(2, lock.nWaiters());

            /*
             * Release txn1 read lock, which causes txn2's read lock to be
             * promoted to a write lock.
             */
            lock.release(txn1, mb, 0 /* lockTableIndex */);
            assertEquals(1, lock.nOwners());
            assertEquals(1, lock.nWaiters());

            /* Release txn2 write lock, now txn1 will get its write lock. */
            lock.release(txn2, mb, 0 /* lockTableIndex */);
            assertEquals(1, lock.nOwners());
            assertEquals(0, lock.nWaiters());

            /* Release txn1's write lock. */
            lock.release(txn1, mb, 0 /* lockTableIndex */);
            assertEquals(0, lock.nOwners());
            assertEquals(0, lock.nWaiters());

            /* Start fresh. Get a write lock, then get a read lock. */
            lock = new LockImpl();

            assertEquals(LockGrantType.NEW,
                         lock.lock(LockType.WRITE, txn1, false, false, mb, 0).
                         lockGrant);
            assertEquals(LockGrantType.EXISTING,
                         lock.lock(LockType.READ, txn1, false, false, mb, 0).
                         lockGrant);
            assertEquals(1, lock.nOwners());
            assertEquals(0, lock.nWaiters());
            lock.release(txn1, mb, 0 /* lockTableIndex */);

            /* Start fresh. Get a read lock, upgrade to a write lock. */
            lock = new LockImpl();
            assertEquals(LockGrantType.NEW,
                         lock.lock(LockType.READ, txn1, false, false, mb, 0).
                         lockGrant);
            assertEquals(LockGrantType.PROMOTION,
                         lock.lock(LockType.WRITE, txn1, false, false, mb, 0).
                         lockGrant);
            assertEquals(1, lock.nOwners());
            assertEquals(0, lock.nWaiters());
            lock.release(txn1, mb, 0 /* lockTableIndex */);

            /*
             * Start fresh. Get a read lock, then ask for a non-blocking
             * write lock. The latter should be denied.
             */
            lock = new LockImpl();

            assertEquals(LockGrantType.NEW,
                         lock.lock(LockType.READ, txn1, false, false, mb, 0).
                         lockGrant);
            assertEquals(LockGrantType.DENIED,
                         lock.lock(LockType.WRITE, txn2, true, false, mb, 0).
                         lockGrant);
            assertEquals(1, lock.nOwners());
            assertEquals(0, lock.nWaiters());
            lock.release(txn1, mb, 0 /* lockTableIndex */);

            /* Two write requests, should be one owner. */
            lock = new LockImpl();
            assertEquals(LockGrantType.NEW,
                         lock.lock(LockType.WRITE, txn1, false, false, mb, 0).
                         lockGrant);
            assertEquals(LockGrantType.EXISTING,
                         lock.lock(LockType.WRITE, txn1, false, false, mb, 0).
                         lockGrant);
            assertEquals(1, lock.nOwners());
            assertEquals(0, lock.nWaiters());
            lock.release(txn1, mb, 0 /* lockTableIndex */);

            /*
             * Ensure that a read request behind a write request that waits
             * also waits.  blocking requests.
             */
            lock = new LockImpl();

            assertEquals(LockGrantType.NEW,
                         lock.lock(LockType.READ, txn1, false, false, mb, 0).
                         lockGrant);

            assertEquals(LockGrantType.WAIT_NEW,
                         lock.lock(LockType.WRITE, txn2, false, false, mb, 0).
                         lockGrant);

            assertEquals(LockGrantType.WAIT_NEW,
                         lock.lock(LockType.READ, txn3, false, false, mb, 0).
                         lockGrant);

            assertEquals(1, lock.nOwners());
            assertEquals(2, lock.nWaiters());
            lock.release(txn1, mb, 0 /* lockTableIndex */);
            lock.release(txn2, mb, 0 /* lockTableIndex */);
            lock.release(txn3, mb, 0 /* lockTableIndex */);

            /* Check non blocking requests */
            lock = new LockImpl();

            assertEquals(LockGrantType.NEW,
                         lock.lock(LockType.READ, txn1, false, false, mb, 0).
                         lockGrant);

            /* Since non-blocking request, this fails and doesn't go
               on the wait queue. */
            assertEquals(LockGrantType.DENIED,
                         lock.lock(LockType.WRITE, txn2, true, false, mb, 0).
                         lockGrant);
            assertEquals(LockGrantType.NEW,
                         lock.lock(LockType.READ, txn3, true, false, mb, 0).
                         lockGrant);
            assertEquals(2, lock.nOwners());
            assertEquals(0, lock.nWaiters());
            lock.release(txn1, mb, 0 /* lockTableIndex */);
            lock.release(txn3, mb, 0 /* lockTableIndex */);

            lock = new LockImpl();

            assertEquals(LockGrantType.NEW,
                         lock.lock(LockType.READ, txn1, false, false, mb, 0).
                         lockGrant);
            assertEquals(LockGrantType.NEW,
                         lock.lock(LockType.READ, txn2, false, false, mb, 0).
                         lockGrant);
            assertEquals(LockGrantType.NEW,
                         lock.lock(LockType.READ, txn3, false, false, mb, 0).
                         lockGrant);
            assertEquals(3, lock.nOwners());
            assertEquals(0, lock.nWaiters());
            lock.release(txn1, mb, 0 /* lockTableIndex */);
            lock.release(txn2, mb, 0 /* lockTableIndex */);
            lock.release(txn3, mb, 0 /* lockTableIndex */);
        } finally {
            txn1.operationEnd();
            txn2.operationEnd();
            txn3.operationEnd();
            close(env);
        }
    }

    @Test
    public void testOwners()
        throws Exception {

        EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);

        Locker txn1 = BasicLocker.createBasicLocker(envImpl);
        Locker txn2 = BasicLocker.createBasicLocker(envImpl);
        Locker txn3 = BasicLocker.createBasicLocker(envImpl);
        Locker txn4 = BasicLocker.createBasicLocker(envImpl);
        MemoryBudget mb = envImpl.getMemoryBudget();

        try {
            /*
             * Build up 3 owners and waiters for a lock, to test the
             * lazy initialization and optimization for single owner/waiter.
             */
            Lock lock = new LockImpl();
            /* should be no writer. */
            assertTrue(lock.getWriteOwnerLocker() == null);

            assertEquals(LockGrantType.NEW,
                         lock.lock(LockType.READ, txn1, false, false, mb, 0).
                         lockGrant);
            assertEquals(LockGrantType.NEW,
                         lock.lock(LockType.READ, txn2, false, false, mb, 0).
                         lockGrant);
            assertEquals(LockGrantType.NEW,
                         lock.lock(LockType.READ, txn3, false, false, mb, 0).
                         lockGrant);

            /* should be no writer. */
            assertTrue(lock.getWriteOwnerLocker() == null);

            /* expect 3 owners, 0 waiters. */
            Set<LockInfo> expectedOwners = new HashSet<LockInfo>();
            expectedOwners.add(new LockInfo(txn1, LockType.READ));
            expectedOwners.add(new LockInfo(txn2, LockType.READ));
            expectedOwners.add(new LockInfo(txn3, LockType.READ));
            checkOwners(expectedOwners, lock, 0);

            /* release the first locker. */
            lock.release(txn1, mb, 0);
            expectedOwners = new HashSet<LockInfo>();
            expectedOwners.add(new LockInfo(txn2, LockType.READ));
            expectedOwners.add(new LockInfo(txn3, LockType.READ));
            checkOwners(expectedOwners, lock, 0);

            /* Add more. */
            assertEquals(LockGrantType.NEW,
                         lock.lock(LockType.READ, txn4, false, false, mb, 0).
                         lockGrant);
            expectedOwners = new HashSet<LockInfo>();
            expectedOwners.add(new LockInfo(txn2, LockType.READ));
            expectedOwners.add(new LockInfo(txn3, LockType.READ));
            expectedOwners.add(new LockInfo(txn4, LockType.READ));
            checkOwners(expectedOwners, lock, 0);

            /* release */
            lock.release(txn2, mb, 0);
            expectedOwners = new HashSet<LockInfo>();
            expectedOwners.add(new LockInfo(txn3, LockType.READ));
            expectedOwners.add(new LockInfo(txn4, LockType.READ));
            checkOwners(expectedOwners, lock, 0);

            /* release */
            lock.release(txn3, mb, 0);
            expectedOwners = new HashSet<LockInfo>();
            expectedOwners.add(new LockInfo(txn4, LockType.READ));
            /* only 1 lock, in the owner set, but not a write owner. */
            assertTrue(lock.getWriteOwnerLocker() == null);

            /* release */
            lock.release(txn4, mb, 0);
            expectedOwners = new HashSet<LockInfo>();
            checkOwners(expectedOwners, lock, 0);

            /* Add owners again. */
            assertEquals(LockGrantType.NEW,
                         lock.lock(LockType.READ, txn1, false, false, mb, 0).
                         lockGrant);
            assertEquals(LockGrantType.NEW,
                         lock.lock(LockType.READ, txn2, false, false, mb, 0).
                         lockGrant);
            expectedOwners = new HashSet<LockInfo>();
            expectedOwners.add(new LockInfo(txn1, LockType.READ));
            expectedOwners.add(new LockInfo(txn2, LockType.READ));
            checkOwners(expectedOwners, lock, 0);

            /* Release for the sake of the memory leak checking */
            lock.release(txn1, mb, 0);
            lock.release(txn2, mb, 0);

        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            txn1.operationEnd();
            txn2.operationEnd();
            txn3.operationEnd();
            txn4.operationEnd();
            close(env);
        }
    }

    @Test
    public void testWaiters()
        throws Exception {

        EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);

        Locker txn1 = Txn.createLocalAutoTxn(envImpl, new TransactionConfig());
        Locker txn2 = Txn.createLocalAutoTxn(envImpl, new TransactionConfig());
        Locker txn3 = Txn.createLocalAutoTxn(envImpl, new TransactionConfig());
        Locker txn4 = Txn.createLocalAutoTxn(envImpl, new TransactionConfig());
        Locker txn5 = Txn.createLocalAutoTxn(envImpl, new TransactionConfig());
        MemoryBudget mb = envImpl.getMemoryBudget();

        try {
            /*
             * Build up 1 owners and 3waiters for a lock, to test the
             * lazy initialization and optimization for single owner/waiter.
             */
            Lock lock = new LockImpl();
            assertEquals(LockGrantType.NEW,
                         lock.lock(LockType.READ, txn1, false, false, mb, 0).
                         lockGrant);
            assertEquals(LockGrantType.NEW,
                         lock.lock(LockType.READ, txn2, false, false, mb, 0).
                         lockGrant);
            assertEquals(LockGrantType.WAIT_NEW,
                         lock.lock(LockType.WRITE, txn3, false, false, mb, 0).
                         lockGrant);
            assertEquals(LockGrantType.WAIT_NEW,
                         lock.lock(LockType.WRITE, txn4, false, false, mb, 0).
                         lockGrant);
            assertEquals(LockGrantType.WAIT_PROMOTION,
                         lock.lock(LockType.WRITE, txn1, false, false, mb, 0).
                         lockGrant);

            /* should be no writer. */
            assertTrue(lock.getWriteOwnerLocker() == null);

            /* expect 2 owners, 3 waiters. */
            Set<LockInfo> expectedOwners = new HashSet<LockInfo>();
            expectedOwners.add(new LockInfo(txn1, LockType.READ));
            expectedOwners.add(new LockInfo(txn2, LockType.READ));
            checkOwners(expectedOwners, lock, 3);

            List<LockInfo> waiters = new ArrayList<LockInfo>();
            waiters.add(new LockInfo(txn1, LockType.WRITE));
            waiters.add(new LockInfo(txn3, LockType.WRITE));
            waiters.add(new LockInfo(txn4, LockType.WRITE));
            checkWaiters(waiters, lock);

            /* release a waiter, shouldn't change anything. */
            lock.release(txn4, mb, 0);
            checkWaiters(waiters, lock);

            /*
             * Release the other read lock, expect txn1 to be promoted to a
             * write lock.
             */
            lock.release(txn2, mb, 0);
            expectedOwners = new HashSet<LockInfo>();
            expectedOwners.add(new LockInfo(txn1, LockType.WRITE));
            checkOwners(expectedOwners, lock, 2);

            waiters.remove(0);
            checkWaiters(waiters, lock);

            /* release */
            lock.release(txn1, mb, 0);
            expectedOwners = new HashSet<LockInfo>();
            expectedOwners.add(new LockInfo(txn3, LockType.WRITE));
            checkOwners(expectedOwners, lock, 1);

            waiters.remove(0);
            checkWaiters(waiters, lock);

            /*
             * Add multiple read lock waiters so that we can promoting multiple
             * waiters.
             */
            assertEquals(LockGrantType.WAIT_NEW,
                         lock.lock(LockType.READ, txn1, false, false, mb, 0).
                         lockGrant);
            assertEquals(LockGrantType.WAIT_NEW,
                         lock.lock(LockType.READ, txn2, false, false, mb, 0).
                         lockGrant);
            assertEquals(LockGrantType.WAIT_NEW,
                         lock.lock(LockType.READ, txn5, false, false, mb, 0).
                         lockGrant);

            checkOwners(expectedOwners, lock, 4);
            waiters.add(new LockInfo(txn1, LockType.READ));
            waiters.add(new LockInfo(txn2, LockType.READ));
            waiters.add(new LockInfo(txn5, LockType.READ));
            checkWaiters(waiters, lock);

            /* flush one of the waiters. */
            lock.flushWaiter(txn5, mb, 0);
            waiters.remove(3);
            checkWaiters(waiters, lock);

            /* re-add. */
            assertEquals(LockGrantType.WAIT_NEW,
                         lock.lock(LockType.READ, txn5, false, false, mb, 0).
                         lockGrant);
            waiters.add(new LockInfo(txn5, LockType.READ));

            /* release txn3 */
            lock.release(txn3, mb, 0);
            expectedOwners = new HashSet<LockInfo>();
            expectedOwners.add(new LockInfo(txn4, LockType.WRITE));
            checkOwners(expectedOwners, lock, 3);
            waiters.remove(0);
            checkWaiters(waiters, lock);

            /* release txn4, expect all read locks to promote. */
            lock.release(txn4, mb, 0);
            expectedOwners = new HashSet<LockInfo>();
            expectedOwners.add(new LockInfo(txn1, LockType.READ));
            expectedOwners.add(new LockInfo(txn2, LockType.READ));
            expectedOwners.add(new LockInfo(txn5, LockType.READ));
            checkOwners(expectedOwners, lock, 0);
            waiters.clear();
            checkWaiters(waiters, lock);

            /* Release for the sake of the memory leak checking */
            lock.release(txn1, mb, 0);
            lock.release(txn2, mb, 0);
            lock.release(txn5, mb, 0);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            txn1.operationEnd();
            txn2.operationEnd();
            txn3.operationEnd();
            txn4.operationEnd();
            txn5.operationEnd();
            close(env);
        }
    }

    @Test
    public void testPromotion()
        throws Exception {

        EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);

        Locker txn1 = Txn.createLocalAutoTxn(envImpl, new TransactionConfig());
        Locker txn2 = Txn.createLocalAutoTxn(envImpl, new TransactionConfig());
        Locker txn3 = Txn.createLocalAutoTxn(envImpl, new TransactionConfig());
        Locker txn4 = Txn.createLocalAutoTxn(envImpl, new TransactionConfig());
        Locker txn5 = Txn.createLocalAutoTxn(envImpl, new TransactionConfig());
        MemoryBudget mb = envImpl.getMemoryBudget();

        try {
            /*
             * Build up 1 owners and 3 read waiters for a lock. Then
             * check that all the waiters promote properly.
             */
            Lock lock = new LockImpl();
            assertEquals(LockGrantType.NEW,
                         lock.lock(LockType.WRITE, txn1, false, false, mb, 0).
                         lockGrant);
            assertEquals(LockGrantType.WAIT_NEW,
                         lock.lock(LockType.READ, txn2, false, false, mb, 0).
                         lockGrant);
            assertEquals(LockGrantType.WAIT_NEW,
                         lock.lock(LockType.READ, txn3, false, false, mb, 0).
                         lockGrant);
            assertEquals(LockGrantType.WAIT_NEW,
                         lock.lock(LockType.READ, txn4, false, false, mb, 0).
                         lockGrant);

            /* Check that 1 owner, 3 waiters exist. */
            Set<LockInfo> expectedOwners = new HashSet<LockInfo>();
            expectedOwners.add(new LockInfo(txn1, LockType.WRITE));
            checkOwners(expectedOwners, lock, 3);

            List<LockInfo> waiters = new ArrayList<LockInfo>();
            waiters.add(new LockInfo(txn2, LockType.READ));
            waiters.add(new LockInfo(txn3, LockType.READ));
            waiters.add(new LockInfo(txn4, LockType.READ));
            checkWaiters(waiters, lock);

            /* Release the writer, expect all 3 waiters to promote. */
            lock.release(txn1, mb, 0);
            expectedOwners = new HashSet<LockInfo>();
            expectedOwners.add(new LockInfo(txn2, LockType.READ));
            expectedOwners.add(new LockInfo(txn3, LockType.READ));
            expectedOwners.add(new LockInfo(txn4, LockType.READ));
            checkOwners(expectedOwners, lock, 0);
            waiters.clear();
            checkWaiters(waiters, lock);

            /* Release for the sake of the memory leak checking */
            lock.release(txn2, mb, 0);
            lock.release(txn3, mb, 0);
            lock.release(txn4, mb, 0);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            txn1.operationEnd();
            txn2.operationEnd();
            txn3.operationEnd();
            txn4.operationEnd();
            txn5.operationEnd();
            close(env);
        }
    }

    /**
     * Tests conflicts between range locks and all other lock types.
     */
    @Test
    public void testRangeConflicts()
        throws Exception {

        /* No owner */
        checkConflict(null,
                      LockType.RANGE_READ,
                      LockGrantType.NEW);
        checkConflict(null,
                      LockType.RANGE_WRITE,
                      LockGrantType.NEW);
        checkConflict(null,
                      LockType.RANGE_INSERT,
                      LockGrantType.NEW);

        /* Owner has READ */
        checkConflict(LockType.READ,
                      LockType.RANGE_READ,
                      LockGrantType.NEW);
        checkConflict(LockType.READ,
                      LockType.RANGE_WRITE,
                      LockGrantType.WAIT_NEW);
        checkConflict(LockType.READ,
                      LockType.RANGE_INSERT,
                      LockGrantType.NEW);

        /* Owner has WRITE */
        checkConflict(LockType.WRITE,
                      LockType.RANGE_READ,
                      LockGrantType.WAIT_NEW);
        checkConflict(LockType.WRITE,
                      LockType.RANGE_WRITE,
                      LockGrantType.WAIT_NEW);
        checkConflict(LockType.WRITE,
                      LockType.RANGE_INSERT,
                      LockGrantType.NEW);

        /* Owner has RANGE_READ */
        checkConflict(LockType.RANGE_READ,
                      LockType.READ,
                      LockGrantType.NEW);
        checkConflict(LockType.RANGE_READ,
                      LockType.WRITE,
                      LockGrantType.WAIT_NEW);
        checkConflict(LockType.RANGE_READ,
                      LockType.RANGE_READ,
                      LockGrantType.NEW);
        checkConflict(LockType.RANGE_READ,
                      LockType.RANGE_WRITE,
                      LockGrantType.WAIT_NEW);
        checkConflict(LockType.RANGE_READ,
                      LockType.RANGE_INSERT,
                      LockGrantType.WAIT_NEW);

        /* Owner has RANGE_WRITE */
        checkConflict(LockType.RANGE_WRITE,
                      LockType.READ,
                      LockGrantType.WAIT_NEW);
        checkConflict(LockType.RANGE_WRITE,
                      LockType.WRITE,
                      LockGrantType.WAIT_NEW);
        checkConflict(LockType.RANGE_WRITE,
                      LockType.RANGE_READ,
                      LockGrantType.WAIT_NEW);
        checkConflict(LockType.RANGE_WRITE,
                      LockType.RANGE_WRITE,
                      LockGrantType.WAIT_NEW);
        checkConflict(LockType.RANGE_WRITE,
                      LockType.RANGE_INSERT,
                      LockGrantType.WAIT_NEW);

        /* Owner has RANGE_INSERT */
        checkConflict(LockType.RANGE_INSERT,
                      LockType.READ,
                      LockGrantType.NEW);
        checkConflict(LockType.RANGE_INSERT,
                      LockType.WRITE,
                      LockGrantType.NEW);
        checkConflict(LockType.RANGE_INSERT,
                      LockType.RANGE_READ,
                      LockGrantType.WAIT_RESTART);
        checkConflict(LockType.RANGE_INSERT,
                      LockType.RANGE_WRITE,
                      LockGrantType.WAIT_RESTART);
        checkConflict(LockType.RANGE_INSERT,
                      LockType.RANGE_INSERT,
                      LockGrantType.NEW);
        close(env);
    }

    /**
     * Tests that when the first request is held and the second request is
     * requested, the second grant type is returned.
     */
    private void checkConflict(LockType firstRequest,
                               LockType secondRequest,
                               LockGrantType secondGrantType)
        throws Exception {

        EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);

        Locker txn1 = Txn.createLocalAutoTxn(envImpl, new TransactionConfig());
        Locker txn2 = Txn.createLocalAutoTxn(envImpl, new TransactionConfig());
        MemoryBudget mb = envImpl.getMemoryBudget();

        try {
            Lock lock = new LockImpl();

            if (firstRequest != null) {
                assertEquals(LockGrantType.NEW,
                             lock.lock(firstRequest, txn1, false, false, mb, 0).
                             lockGrant);
            }
            LockGrantType typeGranted =
                lock.lock(secondRequest, txn2, false, false, mb, 0).
                lockGrant;
            assertEquals(secondGrantType, typeGranted);

            boolean wait = (typeGranted == LockGrantType.WAIT_NEW ||
                            typeGranted == LockGrantType.WAIT_PROMOTION ||
                            typeGranted == LockGrantType.WAIT_RESTART);
            boolean given = (typeGranted == LockGrantType.NEW);
            boolean restart = (typeGranted == LockGrantType.WAIT_RESTART);

            Set<LockInfo> expectedOwners = new HashSet<LockInfo>();
            List<LockInfo> expectedWaiters = new ArrayList<LockInfo>();

            if (firstRequest != null) {
                expectedOwners.add(new LockInfo(txn1, firstRequest));
            }
            if (given) {
                expectedOwners.add(new LockInfo(txn2, secondRequest));
            } else if (wait) {
                if (restart) {
                    expectedWaiters.add(new LockInfo(txn2, LockType.RESTART));
                } else {
                    expectedWaiters.add(new LockInfo(txn2, secondRequest));
                }
            }

            checkOwners(expectedOwners, lock, expectedWaiters.size());
            checkWaiters(expectedWaiters, lock);

            lock.release(txn1, mb, 0);
            if (wait) {
                if (restart) {
                    checkOwners(new HashSet<LockInfo>(), lock, 0);
                } else {
                    checkOwners(new HashSet<LockInfo>(expectedWaiters), lock, 0);
                }
            }
            lock.release(txn2, mb, 0);
            assertEquals(0, lock.nOwners());
            assertEquals(0, lock.nWaiters());
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            txn1.operationEnd();
            txn2.operationEnd();
        }
    }

    /**
     * Tests upgrades between range locks and all other lock types.
     */
    @Test
    public void testRangeUpgrades()
        throws Exception {

        /* Owner has READ */
        checkUpgrade(LockType.READ,
                     LockType.RANGE_READ,
                     LockGrantType.EXISTING,
                     LockType.RANGE_READ);
        checkUpgrade(LockType.READ,
                     LockType.RANGE_WRITE,
                     LockGrantType.PROMOTION,
                     LockType.RANGE_WRITE);
        checkUpgrade(LockType.READ,
                     LockType.RANGE_INSERT,
                     null,
                     LockType.READ);

        /* Owner has WRITE */
        checkUpgrade(LockType.WRITE,
                     LockType.RANGE_READ,
                     LockGrantType.EXISTING,
                     LockType.RANGE_WRITE);
        checkUpgrade(LockType.WRITE,
                     LockType.RANGE_WRITE,
                     LockGrantType.EXISTING,
                     LockType.RANGE_WRITE);
        checkUpgrade(LockType.WRITE,
                     LockType.RANGE_INSERT,
                     null,
                     LockType.WRITE);

        /* Owner has RANGE_READ */
        checkUpgrade(LockType.RANGE_READ,
                     LockType.READ,
                     LockGrantType.EXISTING,
                     LockType.RANGE_READ);
        checkUpgrade(LockType.RANGE_READ,
                     LockType.WRITE,
                     LockGrantType.PROMOTION,
                     LockType.RANGE_WRITE);
        checkUpgrade(LockType.RANGE_READ,
                     LockType.RANGE_READ,
                     LockGrantType.EXISTING,
                     LockType.RANGE_READ);
        checkUpgrade(LockType.RANGE_READ,
                     LockType.RANGE_WRITE,
                     LockGrantType.PROMOTION,
                     LockType.RANGE_WRITE);
        checkUpgrade(LockType.RANGE_READ,
                     LockType.RANGE_INSERT,
                     null,
                     LockType.RANGE_READ);

        /* Owner has RANGE_WRITE */
        checkUpgrade(LockType.RANGE_WRITE,
                     LockType.READ,
                     LockGrantType.EXISTING,
                     LockType.RANGE_WRITE);
        checkUpgrade(LockType.RANGE_WRITE,
                     LockType.WRITE,
                     LockGrantType.EXISTING,
                     LockType.RANGE_WRITE);
        checkUpgrade(LockType.RANGE_WRITE,
                     LockType.RANGE_READ,
                     LockGrantType.EXISTING,
                     LockType.RANGE_WRITE);
        checkUpgrade(LockType.RANGE_WRITE,
                     LockType.RANGE_WRITE,
                     LockGrantType.EXISTING,
                     LockType.RANGE_WRITE);
        checkUpgrade(LockType.RANGE_WRITE,
                     LockType.RANGE_INSERT,
                     null,
                     LockType.RANGE_WRITE);

        /* Owner has RANGE_INSERT */
        checkUpgrade(LockType.RANGE_INSERT,
                     LockType.READ,
                     null,
                     LockType.RANGE_INSERT);
        checkUpgrade(LockType.RANGE_INSERT,
                     LockType.WRITE,
                     null,
                     LockType.RANGE_INSERT);
        checkUpgrade(LockType.RANGE_INSERT,
                     LockType.RANGE_READ,
                     null,
                     LockType.RANGE_INSERT);
        checkUpgrade(LockType.RANGE_INSERT,
                     LockType.RANGE_WRITE,
                     null,
                     LockType.RANGE_INSERT);
        checkUpgrade(LockType.RANGE_INSERT,
                     LockType.RANGE_INSERT,
                     LockGrantType.EXISTING,
                     LockType.RANGE_INSERT);
        close(env);
    }

    /**
     * Tests that when the first request is held and the second request is
     * requested, the second grant type is returned and the final type is then
     * held.  A null secondGrantType arg means that an assertion is expected.
     */
    private void checkUpgrade(LockType firstRequest,
                              LockType secondRequest,
                              LockGrantType secondGrantType,
                              LockType finalType)
        throws Exception {

        EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);

        Locker txn1 = Txn.createLocalAutoTxn(envImpl, new TransactionConfig());
        MemoryBudget mb = envImpl.getMemoryBudget();

        try {
            Lock lock = new LockImpl();

            assertEquals(LockGrantType.NEW,
                         lock.lock(firstRequest, txn1, false, false, mb, 0).
                         lockGrant);
            LockGrantType typeGranted = null;
            try {
                typeGranted =
                    lock.lock(secondRequest, txn1, false, false, mb, 0).
                    lockGrant;
                if (secondGrantType == null) {
                    fail("expected AssertionError");
                }
            } catch (AssertionError e) {
                if (secondGrantType != null) {
                    fail(e.toString());
                }
            }
            assertEquals(secondGrantType, typeGranted);

            Set<LockInfo> expectedOwners = new HashSet<LockInfo>();
            expectedOwners.add(new LockInfo(txn1, finalType));
            checkOwners(expectedOwners, lock, 0);
            lock.release(txn1, mb, 0);
            assertEquals(0, lock.nOwners());
            assertEquals(0, lock.nWaiters());
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            txn1.operationEnd();
        }
    }

    /**
     * Tests that when a range read/write is requested, and a range insert is
     * waiting but not held, a WAIT_RESTART occurs.  This requires that the
     * waiter list is examined by Lock.lock().
     */
    @Test
    public void testRangeInsertWaiterConflict()
        throws Exception {

        EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);

        Locker txn1 = Txn.createLocalAutoTxn(envImpl, new TransactionConfig());
        Locker txn2 = Txn.createLocalAutoTxn(envImpl, new TransactionConfig());
        Locker txn3 = Txn.createLocalAutoTxn(envImpl, new TransactionConfig());
        MemoryBudget mb = envImpl.getMemoryBudget();

        try {
            Lock lock = new LockImpl();
            assertEquals(LockGrantType.NEW,
                         lock.lock(LockType.RANGE_READ, txn1, false, false,
                                   mb, 0).
                         lockGrant);
            assertEquals(LockGrantType.WAIT_NEW,
                         lock.lock(LockType.RANGE_INSERT, txn2, false, false,
                                   mb, 0).
                         lockGrant);
            assertEquals(LockGrantType.WAIT_RESTART,
                         lock.lock(LockType.RANGE_READ, txn3, false, false,
                                   mb, 0).
                         lockGrant);

            /* Check that 1 owner, 1 waiter exist. */

            Set<LockInfo> expectedOwners = new HashSet<LockInfo>();
            expectedOwners.add(new LockInfo(txn1, LockType.RANGE_READ));
            checkOwners(expectedOwners, lock, 2);

            List<LockInfo> waiters = new ArrayList<LockInfo>();
            waiters.add(new LockInfo(txn2, LockType.RANGE_INSERT));
            waiters.add(new LockInfo(txn3, LockType.RESTART));
            checkWaiters(waiters, lock);

            /* Release for the sake of the memory leak checking */
            lock.release(txn1, mb, 0);
            lock.release(txn2, mb, 0);
            lock.release(txn3, mb, 0);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            txn1.operationEnd();
            txn2.operationEnd();
            txn3.operationEnd();
            close(env);
        }
    }

    private void checkOwners(Set<LockInfo> expectedOwners,
                             Lock lock,
                             int numExpectedWaiters) {

        /* check number of owners. */
        Set owners = lock.getOwnersClone();
        assertEquals(expectedOwners.size(), owners.size());

        /* check number of waiters. */
        assertEquals(numExpectedWaiters, lock.nWaiters());

        /* Make sure that isOwner returns the right thing. */
        Iterator<LockInfo> iter = expectedOwners.iterator();
        while (iter.hasNext()) {
            LockInfo info = iter.next();

            /* Make sure it's an owner, of the right type of lock. */
            assertEquals(info.getLockType().isWriteLock(),
                         lock.isOwnedWriteLock(info.getLocker()));
            assertTrue(lock.isOwner(info.getLocker(), info.getLockType()));
        }
    }

    private void checkWaiters(List<LockInfo> expectedWaiters,
                              Lock lock) {
        List waiters = lock.getWaitersListClone();
        assertEquals(expectedWaiters.size(), waiters.size());

        /* check order of the list. */
        for (int i = 0; i < expectedWaiters.size(); i++) {
            LockInfo info = expectedWaiters.get(i);
            LockInfo waiterInfo = (LockInfo) waiters.get(i);
            assertEquals("i=" + i, info.getLocker(), waiterInfo.getLocker());
            assertEquals("i=" + i,
                         info.getLockType(), waiterInfo.getLockType());
            assertFalse(lock.isOwner(info.getLocker(), info.getLockType()));
            assertTrue(lock.isWaiter(info.getLocker()));
        }
    }
}
