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

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.DeadlockException;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockConflictException;
import com.sleepycat.je.LockTimeoutException;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.TransactionTimeoutException;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.junit.JUnitThread;
import com.sleepycat.je.util.DualTestCase;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.je.utilint.TestHook;
import com.sleepycat.util.test.SharedTestUtils;

public class DeadlockTest extends DualTestCase {

    private final int lockerNum = 6;
    private Locker[] txns = new Locker[lockerNum];
    private JUnitThread[] testers = new JUnitThread[lockerNum];
    private AtomicInteger sequence;
    private boolean verbose;

    private Environment env;
    private final File envHome;

    public DeadlockTest() {
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
        
        /*
         * Definitely guarantee a time point whether the left lock actions
         * can continue to execute.
         * 
         * For example, for testDeadlockIntersectionWithOneCommonLocker,
         * if we use owned(lock) to check whether a lock is owned and 
         * whether the current locker can continue to execute, 
         * it may have the following interleaving:
         * 
         * Locker1              locker2              locker3
         * 
         *                       C(3L)
         *  A(1L)                                       A
         *                       B(2L)
         *
         * check Owne(2L)     check Owner(1L)=2                                
         *   ***************************************   
         *  B                     A   
         *           
         *                   Deadlock detection abort
         *                   this locker2, release B
         *                   and C
         *
         *                                         check Owners(3L)
         *                                       // This will loop forever
         *                                       // Becasue 3L is not owned now
         *                                                  C
         *                                                  
         * So for this example, we shuold use sequence rather than owned(lock).
         *   
         *    Locker1        locker2        locker3
         *                    C(3L)
         *      A(1L)                        A
         *                    B(2L)
         *                    
         *    sequence++    sequence++      sequence++
         *   check(seq>3)   check(seq>3)    check(seq>3)
         *   ***************************************    
         *      B             A              C
         *      
         *  Besides, for other test cases where only one deadlock is formed,
         *  we can still use owned(lock). Because no lock will be released
         *  before all owned(lock) is statisfied, and this means that
         *  owned(lock) will not loop infinitely.
         *  
         *  Other possible methods which avoid looping:
         *  1. CountDownLatch
         *  2. com.sleepycat.je.utilint.PollCondition
         */
        sequence = new AtomicInteger(0);
        verbose = true;
        
        for (int i = 0; i < lockerNum; i++) {
            testers[i] = null;
        }
    }

    @After
    public void tearDown() 
        throws Exception {

        LockManager.simulatePartialDeadlockHook = null;
        
        closeEnv();
        
        for (int i = 0; i < lockerNum; i++) {
            if (testers[i] != null) {
                testers[i].shutdown();
                testers[i] = null;
            }
        }

        super.tearDown();
    }
    
    private void initLockers() 
        throws DatabaseException {

        EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);

        for (int i = 0; i < lockerNum; i++) {
            txns[i] = BasicLocker.createBasicLocker(envImpl);
        }
    }

    private void initTxns(TransactionConfig config, EnvironmentImpl envImpl) 
        throws DatabaseException {

        for (int i = 0; i < lockerNum; i++) {
            txns[i] = Txn.createLocalTxn(envImpl, config);
        }
    }

    private void closeEnv() 
        throws DatabaseException {
        
        if (txns[1] instanceof Txn) {
            for (int i = 0; i < lockerNum; i++) {
                ((Txn) txns[i]).abort(false);
            }
        }

        for (int i = 0; i < lockerNum; i++) {
            txns[i].operationEnd();
        }

        close(env);
    }

    /**
     * Test the deadlock between two lockers. Locker1 first acquires L1 and
     * then L2. Locker2 first acquires L2 and then L1.
     * 
     * One locker will be chosen as the victim and the victim will throw
     * DeadlockException.
     */
    @Test
    public void testDeadlockBetweenTwoLockers()
        throws Throwable {
        if (verbose) {
            echo("testDeadlockBetweenTwoLockers");
        }
        final EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);
        final LockManager lockManager =
            envImpl.getTxnManager().getLockManager();

        initLockers();
        
        testers[1] = new JUnitThread("BetweenTwoLockerstestDeadlockLocker1") {
            public void testBody() throws Throwable {
                try {
                    /* Lock L1, should always be granted */
                    lockManager.lock(
                        1L, txns[1], LockType.WRITE, 1,
                        false, false, null);

                    assertTrue(
                        lockManager.isOwner(1L, txns[1], LockType.WRITE));

                    /* Wait for locker2 to own L2. */
                    while (lockManager.nOwners(2L) < 1) {
                        Thread.yield();
                    }

                    /*
                     * Try to lock L2, wait forever. If locker2 is chosen
                     * as the victim, then L2 will be granted after
                     * locker2 aborts and releases L2.
                     */
                    lockManager.lock(
                        2L, txns[1], LockType.WRITE, 0,
                        false, false, null);
                    
                    lockManager.release(1L, txns[1]);
                    lockManager.release(2L, txns[1]);
                    txns[1].removeLock(1L);
                    txns[1].removeLock(2L);
                } catch (DeadlockException e) {
                    checkFail(e);
                    lockManager.release(1L, txns[1]);
                    txns[1].removeLock(1L);
                }
            }
        };

        testers[2] = new JUnitThread("BetweenTwoLockerstestDeadlockLocker2") {
            public void testBody() throws Throwable {
                try {
                    /* Lock L2, should always be granted */
                    lockManager.lock(
                        2L, txns[2], LockType.WRITE, 0,
                        false, false, null);

                    assertTrue(
                        lockManager.isOwner(2L, txns[2], LockType.WRITE));

                    /* Wait for locker1 to own L1. */
                    while (lockManager.nOwners(1L) < 1) {
                        Thread.yield();
                    }
                    
                    /*
                     * Try to lock L1, can wait forever. If locker1 is
                     * chosen as the victim, then L1 will be granted after
                     * locker1 aborts and releases L1.
                     */
                    lockManager.lock(
                        1L, txns[2], LockType.WRITE, 0,
                        false, false, null);
                    
                    lockManager.release(1L, txns[2]);
                    lockManager.release(2L, txns[2]);
                    txns[2].removeLock(1L);
                    txns[2].removeLock(2L);
                } catch (DeadlockException e) {
                    checkFail(e);
                    lockManager.release(2L, txns[2]);
                    txns[2].removeLock(2L);
                }
            }
        };

        testers[1].start();
        testers[2].start();
        testers[1].finishTest();
        testers[2].finishTest();
    }
    
    /**
     * Test a deadlock between two Txns. Txn1 first acquires L1 and then L2.
     * Txn2 first acquires L2 and then L1.
     * 
     * One Txn will be chosen as the victim and the victim will throw
     * DeadlockException.
     */
    @Test
    public void testDeadlockBetweenTwoTxns()
        throws Throwable {
        if (verbose) {
            echo("testDeadlockBetweenTwoTxns");
        }
        final EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);
        final LockManager lockManager =
            envImpl.getTxnManager().getLockManager();
        
        final TransactionConfig config =
            new TransactionConfig().setDurability(Durability.COMMIT_NO_SYNC);

        initTxns(config, envImpl);
        
        testers[1] = new JUnitThread("BetweenTwoTxnstestDeadlockTxn1") {
            public void testBody() throws Throwable {
                try {
                    /* Lock L1, should always be granted */
                    lockManager.lock(
                        1L, txns[1], LockType.WRITE, 1,
                        false, false, null);

                    assertTrue(
                        lockManager.isOwner(1L, txns[1], LockType.WRITE));

                    /* Wait for txns[2] to own L2. */
                    while (lockManager.nOwners(2L) < 1) {
                        Thread.yield();
                    }

                    /*
                     * Try to lock L2, wait forever. If txns[2] is chosen
                     * as the victim, then L2 will be granted after
                     * txns[2] aborts and releases L2.
                     */
                    lockManager.lock(
                        2L, txns[1], LockType.WRITE, 0,
                        false, false, null);
                    
                    lockManager.release(1L, txns[1]);
                    lockManager.release(2L, txns[1]);
                    txns[1].removeLock(1L);
                    txns[1].removeLock(2L);
                } catch (DeadlockException e) {
                    checkFail(e);
                    lockManager.release(1L, txns[1]);
                    txns[1].removeLock(1L);
                }
            }
        };

        testers[2] = new JUnitThread("BetweenTwoTxnstestDeadlockTxn2") {
            public void testBody() throws Throwable {
                try {
                    /* Lock L2, should always be granted */
                    lockManager.lock(
                        2L, txns[2], LockType.WRITE, 0,
                        false, false, null);

                    assertTrue(
                        lockManager.isOwner(2L, txns[2], LockType.WRITE));

                    /* Wait for txns[1] to own L1. */
                    while (lockManager.nOwners(1L) < 1) {
                        Thread.yield();
                    }
                    
                    /*
                     * Try to lock L1, can wait forever. If txns[1] is
                     * chosen as the victim, then L1 will be granted after
                     * txns[1] aborts and releases L1.
                     */
                    lockManager.lock(
                        1L, txns[2], LockType.WRITE, 0,
                        false, false, null);
                    
                    lockManager.release(1L, txns[2]);
                    lockManager.release(2L, txns[2]);
                    txns[2].removeLock(1L);
                    txns[2].removeLock(2L);
                } catch (DeadlockException e) {
                    checkFail(e);
                    lockManager.release(2L, txns[2]);
                    txns[2].removeLock(2L);
                }
            }
        };

        testers[1].start();
        testers[2].start();
        testers[1].finishTest();
        testers[2].finishTest();
    }

    
    /**
     * Test a deadlock among three lockers.
     *  Locker1 owns L1 and waits for L2.
     *  Locker2 owns L2 and waits for L3
     *  Locker3 owns L3 and waits for L1
     */
    @Test
    public void testDeadlockAmongThreeLockers()
        throws Throwable {
        if (verbose) {
            echo("testDeadlockAmongThreeLockers");
        }
        final EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);

        final LockManager lockManager =
            envImpl.getTxnManager().getLockManager();

        initLockers();

        testers[1] = new JUnitThread("AmongThreeLockerstestDeadlockLocker1") {
            public void testBody() throws Throwable {
                try {
                    /* Lock L1, should always be granted */
                    lockManager.lock(
                        1L, txns[1], LockType.WRITE, 0,
                        false, false, null);

                    assertTrue(
                        lockManager.isOwner(1L, txns[1], LockType.WRITE));

                    /* Wait for Locker 2 to lock L2. */
                    while (lockManager.nOwners(2L) < 1) {
                        Thread.yield();
                    }

                    /*
                     * Try to lock L2, wait forever. Lock is granted after
                     * Locker2 aborts and releases L2.
                     */
                    lockManager.lock(
                        2L, txns[1], LockType.WRITE, 0,
                        false, false, null);
                    
                    lockManager.release(1L, txns[1]);
                    lockManager.release(2L, txns[1]);
                    txns[1].removeLock(1L);
                    txns[1].removeLock(2L);
                } catch (LockConflictException e) {
                    checkFail(e);
                    lockManager.release(1L, txns[1]);
                    txns[1].removeLock(1L);
                }
            }
        };

        testers[2] = new JUnitThread("AmongThreeLockerstestDeadlockLocker2") {
            public void testBody() throws Throwable {
                try {
                    /* Lock L2, should always be granted */
                    lockManager.lock(
                        2L, txns[2], LockType.WRITE, 0,
                        false, false, null);

                    assertTrue(
                        lockManager.isOwner(2L, txns[2], LockType.WRITE));

                    /* Wait for Locker 3 to lock L3. */
                    while (lockManager.nOwners(3L) < 1) {
                        Thread.yield();
                    }
                    
                    /*
                     * Try to lock L3, wait forever. Lock is granted after
                     * Locker3 aborts and releases L3.
                     */
                    lockManager.lock(
                        3L, txns[2], LockType.WRITE, 0,
                        false, false, null);
                    
                    lockManager.release(2L, txns[2]);
                    lockManager.release(3L, txns[2]);
                    txns[2].removeLock(2L);
                    txns[2].removeLock(3L);

                } catch (LockConflictException e) {
                    checkFail(e);
                    lockManager.release(2L, txns[2]);
                    txns[2].removeLock(2L);
                }
            }
        };
            
        testers[3] = new JUnitThread("AmongThreeLockerstestDeadlockLocker3") {
            public void testBody() throws Throwable {
                try {
                    /* Lock L3, should always be granted */
                    lockManager.lock(
                        3L, txns[3], LockType.WRITE, 0,
                        false, false, null);

                    assertTrue(
                     lockManager.isOwner(3L, txns[3], LockType.WRITE));

                    /* Wait for Locker1 to lock L1. */
                    while (lockManager.nOwners(1L) < 1) {
                        Thread.yield();
                    }
                    
                    /*
                     * Try to lock L1, wait forever. Lock is granted
                     * after Locker1 aborts and releases L1.
                     */
                    lockManager.lock(
                        1L, txns[3], LockType.WRITE, 0,
                        false, false, null);
                    
                    lockManager.release(3L, txns[3]);
                    lockManager.release(1L, txns[3]);
                    txns[3].removeLock(3L);
                    txns[3].removeLock(1L);

                } catch (LockConflictException e) {
                    checkFail(e);
                    lockManager.release(3L, txns[3]);
                    txns[3].removeLock(3L);
                }
            }
        };

        testers[1].start();
        testers[2].start();
        testers[3].start();
        testers[1].finishTest();
        testers[2].finishTest();
        testers[3].finishTest();
    }
    

    /**
     * Tests a deadlock among three Txns.
     *  Locker1 owns L1 and waits for L2.
     *  Locker2 owns L2 and waits for L3
     *  Locker3 owns L3 and waits for L1
     */
    @Test
    public void testDeadlockAmongThreeTxns()
        throws Throwable {
        if (verbose) {
            echo("testDeadlockAmongThreeTxns");
        }
        
        final EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);

        final LockManager lockManager =
            envImpl.getTxnManager().getLockManager();

        final TransactionConfig config =
            new TransactionConfig().setDurability(Durability.COMMIT_NO_SYNC);

        initTxns(config, envImpl);

        testers[1] = new JUnitThread("AmongThreeTxnstestDeadlockTxn1") {
            public void testBody() throws Throwable {
                try {
                    /* Lock L1, should always be granted */
                    lockManager.lock(
                        1L, txns[1], LockType.WRITE, 0,
                        false, false, null);

                    assertTrue(
                        lockManager.isOwner(1L, txns[1], LockType.WRITE));

                    /* Wait for Locker2 to lock L2. */
                    while (lockManager.nOwners(2L) < 1) {
                        Thread.yield();
                    }

                    /*
                     * Try to lock L2, wait forever. Lock is granted after
                     * Locker2 aborts and releases L2.
                     */
                    lockManager.lock(
                        2L, txns[1], LockType.WRITE, 0,
                        false, false, null);
                    
                    lockManager.release(1L, txns[1]);
                    lockManager.release(2L, txns[1]);
                    txns[1].removeLock(1L);
                    txns[1].removeLock(2L);
                } catch (LockConflictException e) {
                    checkFail(e);
                    lockManager.release(1L, txns[1]);
                    txns[1].removeLock(1L);
                }
            }
        };

        testers[2] = new JUnitThread("AmongThreeTxnstestDeadlockTxn2") {
            public void testBody() throws Throwable {
                try {
                    /* Lock L2, should always be granted */
                    lockManager.lock(
                        2L, txns[2], LockType.WRITE, 0,
                        false, false, null);

                    assertTrue(
                        lockManager.isOwner(2L, txns[2], LockType.WRITE));

                    /* Wait for Locker3 to lock L3. */
                    while (lockManager.nOwners(3L) < 1) {
                        Thread.yield();
                    }
                    
                    /*
                     * Try to lock L3, wait forever. Lock is granted after
                     * Locker3 aborts and releases L3.
                     */
                    lockManager.lock(
                        3L, txns[2], LockType.WRITE, 0,
                        false, false, null);
                    
                    lockManager.release(2L, txns[2]);
                    lockManager.release(3L, txns[2]);
                    txns[2].removeLock(2L);
                    txns[2].removeLock(3L);

                } catch (LockConflictException e) {
                    checkFail(e);
                    lockManager.release(2L, txns[2]);
                    txns[2].removeLock(2L);
                }
            }
        };
            
        testers[3] = new JUnitThread("AmongThreeTxnstestDeadlockTxn3") {
            public void testBody() throws Throwable {
                try {
                    /* Lock L3, should always be granted */
                    lockManager.lock(
                        3L, txns[3], LockType.WRITE, 0,
                        false, false, null);

                    assertTrue(
                     lockManager.isOwner(3L, txns[3], LockType.WRITE));

                    /* Wait for Locker1 to lock L1. */
                    while (lockManager.nOwners(1L) < 1) {
                        Thread.yield();
                    }
                    
                    /*
                     * Try to lock L1, wait forever. Lock is granted
                     * after Locker1 aborts and releases L1.
                     */
                    lockManager.lock(
                        1L, txns[3], LockType.WRITE, 0,
                        false, false, null);
                    
                    lockManager.release(3L, txns[3]);
                    lockManager.release(1L, txns[3]);
                    txns[3].removeLock(3L);
                    txns[3].removeLock(1L);

                } catch (LockConflictException e) {
                    checkFail(e);
                    lockManager.release(3L, txns[3]);
                    txns[3].removeLock(3L);
                }
            }
        };

        testers[1].start();
        testers[2].start();
        testers[3].start();
        testers[1].finishTest();
        testers[2].finishTest();
        testers[3].finishTest();
    }
    
    /**
     * Tests deadlock among Four Txns. Because 3 is always a magic number.
     *  Txn1 owns L1 and waits for L2.
     *  Txn2 owns L2 and waits for L3
     *  Txn3 owns L3 and waits for L4
     *  Txn4 owns L4 and waits for L1
     */
    @Test
    public void testDeadlockAmongFourTxns()
        throws Throwable {
        if (verbose) {
            echo("testDeadlockAmongFourTxns");
        }

        final EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);

        final LockManager lockManager =
            envImpl.getTxnManager().getLockManager();

        final TransactionConfig config =
            new TransactionConfig().setDurability(Durability.COMMIT_NO_SYNC);

        initTxns(config, envImpl);

        testers[1] = new JUnitThread("AmongFourTxnstestDeadlockTxn1") {
            public void testBody() throws Throwable {
                try {
                    /* Lock L1, should always be granted */
                    lockManager.lock(
                        1L, txns[1], LockType.WRITE, 0,
                        false, false, null);

                    assertTrue(
                        lockManager.isOwner(1L, txns[1], LockType.WRITE));

                    /* Wait for Locker2 to lock L2. */
                    while (lockManager.nOwners(2L) < 1) {
                        Thread.yield();
                    }

                    /*
                     * Try to lock L2, wait forever. Lock is granted after
                     * Locker2 aborts and releases L2.
                     */
                    lockManager.lock(
                        2L, txns[1], LockType.WRITE, 0,
                        false, false, null);
                    
                    lockManager.release(1L, txns[1]);
                    lockManager.release(2L, txns[1]);
                    txns[1].removeLock(1L);
                    txns[1].removeLock(2L);
                } catch (LockConflictException e) {
                    checkFail(e);
                    lockManager.release(1L, txns[1]);
                    txns[1].removeLock(1L);
                }
            }
        };

        testers[2] = new JUnitThread("AmongFourTxnstestDeadlockTxn2") {
            public void testBody() throws Throwable {
                try {
                    /* Lock L2 should always be granted */
                    lockManager.lock(
                        2L, txns[2], LockType.WRITE, 0,
                        false, false, null);

                    assertTrue(
                        lockManager.isOwner(2L, txns[2], LockType.WRITE));

                    /* Wait for Locker3 to lock L3. */
                    while (lockManager.nOwners(3L) < 1) {
                        Thread.yield();
                    }
                    
                    /*
                     * Try to lock L3, wait forever. Lock is granted after
                     * Locker3 aborts and releases L3.
                     */
                    lockManager.lock(
                        3L, txns[2], LockType.WRITE, 0,
                        false, false, null);
                    
                    lockManager.release(2L, txns[2]);
                    lockManager.release(3L, txns[2]);
                    txns[2].removeLock(2L);
                    txns[2].removeLock(3L);

                } catch (LockConflictException e) {
                    checkFail(e);
                    lockManager.release(2L, txns[2]);
                    txns[2].removeLock(2L);
                }
            }
        };
            
        testers[3] = new JUnitThread("AmongFourTxnstestDeadlockTxn3") {
            public void testBody() throws Throwable {
                try {
                    /* Lock L3, should always be granted */
                    lockManager.lock(
                        3L, txns[3], LockType.WRITE, 0,
                        false, false, null);

                    assertTrue(
                     lockManager.isOwner(3L, txns[3], LockType.WRITE));

                    /* Wait for Locker1 to lock L4. */
                    while (lockManager.nOwners(4L) < 1) {
                        Thread.yield();
                    }
                    
                    /*
                     * Try to lock L1, wait forever. Lock is granted
                     * after Locker1 aborts and releases L1.
                     */
                    lockManager.lock(
                        4L, txns[3], LockType.WRITE, 0,
                        false, false, null);
                    
                    lockManager.release(3L, txns[3]);
                    lockManager.release(4L, txns[3]);
                    txns[3].removeLock(3L);
                    txns[3].removeLock(4L);

                } catch (LockConflictException e) {
                    checkFail(e);
                    lockManager.release(3L, txns[3]);
                    txns[3].removeLock(3L);
                }
            }
        };

        testers[4] = new JUnitThread("AmongFourTxnstestDeadlockTxn4") {
            public void testBody() throws Throwable {
                try {
                    /* Lock L4, should always be granted */
                    lockManager.lock(
                        4L, txns[4], LockType.WRITE, 0,
                        false, false, null);

                    assertTrue(
                     lockManager.isOwner(4L, txns[4], LockType.WRITE));

                    /* Wait for Locker1 to lock L1. */
                    while (lockManager.nOwners(1L) < 1) {
                        Thread.yield();
                    }
                    
                    /*
                     * Try to lock L1, wait forever. Lock is granted
                     * after Locker1 aborts and releases L1.
                     */
                    lockManager.lock(
                        1L, txns[4], LockType.WRITE, 0,
                        false, false, null);
                    
                    lockManager.release(4L, txns[4]);
                    lockManager.release(1L, txns[4]);
                    txns[4].removeLock(4L);
                    txns[4].removeLock(1L);

                } catch (LockConflictException e) {
                    checkFail(e);
                    lockManager.release(4L, txns[4]);
                    txns[4].removeLock(4L);
                }
            }
        };
        
        testers[1].start();
        testers[2].start();
        testers[3].start();
        testers[4].start();
        testers[1].finishTest();
        testers[2].finishTest();
        testers[3].finishTest();
        testers[4].finishTest();
    }
    
    /**
     * Tests that the correct Exception type is thrown.
     * 
     * When a true deadlock exist, DeadlockException is thrown.
     * When a timeout occurs, LockTimeoutException is thrown.
     */
    @Test
    public void testThrowCorrectException()
        throws Throwable {
        if (verbose) {
            echo("testThrowCorrectException");
        }
        
        final EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);
        final LockManager lockManager =
            envImpl.getTxnManager().getLockManager();
        
        final TransactionConfig config =
            new TransactionConfig().setDurability(Durability.COMMIT_NO_SYNC);

        initTxns(config, envImpl);
        
        testers[1] = new JUnitThread("CorrectExceptiontestDeadlockTxn1") {
            public void testBody() throws Throwable {
                try {
                    /* Lock L1, should always be granted */
                    lockManager.lock(
                        1L, txns[1], LockType.WRITE, 1,
                        false, false, null);

                    assertTrue(
                        lockManager.isOwner(1L, txns[1], LockType.WRITE));

                    /* Wait for txns[2] to own L2. */
                    while (lockManager.nOwners(2L) < 1) {
                        Thread.yield();
                    }

                    /*
                     * Try to lock L2, wait forever. If txns[2] is chosen
                     * as the victim, then L2 will be granted after
                     * txns[2] aborts and releases L2.
                     */
                    lockManager.lock(
                        2L, txns[1], LockType.WRITE, 0,
                        false, false, null);
                    
                    lockManager.release(1L, txns[1]);
                    lockManager.release(2L, txns[1]);
                    txns[1].removeLock(1L);
                    txns[1].removeLock(2L);
                } catch (LockConflictException e) {
                    checkFail(e);
                    assertDeadlock(e);
                    lockManager.release(1L, txns[1]);
                    txns[1].removeLock(1L);
                }
            }
        };

        testers[2] = new JUnitThread("CorrectExceptiontestDeadlockTxn2") {
            public void testBody() throws Throwable {
                try {
                    /* Lock L2, should always be granted */
                    lockManager.lock(
                        2L, txns[2], LockType.WRITE, 0,
                        false, false, null);

                    assertTrue(
                        lockManager.isOwner(2L, txns[2], LockType.WRITE));

                    /* Wait for txns[1] to own L1. */
                    while (lockManager.nOwners(1L) < 1) {
                        Thread.yield();
                    }
                    
                    /*
                     * Try to lock L1, can wait forever. If txns[1] is
                     * chosen as the victim, then L1 will be granted after
                     * txns[1] aborts and releases L1.
                     */
                    lockManager.lock(
                        1L, txns[2], LockType.WRITE, 0,
                        false, false, null);
                    
                    lockManager.release(1L, txns[2]);
                    lockManager.release(2L, txns[2]);
                    txns[2].removeLock(1L);
                    txns[2].removeLock(2L);
                } catch (LockConflictException e) {
                    checkFail(e);
                    assertDeadlock(e);
                    lockManager.release(2L, txns[2]);
                    txns[2].removeLock(2L);
                }
            }
        };

        testers[1].start();
        testers[2].start();
        testers[1].finishTest();
        testers[2].finishTest();     

        testers[3] = new JUnitThread("CorrectExceptiontestDeadlockLocker3") {
            public void testBody() throws Throwable {
                try {
                    /* Lock L1, should always be granted */
                    lockManager.lock(
                        1L, txns[3], LockType.WRITE, 0,
                        false, false, null);

                    assertTrue(
                        lockManager.isOwner(1L, txns[3], LockType.WRITE));
                    
                    sequence.incrementAndGet();

                    /* Wait for txns[4] to own L2. */
                    while (sequence.get() < 2) {
                        Thread.yield();
                    }
                    
                    lockManager.release(1L, txns[3]);
                    txns[3].removeLock(1L);
                } catch (DatabaseException DBE) {
                    DBE.printStackTrace();
                    fail("caught DatabaseException " + DBE);
                }
            }
        };

        testers[4] = new JUnitThread("CorrectExceptiontestDeadlockLocker4") {
            public void testBody() throws Throwable {
                try {
                    /* Wait for txns[3] to lock L1. */
                    while (sequence.get() < 1) {
                        Thread.yield();
                    }
                    
                    /* Lock L1, can not be granted. */
                    lockManager.lock(
                        1L, txns[4], LockType.WRITE, 500,
                        false, false, null);

                    fail("Should throw LockTimeout Exception");
                } catch (LockConflictException e) {
                    checkFail(e);
                    assertLockTimeout(e);
                    sequence.incrementAndGet();
                }
            }
        };

        testers[3].start();
        testers[4].start();
        testers[3].finishTest();
        testers[4].finishTest();
    }
    
    /**
     * DeadlockException is thrown:
     * 1. Before lock timeout wait
     * 2. After lock timeout wait, i.e. one locker/lock timeout
     *
     * This test case focuses on option 1. 
     * This test may be time-sensitive. This means that this test may fail
     * under some specific situation, e.g. due to the CPU schedule.
     */
    @Test
    public void testDeadlockExceptionThrowBeforeLongTimeWait()
        throws Throwable {
        if (verbose) {
            echo("testDeadlockExceptionThrowBeforeLongTimeWait");
        }

        final EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);
        final LockManager lockManager =
            envImpl.getTxnManager().getLockManager();
        
        final TransactionConfig config =
            new TransactionConfig().setDurability(Durability.COMMIT_NO_SYNC);

        initTxns(config, envImpl);
        
        final long lockTimeout = 500;
        
        testers[1] = new JUnitThread("BeforeLongTimeWaittestDeadlockTxn1") {
            public void testBody() throws Throwable {
                long startTime = 0;
                try {
                    /* Lock L1, should always be granted */
                    lockManager.lock(
                        1L, txns[1], LockType.WRITE, 0,
                        false, false, null);

                    assertTrue(
                        lockManager.isOwner(1L, txns[1], LockType.WRITE));

                    /* Wait for txns[2] to lock L2. */
                    while (lockManager.nOwners(2L) < 1) {
                        Thread.yield();
                    }

                    /*
                     * Try to lock L2, wait forever. If txns[2] is chosen
                     * as the victim, then L2 will be granted after
                     * txns[2] aborts and releases L2.
                     */
                    startTime = System.currentTimeMillis();
                    lockManager.lock(
                        2L, txns[1], LockType.WRITE, lockTimeout,
                        false, false, null);

                    lockManager.release(1L, txns[1]);
                    lockManager.release(2L, txns[1]);
                    txns[1].removeLock(1L);
                    txns[1].removeLock(2L);
                } catch (LockConflictException e) {
                    long currentTime = System.currentTimeMillis();
                    checkFail(e);
                    assertTrue(currentTime - startTime < lockTimeout);
                    lockManager.release(1L, txns[1]);
                    txns[1].removeLock(1L);
                }
            }
        };

        testers[2] = new JUnitThread("BeforeLongTimeWaittestDeadlockTxn2") {
            public void testBody() throws Throwable {
                long startTime = 0;
                try {
                    /* Lock L2, should always be granted */
                    lockManager.lock(
                        2L, txns[2], LockType.WRITE, 0,
                        false, false, null);

                    assertTrue(
                        lockManager.isOwner(2L, txns[2], LockType.WRITE));

                    /* Wait for txns[1] to lock L1. */
                    while (lockManager.nOwners(1L) < 1) {
                        Thread.yield();
                    }
                    
                    /*
                     * Try to lock L1, can wait forever. If txns[1] is
                     * chosen as the victim, then L1 will be granted after
                     * txns[1] aborts and releases L1.
                     */
                    startTime = System.currentTimeMillis();
                    lockManager.lock(
                        1L, txns[2], LockType.WRITE, lockTimeout,
                        false, false, null);
                    lockManager.release(1L, txns[2]);
                    lockManager.release(2L, txns[2]);
                    txns[2].removeLock(1L);
                    txns[2].removeLock(2L);
                } catch (LockConflictException e) {
                    long currentTime = System.currentTimeMillis();
                    checkFail(e);
                    assertTrue(currentTime - startTime < lockTimeout);
                    lockManager.release(2L, txns[2]);
                    txns[2].removeLock(2L);
                }
            }
        };

        testers[1].start();
        testers[2].start();
        testers[1].finishTest();
        testers[2].finishTest();
    }
    
    /**
     * DeadlockException is thrown:
     * 1. Before lock timeout wait
     * 2. After lock timeout wait, i.e. one locker/lock timeout
     *
     * This test case focuses on option 2.
     * This test may be time-sensitive. This means that this test may fail
     * under some specific situation, e.g. due to the CPU schedule.
     */
    @Test
    public void testDeadlockExceptionAfterLongTimeWait()
        throws Throwable {
        if (verbose) {
            echo("testDeadlockExceptionAfterLongTimeWait");
        }

        final EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);
        final LockManager lockManager =
            envImpl.getTxnManager().getLockManager();
        
        final TransactionConfig config =
            new TransactionConfig().setDurability(Durability.COMMIT_NO_SYNC);

        initTxns(config, envImpl);
        
        final long lockTimeout = 500;
        
        testers[1] = new JUnitThread("AfterLongTimeWaittestDeadlockTxn1") {
            public void testBody() throws Throwable {
                long startTime = 0;
                try {
                    /* Lock L1, should always be granted */
                    lockManager.lock(
                        1L, txns[1], LockType.WRITE, 1,
                        false, false, null);

                    assertTrue(
                        lockManager.isOwner(1L, txns[1], LockType.WRITE));

                    /* Wait for txns[2] to lock L2. */
                    while (lockManager.nOwners(2L) < 1) {
                        Thread.yield();
                    }

                    /*
                     * Try to lock L2, wait forever. If txns[2] is chosen
                     * as the victim, then L2 will be granted after
                     * txns[2] aborts and releases L2.
                     */
                    startTime = System.currentTimeMillis();
                    lockManager.lock(
                        2L, txns[1], LockType.WRITE, lockTimeout,
                        false, false, null);
                    
                    lockManager.release(1L, txns[1]);
                    lockManager.release(2L, txns[1]);
                    txns[1].removeLock(1L);
                    txns[1].removeLock(2L);
                } catch (LockConflictException e) {
                    long currentTime = System.currentTimeMillis();
                    assertTrue(
                        currentTime - startTime >= (lockTimeout - 10));
                    lockManager.release(1L, txns[1]);
                    txns[1].removeLock(1L);
                }
            }
        };

        testers[2] = new JUnitThread("AfterLongTimeWaittestDeadlockTxn2") {
            public void testBody() throws Throwable {
                try {
                    /* Lock L1, should always be granted */
                    lockManager.lock(
                        2L, txns[2], LockType.WRITE, 0,
                        false, false, null);

                    assertTrue(
                        lockManager.isOwner(2L, txns[2], LockType.WRITE));

                    /* Wait for txns[1] to lock L1. */
                    while (lockManager.nOwners(1L) < 1 ||
                           lockManager.nWaiters(2L) < 1) {
                        Thread.yield();
                    }
                    
                    /* 
                     * Let txns[1] enter the wait region.
                     */
                    try { 
                        Thread.sleep(250); 
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    
                    /*
                     * The following LockManager.lock() will wait
                     * 500 ms between setting waitingFor and detecting
                     * deadlock.
                     */
                    MyHook hook = new MyHook();
                    LockManager.simulatePartialDeadlockHook = hook;
                    lockManager.lock(
                        1L, txns[2], LockType.WRITE, 0,
                        false, false, null);
                    lockManager.release(1L, txns[2]);
                    lockManager.release(2L, txns[2]);
                    txns[2].removeLock(1L);
                    txns[2].removeLock(2L);
                } catch (LockConflictException e) {
                    fail(
                        "Locker1 should timeout and should throw" +
                        "DeadlockException directly");
                }
            }
        };

        testers[1].start();
        testers[2].start();
        testers[1].finishTest();
        testers[2].finishTest();
    }
    
    /**
     * Tests a deadlock produced by two lockers waiting on the same lock.
     * 
     * The test scenario is as follows:
     *         locker1                      locker2
     *         
     *         read lock on 1L
     *         
     *                                      read lock on 1L
     *                                     
     *         write lock on 1L
     *         
     *                                      write lock on 1L
     */
    @Test
    public void testDeadlockProducedByTwoLockersOnOneLock()
        throws Throwable {
        if (verbose) {
            echo("testDeadlockProducedByTwoLockersOnOneLock");
        }

        final EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);
        final LockManager lockManager =
            envImpl.getTxnManager().getLockManager();
        
        final TransactionConfig config =
            new TransactionConfig().setDurability(Durability.COMMIT_NO_SYNC);

        initTxns(config, envImpl);
        
        testers[1] = new JUnitThread("TwoLockersOnOneLocktestDeadlockTxn1") {
            public void testBody() throws Throwable {
                try {
                    /* Lock L1 with READ type, should always be granted */
                    lockManager.lock(
                        1L, txns[1], LockType.READ, 0,
                        false, false, null);

                    assertTrue(
                        lockManager.isOwner(1L, txns[1], LockType.READ));

                    /* Wait for txns[2] to lock L2. */
                    while (lockManager.nOwners(1L) < 2) {
                        Thread.yield();
                    }

                    /*
                     * Try to lock L2, wait forever. If txns[2] is chosen
                     * as the victim, then L2 will be granted after
                     * txns[2] aborts and releases L2.
                     */
                    lockManager.lock(
                        1L, txns[1], LockType.WRITE, 0,
                        false, false, null);
                    
                    lockManager.release(1L, txns[1]);
                    txns[1].removeLock(1L);
                } catch (LockConflictException e) {
                    checkFail(e);
                    if (verbose) {
                        echo("testDeadlockProducedByTwoLockersOnOneLock:Txn1" +
                        e.getMessage()); 
                    }
                    
                    lockManager.release(1L, txns[1]);
                    txns[1].removeLock(1L);
                }
            }
        };

        testers[2] = new JUnitThread("TwoLockersOnOneLocktestDeadlockTxn2") {
            public void testBody() throws Throwable {
                try {
                    /* Lock L1 with READ type, should always be granted */
                    lockManager.lock(
                        1L, txns[2], LockType.READ, 0,
                        false, false, null);

                    assertTrue(
                        lockManager.isOwner(1L, txns[2], LockType.READ));

                    /* Wait for txns[1] to lock L1. */
                    while (lockManager.nOwners(1L) < 2) {
                        Thread.yield();
                    }
                    
                    /*
                     * Try to lock L1, can wait forever. If txns[1] is
                     * chosen as the victim, then L1 will be granted after
                     * txns[1] aborts and releases L1.
                     */
                    lockManager.lock(
                        1L, txns[2], LockType.WRITE, 0,
                        false, false, null);
                    
                    lockManager.release(1L, txns[2]);
                    txns[2].removeLock(1L);
                } catch (LockConflictException e) {
                    checkFail(e);
                    if (verbose) {
                        echo("testDeadlockProducedByTwoLockersOnOneLock:Txn2" +
                        e.getMessage());
                    }
                    
                    lockManager.release(1L, txns[2]);
                    txns[2].removeLock(1L);
                }
            }
        };

        testers[1].start();
        testers[2].start();
        testers[1].finishTest();
        testers[2].finishTest();
    }
    
    /**
     * Test partial deadlock
     * 
     * This test case will test the following scenario. A true deadlock is
     * formed between locker2 and locker3. But the deadlock is first detected
     * by locker1 when it acquires A.
     *    Locker3      locker1        locker2
     *                    A
     *                                   B
     *                    B
     *                                   A
     *      A
     *      
     */
    @Test
    public void testPartialDeadlock()
        throws Throwable {
        if (verbose) {
            echo("testPartialDeadlock");
        }

        final EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);
        final LockManager lockManager =
            envImpl.getTxnManager().getLockManager();
        
        MyHook hook = new MyHook();
        LockManager.simulatePartialDeadlockHook = hook;
        
        final TransactionConfig config =
            new TransactionConfig().setDurability(Durability.COMMIT_NO_SYNC);

        initTxns(config, envImpl);
        
        testers[1] = new JUnitThread("PartialDeadlocktestDeadlockTxn1") {
            public void testBody() throws Throwable {
                try {
                    /* Lock L1, should always be granted */
                    lockManager.lock(
                        1L, txns[1], LockType.WRITE, 1,
                        false, false, null);

                    assertTrue(
                        lockManager.isOwner(1L, txns[1], LockType.WRITE));

                    sequence.incrementAndGet();
                    while (sequence.get() < 2) {
                        Thread.yield();
                    }
                    sequence.incrementAndGet();

                    /*
                     * Try to lock L2, wait forever. If txns[2] is chosen
                     * as the victim, then L2 will be granted after
                     * txns[2] aborts and releases L2.
                     */
                    lockManager.lock(
                        2L, txns[1], LockType.WRITE, 0,
                        false, false, null);
                    
                    lockManager.release(1L, txns[1]);
                    lockManager.release(2L, txns[1]);
                    txns[1].removeLock(1L);
                    txns[1].removeLock(2L);
                } catch (DeadlockException e) {
                    checkFail(e);
                    if (verbose) {
                        echo("testPartialDeadlock: Txn1: " + e.getMessage());
                    }
                   
                    lockManager.release(1L, txns[1]);
                    txns[1].removeLock(1L);
                }
            }
        };

        testers[2] = new JUnitThread("PartialDeadlocktestDeadlockTxn2") {
            public void testBody() throws Throwable {
                try {
                    /* Lock L2, should always be granted */
                    lockManager.lock(
                        2L, txns[2], LockType.WRITE, 0,
                        false, false, null);

                    assertTrue(
                        lockManager.isOwner(2L, txns[2], LockType.WRITE));

                    sequence.incrementAndGet();
                    while (sequence.get() < 2) {
                        Thread.yield();
                    }
                    sequence.incrementAndGet();
                    
                    /*
                     * Try to lock L1, can wait forever. If txns[1] is
                     * chosen as the victim, then L1 will be granted after
                     * txns[1] aborts and releases L1.
                     */
                    lockManager.lock(
                        1L, txns[2], LockType.WRITE, 0,
                        false, false, null);
                    
                    lockManager.release(1L, txns[2]);
                    lockManager.release(2L, txns[2]);
                    txns[2].removeLock(1L);
                    txns[2].removeLock(2L);
                } catch (DeadlockException e) {
                    checkFail(e);
                    if (verbose) {
                        echo("testPartialDeadlock: Txn2: " + e.getMessage());
                    }
                    
                    lockManager.release(2L, txns[2]);
                    txns[2].removeLock(2L);
                }
            }
        };
            
        testers[3] = new JUnitThread("PartialDeadlocktestDeadlockTxn3") {
            public void testBody() throws Throwable {
                try {
                    /* 
                     * Wait for txns[1] to wait on lock L2 and txns[2] to
                     * wait on lock L1. 
                     */
                    while (sequence.get() < 4) {
                        Thread.yield();
                    }
                    
                    /*
                     * In order to guarantee that txns[1] and txns[2] have
                     * already set their waitingFor, we sleep here.
                     * 
                     * Because we set
                     * lockManager.simulatePartialDeadlockHook to be
                     * non-null, so in above two testers, when inovking
                     * LockManager.lock(), bewtween the setting of
                     * waitingFor and detecting deadlock, the thread
                     * will sleep for 500 millis.
                     * 
                     * So after the following 200 millis sleep, txns[3]
                     * will have enough time to do the deadlock detection
                     * as the first locker of txns[1], txns[2] and txns[3].
                     */
                    try { 
                        Thread.sleep(200); 
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    
                    LockManager.simulatePartialDeadlockHook = null;
                    
                    /*
                     * Here we want to check that
                     * DeadlockChecker.hasCycleInternal() will not be
                     * invoked infinitely.
                     */
                    lockManager.lock(
                        1L, txns[3], LockType.WRITE, 0,
                        false, false, null);
                    
                    lockManager.release(1L, txns[3]);
                    txns[3].removeLock(1L);
                } catch (DeadlockException e) {
                    fail(
                        "testPartialDeadlock: Txn3 should not" +
                        "throw DeadlockException.");
                }
            }
        };

        testers[1].start();
        testers[2].start();
        testers[3].start();
        testers[1].finishTest();
        testers[2].finishTest();
        testers[3].finishTest();
    }
    
    /**
     * Two deadlock cycles may intersect:
     *  1. Only have one common locker
     *  2. More than one common locker
     *  
     * This test case tests option 1, i.e. the following scenario: locker1
     * and locker2 have a deadlock; locker2 and locker3 have a deadlock.
     * locker2 is the common locker.
     * 
     *    Locker1      locker2        locker3
     *                    C(3L)
     *      A(1L)                        A
     *                    B(2L)
     *                                   
     *   ***************************************    
     *      B             A              C
     */
    @Test
    public void testDeadlockIntersectionWithOneCommonLocker()
        throws Throwable {
        if (verbose) {
            echo("testDeadlockIntersectionWithOneCommonLocker");
        }

        final EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);
        final LockManager lockManager =
            envImpl.getTxnManager().getLockManager();
        
        final TransactionConfig config =
            new TransactionConfig().setDurability(Durability.COMMIT_NO_SYNC);

        initTxns(config, envImpl);
        
        testers[1] = new JUnitThread("OneCommonLockertestDeadlockTxn1") {
            public void testBody() throws Throwable {
                try {
                    /* Lock L1, should always be granted */
                    lockManager.lock(
                        1L, txns[1], LockType.READ, 0,
                        false, false, null);

                    assertTrue(
                        lockManager.isOwner(1L, txns[1], LockType.READ));
                    
                    if (verbose) {
                       echo("OneCommonLocker: Txn1 owns 1L"); 
                    }
                    

                    sequence.incrementAndGet();
                    while (sequence.get() < 3) {
                        Thread.yield();
                    }

                    if (verbose) {
                        echo("OneCommon: Txn1 finish yield");
                    }
                    
                    /*
                     * Try to lock L2, wait forever. If txns[2] is chosen
                     * as the victim, then L2 will be granted after
                     * txns[2] aborts and releases L2.
                     */
                    lockManager.lock(
                        2L, txns[1], LockType.WRITE, 0,
                        false, false, null);
                    
                    if (verbose) {
                        echo("OneCommonLocker: Txn1 get 2L");
                    }
                    
                    
                    lockManager.release(1L, txns[1]);
                    lockManager.release(2L, txns[1]);
                    txns[1].removeLock(1L);
                    txns[1].removeLock(2L);
                } catch (DeadlockException e) {
                    checkFail(e);
                    if (verbose) {
                        echo("testDeadlockIntersectionWithOneCommonLocker:" +
                        " Txn1: " + e.getMessage()); 
                    }
                    
                    lockManager.release(1L, txns[1]);
                    txns[1].removeLock(1L);
                }
            }
        };

        testers[2] = new JUnitThread("OneCommonLockertestDeadlockTxn2") {
            public void testBody() throws Throwable {
                try {
                    /* Lock L3 and L2, should always be granted */
                    lockManager.lock(
                        3L, txns[2], LockType.WRITE, 0,
                        false, false, null);
                    
                    lockManager.lock(
                        2L, txns[2], LockType.WRITE, 0,
                        false, false, null);

                    assertTrue(
                        lockManager.isOwner(2L, txns[2], LockType.WRITE));
                    
                    assertTrue(
                        lockManager.isOwner(3L, txns[2], LockType.WRITE));
                    
                    if (verbose) {
                        echo("OneCommonLocker: Txn2 owns 2L and 3L"); 
                     }

                    sequence.incrementAndGet();
                    while (sequence.get() < 3) {
                        Thread.yield();
                    }
                    
                    if (verbose) {
                        echo("OneCommon: Txn2 finish yield");
                    }
                    
                    /*
                     * Try to lock L1, can wait forever. If txns[1] is
                     * chosen as the victim, then L1 will be granted after
                     * txns[1] aborts and releases L1.
                     */
                    lockManager.lock(
                        1L, txns[2], LockType.WRITE, 0,
                        false, false, null);
                    
                    if (verbose) {
                        echo("OneCommonLocker: Txn2 get 1L");
                    }
                    
                    lockManager.release(1L, txns[2]);
                    lockManager.release(2L, txns[2]);
                    lockManager.release(3L, txns[2]);
                    txns[2].removeLock(1L);
                    txns[2].removeLock(2L);
                    txns[2].removeLock(3L);
                } catch (DeadlockException e) {
                    checkFail(e);
                    if (verbose) {
                        echo("testDeadlockIntersectionWithOneCommonLocker:" +
                        " Txn2: " + e.getMessage()); 
                    }
                    
                    lockManager.release(2L, txns[2]);
                    lockManager.release(3L, txns[2]);
                    txns[2].removeLock(2L);
                    txns[2].removeLock(3L);
                }
            }
        };
            
        testers[3] = new JUnitThread("OneCommonLockertestDeadlockTxn3") {
            public void testBody() throws Throwable {
                try {
                    /* Lock L1, should always be granted */
                    lockManager.lock(
                        1L, txns[3], LockType.READ, 0,
                        false, false, null);

                    assertTrue(
                        lockManager.isOwner(1L, txns[3], LockType.READ));
                    
                    if (verbose) {
                        echo("OneCommonLocker: Txn3 owns 1L"); 
                     }

                    sequence.incrementAndGet();
                    while (sequence.get() < 3) {
                        Thread.yield();
                    }

                    if (verbose) {
                        echo("OneCommon: Txn3 finish yield");
                    }
                    
                    lockManager.lock(
                        3L, txns[3], LockType.WRITE, 0,
                        false, false, null);
                    
                    if (verbose) {
                        echo("OneCommonLocker: Txn3 get 3L");
                    }
                    
                    lockManager.release(1L, txns[3]);
                    lockManager.release(3L, txns[3]);
                    txns[3].removeLock(1L);
                    txns[3].removeLock(3L);
                } catch (DeadlockException e) {
                    checkFail(e);
                    if (verbose) {
                        echo("testDeadlockIntersectionWithOneCommonLocker:" +
                        " Txn3: " + e.getMessage()); 
                    }
                    lockManager.release(1L, txns[3]);
                    txns[3].removeLock(1L);
                }
            }
        };

        testers[1].start();
        testers[2].start();
        testers[3].start();
        testers[1].finishTest();
        testers[2].finishTest();
        testers[3].finishTest();
    }
    
    /**
     * Two deadlock cycles may intersect:
     *  1. Only have one common locker
     *  2. More than one common locker
     *  
     * This test case tests option 2, i.e. the following scenario: locker1,
     * locker2 and locker3 have a deadlock; locker2, locker3 and locker4
     * have a deadlock. locker2 and locker 3 are the common lockers.
     * 
     *    Locker1      locker2        locker3        locker4
     *                    D(4L)
     *      A(1L)                        C(3L)        A     
     *                    B(2L)
     *                                   
     *   ***************************************************    
     *      B             C              A            D
     */
    @Test
    public void testDeadlockIntersectionWithTwoCommonLocker()
        throws Throwable {
        if (verbose) {
            echo("testDeadlockIntersectionWithTwoCommonLocker");
        }
        
        final EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);
        final LockManager lockManager =
            envImpl.getTxnManager().getLockManager();
        
        final TransactionConfig config =
            new TransactionConfig().setDurability(Durability.COMMIT_NO_SYNC);

        initTxns(config, envImpl);
        
        testers[1] =  new JUnitThread("TwoCommonLockertestDeadlockTxn1") {
            public void testBody() throws Throwable {
                try {
                    /* Lock L1, should always be granted */
                    lockManager.lock(
                        1L, txns[1], LockType.READ, 0,
                        false, false, null);

                    assertTrue(
                        lockManager.isOwner(1L, txns[1], LockType.READ));
                    
                    if (verbose) {
                        echo("TwoCommonLocker: Txn1 owns 1L"); 
                     }
                    
                    sequence.incrementAndGet();
                    while (sequence.get() < 4) {
                        Thread.yield();
                    }
                    
                    if (verbose) {
                        echo("TwoCommon: Txn1 finish yield");
                    }
                    
                    /*
                     * Try to lock L2, wait forever. If txns[2] is chosen
                     * as the victim, then L2 will be granted after
                     * txns[2] aborts and releases L2.
                     */
                    lockManager.lock(
                        2L, txns[1], LockType.WRITE, 0,
                        false, false, null);
                    
                    if (verbose) {
                        echo("TwoCommonLocker: Txn1 get 2L");
                    }
                    
                    lockManager.release(1L, txns[1]);
                    lockManager.release(2L, txns[1]);
                    txns[1].removeLock(1L);
                    txns[1].removeLock(2L);
                } catch (DeadlockException e) {
                    checkFail(e);
                    if (verbose) {
                        echo("testDeadlockIntersectionWithTwoCommonLocker:" +
                        " Txn1: " + e.getMessage()); 
                    }
                    lockManager.release(1L, txns[1]);
                    txns[1].removeLock(1L);
                }
            }
        };

        testers[2] = new JUnitThread("TwoCommonLockertestDeadlockTxn2") {
            public void testBody() throws Throwable {
                try {
                    /* Lock L4 and L2, should always be granted */
                    lockManager.lock(
                        4L, txns[2], LockType.WRITE, 0,
                        false, false, null);
                    
                    lockManager.lock(
                        2L, txns[2], LockType.WRITE, 0,
                        false, false, null);

                    assertTrue(
                        lockManager.isOwner(2L, txns[2], LockType.WRITE));
                    
                    assertTrue(
                        lockManager.isOwner(4L, txns[2], LockType.WRITE));
                    
                    if (verbose) {
                        echo("TwoCommonLocker: Txn2 owns 2L and 4L"); 
                    }
                    
                    sequence.incrementAndGet();
                    while (sequence.get() < 4) {
                        Thread.yield();
                    }
                    
                    if (verbose) {
                        echo("TwoCommon: Txn2 finish yield");
                    }
                    
                    /*
                     * Try to lock L3, can wait forever. If txns[3] is
                     * chosen as the victim, then L3 will be granted after
                     * txns[3] aborts and releases L3.
                     */
                    lockManager.lock(
                        3L, txns[2], LockType.WRITE, 0,
                        false, false, null);
                    
                    if (verbose) {
                        echo("TwoCommonLocker: Txn2 get 2L");
                    }
                    
                    lockManager.release(4L, txns[2]);
                    lockManager.release(2L, txns[2]);
                    lockManager.release(3L, txns[2]);
                    txns[2].removeLock(4L);
                    txns[2].removeLock(2L);
                    txns[2].removeLock(3L);
                } catch (DeadlockException e) {
                    checkFail(e);
                    if (verbose) {
                        echo("testDeadlockIntersectionWithTwoCommonLocker:" +
                        " Txn2: " + e.getMessage()); 
                    }
                    
                    lockManager.release(2L, txns[2]);
                    lockManager.release(4L, txns[2]);
                    txns[2].removeLock(2L);
                    txns[2].removeLock(4L);
                }
            }
        };
            
        testers[3] = new JUnitThread("TwoCommonLockertestDeadlockTxn3") {
            public void testBody() throws Throwable {
                try {
                    /* Lock L3, should always be granted */
                    lockManager.lock(
                        3L, txns[3], LockType.WRITE, 0,
                        false, false, null);

                    assertTrue(
                        lockManager.isOwner(3L, txns[3], LockType.WRITE));
                    
                    if (verbose) {
                        echo("TwoCommonLocker: Txn3 owns 3L"); 
                     }
                    
                    sequence.incrementAndGet();
                    while (sequence.get() < 4) {
                        Thread.yield();
                    }
                    
                    if (verbose) {
                        echo("TwoCommon: Txn3 finish yield");
                    }
                    
                    lockManager.lock(
                        1L, txns[3], LockType.WRITE, 0,
                        false, false, null);
                    
                    if (verbose) {
                        echo("TwoCommonLocker: Txn3 get 1L");
                    }
                    
                    lockManager.release(1L, txns[3]);
                    lockManager.release(3L, txns[3]);
                    txns[3].removeLock(1L);
                    txns[3].removeLock(3L);
                } catch (DeadlockException e) {
                    checkFail(e);
                    if (verbose) {
                        echo("testDeadlockIntersectionWithTwoCommonLocker:" +
                        " Txn3: " + e.getMessage()); 
                    }
                    lockManager.release(3L, txns[3]);
                    txns[3].removeLock(3L);
                }
            }
        };

        testers[4] = new JUnitThread("TwoCommonLockertestDeadlockTxn4") {
            public void testBody() throws Throwable {
                try {
                    /* Lock L1, should always be granted */
                    lockManager.lock(
                        1L, txns[4], LockType.READ, 0,
                        false, false, null);

                    assertTrue(
                        lockManager.isOwner(1L, txns[4], LockType.READ));
                    
                    if (verbose) {
                        echo("TwoCommonLocker: Txn4 owns 1L"); 
                    }
                    
                    sequence.incrementAndGet();
                    while (sequence.get() < 4) {
                        Thread.yield();
                    }
                    
                    if (verbose) {
                        echo("TwoCommon: Txn4 finish yield");
                    }
                    
                    lockManager.lock(
                        4L, txns[4], LockType.WRITE, 0,
                        false, false, null);
                    
                    if (verbose) {
                        echo("TwoCommonLocker: Txn4 get 4L");
                    }
                    
                    lockManager.release(1L, txns[4]);
                    lockManager.release(4L, txns[4]);
                    txns[4].removeLock(1L);
                    txns[4].removeLock(4L);
                } catch (DeadlockException e) {
                    checkFail(e);
                    if (verbose) {
                        echo("testDeadlockIntersectionWithTwoCommonLocker:" +
                        " Txn4: " + e.getMessage()); 
                    }
                    lockManager.release(1L, txns[4]);
                    txns[4].removeLock(1L);
                }
            }
        };
            
        testers[1].start();
        testers[2].start();
        testers[3].start();
        testers[4].start();
        testers[1].finishTest();
        testers[2].finishTest();
        testers[3].finishTest();
        testers[4].finishTest();
    }
    
    private void assertLockTimeout(LockConflictException e) {
        assertTrue(TestUtils.skipVersion(e).startsWith("Lock "));
        assertSame(LockTimeoutException.class, e.getClass());
    }

    private void assertTxnTimeout(LockConflictException e) {
        assertTrue(TestUtils.skipVersion(e).startsWith("Transaction "));
        assertSame(TransactionTimeoutException.class, e.getClass());
    }
    
    private void assertDeadlock(LockConflictException e) {
        assertSame(DeadlockException.class, e.getClass());
    }
    
    /*
     * We may add another Exception API to clarify the deadlock thrown
     * in normal case and in abnormal case. The normal case is that the
     * deadlock is thrown by the chosen victim. The abnormal case is that
     * the deadlock can not be broken by the chosen victim and is thrown
     * when timeout expires.
     * 
     * But now in this unit test, I only check the content of
     * DeadlockException to check whether the deadlock is thrown in
     * unexpected places.
     */
    private void checkFail(LockConflictException e) {
        if (e.getMessage().contains(
            "Unable to break deadlock using random victim")) {
            e.printStackTrace();
            fail("Deadlock should be break by radom victim");
        }
    }
    
    private int echo(String str) {
        System.out.println(str);
        return 0;
    }
    
    class MyHook implements TestHook<Void> {

        @Override
        public void doHook() {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void doHook(Void obj) {
        }
        @Override
        public void hookSetup() {
        }
        @Override
        public void doIOHook() throws IOException {
        }
        @Override
        public Void getHookValue() {
            return null;
        }
    }
}
