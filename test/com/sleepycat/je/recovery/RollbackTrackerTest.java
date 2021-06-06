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

import static com.sleepycat.je.utilint.VLSN.NULL_VLSN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.dbi.EnvironmentFailureReason;
import com.sleepycat.je.recovery.RollbackTracker.RollbackPeriod;
import com.sleepycat.je.txn.RollbackEnd;
import com.sleepycat.je.txn.RollbackStart;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

/**
 * Test that the rollback periods in a log are correctly created.  This test
 * should not be dual mode, because it sets up very specific cases, and does
 * not create a replication stream at all.
 *
 * The test infrastructure can take the declaration of a pseudo log, and then
 * process it accordingly. Each test case is therefore a mini-log excerpt.
 *
 */

/* To add:             ---    ----------  ------------- 
   new VisibleLN  (100L),
   new VisibleLN  (200L),
   new RBStart    (300L,  200L),
   new InvisibleLN(310L),
   new RBEnd      (400L,  200L,        300L)
*/

public class RollbackTrackerTest extends TestBase {
    private static boolean verbose = Boolean.getBoolean("verbose");

    private final File envHome;

    public RollbackTrackerTest() {
        envHome = SharedTestUtils.getTestDir();
    }

    /** 
     * This valid log has a four level nested rollback period 
     */
    @Test
    public void testGoodNestedLogs() {

        LogRecord[] testLog = new LogRecord[] {
            /*            
             * This is a pseudo log.
             *              lsn  txn  match  rollback
             *                   id   point  start    
             *------------   --- ---  ----- -----       */
            new VisibleLN  (10, -500              ),
            new Commit     (11, -500              ),
            new VisibleLN  (12, -501              ),                        

            /* rollback period from lsn 20 -> 22 */
            new Abort      (20, -503              ),                        
            new InvisibleLN(21, -504              ),                        
            new RBStart    (22,       20          ),              

            new VisibleLN  (30, -504              ),
            new VisibleLN  (31, -504              ),

            /* A: second nested rollback period from lsn x -> y */
            /* B: second nested rollback period from lsn x -> y */
            new Abort      (40, -505              ),                  
            new InvisibleLN(41, -506              ), 
            /* C: third nested rollback period from lsn x -> y */
            new Abort      (42, -600              ),                  
            /* D: most nested rollback period from lsn 43 -> 45 */
            new Abort      (43, -600              ),                  
            new InvisibleLN(44, -506              ), 
            new RBStart    (45,       43          ),
            new RBEnd      (46,       43,     45  ),    // D

            new InvisibleLN(47, -507              ), 
            new RBStart    (48,       42          ),    // C
            new InvisibleLN(49, -508              ), 
            new RBStart    (50,       40          ),    // B
            new InvisibleLN(51, -509              ), 
            new RBStart    (52,       40          ),
            new RBEnd      (53,       40,     52  ),    // A

            /* rollback period from lsn 70 -> 92 */
            new Commit     (70, -504              ),
            new InvisibleLN(71, -600              ),
            new InvisibleLN(72, -600              ),
            new RBStart    (73,        70         ),              
            new RBEnd      (74,        70,   73   )
        };

        /* 
         * Each RollbackStart should have a set of txn ids that were
         * active when the rollback started.
         */
        ((RBStart) testLog[5]).addActiveTxnIds(new Long[] {-504L});
        ((RBStart) testLog[13]).addActiveTxnIds(new Long[] {-506L});
        ((RBStart) testLog[16]).addActiveTxnIds(new Long[] {-507L});
        ((RBStart) testLog[18]).addActiveTxnIds(new Long[] {-506L, -508L});
        ((RBStart) testLog[20]).addActiveTxnIds(new Long[] {-509L});
        ((RBStart) testLog[25]).addActiveTxnIds(new Long[] {-600L});

        /* This is what we expect to be in the tracker. */
        List<RollbackPeriod> expected = new ArrayList<RollbackPeriod>();
        expected.add(new RollbackPeriod(70, 73, 74, 9));
        expected.add(new RollbackPeriod(40, 52, 53, 9));
        expected.add(new RollbackPeriod(20, 22, -1, 9));

        /* Process the test data with a tracker. */
        runTest(testLog, expected, 9 /* checkpoint start */);
    }

    /** 
     * This valid log has multiple rollback periods and rollback periods with
     * and without rollback end.
     */
    @Test
    public void testGoodLogs() {

        LogRecord[] testLog = new LogRecord[] {
            /*            
             * This is a pseudo log.
             *              lsn  txn  match  rollback
             *                   id   point  start    
             *------------   --- ---  ----- -----       */
            new VisibleLN  (10, -500              ),

            /* rollback period from lsn 20 -> 22 */
            new Abort      (20  -500              ),                  
            new InvisibleLN(21, -501              ), 
            new RBStart    (22,       20          ),
            new RBEnd      (23,       20,     22  ),   

            new VisibleLN  (30, -502              ),

            /* rollback period from lsn 40 -> 48 */
            new Commit     (40, -502              ),
            new VisibleLN  (45, -503              ),                        
            new Abort      (46, -503              ),                        
            new InvisibleLN(47, -504              ),                        
            new RBStart    (48,       40          ),              

            new VisibleLN  (50, -504              ),
            new VisibleLN  (60, -504              ),

            /* rollback period from lsn 70 -> 92 */
            new Commit     (70, -504              ),
            new InvisibleLN(72, -505              ),

            /* rest of rollback period from lsn 70 -> 92 */
            new InvisibleLN(90, -506              ),
            new RBStart    (92,        70         ),              
            new RBEnd      (94,        70,   92   )
        };

        /* 
         * Each RollbackStart should have a set of txn ids that were
         * active when the rollback started.
         */
        ((RBStart) testLog[3]).addActiveTxnIds(new Long[] {-501L});
        ((RBStart) testLog[10]).addActiveTxnIds(new Long[] {-504L});
        ((RBStart) testLog[16]).addActiveTxnIds(new Long[] {-505L, -506L});

        /* This is what we expect to be in the tracker. */
        List<RollbackPeriod> expected = new ArrayList<RollbackPeriod>();
        expected.add(new RollbackPeriod(70, 92, 94, 9));
        expected.add(new RollbackPeriod(40, 48, -1, 9));
        expected.add(new RollbackPeriod(20, 22, 23, 9));
        /* Process the test data with a tracker. */
        runTest(testLog, expected, 9 /* checkpoint start */);
    }

    /** 
     * This valid log has rollback periods before checkpoint start.
     */
    @Test
    public void testGoodCkptStart() {

        LogRecord[] testLog = new LogRecord[] {
            /*            
             * This is a pseudo log.
             *              lsn  txn  match  rollback
             *                   id   point  start    
             *------------   --- ---  ----- -----       */
            new VisibleLN   (10, -500              ),

            /* rollback period from lsn 20 -> 22 */
            new Abort       (20  -500              ),                  
            new AlreadyRBLN(21, -501              ), 
            new RBStart     (22,       20          ),
            new RBEnd       (23,       20,     22  ),   
            new VisibleLN   (30, -502              )
        };

        /* 
         * Each RollbackStart should have a set of txn ids that were
         * active when the rollback started.
         */
        ((RBStart) testLog[3]).addActiveTxnIds(new Long[] {-501L});

        /* This is what we expect to be in the tracker. */
        List<RollbackPeriod> expected = new ArrayList<RollbackPeriod>();
        expected.add(new RollbackPeriod(20, 22, 23, 40));

        /* Process the test data with a tracker. */
        runTest(testLog, expected, 40 /* checkpoint start. */);

        /* Change the log so that the rollback period has no rollback end */
        testLog = new LogRecord[] {
            /*            
             * This is a pseudo log.
             *              lsn  txn  match  rollback
             *                   id   point  start    
             *------------   --- ---  ----- -----       */
            new VisibleLN   (10, -500              ),

            /* rollback period from lsn 20 -> 22 */
            new Abort       (20  -500              ),                  
            new InvisibleLN (21, -501              ), 
            new RBStart     (22,       20          ),
            new VisibleLN   (30, -502              )
        };

        /* 
         * Each RollbackStart should have a set of txn ids that were
         * active when the rollback started.
         */
        ((RBStart) testLog[3]).addActiveTxnIds(new Long[] {-501L});

        /* This is what we expect to be in the tracker. */
        expected = new ArrayList<RollbackPeriod>();
        expected.add(new RollbackPeriod(20, 22, -1, 40));

        /* Process the test data with a tracker. */
        runTest(testLog, expected, 40L /* checkpoint start. */);
    }

    /** 
     * Bad log - two rollback periods intersect.
     */
    @Test
    public void testBadIntersection() {

        LogRecord[] testLog = new LogRecord[] {
            /*    
             *              lsn  txn  match  rollback
             *                   id   point  start    
             *------------   --- ---  ----- -----       */
            new VisibleLN  (10, -500),
            new Abort      (11, -500),                
            new InvisibleLN(12, -501),
            new Commit     (13, -501),                
            new InvisibleLN(14, -502),
            new RBStart    (15,  11),
            new RBEnd      (16,  11,        15L),
            new InvisibleLN(17, -503),
            new RBStart    (18,  13),
            new VisibleLN  (19),
        };
        ((RBStart) testLog[5]).addActiveTxnIds(new Long[] {-501L, -502L});
        ((RBStart) testLog[8]).addActiveTxnIds(new Long[] {-502L, -503L});
        expectConstructionFailure(testLog);
    }

    /** 
     * Bad log - a commit entry is in the rollback period.
     */
    @Test
    public void testBadCommitInRollbackPeriod() {

        LogRecord[] testLog = new LogRecord[] {
            /*    
             *              lsn  txn  match  rollback
             *                   id   point  start    
             *------------   --- ---  ----- -----       */
            new VisibleLN  (10, -500),
            new Abort      (11, -501),                
            new InvisibleLN(12, -500),
            new Commit     (13, -502),                
            new InvisibleLN(14, -503),
            new RBStart    (15,  11),
            new RBEnd      (16,  11,        15L),
        };

        ((RBStart) testLog[5]).addActiveTxnIds(new Long[] {-500L, -503L});
        expectConstructionFailure(testLog);
    }

    /*
     * Bad log - a LN is inbetween the rBStart and RB commit.
     */

    // TBW


    /**********************************************************************
          Methods for processing test data
     *********************************************************************/

    /** 
     * All test logs are exercised in a way to mimic recovery. The log is
     * - read backwards and constructed (undoLNs w/MapLNs)
     * - then read forwards (redoLN)
     * - then read backwards (undoLN for non-mapLNs)
     */
    private void runTest(LogRecord[] testLog, 
                         List<RollbackPeriod> expected,
                         long checkpointStart) {
        Environment env = createEnvironment();
        RollbackTracker tracker = 
            new RollbackTracker(DbInternal.getNonNullEnvImpl(env));
        tracker.setCheckpointStart(checkpointStart);
        try {
            firstConstructionPass(tracker, testLog);
        } finally {
            env.close();
        }

        /* Check that the rollback period are as expected. */
        assertEquals(expected, tracker.getPeriodList());

        backwardPass(tracker, testLog);
    }

    /**
     * Check that this log fails the construction stage.
     */
    private void expectConstructionFailure(LogRecord[] testLog) {
        Environment env = createEnvironment();
        RollbackTracker tracker = 
            new RollbackTracker(DbInternal.getNonNullEnvImpl(env));
        try {
            firstConstructionPass(tracker, testLog);
            fail("Should have failed");
        } catch(EnvironmentFailureException expected) {
            assertEquals(EnvironmentFailureReason.LOG_INTEGRITY,
                         expected.getReason());
            if (verbose) {
                expected.printStackTrace();
            }
        } finally {
          env.close();
        }
    }

    private Environment createEnvironment() {
        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setAllowCreate(true);
        return new Environment(envHome, envConfig);
    }

    /* 
     * Mimic the first recovery pass, where we scan the log backwards 
     * and create the rollback tracker.
     */
    private void firstConstructionPass(RollbackTracker tracker,
                                       LogRecord[] testData) {
    
        tracker.setFirstPass(true);
        RollbackTracker.Scanner scanner = tracker.getScanner();
        for (int i = testData.length - 1; i >= 0; i--) {
            LogRecord rec = testData[i];
            if (verbose) {
                System.out.println("first pass " + rec);
            }
            rec.doConstructionStep(tracker);
            rec.checkContains(scanner, tracker);
        }
    }

    /* Mimic a later backward scan, after the rollback tracker is created. */
    private void backwardPass(RollbackTracker tracker,
                              LogRecord[] testData) {
    
        tracker.setFirstPass(false);
        RollbackTracker.Scanner scanner = tracker.getScanner();
        for (int i = testData.length - 1; i >= 0; i--) {
            LogRecord rec = testData[i];
            if (verbose) {
                System.out.println("backward pass " + rec);
            }
            rec.checkContains(scanner, tracker);
        }
    }

    /*************************************************************
     * LogRecords and their subclasses represent the log entries that will
     * be passed to the RollbackTracker in recovery.
     *************************************************************/
    
    abstract static class LogRecord {
        final long ownLSN;
    
        LogRecord(long ownLSN) {
            this.ownLSN = ownLSN;
        }

        void doConstructionStep(RollbackTracker tracker) {
            /* 
             * Nothing to do, by default, most log records are not registered
             * with the tracker.
             */
        }

        void checkContains(RollbackTracker.Scanner scanner,
                           RollbackTracker tracker) {
            /* do nothing, containment is only checked for LNs. */
        }

        void checkNeedsRollback(RollbackTracker.Scanner scanner,
                           RollbackTracker tracker) {
            /* do nothing, rollback checking is only checked for LNs. */
        }

        @Override
        public String toString() {
            return getName() + " at lsn " + ownLSN + "[" +
                DbLsn.getNoFormatString(ownLSN) + "]";
        }

        abstract String getName();
    }

    abstract static class TxnLogRecord extends LogRecord {
        final long txnId;

        TxnLogRecord(long lsn, long txnId) {
            super(lsn);
            this.txnId = txnId;
        }

        /* 
         * Don't bother with a txn, this test data is meant to fail before
         * a txn id is needed. Saves on spec'ing the test data.
         */
        TxnLogRecord(long lsn) {
            super(lsn);
            txnId = -1;
        }

        @Override
        public String toString() {
            return super.toString() + " txnId=" + txnId;
        }
    }

    /** A LN that is visible */
    static class VisibleLN extends TxnLogRecord {

        VisibleLN(long ownLSN, long txnId) {
            super(ownLSN, txnId);
        }

        VisibleLN(long ownLSN) {
            super(ownLSN);
        }

        @Override
        void checkContains(RollbackTracker.Scanner scanner,
                           RollbackTracker tracker) {
            assertFalse("contains check for " + this + "\n tracker=" + tracker,
                       scanner.positionAndCheck(ownLSN, txnId));
        }

        @Override
        String getName() {
            return "VisibleLN";
        }
    }

    /** A LN that is in a rollback period, and is invisible */
    static class InvisibleLN extends TxnLogRecord {
        InvisibleLN(long ownLSN, long txnId) {
            super(ownLSN, txnId);
        }

        InvisibleLN(long ownLSN) {
            super(ownLSN);
        }

        @Override
        void checkContains(RollbackTracker.Scanner scanner,
                           RollbackTracker tracker) {
            assertTrue("contains check for " + this + "\n tracker=" + tracker,
                        scanner.positionAndCheck(ownLSN, txnId));
            assertTrue("needsRollback check for " + this + "\n tracker=" + 
                        tracker, scanner.needsRollback());
        }

        @Override
        String getName() {
            return "InvisibleLN";
        }
    }

    /**
     * A LN that is in a rollback period, and is invisible, but is already
     * rolled back.
     */
    static class AlreadyRBLN extends InvisibleLN {
        AlreadyRBLN(long ownLSN, long txnId) {
            super(ownLSN, txnId);
        }

        AlreadyRBLN(long ownLSN) {
            super(ownLSN);
        }


        @Override
        void checkContains(RollbackTracker.Scanner scanner,
                           RollbackTracker tracker) {
            assertTrue("contains check for " + this + "\n tracker=" + tracker,
                        scanner.positionAndCheck(ownLSN, txnId));
            assertFalse("needsRollback check for " + this + "\n tracker=" + 
                        tracker, scanner.needsRollback());
        }    

        @Override
        String getName() {
            return "AlreadyRBLN";
        }
    }

    static class Abort extends TxnLogRecord {
        Abort(long ownLSN, long txnId) {
            super(ownLSN, txnId);
        }

        Abort(long ownLSN) {
            super(ownLSN);
        }

        @Override
        String getName() {
            return "Abort";
        }
    }

    static class Commit extends TxnLogRecord {
        Commit(long ownLSN, long txnId) {
            super(ownLSN, txnId);
        }

        Commit(long ownLSN) {
            super(ownLSN);
        }

        @Override
            void doConstructionStep(RollbackTracker tracker) {
            tracker.checkCommit(ownLSN, txnId);
        }

        @Override
        String getName() {
            return "Commit";
        }
    }

    /** A RollbackStart */
    static class RBStart extends LogRecord {
        final long matchpointLSN;
        Set<Long> activeTxnIds;

        RBStart(long ownLSN, long matchpointLSN) {
            super(ownLSN);
            this.matchpointLSN = matchpointLSN;
            activeTxnIds = new HashSet<Long>();
        }

        void addActiveTxnIds(Long[] txnIds) {
            for (Long id : txnIds) {
                activeTxnIds.add(id);
            }
        }
        
        @Override
        void doConstructionStep(RollbackTracker tracker) {
            tracker.register(new RollbackStart(NULL_VLSN, matchpointLSN,
                                               activeTxnIds),
                             ownLSN);
        }

        @Override
        String getName() {
            return "RBStart";
        }
    }

    /** A RollbackEnd */
    static class RBEnd extends LogRecord {
        final long matchpointLSN;
        final long rollbackStartLSN;

        RBEnd(long ownLSN, long matchpointLSN, long rollbackStartLSN) {
            super(ownLSN);
            this.matchpointLSN = matchpointLSN;
            this.rollbackStartLSN = rollbackStartLSN;
        }

        @Override
        void doConstructionStep(RollbackTracker tracker) {
            tracker.register(new RollbackEnd(matchpointLSN, rollbackStartLSN),
                             ownLSN);
        }

        @Override
        String getName() {
            return "RBEnd";
        }
    }
}

