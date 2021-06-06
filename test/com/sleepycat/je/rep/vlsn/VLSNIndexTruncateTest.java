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

package com.sleepycat.je.rep.vlsn;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.recovery.RecoveryInfo;
import com.sleepycat.je.rep.impl.node.NameIdPair;
import com.sleepycat.je.rep.util.TestLogItem;
import com.sleepycat.je.rep.vlsn.VLSNIndex.ForwardVLSNScanner;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.VLSN;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

import org.junit.Test;

/**
 * Exercise VLSNIndex truncation
 */
public class VLSNIndexTruncateTest extends TestBase {

    private final boolean verbose = Boolean.getBoolean("verbose");
    private final File envRoot;

    public VLSNIndexTruncateTest() {
        envRoot = SharedTestUtils.getTestDir();
    }

    private Environment makeEnvironment()
        throws DatabaseException {

        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        envConfig.setTransactional(false);
        return new Environment(envRoot, envConfig);
    }

    /**
     * Test head truncate extensively. Load up a VLSNIndex. Vary these factors:
     * - truncate at every vlsn
     * - flush at every vlsn, so that we exercise truncating when mappings
     * are in the tracker, when they're in the database, and when they're in
     * both places.
     */
    @Test
    public void testHeadTruncateManyFiles()
        throws Throwable {

        int firstVal = 1;
        int lastVal = 40;

        Environment env = makeEnvironment();
        try {
            /* 
             * Load up a set of expected values. Each VLSN is in its own
             * file. 
             */
            List<VLPair> expected = new ArrayList<VLPair>();
            for (int i = firstVal; i <= lastVal; i++) {
                VLPair m = new VLPair(new VLSN(i), DbLsn.makeLsn(i, i));
                expected.add(m);
            }

            /* Truncate and verify */
            for (int flushPoint = 0; flushPoint < lastVal; flushPoint++) {
                TruncateTester tester = new HeadTruncater(env,
                                                          flushPoint,
                                                          verbose,
                                                          expected);
                tester.runTest();
            }
        } finally {
            env.close();
        }
    }

    @Test
    public void testHeadTruncateSeveralFiles()
        throws Throwable {

        int firstVal = 1;
        int lastVal = 40;

        Environment env = makeEnvironment();

        try {
            /* Load up a set of expected values. 8 VLSNs are in each file. */
            List<VLPair> expected = new ArrayList<VLPair>();
            for (int i = firstVal; i <= lastVal; i++) {
                VLPair m = new VLPair(new VLSN(i), DbLsn.makeLsn(i/8, i));
                expected.add(m);
            }

            /* Truncate and verify */
            for (int flushPoint = 0; flushPoint < lastVal; flushPoint++) {
                TruncateTester tester =
                    new HeadTruncater(env,
                                      flushPoint,
                                      verbose,
                                      expected) {
                        @Override
                         boolean skipMapping(VLPair m) {
                         /* return true only for the last VLSN of each file. */
                            return (m.vlsn.getSequence() % 8)!=7;
                        }
                    };
                tester.runTest();
            }
        } finally {
            env.close();
        }
    }

    /**
     * Make the first vlsn in every file go out of order, and therefore be 
     * skipped. At truncation, we have to add a ghost bucket.
     *
     * Specifically, load vlsns 1->40 in this order, with 8 vlsns per file, so
     * that the first vlsn of a new file is loaded out of order, and is
     * skipped:
     *
     * vlsnIndex.put(vlsn=2, lsn=1/2)
     * vlsnIndex.put(vlsn=1, lsn=1/0)
     * vlsnIndex.put(vlsn=3, lsn=1/3)
     * ...
     * vlsnIndex.put(vlsn=9, lsn=2/9)
     * vlsnIndex.put(vlsn=8, lsn=2/0)
     * vlsnIndex.put(vlsn=10, lsn=2/10)
     * ..
     * That results in a vlsn index full of buckets that have a "skipped"
     * mapping at the head of each one.
     *
     * Then truncate the vlsnindex on file boundaries. Specifically, truncate
     * at lsn 7, 15, 23, etc.
     *
     * Then try to scan the vlsn index. It should have adjusted for the fact 
     * that the first vlsn is not there.
     * TODO: disabled while placeholder bucket work is in progress.
     */
    @Test
    public void testHeadTruncateoutOfOrderMappings()
        throws Throwable {

        int firstVal = 1;
        int lastVal = 40;

        Environment env = makeEnvironment();
        /* 
         * This test needs about 6 files, because log cleaning related
         * truncation of the vlsn index looks at the files in the directory
         * to create ghost buckets. Make fake files, to mimic what would happen
         * if we were really logging vlsns.
         */
        for (int i = 1; i < 7; i++) {
            File f = new File(envRoot, "0000000" + i + ".jdb");
            assertTrue(f.createNewFile());
        }

        try {
            /* Load up a set of expected values. 8 VLSNs are in each file. */
            List<VLPair> expected = new ArrayList<VLPair>();
            for (int i = firstVal; i <= lastVal; i++) {

                /* 
                 * The first vlsn that should be in a file (1, 9, 17, etc)
                 * will have a lsn of file/0 offset, because the ghost bucket
                 * will provide that as the lsn.
                 */
                long lsn = ((i%8) == 1) ?
                    DbLsn.makeLsn(i/8, 0):
                    DbLsn.makeLsn((i-1)/8, i);

                VLPair m = new VLPair(new VLSN(i), lsn);
                expected.add(m);
            }

            /* Truncate and verify */
            for (int flushPoint = 0; flushPoint < lastVal; flushPoint++) {
                TruncateTester tester =
                    new HeadTruncater(env,
                                      flushPoint,
                                      verbose,
                                      expected) {
                        @Override
                         boolean skipMapping(VLPair m) {
                            /* 
                             * Skip mappings for everything except the last 
                             * VLSN of each file. 
                             */
                            return (m.vlsn.getSequence() % 8)!=0;
                        }

                        @Override 
                        void loadMappings() {
                            /* 
                             * Load them in an out of order fashion, creating
                             * buckets that are missing the first mapping.
                             */
                            for (int i = 0; i < expected.size(); i+=8) {
                                loadOne(i+1);
                                loadOne(i);
                                loadOne(i+2);
                                loadOne(i+4);
                                loadOne(i+5);
                                loadOne(i+3);
                                loadOne(i+6);
                                loadOne(i+7);
                            }
                        }

                        private void loadOne(int index) {
                            VLPair m = expected.get(index);
                            vlsnIndex.put(new TestLogItem(m.vlsn, m.lsn, lnType));
                            if (m.vlsn.getSequence() == flushPoint) {
                                vlsnIndex.flushToDatabase
                                   (Durability.COMMIT_NO_SYNC);
                            }
                        }
                    };
                tester.runTest();
            }
        } catch (Throwable e) {
            e.printStackTrace();
            throw e;
        } finally {
            env.close();
        }
    }

    /**
     * Test tail truncate extensively. Load up a VLSNIndex. Vary these factors:
     * - truncate at every vlsn
     * - flush at every vlsn, so that we exercise truncating when mappings
     * are in the tracker, when they're in the database, and when they're in
     * both places.
     */
    @Test
    public void testTailTruncate()
        throws Throwable {

        int firstVal = 1;
        int lastVal = 40;

        Environment env = makeEnvironment();

        try {

            /*
             *  Load up a set of expected values. Each VLSN is in the same
             * file, in order to load up the mappings
             */
            List<VLPair> expected =  new ArrayList<VLPair>();
            for (int i = firstVal; i <= lastVal; i++) {
                VLPair m = new VLPair(new VLSN(i), i);
                expected.add(m);
            }

            for (int flushPoint = 0; flushPoint < lastVal; flushPoint++) {
                TruncateTester tester = new TailTruncater(env,
                                                          flushPoint,
                                                          verbose,
                                                          expected);
                tester.runTest();
            }
        } finally {
            env.close();
        }
    }

    /**
     * Test VLSNIndex.truncateFromTail
     */
    private static class TailTruncater extends TruncateTester {

        TailTruncater(Environment env,
                      int flushPoint,
                      boolean verbose,
                      List<VLPair> expected) {
            super(env, flushPoint, verbose, expected);
        }

        @Override
        void doTruncate(VLPair deletePoint)
            throws DatabaseException {

            vlsnIndex.truncateFromTail(deletePoint.vlsn, deletePoint.lsn-1);
            if (verbose) {
                System.out.println(debugHeader);
            }

            assertTrue(debugHeader, vlsnIndex.verify(verbose));

            if (deletePoint.vlsn.equals(expected.get(0).vlsn)) {
                postTruncateFirst = VLSN.NULL_VLSN;
                postTruncateLast = VLSN.NULL_VLSN;
            } else {
                postTruncateFirst = expected.get(0).vlsn;
                postTruncateLast = deletePoint.vlsn.getPrev();
            }
        }
    }

    /**
     * Test VLSNIndex.truncateFromTail
     */
    private static class HeadTruncater extends TruncateTester {

        HeadTruncater(Environment env,
                      int flushPoint,
                      boolean verbose,
                      List<VLPair> expected) {
            super(env, flushPoint, verbose, expected);
        }

        @Override
        void doTruncate(VLPair deletePoint)
            throws DatabaseException {

            if (verbose) {
                System.out.println("----" + debugHeader);
            }
            vlsnIndex.truncateFromHead(deletePoint.vlsn,
                                       DbLsn.getFileNumber(deletePoint.lsn));
            
            assertTrue(debugHeader, vlsnIndex.verify(verbose));

            VLSN lastVLSN = expected.get(expected.size()-1).vlsn;

            if (deletePoint.vlsn.equals(lastVLSN)) {
                /* We deleted everything out of the index. */
                postTruncateFirst = VLSN.NULL_VLSN;
                postTruncateLast = VLSN.NULL_VLSN;
            } else {
                postTruncateFirst = deletePoint.vlsn.getNext();
                postTruncateLast = lastVLSN;
            }
        }
    }

    /**
     * TruncateTesters truncate a VLSNIndex from either the head or the tail
     * and then check that the range and mappings are as expected.
     */
    private abstract static class TruncateTester {

        private static final String testMapDb = "TEST_MAP_DB";

        private final int stride = 5;
        private final int maxMappings = 4;
        private final int maxDist = 1000;
        private final Environment env;
        protected final boolean verbose;
        protected final int flushPoint;

        protected VLSNIndex vlsnIndex;
        protected VLSN postTruncateFirst;
        protected VLSN postTruncateLast;
        protected String debugHeader;
        protected final byte lnType =   
            LogEntryType.LOG_INS_LN_TRANSACTIONAL.getTypeNum();

        protected List<VLPair> expected;

        TruncateTester(Environment env,
                       int flushPoint,
                       boolean verbose,
                       List<VLPair> expected) {
            this.env = env;
            this.flushPoint = flushPoint;
            this.verbose = verbose;
            this.expected = expected;
        }

        /**
         * Create a VLSNIndex loaded with the values in the expected List.
         */
        private void initIndex()
            throws DatabaseException {

            vlsnIndex = new VLSNIndex(DbInternal.getNonNullEnvImpl(env),
                                      testMapDb, new NameIdPair("n1",1),
                                      stride, maxMappings, maxDist,
                                      new RecoveryInfo());
            loadMappings();
        }

        void loadMappings() {
            for (VLPair m : expected) {
                vlsnIndex.put(new TestLogItem(m.vlsn, m.lsn, lnType));
                if (m.vlsn.getSequence() == flushPoint) {
                    vlsnIndex.flushToDatabase(Durability.COMMIT_NO_SYNC);
                }
            }
        }

        /*
         * @return true if we should not test truncate at this VLSN. In real
         * life, head truncation always happens on the last VLSN in the file,
         * because head truncation is done by the cleaner, which searches out
         * the last vlsn.
         * Tail truncation can specify any vlsn as the truncation point.
         */
        boolean skipMapping(VLPair m) {
            return false;
        }

        void runTest() 
            throws Throwable {

            for (VLPair mapping : expected) {
                try {
                    if (skipMapping(mapping)) {
                        continue;
                    }

                    initIndex();

                    /* Truncate the VLSNIndex. */
                    debugHeader = "deletePoint=" + mapping.vlsn +
                        " flushPoint=" + flushPoint;
                    doTruncate(mapping);

                    /* Check the range. */
                    VLSNRange truncatedRange = vlsnIndex.getRange();
                    assertEquals(postTruncateFirst, truncatedRange.getFirst());
                    assertEquals(postTruncateLast, truncatedRange.getLast());

                    if (postTruncateFirst.equals(VLSN.NULL_VLSN)) {
                        continue;
                    }

                    /* 
                     * Scan the index and check all mappings. We've already
                     * verified the index, so we can use a dump of the mappings
                     * from the index to verify the scanner results.
                     */
                    Map<VLSN, Long> dumpedMappings = vlsnIndex.dumpDb(verbose);
                    ForwardVLSNScanner scanner = 
                        new ForwardVLSNScanner(vlsnIndex);
                    long startLsn = scanner.getStartingLsn(postTruncateFirst);

                    long expectedIndex = postTruncateFirst.getSequence() - 1L;
                    assertEquals(new Long(expected.get((int)expectedIndex).lsn),
                                 new Long(startLsn));

                    for (VLPair m : expected) {
                        if ((m.vlsn.compareTo(postTruncateFirst) >= 0) &&
                            (m.vlsn.compareTo(postTruncateLast) <= 0)) {


                            Long onDiskLsn = dumpedMappings.get(m.vlsn);
                            if (onDiskLsn == null) {
                                continue;
                            }
                            
                            long scannedLsn = scanner.getPreciseLsn(m.vlsn);
                            if (onDiskLsn.longValue() != DbLsn.NULL_LSN) {
                                assertEquals(onDiskLsn.longValue(), scannedLsn);
                            }
                        }
                    }
                } catch (Throwable t) {
                    t.printStackTrace();
                    throw t;
                } finally {
                    if (vlsnIndex != null) {
                        vlsnIndex.close();
                        vlsnIndex = null;

                        /* 
                         * Remove the on-disk mapping database which represents
                         * the persistent storage of the vlsn index, so each
                         * test run starts with a clean slate.
                         */
                        env.removeDatabase(null, testMapDb);
                    }
                }
            }
        }

        abstract void doTruncate(VLPair deletePoint)
            throws DatabaseException;
    }
}
