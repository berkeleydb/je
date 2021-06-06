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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.File;

import org.junit.Test;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.je.BtreeStats;
import com.sleepycat.je.CacheMode;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.SortedLSNTreeWalker.TreeNodeProcessor;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.tree.LN;
import com.sleepycat.je.tree.Node;
import com.sleepycat.je.util.DualTestCase;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.util.test.SharedTestUtils;

public class SortedLSNTreeWalkerTest extends DualTestCase {
    private static boolean DEBUG = false;

    /* Use small NODE_MAX to cause lots of splits. */
    private static final int NODE_MAX = 6;
    private static final int N_RECS = 30;

    private final File envHome;
    private Environment env;
    private Database db;

    public SortedLSNTreeWalkerTest() {
        envHome = SharedTestUtils.getTestDir();
    }

    @Test
    public void testNoDupsLoadLNs()
        throws Throwable {

        doTestNoDups(true);
    }

    @Test
    public void testNoDupsNoLoadLNs()
        throws Throwable {

        doTestNoDups(false);
    }

    private void doTestNoDups(boolean loadLNs)
        throws Throwable {

        open(false);
        writeData(false);
        if (DEBUG) {
            System.out.println("***************");
            DbInternal.getDbImpl(db).getTree().dump();
        }
        BtreeStats stats = (BtreeStats) db.getStats(null);
        close();
        if (DEBUG) {
            System.out.println("***************");
        }
        open(false);
        readData();
        if (DEBUG) {
            DbInternal.getDbImpl(db).getTree().dump();
            System.out.println("***************");
        }
        DatabaseImpl dbImpl = DbInternal.getDbImpl(db);
        db.close();
        db = null;
        assertEquals(N_RECS, walkTree(dbImpl, stats, loadLNs));
        close();
    }

    @Test
    public void testNoDupsDupsAllowed()
        throws Throwable {

        open(true);
        writeData(false);
        if (DEBUG) {
            System.out.println("***************");
            DbInternal.getDbImpl(db).getTree().dump();
        }
        BtreeStats stats = (BtreeStats) db.getStats(null);
        close();
        if (DEBUG) {
            System.out.println("***************");
        }
        open(true);
        if (DEBUG) {
            DbInternal.getDbImpl(db).getTree().dump();
            System.out.println("***************");
        }
        DatabaseImpl dbImpl = DbInternal.getDbImpl(db);
        db.close();
        db = null;
        assertEquals(N_RECS, walkTree(dbImpl, stats, true));
        close();
    }

    @Test
    public void testDups()
        throws Throwable {

        doTestDups(true);
    }

    @Test
    public void testDupsNoLoadLNs()
        throws Throwable {

        doTestDups(false);
    }

    private void doTestDups(boolean loadLNs)
        throws Throwable {

        open(true);
        writeData(true);
        BtreeStats stats = (BtreeStats) db.getStats(null);
        close();
        open(true);
        DatabaseImpl dbImpl = DbInternal.getDbImpl(db);
        db.close();
        db = null;
        assertEquals(N_RECS * 2, walkTree(dbImpl, stats, loadLNs));
        assertEquals(N_RECS * 2, walkTree(dbImpl, stats, loadLNs, loadLNs));
        close();
    }

    @Test
    public void testPendingDeleted()
        throws Throwable {

        open(true);
        int numRecs = writeDataWithDeletes();
        BtreeStats stats = (BtreeStats) db.getStats(null);
        close();
        open(true);
        readData();
        DatabaseImpl dbImpl = DbInternal.getDbImpl(db);
        db.close();
        db = null;
        assertEquals(numRecs, walkTree(dbImpl, stats, true));
        close();
    }

    private void open(boolean allowDuplicates)
        throws Exception {

        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setConfigParam
            (EnvironmentParams.NODE_MAX.getName(), String.valueOf(NODE_MAX));
        /*
        envConfig.setConfigParam
            (EnvironmentParams.MAX_MEMORY.getName(), "10000000");
        envConfig.setConfigParam
            (EnvironmentParams.ENV_RUN_CLEANER.getName(), "false");
        envConfig.setConfigParam
            (EnvironmentParams.ENV_RUN_CHECKPOINTER.getName(), "false");
        */

        envConfig.setConfigParam
            (EnvironmentParams.ENV_RUN_EVICTOR.getName(), "false");

        envConfig.setConfigParam
            (EnvironmentParams.ENV_RUN_INCOMPRESSOR.getName(), "false");

        envConfig.setTransactional(true);
        envConfig.setAllowCreate(true);
        env = create(envHome, envConfig);

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setExclusiveCreate(false);
        dbConfig.setTransactional(true);
        dbConfig.setSortedDuplicates(allowDuplicates);
        /* Use EVICT_LN to test loading LNs via SLTW. */
        dbConfig.setCacheMode(CacheMode.EVICT_LN);
        db = env.openDatabase(null, "testDb", dbConfig);
    }

    private void writeData(boolean dups)
        throws DatabaseException {

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        for (int i = 0; i < N_RECS; i++) {
            IntegerBinding.intToEntry(i, key);
            data.setData(new byte[1000]);
            assertEquals(db.put(null, key, data),
                         OperationStatus.SUCCESS);
            if (dups) {
                IntegerBinding.intToEntry(i + N_RECS + N_RECS, data);
                assertEquals(db.put(null, key, data),
                             OperationStatus.SUCCESS);
            }
        }
    }

    private int writeDataWithDeletes()
        throws DatabaseException {

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        int numInserted = 0;

        data.setData(new byte[10]);

        for (int i = 0; i < N_RECS; i++) {
            IntegerBinding.intToEntry(i, key);
            Transaction txn = env.beginTransaction(null, null);
            assertEquals(db.put(txn, key, data),
                         OperationStatus.SUCCESS);
            boolean deleted = false;
            if ((i % 2) ==0) {
                assertEquals(db.delete(txn, key),
                             OperationStatus.SUCCESS);
                deleted = true;
            }
            if ((i % 3)== 0){
                txn.abort();
            } else {
                txn.commit();
                if (!deleted) {
                    numInserted++;
                }
            }
        }
        return numInserted;
    }

    private void readData()
        throws DatabaseException {

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        IntegerBinding.intToEntry(N_RECS - 1, key);
        assertEquals(db.get(null, key, data, LockMode.DEFAULT),
                     OperationStatus.SUCCESS);
    }

    private int walkTree(final DatabaseImpl dbImpl,
                         final BtreeStats stats,
                         final boolean loadLNNodes) {
        return walkTree(dbImpl, stats, loadLNNodes, false);
    }

    /* Return the number of keys seen in all BINs. */
    private int walkTree(final DatabaseImpl dbImpl,
                         final BtreeStats stats,
                         final boolean loadLNNodes,
                         final boolean loadDupLNNodes) {

        final boolean useOffHeapCache =
            dbImpl.getEnv().getOffHeapCache().isEnabled();

        TestingTreeNodeProcessor tnp = new TestingTreeNodeProcessor() {
                @Override
                public void processLSN(long childLSN,
                                       LogEntryType childType,
                                       Node node,
                                       byte[] lnKey,
                                       int lastLoggedSize) {
                    if (DEBUG) {
                        System.out.println
                            (childType + " " + DbLsn.toString(childLSN));
                    }

                    if (childType.equals(LogEntryType.LOG_DBIN)) {
                        dbinCount++;
                        assertNull(lnKey);
                        assertNotNull(node);
                    } else if (childType.equals(LogEntryType.LOG_BIN)) {
                        binCount++;
                        assertNull(lnKey);
                        assertNotNull(node);
                    } else if (childType.equals(LogEntryType.LOG_DIN)) {
                        dinCount++;
                        assertNull(lnKey);
                        assertNotNull(node);
                    } else if (childType.equals(LogEntryType.LOG_IN)) {
                        inCount++;
                        assertNull(lnKey);
                        assertNotNull(node);
                    } else if (childType.isUserLNType()) {
                        entryCount++;
                        assertNotNull(lnKey);
                        if (dbImpl.getSortedDuplicates()) {
                            if (loadDupLNNodes) {
                                assertNotNull(node);
                            } else if (!useOffHeapCache) {
                                assertNull(node);
                            }
                        } else {
                            if (loadLNNodes) {
                                assertNotNull(node);
                            } else if (!useOffHeapCache) {
                                assertNull(node);
                            }
                        }
                    } else if (childType.equals(LogEntryType.LOG_DUPCOUNTLN)) {
                        dupLNCount++;
                        assertNotNull(lnKey);
                        assertNotNull(node);
                    } else {
                        throw new RuntimeException
                            ("unknown entry type: " + childType);
                    }
                }

                public void processDupCount(long ignore) {
                }
            };

        SortedLSNTreeWalker walker =
            new SortedLSNTreeWalker
            (new DatabaseImpl[] { dbImpl },
             false /*setDbState*/,
             new long[] { dbImpl.getTree().getRootLsn() },
             tnp, null /*savedExceptions*/, null);

        walker.accumulateLNs = loadLNNodes;
        walker.accumulateDupLNs = loadDupLNNodes;

        walker.walk();

        if (DEBUG) {
            System.out.println(stats);
        }

        /* Add one since the root LSN is not passed to the walker. */
        assertEquals(stats.getInternalNodeCount(), tnp.inCount + 1);
        assertEquals(stats.getBottomInternalNodeCount(), tnp.binCount);
        assertEquals(stats.getDuplicateInternalNodeCount(), tnp.dinCount);
        assertEquals(stats.getDuplicateBottomInternalNodeCount(),
                     tnp.dbinCount);
        assertEquals(stats.getLeafNodeCount(), tnp.entryCount);
        assertEquals(stats.getDupCountLeafNodeCount(), tnp.dupLNCount);
        if (DEBUG) {
            System.out.println("INs: " + tnp.inCount);
            System.out.println("BINs: " + tnp.binCount);
            System.out.println("DINs: " + tnp.dinCount);
            System.out.println("DBINs: " + tnp.dbinCount);
            System.out.println("entries: " + tnp.entryCount);
            System.out.println("dupLN: " + tnp.dupLNCount);
        }

        return tnp.entryCount;
    }

    private static class TestingTreeNodeProcessor
        implements TreeNodeProcessor {

        int binCount = 0;
        int dbinCount = 0;
        int dinCount = 0;
        int inCount = 0;
        int entryCount = 0;
        int dupLNCount = 0;

        public void processLSN(long childLSN,
                               LogEntryType childType,
                               Node ignore,
                               byte[] ignore2,
                               int ignore3) {
            throw new RuntimeException("override me please");
        }

        public void processDirtyDeletedLN(long childLsn, LN ln, byte[] lnKey) {
            /* Do nothing. */
        }

        public void processDupCount(int ignore) {
            throw new RuntimeException("override me please");
        }

        public void noteMemoryExceeded() {
        }
    }

    private void close()
        throws Exception {

        if (db != null) {
            db.close();
            db = null;
        }

        if (env != null) {
            close(env);
            env = null;
        }
    }
}
