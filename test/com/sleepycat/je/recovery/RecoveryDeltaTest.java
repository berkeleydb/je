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
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.junit.Test;

import com.sleepycat.je.CacheMode;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.EnvironmentStats;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.evictor.Evictor.EvictionSource;
import com.sleepycat.je.tree.BIN;
import com.sleepycat.je.txn.BasicLocker;

/**
 * Exercise delta BIN logging.
 */
public class RecoveryDeltaTest extends RecoveryTestBase {

    /**
     * The recovery delta tests set extra properties.
     */
    @Override
    public void setExtraProperties() {
        /* Always run with delta logging cranked up. */
        envConfig.setConfigParam
            (EnvironmentParams.BIN_DELTA_PERCENT.getName(), "75");

        /*
         * Make sure that the environments in this unit test always
         * run with checkpointing off, so we can call it explicitly.
         */
        envConfig.setConfigParam
            (EnvironmentParams.ENV_RUN_CHECKPOINTER.getName(), "false");

        /*
         * Make sure that the environments in this unit test always
         * run with the compressor off, so we get known results
         */
        envConfig.setConfigParam
            (EnvironmentParams.ENV_RUN_INCOMPRESSOR.getName(), "false");
    }

    /**
     * Test the interaction of compression and deltas. After a compress,
     * the next entry must be a full one.
     */
    @Test
    public void testCompress()
        throws Throwable {

        createEnvAndDbs(1 << 20, true, NUM_DBS);
        int numRecs = 20;
        try {
            /* Set up an repository of expected data */
            Map<TestData, Set<TestData>> expectedData =
                new HashMap<TestData, Set<TestData>>();

            /* insert all the data */
            Transaction txn = env.beginTransaction(null, null);
            insertData(txn, 0, numRecs - 1, expectedData, 1, true, NUM_DBS);
            txn.commit();

            /* delete every other record */
            txn = env.beginTransaction(null, null);
            deleteData(txn, expectedData, false, true, NUM_DBS);
            txn.commit();

            /* Ask the compressor to run. */
            env.compress();

            /* force a checkpoint, should avoid deltas.. */
            env.checkpoint(forceConfig);

            closeEnv();

            recoverAndVerify(expectedData, NUM_DBS);

        } catch (Throwable t) {
            /* print stacktrace before trying to clean up files */
            t.printStackTrace();
            throw t;
        }
    }

    /**
     * Test a recovery that processes deltas.
     */
    @Test
    public void testRecoveryDelta()
        throws Throwable {

        treeFanout = 16;

        createEnvAndDbs(1 << 20, true, NUM_DBS);

        try {
            /* Set up a repository of expected data */
            Map<TestData, Set<TestData>> expectedData =
                new HashMap<TestData, Set<TestData>>();

            /*
             * Force a checkpoint, to flush a full version of the BIN
             * to disk, so the next checkpoint can cause deltas
             */
            env.checkpoint(forceConfig);

            /*
             * Use repeatable random sequence so that the number of deltas is
             * consistent.
             */
            Random rng = new Random(1);

            /* insert data */
            int numRecs = 80;
            Transaction txn = env.beginTransaction(null, null);
            insertRandomData(txn, rng, numRecs, expectedData, 1, false, 0, 1);
            txn.commit();

            /*
             * Take a checkpoint. This causes a number of BIN-deltas to be
             * logged.
             */
            env.getStats(new StatsConfig().setClear(true));
            env.checkpoint(forceConfig);
            EnvironmentStats envStats = env.getStats(null);
            assertTrue(envStats.getNDeltaINFlush() > 5);

            /* insert data */
            numRecs = 20;
            txn = env.beginTransaction(null, null);
            insertRandomData(txn, rng, numRecs, expectedData, 1, false, 0, 1);
            txn.commit();

            closeEnv();
            recoverAndVerify(expectedData, NUM_DBS);

        } catch (Throwable t) {
            /* print stacktrace before trying to clean up files */
            t.printStackTrace();
            throw t;
        }
    }

    /**
     * This test checks that reconstituting the bin deals properly with
     * the knownDeleted flag
     * insert key1, abort, checkpoint,  -- after abort, childref KD = true;
     * insert key1, commit,             -- after commit, childref KD = false
     * delete key1, abort,              -- BinDelta should have KD = false
     * checkpoint to write deltas,
     * recover. verify key1 is present. -- reconstituteBIN should make KD=false
     */
    @Test
    public void testKnownDeleted()
        throws Throwable {

        createEnvAndDbs(1 << 20, true, NUM_DBS);
        int numRecs = 20;
        try {

            /* Set up a repository of expected data */
            Map<TestData, Set<TestData>> expectedData =
                new HashMap<TestData, Set<TestData>>();

            /* Insert data and abort. */
            Transaction txn = env.beginTransaction(null, null);
            insertData(txn, 0, numRecs - 1, expectedData, 1, false, NUM_DBS);

            /*
             * Add cursors to pin down BINs. Otherwise the checkpoint that
             * follows will compress away all the values.
             */
            Cursor[][] cursors = new Cursor[NUM_DBS][numRecs];
            addCursors(cursors);
            txn.abort();

            /*
             * Force a checkpoint, to flush a full version of the BIN
             * to disk, so the next checkpoint can cause deltas.
             * These checkpointed BINS have known deleted flags set.
             */
            env.checkpoint(forceConfig);
            removeCursors(cursors);

            /*
             * Insert every other data value, makes some known deleted flags
             * false.
             */
            txn = env.beginTransaction(null, null);
            insertData(txn, 0, numRecs - 1, expectedData, 1,
                       true,  true, NUM_DBS);
            txn.commit();

            /* Delete data and abort, keeps known delete flag true */
            txn = env.beginTransaction(null, null);
            deleteData(txn, expectedData, true, false, NUM_DBS);
            txn.abort();

            /* This checkpoint should write deltas. */
            cursors = new Cursor[NUM_DBS][numRecs/2];
            addCursors(cursors);
            env.getStats(new StatsConfig().setClear(true));
            env.checkpoint(forceConfig);
            EnvironmentStats envStats = env.getStats(null);
            assertTrue(envStats.getNDeltaINFlush() > 0);
            removeCursors(cursors);

            closeEnv();
            recoverAndVerify(expectedData, NUM_DBS);

        } catch (Throwable t) {
            /* print stacktrace before trying to clean up files */
            t.printStackTrace();
            throw t;
        }
    }

    /**
     * Checks that BIN-deltas written by eviction (non-provisionally) are
     * applied properly by recovery.
     */
    @Test
    public void testEvictedDelta()
        throws Throwable {

        createEnv(1 << 20, false /*runCheckpointerDaemon*/);
        int numRecs = 200;
        int numDbs = 30;
        try {
            /* Set up a repository of expected data */
            Map<TestData, Set<TestData>> expectedData =
                new HashMap<TestData, Set<TestData>>();

            env.checkpoint(forceConfig);

            createDbs(null /*txn*/, numDbs);

            /*
             * Force eviction to write out non-provisional MapLN deltas.  Must
             * close DBs in order to evict MapLNs.  We verified manually that
             * MapLN deltas are written by evictAllBins. [#21401]
             */
            closeDbs();
            evictAllBins(DbInternal.getNonNullEnvImpl(env).
                         getDbTree().getIdDatabaseImpl());

            createDbs(null /*txn*/, numDbs);

            /* Insert data. */
            Transaction txn = env.beginTransaction(null, null);
            insertData(txn, 0, numRecs - 1, expectedData, 1, true, numDbs);
            txn.commit();

            /*
             * Force eviction to write out non-provisional deltas for app DBs.
             * We verified manually that the deltas are written by
             * evictAllBins.  [#21401]
             */
            for (Database db : dbs) {
                evictAllBins(db);
            }

            closeEnv();
            recoverAndVerify(expectedData, numDbs);

        } catch (Throwable t) {
            /* print stacktrace before trying to clean up files */
            t.printStackTrace();
            throw t;
        }
    }

    private void evictAllBins(Database db) {
        final Cursor cursor = db.openCursor(null, null);
        final List<BIN> bins = collectAllBins(cursor);
        cursor.close();
        /* Must close cursor before evicting. */
        for (BIN bin : bins) {
            evictBin(bin);
        }
    }

    private void evictAllBins(DatabaseImpl dbImpl) {
        final BasicLocker locker =
            BasicLocker.createBasicLocker(DbInternal.getNonNullEnvImpl(env));
        final Cursor cursor = DbInternal.makeCursor(dbImpl, locker, null);
        final List<BIN> bins = collectAllBins(cursor);
        cursor.close();
        locker.operationEnd();
        /* Must close cursor before evicting. */
        for (BIN bin : bins) {
            evictBin(bin);
        }
    }

    private List<BIN> collectAllBins(Cursor cursor) {
        final DatabaseEntry key = new DatabaseEntry();
        final DatabaseEntry data = new DatabaseEntry();
        data.setPartial(0, 0, true);
        final List<BIN> bins = new ArrayList<BIN>();
        BIN prevBin = null;
        while (cursor.getNext(key, data, LockMode.READ_UNCOMMITTED) ==
               OperationStatus.SUCCESS) {
            final BIN thisBin = DbInternal.getCursorImpl(cursor).getBIN();
            if (prevBin != thisBin) {
                prevBin = thisBin;
                bins.add(thisBin);
            }
        }
        return bins;
    }

    private void evictBin(BIN bin) {
        bin.latch(CacheMode.UNCHANGED);
        DbInternal.getNonNullEnvImpl(env).getEvictor().doTestEvict
            (bin, EvictionSource.CACHEMODE);
    }

    /* Add cursors on each value to prevent compression. */
    private void addCursors(Cursor[][] cursors)
        throws DatabaseException {

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();

        /* Pin each record with a cursor. */
        for (int d = 0; d < NUM_DBS; d++) {
            for (int i = 0; i < cursors[d].length; i++) {
                cursors[d][i] = dbs[d].openCursor(null, null);

                for (int j = 0; j < i; j++) {
                    OperationStatus status =
                        cursors[d][i].getNext(key, data,
                                              LockMode.READ_UNCOMMITTED);
                    assertEquals(OperationStatus.SUCCESS, status);
                }
            }
        }
    }

    private void removeCursors(Cursor[][] cursors)
        throws DatabaseException {
        for (int d = 0; d < NUM_DBS; d++) {
            for (int i = 0; i < cursors[d].length; i++) {
                cursors[d][i].close();
            }
        }
    }
}
