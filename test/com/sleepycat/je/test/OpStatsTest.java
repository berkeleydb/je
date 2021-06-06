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
package com.sleepycat.je.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Durability;
import com.sleepycat.je.EnvironmentStats;
import com.sleepycat.je.Get;
import com.sleepycat.je.OperationResult;
import com.sleepycat.je.Put;
import com.sleepycat.je.SecondaryConfig;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.SecondaryKeyCreator;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.WriteOptions;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.impl.RepTestBase;
import com.sleepycat.je.rep.utilint.RepTestUtils.RepEnvInfo;

import org.junit.Before;
import org.junit.Test;

/**
 * Tests throughput/Op statistics.
 */
public class OpStatsTest extends RepTestBase {

    @Override
    @Before
    public void setUp() throws Exception {
        groupSize = 3;
        super.setUp();
        for (RepEnvInfo info : repEnvInfo) {
            info.getEnvConfig().setDurability(new Durability(
                Durability.SyncPolicy.NO_SYNC,
                Durability.SyncPolicy.NO_SYNC,
                Durability.ReplicaAckPolicy.ALL));
        }
    }

    @Test
    public void testPriOps() {

        createGroup();

        final ReplicatedEnvironment env = repEnvInfo[0].getEnv();
        final Database db = env.openDatabase(null, "foo", dbconfig);

        expectStats(0, null);

        final DatabaseEntry key = new DatabaseEntry();
        final DatabaseEntry data = new DatabaseEntry();

        data.setData(new byte[100]);

        /* Primary insert. */

        IntegerBinding.intToEntry(1, key);
        OperationResult result =
            db.put(null, key, data, Put.NO_OVERWRITE, null);
        assertNotNull(result);

        Transaction txn = env.beginTransaction(null, null);
        try (final Cursor c = db.openCursor(txn, null)) {
            IntegerBinding.intToEntry(2, key);
            result = c.put(key, data, Put.OVERWRITE, null);
            assertNotNull(result);
        }
        txn.commit();

        expectStats(1, new ExpectStats() {
            @Override
            public void check(EnvironmentStats stats, boolean isMaster) {
                assertEquals(2, stats.getPriInsertOps());
            }
        });

        /* Primary insert failure. */

        IntegerBinding.intToEntry(1, key);
        result = db.put(null, key, data, Put.NO_OVERWRITE, null);
        assertNull(result);

        txn = env.beginTransaction(null, null);
        try (final Cursor c = db.openCursor(txn, null)) {
            IntegerBinding.intToEntry(2, key);
            result = c.put(key, data, Put.NO_OVERWRITE, null);
            assertNull(result);
        }
        txn.commit();

        expectMasterStats(1, new ExpectStats() {
            @Override
            public void check(EnvironmentStats stats, boolean isMaster) {
                assertEquals(2, stats.getPriInsertFailOps());
            }
        });

        /* Primary update. */

        IntegerBinding.intToEntry(1, key);
        result = db.put(null, key, data, Put.OVERWRITE, null);
        assertNotNull(result);

        txn = env.beginTransaction(null, null);
        try (final Cursor c = db.openCursor(txn, null)) {
            IntegerBinding.intToEntry(2, key);
            result = c.put(key, data, Put.OVERWRITE, null);
            assertNotNull(result);
        }
        txn.commit();

        expectStats(1, new ExpectStats() {
            @Override
            public void check(EnvironmentStats stats, boolean isMaster) {
                assertEquals(2, stats.getPriUpdateOps());
            }
        });

        /* Primary search. */

        IntegerBinding.intToEntry(1, key);
        result = db.get(null, key, data, Get.SEARCH, null);
        assertNotNull(result);

        try (final Cursor c = db.openCursor(null, null)) {
            IntegerBinding.intToEntry(2, key);
            result = c.get(key, data, Get.SEARCH, null);
            assertNotNull(result);
        }

        expectMasterStats(1, new ExpectStats() {
            @Override
            public void check(EnvironmentStats stats, boolean isMaster) {
                assertEquals(2, stats.getPriSearchOps());
            }
        });

        /* Primary search failure. */

        IntegerBinding.intToEntry(10, key);
        result = db.get(null, key, data, Get.SEARCH, null);
        assertNull(result);

        try (final Cursor c = db.openCursor(null, null)) {
            IntegerBinding.intToEntry(20, key);
            result = c.get(key, data, Get.SEARCH, null);
            assertNull(result);
        }

        expectMasterStats(1, new ExpectStats() {
            @Override
            public void check(EnvironmentStats stats, boolean isMaster) {
                assertEquals(2, stats.getPriSearchFailOps());
            }
        });

        /* Primary position. */

        try (final Cursor c = db.openCursor(null, null)) {
            result = c.get(key, data, Get.FIRST, null);
            assertNotNull(result);
            result = c.get(key, data, Get.NEXT, null);
            assertNotNull(result);
            result = c.get(key, data, Get.NEXT, null);
            assertNull(result);
        }

        expectMasterStats(1, new ExpectStats() {
            @Override
            public void check(EnvironmentStats stats, boolean isMaster) {
                assertEquals(2, stats.getPriPositionOps());
            }
        });

        /* Primary deletion. */

        IntegerBinding.intToEntry(1, key);
        result = db.delete(null, key, null);
        assertNotNull(result);

        txn = env.beginTransaction(null, null);
        try (final Cursor c = db.openCursor(txn, null)) {
            IntegerBinding.intToEntry(2, key);
            result = c.get(key, data, Get.SEARCH, null);
            assertNotNull(result);
            result = c.delete(null);
            assertNotNull(result);
        }
        txn.commit();

        expectStats(2, 1, new ExpectStats() {
            @Override
            public void check(EnvironmentStats stats, boolean isMaster) {
                if (isMaster) {
                    assertEquals(1, stats.getPriSearchOps());
                }
                assertEquals(2, stats.getPriDeleteOps());
            }
        });

        /* Primary deletion failure. */

        IntegerBinding.intToEntry(1, key);
        result = db.delete(null, key, null);
        assertNull(result);

        expectMasterStats(1, new ExpectStats() {
            @Override
            public void check(EnvironmentStats stats, boolean isMaster) {
                assertEquals(1, stats.getPriDeleteFailOps());
            }
        });

        /* Close */

        db.close();
        closeNodes(repEnvInfo);
    }

    @Test
    public void testSecOps() {

        createGroup();

        final Database[] priDbs = new Database[repEnvInfo.length];

        final SecondaryDatabase[] secDbs =
            new SecondaryDatabase[repEnvInfo.length];

        for (int i = 0; i < repEnvInfo.length; i += 1) {

            final ReplicatedEnvironment env = repEnvInfo[i].getEnv();

            final Database db = env.openDatabase(null, "pri", dbconfig);

            final SecondaryConfig secConfig = new SecondaryConfig();
            secConfig.setAllowCreate(i == 0);
            secConfig.setTransactional(true);
            secConfig.setSortedDuplicates(true);
            secConfig.setKeyCreator(new SecondaryKeyCreator() {
                @Override
                public boolean createSecondaryKey(
                    SecondaryDatabase secondary,
                    DatabaseEntry key,
                    DatabaseEntry data,
                    DatabaseEntry result) {
                    result.setData(key.getData());
                    return true;
                }
            });

            final SecondaryDatabase secDb =
                env.openSecondaryDatabase(null, "secDb", db, secConfig);

            priDbs[i] = db;
            secDbs[i] = secDb;
        }

        final ReplicatedEnvironment env = repEnvInfo[0].getEnv();
        final Database db = priDbs[0];
        final SecondaryDatabase secDb = secDbs[0];

        expectStats(0, null);

        final DatabaseEntry key = new DatabaseEntry();
        final DatabaseEntry data = new DatabaseEntry();

        data.setData(new byte[100]);

        /* Secondary insert. */

        IntegerBinding.intToEntry(1, key);
        OperationResult result =
            db.put(null, key, data, Put.NO_OVERWRITE, null);
        assertNotNull(result);

        Transaction txn = env.beginTransaction(null, null);
        try (final Cursor c = db.openCursor(txn, null)) {
            IntegerBinding.intToEntry(2, key);
            result = c.put(key, data, Put.OVERWRITE, null);
            assertNotNull(result);
        }
        txn.commit();

        expectStats(2, new ExpectStats() {
            @Override
            public void check(EnvironmentStats stats, boolean isMaster) {
                assertEquals(2, stats.getPriInsertOps());
                assertEquals(2, stats.getSecInsertOps());
            }
        });

        /* Secondary update. */

        IntegerBinding.intToEntry(1, key);
        result = db.put(
            null, key, data, Put.OVERWRITE,
            new WriteOptions().setTTL(1).setUpdateTTL(true));
        assertNotNull(result);

        txn = env.beginTransaction(null, null);
        try (final Cursor c = db.openCursor(txn, null)) {
            IntegerBinding.intToEntry(2, key);
            result = c.put(
                key, data, Put.OVERWRITE, null);
            assertNotNull(result);
        }
        txn.commit();

        expectStats(2, new ExpectStats() {
            @Override
            public void check(EnvironmentStats stats, boolean isMaster) {
                assertEquals(2, stats.getPriUpdateOps());
                assertEquals(1, stats.getSecUpdateOps());
            }
        });

        /* Secondary search. */

        IntegerBinding.intToEntry(1, key);
        result = secDb.get(null, key, data, Get.SEARCH, null);
        assertNotNull(result);

        try (final Cursor c = secDb.openCursor(null, null)) {
            IntegerBinding.intToEntry(2, key);
            result = c.get(key, data, Get.SEARCH, null);
            assertNotNull(result);
        }

        expectMasterStats(2, new ExpectStats() {
            @Override
            public void check(EnvironmentStats stats, boolean isMaster) {
                assertEquals(2, stats.getSecSearchOps());
                assertEquals(2, stats.getPriSearchOps());
            }
        });

        /* Secondary search failure. */

        IntegerBinding.intToEntry(10, key);
        result = secDb.get(null, key, data, Get.SEARCH, null);
        assertNull(result);

        try (final Cursor c = secDb.openCursor(null, null)) {
            IntegerBinding.intToEntry(20, key);
            result = c.get(key, data, Get.SEARCH, null);
            assertNull(result);
        }

        expectMasterStats(1, new ExpectStats() {
            @Override
            public void check(EnvironmentStats stats, boolean isMaster) {
                assertEquals(2, stats.getSecSearchFailOps());
            }
        });

        /* Secondary position. */

        try (final Cursor c = secDb.openCursor(null, null)) {
            result = c.get(key, data, Get.FIRST, null);
            assertNotNull(result);
            result = c.get(key, data, Get.NEXT, null);
            assertNotNull(result);
            result = c.get(key, data, Get.NEXT, null);
            assertNull(result);
        }

        expectMasterStats(2, new ExpectStats() {
            @Override
            public void check(EnvironmentStats stats, boolean isMaster) {
                assertEquals(2, stats.getSecPositionOps());
                assertEquals(2, stats.getPriSearchOps());
            }
        });

        /* Secondary deletion via primary DB. */

        IntegerBinding.intToEntry(1, key);
        result = db.delete(null, key, null);
        assertNotNull(result);

        expectStats(2, new ExpectStats() {
            @Override
            public void check(EnvironmentStats stats, boolean isMaster) {
                assertEquals(1, stats.getPriDeleteOps());
                assertEquals(1, stats.getSecDeleteOps());
            }
        });

        /* Secondary deletion via secondary DB. */

        IntegerBinding.intToEntry(2, key);
        result = secDb.delete(null, key, null);
        assertNotNull(result);

        expectStats(2, new ExpectStats() {
            @Override
            public void check(EnvironmentStats stats, boolean isMaster) {
                assertEquals(1, stats.getPriDeleteOps());
                assertEquals(1, stats.getSecDeleteOps());
            }
        });

        /* Insert records again so we can delete them again. */

        IntegerBinding.intToEntry(1, key);
        result = db.put(null, key, data, Put.NO_OVERWRITE, null);
        assertNotNull(result);

        IntegerBinding.intToEntry(2, key);
        result = db.put(null, key, data, Put.NO_OVERWRITE, null);
        assertNotNull(result);

        for (final RepEnvInfo info : repEnvInfo) {
            info.getEnv().getStats(StatsConfig.CLEAR);
        }

        /* Secondary deletion via primary cursor. */

        txn = env.beginTransaction(null, null);
        try (final Cursor c = db.openCursor(txn, null)) {
            IntegerBinding.intToEntry(1, key);
            result = c.get(key, data, Get.SEARCH, null);
            assertNotNull(result);
            result = c.delete(null);
            assertNotNull(result);
        }
        txn.commit();

        expectStats(3, 2, new ExpectStats() {
            @Override
            public void check(EnvironmentStats stats, boolean isMaster) {
                if (isMaster) {
                    assertEquals(1, stats.getPriSearchOps());
                }
                assertEquals(1, stats.getPriDeleteOps());
                assertEquals(1, stats.getSecDeleteOps());
            }
        });

        /* Secondary deletion via secondary cursor. */

        txn = env.beginTransaction(null, null);
        try (final Cursor c = secDb.openCursor(txn, null)) {
            IntegerBinding.intToEntry(2, key);
            result = c.get(key, data, Get.SEARCH, null);
            assertNotNull(result);
            result = c.delete(null);
            assertNotNull(result);
        }
        txn.commit();

        expectStats(4, 2, new ExpectStats() {
            @Override
            public void check(EnvironmentStats stats, boolean isMaster) {
                if (isMaster) {
                    assertEquals(1, stats.getSecSearchOps());
                    assertEquals(1, stats.getPriSearchOps());
                }
                assertEquals(1, stats.getPriDeleteOps());
                assertEquals(1, stats.getSecDeleteOps());
            }
        });

        /* Secondary deletion failure. */

        IntegerBinding.intToEntry(1, key);
        result = db.delete(null, key, null);
        assertNull(result);

        IntegerBinding.intToEntry(1, key);
        result = secDb.delete(null, key, null);
        assertNull(result);

        expectMasterStats(1, new ExpectStats() {
            @Override
            public void check(EnvironmentStats stats, boolean isMaster) {
                assertEquals(2, stats.getPriDeleteFailOps());
            }
        });

        /* Close */

        for (int i = 0; i < repEnvInfo.length; i += 1) {
            secDbs[i].close();
            priDbs[i].close();
        }
        closeNodes(repEnvInfo);
    }

    private interface ExpectStats {
        void check(EnvironmentStats stats, boolean isMaster);
    }

    private void expectStats(int nNonZero, ExpectStats expectStats) {
        expectStats(nNonZero, nNonZero, expectStats);
    }

    private void expectStats(
        int nNonZeroMaster,
        int nNonZeroReplica,
        ExpectStats expectStats) {

        for (int i = 0; i < repEnvInfo.length; i += 1) {

            EnvironmentStats stats =
                repEnvInfo[i].getEnv().getStats(StatsConfig.CLEAR);

            try {
                if (expectStats != null) {
                    expectStats.check(stats, i == 0);
                }

                expectNonZeroStats(
                    stats,
                    (i == 0) ? nNonZeroMaster : nNonZeroReplica);

            } catch (final Throwable e) {
                System.out.println("master: " + (i == 0) + "\n" + stats);
                throw e;
            }
        }
    }

    private void expectMasterStats(int nNonZero, ExpectStats expectStats) {

        for (int i = 0; i < repEnvInfo.length; i += 1) {

            EnvironmentStats stats =
                repEnvInfo[i].getEnv().getStats(StatsConfig.CLEAR);

            try {
                if (i == 0) {
                    if (expectStats != null) {
                        expectStats.check(stats, true);
                    }
                    expectNonZeroStats(stats, nNonZero);
                } else {
                    expectNonZeroStats(stats, 0);
                }
            } catch (final Throwable e) {
                System.out.println("master: " + (i == 0) + "\n" + stats);
                throw e;
            }
        }
    }

    private void expectNonZeroStats(EnvironmentStats stats, int nNonZero) {
        for (int i = 0;; i += 1) {
            long val;
            switch (i) {
            case 0:
                val = stats.getPriSearchOps();
                break;
            case 1:
                val = stats.getPriSearchFailOps();
                break;
            case 2:
                val = stats.getPriPositionOps();
                break;
            case 3:
                val = stats.getPriInsertOps();
                break;
            case 4:
                val = stats.getPriInsertFailOps();
                break;
            case 5:
                val = stats.getPriUpdateOps();
                break;
            case 6:
                val = stats.getPriDeleteOps();
                break;
            case 7:
                val = stats.getPriDeleteFailOps();
                break;
            case 8:
                val = stats.getSecSearchOps();
                break;
            case 9:
                val = stats.getSecSearchFailOps();
                break;
            case 10:
                val = stats.getSecPositionOps();
                break;
            case 11:
                val = stats.getSecInsertOps();
                break;
            case 12:
                val = stats.getSecUpdateOps();
                break;
            case 13:
                val = stats.getSecDeleteOps();
                break;
            default:
                assertEquals(0, nNonZero);
                return;
            }

            if (val == 0) {
                continue;
            }

            nNonZero -= 1;

            assertTrue(nNonZero >= 0);
        }
    }
}
