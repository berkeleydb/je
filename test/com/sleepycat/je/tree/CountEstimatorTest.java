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

package com.sleepycat.je.tree;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.File;

import org.junit.After;
import org.junit.Test;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.persist.EntityCursor;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.SecondaryIndex;
import com.sleepycat.persist.StoreConfig;
import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;
import com.sleepycat.persist.model.Relationship;
import com.sleepycat.persist.model.SecondaryKey;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

public class CountEstimatorTest extends TestBase {

    private static final boolean DEBUG = false;

    private final File envHome;
    private Environment env;
    private Database db;

    public CountEstimatorTest() {
        envHome = SharedTestUtils.getTestDir();
    }

    @After
    public void tearDown() {
        try {
            if (env != null) {
                env.close();
            }
        } catch (Throwable e) {
            System.out.println("during tearDown: " + e);
        }
    }

    private void openEnv() {
        final EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setAllowCreate(true);
        env = new Environment(envHome, envConfig);

        final DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setSortedDuplicates(true);
        db = env.openDatabase(null, "foo", dbConfig);
    }

    private void closeEnv() {
        if (db != null) {
            db.close();
            db = null;
        }
        if (env != null) {
            env.close();
            env = null;
        }
    }

    /**
     * When insertions are sequential, the estimate is always correct because
     * all INs (except on the right edge of the btree) have the same number of
     * entries.
     */
    @Test
    public void testDupsInsertSequential() {

        openEnv();

        final DatabaseEntry key = new DatabaseEntry();
        final DatabaseEntry data = new DatabaseEntry();

        final int[] nDupsArray =
            {1, 2, 3, 20, 50, 500, 5000, 500, 50, 20, 3, 2, 1};

        long total = 0;
        for (int i = 0; i < nDupsArray.length; i += 1) {
            final int nDups = nDupsArray[i];
            IntegerBinding.intToEntry(i, key);
            for (int j = 0; j < nDups; j += 1) {
                IntegerBinding.intToEntry(j, data);
                final OperationStatus status = db.put(null, key, data);
                assertSame(OperationStatus.SUCCESS, status);
            }
            total += nDups;
            assertEquals(total, db.count());
        }

        final Cursor cursor = db.openCursor(null, null);
        for (int i = 0; i < nDupsArray.length; i += 1) {
            final int nDups = nDupsArray[i];
            IntegerBinding.intToEntry(i, key);
            final OperationStatus status =
                cursor.getSearchKey(key, data, null);
            assertSame(OperationStatus.SUCCESS, status);
            assertEquals(0, IntegerBinding.entryToInt(data));
            assertEquals(nDups, cursor.count());
            assertEquals(nDups, cursor.countEstimate());
        }
        cursor.close();

        closeEnv();
    }

    /**
     * When insertions are non-sequential, the estimate may be off by a factor
     * of two.  The estimate is accurate, however, for counts less than the
     * nodeMax setting.
     */
    @Test
    public void testDupsInsertNonSequential() {

        openEnv();

        final DatabaseEntry key = new DatabaseEntry();
        final DatabaseEntry data = new DatabaseEntry();

        final int[] nDupsArray =
            {1, 2, 3, 20, 50, 500, 5000, 500, 50, 20, 3, 2, 1};

        long total = 0;
        for (int j = 0;; j += 1) {
            boolean wroteOne = false;
            IntegerBinding.intToEntry(j, data);
            for (int i = 0; i < nDupsArray.length; i += 1) {
                final int nDups = nDupsArray[i];
                if (j < nDups) {
                    IntegerBinding.intToEntry(i, key);
                    final OperationStatus status = db.put(null, key, data);
                    assertSame(OperationStatus.SUCCESS, status);
                    wroteOne = true;
                    total += 1;
                }
            }
            if (!wroteOne) {
                break;
            }
            assertEquals(total, db.count());
        }

        final Cursor cursor = db.openCursor(null, null);
        for (int i = 0; i < nDupsArray.length; i += 1) {
            final int nDups = nDupsArray[i];
            IntegerBinding.intToEntry(i, key);
            final OperationStatus status =
                cursor.getSearchKey(key, data, null);
            assertSame(OperationStatus.SUCCESS, status);
            assertEquals(0, IntegerBinding.entryToInt(data));
            assertEquals(nDups, cursor.count());
            if (nDups <= 100) {
                assertEquals(nDups, cursor.countEstimate());
            } else {
                final long est = cursor.countEstimate();
                assertTrue(est > nDups / 2 && est < nDups * 2);
            }
            /*
            System.out.println("nDups=" + nDups + " countEstimate=" +
                               cursor.countEstimate());
            */
        }
        cursor.close();

        closeEnv();
    }

    @Entity
    static class MyEntity {
        @PrimaryKey
        int id;
        @SecondaryKey(relate=Relationship.MANY_TO_ONE)
        int dept;
    }

    /**
     * EntityIndex.countEstimate simply forwards to Cursor.countEstimate, but
     * we should still call it once.
     */
    @Test
    public void testDPL() {
        openEnv();

        final EntityStore store = new EntityStore
            (env, "foo", new StoreConfig().setAllowCreate(true));

        final PrimaryIndex<Integer, MyEntity> priIndex =
            store.getPrimaryIndex(Integer.class, MyEntity.class);

        final SecondaryIndex<Integer, Integer, MyEntity> secIndex =
            store.getSecondaryIndex(priIndex, Integer.class, "dept");

        MyEntity entity = new MyEntity();
        entity.id = 1;
        entity.dept = 9;
        priIndex.put(entity);

        entity = new MyEntity();
        entity.id = 2;
        entity.dept = 9;
        priIndex.put(entity);

        EntityCursor c = secIndex.entities();
        assertNotNull(c.next());

        assertEquals(2, c.count());
        assertEquals(2, c.countEstimate());

        c.close();
        store.close();
        closeEnv();
    }
}
