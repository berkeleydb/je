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

import java.io.File;

import org.junit.Before;
import org.junit.Test;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Put;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.tree.Key.DumpType;
import com.sleepycat.je.util.DualTestCase;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.util.test.SharedTestUtils;

public class SplitTest extends DualTestCase {
    private static final boolean DEBUG = false;

    private final File envHome;
    private Environment env;
    private Database db;

    public SplitTest() {
        envHome = SharedTestUtils.getTestDir();
    }

    @Before
    public void setUp()
        throws Exception {

        super.setUp();
    }

    private void open(int nodeMaxEntries)
        throws DatabaseException {

        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setConfigParam(
            EnvironmentConfig.NODE_MAX_ENTRIES,
            String.valueOf(nodeMaxEntries));
        envConfig.setTransactional(true);
        envConfig.setAllowCreate(true);
        envConfig.setDurability(Durability.COMMIT_NO_SYNC);
        env = create(envHome, envConfig);

        String databaseName = "testDb";
        Transaction txn = env.beginTransaction(null, null);
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(true);
        dbConfig.setAllowCreate(true);
        dbConfig.setNodeMaxEntries(nodeMaxEntries);
        db = env.openDatabase(txn, databaseName, dbConfig);
        txn.commit();
    }

    private void close() {
        db.close();
        close(env);
    }

    /**
     * Test splits on a case where the 0th entry gets promoted.
     */
    @Test
    public void test0Split()
        throws Exception {

        open(4);

        Key.DUMP_TYPE = DumpType.BINARY;
        try {
            /* Build up a tree. */
            for (int i = 160; i > 0; i-= 10) {
                assertEquals(OperationStatus.SUCCESS,
                             db.put(null, new DatabaseEntry
                                 (new byte[] { (byte) i }),
                                    new DatabaseEntry(new byte[] {1})));
            }

            if (DEBUG) {
                System.out.println("<dump>");
                DbInternal.getDbImpl(db).getTree().dump();
            }

            assertEquals(OperationStatus.SUCCESS,
                         db.put(null, new DatabaseEntry(new byte[]{(byte)151}),
                                new DatabaseEntry(new byte[] {1})));
            assertEquals(OperationStatus.SUCCESS,
                         db.put(null, new DatabaseEntry(new byte[]{(byte)152}),
                                new DatabaseEntry(new byte[] {1})));
            assertEquals(OperationStatus.SUCCESS,
                         db.put(null, new DatabaseEntry(new byte[]{(byte)153}),
                                new DatabaseEntry(new byte[] {1})));

            if (DEBUG) {
                DbInternal.getDbImpl(db).getTree().dump();
                System.out.println("</dump>");
            }

            /*
             * These inserts make a tree where the right most mid-level IN
             * has an idkey greater than its parent entry.
             *
             *     +---------------+
             *     | id = 90       |
             *     | 50 | 90 | 130 |
             *     +---------------+
             *       |     |    |
             *                  |
             *              +-----------------+
             *              | id = 160        |
             *              | 130 | 150 | 152 |
             *              +-----------------+
             *                 |      |    |
             *                 |      |    +-----------+
             *                 |      |                |
             *       +-----------+  +-----------+ +-----------------+
             *       | BIN       |  | BIN       | | BIN             |
             *       | id = 130  |  | id = 150  | | id=160          |
             *       | 130 | 140 |  | 150 | 151 | | 152 | 153 | 160 |
             *       +-----------+  +-----------+ +-----------------+
             *
             * Now delete records 130 and 140 to empty out the subtree with BIN
             * with id=130.
             */
            assertEquals(OperationStatus.SUCCESS,
                         db.delete(null,
                                   new DatabaseEntry(new byte[]{(byte) 130})));
            assertEquals(OperationStatus.SUCCESS,
                         db.delete(null,
                                   new DatabaseEntry(new byte[]{(byte) 140})));
            env.compress();

            /*
             * These deletes make the mid level IN's 0th entry > its parent
             * reference.
             *
             *     +---------------+
             *     | id = 90       |
             *     | 50 | 90 | 130 |
             *     +---------------+
             *       |     |    |
             *                  |
             *              +-----------+
             *              | id = 160  |
             *              | 150 | 152 |
             *              +-----------+
             *                 |      |
             *                 |      |
             *                 |      |
             *       +-----------+ +-----------------+
             *       | BIN       | | BIN             |
             *       | id = 150  | | id=160          |
             *       | 150 | 151 | | 152 | 153 | 160 |
             *       +-----------+ +-----------------+
             *
             * Now insert 140 into BIN (id = 150) so that its first entry is
             * less than the mid level IN.
             */
            assertEquals(OperationStatus.SUCCESS,
                         db.put(null, new DatabaseEntry(new byte[]{(byte)140}),
                                new DatabaseEntry(new byte[] {1})));

            /*
             * Now note that the mid level tree's 0th entry is greater than its
             * reference in the root.
             *
             *     +---------------+
             *     | id = 90       |
             *     | 50 | 90 | 130 |
             *     +---------------+
             *       |     |    |
             *                  |
             *              +-----------+
             *              | id = 160  |
             *              | 150 | 152 |
             *              +-----------+
             *                 |      |
             *                 |      |
             *                 |      |
             *   +----------------+ +-----------------+
             *   | BIN            | | BIN             |
             *   | id = 150       | | id=160          |
             *   | 140 |150 | 151 | | 152 | 153 | 160 |
             *   +----------------+ +-----------------+
             *
             * Now split the mid level node, putting the new child on the left.
             */
            for (int i = 154; i < 159; i++) {
                assertEquals(OperationStatus.SUCCESS,
                             db.put(null,
                                    new DatabaseEntry(new byte[]{(byte)i}),
                                    new DatabaseEntry(new byte[] {1})));
            }

            /*
             * This used to result in the following broken tree, which would
             * cause us to not be able to retrieve record 140. With the new
             * split code, entry "150" in the root should stay 130.
             *
             *     +---------------------+
             *     | id = 90             |
             *     | 50 | 90 | 150 | 154 |  NOTE: we'v lost record 140
             *     +---------------------+
             *       |     |    |        \
             *                  |         \
             *              +-----------+  +----------+
             *              | id = 150  |  |id=160    |
             *              | 150 | 152 |  |154 | 156 |
             *              +-----------+  +----------+
             *                 |      |
             *                 |      |
             *                 |      |
             *   +------------+ +-------+
             *   | BIN        | | BIN   |
             *   | id = 150   | | id=152|
             *   | 140|150|151| |152|153|
             *   +------------+ +-------+
             */
            DatabaseEntry data = new DatabaseEntry();
            assertEquals(OperationStatus.SUCCESS,
                         db.get(null, new DatabaseEntry(new byte[]
                             { (byte) 140 }),
                                data, LockMode.DEFAULT));
            close();

        } catch (Throwable t) {
            t.printStackTrace();
            throw new Exception(t);
        }
    }

    /**
     * Tests a fix to a bug [#24917] where splits could fail with
     * ArrayIndexOutOfBoundsException when reducing the fanout. It happens
     * when a node is full before reducing the fanout, the full node is on the
     * leftmost side of the Btree, and then a record is inserted in the
     * leftmost position of the node (a key value lower than any other key).
     *
     * This was never noticed until BINs occasionally started growing beyond
     * their max size due to TTL-related changes to compression. However, it
     * could have also failed when the user reduced the fanout for an existing
     * database, and that's the scenario that is tested here.
     */
    @Test
    public void testSplitOverSizedNode() {

        /* DB initially has a 156 fanout. */
        open(256);

        final DatabaseEntry key = new DatabaseEntry();
        final DatabaseEntry data = new DatabaseEntry(new byte[100]);

        /* Fill BIN with 256 records. */
        for (int i = 1000; i < 1256; i += 1) {
            IntegerBinding.intToEntry(i, key);
            assertNotNull(db.put(null, key, data, Put.NO_OVERWRITE, null));
        }

        /*
         * Change DB fanout to 128. The extra open/close is needed because the
         * first time the fanout is changed it isn't applied in cache, due to a
         * bug.
         */
        close();
        open(128);
        close();
        open(128);

        /*
         * Insert records at the beginning of the BIN. The first insertion will
         * cause a split where the existing node holds only one record and the
         * new sibling holds the other 255 entries. The bug was that the new
         * sibling was sized according to the new fanout (128), and would not
         * hold the 255 entries.
         */
        for (int i = 999; i >= 0; i -= 1) {
            IntegerBinding.intToEntry(i, key);
            assertNotNull(db.put(null, key, data, Put.NO_OVERWRITE, null));
        }

        close();
    }
}
