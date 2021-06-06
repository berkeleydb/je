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

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.je.CacheMode;
import com.sleepycat.je.CheckpointConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.tree.BIN;
import com.sleepycat.je.tree.Tree;
import com.sleepycat.je.util.TestUtils;
import org.junit.Test;

public class CheckBINDeltaTest extends CheckBase {

    private static final String DB_NAME = "simpleDB";
    private static final boolean DEBUG = false;

    /**
     * SR #11123
     * Make sure that BIN-deltas are applied only to non-deleted nodes.
     */
    @Test
    public void testBINDelta()
        throws Throwable {

        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        turnOffEnvDaemons(envConfig);
        envConfig.setConfigParam(EnvironmentParams.NODE_MAX.getName(), "4");
        envConfig.setConfigParam(EnvironmentParams.BIN_DELTA_PERCENT.getName(),
                                 "75");
        envConfig.setAllowCreate(true);

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);

        EnvironmentConfig restartConfig = TestUtils.initEnvConfig();
        turnOffEnvDaemons(restartConfig);
        envConfig.setConfigParam(EnvironmentParams.NODE_MAX.getName(), "4");

        testOneCase(DB_NAME, envConfig, dbConfig,
                    new TestGenerator() {
                        void generateData(Database db)
                            throws DatabaseException {

                            addData(db);
                        }
                    },
                    restartConfig,
                    new DatabaseConfig());
    }

    /**
     * This test checks for the bug described in SR11123.  If an IN and its
     * child-subtree is deleted, an INDeleteInfo is written to the
     * log.  If there is a BIN-delta in the log for a BIN-child of the
     * removed subtree (i.e. compressed), then recovery will apply it to the
     * compressed IN.  Since the IN has no data in * it, that is not
     * necessarily a problem.  However, reinstantiating the obsolete IN
     * may cause a parent IN to split which is not allowed during IN
     * recovery.
     *
     * Here's the case:
     *
     *           |
     *          IN1
     *      +---------------------------------+
     *      |                                 |
     *     IN2                               IN6
     *   /   |                            /    |     \
     * BIN3 BIN4                      BIN7   BIN8   BIN9
     *
     * IN2 and the subtree below are compressed away. During recovery
     * replay, after the pass where INs and INDeleteINfos are
     * processed, the in-memory tree looks like this:
     *
     *                         IN1
     *                          |
     *                         IN6
     *                     /    |     \
     *                  BIN7   BIN8   BIN9
     *
     * However, let's assume that BIN-deltas were written for
     * BIN3, BIN4, BIN5 within the recovery part of the log, before the
     * subtree was compressed.  We'll replay those BIN-deltas in the
     * following pass, and in the faulty implementation, they cause
     * the ghosts of BIN3, BIN4 to be resurrected and applied to
     * IN6. Let's assume that the max node size is 4 -- we won't be
     * able to connect BIN3, BIN4 because IN6 doesn't have the
     * capacity, and we don't expect to have to do splits.
     */
    private void addData(Database db)
        throws DatabaseException {

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();

        /* Populate a tree so there are 3 levels. */
        for (int i = 0; i < 140; i += 10) {
            IntegerBinding.intToEntry(i, key);
            IntegerBinding.intToEntry(i, data);
            assertEquals(OperationStatus.SUCCESS, db.put(null, key, data));
        }

        CheckpointConfig ckptConfig = new CheckpointConfig();
        ckptConfig.setForce(true);
        env.checkpoint(ckptConfig);

        Tree tree = DbInternal.getDbImpl(db).getTree();
        com.sleepycat.je.tree.Key.DUMP_TYPE =
            com.sleepycat.je.tree.Key.DumpType.BINARY;
        if (DEBUG) {
            tree.dump();
        }

        /*
         * Update a key on the BIN3 and a key on BIN4, to create reason for
         * a BIN-delta. Force a BIN-delta for BIN3 and BIN4 out to the log.
         */
        IntegerBinding.intToEntry(0, key);
        IntegerBinding.intToEntry(100, data);
        assertEquals(OperationStatus.SUCCESS, db.put(null, key, data));
        IntegerBinding.intToEntry(20, key);
        assertEquals(OperationStatus.SUCCESS, db.put(null, key, data));

        BIN bin = tree.getFirstNode(CacheMode.DEFAULT);
        bin.log(true, false, false, null);
        bin = tree.getNextBin(bin, CacheMode.DEFAULT);
        bin.log(true, false, false, null);
        bin.releaseLatch();

        /*
         * Delete all of left hand side of the tree, so that the subtree root
         * headed by IN2 is compressed.
         */
        for (int i = 0; i < 50; i+=10) {
            IntegerBinding.intToEntry(i, key);
            assertEquals(OperationStatus.SUCCESS, db.delete(null, key));
        }

        /* force a compression */
        env.compress();
        if (DEBUG) {
            tree.dump();
        }
    }
}
