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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.After;
import org.junit.Test;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.je.CacheMode;
import com.sleepycat.je.CheckpointConfig;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.tree.BIN;
import com.sleepycat.je.tree.IN;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.je.utilint.Pair;
import com.sleepycat.je.utilint.TestHook;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

/**
 * Reproduces a problem to do with checkpointing and splits, and tests a fix.
 * The problem was introduced with the off-heap implementation, although JE was
 * never shipped with this bug.
 *
 * With the off-heap cache, when iterating over the INList to create the
 * checkpoint dirty set, we must select dirty BINs via their parent rather
 * than when they're encountered during the iteration. (This is explained in
 * DirtyINMap.selectForCheckpoint.) When a level 2 is split, the references to
 * half of the BINs go into the new sibling. Those dirty BINs weren't being
 * counted if the split happened in the middle of the INList iteration and the
 * new sibling did not happen to be iterated (ConcurrentHashMap doesn't
 * guarantee that newly added elements will be seen by an in-progress
 * iteration). The fix is to add the dirty BINs to the dirty set during the
 * split via the DirtyINMap.coordinateSplitWithCheckpoint method.
 */
public class Level2SplitBugTest extends TestBase {

    private static final int NODE_MAX = 30;
    private static final int N_RECORDS = 10 * NODE_MAX * NODE_MAX;

    private final File envHome;
    private Environment env;
    private Database db;

    public Level2SplitBugTest() {
        envHome = SharedTestUtils.getTestDir();
    }

    @After
    public void tearDown() {
        try {
            if (env != null) {
                env.close();
            }
        } finally {
            env = null;
            db = null;
        }
    }

    private void open() {

        final EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setAllowCreate(true);
        envConfig.setConfigParam(
            EnvironmentConfig.NODE_MAX_ENTRIES, String.valueOf(NODE_MAX));
        envConfig.setConfigParam(
            EnvironmentConfig.ENV_RUN_EVICTOR, "false");
        envConfig.setConfigParam(
            EnvironmentConfig.ENV_RUN_OFFHEAP_EVICTOR, "false");
        envConfig.setConfigParam(
            EnvironmentConfig.ENV_RUN_CLEANER, "false");
        envConfig.setConfigParam(
            EnvironmentConfig.ENV_RUN_CHECKPOINTER, "false");
        envConfig.setConfigParam(
            EnvironmentConfig.ENV_RUN_IN_COMPRESSOR, "false");
        env = new Environment(envHome, envConfig);

        final DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        db = env.openDatabase(null, "foo", dbConfig);
    }

    private void close() {
        db.close();
        db = null;
        env.close();
        env = null;
    }

    private void write() {

        final DatabaseEntry key = new DatabaseEntry();

        for (int i = 0; i < N_RECORDS; i += 1) {
            IntegerBinding.intToEntry(i, key);
            final OperationStatus status = db.put(null, key, key);
            assertSame(OperationStatus.SUCCESS, status);
        }
    }

    private void verify() {

        final DatabaseEntry key = new DatabaseEntry();
        final DatabaseEntry data = new DatabaseEntry();

        int expectKey = 0;

        try (final Cursor cursor = db.openCursor(null, null)) {

            while (cursor.getNext(key, data, null) ==
                    OperationStatus.SUCCESS) {

                assertEquals(expectKey, IntegerBinding.entryToInt(key));
                assertEquals(expectKey, IntegerBinding.entryToInt(data));

                expectKey += 1;
            }
        }
    }

    @Test
    public void testLevel2SplitBug() {

        open();
        write();
        verify();

        final EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);
        final DatabaseImpl dbImpl = DbInternal.getDbImpl(db);

        write();
        final int inListSize = envImpl.getInMemoryINs().getSize();

        /*
         * After the write() call above, all BINs are dirty. Collect the level
         * 2 INs in our DB and their dirty BIN children.
         */
        final List<Pair<IN, List<BIN>>> list = new ArrayList<>();

        for (final IN in : envImpl.getInMemoryINs()) {

            if (in.getDatabase() != dbImpl) {
                continue;
            }
            if (in.getNormalizedLevel() != 2) {
                continue;
            }

            final List<BIN> dirtyBins = new ArrayList<>();
            list.add(new Pair<>(in, dirtyBins));

            for (int i = 0; i < in.getNEntries(); i += 1) {
                final BIN bin = (BIN) in.getTarget(i);
                if (bin.getDirty()) {
                    dirtyBins.add(bin);
                }
            }
        }

        assertTrue(list.size() >= 5);

        for (final Pair<IN, List<BIN>> pair : list) {
            final List<BIN> dirtyBins = pair.second();
            assertTrue(dirtyBins.size() >= 2);
        }

        final Set<IN> newSiblings = new HashSet<>();

        /*
         * Hook is called after examining each IN during dirty map creation.
         * We must do the splits after the iteration starts, so that the INs
         * added to the INList by the splits (or at least some of them) are not
         * seen in the iteration.
         *
         * When we do the splits we add all new siblings to newSiblings. The
         * INs seen by the the iteration are removed from this set. At the end,
         * the set should be non-empty for the test to be valid.
         */
        class MyHook implements TestHook<IN> {
            int nIters = 0;
            boolean didSplits = false;

            public void doHook(IN inIterated) {

                nIters += 1;

                if (didSplits) {
                    newSiblings.remove(inIterated);
                    return;
                }

                if (nIters < inListSize / 2) {
                    return;
                }

                didSplits = true;

                /*
                 * Do splits in a separate thread to attempt to make the INs
                 * added by the split NOT appear to the checkpoint's INList
                 * iteration. This has been unreliable.
                 */
                final Thread splitThread = new Thread() {
                    @Override
                    public void run() {
                        for (final Pair<IN, List<BIN>> pair : list) {

                            final IN child = pair.first();
                            child.latch(CacheMode.UNCHANGED);

                            final IN parent = child.latchParent();
                            final int index = parent.getKnownChildIndex(child);

                            assertTrue(parent.isRoot());

                            final IN newSibling = child.split(
                                parent, index, null /*grandParent*/, NODE_MAX);

                            newSiblings.add(newSibling);

                            child.releaseLatch();
                            parent.releaseLatch();
                        }
                    }
                };

                try {
                    splitThread.start();
                    splitThread.join(30 * 1000);
                    final boolean completed = !splitThread.isAlive();
                    while (splitThread.isAlive()) {
                        splitThread.interrupt();
                    }
                    assertTrue(completed);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

//                System.out.println(
//                    "newSiblings initial size=" + newSiblings.size());
            }

            /* Unused methods. */
            public void doHook() {
                throw new UnsupportedOperationException();
            }
            public IN getHookValue() {
                throw new UnsupportedOperationException();
            }
            public void doIOHook() {
                throw new UnsupportedOperationException();
            }
            public void hookSetup() {
                throw new UnsupportedOperationException();
            }
        };

        final MyHook hook = new MyHook();

        Checkpointer.examineINForCheckpointHook = hook;
        try {
            env.checkpoint(new CheckpointConfig().setForce(true));
        } finally {
            Checkpointer.examineINForCheckpointHook = null;
        }

//        System.out.println(
//            "newSiblings final size=" + newSiblings.size());

        assertTrue(hook.didSplits);

        if (newSiblings.isEmpty()) {
            System.out.println(
                "Unable to create conditions for test. ConcurrentHashMap " +
                "(INList) iteration can't be relied on to return or not " +
                "return items added during the iteration.");
            close();
            return;
        }

        /*
         * All the BINs that were dirty before the checkpoint must be non-dirty
         * now. Before the bug fix, some dirty BINs were not logged by the
         * checkpoint, and the assertion below fired.
         */
        for (final Pair<IN, List<BIN>> pair : list) {
            final List<BIN> dirtyBins = pair.second();
            for (final BIN bin : dirtyBins) {
                assertFalse(bin.getDirty());
            }
        }

        verify();
        close();

        open();
        verify();
        close();
    }
}
