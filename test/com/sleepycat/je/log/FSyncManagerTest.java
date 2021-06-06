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

package com.sleepycat.je.log;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.junit.JUnitThread;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

import org.junit.Test;

/**
 * Exercise the synchronization aspects of the sync manager.
 */
public class FSyncManagerTest extends TestBase {
    private final File envHome;

    public FSyncManagerTest() {
        super();
        envHome = SharedTestUtils.getTestDir();
    }

    @Test
    public void testBasic()
        throws Throwable{

        Environment env = null;

        try {
            EnvironmentConfig envConfig = TestUtils.initEnvConfig();
            envConfig.setConfigParam(EnvironmentConfig.LOG_FSYNC_TIMEOUT,
                                     "50000000");
            envConfig.setAllowCreate(true);
            env = new Environment(envHome, envConfig);

            WaitVal waitVal = new WaitVal(0);

            FSyncManager syncManager =
                new TestSyncManager(DbInternal.getNonNullEnvImpl(env),
                                    waitVal);
            JUnitThread t1 = new TestSyncThread(syncManager);
            JUnitThread t2 = new TestSyncThread(syncManager);
            JUnitThread t3 = new TestSyncThread(syncManager);
            t1.start();
            t2.start();
            t3.start();

            /* Wait for all threads to request a sync, so they form a group.*/
            Thread.sleep(500);

            /* Free thread 1. */
            synchronized (waitVal) {
                waitVal.value = 1;
                waitVal.notify();
            }

            t1.join();
            t2.join();
            t3.join();

            /*
             * All three threads ask for fsyncs.
             * 2 do fsyncs -- the initial leader, and the leader of the
             * waiting group of 2.
             * The last thread gets a free ride.
             */
            assertEquals(3, syncManager.getNFSyncRequests());
            assertEquals(2, syncManager.getNFSyncs());
            assertEquals(0, syncManager.getNTimeouts());
        } finally {
            if (env != null) {
                env.close();
            }
        }
    }

    /* This test class waits for an object instead of executing a sync.
     * This way, we can manipulate grouping behavior.
     */
    class TestSyncManager extends FSyncManager {
        private final WaitVal waitVal;
        TestSyncManager(EnvironmentImpl env, WaitVal waitVal) {
            super(env);
            this.waitVal = waitVal;
        }
        @Override
        protected void executeFSync() {
            try {
                synchronized (waitVal) {
                    if (waitVal.value < 1) {
                        waitVal.wait();
                    }
                }
            } catch (InterruptedException e) {
                // woken up.
            }
        }
    }

    class TestSyncThread extends JUnitThread {
        private final FSyncManager syncManager;
        TestSyncThread(FSyncManager syncManager) {
            super("syncThread");
            this.syncManager = syncManager;
        }

        @Override
        public void testBody()
            throws Throwable {
            syncManager.flushAndSync(true);
        }
    }

    class WaitVal {
        public int value;

        WaitVal(int value) {
            this.value = value;
        }
    }

    private final int SIM_THREADS = 10;
    private final int SIM_ITERS = 50;
    private final int SIM_MAX_EXECUTE_MS = 1000;

    /**
     * Simulates fsync by maintaining a map of entries that represent writes.
     * Before calling FSyncManager.fsync an entry is added to the map, and we
     * expect it to be removed from the map when fsync returns.  The overridden
     * executeFSync method removes all entries from the map.
     */
    @Test
    public void testSimulatedFsync() {

        Environment env = null;
        JUnitThread[] threads = null;
        try {
            final EnvironmentConfig envConfig = TestUtils.initEnvConfig();
            envConfig.setAllowCreate(true);
            env = new Environment(envHome, envConfig);

            final SimSyncManager syncManager =
                new SimSyncManager(DbInternal.getNonNullEnvImpl(env));

            threads = new JUnitThread[SIM_THREADS];
            for (int i = 0; i < SIM_THREADS; i += 1) {
                threads[i] = new SimSyncThread(syncManager, i);
            }
            for (int i = 0; i < SIM_THREADS; i += 1) {
                threads[i].start();
            }
            for (int i = 0; i < SIM_THREADS; i += 1) {
                threads[i].finishTest();
            }
            threads = null;
            env.close();
            env = null;
        } catch (Throwable e) {
            e.printStackTrace();
            fail(e.toString());
        } finally {
            if (threads != null) {
                for (int i = 0; i < SIM_THREADS; i += 1) {
                    final Thread thread = threads[i];
                    if (thread != null) {
                        while (thread.isAlive()) {
                            thread.interrupt();
                            Thread.yield();
                        }
                    }
                }
            }
            if (env != null) {
                try {
                    env.close();
                } catch (Throwable e) {
                    System.out.println("After failure: " + e);
                }
            }
        }
    }

    class SimSyncThread extends JUnitThread {

        private final SimSyncManager syncManager;

        SimSyncThread(SimSyncManager syncManager, int i) {
            super("SimSyncThread-" + i);
            this.syncManager = syncManager;
        }

        @Override

        public void testBody() {
            for (int i = 0; i < SIM_ITERS && !syncManager.failed(); i += 1) {
                final int entry = syncManager.addEntry();
                syncManager.flushAndSync(true);
                syncManager.expectDone(entry);
            }
        }
    }

    class SimSyncManager extends FSyncManager {

        private final Map<Integer, Integer> entries;
        private final AtomicInteger nextEntry;
        private final Random rnd;
        private volatile boolean failure;

        SimSyncManager(EnvironmentImpl env) {
            super(env);
            entries =
                Collections.synchronizedMap(new HashMap<Integer, Integer>());
            nextEntry = new AtomicInteger(1);
            rnd = new Random(123);
            failure = false;
        }

        boolean failed() {
            return failure;
        }

        int addEntry() {
            final int entry = nextEntry.getAndIncrement();
            entries.put(entry, entry);
            return entry;
        }

        void expectDone(int entry) {
            if (entries.containsKey(entry)) {
                failure = true;
                fail("found entry: " + entry);
            }
        }

        @Override
        protected void executeFSync() {
            try {
                Thread.currentThread().sleep(getNextSleepMs());
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            entries.clear();
        }

        private long getNextSleepMs() {
            synchronized (rnd) {
                return rnd.nextInt(SIM_MAX_EXECUTE_MS) + 1;
            }
        }
    }
}
