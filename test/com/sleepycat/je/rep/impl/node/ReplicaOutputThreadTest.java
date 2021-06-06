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

package com.sleepycat.je.rep.impl.node;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Durability.ReplicaAckPolicy;
import com.sleepycat.je.Durability.SyncPolicy;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicatedEnvironment.State;
import com.sleepycat.je.rep.impl.RepParams;
import com.sleepycat.je.rep.impl.RepTestBase;
import com.sleepycat.je.rep.utilint.RepTestUtils;
import com.sleepycat.je.utilint.TestHook;

/**
 *
 * Tests associated with the ReplicaOutputThread.
 */
public class ReplicaOutputThreadTest extends RepTestBase {

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
    }

    /**
     * The test simulates network stalls via a hook and verifies that
     * responses accumulate in the output queue and are correctly processed
     * when the network stall clears out.
     *
     * @throws InterruptedException
     */
    @Test
    public void testWithGroupAckDisabled() throws InterruptedException {
        internalTest(false);
    }

    @Test
    public void testWithGroupAckEnabled() throws InterruptedException {
        internalTest(true);
    }

    public void internalTest(Boolean groupacksEnabled) throws InterruptedException {

        RepTestUtils.setConfigParam(RepParams.ENABLE_GROUP_ACKS,
                                   groupacksEnabled.toString(), repEnvInfo);
        /* Increase the client timeout since the test throttles acks. */
        RepTestUtils.setConfigParam(RepParams.REPLICA_ACK_TIMEOUT,
                                    "60 s", repEnvInfo);
        createGroup(2);

        ReplicatedEnvironment menv = repEnvInfo[0].getEnv();
        assertEquals(menv.getState(), State.MASTER);

        populateDB(menv, 100);

        final RepNode replica = repEnvInfo[1].getRepNode();
        final ReplicaOutputThread rot =
            replica.getReplica().getReplicaOutputThread();

        int nThreads = 10;
        OutputHook outputHook = new OutputHook(nThreads);
        rot.setOutputHook(outputHook);

        /* Start parallel inserts into the database. */
        final InsertThread ithreads[] = new InsertThread[nThreads];
        for (int i=0; i < nThreads; i++) {
            ithreads[i] = new InsertThread(menv);
            ithreads[i].start();
        }

        /* Wait for parallel inserts to finish or fail. */
        for (int i=0; i < nThreads; i++) {
            final InsertThread t = ithreads[i];
            t.join(60000);
            assertTrue(! t.isAlive());
            assertNoException(t.e);
        }

        assertNoException(outputHook.e);

        assertTrue(outputHook.done);

        if (groupacksEnabled) {
            assertTrue(rot.getNumGroupedAcks() > 0);
        } else {
            assertEquals(0, rot.getNumGroupedAcks());
        }
    }

    private void assertNoException(Exception e) {
        assertTrue(((e != null) ? e.getMessage() : ""),  e == null);
    }

    /* Sequence used to generate keys in the thread below. */
    static final AtomicInteger keyGen = new AtomicInteger(0);

    /**
     * Thread use to do parallel inserts. Each thread will block on the commit
     * waiting for an ack, until the test hook lets them through.
     */
    private class InsertThread extends Thread {

        final ReplicatedEnvironment env;
        Exception e;

        InsertThread(ReplicatedEnvironment env) {
            super();
            this.env = env;
        }

        @Override
        public void run() {
            Database db = null;

            final TransactionConfig txnConfig = new TransactionConfig();
            txnConfig.setDurability(new Durability(SyncPolicy.NO_SYNC,
                                                  SyncPolicy.NO_SYNC,
                                                  ReplicaAckPolicy.ALL));
            Transaction txn = env.beginTransaction(null, txnConfig);
            try {
                db = env.openDatabase(txn,TEST_DB_NAME, dbconfig);
                DatabaseEntry k = new DatabaseEntry();
                IntegerBinding.intToEntry(keyGen.incrementAndGet(), k);
                db.put(txn, k, k);
                /* Wait for acks here. */
                txn.commit();
                txn = null;
            } catch (Exception ie) {
                this.e = ie;
            } finally {
                if (txn != null) {
                    txn.abort();
                }
                if (db != null) {
                    db.close();
                }
            }
        }
    }

    /*
     * Test hook inserted into ReplicaOutputThread to wait until desired
     * number of acks have queued up after which it removes the stall and lets
     * the responses go out on to the network.
     */
    private class OutputHook implements TestHook<Object> {

        final int queueSize;
        volatile boolean done = false ;
        volatile Exception e;

        OutputHook(int queueSize) {
            this.queueSize = queueSize;
        }

        @Override
        public void hookSetup() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void doIOHook() throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void doHook() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void doHook(final Object rot) {

            try {
                Callable<Boolean> predicate = new Callable<Boolean>() {

                    @SuppressWarnings("unchecked")
                    @Override
                    public Boolean call() throws Exception {
                        /*
                         * -1 because the hook is called after an ack is
                         * removed from the queue.
                         */
                       return (((ReplicaOutputThread) rot).getOutputQueueSize()) >=
                              (queueSize - 1);
                    }

                };

                RepTestUtils.awaitCondition(predicate, 60000);

            } catch (Exception ie) {
                e = ie;
            } finally {
                done = true;
            }
        }

        @Override
        public Object getHookValue() {
            throw new UnsupportedOperationException();
        }
    }

}
