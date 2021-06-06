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

package com.sleepycat.je.rep.elections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.bind.tuple.LongBinding;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Durability.ReplicaAckPolicy;
import com.sleepycat.je.Durability.SyncPolicy;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.dbi.DbEnvPool;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.UnknownMasterException;
import com.sleepycat.je.rep.impl.RepGroupImpl;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.impl.RepTestBase;
import com.sleepycat.je.rep.impl.node.RepNode;
import com.sleepycat.je.rep.impl.node.Replica;
import com.sleepycat.je.rep.stream.Protocol;
import com.sleepycat.je.rep.utilint.BinaryProtocol.Message;
import com.sleepycat.je.rep.utilint.RepTestUtils;
import com.sleepycat.je.rep.utilint.RepTestUtils.RepEnvInfo;
import com.sleepycat.je.utilint.TestHookAdapter;

public class JoinerElectionTest extends RepTestBase {
    private static final int GROUP_SIZE = 3;
    private static final int INITIAL_NODES = 2;
    private static final int N_TXN = 100;
    private static final long WAIT_LIMIT = 60000;
    private RepImpl repImpl;
    volatile private RepNode repNode;

    @Override
    @Before
    public void setUp() throws Exception {
        groupSize = GROUP_SIZE;
        super.setUp();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        Replica.setInitialReplayHook(null);
        DbEnvPool.getInstance().setBeforeFinishInitHook(null);
        super.tearDown();
    }

    /** Reproduces 21915. */
    @Test
    public void testPartialGroupDB() throws Exception {
        createGroup(2);
        RepEnvInfo master = repEnvInfo[0];
        ReplicatedEnvironment env = master.getEnv();

        Database db = env.openDatabase(null, TEST_DB_NAME, dbconfig);
        TransactionConfig tc = new TransactionConfig();
        Durability d =
            new Durability(SyncPolicy.NO_SYNC,
                           SyncPolicy.NO_SYNC,
                           ReplicaAckPolicy.NONE);
        tc.setDurability(d);
        for (int count = 0; count < N_TXN; count++) {
            Transaction txn = env.beginTransaction(null, tc);
            IntegerBinding.intToEntry(count, key);
            LongBinding.longToEntry(count, data);
            db.put(txn, key, data);
            txn.commit();
        }
        db.close();

        CountDownLatch latch = new CountDownLatch(1);
        RepImplRetriever rir = new RepImplRetriever(latch);
        DbEnvPool.getInstance().setBeforeFinishInitHook(rir);
        Replica.setInitialReplayHook(new HalfBacklogSink(latch));
        RepEnvInfo replica = repEnvInfo[2];
        try {
            replica.openEnv();
            fail("should have failed initial open");
        } catch (EnvironmentFailureException e) {
            if (!(e.getCause() instanceof MyTestException)) {
                throw e;
            }
        }
        Replica.setInitialReplayHook(null);
        DbEnvPool.getInstance().setBeforeFinishInitHook(null);

        /*
         * Now that we've done all this preparation (getting Node3's copy of
         * the GroupDB only partially applied), start Node3 again, but without
         * being able to reach the master.  Ensure that it doesn't start an
         * election.
         */
        logger.info("preparation complete, will now open env");
        RepTestUtils.disableServices(master);
        String node2hostPort = repEnvInfo[1].getRepConfig().getNodeHostPort();
        replica.getRepConfig().setHelperHosts(node2hostPort);

        try {
            replica.openEnv();
            fail("expected UnknownMasterException");
        } catch (UnknownMasterException e) {
            // expected
        }

        assertEquals(0, repNode.getElections().getElectionCount());
    }

    class RepImplRetriever extends TestHookAdapter<EnvironmentImpl> {
        private final CountDownLatch latch;

        RepImplRetriever(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public
        void doHook(EnvironmentImpl envImpl) {
            repImpl = (RepImpl)envImpl;
            latch.countDown();
        }
    }

    class HalfBacklogSink extends TestHookAdapter<Message> {
        private final CountDownLatch latch;
        private int txnCount;

        HalfBacklogSink(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void doHook(Message m) {

            /*
             * Before our initial attempt to add Node3 to the group, the group
             * did a few initial internal/system transactions, including
             * GroupDB updates for the first two nodes, followed by 100 user
             * txns.  By counting to 100 here we expect we'll be somewhere
             * within the series of user txns (undoubtedly near the end).  The
             * idea is, we want to stop after the point where the two initial
             * nodes have been added, but before we see the addition of Node3.
             */
            if (m.getOp() == Protocol.COMMIT && ++txnCount == N_TXN) {

                // wait til we see desired group composition (happens in
                // another thread)

                // flush log, and then die

                /*
                 * Wait until repImpl has been established in user thread,
                 * though by the time we get to having seen 100 commits that
                 * should long since have happend.
                 */
                try {
                    latch.await(WAIT_LIMIT, TimeUnit.MILLISECONDS);
                } catch (Exception e) {
                    // Should never happen, and if it does test is hopeless.
                    // In fact we might as well get NPE on repImpl.
                    e.printStackTrace();
                }
                repNode = repImpl.getRepNode();

                /*
                 * Wait for the expected changes to the GroupDB to have been
                 * applied by the Replay thread.  Again, by the time we get to
                 * 100 commits that has probably already happened long ago.
                 */
                try {
                    RepTestUtils.awaitCondition(new Callable<Boolean>() {
                            public Boolean call() {
                                RepGroupImpl group =
                                    repImpl.getRepNode().getGroup();
                                if (group == null ||
                                    group.getElectableMembers().size() <
                                    INITIAL_NODES) {
                                    return false;
                                }
                                return true;
                            }
                        });
                } catch (Exception e) {
                    // Shouldn't happen (doesn't include JUnit Assertion
                    // failure).
                    e.printStackTrace();
                }
                repImpl.flushLog(false);
                throw new MyTestException();
            }
        }
    }

    @SuppressWarnings("serial")
    static class MyTestException extends DatabaseException {
        MyTestException() {
            super("testing");
        }
    }
}
