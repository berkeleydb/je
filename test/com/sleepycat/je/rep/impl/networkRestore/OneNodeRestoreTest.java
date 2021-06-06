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

package com.sleepycat.je.rep.impl.networkRestore;

import static com.sleepycat.je.rep.ReplicatedEnvironment.State.MASTER;
import static com.sleepycat.je.rep.ReplicatedEnvironment.State.REPLICA;
import static com.sleepycat.je.rep.ReplicatedEnvironment.State.UNKNOWN;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.Before;
import org.junit.Test;

import com.sleepycat.je.rep.InsufficientLogException;
import com.sleepycat.je.rep.NetworkRestore;
import com.sleepycat.je.rep.NetworkRestoreConfig;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicatedEnvironment.State;
import com.sleepycat.je.rep.ReplicationConfig;
import com.sleepycat.je.rep.StateChangeEvent;
import com.sleepycat.je.rep.StateChangeListener;
import com.sleepycat.je.rep.impl.RepParams;
import com.sleepycat.je.rep.impl.RepTestBase;
import com.sleepycat.je.rep.utilint.RepTestUtils;
import com.sleepycat.je.rep.utilint.RepTestUtils.RepEnvInfo;

public class OneNodeRestoreTest extends RepTestBase {
    
    @Before
    public void setUp() 
        throws Exception {
        
        groupSize = 3;
        super.setUp();
    }

    /**
     * Tests the ability to restore an entire group by manually restoring the
     * files of just one node, and then letting the other nodes do a Network
     * Restore from the one node, even when that first node is in no better
     * than the "UNKNOWN" state.  Forces the two dependent nodes to come up
     * somewhat in parallel, so that no election is completed until all have
     * finished the NR.
     */
    @Test
    public void testBasic() throws Exception {
        createGroup();
        final RepEnvInfo helper = repEnvInfo[0];
        closeNodes(repEnvInfo);
        RepTestUtils.removeRepDirs(repEnvInfo[1], repEnvInfo[2]);

        /*
         * The timeout could be made even quicker, which would make this test
         * run a bit faster, after bug #21427 is fixed.
         */
        final ReplicationConfig conf = helper.getRepConfig();
        conf.setConfigParam(RepParams.ENV_UNKNOWN_STATE_TIMEOUT.getName(),
                            "5 s");
        helper.openEnv();

        final CyclicBarrier barrier = new CyclicBarrier(2);
        class NodeStarter implements Runnable {
            private Throwable exception;
            private final RepEnvInfo node;
            
            NodeStarter(RepEnvInfo node) {
                this.node = node;
            }
            
            public void run() {
                try {
                    try {
                        node.openEnv();
                        fail("expected failure with no env dir contents");
                    } catch (InsufficientLogException ile) {
                        NetworkRestore nr = new NetworkRestore();
                        NetworkRestoreConfig config =
                            new NetworkRestoreConfig();
                        nr.execute(ile, config);
                        barrier.await();
                        node.openEnv();
                    }
                } catch (Throwable t) {
                    t.printStackTrace();
                    exception = t;
                }
            }

            Throwable getException() {
                return exception;
            }
        }

        Thread[] threads = new Thread[2];
        NodeStarter[] starters = new NodeStarter[2];
        for (int i = 0; i < 2; i++) {
            starters[i] = new NodeStarter(repEnvInfo[i+1]);
            threads[i] = new Thread(starters[i]);
            threads[i].start();
        }

        for (int i = 0; i < 2; i++) {
            threads[i].join(20000);
            assertFalse(threads[i].isAlive());
            assertNull(starters[i].getException());
        }

        boolean elected = false;
        final long deadline = System.currentTimeMillis() + 20000;
        outer: while (System.currentTimeMillis() < deadline) {
            for (RepEnvInfo rei : repEnvInfo) {
                ReplicatedEnvironment e = rei.getEnv();
                State s = e.getState();
                if (s.isMaster()) {
                    elected = true;
                    break outer;
                }
            }
            Thread.sleep(100);
        }
        assertTrue(elected);
    }

    /**
     * Tests the 1-node restore in a scenario where a master is elected before
     * all nodes have completed the Network Restore.
     *
     * @see #testBasic
     */
    @Test
    public void testWithElection() throws Exception {
        createGroup();
        final RepEnvInfo helper = repEnvInfo[0];
        closeNodes(repEnvInfo);
        RepTestUtils.removeRepDirs(repEnvInfo[1], repEnvInfo[2]);
        final ReplicationConfig conf = helper.getRepConfig();
        conf.setConfigParam(RepParams.ENV_UNKNOWN_STATE_TIMEOUT.getName(),
                            "5 s");
        helper.openEnv();

        /* Trip the latch when the helper exits "unknown" state. */
        final CountDownLatch latch = new CountDownLatch(1);
        ReplicatedEnvironment helperEnv = helper.getEnv();
        helperEnv.setStateChangeListener(new StateChangeListener() {
                public void stateChange(StateChangeEvent event) {
                    if (!UNKNOWN.equals(event.getState())) {
                        latch.countDown();
                    }
                }
            });

        /*
         * Since we haven't set an unknown-state timeout on Node2, we know that
         * successful completion indicates that it reached either MASTER or
         * REPLICA state, which means an election must have been successfully
         * completed.
         */
        assertTrue("expected failure with no env dir contents",
                   openWithNR(repEnvInfo[1]));

        State state = repEnvInfo[1].getEnv().getState();
        assertTrue(MASTER.equals(state) || REPLICA.equals(state));
        State other = state == MASTER ? REPLICA : MASTER;
        latch.await(1, TimeUnit.MINUTES);
        assertSame(other, helperEnv.getState());

        /* Now that there's a master, opening Node3 here won't need a NR. */
        repEnvInfo[2].openEnv();
    }

    @Test
    public void testIndirectHelper() throws Exception {
        createGroup();
        final RepEnvInfo survivor = repEnvInfo[0];
        closeNodes(repEnvInfo);
        RepTestUtils.removeRepDirs(repEnvInfo[1], repEnvInfo[2]);

        /*
         * Make the helper hosts setting more interesting: Node3 will now
         * "point to" Node2 (instead of Node1 as originally).  And Node2 will
         * point to both Node1 and Node3.  In a moment, we'll start just Node2
         * and Node3, both of which have lost their env dirs.  Both will try to
         * ask the other for Group info, but of course both should find no
         * useful info.  Finally, we'll restart Node1: Node2 will be able to
         * restore, and Node1 and Node2 will elect a master, and then finally
         * Node3 will be able to restore.
         */
        String hp1 = repEnvInfo[0].getRepConfig().getNodeHostPort();
        String hp2 = repEnvInfo[1].getRepConfig().getNodeHostPort();
        String hp3 = repEnvInfo[2].getRepConfig().getNodeHostPort();
        repEnvInfo[1].getRepConfig().setHelperHosts(hp1 + "," + hp3);
        repEnvInfo[2].getRepConfig().setHelperHosts(hp2);

        /*
         * Start two nodes in background threads, and verify that no master is
         * elected immediately.
         */ 
        ExecutorService threads = Executors.newFixedThreadPool(2);
        class Opener implements Runnable {
            int index;
            Opener(int i) { index = i; }
            public void run() {
                openWithNR(repEnvInfo[index]);
            }
        }
        try {
            Future<?> f2 = threads.submit(new Opener(1));
            Future<?> f3 = threads.submit(new Opener(2));
            try {
                f2.get(10, TimeUnit.SECONDS);
                fail("Node2 start wasn't expected to complete yet");
            } catch (TimeoutException te) {
                // expected
            }
            try {
                /* Already waited 10 seconds above. */
                f3.get(1, TimeUnit.MILLISECONDS);
                fail("Node3 start wasn't expected to complete yet");
            } catch (TimeoutException te) {
                // expected
            }

            /*
             * Restart the one node that still has env dir, and then everyone
             * should finally be able to complete startup.
             */
            survivor.openEnv();
            f2.get();
            f3.get();
        } finally {
            threads.shutdownNow();
        }
    }

    /**
     * @return true if we needed NetworkRestore; false if plain open was OK.
     */ 
    private boolean openWithNR(RepEnvInfo rei) {
        try {
            rei.openEnv();
            return false;
        } catch (InsufficientLogException ile) {
            NetworkRestore nr = new NetworkRestore();
            NetworkRestoreConfig config = new NetworkRestoreConfig();
            nr.execute(ile, config);
            rei.openEnv();
            return true;
        }
    }
}
