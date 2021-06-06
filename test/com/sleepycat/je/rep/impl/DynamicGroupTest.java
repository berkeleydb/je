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
package com.sleepycat.je.rep.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Before;
import org.junit.Test;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Durability.ReplicaAckPolicy;
import com.sleepycat.je.Durability.SyncPolicy;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.EnvironmentFailureReason;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.rep.InsufficientAcksException;
import com.sleepycat.je.rep.MasterStateException;
import com.sleepycat.je.rep.MemberActiveException;
import com.sleepycat.je.rep.MemberNotFoundException;
import com.sleepycat.je.rep.NodeType;
import com.sleepycat.je.rep.RepInternal;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicatedEnvironment.State;
import com.sleepycat.je.rep.ReplicationConfig;
import com.sleepycat.je.rep.UnknownMasterException;
import com.sleepycat.je.rep.impl.node.NameIdPair;
import com.sleepycat.je.rep.impl.node.RepNode;
import com.sleepycat.je.rep.txn.MasterTxn;
import com.sleepycat.je.rep.txn.MasterTxn.MasterTxnFactory;
import com.sleepycat.je.rep.utilint.RepTestUtils;
import com.sleepycat.je.rep.utilint.RepTestUtils.RepEnvInfo;
import com.sleepycat.je.util.TestUtils;

public class DynamicGroupTest extends RepTestBase {

    @Override
    @Before
    public void setUp()
        throws Exception {

        groupSize = 5;
        super.setUp();
    }

    @Test
    public void testRemoveMemberExceptions() {
        createGroup(2);
        ReplicatedEnvironment master = repEnvInfo[0].getEnv();
        assertTrue(master.getState().isMaster());

        RepNode masterRep = repEnvInfo[0].getRepNode();
        try {
            masterRep.removeMember(master.getNodeName());
            fail("Exception expected.");
        } catch (MasterStateException e) {
            // Expected
        }

        try {
            masterRep.removeMember("unknown node foobar");
            fail("Exception expected.");
        } catch (MemberNotFoundException e) {
            // Expected
        }

        masterRep.removeMember(repEnvInfo[1].getRepNode().getNodeName());
        try {
            masterRep.removeMember(repEnvInfo[1].getRepNode().getNodeName());
            fail("Exception expected.");
        } catch (MemberNotFoundException e) {
            // Expected
        }
        repEnvInfo[1].closeEnv();
    }

    @Test
    public void testDeleteMemberExceptions() {
        createGroup(2);
        ReplicatedEnvironment master = repEnvInfo[0].getEnv();
        assertTrue(master.getState().isMaster());

        RepNode masterRep = repEnvInfo[0].getRepNode();
        try {
            masterRep.removeMember(master.getNodeName(), true);
            fail("Exception expected.");
        } catch (MasterStateException e) {
            // Expected
        }

        try {
            masterRep.removeMember("unknown node foobar", true);
            fail("Exception expected.");
        } catch (MemberNotFoundException e) {
            // Expected
        }

        final String delName = repEnvInfo[1].getRepNode().getNodeName();
        try {
            masterRep.removeMember(delName, true);
            fail("Exception expected.");
        } catch (MemberActiveException e) {
            // Expected
        }

        repEnvInfo[1].closeEnv();
        masterRep.removeMember(delName, true);

        try {
            masterRep.removeMember(delName, true);
            fail("Exception expected.");
        } catch (MemberNotFoundException e) {
            // Expected
        }
    }

    /*
     * Tests internal node removal APIs.
     */
    @Test
    public void testRemoveMember() {
        createGroup(groupSize);
        ReplicatedEnvironment master = repEnvInfo[0].getEnv();
        assertTrue(master.getState().isMaster());

        RepNode masterRep = repEnvInfo[0].getRepNode();

        /* Reduce the group size all the way down to one. */
        for (int i = 1; i < groupSize;  i++) {
            assertTrue(repEnvInfo[i].getEnv().isValid());
            masterRep.removeMember(repEnvInfo[i].getEnv().getNodeName());
            assertEquals((groupSize-i),
                         masterRep.getGroup().getElectableGroupSize());
        }

        /* Close the replica handles*/
        for (int i = groupSize-1; i > 0;  i--) {
            repEnvInfo[i].closeEnv();
        }

        /* Attempting to re-open them with the same node names should fail. */
        for (int i = 1; i < groupSize;  i++) {
            try {
                repEnvInfo[i].openEnv();
                fail("Exception expected");
            } catch (EnvironmentFailureException e) {
                /* Expected, the master should reject the attempt. */
                assertEquals(EnvironmentFailureReason.HANDSHAKE_ERROR,
                             e.getReason());
            }
        }

        /* Doing the same but with different node names should be ok. */
        for (int i = 1; i < groupSize;  i++) {
            final RepEnvInfo ri = repEnvInfo[i];
            final ReplicationConfig repConfig = ri.getRepConfig();
            TestUtils.removeLogFiles("RemoveRepEnvironments",
                                     ri.getEnvHome(),
                                     false);

            repConfig.setNodeName("ReplaceNode_" + i);
            ri.openEnv();
            assertEquals(i+1, masterRep.getGroup().getElectableGroupSize());
        }
        master.close();
    }

    /*
     * Tests internal node deletion APIs.
     */
    @Test
    public void testDeleteMember() {
        createGroup(groupSize);
        ReplicatedEnvironment master = repEnvInfo[0].getEnv();
        assertTrue(master.getState().isMaster());

        RepNode masterRep = repEnvInfo[0].getRepNode();

        /* Reduce the group size all the way down to one. */
        for (int i = 1; i < groupSize;  i++) {
            assertTrue(repEnvInfo[i].getEnv().isValid());
            final String delName = repEnvInfo[i].getEnv().getNodeName();
            repEnvInfo[i].closeEnv();
            masterRep.removeMember(delName, true);
            assertEquals((groupSize-i),
                         masterRep.getGroup().getElectableGroupSize());
        }

        /*
         * Attempting to re-open them with the same node names should succeed
         */
        for (int i = 1; i < groupSize;  i++) {
            repEnvInfo[i].openEnv();
        }
    }

    /*
     * Verifies that an InsufficientAcksException is not thrown if the group
     * size changes while a transaction commit is waiting for acknowledgments.
     */
    @Test
    public void testMemberRemoveAckInteraction() {
        testMemberRemoveAckInteraction(false);
    }

    /* Same but deleting the members. */
    @Test
    public void testDeleteRemoveAckInteraction() {
        testMemberRemoveAckInteraction(true);
    }

    private void testMemberRemoveAckInteraction(final boolean delete) {
        createGroup(groupSize);
        Transaction txn;
        Database db;
        try {
            MasterTxn.setFactory(new TxnFactory(delete));
            ReplicatedEnvironment master = repEnvInfo[0].getEnv();

            txn = master.beginTransaction(null, null);
            /* Write to the environment. */
            db = master.openDatabase(txn, "random", dbconfig);
            db.close();
            txn.commit();
        } catch (InsufficientAcksException e) {
            fail ("No exception expected.");
        } finally {
            MasterTxn.setFactory(null);
        }
    }

    @Test
    public void testNoQuorum()
        throws DatabaseException,
               InterruptedException {

        for (int i=0; i < 3; i++) {
            ReplicatedEnvironment rep = repEnvInfo[i].openEnv();
            State state = rep.getState();
            assertEquals((i == 0) ? State.MASTER : State.REPLICA, state);
        }
        RepTestUtils.syncGroupToLastCommit(repEnvInfo, 3);
        repEnvInfo[1].closeEnv();
        repEnvInfo[2].closeEnv();

        // A new node joining in the absence of a quorum must fail
        try {
            repEnvInfo[3].openEnv();
            fail("Expected exception");
        } catch (UnknownMasterException e) {
            /* Expected. */
        }
    }

    /* Start the master (the helper node) first */
    @Test
    public void testGroupCreateMasterFirst()
        throws DatabaseException {

        for (int i=0; i < repEnvInfo.length; i++) {
            ReplicatedEnvironment rep = repEnvInfo[i].openEnv();
            State state = rep.getState();
            assertEquals((i == 0) ? State.MASTER : State.REPLICA, state);
            RepNode repNode = RepInternal.getNonNullRepImpl(rep).getRepNode();
            /* No elections, helper nodes or members queried for master. */
            assertEquals(0, repNode.getElections().getElectionCount());
        }
    }

    /*
     * Start the master (the helper node) last, so the other nodes have to
     * wait and retry until the helper node comes up.
     */
    @Test
    public void testGroupCreateMasterLast()
        throws DatabaseException,
           InterruptedException {

        RepNodeThread threads[] = new RepNodeThread[repEnvInfo.length];

        /* Start up non-masters, they should wait */
        for (int i=1; i < repEnvInfo.length; i++) {
            threads[i]=new RepNodeThread(i);
            threads[i].start();
        }

        State state = repEnvInfo[0].openEnv().getState();
        assertEquals(State.MASTER, state);

        for (int i=1; i < repEnvInfo.length; i++) {
            threads[i].join(30000);
            assertTrue(!threads[i].isAlive());
            assertNull(threads[i].te);
        }
    }

    /**
     * Test that a timeout in the feeder while attempting to read the group
     * database because other feeders have it write locked causes the feeder
     * (and replica) to fail, but allows the master to continue operating.
     * [#23822]
     */
    @Test
    public void testJoinGroupReadGroupTimeout()
        throws DatabaseException, InterruptedException {

        /* Start first node as master */
        ReplicatedEnvironment repEnv = repEnvInfo[0].openEnv();
        assertEquals("Master node state", State.MASTER, repEnv.getState());

        RepImpl repImpl = RepInternal.getNonNullRepImpl(repEnv);

        for (int i = 1; i <= 2; i++) {

            /* Get a write lock on the RepGroupDB */
            final MasterTxn txn = new MasterTxn(
                repImpl,
                new TransactionConfig().setDurability(
                    new Durability(SyncPolicy.SYNC,
                                   SyncPolicy.SYNC,
                                   ReplicaAckPolicy.SIMPLE_MAJORITY)),
                repImpl.getNameIdPair());
            final DatabaseImpl groupDbImpl = repImpl.getGroupDb();
            final DatabaseEntry value = new DatabaseEntry();
            final Cursor cursor =
                DbInternal.makeCursor(groupDbImpl, txn, new CursorConfig());
            final OperationStatus status = cursor.getNext(
                RepGroupDB.groupKeyEntry, value, LockMode.RMW);
            assertEquals(i + ": Lock group result",
                         OperationStatus.SUCCESS, status);

            /* Wait longer than the default 500 ms read timeout */
            Thread.sleep(600);

            /* Test both electable and secondary nodes */
            if (i == 2) {
                repEnvInfo[i].getRepConfig().setNodeType(NodeType.SECONDARY);
            }

            /* Create a thread that attempts to join another environment */
            RepNodeThread repNodeThread = new RepNodeThread(i, i != 1);
            repNodeThread.start();

            /* Wait for attempt to complete */
            repNodeThread.join(30000);
            assertEquals("RN thread alive", false, repNodeThread.isAlive());

            if (i == 1) {

                /* Join attempt should fail for primary */
                assertNotNull("Expected RN thread exception",
                              repNodeThread.te);

                /* Release write lock on RepGroupDB */
                cursor.close();
                txn.abort();

                /* Second join attempt should succeed */
                repNodeThread = new RepNodeThread(1);
                repNodeThread.start();
                repNodeThread.join(30000);
                assertEquals("RN thread alive",
                             false, repNodeThread.isAlive());
                assertEquals("RN thread exception", null, repNodeThread.te);
            } else {

                /* Join attempt should succeed for secondary */
                assertEquals("RN thread exception", null, repNodeThread.te);

                /* Release write lock on RepGroupDB */
                cursor.close();
                txn.abort();
            }
        }
    }

    private class RepNodeThread extends Thread {
        private final int id;
        private final boolean printStackTrace;
        volatile Throwable te;

        RepNodeThread(int id) {
            this(id, false);
        }

        RepNodeThread(int id, boolean printStackTrace) {
            this.id = id;
            this.printStackTrace = printStackTrace;
        }

        @Override
        public void run() {

            try {
                repEnvInfo[id].openEnv().getState();
            } catch (Throwable e) {
                te = e;
                if (printStackTrace) {
                    te.printStackTrace();
                }
            }
        }
    }

    /*
     * Factory for producing test MasterTxns
     */
    private class TxnFactory implements MasterTxnFactory {
        final boolean delete;
        final Thread thread = Thread.currentThread();

        TxnFactory(final boolean delete) {
            this.delete = delete;
        }

        @Override
        public MasterTxn create(EnvironmentImpl envImpl,
                                TransactionConfig config,
                                NameIdPair nameIdPair) {
            if (Thread.currentThread() != thread) {
                return new MasterTxn(envImpl, config, nameIdPair);
            }
            return new TestMasterTxn(envImpl, config, nameIdPair, delete);
        }

        @Override
        public MasterTxn createNullTxn(EnvironmentImpl envImpl,
                                       TransactionConfig config,
                                       NameIdPair nameIdPair) {

            return new MasterTxn(envImpl, config, nameIdPair) {
                @Override
                protected boolean updateLoggedForTxn() {
                    return true;
                }
            };
        }
    }

    private class TestMasterTxn extends MasterTxn {
        private final boolean delete;

        public TestMasterTxn(EnvironmentImpl envImpl,
                             TransactionConfig config,
                             NameIdPair nameIdPair,
                             boolean delete)
            throws DatabaseException {

            super(envImpl, config, nameIdPair);
            this.delete = delete;
        }

        @Override
        protected void preLogCommitHook() {
            super.preLogCommitHook();
            RepNode rmMasterNode = repEnvInfo[0].getRepNode();
            int size = rmMasterNode.getGroup().getAllElectableMembers().size();
            int delNodes = ((size & 1) == 1) ? 2 : 1;
            int closeNodeIndex = (size - delNodes) - 1;

            /*
             * The loop below simulates the concurrent removal of a node while
             * a transaction is in progress. It deletes a sufficient number of
             * nodes so as to get a lower simple nodes to get to a new lower
             * simple majority.
             */
            for (int i= repEnvInfo.length-1; delNodes-- > 0; i--) {
                repEnvInfo[i].closeEnv();
                rmMasterNode.removeMember(
                    repEnvInfo[i].getRepConfig().getNodeName(), delete);
            }

            /*
             * Shut down an additional undeleted Replica to provoke a
             * lack of acks based on the old simple majority.
             */
            repEnvInfo[closeNodeIndex].closeEnv();
        }
    }
}
