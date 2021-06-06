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
import static org.junit.Assert.fail;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.dbi.EnvironmentFailureReason;
import com.sleepycat.je.rep.InsufficientAcksException;
import com.sleepycat.je.rep.InsufficientReplicasException;
import com.sleepycat.je.rep.NodeType;
import com.sleepycat.je.rep.RepInternal;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicationConfig;
import com.sleepycat.je.rep.ReplicationMutableConfig;
import com.sleepycat.je.rep.UnknownMasterException;
import com.sleepycat.je.rep.impl.RepParams;
import com.sleepycat.je.rep.impl.RepTestBase;
import com.sleepycat.je.rep.impl.node.cbvlsn.LocalCBVLSNUpdater;
import com.sleepycat.je.rep.utilint.RepTestUtils;
import com.sleepycat.je.rep.utilint.RepTestUtils.RepEnvInfo;

import org.junit.Before;
import org.junit.Test;

public class PrimaryNodeTest extends RepTestBase {

    final TransactionConfig txnConfig = new TransactionConfig();

    final DatabaseConfig dbConfig = new DatabaseConfig();

    RepEnvInfo primary = null;
    RepEnvInfo second = null;
    RepEnvInfo third = null;
    RepEnvInfo allInfo[] = null;

    @Override
    @Before
    public void setUp()
        throws Exception {

        groupSize = 3;
        super.setUp();
        allInfo = repEnvInfo;
        groupSize = 2;
        repEnvInfo = new RepEnvInfo[2];
        primary = repEnvInfo[0] = allInfo[0];
        second = repEnvInfo[1] = allInfo[1];
        third = allInfo[2];

        dbConfig.setTransactional(true);
        dbConfig.setAllowCreate(true);

        /* Config to speed up test elapsed time. */
        primary.getRepConfig().
            setConfigParam(ReplicationConfig.INSUFFICIENT_REPLICAS_TIMEOUT,
                       "5 s");
        primary.getRepConfig().
            setConfigParam(ReplicationConfig.ELECTIONS_PRIMARY_RETRIES, "1");

        // TODO: is this needed now that hard recovery works?
        LocalCBVLSNUpdater.setSuppressGroupDBUpdates(true);
        for (RepEnvInfo ri : allInfo) {
            ri.getEnvConfig().setConfigParam("je.env.runCleaner", "false");
        }
    }

    @Test
    public void testPrimaryParam() {
        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setTransactional(true);
        envConfig.setAllowCreate(true);

        ReplicationConfig repEnvConfig = new ReplicationConfig();
        repEnvConfig.setGroupName("ExampleGroup");
        repEnvConfig.setNodeName("node1");
        repEnvConfig.setNodeHostPort("localhost:5000");
        repEnvConfig.setHelperHosts("localhost:5000");

        final int defaultPriority =
            Integer.parseInt(RepParams.NODE_PRIORITY.getDefault());

        assertEquals(false, repEnvConfig.getDesignatedPrimary());
        assertEquals(defaultPriority, repEnvConfig.getNodePriority());

        ReplicatedEnvironment repEnv =
            new ReplicatedEnvironment(envRoot, repEnvConfig, envConfig);
        /* Default value must be false. */
        assertEquals(false, repEnv.getRepConfig().getDesignatedPrimary());
        ReplicationMutableConfig repMutableConfig =
            repEnv.getRepMutableConfig();
        assertEquals(false, repMutableConfig.getDesignatedPrimary());
        repMutableConfig.setDesignatedPrimary(true);
        repEnv.setRepMutableConfig(repMutableConfig);
        assertEquals(true,
                     repEnv.getRepMutableConfig().getDesignatedPrimary());

        repEnv.close();

        repEnvConfig.setDesignatedPrimary(true);

        /* Ensure that the priority is also increased. */

        repEnv = new ReplicatedEnvironment(envRoot, repEnvConfig, envConfig);
        assertTrue(defaultPriority <
                   RepInternal.getNonNullRepImpl(repEnv).getRepNode().
                   getElectionPriority());
        assertEquals(true, repEnv.getRepConfig().getDesignatedPrimary());
        repEnv.close();
    }

    @Test
    public void testConflictingPrimary() {
        primary.getRepConfig().setDesignatedPrimary(true);
        second.getRepConfig().setDesignatedPrimary(true);
        primary.openEnv();
        try {
            second.openEnv();
            second.closeEnv();
            fail("expected exception");
        } catch (EnvironmentFailureException e) {
            assertEquals(EnvironmentFailureReason.HANDSHAKE_ERROR,
                         e.getReason());
        }
        primary.closeEnv();
    }

    /**
     * Verifies that Txn begin and commits activate a Primary. Also that
     * addition of a new node passivates a Primary.
     *
     * @throws DatabaseException
     * @throws InterruptedException
     */
    @Test
    public void testActivatePassivate()
        throws DatabaseException,
               InterruptedException {

        createGroup();
        second.closeEnv();

        verifyDefaultTwoNodeTxnBehavior();

        primary.getRepConfig().setDesignatedPrimary(true);
        primary.closeEnv();

        RepTestUtils.restartGroup(repEnvInfo);
        second.closeEnv();
        /* Will activate the Primary since it causes an election. */
        primary.getRepNode().forceMaster(true);
        for (int i = 0; i < 10; i++) {
            if (primary.getEnv().getState().isMaster()) {
                break;
            }
            Thread.sleep(1000);
        }
        assertTrue(primary.getEnv().getState().isMaster());

        activateOnBegin();

        activateOnCommit();

        passivateWithNewNode();

        primary.closeEnv();
    }

    /*
     * Basic test that does insertion with
     * majority durability and that ALL durability
     * fails.
     */
    @Test
    public void testBasic()
        throws DatabaseException,
               InterruptedException {
        createGroup();
        ReplicationMutableConfig repMutableConfig =
                primary.getEnv().getRepMutableConfig();
            assertEquals(false, repMutableConfig.getDesignatedPrimary());
            repMutableConfig.setDesignatedPrimary(true);
            primary.getEnv().setRepMutableConfig(repMutableConfig);
        second.closeEnv();
        populateDB(repEnvInfo[0].getEnv(), "db", 0, 10, null);
        boolean gotException = false;
        try {
            populateDB(repEnvInfo[0].getEnv(), "db", 0, 100,
                    new TransactionConfig().setDurability(
                        RepTestUtils.SYNC_SYNC_ALL_DURABILITY));
        } catch (InsufficientReplicasException e) {
            gotException = true;
        }
        assertEquals(true, gotException);

    }

    /*
     * Test the typical dynamic use of designating and undesignating a node
     * as a primary.
     */
    @Test
    public void testDynamicPrimary() {
        createGroup();
        second.closeEnv();

        /* Start by verifying the default two node txn behavior. */
        verifyDefaultTwoNodeTxnBehavior();
        assertTrue(!primary.getRepNode().getArbiter().isActive());

        ReplicationMutableConfig repMutableConfig =
            primary.getEnv().getRepMutableConfig();
        assertEquals(false, repMutableConfig.getDesignatedPrimary());
        repMutableConfig.setDesignatedPrimary(true);
        primary.getEnv().setRepMutableConfig(repMutableConfig);

        assertTrue(!primary.getRepNode().getArbiter().isActive());
        try {
            Transaction txn =
                primary.getEnv().beginTransaction(null, txnConfig);
            /* Verify that it has transitioned to an active primary. */
            assertTrue(primary.getRepNode().getArbiter().isActive());
            txn.abort();
        } catch (Exception e) {
            fail("Unxpected exception:" + e);
        }

        /* Revert it back, and verify unaltered behavior */
        repMutableConfig = primary.getEnv().getRepMutableConfig();
        assertEquals(true, repMutableConfig.getDesignatedPrimary());
        repMutableConfig.setDesignatedPrimary(false);
        primary.getEnv().setRepMutableConfig(repMutableConfig);

        /* Verify that it's back to the default two node behavior. */
        verifyDefaultTwoNodeTxnBehavior();

        assertTrue(!primary.getRepNode().getArbiter().isActive());
    }

    private void verifyDefaultTwoNodeTxnBehavior() {
        boolean success = false;
        Transaction txn = null;
        Database db = null;
        try {
            txn = primary.getEnv().beginTransaction(null, txnConfig);
            db = primary.getEnv().openDatabase(txn, "dbx", dbConfig);
            success = true;
        } catch (InsufficientReplicasException e) {
            /* Expected. */
        } catch (InsufficientAcksException e) {

            /*
             * Expected alternative, depending upon network timing. That is,
             * if the master had not realized that the replica connection had
             * already been closed at the time of the begin transaction.
             */
        } finally {
            if (db != null) {
                db.close();
            }
            if (txn != null) {
                if (success) {
                    /* Should throw exception, if begin() did not. */
                    try {
                        txn.commit();
                        fail("expected exception");
                    } catch (InsufficientReplicasException e) {
                        /* Expected. */
                    } catch (InsufficientAcksException e) {
                        /* Expected. */
                    } finally {
                        txn.abort();
                    }
                } else {
                    txn.abort();
                }
            }
        }
    }


    /*
     * Increasing the group size to three should passivate an active Primary.
     */
    private void passivateWithNewNode()
        throws InterruptedException {

        assertTrue(primary.getRepNode().getArbiter().isActive());

        third.openEnv();
        waitUntilPassive();

        assertTrue(!primary.getRepNode().getArbiter().isActive());
        third.closeEnv();
    }

    /*
     * Passivate the Primary by restarting the second node. It leaves the
     * second node's env open.
     */
    private void passivatePrimary()
        throws InterruptedException {

        second.openEnv();
        waitUntilPassive();
    }

    private void waitUntilPassive()
        throws InterruptedException {
        for (int i = 0; i < 60; i++) {
            if (!primary.getRepNode().getArbiter().isActive()) {
                return;
            }
            Thread.sleep(1000);
        }
        fail("failed to passivate primary");
    }

    /* A commit should activate the Primary. */
    private void activateOnCommit()
        throws InterruptedException {

        passivatePrimary();

        assertTrue(!primary.getRepNode().getArbiter().isActive());
        /* A commit transaction should activate the primary. */
        boolean success = false;
        Transaction txn = null;
        Database db = null;
        try {
            txn = primary.getEnv().beginTransaction(null, txnConfig);
            second.closeEnv();

            assertTrue(!primary.getRepNode().getArbiter().isActive());

            /* Update something */
            db = primary.getEnv().openDatabase(txn, "db1", dbConfig);
            success = true;

            /*
             * Sleep five seconds so that the tcp connection is closed down.
             * and the primary has had time to react to it before it reaches
             * the commit.
             */
            Thread.sleep(5000);
        } catch (Exception e) {
            fail("Unxpected exception:" + e);
        } finally {
            if (db != null) {
                db.close();
            }
            if (txn != null) {
                if (success) {
                    txn.commit();
                    assertTrue(primary.getRepNode().getArbiter().isActive());
                } else {
                    txn.abort();
                }
            }
        }
    }

    /* A begin transaction should activate the primary. */
    private void activateOnBegin()
        throws InterruptedException {

        passivatePrimary();
        second.closeEnv();
        assertTrue(!primary.getRepNode().getArbiter().isActive());

        Transaction txn = null;
        for (int i = 0; i < 10; i++) {
            try {
                txn = primary.getEnv().beginTransaction(null, txnConfig);
                /* Verify that it has transitioned to an active primary. */
                if (primary.getRepNode().getArbiter().isActive()) {
                    /* test passed */
                    return;
                }
            } finally {
                if (txn != null) {
                    txn.abort();
                }
                txn = null;
            }

            /*
             * Retry the test, the master might think the feeder for the
             * Replica is still alive, that is, the tcp connection has not yet
             * timed out.
             */
            Thread.sleep(2000);
        }
        fail("failed despite retries");
    }

    @Test
    public void testElectionsActivate()
        throws InterruptedException {

        createGroup();
        closeEnvironments();

        /* Fail elections if the second node is not available. */
        String saveTimeout = primary.getRepConfig().
            getConfigParam(RepParams.ENV_SETUP_TIMEOUT.getName());
        /* Speed up the test. */
        primary.getRepConfig().
            setConfigParam(RepParams.ENV_SETUP_TIMEOUT.getName(), "5 s");
        try {
            primary.openEnv();
            fail("Expected exception");
        } catch (UnknownMasterException e) {
            // Expected join group times out since the second node is down
        }
        /* Restore the timeout */
        primary.getRepConfig().
            setConfigParam(RepParams.ENV_SETUP_TIMEOUT.getName(), saveTimeout);

        /*
         * Pass elections if the second node is not available, but Primary is
         * active.
         */
        primary.getRepConfig().setDesignatedPrimary(true);
        try {
            primary.openEnv();
            /* Verify that it has transitioned to an active primary. */
            assertTrue(primary.getRepNode().getArbiter().isActive());
        } catch (UnknownMasterException e) {
            fail("Unxpected exception:" + e);
        }
        passivatePrimary();
        /* Primary should be passivated. */
        assertTrue(!primary.getRepNode().getArbiter().isActive());
        primary.closeEnv();
        second.closeEnv();
    }

    /**
     * Regression test for [#21536].
     */
    @Test
    public void testActivateWhileAwaitingAck() throws Exception {
        primary.getRepConfig().
            setConfigParam(ReplicationConfig.REPLICA_ACK_TIMEOUT,
                       "5 s");
        primary.getRepConfig().setDesignatedPrimary(true);
        createGroup();
        ReplicatedEnvironment masterEnv = primary.getEnv();
        Database db = masterEnv.openDatabase(null, TEST_DB_NAME, dbconfig);
        DatabaseEntry key1 = new DatabaseEntry("1".getBytes());
        DatabaseEntry value = new DatabaseEntry("one".getBytes());
        db.put(null, key1, value);

        second.getRepNode().replica().setDontProcessStream();
        key1 = new DatabaseEntry("2".getBytes());
        value = new DatabaseEntry("two".getBytes());
        db.put(null, key1, value);

        /*
         * Even though we forced the replica not to ack the txn, it's still
         * connected, and we decided in this case that it makes sense to
         * refrain from setting active primary mode.  (We only set active
         * primary mode when the connection is broken.)
         */
        assertTrue(!primary.getRepNode().getArbiter().isActive());

        db.close();
    }

    /**
     * Test designating a primary in a two (electable) node group that also has
     * a secondary node.
     */
    @Test
    public void testSecondaryNode()
        throws Exception {

        /*
         * Create a group, add a secondary node, and confirm that closing one
         * electable node prevents creating write transactions
         */
        createGroup();
        third.getRepConfig().setNodeType(NodeType.SECONDARY);
        third.openEnv();
        try {
            second.closeEnv();
            verifyDefaultTwoNodeTxnBehavior();
            assertTrue(!primary.getRepNode().getArbiter().isActive());

            /*
             * Specify a designated primary and check that write transactions
             * are now supported
             */
            final ReplicationMutableConfig repMutableConfig =
                primary.getEnv().getRepMutableConfig();
            repMutableConfig.setDesignatedPrimary(true);
            primary.getEnv().setRepMutableConfig(repMutableConfig);
            Transaction txn =
                primary.getEnv().beginTransaction(null, txnConfig);
            assertTrue(primary.getRepNode().getArbiter().isActive());
            txn.abort();

            /* Try designating the secondary as a primary */
            repMutableConfig.setDesignatedPrimary(false);
            primary.getEnv().setRepMutableConfig(repMutableConfig);
            repMutableConfig.setDesignatedPrimary(true);
            third.getEnv().setRepMutableConfig(repMutableConfig);
            try {
                txn = primary.getEnv().beginTransaction(null, txnConfig);
                fail("Shouldn't support write transactions");
            } catch (InsufficientReplicasException e) {
                /* Expect this... */
            } catch (InsufficientAcksException e) {
                /* ... or this */
            }

        } finally {             /* Clean up */
            try {
                third.closeEnv();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void closeEnvironments() {
        for (RepEnvInfo ri : repEnvInfo) {
            ri.closeEnv();
        }
    }
}
