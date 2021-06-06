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

import static com.sleepycat.je.Durability.ReplicaAckPolicy.ALL;
import static com.sleepycat.je.Durability.SyncPolicy.NO_SYNC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.impl.RepTestBase;
import com.sleepycat.je.rep.utilint.RepTestUtils;
import com.sleepycat.je.rep.utilint.RepTestUtils.RepEnvInfo;

/**
 * Tests for the influence of log version on the outcome of elections.
 * Generally the node(s) with the oldest log versions should be preferred,
 * because an old-version replica downstream of a newer master may not know how
 * to process newer log record types.  But if a group-majority of nodes is at
 * the later version, it's OK to forsake any other nodes at older versions;
 * because although the older replicas might choke and die, the group as a
 * whole survives (relying on the majority).
 */
public class ElectionWithLogVersionTest extends RepTestBase {
    /*
     * Check that JE HA can support more than 2 log formats during the upgrade.
     */
    @Test
    public void testMultiVersions()
        throws Exception {

        createGroup();

        /* Set different log versions on each node. */
        for (int i = 0; i < repEnvInfo.length; i++) {
            repEnvInfo[i].getRepNode().setVersion(i);
        }

        /* Make sure we're really getting at least 3 different log versions. */
        assertTrue(repEnvInfo.length > 2);

        /*
         * Shut down the master, in order to provoke an election.
         */
        RepEnvInfo master = repEnvInfo[0];
        assertTrue(master.isMaster());
        master.closeEnv();

        /*
         * The node with the lowest log version should be elected as the
         * master, which is repEnvInfo[1].
         */
        awaitElectionResult(1);

        /*
         * Restart the closed node, just so as to leave the group in the
         * healthy state that's expected by tearDown().
         */
        repEnvInfo[0].openEnv();
    }

    private int awaitElectionResult(int ... nodes)
        throws InterruptedException {

        long deadline = System.currentTimeMillis() + 4000;
        while (System.currentTimeMillis() < deadline) {
            for (int i : nodes) {
                if (repEnvInfo[i].isMaster()) {
                    return i;
                }
            }
            Thread.sleep(100);
        }
        fail("no election winner emerged from expected set");
        return -1;              // not reached
    }

    /**
     * Check that election results comply with the rules about nodes' log
     * versions for JE 5.
     */
    @Test
    public void testLogVersionSensitivityJe5()
        throws Exception {

        /*
         * Set the log version that supports replication in the previous format
         * to LOG_VERSION + 1, so that the current version requires enforcing
         * rules involving log versions.
         */
        RankingProposer.testLogVersionReplicatePrevious =
            LogEntryType.LOG_VERSION + 1;
        try {
            testLogVersionSensitivityInternal(true);
        } finally {
            RankingProposer.testLogVersionReplicatePrevious = 0;
        }
    }

    /**
     * Check that election results are not sensitive to log versions for
     * releases greater than JE 5.
     */
    @Test
    public void testLogVersionSensitivity()
        throws Exception {

        /*
         * Set the log version that supports replication in the previous format
         * to be LOG_VERSION, so that the current version does not require
         * enforcing rules involving log versions.
         */
        RankingProposer.testLogVersionReplicatePrevious =
            LogEntryType.LOG_VERSION;
        try {
            testLogVersionSensitivityInternal(false);
        } finally {
            RankingProposer.testLogVersionReplicatePrevious = 0;
        }
    }

    private void testLogVersionSensitivityInternal(final boolean je5)
        throws Exception {

        createGroup();

        /*
         * Set the log version of the first four replicas to a lower version,
         * then shutdown the master in order to provoke an election. In the
         * normal case, the 5th replica has the largest port number and will be
         * elected master, but in this case, and only when using log versions
         * for JE 5 and earlier, it will still be a replica because it has the
         * largest version.
         */
        for (int i = 0; i < 4; i++) {
            repEnvInfo[i].getRepNode().setVersion(LogEntryType.LOG_VERSION - 1);
        }
        RepTestUtils.syncGroupToLastCommit(repEnvInfo, repEnvInfo.length);

        RepEnvInfo master = repEnvInfo[0];
        master.closeEnv();
        int newMasterIndex = awaitElectionResult(1, 2, 3, 4);
        if (je5) {
            assertTrue(newMasterIndex != 4);
        }

        master = repEnvInfo[newMasterIndex];

        /*
         * Now try setting a majority of the group to the higher log version.
         * In this case, it is considered OK to elect one of the higher-version
         * nodes, effectively abandoning the laggards.
         *
         * repEnvInfo[4] is still already at the higher log version.  We know
         * [0] is a replica, since we're just now restarting it.  Need to find
         * one more node other than the current master to have its log version
         * set and participate in the next election: try "1", but if that
         * happens to be the previous election winner then use "2" instead.
         */
        repEnvInfo[0].openEnv();
        repEnvInfo[0].getRepNode().setVersion(LogEntryType.LOG_VERSION);
        int otherReplica = newMasterIndex == 1 ? 2 : 1;
        repEnvInfo[otherReplica].getRepNode().setVersion(
            LogEntryType.LOG_VERSION);

        /*
         * Make sure all replicas are caught up with the master, to avoid
         * having differing VLSNs influence the outcome of the following
         * election.
         */
        ReplicatedEnvironment masterEnv = master.getEnv();

        /*
         * Ensure all replicas have had a chance to join before using the
         * durability ALL commit to avoid an IRE.
         */
        assertEquals(master,
                     findMasterAndWaitForReplicas(60000,
                                                  repEnvInfo.length -1,
                                                  repEnvInfo));
        Database db = masterEnv.openDatabase(null, TEST_DB_NAME, dbconfig);
        @SuppressWarnings("hiding")
        DatabaseEntry key = new DatabaseEntry(new byte[1]);
        DatabaseEntry value = new DatabaseEntry(new byte[1]);
        TransactionConfig tc =
            new TransactionConfig().setDurability
            (new Durability(NO_SYNC, NO_SYNC, ALL));
        Transaction txn = masterEnv.beginTransaction(null, tc);
        db.put(txn, key, value);
        txn.commit();
        db.close();

        master.closeEnv();
        if (je5) {
            awaitElectionResult(0, otherReplica, 4);
        } else {
            awaitElectionResult(0, 1, 2, 3, 4);
        }

        /* As usual, leave in a clean state, just to placate tearDown(). */
        master.openEnv();       // not really still master at this point
    }
}
