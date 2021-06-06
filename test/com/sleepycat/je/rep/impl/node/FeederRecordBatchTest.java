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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.je.Database;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicatedEnvironment.State;
import com.sleepycat.je.rep.ReplicatedEnvironmentStats;
import com.sleepycat.je.rep.ReplicationConfig;
import com.sleepycat.je.rep.impl.RepParams;
import com.sleepycat.je.rep.impl.RepTestBase;
import com.sleepycat.je.rep.utilint.RepTestUtils;
import com.sleepycat.je.utilint.VLSN;

/**
 * Tests to verify that feeder output records are being batched as expected.
 */
public class FeederRecordBatchTest extends RepTestBase {

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        /**
         * Relax the time allowed for batching to a full one second to avoid
         * breaking up batches.
         */
        ReplicationConfig mrc = repEnvInfo[0].getRepConfig();
        mrc.setConfigParam(RepParams.FEEDER_BATCH_NS.getName(),
                           "1000000000");
        /**
         * Effectively suppress periodic heartbeats so they don't interfere
         * with message counts.
         */
        mrc.setConfigParam(RepParams.HEARTBEAT_INTERVAL.getName(),
                           "1000000");
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
    }

    /**
     * Basic test to check that messages are being batched as follows:
     *
     * 1) Create a two node group.
     * 2) Shut down the replica.
     * 3) Populate a database at the master.
     * 4) Clear the stats.
     * 5) Bring up the replica which needs to replay and catch up.
     * 6) The Feeder has the opportunity to batch records, since the replica
     * is lagging.
     * 7) Verify that records are being batched by checking the statistics.
     * 8) Verify that records are the same and have not been mangled.
     */
    @Test
    public void testBasic() throws InterruptedException {
        createGroup(2);

        ReplicatedEnvironment menv = repEnvInfo[0].getEnv();
        assertEquals(menv.getState(), State.MASTER);
        repEnvInfo[1].closeEnv();

        final int nRecords = 1000;
        populateDB(menv, nRecords);

        ReplicatedEnvironmentStats stats =
            menv.getRepStats(StatsConfig.CLEAR);
        assertEquals(0, stats.getNProtocolMessagesBatched());

        /* Open replica and catch up. */
        repEnvInfo[1].openEnv();

        /* Wait for catchup. */
        VLSN vlsn = RepTestUtils.syncGroup(repEnvInfo);
        /*
         * All the messages must have been sent in batches as part of the sync
         * operation.
         */
        stats = menv.getRepStats(null);

        assertTrue(stats.getNProtocolMessagesBatched() >= nRecords);

        /* Verify contents. */
        RepTestUtils.checkNodeEquality(vlsn, false, repEnvInfo);
    }

    /**
     * Test to ensure that batching works correctly around a large object that
     * does not fit into the cache.
     */
    @Test
    public void testDataMix()
        throws InterruptedException {

        createGroup(2);

        ReplicatedEnvironment menv = repEnvInfo[0].getEnv();
        TransactionConfig txnConfig = RepTestUtils.WNSYNC_NONE_TC;
        Transaction txn = menv.beginTransaction(null, txnConfig);
        Database db = menv.openDatabase(txn, "mixed", dbconfig);
        txn.commit();
        txn = null;
        RepTestUtils.syncGroup(repEnvInfo);

        repEnvInfo[1].closeEnv();
        assertEquals(menv.getState(), State.MASTER);

        final int batchBuffSize =
            Integer.parseInt(RepParams.FEEDER_BATCH_BUFF_KB.getDefault()) *
            1024;

        txn = menv.beginTransaction(null, txnConfig);

        /*
         * Generate a log pattern with an intervening large object that serves
         * to break up a batch.
         */
        for (int size : new int[] { 1, 2, /* batch 1 */
                                    batchBuffSize + 1, /* break the batch */
                                    3, 4  /* batch 2 */
                                  }) {
            IntegerBinding.intToEntry(size, key);
            data.setData(new byte[size]);
            db.put(txn, key, data);
        }
        txn.commit();
        db.close();


        ReplicatedEnvironmentStats stats =
            menv.getRepStats(StatsConfig.CLEAR);

        repEnvInfo[1].openEnv();
        final VLSN vlsn = RepTestUtils.syncGroup(repEnvInfo);

        stats = menv.getRepStats(null);

        /*
         * Seven total messages: 1 unconditional startup heartbeat +
         * 5 puts + 1 commit
         */
        assertEquals(7, stats.getNProtocolMessagesWritten());

        /* 4 puts + 1 commit batched. */
        assertEquals(5, stats.getNProtocolMessagesBatched());

        /* 2 batches as above. */
        assertEquals(2, stats.getNProtocolMessageBatches());

        RepTestUtils.checkNodeEquality(vlsn, false, repEnvInfo);
    }

}
