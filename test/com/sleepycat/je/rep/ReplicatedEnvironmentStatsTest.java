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

package com.sleepycat.je.rep;

import org.junit.Test;

import com.sleepycat.je.rep.impl.RepTestBase;
import com.sleepycat.je.rep.utilint.RepTestUtils.RepEnvInfo;

public class ReplicatedEnvironmentStatsTest extends RepTestBase {

    // TODO: more detailed tests check for expected stat return values under
    // simulated conditions.

    /**
     * Exercise every public entry point on master and replica stats.
     */
    @Test
    public void testBasic() {
        createGroup();

        for (RepEnvInfo ri : repEnvInfo) {
            ReplicatedEnvironment rep = ri.getEnv();
            final ReplicatedEnvironmentStats repStats = rep.getRepStats(null);
            invokeAllAccessors(repStats);
        }
    }

    /**
     * Simply exercise the code path
     */
    private void invokeAllAccessors(ReplicatedEnvironmentStats stats) {
        stats.getAckWaitMs();
        stats.getLastCommitTimestamp();
        stats.getLastCommitVLSN();
        stats.getNFeedersCreated();
        stats.getNFeedersShutdown();
        stats.getNMaxReplicaLag();
        stats.getNMaxReplicaLagName();
        stats.getNProtocolBytesRead();
        stats.getNProtocolBytesWritten();
        stats.getNProtocolMessagesRead();
        stats.getNProtocolMessagesWritten();
        stats.getNReplayAborts();
        stats.getNReplayCommitAcks();
        stats.getNReplayCommitNoSyncs();
        stats.getNReplayCommitSyncs();
        stats.getNReplayCommitWriteNoSyncs();
        stats.getNReplayCommits();
        stats.getNReplayGroupCommitMaxExceeded();
        stats.getNReplayGroupCommitTimeouts();
        stats.getNReplayGroupCommits();
        stats.getNReplayLNs();
        stats.getNReplayNameLNs();
        stats.getNTxnsAcked();
        stats.getNTxnsNotAcked();
        stats.getProtocolBytesReadRate();
        stats.getProtocolBytesWriteRate();
        stats.getProtocolMessageReadRate();
        stats.getProtocolMessageWriteRate();
        stats.getProtocolReadNanos();
        stats.getProtocolWriteNanos();
        stats.getReplayElapsedTxnTime();
        stats.getReplayLatestCommitLagMs();
        stats.getReplayMaxCommitProcessingNanos();
        stats.getReplayMinCommitProcessingNanos();
        stats.getReplayTotalCommitLagMs();
        stats.getReplayTotalCommitProcessingNanos();
        stats.getReplicaDelayMap();
        stats.getReplicaLastCommitTimestampMap();
        stats.getReplicaLastCommitVLSNMap();
        stats.getReplicaVLSNLagMap();
        stats.getReplicaVLSNRateMap();
        stats.getStatGroups();
        stats.getTips();
        stats.getTotalTxnMs();
        stats.getTrackerLagConsistencyWaitMs();
        stats.getTrackerLagConsistencyWaits();
        stats.getTrackerVLSNConsistencyWaitMs();
        stats.getTrackerVLSNConsistencyWaits();
        stats.getVLSNRate();
    }
}
