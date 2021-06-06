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
package com.sleepycat.je.rep.arbiter;

import static com.sleepycat.je.rep.arbiter.impl.ArbiterStatDefinition.ARB_DTVLSN;
import static com.sleepycat.je.rep.arbiter.impl.ArbiterStatDefinition.ARB_MASTER;
import static com.sleepycat.je.rep.arbiter.impl.ArbiterStatDefinition.ARB_N_ACKS;
import static com.sleepycat.je.rep.arbiter.impl.ArbiterStatDefinition.ARB_N_FSYNCS;
import static com.sleepycat.je.rep.arbiter.impl.ArbiterStatDefinition.ARB_N_REPLAY_QUEUE_OVERFLOW;
import static com.sleepycat.je.rep.arbiter.impl.ArbiterStatDefinition.ARB_N_WRITES;
import static com.sleepycat.je.rep.arbiter.impl.ArbiterStatDefinition.ARB_STATE;
import static com.sleepycat.je.rep.arbiter.impl.ArbiterStatDefinition.ARB_VLSN;

import java.io.Serializable;

import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.rep.arbiter.impl.ArbiterStatDefinition;
import com.sleepycat.je.utilint.StatGroup;

/**
 * Statistics for an {@link Arbiter}.
 *
 * @see Arbiter#getStats(StatsConfig)
 */
public class ArbiterStats implements Serializable {

    private static final long serialVersionUID = 1734048134L;

    private final StatGroup arbStats;

    /**
     * @hidden
     * Internal use only.
     */
    ArbiterStats(StatGroup arbGrp) {
        if (arbGrp != null) {
            arbStats = arbGrp;
        } else {
            arbStats = new StatGroup(ArbiterStatDefinition.GROUP_NAME,
                    ArbiterStatDefinition.GROUP_DESC);
        }
    }

    /**
     * The number of attempts to queue a response when
     * the queue was full.
     */
    public long getReplayQueueOverflow() {
        return arbStats.getLong(ARB_N_REPLAY_QUEUE_OVERFLOW);
    }

    /**
     * The number of transactions that has been
     * acknowledged.
     */
    public long getAcks() {
        return arbStats.getLong(ARB_N_ACKS);
    }

    /**
     * The current master node.
     */
    public String getMaster() {
        return arbStats.getString(ARB_MASTER);
    }

    /**
     * The ReplicatedEnvironment.State of the node.
     */
    public String getState() {
        return arbStats.getString(ARB_STATE);
    }

    /**
     * The highest commit VLSN that has been
     * acknowledged.
     */
    public long getVLSN() {
        return arbStats.getLong(ARB_VLSN);
    }

    /**
     * The highest commit DTVLSN that has been
     * acknowledged.
     */
    public long getDTVLSN() {
        return arbStats.getLong(ARB_DTVLSN);
    }

    /**
     * The number of file writes.
     */
    public long getWrites() {
        return arbStats.getLong(ARB_N_WRITES);
    }

    /**
     * The number of file fsyncs.
     */
    public long getFSyncs() {
        return arbStats.getLong(ARB_N_FSYNCS);
    }
}

