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

package com.sleepycat.je.rep.arbiter.impl;

import com.sleepycat.je.utilint.StatDefinition;
import com.sleepycat.je.utilint.StatDefinition.StatType;

public class ArbiterStatDefinition {

    public static final String GROUP_NAME = "Arbiter";
    public static final String GROUP_DESC =
        "Arbiter statistics";

    public static final String ARBIO_GROUP_NAME = "ArbFileIO";
    public static final String ARBIO_GROUP_DESC =
        "Arbiter file I/O statistics";

    public static final StatDefinition ARB_N_FSYNCS =
            new StatDefinition(
                "nFSyncs",
                "The number of fsyncs.");

    public static final StatDefinition ARB_N_WRITES =
            new StatDefinition(
                "nWrites",
                "The number of file writes.");

    public static final StatDefinition ARB_N_REPLAY_QUEUE_OVERFLOW =
        new StatDefinition(
            "nReplayQueueOverflow",
            "The number of times replay queue failed to insert " +
             "because if was full.");

    public static final StatDefinition ARB_N_ACKS =
        new StatDefinition(
            "nAcks",
             "The number of transactions acknowledged.");

    public static final StatDefinition ARB_MASTER =
        new StatDefinition(
            "master",
            "The current or last Master Replication Node the Arbiter accessed.",
            StatType.CUMULATIVE);

    public static final StatDefinition ARB_STATE =
            new StatDefinition(
                 "state",
                 "The current state of the Arbiter.",
                 StatType.CUMULATIVE);

    public static final StatDefinition ARB_VLSN =
            new StatDefinition(
                "vlsn",
                "The highest VLSN that was acknowledged by the Arbiter.",
                StatType.CUMULATIVE);

    public static final StatDefinition ARB_DTVLSN =
            new StatDefinition(
                "dtvlsn",
                "The highest DTVLSN that was acknowledged by the Arbiter.",
                StatType.CUMULATIVE);
}
