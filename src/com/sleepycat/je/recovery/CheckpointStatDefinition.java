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

package com.sleepycat.je.recovery;

import com.sleepycat.je.utilint.StatDefinition;
import com.sleepycat.je.utilint.StatDefinition.StatType;

/**
 * Per-stat Metadata for JE checkpointer statistics.
 */
public class CheckpointStatDefinition {
    public static final String GROUP_NAME = "Checkpoints";
    public static final String GROUP_DESC =
        "Dirty Btree internal nodes are written to the data " +
            "log periodically to bound recovery time.";

    public static final String CKPT_CHECKPOINTS_NAME =
        "nCheckpoints";
    public static final String CKPT_CHECKPOINTS_DESC =
        "Total number of checkpoints run so far.";
    public static final StatDefinition CKPT_CHECKPOINTS =
        new StatDefinition(
            CKPT_CHECKPOINTS_NAME,
            CKPT_CHECKPOINTS_DESC);

    public static final String CKPT_LAST_CKPTID_NAME =
        "lastCheckpointId";
    public static final String CKPT_LAST_CKPTID_DESC =
        "Id of the last checkpoint.";
    public static final StatDefinition CKPT_LAST_CKPTID =
        new StatDefinition(
            CKPT_LAST_CKPTID_NAME,
            CKPT_LAST_CKPTID_DESC,
            StatType.CUMULATIVE);

    public static final String CKPT_FULL_IN_FLUSH_NAME =
        "nFullINFlush";
    public static final String CKPT_FULL_IN_FLUSH_DESC =
        "Accumulated number of full INs flushed to the log.";
    public static final StatDefinition CKPT_FULL_IN_FLUSH =
        new StatDefinition(
            CKPT_FULL_IN_FLUSH_NAME,
            CKPT_FULL_IN_FLUSH_DESC);

    public static final String CKPT_FULL_BIN_FLUSH_NAME =
        "nFullBINFlush";
    public static final String CKPT_FULL_BIN_FLUSH_DESC =
        "Accumulated number of full BINs flushed to the log.";
    public static final StatDefinition CKPT_FULL_BIN_FLUSH =
        new StatDefinition(
            CKPT_FULL_BIN_FLUSH_NAME,
            CKPT_FULL_BIN_FLUSH_DESC);

    public static final String CKPT_DELTA_IN_FLUSH_NAME =
        "nDeltaINFlush";
    public static final String CKPT_DELTA_IN_FLUSH_DESC =
        "Accumulated number of Delta INs flushed to the log.";
    public static final StatDefinition CKPT_DELTA_IN_FLUSH =
        new StatDefinition(
            CKPT_DELTA_IN_FLUSH_NAME,
            CKPT_DELTA_IN_FLUSH_DESC);

    public static final String CKPT_LAST_CKPT_INTERVAL_NAME =
        "lastCheckpointInterval";
    public static final String CKPT_LAST_CKPT_INTERVAL_DESC =
        "Byte length from last checkpoint start to the previous checkpoint " +
            "start.";
    public static final StatDefinition CKPT_LAST_CKPT_INTERVAL =
        new StatDefinition(
            CKPT_LAST_CKPT_INTERVAL_NAME,
            CKPT_LAST_CKPT_INTERVAL_DESC,
            StatType.CUMULATIVE);

    public static final String CKPT_LAST_CKPT_START_NAME =
        "lastCheckpointStart";
    public static final String CKPT_LAST_CKPT_START_DESC =
        "Location in the log of the last checkpoint start.";
    public static final StatDefinition CKPT_LAST_CKPT_START =
        new StatDefinition(
            CKPT_LAST_CKPT_START_NAME,
            CKPT_LAST_CKPT_START_DESC,
            StatType.CUMULATIVE);

    public static final String CKPT_LAST_CKPT_END_NAME =
        "lastCheckpointEnd";
    public static final String CKPT_LAST_CKPT_END_DESC =
        "Location in the log of the last checkpoint end.";
    public static final StatDefinition CKPT_LAST_CKPT_END =
        new StatDefinition(
            CKPT_LAST_CKPT_END_NAME,
            CKPT_LAST_CKPT_END_DESC,
            StatType.CUMULATIVE);
}
