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

package com.sleepycat.je.rep.vlsn;

import com.sleepycat.je.utilint.StatDefinition;

/**
 * Statistics associated with the VLSN Index used by HA.
 */
public class VLSNIndexStatDefinition {

    public static final String GROUP_NAME = "VLSNIndex";

    public static final String GROUP_DESC = "VLSN Index related stats.";

    public static final String N_HITS_NAME =
        "nHits";
    public static final String N_HITS_DESC =
        "Number of hits to the VLSN index cache";
    public static final StatDefinition N_HITS =
        new StatDefinition(
            N_HITS_NAME,
            N_HITS_DESC);

    public static final String N_MISSES_NAME =
        "nMisses";
    public static final String N_MISSES_DESC =
        "Number of log entry misses upon access to the VLSN index cache. Upon" +
            " a miss the Feeder will fetch the log enty from the log buffer, " +
            "or the log file.";
    public static final StatDefinition N_MISSES =
        new StatDefinition(
            N_MISSES_NAME,
            N_MISSES_DESC);

    public static final String N_HEAD_BUCKETS_DELETED_NAME =
        "nHeadBucketsDeleted";
    public static final String N_HEAD_BUCKETS_DELETED_DESC =
        "Number of VLSN index buckets deleted at the head(the low end) of the" +
            " VLSN index.";
    public static final StatDefinition N_HEAD_BUCKETS_DELETED =
        new StatDefinition(
            N_HEAD_BUCKETS_DELETED_NAME,
            N_HEAD_BUCKETS_DELETED_DESC);

    public static final String N_TAIL_BUCKETS_DELETED_NAME =
        "nTailBucketsDeleted";
    public static final String N_TAIL_BUCKETS_DELETED_DESC =
        "Number of VLSN index buckets deleted at the tail(the high end) of " +
            "the index.";
    public static final StatDefinition N_TAIL_BUCKETS_DELETED =
        new StatDefinition(
            N_TAIL_BUCKETS_DELETED_NAME,
            N_TAIL_BUCKETS_DELETED_DESC);

    public static final String N_BUCKETS_CREATED_NAME =
        "nBucketsCreated";
    public static final String N_BUCKETS_CREATED_DESC =
        "Number of new VLSN buckets created in the VLSN index.";
    public static final StatDefinition N_BUCKETS_CREATED =
        new StatDefinition(
            N_BUCKETS_CREATED_NAME,
            N_BUCKETS_CREATED_DESC);
}
