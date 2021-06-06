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

package com.sleepycat.je;

import java.io.Serializable;

/**
 * Describes the result of the {@link com.sleepycat.je.Database#preload
 * Database.preload} operation.
 */
public class PreloadStatus implements Serializable {

    private static final long serialVersionUID = 903470137L;

        /* For toString. */
    private String statusName;

    /* Make the constructor public for serializability testing. */
    public PreloadStatus(String statusName) {
        this.statusName = statusName;
    }

    @Override
    public String toString() {
        return "PreloadStatus." + statusName;
    }

    /**
     * {@link com.sleepycat.je.Database#preload Database.preload}
     * was successful.
     */
    public static final PreloadStatus SUCCESS =
        new PreloadStatus("SUCCESS");

    /**
     * {@link com.sleepycat.je.Database#preload Database.preload}
     * filled maxBytes of the cache.
     */
    public static final PreloadStatus FILLED_CACHE =
        new PreloadStatus("FILLED_CACHE");

    /**
     * {@link com.sleepycat.je.Database#preload Database.preload}
     * took more than maxMillisecs.
     */
    public static final PreloadStatus EXCEEDED_TIME =
        new PreloadStatus("EXCEEDED_TIME");

    /**
     * The user requested that preload stop during a call to
     * ProgressListener.progress().
     */
    public static final PreloadStatus USER_HALT_REQUEST =
        new PreloadStatus("USER_HALT_REQUEST");
}
