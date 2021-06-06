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

package com.sleepycat.je.dbi;

/**
 * Internal class used to distinguish which variety of getXXX() that
 * Cursor.retrieveNext should use.
 */
public enum GetMode {
    NEXT("NEXT", true),
    PREV("PREV", false),
    NEXT_DUP("NEXT_DUP", true),
    PREV_DUP("PREV_DUP", false),
    NEXT_NODUP("NEXT_NODUP", true),
    PREV_NODUP("PREV_NODUP", false);

    private String name;
    private boolean forward;

    private GetMode(String name, boolean forward) {
        this.name = name;
        this.forward = forward;
    }

    public final boolean isForward() {
        return forward;
    }

    @Override
    public String toString() {
        return name;
    }
}
