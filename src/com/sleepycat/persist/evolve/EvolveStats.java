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

package com.sleepycat.persist.evolve;

/**
 * Statistics accumulated during eager entity evolution.
 *
 * @see com.sleepycat.persist.evolve Class Evolution
 * @author Mark Hayes
 */
public class EvolveStats {

    private int nRead;
    private int nConverted;

    EvolveStats() {
    }

    void add(int nRead, int nConverted) {
        this.nRead += nRead;
        this.nConverted += nConverted;
    }

    /**
     * The total number of entities read during eager evolution.
     *
     * @return the number of entities read.
     */
    public int getNRead() {
        return nRead;
    }

    /**
     * The total number of entities converted during eager evolution.
     *
     * @return the number of entities converted.
     */
    public int getNConverted() {
        return nConverted;
    }
}
