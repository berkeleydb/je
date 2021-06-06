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

package com.sleepycat.je.tree;

/**
 * Indicates that we need to return to the top of the tree in order to
 * do a forced splitting pass.  A checked exception is used to ensure that it
 * is handled internally and not propagated through the API.
 */
@SuppressWarnings("serial")
class SplitRequiredException extends Exception {
    public SplitRequiredException() {
    }
}
