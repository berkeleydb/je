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
 * Contains the result of a tree search
 */
public class SearchResult {

    public boolean exactParentFound;
    public IN parent;
    public int index;
    /*
     * Set to true if a search stopped because a child was not resident, and
     * we are doing a do-not-fetch kind of search.
     */
    public boolean childNotResident;
        
    public SearchResult() {
        reset();
    }

    public void reset() {
        exactParentFound = false;
        parent = null;
        index = -1;
        childNotResident = false;
    }

    @Override
    public String toString() {
        return
            "exactParentFound="+ exactParentFound +
            " parent=" + ((parent == null)? "null":
                          Long.toString(parent.getNodeId())) +
            " index=" + index +
            " childNotResident=" + childNotResident;
    }
}
