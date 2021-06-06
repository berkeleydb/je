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

package com.sleepycat.je.latch;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Table of latches by thread for debugging.
 */
public class LatchTable {

    private ThreadLocal<Set<Object>> latchesByThread;
            
    LatchTable() {
        latchesByThread = new ThreadLocal<Set<Object>>();
    }

    /**
     * Adds latch acquired by this thread.
     * @return true if added, false if already present.
     */
    boolean add(Object latch) {
        Set<Object> threadLatches = latchesByThread.get();
        if (threadLatches == null) {
            threadLatches = new HashSet<Object>();
            latchesByThread.set(threadLatches);
        }
        return threadLatches.add(latch);
    }

    /**
     * Removes latch acquired by this thread.
     * @return true if removed, false if not present.
     */
    boolean remove(Object latch) {
        Set<Object> threadLatches = latchesByThread.get();
        if (threadLatches == null) {
            return false;
        } else {
            return threadLatches.remove(latch);
        }
    }

    /**
     * Returns the number of latches held by this thread.
     */
    int nLatchesHeld() {
        Set<Object> threadLatches = latchesByThread.get();
        if (threadLatches != null) {
            return threadLatches.size();
        } else {
            return 0;
        }
    }

    String latchesHeldToString() {
        Set<Object> threadLatches = latchesByThread.get();
        StringBuilder sb = new StringBuilder();
        if (threadLatches != null) {
            Iterator<Object> i = threadLatches.iterator();
            while (i.hasNext()) {
                sb.append(i.next()).append('\n');
            }
        }
        return sb.toString();
    }

    void clear() {
        latchesByThread = new ThreadLocal<Set<Object>>();
    }
}
