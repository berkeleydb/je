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

package com.sleepycat.je.txn;

import java.util.Collections;
import java.util.Map;
import java.util.WeakHashMap;

import com.sleepycat.je.utilint.LoggerUtils;

/**
 * LockInfo is a class that embodies information about a lock instance.  The
 * holding thread and the locktype are all contained in the object.
 *
 * This class is public for unit tests.
 */
public class LockInfo implements Cloneable {
    protected Locker locker;
    protected LockType lockType;

    private static boolean deadlockStackTrace = false;
    private static Map<LockInfo, StackTraceAtLockTime> traceExceptionMap =
        Collections.synchronizedMap(new WeakHashMap<LockInfo, 
                                    StackTraceAtLockTime>());
    @SuppressWarnings("serial")
    private static class StackTraceAtLockTime extends Exception {}

    /**
     * Called when the je.txn.deadlockStackTrace property is changed.
     */
    static void setDeadlockStackTrace(boolean enable) {
        deadlockStackTrace = enable;
    }

    /**
     * For unit testing only.
     */
    public static boolean getDeadlockStackTrace() {
        return deadlockStackTrace;
    }

    /**
     * Construct a new LockInfo.  public for Sizeof program.
     */
    public LockInfo(Locker locker, LockType lockType) {
        this.locker = locker;
        this.lockType = lockType;

        if (deadlockStackTrace) {
            traceExceptionMap.put(this, new StackTraceAtLockTime());
        }
    }

    /**
     * Clone from given LockInfo.  Use this constructor when copying a LockInfo
     * and its identity should be copied (e.g., when mutating a thin lock to a
     * thick lock) to ensure that debugging info is retained.
     */
    LockInfo(LockInfo other) {
        this.locker = other.locker;
        this.lockType = other.lockType;

        if (deadlockStackTrace) {
            traceExceptionMap.put(this, traceExceptionMap.get(other));
        }
    }

    /**
     * Change this lockInfo over to the prescribed locker.
     */
    void setLocker(Locker locker) {
        this.locker = locker;
    }

    /**
     * @return The transaction associated with this Lock.
     */
    public Locker getLocker() {
        return locker;
    }

    /**
     * @return The LockType associated with this Lock.
     */
    void setLockType(LockType lockType) {
        this.lockType = lockType;
    }

    /**
     * @return The LockType associated with this Lock.
     */
    LockType getLockType() {
        return lockType;
    }

    @Override
    public Object clone()
        throws CloneNotSupportedException {

        return super.clone();
    }

    /**
     * Debugging
     */
    public void dump() {
        System.out.println(this);
    }

    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder(500);

        buf.append("<LockInfo locker=\"");
        buf.append(locker);
        buf.append("\" type=\"");
        buf.append(lockType);
        buf.append("\"/>");

        if (deadlockStackTrace) {
            Exception traceException = traceExceptionMap.get(this);
            if (traceException != null) {
                buf.append(" lock taken at: ");
                buf.append(LoggerUtils.getStackTrace(traceException));
            }
        }

        return buf.toString();
    }
}
