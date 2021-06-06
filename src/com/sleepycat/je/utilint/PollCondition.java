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

package com.sleepycat.je.utilint;

/**
 * Utility class that permits a "poll based" waiting for a condition.
 */
public abstract class PollCondition {

    private final long checkPeriodMs;
    private final long timeoutMs;

    public PollCondition(long checkPeriodMs,
                         long timeoutMs) {
        super();
        assert checkPeriodMs <= timeoutMs;
        this.checkPeriodMs = checkPeriodMs;
        this.timeoutMs = timeoutMs;
    }

    protected abstract boolean condition();

    public boolean await() {

        if (condition()) {
            return true;
        }

        final long timeLimit = System.currentTimeMillis() + timeoutMs;
        do {
            try {
                Thread.sleep(checkPeriodMs);
            } catch (InterruptedException e) {
                return false;
            }
            if (condition()) {
                return true;
            }
        } while (System.currentTimeMillis() < timeLimit);

        return false;
    }
}
