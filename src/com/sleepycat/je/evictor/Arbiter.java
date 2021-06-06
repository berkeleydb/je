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

package com.sleepycat.je.evictor;

import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.DbConfigManager;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.dbi.MemoryBudget;
import com.sleepycat.je.utilint.TestHook;

/**
 * The Arbiter determines whether eviction should occur, by consulting the
 * memory budget.
 */
class Arbiter {

    private final MemoryBudget.Totals memBudgetTotals;

    /* Debugging and unit test support. */
    private TestHook<Boolean> runnableHook;

    /* je.evictor.evictBytes */
    private final long evictBytesSetting;

    Arbiter(EnvironmentImpl envImpl) {

        DbConfigManager configManager = envImpl.getConfigManager();

        evictBytesSetting = configManager.getLong(
            EnvironmentParams.EVICTOR_EVICT_BYTES);

        memBudgetTotals = envImpl.getMemoryBudget().getTotals();
    }

    /**
     * Return true if the memory budget is overspent.
     */
    boolean isOverBudget() {

        return memBudgetTotals.getCacheUsage() >
            memBudgetTotals.getMaxMemory();
    }

    /**
     * Do a check on whether synchronous eviction is needed.
     *
     * Note that this method is intentionally not synchronized in order to
     * minimize overhead when checking for critical eviction.  This method is
     * called from application threads for every cursor operation.
     */
    boolean needCriticalEviction() {

        final long over = memBudgetTotals.getCacheUsage() -
            memBudgetTotals.getMaxMemory();

        return (over > memBudgetTotals.getCriticalThreshold());
    }

    /**
     * Do a check on whether the cache should still be subject to eviction.
     *
     * Note that this method is intentionally not synchronized in order to
     * minimize overhead, because it's checked on every iteration of the
     * evict batch loop.
     */
    boolean stillNeedsEviction() {

        return (memBudgetTotals.getCacheUsage() + evictBytesSetting) >
            memBudgetTotals.getMaxMemory();
    }

    /**
     * Return non zero number of bytes if eviction should happen. Caps the 
     * number of bytes a single thread will try to evict.
     */
    long getEvictionPledge() {

        long currentUsage  = memBudgetTotals.getCacheUsage();
        long maxMem = memBudgetTotals.getMaxMemory();

        long overBudget = currentUsage - maxMem;
        boolean doRun = (overBudget > 0);

        long requiredEvictBytes = 0;

        /* If running, figure out how much to evict. */
        if (doRun) {
            requiredEvictBytes = overBudget + evictBytesSetting;
            /* Don't evict more than 50% of the cache. */
            if (currentUsage - requiredEvictBytes < maxMem / 2) {
                requiredEvictBytes = overBudget + (maxMem / 2);
            }
        }

        /* Unit testing, force eviction. */
        if (runnableHook != null) {
            doRun = runnableHook.getHookValue();
            if (doRun) {
                requiredEvictBytes = maxMem;
            } else {
                requiredEvictBytes = 0;
            }
        }
        return requiredEvictBytes;
    }

    /* For unit testing only. */
    void setRunnableHook(TestHook<Boolean> hook) {
        runnableHook = hook;
    }
}
