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

import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.dbi.EnvironmentImpl;

public class LatchFactory {

    /**
     * Creates a SharedLatch using a given LatchContext.
     *
     * @param exclusiveOnly indicates whether this latch can only be set
     * exclusively (not shared).
     */
    public static SharedLatch createSharedLatch(final LatchContext context,
                                                final boolean exclusiveOnly) {
        if (exclusiveOnly) {
            return new LatchImpl(context);
        }
        return new SharedLatchImpl(false /*fair*/, context);
    }

    /**
     * Creates a SharedLatch, creating a LatchContext from the given name and
     * envImpl.
     *
     * @param exclusiveOnly indicates whether this latch can only be set
     * exclusively (not shared).
     */
    public static SharedLatch createSharedLatch(final EnvironmentImpl envImpl,
                                                final String name,
                                                final boolean exclusiveOnly) {
        if (exclusiveOnly) {
            return new LatchImpl(createContext(envImpl, name));
        }
        return new SharedLatchImpl(
            false /*fair*/, createContext(envImpl, name));
    }

    /**
     * Creates a Latch using a given LatchContext.
     *
     * @param collectStats is true to collect stats.  If false, a smaller and
     * faster implementation is used.
     */
    public static Latch createExclusiveLatch(final LatchContext context,
                                             final boolean collectStats) {
        if (collectStats) {
            return new LatchWithStatsImpl(context);
        }
        return new LatchImpl(context);
    }

    /**
     * Creates a Latch, creating a LatchContext from the given name and
     * envImpl.
     *
     * @param collectStats is true to collect stats.  If false, a smaller and
     * faster implementation is used.
     */
    public static Latch createExclusiveLatch(final EnvironmentImpl envImpl,
                                             final String name,
                                             final boolean collectStats) {
        if (collectStats) {
            return new LatchWithStatsImpl(createContext(envImpl, name));
        }
        return new LatchImpl(createContext(envImpl, name));
    }

    private static LatchContext createContext(final EnvironmentImpl envImpl,
                                              final String name) {
        return new LatchContext() {
            @Override
            public int getLatchTimeoutMs() {
                return envImpl.getLatchTimeoutMs();
            }
            @Override
            public String getLatchName() {
                return name;
            }
            @Override
            public LatchTable getLatchTable() {
                return LatchSupport.otherLatchTable;
            }
            @Override
            public EnvironmentImpl getEnvImplForFatalException() {
                return envImpl;
            }
        };
    }

    /**
     * Used for creating latches in tests, with having an EnvironmentImpl.
     */
    public static LatchContext createTestLatchContext(final String name) {
        return new LatchContext() {
            @Override
            public int getLatchTimeoutMs() {
                return 1000;
            }
            @Override
            public String getLatchName() {
                return name;
            }
            @Override
            public LatchTable getLatchTable() {
                return LatchSupport.otherLatchTable;
            }
            @Override
            public EnvironmentImpl getEnvImplForFatalException() {
                throw EnvironmentFailureException.unexpectedState();
            }
        };
    }
}
