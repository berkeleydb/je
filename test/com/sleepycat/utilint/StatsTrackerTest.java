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

package com.sleepycat.utilint;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.logging.Logger;

import org.junit.Test;

import com.sleepycat.util.test.TestBase;

public class StatsTrackerTest extends TestBase {

    enum TestType {GET, PUT, DELETE} ;

    /*
     * Test thread stack trace dumping
     */
    @Test
    public void testActivityCounter() 
        throws InterruptedException {
     
        Integer maxNumThreadDumps = 3;

        Logger logger = Logger.getLogger("test");
        StatsTracker<TestType> tracker = 
            new StatsTracker<TestType>(TestType.values(),
                                       logger,
                                       2,
                                       1,
                                       maxNumThreadDumps,
                                       100);

        /* 
         * If there is only one concurrent thread, there should be no thread 
         * dumps.
         */
        for (int i = 0; i < 20; i++) {
            long startA = tracker.markStart();
            Thread.sleep(10);
            tracker.markFinish(TestType.GET, startA);
        }

        /* Did we see some thread dumps? */
        assertEquals(0, tracker.getNumCompletedDumps() );

        /*
         * Simulate three concurrent threads. There should be automatic thread
         * dumping, because the tracker is configured to dump when there are
         * more than two concurrent threads with operations of > 1 ms.
         */
        for (int i = 0; i < 20; i++) {
            long startA = tracker.markStart();
            long startB = tracker.markStart();
            long startC = tracker.markStart();
            Thread.sleep(10);
            tracker.markFinish(TestType.GET, startA);
            tracker.markFinish(TestType.GET, startB);
            tracker.markFinish(TestType.GET, startC);
        }

        long expectedMaxDumps = maxNumThreadDumps;

        /* Did we see some thread dumps? */
        assertTrue(tracker.getNumCompletedDumps() > 1);
        assertTrue(tracker.getNumCompletedDumps() <= expectedMaxDumps);
    }
}
