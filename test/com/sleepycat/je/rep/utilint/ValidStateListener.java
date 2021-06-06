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

package com.sleepycat.je.rep.utilint;

import java.util.concurrent.CountDownLatch;

import com.sleepycat.je.rep.StateChangeEvent;
import com.sleepycat.je.rep.StateChangeListener;

public class ValidStateListener implements StateChangeListener {
    CountDownLatch waitForValidState = new CountDownLatch(1);

    public void stateChange(StateChangeEvent stateChangeEvent) {
        if (stateChangeEvent.getState().isActive()) {
            waitForValidState.countDown();
        }
    }

    public void awaitValidState()
        throws InterruptedException {

        waitForValidState.await();
    }
}
