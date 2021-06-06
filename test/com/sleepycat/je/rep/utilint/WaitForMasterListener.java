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

import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.StateChangeEvent;
import com.sleepycat.je.rep.StateChangeListener;

public class WaitForMasterListener implements StateChangeListener {
    CountDownLatch waitForMaster = new CountDownLatch(1);
    private boolean success = true;

    public void stateChange(StateChangeEvent stateChangeEvent) {
        if (stateChangeEvent.getState().equals
            (ReplicatedEnvironment.State.MASTER)) {
            waitForMaster.countDown();
        }

        if (stateChangeEvent.getState().isDetached()) {
            /* It will never return to the replica state. */
            success = false;
            waitForMaster.countDown();
        }
    }

    public boolean awaitMastership()
        throws InterruptedException {

        waitForMaster.await();
        return success;
    }
}
