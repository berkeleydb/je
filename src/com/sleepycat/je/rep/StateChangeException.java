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
package com.sleepycat.je.rep;

import java.util.Date;

import com.sleepycat.je.OperationFailureException;
import com.sleepycat.je.txn.Locker;

/**
 * Provides a synchronous mechanism for informing an application about a change
 * in the state of the replication node. StateChangeException is an abstract
 * class, with subtypes for each type of Transition.
 * <p>
 * A single state change can result in multiple state change exceptions (one
 * per thread operating against the environment). Each exception is associated
 * with the event that provoked the exception. The application can use this
 * association to ensure that each such event is processed just once.
 */
public abstract class StateChangeException extends OperationFailureException {
    private static final long serialVersionUID = 1;

    /* Null if the event is not available. */
    private final StateChangeEvent stateChangeEvent;

    /**
     * For internal use only.
     * @hidden
     */
    protected StateChangeException(Locker locker,
                                   StateChangeEvent stateChangeEvent) {
        super(locker, (locker != null),
              makeMessage(locker, stateChangeEvent), null);
        this.stateChangeEvent = stateChangeEvent;
    }

    /**
     * Used when no state change event is available
     */
    protected StateChangeException(String message, Exception reason) {
        super(null, false, message, reason);
        this.stateChangeEvent = null;
    }

    /**
     * Returns the event that resulted in this exception.
     *
     * @return the state change event
     */
    public StateChangeEvent getEvent() {
        return stateChangeEvent;
    }

    private static String makeMessage(Locker locker, StateChangeEvent event) {
        long lockerId = (locker == null) ? 0 : locker.getId();
        return (event != null) ?
              ("Problem closing transaction " + lockerId +
               ". The current state is:" + event.getState() + "." +
                " The node transitioned to this state at:" +
                 new Date(event.getEventTime())) :
               "Node state inconsistent with operation";
    }

    /**
     * For internal use only.
     * @hidden
     * Only for use by wrapSelf methods.
     */
    protected StateChangeException(String message,
                                   StateChangeException cause) {
        super(message, cause);
        stateChangeEvent =
            (cause != null) ? cause.stateChangeEvent : null;
    }
}
