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

import com.sleepycat.je.OperationFailureException;
import com.sleepycat.je.txn.Locker;

/**
 * Indicates that the underlying operation requires communication with a
 * Master, but that a Master was not available.
 * <p>
 * This exception typically indicates there is a system level problem. It could
 * indicate for example, that a sufficient number of nodes are not available to
 * hold an election and elect a Master, or that this node was having problems
 * with the network and was unable to communicate with other nodes.
 * <p>
 * The application can choose to retry the operation, potentially logging the
 * problem, until the underlying system level problem has been addressed.
 */
public class UnknownMasterException extends StateChangeException {
    private static final long serialVersionUID = 1;

    public UnknownMasterException(Locker locker,
                                  StateChangeEvent stateChangeEvent) {
        super(locker, stateChangeEvent);
    }

    /**
     * Used when the inability to determine a master is not related to a
     * state change.
     */
    public UnknownMasterException(String message) {
        super(message, null);
    }

    /**
     * Used when the inability to determine a master is not related to a
     * state change but some inability to communicate with a node identified
     * as a master. The reason contains further explanation.
     */
    public UnknownMasterException(String message, Exception reason) {
        super(message, reason);
    }

    private UnknownMasterException(String message,
                                   UnknownMasterException cause) {
        super(message, cause);
    }
    /**
     * For internal use only.
     * @hidden
     */
    @Override
    public OperationFailureException wrapSelf(String msg) {
        return new UnknownMasterException(msg, this);
    }
}
