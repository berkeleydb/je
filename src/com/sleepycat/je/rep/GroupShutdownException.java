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

import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.dbi.EnvironmentFailureReason;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.impl.node.RepNode;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.VLSN;

/**
 * Thrown when an attempt is made to access an environment  that was
 * shutdown by the Master as a result of a call to
 * {@link ReplicatedEnvironment#shutdownGroup(long, TimeUnit)}.
 */
public class GroupShutdownException extends EnvironmentFailureException {
    private static final long serialVersionUID = 1;

    /* The time that the shutdown was initiated on the master. */
    private final long shutdownTimeMs;

    /* The master node that initiated the shutdown. */
    private final String masterNodeName;

    /* The VLSN at the time of shutdown */
    private final VLSN shutdownVLSN;

    /**
     * For internal use only.
     * @hidden
     */
    public GroupShutdownException(Logger logger,
                                  RepNode repNode,
                                  long shutdownTimeMs) {
        super(repNode.getRepImpl(),
              EnvironmentFailureReason.SHUTDOWN_REQUESTED,
              String.format("Master:%s, initiated shutdown at %1tc.",
                            repNode.getMasterStatus().getNodeMasterNameId().
                                getName(),
                            shutdownTimeMs));

        shutdownVLSN = repNode.getVLSNIndex().getRange().getLast();
        masterNodeName =
            repNode.getMasterStatus().getNodeMasterNameId().getName();
        this.shutdownTimeMs = shutdownTimeMs;

        LoggerUtils.warning(logger, repNode.getRepImpl(),
                            "Explicit shutdown request from Master:" +
                            masterNodeName);
    }

    /**
     * For internal use only.
     * @hidden
     */
    public GroupShutdownException(Logger logger,
                                 RepImpl repImpl,
                                 String masterNodeName,
                                 VLSN shutdownVLSN,
                                 long shutdownTimeMs) {
        super(repImpl,
                EnvironmentFailureReason.SHUTDOWN_REQUESTED,
                String.format("Master:%s, initiated shutdown at %1tc.",
                              masterNodeName,
                              shutdownTimeMs));

          this.shutdownVLSN = shutdownVLSN;
          this.masterNodeName = masterNodeName;
          this.shutdownTimeMs = shutdownTimeMs;

          LoggerUtils.warning(logger, repImpl,
                              "Explicit shutdown request from Master:" +
                              masterNodeName);

    }

    /**
     * For internal use only.
     * @hidden
     */
    private GroupShutdownException(String message,
                                   GroupShutdownException shutdownException) {
        super(message, shutdownException);
        shutdownVLSN = shutdownException.shutdownVLSN;
        shutdownTimeMs = shutdownException.shutdownTimeMs;
        masterNodeName = shutdownException.masterNodeName;
    }

    /**
     * For internal use only.
     * @hidden
     */
    @Override
    public GroupShutdownException wrapSelf(String msg) {
        return new GroupShutdownException(msg, this);
    }

    /**
     * For internal use only.
     *
     * Returns the shutdownVLSN, if it was available, at the time of the
     * exception
     *
     * @hidden
     */
    public VLSN getShutdownVLSN() {
        return shutdownVLSN;
    }
}
