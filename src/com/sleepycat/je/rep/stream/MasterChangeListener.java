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
package com.sleepycat.je.rep.stream;

import java.net.InetSocketAddress;
import java.util.logging.Logger;

import com.sleepycat.je.rep.elections.Learner;
import com.sleepycat.je.rep.elections.MasterValue;
import com.sleepycat.je.rep.elections.Proposer.Proposal;
import com.sleepycat.je.rep.elections.Protocol.Value;
import com.sleepycat.je.rep.impl.node.RepNode;
import com.sleepycat.je.utilint.LoggerUtils;

/**
 * The Listener registered with Elections to learn about new Masters
 */
public class MasterChangeListener implements Learner.Listener {

    /* The Value that is "current" for this Node. */
    private Value currentValue = null;

    private final RepNode repNode;
    private final Logger logger;

    public MasterChangeListener(RepNode repNode) {
        this.repNode = repNode;
        logger = LoggerUtils.getLogger(getClass());
    }

    /**
     * Implements the Listener protocol. The method should not have any
     * operations that might wait, since notifications are single threaded.
     */
    @Override
    public void notify(Proposal proposal, Value value) {

        try {
            repNode.getVLSNFreezeLatch().vlsnEvent(proposal);
            /* We have a new proposal, is it truly different? */
            if (value.equals(currentValue)) {
                LoggerUtils.fine(logger, repNode.getRepImpl(),
                                 "Master change listener -- no value change." +
                                 "Proposal: " + proposal + " Value: " + value);
                return;
            }

            MasterValue masterValue = ((MasterValue) value);

            LoggerUtils.fine(logger, repNode.getRepImpl(),
                    "Master change listener notified. Proposal:" +
                    proposal + " Value: " + value);
            LoggerUtils.info(logger, repNode.getRepImpl(),
                    "Master changed to " +
                     masterValue.getNameId().getName());

            repNode.getMasterStatus().setGroupMaster
                (masterValue.getHostName(),
                 masterValue.getPort(),
                 masterValue.getNameId());

            /* Propagate the information to any monitors. */
            repNode.getElections().asyncInformMonitors(proposal, value);
        } finally {
            currentValue = value;
        }
    }
}
