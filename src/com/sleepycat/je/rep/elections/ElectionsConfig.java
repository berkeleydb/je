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

package com.sleepycat.je.rep.elections;

import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.impl.node.NameIdPair;
import com.sleepycat.je.rep.impl.node.RepNode;
import com.sleepycat.je.rep.utilint.ServiceDispatcher;

public interface ElectionsConfig {

    /**
     * Gets the replication group name.
     * @return group name
     */
    public String getGroupName();

    /**
     * Gets the nodes NameIdPair.
     * @return NameIdPair
     */
    public NameIdPair getNameIdPair();

    /**
     * Gets the ServiceDispatcher.
     * @return ServiceDispatcher
     */
    public ServiceDispatcher getServiceDispatcher();

    /**
     * Gets the election priority.
     * @return election priority
     */
    public int getElectionPriority();

    /**
     * Gets the JE log version.
     * @return log version
     */
    public int getLogVersion();

    /**
     * Gets the RepImpl.
     * @return RepImpl
     */
    public RepImpl getRepImpl();

    /**
     * Get the RepNode. May be null if the Elections
     * object is not used for the initiation of
     * an election.
     * @return RepNode
     */
    public RepNode getRepNode();
}
