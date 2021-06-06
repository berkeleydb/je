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

import com.sleepycat.je.rep.NodeType;
import com.sleepycat.je.rep.impl.RepGroupImpl;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.impl.node.NameIdPair;
import com.sleepycat.je.rep.utilint.NamedChannel;
import com.sleepycat.je.rep.utilint.RepUtils.Clock;

public interface ReplicaFeederHandshakeConfig {

    /**
     * Gets the RepImpl.
     * @return RepImpl
     */
    public RepImpl getRepImpl();

    /**
     * Gets the nodes NameIdPair.
     * @return NameIdPair
     */
    public NameIdPair getNameIdPair();

    /**
     * Gets the clock.
     * @return Clock
     */
    public Clock getClock();

    /**
     * Gets the NodeType.
     * @return NodeType
     */
    public NodeType getNodeType();

    /**
     * Gets the RepGroupImpl.
     * @return RepGroupImpl
     */
    public RepGroupImpl getGroup();

    /**
     * Gets the NamedChannel.
     * @return NamedChannel
     */
    public NamedChannel getNamedChannel();
}
