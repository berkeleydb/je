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

package com.sleepycat.je.rep.impl.node;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.impl.RepTestBase;
import com.sleepycat.je.rep.utilint.RepTestUtils.RepEnvInfo;

/**
 * A master going up and down should result in an election being held even if
 * all the replicas still agree that it's the master. This is because the
 * other replicas may have more up to date logs than the master, which may
 * have lost transactions when it went down.
 *
 * SR17911 has more detail.
 */
public class MasterBounceTest extends RepTestBase {

    /* (non-Javadoc)
     * @see com.sleepycat.je.rep.impl.RepTestBase#setUp()
     */
    @Before
    public void setUp() 
        throws Exception {

        /*
         * A rep group of two effectively prevents another election from being
         * held across the bounce, since there is no election quorum.
         */
        groupSize = 2;
        super.setUp();
    }

    @Test
    public void testBounce() {
        createGroup();
        final RepEnvInfo masterInfo = repEnvInfo[0];
        ReplicatedEnvironment master = masterInfo.getEnv();
        assertTrue(master.getState().isMaster());

        /* No elections since the group grew around the first node. */
        assertEquals(0, masterInfo.getRepNode().getElections().
                        getElectionCount());

        masterInfo.closeEnv();
        masterInfo.openEnv();

        /* Verify that an election was held to select a new master. */
        assertEquals(1, masterInfo.getRepNode().getElections().
                        getElectionCount());
    }
}
