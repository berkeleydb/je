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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import com.sleepycat.je.rep.elections.Proposer.Proposal;
import com.sleepycat.je.rep.impl.node.CommitFreezeLatch;

public class VLSNFreezeLatchTest {

    private CommitFreezeLatch latch = new CommitFreezeLatch();
    /* A sequential series of proposals */
    private Proposal p1, p2, p3;

    @Before
    public void setUp() 
        throws Exception {
        
        latch = new CommitFreezeLatch();
        latch.setTimeOut(10 /* ms */);
        TimebasedProposalGenerator pg = new TimebasedProposalGenerator(1);
        p1 = pg.nextProposal();
        p2 = pg.nextProposal();
        p3 = pg.nextProposal();
    }

    @Test
    public void testTimeout()
        throws InterruptedException {

        latch.freeze(p2);
        // Earlier event does not release waiters
        latch.vlsnEvent(p1);

        assertFalse(latch.awaitThaw());
        assertEquals(1, latch.getAwaitTimeoutCount());
    }

    @Test
    public void testElection()
        throws InterruptedException {

        latch.freeze(p2);
        latch.vlsnEvent(p2);
        assertTrue(latch.awaitThaw());
        assertEquals(1, latch.getAwaitElectionCount());
    }

    @Test
    public void testNewerElection()
        throws InterruptedException {

        latch.freeze(p2);
        latch.vlsnEvent(p3);
        assertTrue(latch.awaitThaw());
        assertEquals(1, latch.getAwaitElectionCount());
    }

    @Test
    public void testNoFreeze()
        throws InterruptedException {

        latch.vlsnEvent(p1);

        assertFalse(latch.awaitThaw());
        assertEquals(0, latch.getAwaitTimeoutCount());
    }
}
