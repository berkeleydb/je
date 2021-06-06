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

import static org.junit.Assert.assertTrue;

import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.rep.InsufficientLogException;
import com.sleepycat.je.rep.NetworkRestore;
import com.sleepycat.je.rep.NetworkRestoreConfig;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.impl.RepParams;
import com.sleepycat.je.rep.impl.RepTestBase;
import com.sleepycat.je.rep.utilint.RepTestUtils;
import com.sleepycat.je.rep.utilint.RepTestUtils.RepEnvInfo;
import com.sleepycat.je.rep.vlsn.VLSNRange;
import com.sleepycat.je.utilint.VLSN;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MinRetainedVLSNsTest extends RepTestBase {

    @Override
    @Before
    public void setUp() throws Exception {
        groupSize = 4;
        super.setUp();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
    }

    /**
     * Test old behavior with no retained VLSNS
     */
    @Test
    public void testNoneRetained() {
        retainedInternalTest(0);
    }

    @Test
    public void testRetained() {
        retainedInternalTest(1000);
    }

    /**
     * Test to ensure that at least the ninimum number of configured VLSNs
     * is maintained.
     */
    public void retainedInternalTest(int minRetainedVLSNs) {
        setRepConfigParam(RepParams.MIN_RETAINED_VLSNS,
                          Integer.toString(minRetainedVLSNs));

        /*
         * For rapid updates of the global cbvlsn as new log files are created
         */
        setEnvConfigParam(EnvironmentParams.LOG_FILE_MAX, "4000");
        createGroup(3);

        final ReplicatedEnvironment master = repEnvInfo[0].getEnv();
        populateDB(master, 1000);

        /* Create garbage by overwriting */
        populateDB(master, 1000);

        /* Sync group. */
        RepTestUtils.syncGroup(repEnvInfo);

        checkGlobalCBVLSN();

        /*
         * Open a new environment. It must be able to syncup or network
         * restore.
         */
        try {
            repEnvInfo[repEnvInfo.length - 1].openEnv();
        } catch (InsufficientLogException ile) {
            new NetworkRestore().execute(ile, new NetworkRestoreConfig());
            repEnvInfo[repEnvInfo.length - 1].openEnv();
        }

        checkGlobalCBVLSN();
    }

    private void checkGlobalCBVLSN() {
        for (RepEnvInfo info : repEnvInfo) {
            if (info.getEnv() == null) {
                continue;
            }
            final int minRetainedVLSNs = Integer.parseInt(info.getRepConfig().
               getConfigParam(RepParams.MIN_RETAINED_VLSNS.getName()));
            final VLSN groupCBVLSN = info.getRepNode().getGlobalCBVLSN();
            final VLSNRange range = info.getRepImpl().getVLSNIndex().getRange();
            final long retainedVLSNs = range.getLast().getSequence() -
                        groupCBVLSN.getSequence();

            assertTrue(retainedVLSNs >= minRetainedVLSNs);
        }
    }
}
