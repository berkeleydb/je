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

package com.sleepycat.je.recovery;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import com.sleepycat.je.CheckpointConfig;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.config.EnvironmentParams;

public class RecoveryDeleteTest extends RecoveryTestBase {

    @Override
    protected void setExtraProperties() {
        envConfig.setConfigParam(
                      EnvironmentParams.ENV_RUN_INCOMPRESSOR.getName(),
                      "false");
    }

    /* Make sure that we can recover after the entire tree is compressed away. */
    @Test
    public void testDeleteAllAndCompress()
        throws Throwable {

        createEnvAndDbs(1 << 20, false, NUM_DBS);
        int numRecs = 10;

        try {
            // Set up an repository of expected data
            Map<TestData, Set<TestData>> expectedData =
                new HashMap<TestData, Set<TestData>>();

            // insert all the data
            Transaction txn = env.beginTransaction(null, null);
            insertData(txn, 0, numRecs -1 , expectedData, 1, true, NUM_DBS);
            txn.commit();

            /*
             * Do two checkpoints here so that the INs that make up this new
             * tree are not in the redo part of the log.
             */
            CheckpointConfig ckptConfig = new CheckpointConfig();
            ckptConfig.setForce(true);
            env.checkpoint(ckptConfig);
            env.checkpoint(ckptConfig);
            txn = env.beginTransaction(null, null);
            insertData(txn, numRecs, numRecs + 1, expectedData, 1, true, NUM_DBS);
            txn.commit();

            /* delete all */
            txn = env.beginTransaction(null, null);
            deleteData(txn, expectedData, true, true, NUM_DBS);
            txn.commit();

            /* This will remove the root. */
            env.compress();

            closeEnv();
            recoverAndVerify(expectedData, NUM_DBS);
        } catch (Throwable t) {
            // print stacktrace before trying to clean up files
            t.printStackTrace();
            throw t;
        }
    }
}
