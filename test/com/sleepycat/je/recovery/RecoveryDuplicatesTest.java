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

import com.sleepycat.je.Transaction;

public class RecoveryDuplicatesTest extends RecoveryTestBase {

    @Test
    public void testDuplicates()
        throws Throwable {

        createEnvAndDbs(1 << 20, true, NUM_DBS);
        int numRecs = 10;
        int numDups = N_DUPLICATES_PER_KEY;

        try {
            /* Set up an repository of expected data. */
            Map<TestData, Set<TestData>> expectedData = 
                new HashMap<TestData, Set<TestData>>();

            /* Insert all the data. */
            Transaction txn = env.beginTransaction(null, null);
            insertData(txn, 0, numRecs - 1, expectedData,
                       numDups, true, NUM_DBS);
            txn.commit();
            closeEnv();
            recoverAndVerify(expectedData, NUM_DBS);
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }

    @Test
    public void testDuplicatesWithDeletion()
        throws Throwable {

        createEnvAndDbs(1 << 20, true, NUM_DBS);
        int numRecs = 10;
        int nDups = N_DUPLICATES_PER_KEY;

        try {
            /* Set up an repository of expected data. */
            Map<TestData, Set<TestData>> expectedData = 
                new HashMap<TestData, Set<TestData>>();

            /* Insert all the data. */
            Transaction txn = env.beginTransaction(null, null);
            insertData(txn, 0, numRecs -1, expectedData, nDups, true, NUM_DBS);

            /* Delete all the even records. */
            deleteData(txn, expectedData, false, true, NUM_DBS);
            txn.commit();

            /* Modify all the records. */
            //    modifyData(expectedData);

            closeEnv();

            recoverAndVerify(expectedData, NUM_DBS);
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }

    @Test
    public void testDuplicatesWithAllDeleted()
        throws Throwable {

        createEnvAndDbs(1 << 20, true, NUM_DBS);
        int numRecs = 10;
        int nDups = N_DUPLICATES_PER_KEY;

        try {
            /* Set up an repository of expected data. */
            Map<TestData, Set<TestData>> expectedData = 
                new HashMap<TestData, Set<TestData>>();

            /* Insert all the data. */
            Transaction txn = env.beginTransaction(null, null);
            insertData(txn, 0, numRecs - 1, expectedData, nDups,
                       true, NUM_DBS);

            /* Delete all data. */
            deleteData(txn, expectedData, true, true, NUM_DBS);
            txn.commit();

            /* Modify all the records. */
            //    modifyData(expectedData);
            closeEnv();

            recoverAndVerify(expectedData, NUM_DBS);
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }
}
