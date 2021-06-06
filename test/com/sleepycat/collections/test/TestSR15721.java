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

package com.sleepycat.collections.test;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.sleepycat.collections.CurrentTransaction;
import com.sleepycat.je.Environment;
import com.sleepycat.util.test.TestBase;
import com.sleepycat.util.test.TestEnv;

/**
 * @author Chao Huang
 */
public class TestSR15721 extends TestBase {

    private Environment env;
    private CurrentTransaction currentTxn;

    @Before
    public void setUp()
        throws Exception {

        env = TestEnv.TXN.open("TestSR15721");
        currentTxn = CurrentTransaction.getInstance(env);
    }

    @After
    public void tearDown() {
        try {
            if (env != null) {
                env.close();
            }
        } catch (Exception e) {
            System.out.println("Ignored exception during tearDown: " + e);
        } finally {
            /* Ensure that GC can cleanup. */
            env = null;
            currentTxn = null;
        }
    }

    /**
     * Tests that the CurrentTransaction instance doesn't indeed allow GC to
     * reclaim while attached environment is open. [#15721]
     */
    @Test
    public void testSR15721Fix()
        throws Exception {

        int hash = currentTxn.hashCode();
        int hash2 = -1;

        currentTxn = CurrentTransaction.getInstance(env);
        hash2 = currentTxn.hashCode();
        assertTrue(hash == hash2);

        currentTxn.beginTransaction(null);
        currentTxn = null;
        hash2 = -1;

        for (int i = 0; i < 10; i += 1) {
            byte[] x = null;
            try {
                 x = new byte[Integer.MAX_VALUE - 1];
                 fail();
            } catch (OutOfMemoryError expected) {
            }
            assertNull(x);

            System.gc();
        }

        currentTxn = CurrentTransaction.getInstance(env);
        hash2 = currentTxn.hashCode();
        currentTxn.commitTransaction();

        assertTrue(hash == hash2);
    }
}
