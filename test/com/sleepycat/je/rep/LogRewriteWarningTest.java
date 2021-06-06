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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.Set;
import java.util.concurrent.Callable;

import org.junit.Before;
import org.junit.Test;

import com.sleepycat.je.Database;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.rep.impl.RepTestBase;
import com.sleepycat.je.rep.utilint.RepTestUtils;
import com.sleepycat.je.rep.utilint.RepTestUtils.RepEnvInfo;

/**
 * Simple tests for the log rewrite notification feature.
 */
public class LogRewriteWarningTest extends RepTestBase {

    @Before
    public void setUp() 
        throws Exception {
        
        groupSize = 3;
        super.setUp();
    }

    /**
     * Verifies that if there has been no hard recovery, we don't get any
     * spurious notification.
     */
    @Test
    public void testUsuallyQuiet() throws Exception {
        checkRollbacks(false);
    }

    /**
     * Verifies that hard recovery generates the desired notification callback.
     */
    @Test
    public void testNotification() throws Exception {
        checkRollbacks(true);
    }

    /**
     * @param hard whether to do hard recovery or not.
     */
    private void checkRollbacks(boolean hard) throws Exception {
        createGroup();
        ReplicatedEnvironment env = repEnvInfo[0].getEnv();
        Database db = env.openDatabase(null, TEST_DB_NAME, dbconfig);

        boolean left = false;
        if (hard) {
            leaveGroupAllButMaster();
            left = true;
            
            /*
             * Write a transaction at the master, at a time when the other two
             * nodes are not running, so they won't receive the update.
             */ 
            Transaction txn =
                env.beginTransaction(null, RepTestUtils.SYNC_SYNC_NONE_TC);
            db.put(txn, key, data);
            txn.commit();
        } else {
            db.put(null, key, data);
        }
        db.close();

        closeNodes(repEnvInfo[0]);
        if (left) {
            /*
             * Restart the other two nodes, after shutting down the master.
             * The other two nodes will elect a new master bewteen them, and
             * this new master will lack the transaction we created above.
             */ 
            restartNodes(repEnvInfo[1], repEnvInfo[2]);
        } else {
            /*
             * Wait for a new master to be elected, because we want to make
             * sure the old master doesn't become master again when we restart
             * it.
             */
            final RepEnvInfo node2 = repEnvInfo[1];
            final RepEnvInfo node3 = repEnvInfo[2];
            RepTestUtils.awaitCondition(new Callable<Boolean>() {
                    public Boolean call() {
                        return node2.isMaster() || node3.isMaster();
                    }
                });
        }

        /*
         * Restart the former master.  It will sync with the new master, which
         * will result in a rollback of a committed transaction.
         */
        final boolean warned[] = new boolean[1];
        repEnvInfo[0].getRepConfig().setLogFileRewriteListener
            (new LogFileRewriteListener() {
                    public void rewriteLogFiles(Set<File> files) {
                        warned[0] = true;
                    }
                });
        restartNodes(repEnvInfo[0]);
        if (hard) {
            assertTrue(warned[0]);
        } else {
            /*
             * We haven't done a hard recovery, so the callback should not have
             * been invoked.
             */ 
            assertFalse(warned[0]);
        }
    }

    /**
     * Verifies that an exception that occurs in the user's callback results in
     * failure to open the environment, and is preserved as the "cause"
     * reported back to the caller.
     */
    @Test
    public void testException() throws Exception {
        createGroup();
        ReplicatedEnvironment env = repEnvInfo[0].getEnv();
        Database db = env.openDatabase(null, TEST_DB_NAME, dbconfig);

        /*
         * Use the same technique as other tests in this class to produce a
         * hard recovery scenario.
         */ 
        leaveGroupAllButMaster();
        Transaction txn =
            env.beginTransaction(null, RepTestUtils.SYNC_SYNC_NONE_TC);
        db.put(txn, key, data);
        txn.commit();
        db.close();

        closeNodes(repEnvInfo[0]);
        restartNodes(repEnvInfo[1], repEnvInfo[2]);

        final RuntimeException problem =
            new RuntimeException("application problem in callback");
        repEnvInfo[0].getRepConfig().setLogFileRewriteListener
            (new LogFileRewriteListener() {
                    public void rewriteLogFiles(Set<File> files) {
                        throw problem;
                    }
                });
        try {
            repEnvInfo[0].openEnv();
        } catch (EnvironmentFailureException e) {
            assertSame(problem, e.getCause());
        }
    }
}
