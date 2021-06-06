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

package com.sleepycat.je.tree;

import static org.junit.Assert.assertNull;

import java.io.File;

import com.sleepycat.collections.CurrentTransaction;
import com.sleepycat.collections.TransactionRunner;
import com.sleepycat.collections.TransactionWorker;
import com.sleepycat.compat.DbCompat;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.RunRecoveryException;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.je.utilint.JVMSystemUtils;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

import org.junit.After;
import org.junit.Test;

/**
 */
public class SR13126Test extends TestBase {

    private final File envHome;
    private Environment env;
    private Database db;
    private long maxMem;

    public SR13126Test() {
        envHome = SharedTestUtils.getTestDir();
    }

    @After
    public void tearDown() {
        try {
            if (env != null) {
                env.close();
            }
        } catch (Exception e) {
            System.out.println("During tearDown: " + e);
        }

        env = null;
        db = null;
    }

    private boolean open()
        throws DatabaseException {

        maxMem = JVMSystemUtils.getRuntimeMaxMemory();
        if (maxMem == -1) {
            System.out.println
                ("*** Warning: not able to run this test because the JVM " +
                 "heap size is not available");
            return false;
        }

        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setAllowCreate(true);
        envConfig.setTransactional(true);
        /* Do not run the daemons to avoid timing considerations. */
        envConfig.setConfigParam
            (EnvironmentParams.ENV_RUN_CLEANER.getName(), "false");
        envConfig.setConfigParam
            (EnvironmentParams.ENV_RUN_EVICTOR.getName(), "false");
        envConfig.setConfigParam
            (EnvironmentParams.ENV_RUN_CHECKPOINTER.getName(), "false");
        envConfig.setConfigParam
            (EnvironmentParams.ENV_RUN_INCOMPRESSOR.getName(), "false");
        env = new Environment(envHome, envConfig);

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setTransactional(true);
        db = env.openDatabase(null, "foo", dbConfig);

        return true;
    }

    private void close()
        throws DatabaseException {

        db.close();
        db = null;

        env.close();
        env = null;
    }

    @Test
    public void testSR13126()
        throws DatabaseException {

        if (!open()) {
            return;
        }

        Transaction txn = env.beginTransaction(null, null);

        try {
            insertUntilOutOfMemory(txn);
            /* OOME outside of put() -- fall through. */
            txn.abort();
            db.close();
        } catch (OutOfMemoryError expected) {
        } catch (RunRecoveryException expected) {
        }

        verifyDataAndClose();
    }

    @Test
    public void testTransactionRunner()
        throws Exception {

        if (!open()) {
            return;
        }

        final CurrentTransaction currentTxn =
            CurrentTransaction.getInstance(env);

        TransactionRunner runner = new TransactionRunner(env);
        /* Don't print exception stack traces during test runs. */
        DbCompat.TRANSACTION_RUNNER_PRINT_STACK_TRACES = false;
        try {
            runner.run(new TransactionWorker() {
                public void doWork()
                    throws Exception {

                    insertUntilOutOfMemory(currentTxn.getTransaction());
                }
            });
            /* OOME outside of put() -- env is not invalid. */
            db.close();
            return;
        } catch (OutOfMemoryError expected) {
        } catch (RunRecoveryException expected) {
        }

        /*
         * If TransactionRunner does not abort the transaction, this thread
         * will be left with a transaction attached.
         */
        assertNull(currentTxn.getTransaction());

        verifyDataAndClose();
    }

    private void insertUntilOutOfMemory(Transaction txn)
        throws DatabaseException, OutOfMemoryError {

        DatabaseEntry key = new DatabaseEntry(new byte[1]);
        DatabaseEntry data = new DatabaseEntry();

        int startMem = (int) (maxMem / 3);
        int bumpMem = (int) ((maxMem - maxMem / 3) / 5);

        /* Insert larger and larger LNs until an OutOfMemoryError occurs. */
        for (int memSize = startMem;; memSize += bumpMem) {

            /*
             * If the memory error occurs when we do "new byte[]" below, this
             * is not a test of the bug in question.
             */
            try {
                data.setData(new byte[memSize]);
            } catch (OutOfMemoryError e) {
                System.out.println("OOME outside of put(), invalid test.");
                return;
            }

            try {
                db.put(null, key, data);
            } catch (OutOfMemoryError e) {
                //System.err.println("Error during write " + memSize);
                throw e;
            }
        }
    }

    private void verifyDataAndClose()
        throws DatabaseException {

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();

        /*
         * If a NULL_LSN is present in a BIN entry because of an incomplete
         * insert, an assertion will fire during the checkpoint when writing
         * the BIN.
         */
        env.close();
        env = null;

        /*
         * If the NULL_LSN was written above because assertions are disabled,
         * check that we don't get an exception when fetching it.
         */
        open();
        Cursor c = db.openCursor(null, null);
        while (c.getNext(key, data, null) == OperationStatus.SUCCESS) {}
        c.close();
        close();
    }
}
