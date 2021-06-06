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

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import javax.transaction.xa.XAResource;

import org.junit.runners.Parameterized.Parameters;

import com.sleepycat.collections.TransactionRunner;
import com.sleepycat.collections.TransactionWorker;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockConflictException;
import com.sleepycat.je.XAEnvironment;
import com.sleepycat.je.log.LogUtils.XidImpl;
import com.sleepycat.util.ExceptionUnwrapper;
import com.sleepycat.util.test.TestEnv;
import com.sleepycat.utilint.StringUtils;

/**
 * Runs CollectionTest with special TestEnv and TransactionRunner objects to
 * simulate XA transactions.
 *
 * <p>This test is currently JE-only and will not compile on DB core.</p>
 */
public class XACollectionTest extends CollectionTest {

    @Parameters
    public static List<Object[]> genParams() {
        EnvironmentConfig config = new EnvironmentConfig();
        config.setTransactional(true);
        TestEnv xaTestEnv = new XATestEnv(config);
        List<Object[]> params = new ArrayList<Object[]>();
        for (int j = 0; j < TestStore.ALL.length; j += 1) {
            for (int k = 0; k < 2; k += 1) {
                boolean entityBinding = (k != 0);
                params.add(new Object[] 
                    {xaTestEnv, TestStore.ALL[j], entityBinding});
            }
        }
        
        return params;
    }
    
    public XACollectionTest(TestEnv testEnv,
                            TestStore testStore,
                            boolean isEntityBinding) {
        super(testEnv, testStore, isEntityBinding, false /*isAutoCommit*/, 
            false, DEFAULT_MAX_KEY);
    }

    @Override
    protected TransactionRunner newTransactionRunner(Environment env) {
        return new XARunner((XAEnvironment) env);
    }

    private static class XATestEnv extends TestEnv {

        private XATestEnv(EnvironmentConfig config) {
            super("XA", config);
        }

        @Override
        protected Environment newEnvironment(File dir,
                                             EnvironmentConfig config)
            throws DatabaseException {

            return new XAEnvironment(dir, config);
        }
    }

    private static class XARunner extends TransactionRunner {

        private final XAEnvironment xaEnv;
        private static int sequence;

        private XARunner(XAEnvironment env) {
            super(env);
            xaEnv = env;
        }

        @Override
        public void run(TransactionWorker worker)
            throws Exception {

            if (xaEnv.getThreadTransaction() == null) {
                for (int i = 0;; i += 1) {
                    sequence += 1;
                    XidImpl xid = new XidImpl
                        (1, StringUtils.toUTF8(String.valueOf(sequence)),
                         null);
                    try {
                        xaEnv.start(xid, XAResource.TMNOFLAGS);
                        worker.doWork();
                        int ret = xaEnv.prepare(xid);
                        xaEnv.end(xid, XAResource.TMSUCCESS);
                        if (ret != XAResource.XA_RDONLY) {
                            xaEnv.commit(xid, false);
                        }
                        return;
                    } catch (Exception e) {
                        e = ExceptionUnwrapper.unwrap(e);
                        try {
                            xaEnv.end(xid, XAResource.TMSUCCESS);
                            xaEnv.rollback(xid);
                        } catch (Exception e2) {
                            e2.printStackTrace();
                            throw e;
                        }
                        if (i >= getMaxRetries() ||
                            !(e instanceof LockConflictException)) {
                            throw e;
                        }
                    }
                }
            } else { /* Nested */
                try {
                    worker.doWork();
                } catch (Exception e) {
                    throw ExceptionUnwrapper.unwrap(e);
                }
            }
        }
    }
}
