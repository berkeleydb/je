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
package com.sleepycat.je.rep.vlsn;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentMutableConfig;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.LogManager;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.impl.node.cbvlsn.LocalCBVLSNUpdater;
import com.sleepycat.je.rep.utilint.RepTestUtils;
import com.sleepycat.je.rep.utilint.RepTestUtils.RepEnvInfo;
import com.sleepycat.je.utilint.TestHook;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

import org.junit.Test;

/**
 * Make sure that the VLSNIndex.awaitConsistency method correctly checks for
 * invalidated environment and exits its while loop. Before the fix for
 * [#20919], the method could hang if a thread failed after it had allocated a
 * new VLSN, but before the vlsn mapping was entered into the VLSNIndex.
 */
public class VLSNAwaitConsistencyTest extends TestBase {

    private final File envRoot;

    public VLSNAwaitConsistencyTest() {
        envRoot = SharedTestUtils.getTestDir();
    }

    /**
     * Make a thread allocate a vlsn, but then fail before it's tracked 
     * by the vlsn index. This happened in [#20919] when
     * 1.rep environment close was called
     * 2.the repNode was nulled out
     * 3.a concurrent writing thread got a NPE within its call to LogManager.log
     *   because the repNode was null. This thread exited after it had bumped
     *   the vlsn, but before it had entered the vlsn in the vlsnIndex
     * 4.rep environment close tried to do a checkpoint, but the checkpoint 
     *   hung.
     * This fix works by having (3) invalidate the environment, and by having
     * (4) check for an invalidated environment.
     *
     */
    @Test
    public void testLoggingFailure()
        throws DatabaseException, IOException {

        /* Make a single replicated environment. */
        RepEnvInfo[] repEnvInfo = RepTestUtils.setupEnvInfos(envRoot, 1);
        RepTestUtils.joinGroup(repEnvInfo);

        /* 
         * Disable cleaning and CBVLSN updating, to control vlsn creation
         * explicitly.
         */
        Environment env = repEnvInfo[0].getEnv();
        EnvironmentMutableConfig config = env.getMutableConfig();
        config.setConfigParam("je.env.runCleaner", "false");
        env.setMutableConfig(config);
        LocalCBVLSNUpdater.setSuppressGroupDBUpdates(false);

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(true);
        dbConfig.setAllowCreate(true);
        Database db = env.openDatabase(null, "foo", dbConfig);
        DatabaseEntry value = new DatabaseEntry(new byte[4]);
        
        EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);
        LogManager logManager = 
            DbInternal.getNonNullEnvImpl(env).getLogManager();

        /* 
         * Inject an exception into the next call to log() that is made
         * for a replicated log entry.
         */
        logManager.setDelayVLSNRegisterHook(new ForceException());
        
        VLSNIndex vlsnIndex = ((RepImpl)envImpl).getVLSNIndex();

        try {
           db.put(null, value, value);
           fail("Should throw exception");
        } catch (Exception expected) {
            assertTrue("latest=" + vlsnIndex.getLatestAllocatedVal()  +
                       " last mapped=" + 
                       vlsnIndex.getRange().getLast().getSequence(),
                       vlsnIndex.getLatestAllocatedVal() > 
                       vlsnIndex.getRange().getLast().getSequence());
        }

        try {
            VLSNIndex.AWAIT_CONSISTENCY_MS = 1000;
            envImpl.awaitVLSNConsistency();
            fail("Should throw and break out");
        } catch (DatabaseException expected) {
        } 

        /* Before the fix, this test hung. */
    }
    
    private class ForceException implements TestHook<Object> {

        public void doHook() {
            throw new NullPointerException("fake NPE");
        }

        public void hookSetup() {
        }

        public void doIOHook() throws IOException {

        }

        public void doHook(Object obj) {
        }

        public CountDownLatch getHookValue() {
            return null;
        }        
    }
}