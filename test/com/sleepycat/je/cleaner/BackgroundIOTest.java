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

package com.sleepycat.je.cleaner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.sleepycat.bind.tuple.TupleBase;
import com.sleepycat.bind.tuple.TupleOutput;
import com.sleepycat.je.CheckpointConfig;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.latch.LatchSupport;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.je.utilint.TestHook;

@RunWith(Parameterized.class)
public class BackgroundIOTest extends CleanerTestBase {

    final static int FILE_SIZE = 1000000;

    private static CheckpointConfig forceConfig;
    static {
        forceConfig = new CheckpointConfig();
        forceConfig.setForce(true);
    }

    private int readLimit;
    private int writeLimit;
    private int nSleeps;

    private boolean embeddedLNs = false;

    public BackgroundIOTest(boolean multiSubDir) {
        envMultiSubDir = multiSubDir;
        customName = envMultiSubDir ? "multi-sub-dir" : null ;
    }
    
    @Parameters
    public static List<Object[]> genParams() {
        
        return getEnv(new boolean[] {false, true});
    }

    @Test
    public void testBackgroundIO1()
        throws DatabaseException {

        openEnv(10, 10);
        if (isCkptHighPriority()) {
            doTest(93, 113);
        } else {
            if (embeddedLNs) {
                doTest(240, 270);
            } else {
                doTest(186, 206);
            }
        }
    }

    @Test
    public void testBackgroundIO2()
        throws DatabaseException {

        openEnv(10, 5);
        if (isCkptHighPriority()) {
            doTest(93, 113);
        } else {
            if (embeddedLNs) {
                doTest(410, 440);
            } else {
                doTest(310, 330);
            }
        }
    }

    @Test
    public void testBackgroundIO3()
        throws DatabaseException {

        openEnv(5, 10);
        if (isCkptHighPriority()) {
            doTest(167, 187);
        } else {
            if (embeddedLNs) {
                doTest(310, 350);
            } else {
                doTest(259, 279);
            }
        }
    }

    @Test
    public void testBackgroundIO4()
        throws DatabaseException {

        openEnv(5, 5);
        if (isCkptHighPriority()) {
            doTest(167, 187);
        } else {
            if (embeddedLNs) {
                doTest(490, 520);
            } else {
                doTest(383, 403);
            }
        }
    }

    private boolean isCkptHighPriority()
        throws DatabaseException {

        return "true".equals(env.getConfig().getConfigParam
            (EnvironmentParams.CHECKPOINTER_HIGH_PRIORITY.getName()));
    }

    private void openEnv(int readLimit, int writeLimit)
        throws DatabaseException {

        this.readLimit = readLimit;
        this.writeLimit = writeLimit;

        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setAllowCreate(true);

        envConfig.setConfigParam
            (EnvironmentParams.ENV_RUN_CLEANER.getName(), "false");
        envConfig.setConfigParam
            (EnvironmentParams.ENV_RUN_CHECKPOINTER.getName(), "false");
        envConfig.setConfigParam
            (EnvironmentParams.ENV_RUN_INCOMPRESSOR.getName(), "false");

        envConfig.setConfigParam
            (EnvironmentParams.LOG_BUFFER_MAX_SIZE.getName(),
             Integer.toString(1024));

        envConfig.setConfigParam
            (EnvironmentParams.LOG_FILE_MAX.getName(),
             Integer.toString(FILE_SIZE));

        envConfig.setConfigParam
            (EnvironmentParams.CLEANER_MIN_UTILIZATION.getName(), "60");
        //*
        envConfig.setConfigParam
            (EnvironmentParams.ENV_BACKGROUND_READ_LIMIT.getName(),
             String.valueOf(readLimit));
        envConfig.setConfigParam
            (EnvironmentParams.ENV_BACKGROUND_WRITE_LIMIT.getName(),
             String.valueOf(writeLimit));

        if (envMultiSubDir) {
            envConfig.setConfigParam
                (EnvironmentConfig.LOG_N_DATA_DIRECTORIES, DATA_DIRS + "");
        }

        //*/
        env = new Environment(envHome, envConfig);

        EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);

        embeddedLNs = (envImpl.getMaxEmbeddedLN() >= 4);
    }

    private void doTest(int minSleeps, int maxSleeps)
        throws DatabaseException {

        EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);

        envImpl.setBackgroundSleepHook(
            new TestHook() {
                public void doHook() {
                    nSleeps += 1;
                    assertEquals(0, LatchSupport.nBtreeLatchesHeld());
                }
                public Object getHookValue() {
                    throw new UnsupportedOperationException();
                }
                public void doIOHook() {
                    throw new UnsupportedOperationException();
                }
                public void hookSetup() {
                    throw new UnsupportedOperationException();
                }
                public void doHook(Object obj) {
                    throw new UnsupportedOperationException();
                }
            });

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setExclusiveCreate(true);
        Database db = env.openDatabase(null, "BackgroundIO", dbConfig);

        final int nFiles = 3;
        final int keySize = 20;
        final int dataSize = 10;
        final int recSize = keySize + dataSize + 35 /* LN overhead */;

        /* 3 * (1,000,000 / 65) = 46,153 */
        final int nRecords = nFiles * (FILE_SIZE / recSize); 

        /*
         * Insert records first so we will have a sizeable checkpoint.  Insert
         * interleaved because sequential inserts flush the BINs, and we want
         * to defer BIN flushing until the checkpoint.
         */
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry(new byte[dataSize]);
        for (int i = 0; i <= nRecords; i += 2) {
            setKey(key, i, keySize);
            db.put(null, key, data);
        }
        for (int i = 1; i <= nRecords; i += 2) {
            setKey(key, i, keySize);
            db.put(null, key, data);
        }

        /* Perform a checkpoint to perform background writes. */
        env.checkpoint(forceConfig);

        /* Delete records so we will have a sizable cleaning. */
        for (int i = 0; i <= nRecords; i += 1) {
            setKey(key, i, keySize);
            db.delete(null, key);
        }

        /* Perform cleaning to perform background reading. */
        env.checkpoint(forceConfig);
        env.cleanLog();
        env.checkpoint(forceConfig);

        db.close();
        env.close();
        env = null;

        String msg;
        msg = "readLimit=" + readLimit +
              " writeLimit=" + writeLimit +
              " minSleeps=" + minSleeps +
              " maxSleeps=" + maxSleeps +
              " actualSleeps=" + nSleeps;
        //System.out.println(msg);

        //*
        assertTrue(msg, nSleeps >= minSleeps && nSleeps <= maxSleeps);
        //*/
    }

    /**
     * Outputs an integer followed by pad bytes.
     */
    private void setKey(DatabaseEntry entry, int val, int len) {
        TupleOutput out = new TupleOutput();
        out.writeInt(val);
        for (int i = 0; i < len - 4; i += 1) {
            out.writeByte(0);
        }
        TupleBase.outputToEntry(out, entry);
    }
}
