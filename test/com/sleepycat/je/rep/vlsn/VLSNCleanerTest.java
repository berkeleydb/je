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

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.je.CheckpointConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentStats;
import com.sleepycat.je.rep.RepInternal;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.utilint.RepTestUtils;
import com.sleepycat.je.rep.utilint.RepTestUtils.RepEnvInfo;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

import org.junit.After;
import org.junit.Test;

/**
 * Exercise VLSNIndex and cleaning
 */
public class VLSNCleanerTest extends TestBase {
    private final boolean verbose = Boolean.getBoolean("verbose");
    private ReplicatedEnvironment master;
    private Database db;
    private final File envRoot;

    public VLSNCleanerTest() {
        envRoot = SharedTestUtils.getTestDir();
    }

    @After
    public void tearDown() {
        master.close();
    }

    @Test
    public void testBasic()
        throws DatabaseException {

        /*
         * Set the environment config to use very small files, have high
         * utilization, and permit manual cleaning.
         */
        EnvironmentConfig envConfig = new EnvironmentConfig();
        DbInternal.disableParameterValidation(envConfig);
        envConfig.setConfigParam(EnvironmentConfig.CLEANER_MIN_UTILIZATION,
                                 "90");
        envConfig.setConfigParam(EnvironmentConfig.ENV_RUN_CLEANER,
                                 "false");
        envConfig.setConfigParam(EnvironmentConfig.LOG_FILE_MAX, "10000");
        /* Need a disk limit to delete files. */
        envConfig.setConfigParam(
            EnvironmentConfig.MAX_DISK, String.valueOf(10 * 10000));
        envConfig.setTransactional(true);
        envConfig.setDurability(Durability.COMMIT_NO_SYNC);
        envConfig.setAllowCreate(true);

        RepEnvInfo masterInfo = RepTestUtils.setupEnvInfo(
            envRoot, envConfig, (short) 1, null);

        master = RepTestUtils.joinGroup(masterInfo);

        setupDatabase();
        int maxDeletions = 10;
        Environment env = master;
        EnvironmentStats stats = null;
        RepImpl repImpl = RepInternal.getNonNullRepImpl(master);

        for (int i = 0; i < 100; i += 1) {
            putAndDelete();

            boolean anyCleaned = false;
            while (env.cleanLog() > 0) {
                anyCleaned = true;
                if (verbose) {
                    System.out.println("anyCleaned");
                }
            }

            if (anyCleaned) {
                CheckpointConfig force = new CheckpointConfig();
                force.setForce(true);
                env.checkpoint(force);
            }

            stats = env.getStats(null);
            long nDeletions = stats.getNCleanerDeletions();

            if (verbose) {
                System.out.println("ckpt w/nCleanerDeletions=" + nDeletions);
            }

            assertTrue(repImpl.getVLSNIndex().verify(verbose));

            if (nDeletions >= maxDeletions) {
                db.close();
                master.close();
                return;
            }
        }

        fail("" + maxDeletions + " files were not deleted. " + stats);
    }

    private void setupDatabase()
        throws DatabaseException {

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(true);
        dbConfig.setAllowCreate(true);
        db = master.openDatabase(null, "TEST", dbConfig);
    }

    private void putAndDelete()
        throws DatabaseException {

        int WORK = 100;
        DatabaseEntry value = new DatabaseEntry();

        for (int i = 0; i < WORK; i++) {
            IntegerBinding.intToEntry(i, value);
            db.put(null, value, value);
            db.delete(null, value);
        }
    }
}
