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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;

import org.junit.Test;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

/**
 * Make sure the unadvertised per-db replication config setting works.
 */
public class PerDbReplicationTest extends TestBase {

    private static final String TEST_DB = "testdb";
    private final File envRoot;
    private Environment env;
    private Database db;

    public PerDbReplicationTest() {
        envRoot = SharedTestUtils.getTestDir();
    }

    /**
     * A database in a replicated environment should replicate by default.
     */
    @Test
    public void testDefault() {
//        Replicator[] replicators = RepTestUtils.startGroup(envRoot,
//                                                           1,
//                                                           false /* verbose */);
//        try {
//            env = replicators[0].getEnvironment();
//            DatabaseConfig config = new DatabaseConfig();
//            config.setAllowCreate(true);
//            config.setTransactional(true);
//
//            validate(config, true /* replicated */);
//        } finally {
//            if (db != null) {
//                db.close();
//            }
//
//            for (Replicator rep: replicators) {
//                rep.close();
//            }
//        }
    }

    /**
     * Check that a database in a replicated environment which is configured to
     * not replicate is properly saved.
     * (Not a public feature yet).
     */
    @Test
    public void testNotReplicated() {
//        Replicator[] replicators = RepTestUtils.startGroup(envRoot,
//                                                           1,
//                                                           false /* verbose*/);
//        try {
//            env = replicators[0].getEnvironment();
//            DatabaseConfig config = new DatabaseConfig();
//            config.setAllowCreate(true);
//            config.setTransactional(true);
//            config.setReplicated(false);
//
//            validate(config, false /* replicated */);
//        } finally {
//            if (db != null) {
//                db.close();
//            }
//
//            for (Replicator rep: replicators) {
//                rep.close();
//            }
//        }
    }

    /**
     * A database in a standalone environment should not be replicated.
     */
    @Test
    public void testStandalone()
        throws DatabaseException {

        try {
            EnvironmentConfig envConfig = new EnvironmentConfig();
            envConfig.setAllowCreate(true);
            env = new Environment(envRoot, envConfig);
            DatabaseConfig config = new DatabaseConfig();
            config.setAllowCreate(true);

            validate(config, false /* replicated */);
        } finally {
            if (db != null) {
                db.close();
            }

            if (env != null) {
                env.close();
            }
        }
    }

    /*
     * Check that the notReplicate attribute is properly immutable and
     * persistent.
     */
    private void validate(DatabaseConfig config,
                          boolean replicated)
            throws DatabaseException {

        /* Create the database -- is its config what we expect? */
        db = env.openDatabase(null, TEST_DB, config);
        DatabaseConfig inUseConfig = db.getConfig();
        assertEquals(replicated, inUseConfig.getReplicated());

        /* Close, re-open. */
        db.close();
        db = null;
        db = env.openDatabase(null, TEST_DB, inUseConfig);
        assertEquals(replicated, db.getConfig().getReplicated());

        /*
         * Close, re-open w/inappropriate value for the replicated bit. This is
         * only checked for replicated environments.
         */
        db.close();
        db = null;
        if (DbInternal.getNonNullEnvImpl(env).isReplicated()) {
            inUseConfig.setReplicated(!replicated);
            try {
                db = env.openDatabase(null, TEST_DB, inUseConfig);
                fail("Should have caught config mismatch");
            } catch (IllegalArgumentException expected) {
            }
        }
    }
}
