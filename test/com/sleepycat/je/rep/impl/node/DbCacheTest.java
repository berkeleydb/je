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
package com.sleepycat.je.rep.impl.node;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

public class DbCacheTest extends TestBase {

    protected final File envRoot = SharedTestUtils.getTestDir();

    static final int dbCount = 10;
    static final int cacheSize = 3;

    DatabaseImpl dbImpls[] = new DatabaseImpl[dbCount];

    Environment env = null;
    private EnvironmentImpl envImpl;

    @Before
    public void setUp() 
        throws Exception {
        
        super.setUp();
        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        envConfig.setConfigParam(EnvironmentConfig.VERIFY_BTREE, "false");
        env = new Environment(envRoot, envConfig);
        envImpl = DbInternal.getNonNullEnvImpl(env);

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        for (int i=0; i < dbCount; i++) {
            Database db = env.openDatabase(null, "db"+i, dbConfig);
            dbImpls[i] = DbInternal.getDbImpl(db);
            db.close();
        }
    }

    @After
    public void tearDown() {
        env.close();
    }

    @Test
    public void testCacheMRU() {

        DbCache dbCache = new DbCache(envImpl.getDbTree(), cacheSize,
                                      Integer.MAX_VALUE);
        for (int i=0; i < dbCount; i++) {
            DatabaseImpl dbImpl = dbCache.get(dbImpls[i].getId(), null);
            assertEquals(Math.min(i+1, cacheSize), dbCache.getMap().size());
            assertEquals(dbImpls[i].getId(), dbImpl.getId());
        }

        /* Verify that the correct handles have been released. */
        for (int i=0; i < dbCount; i++) {
            if (i < (dbCount - cacheSize)) {
                assertTrue(!dbImpls[i].isInUse());
            } else {
                assertTrue(dbImpls[i].isInUse());
            }
        }
    }

    @Test
    public void testCacheTimeout() throws InterruptedException {

        DbCache dbCache = new DbCache(envImpl.getDbTree(), cacheSize, 1000);
        Thread.sleep(1000);
        dbCache.tick();
        Thread.sleep(1000);
        dbCache.tick();

        /* Verify that the correct handles have been released. */
        for (int i=0; i < dbCount; i++) {
            assertTrue(!dbImpls[i].isInUse());
        }
        assertEquals(0, dbCache.getMap().size());

        dbCache = new DbCache(envImpl.getDbTree(), cacheSize, 0);
        dbCache.get(dbImpls[0].getId(), null);
        dbCache.get(dbImpls[1].getId(), null);
        dbCache.tick();
        dbCache.get(dbImpls[0].getId(), null);
        dbCache.tick();
        assertTrue(!dbImpls[1].isInUse());
        assertTrue(dbImpls[0].isInUse());
    }

}
