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

package com.sleepycat.persist.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.StoreConfig;
import com.sleepycat.persist.evolve.Mutations;
import com.sleepycat.persist.evolve.Renamer;
import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.Persistent;
import com.sleepycat.persist.model.PrimaryKey;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;
import com.sleepycat.util.test.TestEnv;

/**
 * Test a bug fix for an evolution error when a class is evolved and then
 * changed back to its original version.  Say there are two versions of a
 * class A1 and A2 in the catalog, plus a new version A3 of the class.  The
 * problem occurs when A2 is different than A3 and must be evolved, but A1
 * happens to be identical to A3 and no evolution is needed.  In that case, A3
 * was never added to the format list in the catalog (never assigned a format
 * ID), but was still used as the "latest version" of A2.  This caused all
 * kinds of trouble since the class catalog was effectively corrupt.  [#16467]
 *
 * We reproduce this scenario using type Other[], which is represented using
 * ArrayObjectFormat internally.  By renaming Other to Other2, and then back to
 * Other, we create the scenario described above for the array format itself.
 * Array formats are only evolved if their component class name has changed
 * (see ArrayObjectFormat.evolve).
 *
 * A modified version of this program was run manually with JE 3.3.71 to
 * produce a log, which is the result of the testSetup() test.  The sole log
 * file was renamed from 00000000.jdb to DevolutionTest.jdb and added to CVS
 * in this directory.  When that log file is opened here, the bug is
 * reproduced.
 *
 * This test should be excluded from the BDB build because it uses a stored JE
 * log file and it tests a fix for a bug that was never present in BDB.
 *
 * @author Mark Hayes
 */
public class DevolutionTest extends TestBase {

    private static final String STORE_NAME = "test";

    private File envHome;
    private Environment env;

    @Before
    public void setUp() 
        throws Exception {
        
        envHome = SharedTestUtils.getTestDir();
        super.setUp();
    }

    @After
    public void tearDown() {
        if (env != null) {
            try {
                env.close();
            } catch (Throwable e) {
                System.out.println("During tearDown: " + e);
            }
        }
        envHome = null;
        env = null;
    }

    private EntityStore open()
        throws DatabaseException {

        EnvironmentConfig envConfig = TestEnv.BDB.getConfig();
        envConfig.setAllowCreate(true);
        env = new Environment(envHome, envConfig);

        /*
         * When version 0 of Other is used, no renamer is configured.  When
         * version 1 is used, a renamer from Other version 0 to Other2 is used.
         * For version 2, the current version, a renamer from Other2 version 1
         * to Other is used.
         */
        String clsName = getClass().getName() + "$Other";
        Renamer renamer = new Renamer(clsName + '2', 1, clsName);
        Mutations mutations = new Mutations();
        mutations.addRenamer(renamer);

        StoreConfig storeConfig = new StoreConfig();
        storeConfig.setAllowCreate(true);
        storeConfig.setMutations(mutations);
        return new EntityStore(env, "foo", storeConfig);
    }

    private void close(EntityStore store)
        throws DatabaseException {

        store.close();
        env.close();
        env = null;
    }

    @Test
    public void testDevolution()
        throws DatabaseException, IOException {

        /* Copy log file resource to log file zero. */
        TestUtils.loadLog(getClass(), "DevolutionTest.jdb", envHome);

        EntityStore store = open();

        PrimaryIndex<Long, MyEntity> index =
            store.getPrimaryIndex(Long.class, MyEntity.class);

        MyEntity entity = index.get(1L);
        assertNotNull(entity);
        assertEquals(123, entity.b);

        close(store);
    }

    public void xtestSetup()
        throws DatabaseException {

        EntityStore store = open();

        PrimaryIndex<Long, MyEntity> index =
            store.getPrimaryIndex(Long.class, MyEntity.class);

        MyEntity entity = new MyEntity();
        entity.key = 1L;
        entity.b = 123;
        index.put(entity);

        close(store);
    }

    /**
     * This class name is changed from Other to Other2 in version 1 and back to
     * Other in the version 2.  testSetup is executed for versions 0 and 1,
     * which evolves the format.  testDevolution is run with version 2.
     */
    @Persistent(version=2)
    static class Other {
    }

    @Entity(version=0)
    static class MyEntity {

        @PrimaryKey
        long key;

        Other[] a;

        int b;

        private MyEntity() {}
    }
}
