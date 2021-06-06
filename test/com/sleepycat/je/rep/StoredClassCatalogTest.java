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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;

import org.junit.Test;

import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.rep.utilint.RepTestUtils;
import com.sleepycat.je.rep.utilint.RepTestUtils.RepEnvInfo;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

public class StoredClassCatalogTest extends TestBase {
    private static final String dbName = "catalogDb";
    private final File envRoot;
    private RepEnvInfo[] repEnvInfo;

    public StoredClassCatalogTest() {
        envRoot = SharedTestUtils.getTestDir();
    }

    /*
     * Test that opening the StoredClassCatalog on the replicas after the 
     * database used for store the ClassCatalog is created doesn't throw a 
     * ReplicaWriteException, see SR 18938.
     */
    @Test
    public void testOpenClassCatalogOnReplicas()
        throws Exception {

        repEnvInfo = RepTestUtils.setupEnvInfos(envRoot, 3);
        ReplicatedEnvironment master = RepTestUtils.joinGroup(repEnvInfo);
        assertTrue(master.getState().isMaster());

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setTransactional(true);

        Database catalogDb = master.openDatabase(null, dbName, dbConfig);
        StoredClassCatalog catalog = new StoredClassCatalog(catalogDb);

        /* 
         * Sync the whole group to make sure the database has been created on 
         * the replicas. 
         */
        RepTestUtils.syncGroupToLastCommit(repEnvInfo, repEnvInfo.length);

        /* Check we can open the catalog db on the replicas. */
        Database repCatalogDb = null;
        try {
            repCatalogDb = 
                repEnvInfo[1].getEnv().openDatabase(null, dbName, dbConfig);
        } catch (Exception e) {
            e.printStackTrace();
            fail("unexpected exception: " + e);
        }

        /* Check no exceptions thrown while opening the StoredClassCatalog. */
        try {
            catalog = new StoredClassCatalog(repCatalogDb);
        } catch (Exception e) {
            e.printStackTrace();
            fail("unexpected exception: " + e);
        }

        catalogDb.close();
        repCatalogDb.close();

        RepTestUtils.shutdownRepEnvs(repEnvInfo);
    }
}
