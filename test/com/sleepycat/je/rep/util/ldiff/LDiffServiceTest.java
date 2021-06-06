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

package com.sleepycat.je.rep.util.ldiff;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.net.InetSocketAddress;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.bind.tuple.StringBinding;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicationConfig;
import com.sleepycat.je.rep.ReplicationNetworkConfig;
import com.sleepycat.je.rep.utilint.BinaryProtocol.ProtocolException;
import com.sleepycat.je.rep.utilint.RepTestUtils;
import com.sleepycat.je.rep.utilint.RepTestUtils.RepEnvInfo;
import com.sleepycat.je.rep.utilint.net.DataChannelFactoryBuilder;
import com.sleepycat.je.utilint.PollCondition;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

public class LDiffServiceTest extends TestBase {
    private final File envRoot;
    private RepEnvInfo[] repEnvInfo;
    private static final String DB_NAME = "testDb";

    public LDiffServiceTest() {
        envRoot = SharedTestUtils.getTestDir();
    }

    @Override
    @Before
    public void setUp() 
        throws Exception {

        /*
         * It may take a little while for environment files to be closed, so do
         * this cleanup in a retry loop
         */
        new PollCondition(1000, 10000) {
            @Override
            protected boolean condition() {
                try {
                    SharedTestUtils.cleanUpTestDir(
                        SharedTestUtils.getTestDir());
                    return true;
                } catch (IllegalStateException e) {
                    return false;
                }
            }
        }.await();

        super.setUp();
        repEnvInfo = RepTestUtils.setupEnvInfos(envRoot, 2);

        /*
         * Disable the shared cache for this particular test because it causes
         * an assertion to fire when shutting down the LDiffService.  The
         * service has a env handle open, and it closes it in the middle of
         * normal close processing.  We could potentially fix this, but since
         * the LDiffService is not for public consumption, it's simpler to
         * avoid the problem for now at least.
         *
         * Since this test opens two nodes at most per rep group, the shared
         * cache had limited benefit anyway.
         */
        for (int i = 0; i < repEnvInfo.length; i += 1) {
            repEnvInfo[i].getEnvConfig().setSharedCache(false);
        }
    }

    @Override
    @After
    public void tearDown() 
        throws Exception {

        super.tearDown();
        if (repEnvInfo != null) {
            for (int i = 0; i < repEnvInfo.length; i++) {
                repEnvInfo[i].closeEnv();
            }
        }
    }

    /* Do a diff between two replicators. */
    @Test
    public void testSame() 
        throws Exception {

        ReplicatedEnvironment master = RepTestUtils.joinGroup(repEnvInfo);

        insertData(master, DB_NAME, 6000, "herococo");

        InetSocketAddress replicaAddress = 
            repEnvInfo[1].getRepConfig().getNodeSocketAddress();
        checkLDiff(master, replicaAddress, false, true);
    }

    /* Insert records into the database on a replicator. */
    private void insertData(ReplicatedEnvironment repEnv,  
                            String dbName,
                            int dbSize, 
                            String dataStr)
        throws Exception {

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setTransactional(true);
        Database db = repEnv.openDatabase(null, dbName, dbConfig);

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        for (int i = 1; i <= dbSize; i++) {
            IntegerBinding.intToEntry(i, key);
            StringBinding.stringToEntry(dataStr, data);
            db.put(null, key, data);
        }
        db.close();
    }

    /* Check the LDiff result between two replicators. */
    private void checkLDiff(ReplicatedEnvironment localEnv,
                            InetSocketAddress remoteAddress,
                            boolean doAnalysis,
                            boolean expectedSame)
        throws Exception {

        LDiffConfig config = new LDiffConfig();
        config.setWaitIfBusy(true, -1, 0);
        config.setBlockSize(doAnalysis ? 10 : 1000);
        /* If do analysis, disable the output. */
        if (doAnalysis) {
            config.setDiffAnalysis(true);
            config.setVerbose(false);
        }
        config.setDiffAnalysis(doAnalysis);
        LDiff ldf = new LDiff(config);

        ReplicationNetworkConfig repNetConfig =
            RepTestUtils.readRepNetConfig();
        if (repNetConfig.getChannelType().isEmpty()) {
            assertEquals(ldf.diff(localEnv, remoteAddress), expectedSame);
        } else {
            assertEquals(ldf.diff(localEnv,
                                  remoteAddress,
                                  DataChannelFactoryBuilder.construct(
                                      repNetConfig)),
                         expectedSame);
        }
    }

    /* Test local Environment has additional data. */
    @Test
    public void testExtraLocalData() 
        throws Exception {

        makeTwoGroups();

        insertData(repEnvInfo[0].getEnv(), DB_NAME, 6000, "herococo");
        insertData(repEnvInfo[1].getEnv(), DB_NAME, 3000, "herococo");

        checkLDiff(repEnvInfo[0].getEnv(), 
                   repEnvInfo[1].getRepConfig().getNodeSocketAddress(),
                   false,
                   false);
    }

    /*
     * Make two replication groups.
     *
     * To compare the records between two replicators, since it's hard to make
     * records different on replicators in a group, so make two groups and
     * do compare between the masters of the two groups.
     */
    private void makeTwoGroups()
        throws Exception {

        ReplicationConfig repConfig = repEnvInfo[0].getRepConfig();
        repConfig.setGroupName("TestGroup1");

        repConfig = repEnvInfo[1].getRepConfig();
        repConfig.setGroupName("TestGroup2");
        repConfig.setHelperHosts(repConfig.getNodeHostPort());

        repEnvInfo[0].openEnv();
        assertTrue(repEnvInfo[0].isMaster());
        repEnvInfo[1].openEnv();
        assertTrue(repEnvInfo[1].isMaster());
    }

    /* Test remote Environment has additional data. */
    @Test
    public void testExtraRemoteData() 
        throws Exception {

        makeTwoGroups();

        insertData(repEnvInfo[0].getEnv(), DB_NAME, 3000, "herococo");
        insertData(repEnvInfo[1].getEnv(), DB_NAME, 6000, "herocooc");

        checkLDiff(repEnvInfo[0].getEnv(),
                   repEnvInfo[1].getRepConfig().getNodeSocketAddress(),
                   false,
                   false);
    }

    /* Test two replicators have different data. */
    @Test
    public void testDifferentData() 
        throws Exception {

        makeTwoGroups();

        insertData(repEnvInfo[0].getEnv(), DB_NAME, 6000, "herococo");
        insertData(repEnvInfo[1].getEnv(), DB_NAME, 6000, "hero&&coco");

        checkLDiff(repEnvInfo[0].getEnv(),
                   repEnvInfo[1].getRepConfig().getNodeSocketAddress(),
                   false,
                   false);
    }

    /* Test local Environment has a database but remote Environment doesn't. */
    @Test
    public void testNonExistentDb() 
        throws Exception {

        makeTwoGroups();

        insertData(repEnvInfo[0].getEnv(), DB_NAME, 6000, "herococo");

        try {
            checkLDiff(repEnvInfo[0].getEnv(),
                       repEnvInfo[1].getRepConfig().getNodeSocketAddress(),
                       false,
                       false);
        } catch (ProtocolException e) {
            /* Expected, do nothing. */
        }
    }

    /* Test remote Environment doesn't have any records in the database. */
    @Test
    public void testEmptyRemoteDb() 
        throws Exception {

        makeTwoGroups();

        insertData(repEnvInfo[0].getEnv(), DB_NAME, 6000, "herococo");
        insertData(repEnvInfo[1].getEnv(), DB_NAME, 0, "herococo");

        checkLDiff(repEnvInfo[0].getEnv(),
                   repEnvInfo[1].getRepConfig().getNodeSocketAddress(),
                   false, 
                   false);
    }

    /* Test local and remote Environment have multi databases. */
    @Test
    public void testSameEnvs() 
        throws Exception {

        makeTwoGroups();

        insertData(repEnvInfo[0].getEnv(), DB_NAME, 6000, "herococo");
        insertData
            (repEnvInfo[0].getEnv(), "another" + DB_NAME, 6000, "hero&&coco");
        insertData(repEnvInfo[1].getEnv(), DB_NAME, 6000, "herococo");
        insertData
            (repEnvInfo[1].getEnv(), "another" + DB_NAME, 6000, "hero&&coco");

        checkLDiff(repEnvInfo[0].getEnv(),
                   repEnvInfo[1].getRepConfig().getNodeSocketAddress(),
                   false,
                   true);
    }

    /* 
     * Test local and remote Environment have multi databases with different 
     * data. 
     */
    @Test
    public void testEnvsWithDifferentData() 
        throws Exception {

        makeTwoGroups();

        insertData(repEnvInfo[0].getEnv(), DB_NAME, 6001, "herococo");
        insertData
            (repEnvInfo[0].getEnv(), "another" + DB_NAME, 6000, "hero&&coco");
        insertData(repEnvInfo[1].getEnv(), DB_NAME, 6000, "herococo");
        insertData
            (repEnvInfo[1].getEnv(), "another" + DB_NAME, 6000, "hero&&coco");

        checkLDiff(repEnvInfo[0].getEnv(),
                   repEnvInfo[1].getRepConfig().getNodeSocketAddress(),
                   false,
                   false);
    }

    /* Test local Environment have more databases than remote. */
    @Test
    public void testEnvsWithExtraLocalDatabase() 
        throws Exception {

        makeTwoGroups();

        insertData(repEnvInfo[0].getEnv(), DB_NAME, 6000, "herococo");
        insertData
            (repEnvInfo[0].getEnv(), "another" + DB_NAME, 6000, "hero&&coco");
        insertData(repEnvInfo[1].getEnv(), DB_NAME, 6000, "herococo");

        checkLDiff(repEnvInfo[0].getEnv(),
                   repEnvInfo[1].getRepConfig().getNodeSocketAddress(),
                   false,
                   false);
    }

    /* Test remote Environment have more databases than local. */
    @Test
    public void testEnvsWithExtraRemoteDatabase() 
        throws Exception {

        makeTwoGroups();

        insertData(repEnvInfo[0].getEnv(), DB_NAME, 6000, "herococo");
        insertData(repEnvInfo[1].getEnv(), DB_NAME, 6000, "herococo");
        insertData
            (repEnvInfo[1].getEnv(), "another" + DB_NAME, 6000, "hero&&coco");

        checkLDiff(repEnvInfo[0].getEnv(),
                   repEnvInfo[1].getRepConfig().getNodeSocketAddress(),
                   false,
                   false);
    }

    /* Test local Environment has more data with analysis enabled. */
    @Test
    public void testAdditional()
        throws Exception {

        makeTwoGroups();

        insertData(repEnvInfo[0].getEnv(), DB_NAME, 200, "herococo");
        insertData(repEnvInfo[1].getEnv(), DB_NAME, 100, "herococo");

        checkLDiff(repEnvInfo[0].getEnv(),
                   repEnvInfo[1].getRepConfig().getNodeSocketAddress(),
                   true, 
                   false);
    }

    /* Test local and Environment have different data with analysis enabled. */
    @Test
    public void testDifferentArea()
        throws Exception {
    
        makeTwoGroups();

        insertData(repEnvInfo[0].getEnv(), DB_NAME, 200, "herococo");
        insertData(repEnvInfo[1].getEnv(), DB_NAME, 300, "hero&&coco");    

        checkLDiff(repEnvInfo[0].getEnv(),
                   repEnvInfo[1].getRepConfig().getNodeSocketAddress(),
                   true,
                   false);
    }
}
