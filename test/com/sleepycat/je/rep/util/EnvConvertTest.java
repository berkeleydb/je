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

package com.sleepycat.je.rep.util;

import static com.sleepycat.je.rep.impl.RepParams.DEFAULT_PORT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.List;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.bind.tuple.StringBinding;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.dbi.DbConfigManager;
import com.sleepycat.je.rep.InsufficientLogException;
import com.sleepycat.je.rep.NetworkRestore;
import com.sleepycat.je.rep.NetworkRestoreConfig;
import com.sleepycat.je.rep.RepInternal;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicationConfig;
import com.sleepycat.je.rep.ReplicationMutableConfig;
import com.sleepycat.je.rep.utilint.RepTestUtils;
import com.sleepycat.je.rep.utilint.RepTestUtils.RepEnvInfo;
import com.sleepycat.je.rep.utilint.WaitForMasterListener;
import com.sleepycat.je.utilint.VLSN;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

public class EnvConvertTest extends TestBase {
    private final boolean verbose = Boolean.getBoolean("verbose");
    private final File envRoot;
    private final String DB_NAME = "test";
    private final String EMPTY_DB = "emptyDB";
    private final int dbSize = 100;
    private EnvironmentConfig envConfig;
    private DatabaseConfig dbConfig;
    private RepEnvInfo[] repEnvInfo;

    public EnvConvertTest() {
        envRoot = SharedTestUtils.getTestDir();
    }

    @Before
    public void setUp()
        throws Exception {

        super.setUp();
        repEnvInfo = RepTestUtils.setupEnvInfos(envRoot, 2);

        envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        envConfig.setReadOnly(false);

        dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setReadOnly(false);
    }

    @After
    public void tearDown() {
        RepTestUtils.shutdownRepEnvs(repEnvInfo);
    }

    @Test
    public void testOpenSequence()
        throws Exception {

        Properties temp = new Properties();
        DbConfigManager.applyFileConfig(repEnvInfo[0].getEnvHome(), 
                                        temp, true);
        if ("true".equals
                (temp.get("je.rep.preserveRecordVersion"))) {
             // TODO: enable this and fix the JE bug
             return;
        }
        
        /* Create, populate, and close a standalone environment. */
        openStandaloneEnvAndInsertData();

        /* Open a r/w standalone Environment on it. */
        Environment env = null;
        
        try {
            env = new Environment(repEnvInfo[0].getEnvHome(), envConfig);
        } catch (Exception e) {
            fail("Expect no exceptions here.");
        } finally {
            if (env != null) {
                env.close();
            }
        }

        /*
         * Open a replicated Environment on it, without setting it
         * allowConvert.
         */
        try {
            repEnvInfo[0].openEnv();
            fail("Shouldn't go here.");
        } catch (UnsupportedOperationException e) {
            /* An expected exception. */
        }

        /* Open a replicated Environment on it, now permitting conversion. */
        try {
            ReplicationConfig repConfig = repEnvInfo[0].getRepConfig();
            RepInternal.setAllowConvert(repConfig, true);
            repEnvInfo[0].openEnv();
        } catch (Exception e) {
            fail("Expect no exceptions here.");
        } finally {
            repEnvInfo[0].closeEnv();
        }

        /*
         * Open a replicated Environment on it again. Test that user defined
         * databases have been converted to replicated and were properly
         * written to the log.
         */
        Database db = null;
        try {
            ReplicationConfig repConfig = repEnvInfo[0].getRepConfig();
            RepInternal.setAllowConvert(repConfig, false);
            repEnvInfo[0].openEnv();
            dbConfig.setTransactional(true);
            db = repEnvInfo[0].getEnv().openDatabase(null, DB_NAME, dbConfig);
        } catch (Exception e) {
            fail("Got " + e + " expect no exceptions here.");
        } finally {
            if (db != null) {
                db.close();
            }
            repEnvInfo[0].closeEnv();
        }

        /* Open a r/o standalone Environment on it. */
        try {
            envConfig.setReadOnly(true);
            env = new Environment(repEnvInfo[0].getEnvHome(), envConfig);
        } catch (Exception e) {
            fail("Expect no exceptions here.");
        } finally {
            if (env != null) {
               env.close();
            }
        }

        /* Open a r/w standalone Environmetn on it. */
        try {
            envConfig.setReadOnly(false);
            env = new Environment(repEnvInfo[0].getEnvHome(), envConfig);
            fail("Shouldn't go here.");
        } catch (UnsupportedOperationException e) {
            /* An expected exception. */
        }
    }

    @Test
    public void testCRUDOnDb()
        throws Exception {

        Properties temp = new Properties();
        DbConfigManager.applyFileConfig(repEnvInfo[0].getEnvHome(), 
                                        temp, true);
        if ("true".equals
                (temp.get("je.rep.preserveRecordVersion"))) {
             // TODO: enable this and fix the JE bug
             return;
        }
        
        syncupGroup();

        assertTrue(repEnvInfo[0].getEnv().getState().isMaster());
        assertFalse(repEnvInfo[1].getEnv().getState().isMaster());

        dbConfig.setTransactional(true);
        Database db =
            repEnvInfo[0].getEnv().openDatabase(null, DB_NAME, dbConfig);

        /* Read data. */
        doCRUDOperations(1, dbSize, db, OpType.READ);

        /* Insert data. */
        doCRUDOperations(101, 200, db, OpType.CREATE);

        /* Delete data. */
        doCRUDOperations(51, 200, db, OpType.DELETE);

        /* Update data. */
        doCRUDOperations(1, dbSize - 50, db, OpType.UPDATE);

        db.close();

        checkEquality(repEnvInfo);
    }

    @Test
    public void testDbOps()
        throws Exception {

        Properties temp = new Properties();
        DbConfigManager.applyFileConfig(repEnvInfo[0].getEnvHome(), 
                                        temp, true);
        if ("true".equals
                (temp.get("je.rep.preserveRecordVersion"))) {
             // TODO: enable this and fix the JE bug
             return;
        }
        
        syncupGroup();

        assertTrue(repEnvInfo[0].getEnv().getState().isMaster());
        assertFalse(repEnvInfo[1].getEnv().getState().isMaster());

        /* Truncate database. */
        assertEquals(repEnvInfo[0].getEnv().truncateDatabase
                     (null, DB_NAME, true), dbSize);

        /* Rename database. */
        repEnvInfo[0].getEnv().renameDatabase(null, DB_NAME, "db1");
        List<String> namesList = repEnvInfo[0].getEnv().getDatabaseNames();
        assertTrue(!namesList.contains(DB_NAME) && namesList.contains("db1"));

        /* Remove database. */
        repEnvInfo[0].getEnv().removeDatabase(null, "db1");
        namesList = repEnvInfo[0].getEnv().getDatabaseNames();
        assertFalse(namesList.contains("db1"));

        /* Create database. */
        dbConfig.setTransactional(true);
        Database db =
            repEnvInfo[0].getEnv().openDatabase(null, DB_NAME, dbConfig);
        namesList = repEnvInfo[0].getEnv().getDatabaseNames();
        assertTrue(namesList.contains(DB_NAME));
        db.close();

        checkEquality(repEnvInfo);
    }

    /* Test the read operations on replica after NetworkRestore. */
    @Test
    public void testReadOnReplica()
        throws Exception {
        
        Properties temp = new Properties();
        DbConfigManager.applyFileConfig(repEnvInfo[0].getEnvHome(), 
                                        temp, true);
        if ("true".equals
                (temp.get("je.rep.preserveRecordVersion"))) {
             // TODO: enable this and fix the JE bug
             return;
        }
        
        syncupGroup();

        try {
            dbConfig.setTransactional(true);
            Database db =
                repEnvInfo[1].getEnv().openDatabase(null, DB_NAME, dbConfig);
            doCRUDOperations(1, dbSize, db, OpType.READ);
            db.close();
        } catch (Exception e) {
            fail("Shouldn't throw out exceptions here.");
        }

        checkEquality(repEnvInfo);
    }

    /* Test replica can be the master if the former master is shutdown. */
    @Test
    public void testTwoNodesFailover()
        throws Exception {

        Properties temp = new Properties();
        DbConfigManager.applyFileConfig(repEnvInfo[0].getEnvHome(), 
                                        temp, true);
        if ("true".equals
                (temp.get("je.rep.preserveRecordVersion"))) {
             // TODO: enable this and fix the JE bug
             return;
        }
        
        syncupGroup();

        repEnvInfo[0].closeEnv();

        WaitForMasterListener waitForMaster = new WaitForMasterListener();
        repEnvInfo[1].getEnv().setStateChangeListener(waitForMaster);
        ReplicationMutableConfig config =
            repEnvInfo[1].getEnv().getRepMutableConfig();
        assertEquals(false, config.getDesignatedPrimary());
        config.setDesignatedPrimary(true);
        repEnvInfo[1].getEnv().setRepMutableConfig(config);

        waitForMaster.awaitMastership();

        assertTrue(repEnvInfo[1].getRepNode().getArbiter().isActive());
        assertTrue(repEnvInfo[1].getEnv().getState().isMaster());
    
        dbConfig.setTransactional(true);
        Database db = null;
        try {
            db = repEnvInfo[1].getEnv().openDatabase(null, DB_NAME, dbConfig);
            /* Do some update operations. */
            doCRUDOperations(1, dbSize, db, OpType.UPDATE);
        } finally {
            if (db != null) {
                db.close();
            }
        }
    }

    /* Test three nodes fail over behaviors. */
    @Test
    public void testThreeNodesFailover()
        throws Exception {

        Properties temp = new Properties();
        DbConfigManager.applyFileConfig(repEnvInfo[0].getEnvHome(), 
                                        temp, true);
        if ("true".equals
                (temp.get("je.rep.preserveRecordVersion"))) {
             // TODO: enable this and fix the JE bug
             return;
        }
        
        repEnvInfo = null;
        /* Make a three nodes replication group. */
        repEnvInfo = RepTestUtils.setupEnvInfos(envRoot, 3);

        syncupGroup();

        /* Sync up node 3. */
        doNetworkRestore(repEnvInfo[2]);

        /* Close the former master. */
        String masterName = repEnvInfo[0].getEnv().getNodeName();
        repEnvInfo[0].closeEnv();

        /* Select a new master, check to make sure the master is changed. */
        ReplicatedEnvironment master =
            RepTestUtils.openRepEnvsJoin(repEnvInfo);
        assertTrue(master.getState().isMaster());
        assertTrue(!master.getNodeName().equals(masterName));

        checkEquality(RepTestUtils.getOpenRepEnvs(repEnvInfo));
    }

    /*
     * Test the behaviors of creating a ReplicatedEnvironment on an open
     * standalone Environment, see SR 18649.
     */
    @Test
    public void testOpenRepEnvOnUnClosedStandaloneEnv()
        throws Exception {

        if ("true".equals
                (repEnvInfo[0].getEnvConfig().getConfigParam
                    ("je.rep.preserveRecordVersion"))) {
                // TODO: enable this and fix the JE bug
                return;
        }
        
        File envHome = new File(envRoot, "testOpenRepEnv");
        envHome.mkdir();

        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        envConfig.setTransactional(true);

        Environment env = new Environment(envHome, envConfig);

        ReplicationConfig repConfig = new ReplicationConfig();
        repConfig.setGroupName("group1");
        repConfig.setNodeName("node1");
        repConfig.setNodeHostPort("localhost:5001");
        repConfig.setHelperHosts(repConfig.getNodeHostPort());

        ReplicatedEnvironment repEnv = null;
        try {
            repEnv = new ReplicatedEnvironment(envHome, repConfig, envConfig);
            fail("Expected UnsupportedOperationException here.");
        } catch (UnsupportedOperationException e) {
            /* Expected exception. */
        } catch (Exception e) {
            e.printStackTrace();
            fail("unexpected exception: " + e);
        } finally {
            env.close();
            if (repEnv != null) {
                repEnv.close();
            }
        }

        for (File file : envHome.listFiles()) {
            if (file.isFile()) {
                file.delete();
            }
        }
        envHome.delete();
    } 

    /* Sync up the whole group. */
    private void syncupGroup()
        throws Exception {

        openStandaloneEnvAndInsertData();

        DbEnableReplication converter = new DbEnableReplication
            (repEnvInfo[0].getEnvHome(), RepTestUtils.TEST_REP_GROUP_NAME,
             "Node1", RepTestUtils.TEST_HOST + ":" + 
             DEFAULT_PORT.getDefault());

        converter.convert();

        repEnvInfo[0].openEnv();

        doNetworkRestore(repEnvInfo[1]);
    }

    /*
     * Do a NetworkRestore to copy the latest log files from master to
     * replica.
     */
    private void doNetworkRestore(RepEnvInfo repNode)
        throws Exception {

        try {
            repNode.openEnv();
        } catch (InsufficientLogException e) {
            NetworkRestore restore = new NetworkRestore();
            NetworkRestoreConfig config = new NetworkRestoreConfig();
            config.setRetainLogFiles(false);
            restore.execute(e, config);
        } finally {
            if (repNode.getEnv() != null) {
                repNode.closeEnv();
            }
        }

        try {
            repNode.openEnv();
        } catch (Exception e) {
            e.printStackTrace();
            fail("Shouldn't throw out exceptions here.");
        }

        /* Do read operations after the network restore. */
        Database db = null;
        try {
            dbConfig.setTransactional(true);
            db = repEnvInfo[0].getEnv().openDatabase(null, DB_NAME, dbConfig);
            doCRUDOperations(1, dbSize, db, OpType.READ);
        } catch (Exception e) {
            fail("Shouldn't throw out exceptions here.");
        } finally {
            if (db != null) {
               db.close();
            }
        }
    }

    /* Check the equality of replicas in the same group. */
    private void checkEquality(RepEnvInfo[] repInfoArray)
        throws Exception {

        VLSN vlsn = RepTestUtils.syncGroupToLastCommit(repInfoArray,
                                                       repInfoArray.length);
        RepTestUtils.checkNodeEquality(vlsn, verbose, repInfoArray);
    }

    /* Create a standalone environment, insert some records and close it. */
    private void openStandaloneEnvAndInsertData()
        throws Exception {

        Environment env =
            new Environment(repEnvInfo[0].getEnvHome(), envConfig);
        Database db = env.openDatabase(null, DB_NAME, dbConfig);
        doCRUDOperations(1, dbSize, db, OpType.CREATE);
        db.close();

        Database emptyDb = env.openDatabase(null, EMPTY_DB, dbConfig);
        emptyDb.close();
        env.removeDatabase(null, EMPTY_DB);
        env.close();
    }

    /* Do CRUD operations on the specified database. */
    private void doCRUDOperations(int beginId,
                                  int endId,
                                  Database db,
                                  OpType type)
        throws Exception {

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        for (int i = beginId; i <= endId; i++) {
            IntegerBinding.intToEntry(i, key);
            switch (type) {
                case CREATE:
                    StringBinding.stringToEntry
                        ("herococo" + new Integer(i).toString(), data);
                    assertEquals(OperationStatus.SUCCESS,
                                 db.put(null, key, data));
                    break;
                case READ:
                    assertEquals(OperationStatus.SUCCESS,
                                 db.get(null, key, data, null));
                    assertEquals("herococo" + new Integer(i).toString(),
                                 StringBinding.entryToString(data));
                    break;
                case DELETE:
                    assertEquals(OperationStatus.SUCCESS,
                                 db.delete(null, key));
                    break;
                case UPDATE:
                    StringBinding.stringToEntry("here&&coco", data);
                    assertEquals(OperationStatus.SUCCESS,
                                 db.put(null, key, data));
                    break;
            }
        }
    }

    enum OpType {
        CREATE, READ, UPDATE, DELETE;
    }

    /** 
     * Make sure that the code example for DbEnableReplication compiles. The
     * code will not actually execute because the hostname of
     * "hostname.widget.org is not valid, so catch the expected exception.
     */
    @Test
    public void testJavadocForDbEnableReplication() {

        File envDirMars = repEnvInfo[0].getEnvHome();
        File envDirVenus = repEnvInfo[1].getEnvHome();
        @SuppressWarnings("unused")
        ReplicatedEnvironment nodeVenus;
        try {

            /* ------------- example below ------------- */

            // Create the first node using an existing environment 
            DbEnableReplication converter = 
                new DbEnableReplication(envDirMars,          // env home dir
                                        "UniversalRepGroup", // group name
                                        "nodeMars",          // node name
                                        "mars:5001");        // node host,port
            converter.convert();

            /* 
             * This line is in the example, but it's pseudo code so it won't
             * compile ReplicatedEnvironment nodeMars =
             *       new ReplicatedEnvironment(envDirMars, ...);
             */
 
            // Bring up additional nodes, which will be initialized from 
            // nodeMars.
            ReplicationConfig repConfig = null;
            try {
                repConfig = 
                    new ReplicationConfig("UniversalRepGroup", // groupName
                                          "nodeVenus",         // nodeName
                                          "venus:5008");       // nodeHostPort
                repConfig.setHelperHosts("mars:5001");
 
                nodeVenus = new ReplicatedEnvironment(envDirVenus, 
                                                      repConfig, 
                                                      envConfig);
            } catch (InsufficientLogException insufficientLogEx) {
 
                // log files will be copied from another node in the group
                NetworkRestore restore = new NetworkRestore();
                restore.execute(insufficientLogEx, new NetworkRestoreConfig());
      
                // try opening the node now
                nodeVenus = new ReplicatedEnvironment(envDirVenus, 
                                                      repConfig,
                                                      envConfig);
            }

            /* ----------- end of example --------- */

        } catch (IllegalArgumentException expected) {
            /* 
             * The hostnames are invalid. We just want to make sure the
             * example compiles.
             */
        }
    }
}
