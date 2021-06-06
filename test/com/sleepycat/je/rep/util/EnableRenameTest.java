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

import java.util.Properties;

import org.junit.Before;
import org.junit.Test;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.Environment;
import com.sleepycat.je.dbi.DbConfigManager;
import com.sleepycat.je.rep.InsufficientLogException;
import com.sleepycat.je.rep.NetworkRestore;
import com.sleepycat.je.rep.NetworkRestoreConfig;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicationConfig;
import com.sleepycat.je.rep.impl.RepTestBase;
import com.sleepycat.je.rep.utilint.RepTestUtils.RepEnvInfo;

/**
 * Regression test for SR #21537, which was first reported on OTN forum.
 */
public class EnableRenameTest extends RepTestBase {
    public static final String NEW_DB_NAME = TEST_DB_NAME + "2";

    @Before
    public void setUp() 
        throws Exception {
        
        groupSize = 2;
        super.setUp();
    }

    @Test
    public void testEnableWithRename() throws Exception {
        
        RepEnvInfo master = repEnvInfo[0];
        Properties temp = new Properties();
        DbConfigManager.applyFileConfig(repEnvInfo[0].getEnvHome(), 
                                        temp, true);
        if ("true".equals
                (temp.get("je.rep.preserveRecordVersion"))) {
             // TODO: enable this and fix the JE bug
             return;
        } 
        
        Environment env = new Environment(master.getEnvHome(),
                                          master.getEnvConfig());
        Database db = env.openDatabase(null, TEST_DB_NAME, dbconfig);
        db.close();
        env.close();

        ReplicationConfig masterConf =
            master.getRepConfig();
        DbEnableReplication enabler =
            new DbEnableReplication(master.getEnvHome(),
                                    masterConf.getGroupName(),
                                    masterConf.getNodeName(),
                                    masterConf.getNodeHostPort());
        enabler.convert();

        restartNodes(master);
        ReplicatedEnvironment masterEnv = master.getEnv();
        masterEnv.renameDatabase(null, TEST_DB_NAME, NEW_DB_NAME);

        ReplicatedEnvironment replicaEnv = null;
        try {
            replicaEnv = openRepEnv();
        } catch (InsufficientLogException ile) {
            NetworkRestore restore = new NetworkRestore();
            restore.execute(ile, new NetworkRestoreConfig());
            replicaEnv = openRepEnv();
        }
        DatabaseConfig dc2 = new DatabaseConfig();
        dc2.setReadOnly(true);
        dc2.setTransactional(true);

        try {
            db = replicaEnv.openDatabase(null, NEW_DB_NAME, dc2);
            db.close();
        } finally {
            replicaEnv.close();
        }
    }

    @Test
    public void testWorkaround() throws Exception {
        // same as above, except start new replica before doing the rename
        RepEnvInfo master = repEnvInfo[0];
        
        Properties temp = new Properties();
        DbConfigManager.applyFileConfig(repEnvInfo[0].getEnvHome(), 
                                        temp, true);
        if ("true".equals
                (temp.get("je.rep.preserveRecordVersion"))) {
             // TODO: enable this and fix the JE bug
             return;
        }
        
        Environment env = new Environment(master.getEnvHome(),
                                          master.getEnvConfig());
        Database db = env.openDatabase(null, TEST_DB_NAME, dbconfig);
        db.close();
        env.close();

        ReplicationConfig masterConf =
            master.getRepConfig();
        DbEnableReplication enabler =
            new DbEnableReplication(master.getEnvHome(),
                                    masterConf.getGroupName(),
                                    masterConf.getNodeName(),
                                    masterConf.getNodeHostPort());
        enabler.convert();

        restartNodes(master);
        ReplicatedEnvironment replicaEnv = null;
        try {
            replicaEnv = openRepEnv();
        } catch (InsufficientLogException ile) {
            NetworkRestore restore = new NetworkRestore();
            restore.execute(ile, new NetworkRestoreConfig());
            replicaEnv = openRepEnv();
        }

        ReplicatedEnvironment masterEnv = master.getEnv();
        masterEnv.renameDatabase(null, TEST_DB_NAME, NEW_DB_NAME);

        DatabaseConfig dc2 = new DatabaseConfig();
        dc2.setReadOnly(true);
        dc2.setTransactional(true);
        db = replicaEnv.openDatabase(null, NEW_DB_NAME, dc2);

        db.close();
        replicaEnv.close();
    }

    private ReplicatedEnvironment openRepEnv() throws Exception {
        RepEnvInfo replica = repEnvInfo[1];
        return new ReplicatedEnvironment(replica.getEnvHome(),
                                         replica.getRepConfig(),
                                         replica.getEnvConfig());
    }
}
