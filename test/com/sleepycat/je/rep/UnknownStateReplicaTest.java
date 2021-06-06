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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;

import org.junit.Test;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.bind.tuple.StringBinding;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.rep.utilint.RepTestUtils;
import com.sleepycat.je.rep.utilint.RepTestUtils.RepEnvInfo;
import com.sleepycat.je.rep.utilint.ValidStateListener;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

/**
 * Check unknown state replica can serve read opertions.
 */
public class UnknownStateReplicaTest extends TestBase {

    private final File envRoot;
    private static final String DB_NAME = "testDB";
    private RepEnvInfo[] repEnvInfo;

    public UnknownStateReplicaTest() {
        envRoot = SharedTestUtils.getTestDir();
    }

    /**
     * Check a replica works in Unknown state.
     */
    @Test
    public void testBasic()
        throws Throwable {

        try {
            repEnvInfo = RepTestUtils.setupEnvInfos(envRoot, 3);
            ReplicatedEnvironment master = RepTestUtils.joinGroup(repEnvInfo);

            /* Open a new database on replicas. */
            DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setAllowCreate(true);
            dbConfig.setTransactional(true);

            Database db = master.openDatabase(null, DB_NAME, dbConfig);

            /* Insert some data. */
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry data = new DatabaseEntry();
            for (int i = 1; i <= 10; i++) {
                IntegerBinding.intToEntry(i, key);
                StringBinding.stringToEntry("herococo" + i, data);
                db.put(null, key, data);
            }
            db.close();

            /* Shutdown all the replicas. */
            for (int i = repEnvInfo.length - 1; i >= 0; i--) {
                repEnvInfo[i].closeEnv();
            }

            /* 
             * Configure the replica work in Unknown state and use the 
             * NoConsistencyPolicy for read. 
             */
            repEnvInfo[0].getRepConfig().setConfigParam
                (ReplicationConfig.ALLOW_UNKNOWN_STATE_ENV_OPEN, "true");
            repEnvInfo[0].getRepConfig().setConsistencyPolicy
                (new NoConsistencyRequiredPolicy());

            /* Reopen the replica, and make sure its state is Unknown. */
            repEnvInfo[0].openEnv();            
            assertTrue(repEnvInfo[0].isUnknown());            

            /* Read the database to make sure the content is correct. */
            db = repEnvInfo[0].getEnv().openDatabase(null, DB_NAME, dbConfig);
            for (int i = 1; i <= 10; i++) {
                IntegerBinding.intToEntry(i, key);
                db.get(null, key, data, null);
                assertEquals
                    ("herococo" + i, StringBinding.entryToString(data));
            }

            /* 
             * Do some write and expect to see ReplicaWriteException, because 
             * it's not a master. 
             */
            try {
                IntegerBinding.intToEntry(11, key);
                StringBinding.stringToEntry("herococo11", data);
                db.put(null, key, data);
                fail("Expect to see exceptions.");
            } catch (ReplicaWriteException e) {
                /* Expected exceptions. */
            } catch (Exception e) {
                e.printStackTrace();
                throw e;
            }
            db.close();

            /* Configure the state change listener to repEnvInfo[0]. */
            ValidStateListener stateListener = new ValidStateListener();
            repEnvInfo[0].getEnv().setStateChangeListener(stateListener);

            /* 
             * Open another replica and check to see whether the state has 
             * changed, because this is a three nodes group. 
             */
            repEnvInfo[1].openEnv();
            assertTrue(repEnvInfo[1].isMaster() || repEnvInfo[1].isReplica());

            /* Make sure that the state of repEnvInfo[0] has changed. */
            stateListener.awaitValidState();
            assertTrue(repEnvInfo[0].isMaster() || repEnvInfo[0].isReplica());

            /* Find out the current master. */
            int masterIndex = -1;
            for (int i = 0; i <= 1; i++) {
                if (repEnvInfo[i].isMaster()) {
                    masterIndex = i;
                    break;
                }
            }
            assertTrue(masterIndex != -1);

            /* Open the database again and do some inserts. */
            master = repEnvInfo[masterIndex].getEnv();
            db = master.openDatabase(null, DB_NAME, dbConfig);

            for (int i = 11; i <= 20; i++) {
                IntegerBinding.intToEntry(i, key);
                StringBinding.stringToEntry("herococo" + i, data);
                db.put(null, key, data);
            }
            db.close();

            /* Open the rest replica and we're sure it should be a replica. */
            repEnvInfo[2].openEnv();
            assertTrue(repEnvInfo[2].isReplica());

            /* 
             * Open the database on all replicas and make sure the database 
             * contents are correct. 
             */
            for (int i = 0; i < repEnvInfo.length; i++) {
                db = repEnvInfo[i].getEnv().openDatabase
                    (null, DB_NAME, dbConfig);
                for (int j = 1; j <= 20; j++) {
                    IntegerBinding.intToEntry(j, key);
                    db.get(null, key, data, null);
                    assertEquals
                        ("herococo" + j, StringBinding.entryToString(data));
                }
                db.close();
            }
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        } finally {
            RepTestUtils.shutdownRepEnvs(repEnvInfo);
        }
    }
}
