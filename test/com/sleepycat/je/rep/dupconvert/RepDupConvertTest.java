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

package com.sleepycat.je.rep.dupconvert;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

import org.junit.Before;
import org.junit.Test;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.impl.RepParams;
import com.sleepycat.je.rep.utilint.RepTestUtils;
import com.sleepycat.je.rep.utilint.RepTestUtils.RepEnvInfo;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

/**
 * JE 5.0 changes the duplicated entries representation, and this change
 * requires that no duplicated entries exist in the last recovery interval
 * of the logs that created by JE 4.1.
 *
 * To achieve this goal, JE 4.1 needs to invoke DbRepPreUpgrade_4_2 to assure
 * no duplicated entries exist in the last recover interval. The goal of the
 * test is to make sure JE 5.0 can read the logs correctly if we perform the
 * upgrade on the original log, see SR 19165 for more details.
 */
public class RepDupConvertTest extends TestBase {
    private static final int groupSize = 3;
    private static final String dbName = "testDB";
    private final File envRoot;
    private RepEnvInfo[] repEnvInfo;

    public RepDupConvertTest() {
        envRoot = SharedTestUtils.getTestDir();
    }

    @Before
    public void setUp()
        throws Exception {

        System.setProperty(RepParams.SKIP_NODENAME_VALIDATION, "true");
        super.setUp();
    }

    @Test
    public void testLogWithSingletonLN()
        throws Throwable {

        doNormalTest("singletonLN", 0, 100, 0, 0);
    }

    @Test
    public void testLogWithDIN()
        throws Throwable {

        doNormalTest("din", 0, 10, 0, 10);
    }

    @Test
    public void testLogWithDeletedLNCommit()
        throws Throwable {

        doNormalTest("deletedLNCommit", 0, 10, 5, 10);
    }

    @Test
    public void testLogWithDeleteLNNoCommit()
        throws Throwable {

        doNormalTest("deletedLNNoCommit", 0, 10, 0, 10);
    }

    @Test
    public void testLogWithMixIN()
        throws Throwable {

        doNormalTest("mixIN", 0, 10, 3, 7);
    }

    public void doNormalTest(String logName,
                             int outerStart,
                             int outerEnd,
                             int innerStart,
                             int innerEnd)
        throws Throwable {

        if (readPreserveRecordVersionProperty()) {
            return;
        }

        loadLogFiles(logName);

        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        envConfig.setTransactional(true);
        envConfig.setConfigParam(EnvironmentParams.NODE_MAX.getName(), "4");

        /* Start the group. */
        repEnvInfo = RepTestUtils.setupEnvInfos(envRoot, groupSize, envConfig);

        adjustNodeNames();

        ReplicatedEnvironment master =
            RepTestUtils.restartGroup(true, repEnvInfo);

        /* Create databases on the master. */
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setSortedDuplicates(true);
        dbConfig.setTransactional(true);

        /* Check the database content on all replicas. */
        for (int i = 0; i < repEnvInfo.length; i++) {
            Database db = repEnvInfo[i].getEnv().
                openDatabase(null, dbName, dbConfig);
            checkDatabaseContents
                (db, outerStart, outerEnd, innerStart, innerEnd);
            db.close();
        }

        /* Do some updates on database. */
        Database db = master.openDatabase(null, dbName, dbConfig);
        updateDatabase(db);

        db.close();

        RepTestUtils.shutdownRepEnvs(repEnvInfo);
    }

    /**
     * Modifies the rep configuration to match the older convention of allowing
     * blank space in node names.  This avoids the need to regenerate old
     * version log files to match the new, more restrictive naming rules
     * [#21407], but also, more signficantly, gives us an easy way to test the
     * override (RepParams.SKIP_NODENAME_VALIDATION).
     */
    private void adjustNodeNames() {
        for (int i = 0; i < repEnvInfo.length; i++) {
            int nodeId = i + 1;
            repEnvInfo[i].getRepConfig().setNodeName("Node " + nodeId);
        }
    }

    private boolean readPreserveRecordVersionProperty()
        throws Exception {

        FileInputStream fis =
            new FileInputStream(new File(envRoot, "je.properties"));
        Properties jeProperties = new Properties();
        jeProperties.load(fis);

        return new Boolean(jeProperties.getProperty
            (RepParams.PRESERVE_RECORD_VERSION.getName()));
    }

    private void checkDatabaseContents(Database db,
                                       int outerStart,
                                       int outerEnd,
                                       int innerStart,
                                       int innerEnd)
        throws Throwable {

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        Cursor cursor = db.openCursor(null, null);
        try {
            for (int i = outerStart; i < outerEnd; i++) {
                if (innerStart == innerEnd) {
                    cursor.getNext(key, data, null);
                    assertEquals(key, makeEntry(i));
                    assertEquals(data, makeEntry(i));
                    continue;
                }
                for (int j = innerStart; j < innerEnd; j++) {
                    cursor.getNext(key, data, null);
                    assertEquals(key, makeEntry(i));
                    assertEquals(data, makeEntry(j * 10 + i));
                }
            }
        } finally {
            cursor.close();
        }
    }

    private void loadLogFiles(String logName)
        throws Throwable {

        /* Create the environment home if it doesn't exist. */
        for (int i = 0; i < groupSize; i++) {
            File envDir = new File(envRoot, "rep" + i);
            if (!envDir.exists()) {
                envDir.mkdir();
            }
        }

        for (int i = 0; i < groupSize; i++) {
            String resName = "je-4.1.7_" + logName + "_" + i + ".jdb";
            TestUtils.loadLog
                (getClass(), resName, new File(envRoot, "rep" + i));
        }
    }

    private void updateDatabase(Database db) {
        /* Insert a piece of non duplicated record. */
        DatabaseEntry key = makeEntry(100);
        DatabaseEntry data = makeEntry(100);
        OperationStatus status = db.putNoOverwrite(null, key, data);
        assertEquals(OperationStatus.SUCCESS, status);

        /* Insert duplicated data. */
        key = makeEntry(5);
        data = makeEntry(101);
        status = db.putNoOverwrite(null, key, data);
        assertEquals(OperationStatus.KEYEXIST, status);
        status = db.put(null, key, data);
        assertEquals(OperationStatus.SUCCESS, status);

        /* Delete a piece of duplicated record. */
        status = db.delete(null, makeEntry(6));
        assertEquals(OperationStatus.SUCCESS, status);
        status = db.get(null, makeEntry(6), data, null);
        assertEquals(OperationStatus.NOTFOUND, status);
    }

    private DatabaseEntry makeEntry(int val) {
        byte[] data = new byte[] { (byte) val };
        return new DatabaseEntry(data);
    }
}
