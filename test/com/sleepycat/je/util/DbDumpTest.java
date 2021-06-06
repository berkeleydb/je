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

package com.sleepycat.je.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.Hashtable;

import org.junit.Test;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.tree.Key;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;
import com.sleepycat.utilint.StringUtils;

public class DbDumpTest extends TestBase {

    private final File envHome;

    private static final int N_KEYS = 100;
    private static final int N_KEY_BYTES = 1000;
    private static final String dbName = "testDB";

    private Environment env;

    public DbDumpTest() {
        envHome = SharedTestUtils.getTestDir();
    }

    /**
     * A simple test to check if JE's dump format matches Core.
     */
    @Test
    public void testMatchCore()
        throws Throwable {

        try {
            /* Set up a new environment. */
            EnvironmentConfig envConfig = TestUtils.initEnvConfig();
            envConfig.setAllowCreate(true);
            env = new Environment(envHome, envConfig);

            /*
             * Make a stream holding a small dump in a format known to be
             * the same as Core DB.
             */
            ByteArrayOutputStream dumpInfo = new ByteArrayOutputStream();
            PrintStream dumpStream = new PrintStream(dumpInfo);
            dumpStream.println("VERSION=3");
            dumpStream.println("format=print");
            dumpStream.println("type=btree");
            dumpStream.println("dupsort=0");
            dumpStream.println("HEADER=END");
            dumpStream.println(" abc");
            dumpStream.println(" firstLetters");
            dumpStream.println(" xyz");
            dumpStream.println(" lastLetters");
            dumpStream.println("DATA=END");

            /* load it */
            DbLoad loader = new DbLoad();
            loader.setEnv(env);
            loader.setInputReader(new BufferedReader(new InputStreamReader
                 (new ByteArrayInputStream(dumpInfo.toByteArray()))));
            loader.setNoOverwrite(false);
            loader.setDbName("foobar");
            loader.load();

            /* Make sure we retrieve the expected data. */
            Database checkDb = env.openDatabase(null, "foobar", null);
            Cursor cursor = checkDb.openCursor(null, null);
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry data = new DatabaseEntry();
            assertEquals(OperationStatus.SUCCESS,
                         cursor.getNext(key, data, LockMode.DEFAULT));
            assertEquals("abc", StringUtils.fromUTF8(key.getData()));
            assertEquals("firstLetters", StringUtils.fromUTF8(data.getData()));
            assertEquals(OperationStatus.SUCCESS,
                         cursor.getNext(key, data, LockMode.DEFAULT));
            assertEquals("xyz", StringUtils.fromUTF8(key.getData()));
            assertEquals("lastLetters", StringUtils.fromUTF8(data.getData()));
            assertEquals(OperationStatus.NOTFOUND,
                         cursor.getNext(key, data, LockMode.DEFAULT));
            cursor.close();
            checkDb.close();

            /* Check that a dump of the database matches the input file. */
            ByteArrayOutputStream dump2 = new ByteArrayOutputStream();
            DbDump dumper2 = new DbDump(env, "foobar",
                                        new PrintStream(dump2), true);
            dumper2.dump();
            assertEquals(dump2.toString(), dumpInfo.toString());

            env.close();
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }

    @Test
    public void testDumpLoadBinary()
        throws Throwable {

        try {
            doDumpLoadTest(false, 1);
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }

    @Test
    public void testDumpLoadPrintable()
        throws IOException, DatabaseException {

        doDumpLoadTest(true, 1);
    }

    @Test
    public void testDumpLoadTwo()
        throws IOException, DatabaseException {

        doDumpLoadTest(false, 2);
    }

    @Test
    public void testDumpLoadThree()
        throws IOException, DatabaseException {

        doDumpLoadTest(true, 3);
    }

    private void doDumpLoadTest(boolean printable, int nDumps)
        throws IOException, DatabaseException {

        Hashtable[] dataMaps = new Hashtable[nDumps];
        for (int i = 0; i < nDumps; i += 1) {
            dataMaps[i] = new Hashtable();
        }
        initDbs(nDumps, dataMaps);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(baos);
        for (int i = 0; i < nDumps; i += 1) {
            DbDump dumper =
                new DbDump(env, dbName + i, out, printable);
            dumper.dump();
        }
        byte[] baosba = baos.toByteArray();
        BufferedReader rdr = new BufferedReader
            (new InputStreamReader(new ByteArrayInputStream(baosba)));
        for (int i = 0; i < nDumps; i += 1) {
            DbLoad loader = new DbLoad();
            loader.setEnv(env);
            loader.setInputReader(rdr);
            loader.setNoOverwrite(false);
            loader.setDbName(dbName + i);
            loader.load();
            verifyDb(dataMaps[i], i);
        }

        ByteArrayOutputStream baos2 = new ByteArrayOutputStream();
        PrintStream out2 = new PrintStream(baos2);
        for (int i = 0; i < nDumps; i += 1) {
            DbDump dumper2 =
                new DbDump(env, dbName + i, out2, printable);
            dumper2.dump();
        }
        assertEquals(0, Key.compareKeys(baosba, baos2.toByteArray(), null));

        env.close();
    }

    /**
     * Set up the environment and db.
     */
    private void initDbs(int nDumps, Hashtable[] dataMaps)
        throws DatabaseException {

        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setConfigParam(EnvironmentParams.NODE_MAX.getName(), "6");
        envConfig.setAllowCreate(true);
        env = new Environment(envHome, envConfig);

        /* Make a db and open it. */
        for (int i = 0; i < nDumps; i += 1) {
            DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setAllowCreate(true);
            dbConfig.setSortedDuplicates(true);
            Database myDb = env.openDatabase(null, dbName + i, dbConfig);
            Cursor cursor = myDb.openCursor(null, null);
            doLargePut(dataMaps[i], cursor, N_KEYS);
            cursor.close();
            myDb.close();
        }
    }

    private void verifyDb(Hashtable<String,String> dataMap, int dumpIndex)
        throws DatabaseException {

        DatabaseConfig config = new DatabaseConfig();
        config.setReadOnly(true);
        DbInternal.setUseExistingConfig(config, true);
        Database myDb = env.openDatabase(null, dbName + dumpIndex, config);
        Cursor cursor = myDb.openCursor(null, null);
        StringDbt foundKey = new StringDbt();
        StringDbt foundData = new StringDbt();
        OperationStatus status =
            cursor.getFirst(foundKey, foundData, LockMode.DEFAULT);
        while (status == OperationStatus.SUCCESS) {
            String foundKeyString = foundKey.getString();
            String foundDataString = foundData.getString();
            if (dataMap.get(foundKeyString) != null) {
                assertTrue((dataMap.get(foundKeyString)).
                           equals(foundDataString));
                dataMap.remove(foundKeyString);
            } else {
                fail("didn't find key in either map (" +
                     foundKeyString +
                     ")");
            }
            status = cursor.getNext(foundKey, foundData, LockMode.DEFAULT);
        }
        assertTrue(dataMap.size() == 0);
        cursor.close();
        myDb.close();
    }

    private void doLargePut(Hashtable<String, String> dataMap, Cursor cursor, int nKeys)
        throws DatabaseException {

        for (int i = 0; i < nKeys; i++) {
            byte[] key = new byte[N_KEY_BYTES];
            TestUtils.generateRandomAlphaBytes(key);
            String keyString = StringUtils.fromUTF8(key);
            String dataString = Integer.toString(i);
            OperationStatus status =
                cursor.put(new StringDbt(key),
                           new StringDbt(dataString));
            assertEquals(OperationStatus.SUCCESS, status);
            if (dataMap != null) {
                dataMap.put(keyString, dataString);
            }
        }
    }
}
