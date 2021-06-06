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
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import org.junit.After;
import org.junit.Test;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DatabaseNotFoundException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.log.FileManager;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;
import com.sleepycat.utilint.StringUtils;

public class DbScavengerTest extends TestBase {

    private static final int TRANSACTIONAL = 1 << 0;
    private static final int WRITE_MULTIPLE = 1 << 1;
    private static final int PRINTABLE = 1 << 2;
    private static final int ABORT_BEFORE = 1 << 3;
    private static final int ABORT_AFTER = 1 << 4;
    private static final int CORRUPT_LOG = 1 << 5;
    private static final int DELETE_DATA = 1 << 6;
    private static final int AGGRESSIVE = 1 << 7;

    private static final int N_DBS = 3;
    private static final int N_KEYS = 100;
    private static final int N_DATA_BYTES = 100;
    private static final int LOG_SIZE = 10000;

    private final String envHomeName;
    private final File envHome;

    private Environment env;

    private Database[] dbs = new Database[N_DBS];

    private final boolean duplicatesAllowed = true;

    public DbScavengerTest() {
        envHome = SharedTestUtils.getTestDir();
        envHomeName = envHome.getAbsolutePath();
    }

    @After
    public void tearDown() {
        if (env != null) {
            try {
                env.close();
            } catch (Exception e) {
                System.out.println("TearDown: " + e);
            }
            env = null;
        }
    }

    @Test
    public void testScavenger1() {
        doScavengerTest(PRINTABLE | TRANSACTIONAL |
                        ABORT_BEFORE | ABORT_AFTER);
    }

    @Test
    public void testScavenger2() {
        doScavengerTest(PRINTABLE | TRANSACTIONAL | ABORT_BEFORE);
    }

    @Test
    public void testScavenger3() {
        doScavengerTest(PRINTABLE | TRANSACTIONAL | ABORT_AFTER);
    }

    @Test
    public void testScavenger4() {
        doScavengerTest(PRINTABLE | TRANSACTIONAL);
    }

    @Test
    public void testScavenger5() {
        doScavengerTest(PRINTABLE | WRITE_MULTIPLE | TRANSACTIONAL);
    }

    @Test
    public void testScavenger6() {
        doScavengerTest(PRINTABLE);
    }

    @Test
    public void testScavenger7() {
        doScavengerTest(TRANSACTIONAL | ABORT_BEFORE | ABORT_AFTER);
    }

    @Test
    public void testScavenger8() {
        doScavengerTest(TRANSACTIONAL | ABORT_BEFORE);
    }

    @Test
    public void testScavenger9() {
        doScavengerTest(TRANSACTIONAL);
    }

    @Test
    public void testScavenger10() {
        doScavengerTest(TRANSACTIONAL | ABORT_AFTER);
    }

    @Test
    public void testScavenger11() {
        doScavengerTest(0);
    }

    @Test
    public void testScavenger12() {
        doScavengerTest(CORRUPT_LOG);
    }

    @Test
    public void testScavenger13() {
        doScavengerTest(DELETE_DATA);
    }

    @Test
    public void testScavenger14() {
        doScavengerTest(AGGRESSIVE);
    }

    @Test
    public void testScavengerAbortedDbLevelOperations() {
        createEnv(true, true, false);
        boolean doAbort = true;
        byte[] dataBytes = new byte[N_DATA_BYTES];
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry(dataBytes);
        IntegerBinding.intToEntry(1, key);
        TestUtils.generateRandomAlphaBytes(dataBytes);
        for (int i = 0; i < 2; i++) {
            Transaction txn = env.beginTransaction(null, null);
            for (int dbCnt = 0; dbCnt < N_DBS; dbCnt++) {
                String databaseName = null;
                if (doAbort) {
                    databaseName = "abortedDb" + dbCnt;
                } else {
                    databaseName = "simpleDb" + dbCnt;
                }
                DatabaseConfig dbConfig = new DatabaseConfig();
                dbConfig.setAllowCreate(true);
                dbConfig.setSortedDuplicates(duplicatesAllowed);
                dbConfig.setTransactional(true);
                if (dbs[dbCnt] != null) {
                    throw new RuntimeException("database already open");
                }
                Database db =
                    env.openDatabase(txn, databaseName, dbConfig);
                dbs[dbCnt] = db;
                db.put(txn, key, data);
            }
            if (doAbort) {
                txn.abort();
                dbs = new Database[N_DBS];
            } else {
                txn.commit();
            }
            doAbort = !doAbort;
        }

        closeEnv();
        createEnv(false, false, false);
        openDbs(false, false, duplicatesAllowed, null);
        dumpDbs(false, false);

        /* Close the environment, delete it completely from the disk. */
        closeEnv();
        TestUtils.removeLogFiles("doScavengerTest", envHome, false);

        /* Recreate and reload the environment from the scavenger files. */
        createEnv(true, true, false);
        loadDbs();

        /* Verify that the data is the same as when it was created. */
        for (int dbCnt = 0; dbCnt < N_DBS; dbCnt++) {
            String databaseName = "abortedDb" + dbCnt;
            DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setAllowCreate(false);
            try {
                env.openDatabase(null, databaseName, dbConfig);
                fail("expected DatabaseNotFoundException");
            } catch (DatabaseNotFoundException DNFE) {
                /* Expected. */
            }
        }
        closeEnv();
    }

    private void doScavengerTest(int config)
        throws DatabaseException {

        boolean printable = (config & PRINTABLE) != 0;
        boolean transactional = (config & TRANSACTIONAL) != 0;
        boolean writeMultiple = (config & WRITE_MULTIPLE) != 0;
        boolean abortBefore = (config & ABORT_BEFORE) != 0;
        boolean abortAfter = (config & ABORT_AFTER) != 0;
        boolean corruptLog = (config & CORRUPT_LOG) != 0;
        boolean deleteData = (config & DELETE_DATA) != 0;
        boolean aggressive = (config & AGGRESSIVE) != 0;

        assert transactional ||
            (!abortBefore && !abortAfter);

        Map[] dataMaps = new Map[N_DBS];
        Set<Long> lsnsToCorrupt = new HashSet<Long>();
        /* Create the environment and some data. */
        createEnvAndDbs(dataMaps,
                        writeMultiple,
                        transactional,
                        abortBefore,
                        abortAfter,
                        corruptLog,
                        lsnsToCorrupt,
                        deleteData);
        closeEnv();
        createEnv(false, false, corruptLog);
        if (corruptLog) {
            corruptFiles(lsnsToCorrupt);
        }
        openDbs(false, false, duplicatesAllowed, null);
        dumpDbs(printable, aggressive);

        /* Close the environment, delete it completely from the disk. */
        closeEnv();
        TestUtils.removeLogFiles("doScavengerTest", envHome, false);

        /* Recreate the environment and load it from the scavenger files. */
        createEnv(true, transactional, corruptLog);
        loadDbs();

        /* Verify that the data is the same as when it was created. */
        openDbs(false, false, duplicatesAllowed, null);
        verifyDbs(dataMaps);
        closeEnv();
    }

    private void closeEnv()
        throws DatabaseException {

        for (int i = 0; i < N_DBS; i++) {
            if (dbs[i] != null) {
                dbs[i].close();
                dbs[i] = null;
            }
        }

        env.close();
        env = null;
    }

    private void createEnv(
        boolean create,
        boolean transactional,
        boolean corruptLog)
        throws DatabaseException {

        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        DbInternal.disableParameterValidation(envConfig);
        envConfig.setConfigParam(
            EnvironmentParams.ENV_RUN_CHECKPOINTER.getName(), "false");
        envConfig.setConfigParam(
            EnvironmentParams.ENV_RUN_CLEANER.getName(), "false");
        envConfig.setConfigParam(
            EnvironmentParams.LOG_FILE_MAX.getName(), "" + LOG_SIZE);
        envConfig.setTransactional(transactional);
        envConfig.setAllowCreate(create);
        if (corruptLog) {
            envConfig.setConfigParam(
                EnvironmentConfig.ENV_RUN_VERIFIER, "false"); 
        }
        env = new Environment(envHome, envConfig);
    }

    private void createEnvAndDbs(Map[] dataMaps,
                                 boolean writeMultiple,
                                 boolean transactional,
                                 boolean abortBefore,
                                 boolean abortAfter,
                                 boolean corruptLog,
                                 Set<Long> lsnsToCorrupt,
                                 boolean deleteData)
        throws DatabaseException {

        createEnv(true, transactional, corruptLog);
        Transaction txn = null;
        if (transactional) {
            txn = env.beginTransaction(null, null);
        }

        openDbs(true, transactional, duplicatesAllowed, txn);

        if (transactional) {
            txn.commit();
        }

        long lastCorruptedFile = -1;
        for (int dbCnt = 0; dbCnt < N_DBS; dbCnt++) {
            Map<Integer, String> dataMap = new HashMap<Integer, String>();
            dataMaps[dbCnt] = dataMap;
            Database db = dbs[dbCnt];

            for (int i = 0; i < N_KEYS; i++) {
                byte[] dataBytes = new byte[N_DATA_BYTES];
                DatabaseEntry key = new DatabaseEntry();
                DatabaseEntry data = new DatabaseEntry(dataBytes);
                IntegerBinding.intToEntry(i, key);
                TestUtils.generateRandomAlphaBytes(dataBytes);

                boolean corruptedThisEntry = false;

                if (transactional) {
                    txn = env.beginTransaction(null, null);
                }

                if (transactional &&
                    abortBefore) {
                    assertEquals(OperationStatus.SUCCESS,
                                 db.put(txn, key, data));
                    txn.abort();
                    txn = env.beginTransaction(null, null);
                }

                assertEquals(OperationStatus.SUCCESS,
                             db.put(txn, key, data));
                if (corruptLog) {
                    long currentLsn = getLastLsn();
                    long fileNumber = DbLsn.getFileNumber(currentLsn);
                    long fileOffset = DbLsn.getFileOffset(currentLsn);
                    if (fileOffset > (LOG_SIZE >> 1) &&
                        /* We're writing in the second half of the file. */
                        fileNumber > lastCorruptedFile) {
                        /* Corrupt this file. */
                        lsnsToCorrupt.add(new Long(currentLsn));
                        lastCorruptedFile = fileNumber;
                        corruptedThisEntry = true;
                    }
                }

                if (writeMultiple) {
                    assertEquals(OperationStatus.SUCCESS,
                                 db.delete(txn, key));
                    assertEquals(OperationStatus.SUCCESS,
                                 db.put(txn, key, data));
                }

                if (deleteData) {
                    assertEquals(OperationStatus.SUCCESS,
                                 db.delete(txn, key));
                    /* overload this for deleted data. */
                    corruptedThisEntry = true;
                }

                if (!corruptedThisEntry) {
                    dataMap.put(new Integer(i),
                                StringUtils.fromUTF8(dataBytes));
                }

                if (transactional) {
                    txn.commit();
                }

                if (transactional &&
                    abortAfter) {
                    txn = env.beginTransaction(null, null);
                    assertEquals(OperationStatus.SUCCESS,
                                 db.put(txn, key, data));
                    txn.abort();
                }
            }
        }
    }

    private void openDbs(boolean create,
                         boolean transactional,
                         boolean duplicatesAllowed,
                         Transaction txn)
        throws DatabaseException {

        for (int dbCnt = 0; dbCnt < N_DBS; dbCnt++) {
            String databaseName = "simpleDb" + dbCnt;
            DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setAllowCreate(create);
            dbConfig.setSortedDuplicates(duplicatesAllowed);
            dbConfig.setTransactional(transactional);
            if (dbs[dbCnt] != null) {
                throw new RuntimeException("database already open");
            }
            dbs[dbCnt] = env.openDatabase(txn, databaseName, dbConfig);
        }
    }

    private void dumpDbs(boolean printable, boolean aggressive)
        throws DatabaseException {

        try {
            DbScavenger scavenger =
                new DbScavenger(env, envHomeName, printable, aggressive,
                                false /* verbose */);
            scavenger.dump();
        } catch (IOException IOE) {
            throw new RuntimeException(IOE);
        }
    }

    private void loadDbs()
        throws DatabaseException {

        try {
            String dbNameBase = "simpleDb";
            for (int i = 0; i < N_DBS; i++) {
                DbLoad loader = new DbLoad();
                File file = new File(envHomeName, dbNameBase + i + ".dump");
                FileInputStream is = new FileInputStream(file);
                BufferedReader reader =
                    new BufferedReader(new InputStreamReader(is));
                loader.setEnv(env);
                loader.setInputReader(reader);
                loader.setNoOverwrite(false);
                loader.setDbName(dbNameBase + i);
                loader.load();
                is.close();
            }
        } catch (IOException IOE) {
            throw new RuntimeException(IOE);
        }
    }

    private void verifyDbs(Map[] dataMaps)
        throws DatabaseException {

        for (int i = 0; i < N_DBS; i++) {
            Map dataMap = dataMaps[i];
            Cursor cursor = dbs[i].openCursor(null, null);
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry data = new DatabaseEntry();
            while (cursor.getNext(key, data, null) ==
                   OperationStatus.SUCCESS) {
                Integer keyInt =
                    new Integer(IntegerBinding.entryToInt(key));
                String databaseString = StringUtils.fromUTF8(data.getData());
                String originalString = (String) dataMap.get(keyInt);
                if (originalString == null) {
                    fail("couldn't find " + keyInt);
                } else if (databaseString.equals(originalString)) {
                    dataMap.remove(keyInt);
                } else {
                    fail(" Mismatch: key=" + keyInt +
                         " Expected: " + originalString +
                         " Found: " + databaseString);
                }
            }

            if (dataMap.size() > 0) {
                fail("entries still remain for db " + i + ": " +
                     dataMap.keySet());
            }

            cursor.close();
        }
    }

    private static DumpFileFilter dumpFileFilter = new DumpFileFilter();

    static class DumpFileFilter implements FilenameFilter {

        /**
         * Accept files of this format:
         * *.dump
         */
        public boolean accept(File dir, String name) {
            StringTokenizer tokenizer = new StringTokenizer(name, ".");
            /* There should be two parts. */
            if (tokenizer.countTokens() == 2) {
                tokenizer.nextToken();
                String fileSuffix = tokenizer.nextToken();

                /* Check the length and the suffix. */
                if (fileSuffix.equals("dump")) {
                    return true;
                }
            }

            return false;
        }
    }

    private long getLastLsn() {
        return DbInternal.getNonNullEnvImpl(env).
            getFileManager().getLastUsedLsn();
    }

    private void corruptFiles(Set<Long> lsnsToCorrupt)
        throws DatabaseException {

        Iterator<Long> iter = lsnsToCorrupt.iterator();
        while (iter.hasNext()) {
            long lsn = iter.next().longValue();
            corruptFile(DbLsn.getFileNumber(lsn),
                        DbLsn.getFileOffset(lsn));
        }
    }

    private void corruptFile(long fileNumber, long fileOffset)
        throws DatabaseException {

        String fileName = DbInternal.getNonNullEnvImpl(env).
            getFileManager().getFullFileName(fileNumber,
                                             FileManager.JE_SUFFIX);
        /*
        System.out.println("corrupting 1 byte at " +
                           DbLsn.makeLsn(fileNumber, fileOffset));
        */
        try {
            RandomAccessFile raf = new RandomAccessFile(fileName, "rw");
            raf.seek(fileOffset);
            int current = raf.read();
            raf.seek(fileOffset);
            raf.write(current + 1);
            raf.close();
        } catch (IOException IOE) {
            throw new RuntimeException(IOE);
        }
    }
}
