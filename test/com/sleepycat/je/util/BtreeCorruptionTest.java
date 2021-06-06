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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.je.CheckpointConfig;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.Get;
import com.sleepycat.je.OperationFailureException;
import com.sleepycat.je.OperationResult;
import com.sleepycat.je.SecondaryConfig;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.SecondaryIntegrityException;
import com.sleepycat.je.SecondaryKeyCreator;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.CursorImpl;
import com.sleepycat.je.log.FileManager.FileMode;
import com.sleepycat.je.tree.BIN;
import com.sleepycat.je.tree.Key;
import com.sleepycat.je.txn.LockManager;
import com.sleepycat.je.util.verify.BtreeVerifier;
import com.sleepycat.je.utilint.CronScheduleParser;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.TestHook;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/*
 * Test the data corruption caused by internal bugs, i.e. Btree corruption.
 */
public class BtreeCorruptionTest extends TestBase {

    private static final String DB_NAME = "tempDB";
    private static final String SEC_DB1_NAME = "secDb1";
    private static final String SEC_DB2_NAME = "secDb2";
    private static final String DB_PRI_NAME = "priDb";
    private static final String DB_FOREIGN_NAME = "foreignDb";

    private Environment env;
    private File envHome;
    private final int recNum = 1000; //1000 * 50; //(1000 * 500) * 50 files
    private final int dataLen = 500;
    private final int totalWaitTries = 70;
    private int totalFiles = 0;

    /* Used to test basic btree verification code. */
    private Database db;
    private Cursor cursor;

    /*
     * Use two secondary database aims to check whether BtreeVerifer can still
     * continue to do check after find one corruption secondary database, i.e.
     * we expect that there should be two WARNING message for two corrupted
     * secondary database.
     */
    private SecondaryDatabase secDb1;
    private SecondaryDatabase secDb2;
    private SecondaryConfig secConfig1; 
    private SecondaryConfig secConfig2;
    private Database priDb;
    private Database foreignDb;

    private static final EnvironmentConfig envConfigWithVerifier
        = initConfig();
    private static final EnvironmentConfig envConfigWithoutVerifier
        = initConfig();

    static {
        envConfigWithoutVerifier.setConfigParam(
            EnvironmentParams.ENV_RUN_VERIFIER.getName(), "false");
    }

    @Before
    public void setUp() 
        throws Exception {
        envHome = SharedTestUtils.getTestDir();
        super.setUp();
    }

    @After
    public void tearDown() 
        throws Exception {
        CronScheduleParser.setCurCalHook = null;

        if (cursor != null) {
            try {
                cursor.close(); 
            } catch (EnvironmentFailureException efe) {

            }
            cursor = null;
        }

        if (db != null) {
            try {
                db.close(); 
            } catch (EnvironmentFailureException efe) {

            }
            db = null;
        }

        if (secDb1 != null) {
            try {
                secDb1.close(); 
            } catch (EnvironmentFailureException efe) {

            }
            secDb1 = null;
        }

        if (secDb2 != null) {
            try {
                secDb2.close(); 
            } catch (EnvironmentFailureException efe) {

            }
            secDb2 = null;
        }

        if (priDb != null) {
            try {
                priDb.close(); 
            } catch (EnvironmentFailureException efe) {

            }
            priDb = null;
        }

        if (foreignDb != null) {
            try {
                foreignDb.close(); 
            } catch (EnvironmentFailureException efe) {

            }
            foreignDb = null;
        }

        if (env != null) {
            env.close();
            env = null;
        }

        BtreeVerifier.databaseOperBeforeBatchCheckHook = null;
        BtreeVerifier.databaseOperDuringBatchCheckHook = null;

        super.tearDown();
    }

    private static EnvironmentConfig initConfig() {
        EnvironmentConfig config = TestUtils.initEnvConfig();
        config.setAllowCreate(true);
        config.setConfigParam(EnvironmentConfig.ENV_RUN_CLEANER, "false");
        config.setConfigParam(EnvironmentConfig.ENV_RUN_EVICTOR, "false");
        config.setConfigParam(EnvironmentConfig.ENV_RUN_CHECKPOINTER,
            "false");
        config.setConfigParam(EnvironmentConfig.ENV_RUN_IN_COMPRESSOR,
            "false");
        config.setCacheSize(1000000);
        config.setConfigParam(EnvironmentConfig.LOG_FILE_MAX, "1000000");
        config.setConfigParam(EnvironmentConfig.VERIFY_SCHEDULE, "* * * * *");
        return config;
    }

    public void openEnvAndDb(EnvironmentConfig config) {
        env = new Environment(envHome, config);

        final DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        db = env.openDatabase(null, DB_NAME, dbConfig);

        cursor = db.openCursor(null, null);
    }

    public void initialDb() {
        try {
            for (int i = 0 ; i < recNum; i++) {
                final DatabaseEntry key = new DatabaseEntry();
                IntegerBinding.intToEntry(i, key);
                final DatabaseEntry data = new DatabaseEntry(new byte[dataLen]);
                db.put(null, key, data);
            }
        } catch (DatabaseException dbe) {
            throw new RuntimeException("Initiate Database fails.", dbe);
        }

        DbInternal.getNonNullEnvImpl(env).getLogManager().flushSync();
        totalFiles =
            DbInternal.getEnvironmentImpl(env).getFileManager().                            
            getAllFileNumbers().length;
        System.out.println("Create files: " + totalFiles);
        assert totalFiles < 100 : "Total file number is " + totalFiles;
    }

    /*
     * TODO: This method only simulate the lsn/keyOrder/indentifierKey issues
     * on BIN. But it may not cover the following aspects:
     *   1. Issues on upperIN, see getParentIN
     *   2. Issues on BIN-delta, see DatabaseTest.mutateBINs. 
     */
    private void createBtreeCorrupt(String type, boolean persistent) {
        final String persis = persistent ? "persistent" : "transient";
        System.out.println("Create " + persis + " Corrupt type: " + type);

        final DatabaseEntry key = new DatabaseEntry();
        final DatabaseEntry data = new DatabaseEntry();

        final int usedKey = recNum / 100;
        final int usedIndex = 10;
        IntegerBinding.intToEntry(usedKey, key);
        assert cursor.get(key, data, Get.SEARCH, null) != null :
                "The db should contain this record: key is " + usedKey;

        final CursorImpl cursorImpl = DbInternal.getCursorImpl(cursor);
        cursorImpl.latchBIN();
        final BIN bin = cursorImpl.getBIN();
        try {
            if (type.equals("lsn")) {
                final long origLsn = bin.getLsn(usedIndex);
                bin.setLsn(
                    usedIndex, DbLsn.makeLsn(totalFiles + 100, 0x100));
                System.out.println(
                    "Chosen key: " + Key.dumpString(bin.getKey(usedIndex), 2));
                System.out.println(
                    "Problematic LSN: " +
                    DbLsn.getNoFormatString(bin.getLsn(usedIndex)) +
                    "Original LSN: " +
                    DbLsn.getNoFormatString(origLsn));
            } else if (type.equals("keyorder")) {
                final byte[] key1 = bin.getKey(usedIndex);
                final byte[] key2 = bin.getKey(usedIndex + 1);

                bin.setKey(usedIndex, key2, null, false);
                bin.setKey(usedIndex + 1, key1, null, false);

                System.out.println("Chosen keys are: " +
                    Key.dumpString(key1, 2) + "(index:" + usedIndex + ")" +
                    " " + Key.dumpString(key2, 2) + "(index:" +
                    (usedIndex + 1) + ")");
            } else if (type.equals("idenkey")) {
                final byte[] origIdenKey = bin.getIdentifierKey();

                /* The key of the last entry of the BIN*/
                final byte[] maxKey = bin.getKey(bin.getNEntries() - 1);
                final int len = maxKey.length;
                final byte[] newIdenKey = new byte[(len + 1)];
                System.arraycopy(maxKey, 0, newIdenKey, 0, len);
                newIdenKey[len] = 100;
                bin.setIdentifierKey(newIdenKey, true);

                System.out.println("Original Identifier Key is: " +
                    Key.dumpString(origIdenKey, 2) +
                 "Current Identifier Key is: " +
                    Key.dumpString(newIdenKey, 2));
            }
        } finally {
            cursorImpl.releaseBIN();
        }

        if (persistent) {
            CheckpointConfig cc = new CheckpointConfig();
            cc.setForce(true);
            env.checkpoint(cc);
            DbInternal.getNonNullEnvImpl(env).getLogManager().flushSync();   
        }
    }

    /*
     * The first pass traverse aims to cache all the log files. The second
     * pass traverse aims to check whether the Read operation can succeed
     * when one log file is corrupted, depending on whether ENV_RUN_VERIFIER
     * is set.
     */
    private void traverseDb(boolean check, boolean persistent) {
        boolean verify = DbInternal.getEnvironmentImpl(env).getConfigManager().
            getBoolean(EnvironmentParams.ENV_RUN_VERIFIER);
        try {
            final DatabaseEntry key = new DatabaseEntry();
            final DatabaseEntry data = new DatabaseEntry();

            int recordCount = 0;
            int firstKey;
            do {
                if (!check) {
                    firstKey = 0;
                } else {
                    firstKey = recNum / 2;
                }
                IntegerBinding.intToEntry(firstKey, key);

                assert cursor.get(key, data, Get.SEARCH, null) != null :
                    "The db should contain this record: key is " + firstKey;

                if (!check) {
                    while (cursor.get(key, data, Get.NEXT, null) != null) {
                        // Do nothing.
                    }
                }
                /*
                 * The smallest interval of the VERIFY_SCHEDULE is 1 minutes,
                 * so here we try to sleep 1s for totalWaitTries times to
                 * guarantee that the data corruption verifier task run at
                 * least once.
                 */
                try {Thread.sleep(1000);} catch (Exception e) {}
                System.out.println("check " + recordCount + " times.");
            } while (check && ++recordCount < totalWaitTries);

            if (check) {
                if (verify) {
                    fail("With verifying data corruption, we should catch" +
                        "EnvironmentFailureException.");
                }
            }
        } catch (EnvironmentFailureException efe) {
            //efe.printStackTrace();
            if (persistent) {
                assertTrue(efe.isCorrupted());
            } else {
                assertTrue(!efe.isCorrupted());
            }

            if (check) {
                if (!verify) {
                    fail("Without verifying data corruption, we should" +
                        "not catch EnvironmentFailureException");
                }
            }
            // Leave tearDown() to close cursor, db and env.
        }
    }

    /*
     * Lsn Dangling test
     */
    @Test
    public void testLSNDanglingWithVerifierTransient() {
        System.out.println("testLSNDanglingWithVerifierTransient");
        testCorruptionInternal(envConfigWithVerifier, "lsn", false);
    }

    @Test
    public void testLSNDanglingWithVerifierPersitent() {
        System.out.println("testLSNDanglingWithVerifierPersitent");
        testCorruptionInternal(envConfigWithVerifier, "lsn", true);
    }

    @Test
    public void testLSNDanglingWithoutVerifierTransient() {
        System.out.println("testLSNDanglingWithoutVerifierTransient");
        testCorruptionInternal(envConfigWithoutVerifier, "lsn", false);
    }

    @Test
    public void testLSNDanglingWithoutVerifierPersitent() {
        System.out.println("testLSNDanglingWithoutVerifierPersitent");
        testCorruptionInternal(envConfigWithoutVerifier, "lsn", true);
    }

    /*
     * Key order test
     */
    @Test
    public void testKeyOrderWithVerifierTransient() {
        System.out.println("testKeyOrderWithVerifierTransient");
        testCorruptionInternal(envConfigWithVerifier, "keyorder", false);
    }

    @Test
    @Ignore
    /*
     * TODO: Why ignore?
     *   I indeed create the key order violation, do checkpoint and at last
     *   call flushSync.
     *   
     *   But above action causes a BIN-delta to be logged. So when the
     *   BtreeVerifier wants to check whether the issue is persistent,
     *   BtreeVerifier will read the BIN-delta and the full BIN, and then
     *   re-constitute the BIN. During the process, for each slot of the
     *   BIN-delta, it will be checked whether its key is in the full BIN.
     *   If the key is in the full BIN, the corresponding slot of the full
     *   BIN will be updated. This kind of processing method will cause that
     *   the key order of the new reconstituted full BIN is right. So
     *   BtreeVerifier will think that the corruption is not persistent.
     *   
     *   Because key order issue seems to be rare and it seems that the work
     *   will be a little complicate to handle this issue, I am not sure
     *   how to handle this.
     */
    public void testKeyOrderWithVerifierPersitent() {
        System.out.println("testKeyOrderWithVerifierPersitent");
        testCorruptionInternal(envConfigWithVerifier, "keyorder", true);
    }

    @Test
    public void testKeyOrderWithoutVerifierTransient() {
        System.out.println("testKeyOrderWithoutVerifierTransient");
        testCorruptionInternal(envConfigWithoutVerifier, "keyorder", false);
    }

    @Test
    public void testKeyOrderWithoutVerifierPersitent() {
        System.out.println("testKeyOrderWithoutVerifierPersitent");
        testCorruptionInternal(envConfigWithoutVerifier, "keyorder", true);
    }

    /*
     * IndentifierKey test
     */
    @Test
    public void testIndentifyKeyWithVerifierTransient() {
        System.out.println("testIndentifyKeyWithVerifierTransient");
        testCorruptionInternal(envConfigWithVerifier, "idenkey", false);
    }

    @Test
    public void testIndentifyKeyWithVerifierPersitent() {
        System.out.println("testIndentifyKeyWithVerifierPersitent");
        testCorruptionInternal(envConfigWithVerifier, "idenkey", true);
    }

    @Test
    public void testIndentifyKeyWithoutVerifierTransient() {
        System.out.println("testIndentifyKeyWithoutVerifierTransient");
        testCorruptionInternal(envConfigWithoutVerifier, "idenkey", false);
    }

    @Test
    public void testIndentifyKeyWithoutVerifierPersitent() {
        System.out.println("testIndentifyKeyWithoutVerifierPersitent");
        testCorruptionInternal(envConfigWithoutVerifier, "idenkey", true);
    }

    private void testCorruptionInternal(
        EnvironmentConfig config,
        String type,
        boolean persistent) {

        openEnvAndDb(config);
        System.out.println("Finish open env");
        initialDb();
        System.out.println("Finish init db");
        traverseDb(false, persistent);
        System.out.println("Finish first pass traverse");
        createBtreeCorrupt(type, persistent);
        System.out.println("Finish create btree corruption");
        traverseDb(true, persistent);
        System.out.println("Finish second pass traverse");
    }

    /* 
     * The following tests aims to test index corruption verification code.
     */

    /*
     * The following part is used to test part 1 of index corruption
     * verification, i.e. BtreeVerifier can detect the corruption.
     */

    public void openEnvAndDbForIndexCorrupt(
        EnvironmentConfig config,
        String mode) {

        env = new Environment(envHome, config);

        final DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        priDb = env.openDatabase(null, DB_PRI_NAME, dbConfig);

        if (mode.equals("foreignNotExist")) {
            foreignDb = env.openDatabase(null, DB_FOREIGN_NAME, dbConfig);
        }

        secConfig1 = new SecondaryConfig();
        secConfig1.setAllowCreate(true);
        secConfig1.setAllowPopulate(true);
        secConfig1.setSortedDuplicates(true);
        if (mode.equals("foreignNotExist")) {
            secConfig1.setForeignKeyDatabase(foreignDb);
        }
        secConfig1.setKeyCreator(new SecondaryKeyCreator() {
            public boolean createSecondaryKey(SecondaryDatabase secondary,
                                              DatabaseEntry key,
                                              DatabaseEntry data,
                                              DatabaseEntry result) {
                result.setData
                    (data.getData(), data.getOffset(), data.getSize());
                return true;
            }
        });
        secDb1 =
            env.openSecondaryDatabase(null, SEC_DB1_NAME, priDb, secConfig1);

        /*
         * For testing the scenario where the foreing record does not exist,
         * we only use one secondary database. For other two modes, we use
         * two secondary databases.
         */
        if (!mode.equals("foreignNotExist")) {
            secConfig2 = new SecondaryConfig();
            secConfig2.setAllowCreate(true);
            secConfig2.setAllowPopulate(true);
            secConfig2.setSortedDuplicates(true);
            secConfig2.setKeyCreator(new SecondaryKeyCreator() {
                public boolean createSecondaryKey(SecondaryDatabase secondary,
                                                  DatabaseEntry key,
                                                  DatabaseEntry data,
                                                  DatabaseEntry result) {
                    int origData = IntegerBinding.entryToInt(data);
                    int newSecKey = origData * 2;
                    DatabaseEntry newKey = new DatabaseEntry();
                    IntegerBinding.intToEntry(newSecKey, newKey);
                    result.setData
                        (newKey.getData(), newKey.getOffset(), newKey.getSize());
                    return true;
                }
            });
            secDb2 =
                env.openSecondaryDatabase(null, SEC_DB2_NAME, priDb, secConfig2);
        }
    }

    public void initialDbForIndexCorrupt(String mode) {
        try {
            for (int i = 0 ; i < recNum; i++) {
                final DatabaseEntry key = new DatabaseEntry();
                final DatabaseEntry data = new DatabaseEntry();
                IntegerBinding.intToEntry(i, key);
                IntegerBinding.intToEntry(i + 1, data);
                /*
                 * Need to insert record to foreign db before inserting
                 * record to primary db, because the latter will update
                 * the secondary db, which will check whether the secondary
                 * key exist in the foreign db.
                 */
                if (mode.equals("foreignNotExist")) {
                    foreignDb.put(null, data, new DatabaseEntry(new byte[10]));
                }
                priDb.put(null, key, data);
            }
        } catch (DatabaseException dbe) {
            throw new RuntimeException("Initiate Database fails.", dbe);
        }

        DbInternal.getNonNullEnvImpl(env).getLogManager().flushSync();
        totalFiles =
            DbInternal.getEnvironmentImpl(env).getFileManager().                            
            getAllFileNumbers().length;
        System.out.println("Create files: " + totalFiles);
        assert totalFiles < 100 : "Total file number is " + totalFiles;
    }

    private void traverseOneDb(Database dbHandle, boolean sleep) {
        if (dbHandle == null) {
            return;
        }

        Cursor tmpCursor = dbHandle.openCursor(null, null);
        try {
            if (!sleep) {
                DatabaseEntry key = new DatabaseEntry();
                DatabaseEntry data = new DatabaseEntry();
                OperationResult opResult =
                    tmpCursor.get(key, data, Get.FIRST, null);
                assert opResult != null;
                while (opResult != null) {
                    opResult = tmpCursor.get(key, data, Get.NEXT, null);
                }
            } else {
                int saftToAccessKeyValue = recNum / 10;
                DatabaseEntry saftToAccessKey = new DatabaseEntry();
                IntegerBinding.intToEntry(saftToAccessKeyValue, saftToAccessKey);
                DatabaseEntry data = new DatabaseEntry();
                int count = 0;
                while (count++ < totalWaitTries) {
                    OperationResult opResult =
                        tmpCursor.get(saftToAccessKey, data, Get.SEARCH, null);
                    assert opResult != null;
                    try {Thread.sleep(1000);} catch (Exception e) {}
                    System.out.println(
                        "Wait for " + count + " times. IsCorrupt: " +
                        DbInternal.isCorrupted(dbHandle));
                }
            }
        } finally {
            if (tmpCursor != null) {
                tmpCursor.close();
            }
        }
    }

    private void traverseDbNormally() {
        traverseOneDb(priDb, false);
        traverseOneDb(secDb1, false);
        traverseOneDb(secDb2, false);
        traverseOneDb(foreignDb, false);
    }

    private void createIndexCorrupt(String mode) {
        if (mode.equals("primaryNotExist")) {
            /* Step1: close secondary db first. */
            secDb1.close();
            secDb2.close();

            /*
             * Step2: Delete one primary record. This will create index
             *        corruption because now the secondary databases can not
             *        be updated.
             */
            final int usedPriKeyValue = recNum / 100;
            DatabaseEntry usedPriKey = new DatabaseEntry();
            IntegerBinding.intToEntry(usedPriKeyValue, usedPriKey);
            priDb.delete(null, usedPriKey);

            /*
             * Step3: Reopen the secondary database. It seems to not matter
             *        whether I set allowPopulate to be true or false.
             * 1. Open as secondary is necessary because our verifier
             *    only check index when the dbImpl has secondary db handle.
             * 2. We can not use SecondaryCursor to traverse the secondary
             *    database, because SecondaryCursor can also detect SIE when
             *    accessing the problematic record. So we just use
             *    SecondaryCursor to access one safe record to  wait for the
             *    verifier to detect the corruption. 
             */
            secConfig1.setAllowPopulate(false);
            secConfig2.setAllowPopulate(false);
            secDb1 =
                env.openSecondaryDatabase(null, SEC_DB1_NAME, priDb, secConfig1);
            secDb2 =
                env.openSecondaryDatabase(null, SEC_DB2_NAME, priDb, secConfig2);
        } else if (mode.equals("foreignNotExist")) {
            /* Step1: close secondary db first. */
            secDb1.close();
    
            /*
             *  Step2: Delete one foreign record. This will create index
             *         corruption because now the secondary databases can not
             *         be updated.
             */
            final int usedForeingKeyValue = recNum / 100 + 1;
            DatabaseEntry usedForeignKey = new DatabaseEntry();
            IntegerBinding.intToEntry(usedForeingKeyValue, usedForeignKey);
            foreignDb.delete(null, usedForeignKey);
            
            /*
             *  Step3: Reopen the secondary database. It seems to not matter
             *         whether I set allowPopulate to be true or false.
             */
            secConfig1.setAllowPopulate(false);
            secDb1 =
                env.openSecondaryDatabase(null, SEC_DB1_NAME, priDb, secConfig1);
        } else if (mode.equals("SecondaryNotExist")) {
            /*
             *  Step1: close primary db first. But if we openSecondaryDb with
             *         priDb, we should first close secDb before closing priDb.
             */
            secDb1.close();
            secDb2.close();
            priDb.close();
    
            /*
             *  Step2: Delete secondary record. Open the secondary database
             *         as normal database. Now the primary databases can not
             *         be updated. The value is set according to the
             *         SecondaryKeyCreator.
             */
            final DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setAllowCreate(true);
            dbConfig.setSortedDuplicates(true);

            Database tmpSecDb1 = env.openDatabase(null, SEC_DB1_NAME, dbConfig);
            final int usedSecKeyValue1 = recNum / 100 + 1;
            DatabaseEntry usedSecKey1 = new DatabaseEntry();
            IntegerBinding.intToEntry(usedSecKeyValue1, usedSecKey1);
            tmpSecDb1.delete(null, usedSecKey1);

            Database tmpSecDb2 = env.openDatabase(null, SEC_DB2_NAME, dbConfig);
            final int usedSecKeyValue2 = (recNum / 100 + 2) * 2;
            DatabaseEntry usedSecKey2 = new DatabaseEntry();
            IntegerBinding.intToEntry(usedSecKeyValue2, usedSecKey2);
            tmpSecDb2.delete(null, usedSecKey2);

            tmpSecDb1.close();
            tmpSecDb2.close();

            /*
             *  Step3: Reopen the secondary database. It seems to not matter
             *         whether I set allowPopulate to be true or false.
             */
            final DatabaseConfig dbConfig1 = new DatabaseConfig();
            dbConfig1.setAllowCreate(true);
            priDb = env.openDatabase(null, DB_PRI_NAME, dbConfig1);

            secConfig1.setAllowPopulate(false);
            secConfig2.setAllowPopulate(false);
            secDb1 =
                env.openSecondaryDatabase(null, SEC_DB1_NAME, priDb, secConfig1);
            secDb2 =
                env.openSecondaryDatabase(null, SEC_DB2_NAME, priDb, secConfig2);
        } else {
            fail("Should at least specify one mode in 'primaryNotExist', " +
                "'foreignNotExist' and 'SecondaryNotExist'");
        }
    }

    private void traverseDbWaitToCheck(String mode) {
        /*
         * For any situation, at any time, traversing primary database and
         * foreign database should be OK. 
         */
        try {
            traverseOneDb(priDb, false);
            traverseOneDb(foreignDb, false);
        } catch (DatabaseException dbe) {
            fail("For any situation, at any time, traversing primary" +
                "database and foreign database should be OK.");
        }

        boolean verify =
            DbInternal.getEnvironmentImpl(env).getConfigManager().
            getBoolean(EnvironmentParams.ENV_RUN_VERIFIER);
        
        try {
            traverseOneDb(secDb1, true);
            if (verify) {
                fail("With verifying index corruption, the status of the " +
                    "secondary database should be CORRUPT. So we should" +
                    "catch OperationFailureException when calling" +
                    "Database.checkOpen.");
            }
        } catch (OperationFailureException ofe) {
            assert (ofe instanceof SecondaryIntegrityException);
            assert DbInternal.isCorrupted(secDb1);
            if (mode.equals("primaryNotExist")) {
                assert ((SecondaryIntegrityException) ofe.getCause()).
                    getMessage().contains(
                    "Secondary refers to a missing key in the primary database");
            } else if (mode.equals("foreignNotExist")) {
                assert ((SecondaryIntegrityException) ofe.getCause()).
                    getMessage().contains(
                    "Secondary key does not exist in foreign database");
            } else if (mode.equals("SecondaryNotExist")) {
                assert ((SecondaryIntegrityException) ofe.getCause()).
                    getMessage().contains(
                    "the primary record contains a key that is not present " +
                    "in this secondary database");
            }

            if (!verify) {
                fail("Without verifying index corruption, we should" +
                    "not catch OperationFailureException");
            }
        }

        if (!mode.equals("foreignNotExist")) {
            try {
                traverseOneDb(secDb2, true);
                if (verify) {
                    fail("With verifying index corruption, the status of the" +
                        "secondary database should be CORRUPT. So we should" +
                        "catch OperationFailureException when calling" +
                        "Database.checkOpen.");
                }
            } catch (OperationFailureException ofe) {
                assert (ofe instanceof SecondaryIntegrityException);
                assert DbInternal.isCorrupted(secDb2);
                if (mode.equals("primaryNotExist")) {
                    assert ((SecondaryIntegrityException) ofe.getCause()).
                        getMessage().contains(
                        "Secondary refers to a missing key in the primary database");
                } else if (mode.equals("SecondaryNotExist")) {
                    assert ((SecondaryIntegrityException) ofe.getCause()).
                        getMessage().contains(
                        "the primary record contains a key that is not present " +
                        "in this secondary database");
                }

                if (!verify) {
                    fail("Without verifying index corruption, we should" +
                        "not catch OperationFailureException");
                }
            }
        }

        /* Check the log contains the WARNING message if we enable verifier. */
        RandomAccessFile raf = null;
        try {
            final File file = new File(envHome.getCanonicalFile(), "je.info.0");
            assert file.exists();
            raf = new RandomAccessFile(
                file.getCanonicalFile(),
                FileMode.READWRITE_MODE.getModeValue());
            String newLine;
            int count = 0;
            while ((newLine = raf.readLine()) != null) {
                if (newLine.contains(
                    "Secondary corruption is detected during btree " +
                    "verification")) {
                    count++;
                }
            }

            /*
             * TODO: need to confirm. For each corrupted secondary database,
             *       there exist and only exist one WARNING message.
             */
            if (verify) {
                if (mode.equals("foreignNotExist")) {
                    assert count == 1 : count;
                } else {
                    assert count == 2 : count;
                }
            } else {
                assert count == 0 : count;
            }
        } catch (IOException ioe) {

        } finally {
            try { raf.close();} catch (IOException e) {}
        }
    }

    private void testIndexCorruptionInternal(
        EnvironmentConfig config, String mode) {

        openEnvAndDbForIndexCorrupt(config, mode);
        System.out.println("Finish open env");
        initialDbForIndexCorrupt(mode);
        System.out.println("Finish init db");
        traverseDbNormally();
        System.out.println("Finish first pass traverse");
        createIndexCorrupt(mode);
        System.out.println("Finish create index corruption");
        traverseDbWaitToCheck(mode);
        System.out.println("Finish check phase");
    }
    
    @Test
    public void testWithVerifierPrimaryRecordDoesNotExist () {
        testIndexCorruptionInternal(envConfigWithVerifier, "primaryNotExist");
    }

    @Test
    public void testWithoutVerifierPrimaryRecordDoesNotExist () {
        testIndexCorruptionInternal(envConfigWithoutVerifier, "primaryNotExist");
    }

    @Test
    public void testWithVerifierForeignRecordDoesNotExist () {
        testIndexCorruptionInternal(envConfigWithVerifier, "foreignNotExist");
    }

    @Test
    public void testWithoutVerifierForeignRecordDoesNotExist () {
        testIndexCorruptionInternal(envConfigWithoutVerifier, "foreignNotExist");
    }

    @Test
    public void testWithVerifierSecondaryRecordDoesNotExist () {
        EnvironmentConfig usedConfig = envConfigWithVerifier.clone();
        usedConfig.setConfigParam(
            EnvironmentParams.VERIFY_DATA_RECORDS.getName(), "true");
        testIndexCorruptionInternal(usedConfig, "SecondaryNotExist");
    }

    @Test
    public void testWithoutVerifierSecondaryRecordDoesNotExist () {
        testIndexCorruptionInternal(envConfigWithoutVerifier, "SecondaryNotExist");
    }

    /*
     * The following part is used to test part 2 of index corruption
     * verification, i.e. BtreeVerifier can still run normally after we inject
     * some abnormal situation, e.g. close or remove databases before or during
     * the batch check procedure.
     */

    private void openEnvAndDbForIndexCorruptPart2(
        boolean foreign,
        boolean secondary) {

        EnvironmentConfig usedConfig = envConfigWithVerifier.clone();
        usedConfig.setConfigParam(
            EnvironmentParams.VERIFY_DATA_RECORDS.getName(), "true");
        env = new Environment(envHome, usedConfig);

        final DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        priDb = env.openDatabase(null, DB_PRI_NAME, dbConfig);

        if (foreign) {
            foreignDb = env.openDatabase(null, DB_FOREIGN_NAME, dbConfig);
        }

        if (secondary) {
            secConfig1 = new SecondaryConfig();
            secConfig1.setAllowCreate(true);
            secConfig1.setAllowPopulate(true);
            secConfig1.setSortedDuplicates(true);
            if (foreign) {
                secConfig1.setForeignKeyDatabase(foreignDb);
            }
            secConfig1.setKeyCreator(new SecondaryKeyCreator() {
                public boolean createSecondaryKey(SecondaryDatabase secondary,
                                                  DatabaseEntry key,
                                                  DatabaseEntry data,
                                                  DatabaseEntry result) {
                    result.setData
                        (data.getData(), data.getOffset(), data.getSize());
                    return true;
                }
            });
            secDb1 =
                env.openSecondaryDatabase(null, SEC_DB1_NAME, priDb, secConfig1);
        }
    }

    private void initialDbForIndexCorruptPart2(boolean foreign) {
        try {
            for (int i = 0 ; i < recNum; i++) {
                final DatabaseEntry key = new DatabaseEntry();
                final DatabaseEntry data = new DatabaseEntry();
                IntegerBinding.intToEntry(i, key);
                IntegerBinding.intToEntry(i + 1, data);
                /*
                 * Need to insert record to foreign db before inserting
                 * record to primary db, because the latter will update
                 * the secondary db, which will check whether the secondary
                 * key exist in the foreign db.
                 */
                if (foreign) {
                    foreignDb.put(null, data, new DatabaseEntry(new byte[10]));
                }
                priDb.put(null, key, data);
            }
        } catch (DatabaseException dbe) {
            throw new RuntimeException("Initiate Database fails.", dbe);
        }

        DbInternal.getNonNullEnvImpl(env).getLogManager().flushSync();
        totalFiles =
            DbInternal.getEnvironmentImpl(env).getFileManager().                            
            getAllFileNumbers().length;
        System.out.println("Create files: " + totalFiles);
        assert totalFiles < 100 : "Total file number is " + totalFiles;
    }

    private void testPart2Internal (
        boolean foreign,
        boolean secondary,
        String testName,
        boolean isPri,
        boolean isSec,
        String action,
        String position) {

        openEnvAndDbForIndexCorruptPart2(foreign, secondary);
        System.out.println("Finish open env and db.");
        
        initialDbForIndexCorruptPart2(foreign);
        System.out.println("Finish init db.");

        /* Skip the first time run of BtreeVerifier .*/
        for (int i = 0; i < 3; i++) {
            try {Thread.sleep(10000);} catch (Exception e) {}
            System.out.println(
                i + "th: Sleep 10 seconds to skip first time run " +
                "of BtreeVerifier");
        }

        MyHook testHook = new MyHook(isPri, isSec, testName, action);
        if (position.equals("before")) {
            BtreeVerifier.databaseOperBeforeBatchCheckHook = testHook;
        } else if (position.equals("during")) {
            BtreeVerifier.databaseOperDuringBatchCheckHook = testHook;
        } else {
            fail("Wrong option");
        }

        /* Wait for the second time run of BtreeVerifier. */
        int i = 0;
        while (testHook.getCount() <= 0) {
            try {Thread.sleep(10000);} catch (Exception e) {}
            System.out.println(
                i + "th: Sleep 10 seconds to wait second time run " +
                "of BtreeVerifier");
            i++;
        }

        /*
         * Wait 5 more seconds to let the second time run of BtreeVerifier
         * finish.
         */
        try {Thread.sleep(5000);} catch (Exception e) {}
        System.out.println(
            "Wait 5 more seconds to let the second time run " +
            "of BtreeVerifier finish.");
        
        /* Check whether the test pass. */
        assert(env.isValid());
        if (testHook.getThrowable() != null) {
            testHook.getThrowable().printStackTrace();
        }
        assert testHook.getThrowable() == null;

        /*
         * TODO: Maybe need to add check whether the secondary database is
         *       corrupt and whether the log contains WARNING messages.
         *       
         *       FurtherMore, if we simulate index corruption, then whether
         *       the secondary database is corrupt and whether the log
         *       contains WARNING messages if we close/remove db before/during
         *       batch check.
         */
    }

    class MyHook implements TestHook<Database> {
        
        private boolean isPri;
        private boolean isSec;
        private String testName;
        private String action;

        /*
         * To indicate how many times doHook run really.
         */
        private int count = 0;
        private Throwable anyT;
        private boolean needSleep;

        public MyHook(
            boolean isPri,
            boolean isSec,
            String testName,
            String action) {
            this.isPri = isPri;
            this.isSec = isSec;
            this.testName = testName;
            this.action = action;
        }

        @Override
        public void doHook(final Database obj) {
            new Thread(testName) {
                @Override
                public void run() {
                    try {
                        if (isPri) {
                            if (obj == priDb && action.equals("close")) {
                                priDb.close();
                                needSleep = true;
                                count++;
                            }

                            if (obj == priDb && action.equals("remove")) {
                                if (secDb1 != null) {
                                    secDb1.close();
                                }
                                priDb.close();
                                env.removeDatabase(null, DB_PRI_NAME);
                                if (secDb1 != null) {
                                    env.removeDatabase(null, SEC_DB1_NAME);
                                }
                                needSleep = true;
                                count++;
                            }

                            if (obj == priDb && action.equals("truncate")) {
                                priDb.close();
                                env.truncateDatabase(null, DB_PRI_NAME, false);
                                needSleep = true;
                                count++;
                            }
                        } else if (isSec) {
                            if (obj == secDb1 && action.equals("close")) {
                                secDb1.close();
                                needSleep = true;
                                count++;
                            }

                            if (obj == secDb1 && action.equals("remove")) {
                                secDb1.close();
                                env.removeDatabase(null, SEC_DB1_NAME);
                                needSleep = true;
                                count++;
                            }
                            
                            if (obj == secDb1 && action.equals("truncate")) {
                                secDb1.close();
                                env.truncateDatabase(null, SEC_DB1_NAME, false);
                                needSleep = true;
                                count++;
                            }
                        } else {
                            fail("Wrong option");
                        }
                    } catch (Throwable t) {
                        anyT = t;
                    } finally {
                        
                    }  
                }
            
            }.start();

            if (needSleep) {
                /* Wait 5 more seconds to let above thread to run. */
                try {Thread.sleep(5000);} catch (Exception e) {}
                System.out.println(
                    "Wait 5 seconds to let testHook do " +
                    "close/remove/truncate.");
                needSleep = false;
            }
        }

        public int getCount() {
            return count;
        }

        public Throwable getThrowable() {
            return anyT;
        }

        @Override
        public void doHook() {
        }
        @Override
        public void hookSetup() {
        }
        @Override
        public void doIOHook() throws IOException {
        }
        @Override
        public Database getHookValue() {
            return null;
        }
    }

    /*
     * TODO: We also need to add test cases about foreign database. Now the
     *       following test cases only contain Primary/Secondary databases.
     */
    /*
    @Test
    public void testClosePriDbBeforeBatch () {
        testPart2Internal(
            false, false, "testClosePriDbBeforeBatch", true, false,
            "close", "before");
    }

    @Test
    public void testCloseSecDbBeforeBatch () {
        testPart2Internal(
            false, true, "testCloseSecDbBeforeBatch", false, true,
            "close", "before");
    }

    @Test
    public void testRemovePriDbBeforeBatch () {
        testPart2Internal(
            false, false, "testRemovePriDbBeforeBatch", true, false,
            "remove", "before");
    }

    @Test
    public void testRemoveSecDbBeforeBatch () {
        testPart2Internal(
            false, true, "testRemoveSecDbBeforeBatch", false, true,
            "remove", "before");
    }

    @Test
    public void testTruncatePriDbBeforeBatch () {
        testPart2Internal(
            false, false, "testRemovePriDbBeforeBatch", true, false,
            "truncate", "before");
    }

    @Test
    public void testTruncateSecDbBeforeBatch () {
        testPart2Internal(
            false, true, "testRemoveSecDbBeforeBatch", false, true,
            "truncate", "before");
    }

    @Test
    public void testRemovePriAndSecDbBeforeBatch () {
        testPart2Internal(
            false, true, "testRemovePriAndSecDbBeforeBatch", true, false,
            "remove", "before");
    }

    @Test
    public void testClosePriDbDuringBatch () {
        testPart2Internal(
            false, false, "testClosePriDbBeforeBatch", true, false,
            "close", "during");
    }

    @Test
    public void testCloseSecDbDuringBatch () {
        testPart2Internal(
            false, true, "testCloseSecDbBeforeBatch", false, true,
            "close", "during");
    }

    @Test
    public void testRemovePriDbDuringBatch () {
        testPart2Internal(
            false, false, "testRemovePriDbBeforeBatch", true, false,
            "remove", "during");
    }

    @Test
    public void testRemoveSecDbDuringBatch () {
        testPart2Internal(
            false, true, "testRemoveSecDbBeforeBatch", false, true,
            "remove", "during");
    }

    @Test
    public void testTruncatePriDbDuringBatch () {
        testPart2Internal(
            false, false, "testRemovePriDbBeforeBatch", true, false,
            "truncate", "during");
    }

    @Test
    public void testTruncateSecDbDuringBatch () {
        testPart2Internal(
            false, true, "testRemoveSecDbBeforeBatch", false, true,
            "truncate", "during");
    }

    @Test
    public void testRemovePriAndSecDbDuringBatch () {
        testPart2Internal(
            false, true, "testRemovePriAndSecDbBeforeBatch", true, false,
            "remove", "during");
    }
    */
}

