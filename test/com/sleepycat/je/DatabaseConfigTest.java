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

package com.sleepycat.je;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.Serializable;
import java.util.Comparator;
import java.util.Set;

import org.junit.Test;

import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.DbConfigManager;
import com.sleepycat.je.util.DualTestCase;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.util.test.SharedTestUtils;

/**
 * Basic database configuration testing.
 */
public class DatabaseConfigTest extends DualTestCase {
    private static final boolean DEBUG = false;

    private final File envHome;
    private Environment env;

    public DatabaseConfigTest() {
        envHome = SharedTestUtils.getTestDir();
    }

    /**
     * Test that we can retrieve a database configuration and that it clones
     * its configuration appropriately.
     */
    @Test
    public void testConfig()
        throws Throwable {

        try {
            EnvironmentConfig envConfig = TestUtils.initEnvConfig();
            envConfig.setAllowCreate(true);
            env = create(envHome, envConfig);

            /*
             * Make sure that the database keeps its own copy of the
             * configuration object.
             */
            DatabaseConfig dbConfigA = new DatabaseConfig();
            dbConfigA.setAllowCreate(true);
            Database dbA = env.openDatabase(null, "foo", dbConfigA);

            /* Change the original dbConfig */
            dbConfigA.setAllowCreate(false);
            DatabaseConfig getConfig1 = dbA.getConfig();
            assertEquals(true, getConfig1.getAllowCreate());
            assertEquals(false, getConfig1.getSortedDuplicates());

            /*
             * Change the retrieved config, ought to have no effect on what the
             * Database is storing.
             */
            getConfig1.setSortedDuplicates(true);
            DatabaseConfig getConfig2 = dbA.getConfig();
            assertEquals(false, getConfig2.getSortedDuplicates());

            dbA.close();
            close(env);
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }

    @Test
    public void testConfigMatching()
        throws Throwable {

        try {
            /* DatabaseConfig matching. */

            DatabaseConfig confA = new DatabaseConfig();
            DatabaseConfig confB = new DatabaseConfig();

            try {
                confA.validate(confB);
            } catch (Exception E) {
                fail("expected valid match");
            }

            try {
                confB.validate(confA);
            } catch (Exception E) {
                fail("expected valid match");
            }

            try {
                confA.validate(null); // uses the DEFAULT config
            } catch (Exception E) {
                fail("expected valid match");
            }

            confA.setReadOnly(true);
            try {
                confA.validate(confB);
                fail("expected exception");
            } catch (IllegalArgumentException E) {
                // ok
            }

            confA.setReadOnly(false);
            confA.setSortedDuplicates(true);
            try {
                confA.validate(confB);
                fail("expected exception");
            } catch (IllegalArgumentException E) {
                // ok
            }
            confA.setSortedDuplicates(false);

            confA.setOverrideBtreeComparator(true);
            confA.setBtreeComparator(TestComparator.class);
            confB.setOverrideBtreeComparator(true);
            confB.setBtreeComparator(TestComparator2.class);
            try {
                confA.validate(confB);
                fail("expected exception");
            } catch (IllegalArgumentException E) {
                // ok
            }
            confA.setBtreeComparator((Class) null);
            confA.setOverrideBtreeComparator(false);
            confB.setBtreeComparator((Class) null);
            confB.setOverrideBtreeComparator(false);

            confA.setOverrideDuplicateComparator(true);
            confA.setDuplicateComparator(TestComparator.class);
            confB.setOverrideDuplicateComparator(true);
            confB.setDuplicateComparator(TestComparator2.class);
            try {
                confA.validate(confB);
                fail("expected exception");
            } catch (IllegalArgumentException E) {
                // ok
            }

            /* Same tests as above but for serialized comparators. */

            confA.setOverrideBtreeComparator(true);
            confA.setBtreeComparator(new TestSerialComparator());
            confB.setOverrideBtreeComparator(true);
            confB.setBtreeComparator(new TestSerialComparator2());
            try {
                confA.validate(confB);
                fail("expected exception");
            } catch (IllegalArgumentException E) {
                // ok
            }
            confA.setBtreeComparator((Comparator) null);
            confA.setOverrideBtreeComparator(false);
            confB.setBtreeComparator((Comparator) null);
            confB.setOverrideBtreeComparator(false);

            confA.setOverrideDuplicateComparator(true);
            confA.setDuplicateComparator(new TestSerialComparator());
            confB.setOverrideDuplicateComparator(true);
            confB.setDuplicateComparator(new TestSerialComparator2());
            try {
                confA.validate(confB);
                fail("expected exception");
            } catch (IllegalArgumentException E) {
                // ok
            }

            /* SecondaryConfig matching. */

            SecondaryConfig confC = new SecondaryConfig();
            SecondaryConfig confD = new SecondaryConfig();
            confC.setKeyCreator(new SecKeyCreator1());
            confD.setKeyCreator(new SecKeyCreator1());

            try {
                confC.validate(confD);
            } catch (Exception E) {
                E.printStackTrace();
                fail("expected valid match");
            }

            try {
                confD.validate(confC);
            } catch (Exception E) {
                fail("expected valid match");
            }

            try {
                confC.validate(null);
                fail("expected exception");
            } catch (IllegalArgumentException E) {
                // ok
            }

            confD.setKeyCreator(new SecKeyCreator2());
            try {
                confC.validate(confD);
                fail("expected exception");
            } catch (IllegalArgumentException E) {
                // ok
            }
            confD.setKeyCreator(new SecKeyCreator1());

            confD.setMultiKeyCreator(new SecMultiKeyCreator1());
            try {
                confC.validate(confD);
                fail("expected exception");
            } catch (IllegalArgumentException E) {
                // ok
            }
            confD.setMultiKeyCreator(null);

            confC.setForeignKeyDeleteAction(ForeignKeyDeleteAction.NULLIFY);
            try {
                confC.validate(confD);
                fail("expected exception");
            } catch (IllegalArgumentException E) {
                // ok
            }
            confC.setForeignKeyDeleteAction(ForeignKeyDeleteAction.ABORT);

            confC.setForeignKeyNullifier(new ForeignKeyNullifier1());
            try {
                confC.validate(confD);
                fail("expected exception");
            } catch (IllegalArgumentException E) {
                // ok
            }
            confC.setForeignKeyNullifier(null);

            confC.setForeignMultiKeyNullifier(new ForeignMultiKeyNullifier1());
            try {
                confC.validate(confD);
                fail("expected exception");
            } catch (IllegalArgumentException E) {
                // ok
            }
            confC.setForeignMultiKeyNullifier(null);

            confC.setImmutableSecondaryKey(true);
            try {
                confC.validate(confD);
                fail("expected exception");
            } catch (IllegalArgumentException E) {
                // ok
            }
            confC.setImmutableSecondaryKey(false);
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }

    /**
     * Make sure we can instantiate a comparator at the time it's set.
     */
    @Test
    public void testComparator()
        throws Throwable {

        try {
            /* Can't be instantiated, a nested class */
            try {
                DatabaseConfig config = new DatabaseConfig();
                config.setBtreeComparator(BadComparator1.class);
                fail("Comparator shouldn't be instantiated");
            } catch (IllegalArgumentException e) {
                /* Expected. */
                if (DEBUG) {
                    System.out.println(e);
                }
            }

            /* No zero-parameter constructor */
            try {
                DatabaseConfig config = new DatabaseConfig();
                config.setBtreeComparator(BadComparator2.class);
                fail("Comparator shouldn't be instantiated");
            } catch (IllegalArgumentException e) {
                /* Expected. */
                if (DEBUG) {
                    System.out.println(e);
                }
            }

            /* Can't be serialized, not serializable */
            try {
                DatabaseConfig config = new DatabaseConfig();
                config.setBtreeComparator(new BadSerialComparator1());
                fail("Comparator shouldn't be instantiated");
            } catch (IllegalArgumentException e) {
                /* Expected. */
                if (DEBUG) {
                    System.out.println(e);
                }
            }

            /* Can't be serialized, contains non-serializable field */
            try {
                DatabaseConfig config = new DatabaseConfig();
                config.setBtreeComparator(new BadSerialComparator2());
                fail("Comparator shouldn't be instantiated");
            } catch (IllegalArgumentException e) {
                /* Expected. */
                if (DEBUG) {
                    System.out.println(e);
                }
            }

            /* Valid comparators */
            DatabaseConfig config = new DatabaseConfig();
            config.setBtreeComparator(TestComparator.class);
            config.setBtreeComparator(TestComparator2.class);
            config.setBtreeComparator(new TestSerialComparator());
            config.setBtreeComparator(new TestSerialComparator2());

        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }

    /**
     * Test that any conflicts between configuration object settings and the
     * underlying impl object are detected.
     */
    @Test
    public void testConfigConflict()
        throws Throwable {

        try {
            EnvironmentConfig envConfig = TestUtils.initEnvConfig();
            envConfig.setAllowCreate(true);
            env = create(envHome, envConfig);

            /*
             * Test conflicts of duplicate allowed configuration.
             */

            /* 1a. Create allowing duplicates. */
            DatabaseConfig firstConfig = new DatabaseConfig();
            firstConfig.setAllowCreate(true);
            firstConfig.setSortedDuplicates(true);
            Database firstHandle = env.openDatabase(null, "fooDups",
                                                    firstConfig);
            /* 1b. Try to open w/no duplicates. */
            DatabaseConfig secondConfig = new DatabaseConfig();
            secondConfig.setSortedDuplicates(false);
            try {
                env.openDatabase(null, "fooDups", secondConfig);
                fail("Conflict in duplicates allowed should be detected.");
            } catch (IllegalArgumentException expected) {
            }

            firstHandle.close();
            env.removeDatabase(null, "fooDups");

            /* 2a. Create dis-allowing duplicates. */
            firstConfig.setSortedDuplicates(false);
            firstConfig.setKeyPrefixing(false);
            firstHandle = env.openDatabase(null, "fooDups", firstConfig);
            /* 2b. Try to open w/duplicates. */
            secondConfig.setSortedDuplicates(true);
            try {
                env.openDatabase(null, "fooDups", secondConfig);
                fail("Conflict in duplicates allowed should be detected.");
            } catch (IllegalArgumentException expected) {
            }
            firstHandle.close();

            /*
             * Test conflicts of read only. If the environment is read/write
             * we should be able to open handles in read only or read/write
             * mode. If the environment is readonly, the database handles
             * must also be read only.
             */
            DatabaseConfig readOnlyConfig = new DatabaseConfig();
            readOnlyConfig.setReadOnly(true);
            Database roHandle = env.openDatabase(null, "fooDups",
                                                 readOnlyConfig);
            roHandle.close();

            /* Open the environment in read only mode. */
            close(env);
            envConfig = TestUtils.initEnvConfig();
            envConfig.setReadOnly(true);
            env = create(envHome, envConfig);

            /* Open a readOnly database handle, should succeed */
            roHandle = env.openDatabase(null, "fooDups",
                                        readOnlyConfig);
            roHandle.close();

            /* Open a read/write database handle, should not succeed. */
            try {
                env.openDatabase(null, "fooDups", null);
                fail("Should not be able to open read/write");
            } catch (IllegalArgumentException expected) {
            }
            close(env);

            /*
             * Check comparator changes.
             */
            /* 1a. Open w/a null comparator */
            env = create(envHome, null);
            firstConfig = new DatabaseConfig();
            firstConfig.setAllowCreate(true);
            firstHandle = env.openDatabase(null,
                                           "fooComparator",
                                           firstConfig);
            DatabaseConfig firstRetrievedConfig = firstHandle.getConfig();
            assertEquals(null, firstRetrievedConfig.getBtreeComparator());
            assertEquals(null, firstRetrievedConfig.getDuplicateComparator());

            /*
             * 1b. Open a db w/a different comparator, shouldn't take effect
             * because override is not set.
             */
            secondConfig = new DatabaseConfig();
            Comparator btreeComparator = new TestComparator();
            Comparator dupComparator = new TestComparator();
            secondConfig.setBtreeComparator
                ((Class<Comparator<byte[]>>)btreeComparator.getClass());
            secondConfig.setDuplicateComparator
                ((Class<Comparator<byte[]>>)dupComparator.getClass());
            Database secondHandle =
                env.openDatabase(null, "fooComparator", secondConfig);
            DatabaseConfig retrievedConfig = secondHandle.getConfig();
            assertEquals(null, retrievedConfig.getBtreeComparator());
            assertEquals(null, retrievedConfig.getDuplicateComparator());
            secondHandle.close();

            /* Same as above but with a serialized comparator. */
            secondConfig = new DatabaseConfig();
            btreeComparator = new TestSerialComparator();
            dupComparator = new TestSerialComparator();
            secondConfig.setBtreeComparator(btreeComparator);
            secondConfig.setDuplicateComparator(dupComparator);
            secondHandle =
                env.openDatabase(null, "fooComparator", secondConfig);
            retrievedConfig = secondHandle.getConfig();
            assertEquals(null, retrievedConfig.getBtreeComparator());
            assertEquals(null, retrievedConfig.getDuplicateComparator());
            secondHandle.close();

            /* 
             * Test that update DatabaseConfig while there are open handles 
             * should throw exceptions. 
             */
            secondConfig.setOverrideBtreeComparator(true);
            secondConfig.setOverrideDuplicateComparator(true);
            btreeComparator = new TestComparator();
            dupComparator = new TestComparator();
            secondConfig.setBtreeComparator
                ((Class<Comparator<byte[]>>)btreeComparator.getClass());
            secondConfig.setDuplicateComparator
                ((Class<Comparator<byte[]>>)dupComparator.getClass());
            try {
                secondHandle = env.openDatabase(null,
                                                "fooComparator",
                                                 secondConfig);
                fail("Expect exceptions here");
            } catch (IllegalStateException e) {
                /* Expected exception. */
            } catch (Exception e) {
                fail("Unexpected exception: " + e.getMessage());
            }
            secondHandle.close();

            /* 
             * Open a new database handle without DatabaseConfig changes should 
             * be valid. 
             */
            try {
                secondHandle = env.openDatabase(null,
                                                "fooComparator",
                                                firstConfig);
            } catch (Exception e) {
                fail("Unexpected exception: " + e.getMessage());
            }

            secondHandle.close();
            firstHandle.close();
            close(env);
        } catch (Throwable t) {
            t.printStackTrace();
            close(env);
            throw t;
        }
    }

    @Test
    public void testIsTransactional()
        throws Throwable {

        try {
            /* Open environment in transactional mode.*/
            EnvironmentConfig envConfig = TestUtils.initEnvConfig();
            envConfig.setTransactional(true);
            envConfig.setAllowCreate(true);
            env = create(envHome, envConfig);

            /* Create a db, open transactionally with implied auto-commit. */
            DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setAllowCreate(true);
            dbConfig.setTransactional(true);
            Database myDb = env.openDatabase(null, "testDB", dbConfig);
            assertTrue(myDb.isTransactional());
            assertTrue(myDb.getConfig().getTransactional());
            myDb.close();

            /* Open an existing db, can open it non-transactionally. */
            dbConfig.setTransactional(false);
            myDb = env.openDatabase(null, "testDB", null);
            assertFalse(myDb.isTransactional());
            assertFalse(myDb.getConfig().getTransactional());
            myDb.close();

            /* Open another db, pass an explicit transaction. */
            dbConfig.setTransactional(true);
            Transaction txn = env.beginTransaction(null, null);
            myDb = env.openDatabase(txn, "testDB2", dbConfig);
            assertTrue(myDb.isTransactional());
            assertTrue(myDb.getConfig().getTransactional());

            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry data = new DatabaseEntry();
            key.setData(TestUtils.getTestArray(0));
            data.setData(TestUtils.getTestArray(0));
            try {
                myDb.put(null, key, data);
            } catch (DatabaseException DBE) {
                fail("didn't expect DatabaseException, implied autocommit");
            }

            key.setData(TestUtils.getTestArray(1));
            data.setData(TestUtils.getTestArray(1));
            try {
                myDb.put(txn, key, data);
            } catch (DatabaseException DBE) {
                fail("didn't expect DatabaseException with txn passed");
            }

            try {
                myDb.get(txn, key, data, LockMode.DEFAULT);
            } catch (DatabaseException DBE) {
                fail("didn't expect DatabaseException with txn passed");
            }

            txn.commit();

            try {
                myDb.get(null, key, data, LockMode.DEFAULT);
            } catch (DatabaseException DBE) {
                fail("didn't expect DatabaseException because no txn passed");
            }

            myDb.close();

            close(env);
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }

    @Test
    public void testOpenReadOnly()
        throws Throwable {

        try {
            EnvironmentConfig envConfig = TestUtils.initEnvConfig();
            envConfig.setTransactional(true);
            envConfig.setAllowCreate(true);
            env = create(envHome, envConfig);

            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry data = new DatabaseEntry();

            Transaction txn = env.beginTransaction(null, null);
            DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setTransactional(true);
            dbConfig.setAllowCreate(true);
            Database myDb = env.openDatabase(txn, "testDB2", dbConfig);

            key.setData(TestUtils.getTestArray(0));
            data.setData(TestUtils.getTestArray(0));
            try {
                myDb.put(txn, key, data);
            } catch (DatabaseException DBE) {
                fail("unexpected DatabaseException during put");
            }

            txn.commit();
            myDb.close();

            dbConfig = new DatabaseConfig();
            dbConfig.setTransactional(true);
            dbConfig.setReadOnly(true);
            txn = env.beginTransaction(null, null);
            myDb = env.openDatabase(txn, "testDB2", dbConfig);
            assertTrue(myDb.isTransactional());
            assertTrue(myDb.getConfig().getTransactional());

            key.setData(TestUtils.getTestArray(0));
            data.setData(TestUtils.getTestArray(0));
            try {
                myDb.put(txn, key, data);
                fail("expected UnsupportedOperationException " +
                     "because open RDONLY");
            } catch (UnsupportedOperationException expected) {
            }

            key.setData(TestUtils.getTestArray(0));
            data.setData(TestUtils.getTestArray(0));
            assertEquals(OperationStatus.SUCCESS,
                         myDb.get(txn, key, data, LockMode.DEFAULT));

            Cursor cursor = myDb.openCursor(txn, null);

            assertEquals(OperationStatus.SUCCESS,
                         cursor.getFirst(key, data, LockMode.DEFAULT));

            try {
                cursor.delete();
                fail("expected Exception from delete on RD_ONLY db");
            } catch (UnsupportedOperationException e) {
            }

            key.setData(TestUtils.getTestArray(1));
            data.setData(TestUtils.getTestArray(1));
            try {
                myDb.put(txn, key, data);
                fail
              ("expected UnsupportedOperationException because open RDONLY");
            } catch (UnsupportedOperationException expected) {
            }

            cursor.close();
            txn.commit();
            myDb.close();

            close(env);
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }

    /**
     * Test exclusive creation.
     */
    @Test
    public void testExclusive()
        throws Throwable {

        try {
            EnvironmentConfig envConfig = TestUtils.initEnvConfig();

            /*
             * Make sure that the database keeps its own copy of the
             * configuration object.
             */
            envConfig.setAllowCreate(true);
            env = create(envHome, envConfig);
            DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setAllowCreate(true);
            dbConfig.setExclusiveCreate(true);

            /* Should succeed and create the database. */
            Database dbA = env.openDatabase(null, "foo", dbConfig);
            dbA.close();

            /* Should not succeed, because the database exists. */
            try {
                env.openDatabase(null, "foo", dbConfig);
                fail("Database already exists");
            } catch (DatabaseException e) {
            }
            close(env);
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }

    /*
     * Test that changing the Btree comparator really writes it to disk.
     */
    @Test
    public void testConfigOverrideUpdateSR15743()
        throws Throwable {

        try {
            EnvironmentConfig envConfig = TestUtils.initEnvConfig();
            envConfig.setAllowCreate(true);
            env = create(envHome, envConfig);

            /*
             * Make sure that the database keeps its own copy of the
             * configuration object.
             */
            DatabaseConfig dbConfigA = new DatabaseConfig();
            dbConfigA.setOverrideBtreeComparator(false);
            dbConfigA.setBtreeComparator(TestComparator.class);
            dbConfigA.setAllowCreate(true);
            Database dbA = env.openDatabase(null, "foo", dbConfigA);

            /* Change the original dbConfig */
            dbConfigA.setBtreeComparator(TestComparator2.class);
            DatabaseConfig getConfig1 = dbA.getConfig();
            assertEquals(TestComparator.class,
                         getConfig1.getBtreeComparator().getClass());

            /*
             * Change the retrieved config, ought to have no effect on what the
             * Database is storing.
             */
            getConfig1.setBtreeComparator(TestComparator2.class);
            DatabaseConfig getConfig2 = dbA.getConfig();
            assertEquals(TestComparator.class,
                         getConfig2.getBtreeComparator().getClass());

            dbA.close();
            close(env);

            /* Ensure new comparator is written to disk. */
            envConfig = TestUtils.initEnvConfig();
            env = create(envHome, envConfig);

            dbConfigA = new DatabaseConfig();
            /* Change the comparator. */
            dbConfigA.setOverrideBtreeComparator(true);
            dbConfigA.setBtreeComparator(TestComparator2.class);
            dbA = env.openDatabase(null, "foo", dbConfigA);

            getConfig2 = dbA.getConfig();
            assertEquals(TestComparator2.class,
                         getConfig2.getBtreeComparator().getClass());

            dbA.close();
            close(env);

            /* Read it back during recovery to ensure it was written. */
            envConfig = TestUtils.initEnvConfig();
            env = create(envHome, envConfig);

            dbConfigA = new DatabaseConfig();
            dbA = env.openDatabase(null, "foo", dbConfigA);
            getConfig2 = dbA.getConfig();
            assertEquals(TestComparator2.class,
                         getConfig2.getBtreeComparator().getClass());

            /* Create a root for the tree. */
            dbA.put(null,
                    new DatabaseEntry(new byte[1]),
                    new DatabaseEntry(new byte[1]));

            dbA.close();
            close(env);

            /* Change it to a third one when there is a root present. */
            envConfig = TestUtils.initEnvConfig();
            env = create(envHome, envConfig);

            dbConfigA = new DatabaseConfig();
            /* Change the comparator. */
            dbConfigA.setOverrideBtreeComparator(true);
            dbConfigA.setBtreeComparator(TestComparator3.class);
            dbA = env.openDatabase(null, "foo", dbConfigA);
            getConfig2 = dbA.getConfig();
            assertEquals(TestComparator3.class,
                         getConfig2.getBtreeComparator().getClass());
            dbA.close();
            close(env);

            /* Read it back during recovery to ensure it was written. */
            envConfig = TestUtils.initEnvConfig();
            env = create(envHome, envConfig);

            dbConfigA = new DatabaseConfig();
            dbA = env.openDatabase(null, "foo", dbConfigA);
            getConfig2 = dbA.getConfig();
            assertEquals(TestComparator3.class,
                         getConfig2.getBtreeComparator().getClass());
            dbA.close();
            close(env);

        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }

    /* Test mutable and persistent configurations. */
    @Test
    public void testPersistentAndMutableConfigs()
        throws Exception {

        final String dbName = "foo";

        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setAllowCreate(true);
        envConfig.setTransactional(true);
        env = create(envHome, envConfig);

        DbConfigManager configMgr = 
            DbInternal.getNonNullEnvImpl(env).getConfigManager();
        int defaultNodeMaxEntries = 
            configMgr.getInt(EnvironmentParams.NODE_MAX);
        int defaultNodeDupTreeMaxEntries =
            configMgr.getInt(EnvironmentParams.NODE_MAX_DUPTREE);

        /* Check the default node max entries setting. */
        assertEquals(defaultNodeMaxEntries, 128);
        assertEquals(defaultNodeDupTreeMaxEntries, 128);

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setTransactional(true);

        /* Do updates on each persistent and mutable config. */

        /* Check whether BtreeComparator setting is persisted. */
        dbConfig.setOverrideBtreeComparator(true);
        dbConfig.setBtreeComparator(TestComparator.class);
        DatabaseConfig newConfig = setAndGetDbConfig(env, dbConfig, dbName);
        assertTrue(newConfig.getBtreeComparator() instanceof TestComparator);
        
        /* Check whether DuplicateComparator setting is persisted. */
        dbConfig.setOverrideDuplicateComparator(true);
        dbConfig.setDuplicateComparator(new TestSerialComparator());
        newConfig = setAndGetDbConfig(env, dbConfig, dbName);
        assertTrue(newConfig.getDuplicateComparator() instanceof 
                   TestSerialComparator);

        /* Check whether KeyPrefixing setting is persisted. */
        dbConfig.setKeyPrefixing(true);
        newConfig = setAndGetDbConfig(env, dbConfig, dbName);
        assertTrue(newConfig.getKeyPrefixing());

        /* Check whether NodeMaxEntries setting is persisted. */
        dbConfig.setNodeMaxEntries(512);
        newConfig = setAndGetDbConfig(env, dbConfig, dbName);
        assertTrue(newConfig.getNodeMaxEntries() == 512);

        close(env);
    }

    /* 
     * This method will:
     * 1. apply the modified DatabaseConfig to the database.
     * 2. close the database and do a sync to make sure the new configuration
     *    is written to the log.
     * 3. open the database with a useExisting config and return the current
     *    DatabaseConfig.
     */
    private DatabaseConfig setAndGetDbConfig(Environment env, 
                                             DatabaseConfig dbConfig,
                                             String dbName) 
        throws Exception {

        Database db = env.openDatabase(null, "foo", dbConfig);
        db.close();
        
        env.sync();

        /* 
         * Open with the useExisting config to see what attributes have been
         * persisted.
         */
        DatabaseConfig newConfig = new DatabaseConfig();
        newConfig.setReadOnly(true);
        newConfig.setTransactional(true);
        newConfig.setUseExistingConfig(true);

        db = env.openDatabase(null, dbName, newConfig);
        newConfig = db.getConfig();
        db.close();

        return newConfig;
    }

    /*
     * This Comparator can't be instantiated because it's private and not
     * static.
     */
    private class BadComparator1 implements Comparator<byte[]> {
        public BadComparator1(int foo) {
        }

        public int compare(byte[] o1, byte[] o2) {
            return 0;
        }
    }

    /*
     * This Comparator can't be instantiated because it doesn't have zero
     * parameter constructor.
     */
    public static class BadComparator2 implements Comparator<byte[]> {
        public BadComparator2(int i) {
        }

        public int compare(byte[] o1, byte[] o2) {
            return 0;
        }
    }

    /*
     * OK comparator for setting comparators.
     */
    public static class TestComparator implements Comparator<byte[]> {
        public TestComparator() {
        }

        public int compare(byte[] o1, byte[] o2) {
            return 0;
        }
    }

    /*
     * OK comparator for setting comparators.
     */
    public static class TestComparator2 implements Comparator<byte[]> {
        public TestComparator2() {
        }

        public int compare(byte[] o1, byte[] o2) {
            return 0;
        }
    }

    /*
     * OK comparator for setting comparators.
     */
    public static class TestComparator3 implements Comparator<byte[]> {
        public TestComparator3() {
        }

        public int compare(byte[] o1, byte[] o2) {
            return 0;
        }
    }

    /*
     * This Comparator can't be serialized because it's not serializable.
     */
    public class BadSerialComparator1 implements Comparator<byte[]> {

        public BadSerialComparator1() {
        }

        public int compare(byte[] o1, byte[] o2) {
            return 0;
        }
    }

    /*
     * This Comparator can't be serialized because it contains a reference to
     * an object that's not serializable.
     */
    @SuppressWarnings("serial")
    public class BadSerialComparator2 implements Comparator<byte[]>,
                                                 Serializable {

        private final BadSerialComparator1 o = new BadSerialComparator1();

        public BadSerialComparator2() {
        }

        public int compare(byte[] o1, byte[] o2) {
            return 0;
        }
    }

    /*
     * OK comparator for setting comparators -- private class, private
     * constructor, and serializable fields are allowed.
     */
    @SuppressWarnings("serial")
    private static class TestSerialComparator
        implements Comparator<byte[]>, Serializable {

        private final String s = "sss";

        private TestSerialComparator() {
        }

        public int compare(byte[] o1, byte[] o2) {
            return 0;
        }

        @Override
        public boolean equals(Object other) {
            TestSerialComparator o = (TestSerialComparator) other;
            return s.equals(o.s);
        }
    }

    /*
     * OK comparator for setting comparators.
     */
    @SuppressWarnings("serial")
    public static class TestSerialComparator2
        implements Comparator<byte[]>, Serializable {

        public int compare(byte[] o1, byte[] o2) {
            return 0;
        }
    }

    public static class SecKeyCreator1 implements SecondaryKeyCreator {
        public boolean createSecondaryKey(SecondaryDatabase secondary,
                                          DatabaseEntry key,
                                          DatabaseEntry data,
                                          DatabaseEntry result) {
            return true;
        }

        @Override
    public boolean equals(Object o) {
            if (o == null) {
                return false;
            }
            return (o.getClass() == getClass());
        }
    }

    public static class SecKeyCreator2 implements SecondaryKeyCreator {
        public boolean createSecondaryKey(SecondaryDatabase secondary,
                                          DatabaseEntry key,
                                          DatabaseEntry data,
                                          DatabaseEntry result) {
            return true;
        }

        @Override
    public boolean equals(Object o) {
            if (o == null) {
                return false;
            }
            return (o.getClass() == getClass());
        }
    }

    public static class SecMultiKeyCreator1
        implements SecondaryMultiKeyCreator {
        public void createSecondaryKeys(SecondaryDatabase secondary,
                                        DatabaseEntry key,
                                        DatabaseEntry data,
                                        Set results) {
        }

        @Override
    public boolean equals(Object o) {
            if (o == null) {
                return false;
            }
            return (o.getClass() == getClass());
        }
    }

    public static class ForeignKeyNullifier1 implements ForeignKeyNullifier {
        public boolean nullifyForeignKey(SecondaryDatabase secondary,
                                         DatabaseEntry data) {
            return true;
        }
    }

    public static class ForeignMultiKeyNullifier1
        implements ForeignMultiKeyNullifier {
        public boolean nullifyForeignKey(SecondaryDatabase secondary,
                                         DatabaseEntry key,
                                         DatabaseEntry data,
                                         DatabaseEntry secKey) {
            return true;
        }
    }
}
