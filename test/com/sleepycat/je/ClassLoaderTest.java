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
import static org.junit.Assert.assertSame;

import java.io.File;
import java.util.Comparator;

import org.junit.Before;
import org.junit.Test;

import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.je.util.DualTestCase;
import com.sleepycat.je.util.SimpleClassLoader;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.StoreConfig;
import com.sleepycat.util.test.SharedTestUtils;

/**
 * Tests the Environment ClassLoader property.
 */
public class ClassLoaderTest extends DualTestCase {

    public interface LoadedClass {

        Comparator<byte[]> getReverseComparator();

        Class<? extends Comparator<byte[]>> getReverseComparatorClass();

        Object getSerializableInstance();

        void writeEntities(EntityStore store);

        void readEntities(EntityStore store);
    }

    private static final String LOADED_CLASS_IMPL =
        "com.sleepycat.je.LoadedClassImpl";

    private final File envHome;
    private Environment env;
    private ClassLoader myLoader;
    private LoadedClass loadedClass;

    public ClassLoaderTest() {
        envHome = SharedTestUtils.getTestDir();
    }

    @Before
    public void setUp()
        throws Exception {

        super.setUp();

        final File classDir = new File(System.getProperty("testclassloader"));
        final ClassLoader parentClassLoader =
            Thread.currentThread().getContextClassLoader();
        myLoader = new SimpleClassLoader(parentClassLoader, classDir);

        final Class cls =
            Class.forName(LOADED_CLASS_IMPL, true /*initialize*/, myLoader);
        loadedClass = (LoadedClass) cls.newInstance();
    }

    abstract class WithThreadLoader {

        void exec() {
            final ClassLoader save =
                Thread.currentThread().getContextClassLoader();
            Thread.currentThread().setContextClassLoader(myLoader);
            try {
                run();
            } finally {
                Thread.currentThread().setContextClassLoader(save);
            }
        }

        abstract void run();
    }

    private void openEnv(boolean configLoader) {
        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setAllowCreate(true);
        envConfig.setTransactional(true);
        envConfig.setClassLoader(configLoader ? myLoader : null);
        env = create(envHome, envConfig);
    }

    private void closeEnv() {
        close(env);
        env = null;
    }

    @Test
    public void testDbComparatorWithThreadLoader()
        throws Exception {

        new WithThreadLoader() {
            void run() {
                checkDbComparator(false /*configLoader*/);
            }
        }.exec();
    }

    @Test
    public void testDbComparatorWithEnvLoader()
        throws Exception {

        checkDbComparator(true /*configLoader*/);
    }

    private void checkDbComparator(boolean configLoader) {
        
        /* Create new env and dbs. */

        /* First DB using comparator stored by class name. */
        openEnv(configLoader);
        Database db = openDbWithComparator(true /*cmpByClass*/);
        insertReverseOrder(db);
        readReverseOrder(db);
        db.close();

        /* Second DB using comparator stored as an instance. */
        db = openDbWithComparator(false /*cmpByClass*/);
        insertReverseOrder(db);
        readReverseOrder(db);
        db.close();
        closeEnv();

        /* Open existing env and dbs. */

        /* First DB using comparator stored by class name. */
        openEnv(configLoader);
        db = openDbWithComparator(true /*cmpByClass*/);
        readReverseOrder(db);
        db.close();

        /* Second DB using comparator stored as an instance. */
        db = openDbWithComparator(false /*cmpByClass*/);
        readReverseOrder(db);
        db.close();
        closeEnv();
    }

    private Database openDbWithComparator(boolean cmpByClass) {

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setTransactional(true);
        dbConfig.setSortedDuplicates(true);
        if (cmpByClass) {
            dbConfig.setBtreeComparator
                (loadedClass.getReverseComparatorClass());
            dbConfig.setDuplicateComparator
                (loadedClass.getReverseComparatorClass());
        } else {
            dbConfig.setBtreeComparator
                (loadedClass.getReverseComparator());
            dbConfig.setDuplicateComparator
                (loadedClass.getReverseComparator());
        }
        final String name = "testDB" +
            (cmpByClass ? "CmpByClass" : "CmpByInstance");
        return env.openDatabase(null, name, dbConfig);
    }

    private void insertReverseOrder(Database db) {

        final DatabaseEntry key = new DatabaseEntry();
        final DatabaseEntry data = new DatabaseEntry();

        /* Insert non-dups. */
        IntegerBinding.intToEntry(1, data);
        for (int i = 1; i <= 5; i += 1) {
            IntegerBinding.intToEntry(i, key);
            assertSame(OperationStatus.SUCCESS,
                       db.putNoOverwrite(null, key, data));
        }

        /* Insert dups. */
        IntegerBinding.intToEntry(5, key);
        for (int i = 2; i <= 5; i += 1) {
            IntegerBinding.intToEntry(i, data);
            assertSame(OperationStatus.SUCCESS,
                       db.putNoDupData(null, key, data));
        }
    }

    private void readReverseOrder(Database db) {

        final DatabaseEntry key = new DatabaseEntry();
        final DatabaseEntry data = new DatabaseEntry();

        /* Read non-dups. */
        final Cursor c = db.openCursor(null, null);
        for (int i = 5; i >= 1; i -= 1) {
            assertSame(OperationStatus.SUCCESS,
                       c.getNextNoDup(key, data, LockMode.DEFAULT));
            assertEquals(i, IntegerBinding.entryToInt(key));
        }
        assertSame(OperationStatus.NOTFOUND,
                   c.getNextNoDup(key, data, LockMode.DEFAULT));

        /* Read dups. */
        assertSame(OperationStatus.SUCCESS,
                   c.getFirst(key, data, LockMode.DEFAULT));
        assertEquals(5, IntegerBinding.entryToInt(key));
        assertEquals(5, IntegerBinding.entryToInt(data));
        for (int i = 4; i >= 1; i -= 1) {
            assertSame(OperationStatus.SUCCESS,
                       c.getNext(key, data, LockMode.DEFAULT));
            assertEquals(5, IntegerBinding.entryToInt(key));
            assertEquals(i, IntegerBinding.entryToInt(data));
        }

        c.close();
    }

    @Test
    public void testSerialBindingWithThreadLoader()
        throws Exception {

        new WithThreadLoader() {
            void run() {
                checkSerialBinding(false /*configLoader*/);
            }
        }.exec();
    }

    @Test
    public void testSerialBindingWithEnvLoader()
        throws Exception {

        checkSerialBinding(true /*configLoader*/);
    }

    private void checkSerialBinding(boolean configLoader) {
        
        openEnv(configLoader);

        final DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setTransactional(true);
        final Database db = env.openDatabase(null, "catalog", dbConfig);

        final StoredClassCatalog catalog = new StoredClassCatalog(db);
        final SerialBinding binding = new SerialBinding(catalog, null);

        final Object o = loadedClass.getSerializableInstance();
        final DatabaseEntry entry = new DatabaseEntry();

        binding.objectToEntry(o, entry);
        final Object o2 = binding.entryToObject(entry);
        assertEquals(o, o2);

        db.close();
        closeEnv();
    }

    @Test
    public void testDPLWithThreadLoader()
        throws Exception {

        new WithThreadLoader() {
            void run() {
                checkDPL(false /*configLoader*/);
            }
        }.exec();
    }

    @Test
    public void testDPLWithEnvLoader()
        throws Exception {

        checkDPL(true /*configLoader*/);
    }

    private void checkDPL(boolean configLoader) {
        
        openEnv(configLoader);
        final StoreConfig config = new StoreConfig();
        config.setAllowCreate(true);
        config.setTransactional(true);
        EntityStore store = new EntityStore(env, "foo", config);
        loadedClass.writeEntities(store);
        store.close();
        closeEnv();
        
        openEnv(configLoader);
        store = new EntityStore(env, "foo", config);
        loadedClass.readEntities(store);
        store.close();
        closeEnv();
    }
}
