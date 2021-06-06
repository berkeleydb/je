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

package com.sleepycat.persist.test;

import static com.sleepycat.persist.model.Relationship.MANY_TO_ONE;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.sleepycat.compat.DbCompat;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.StoreConfig;
import com.sleepycat.persist.impl.PersistCatalog;
import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.KeyField;
import com.sleepycat.persist.model.Persistent;
import com.sleepycat.persist.model.PrimaryKey;
import com.sleepycat.persist.model.SecondaryKey;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;
import com.sleepycat.util.test.TestEnv;

/**
 * String was treated as an object in pre-5.0 JE version. But String will be 
 * treated as primitive type in JE 5.0. Therefore we need to test if the String 
 * data stored using older je version can be correctly read and updated in JE 
 * 5.0. [#19247]
 * 
 * The old database is created using je-4.0.103.
 * 
 * This test should be excluded from the BDB build because it uses a stored JE
 * log file.
 */
public class StringFormatCompatibilityTest extends TestBase {

    private static final String STORE_NAME = "test";
    private static final String STORE_PREFIX = "persist#foo#";

    private File envHome;
    private Environment env;
    private EntityStore store;
    private PersistCatalog catalog;

    @Before
    public void setUp() 
        throws Exception {
        
        envHome = SharedTestUtils.getTestDir();
        super.setUp();
    }

    @After
    public void tearDown() {
        if (catalog != null) {
            try {
                catalog.close();
            } catch (DatabaseException e) {
                System.out.println("During tearDown: " + e);
            }
        }
        if (store != null) {
            try {
                store.close();
            } catch (DatabaseException e) {
                System.out.println("During tearDown: " + e);
            }
        }
        if (env != null) {
            try {
                env.close();
            } catch (DatabaseException e) {
                System.out.println("During tearDown: " + e);
            }
        }
        envHome = null;
        env = null;
        store = null;
        catalog = null;
    }

    private void open()
        throws DatabaseException {

        EnvironmentConfig envConfig = TestEnv.BDB.getConfig();
        envConfig.setAllowCreate(true);
        env = new Environment(envHome, envConfig);

        StoreConfig storeConfig = new StoreConfig();
        storeConfig.setAllowCreate(true);
        store = new EntityStore(env, STORE_NAME, storeConfig);
        openCatalog();
    }
    
    private void openCatalog()
        throws DatabaseException {
    
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        DbCompat.setTypeBtree(dbConfig);
        catalog = new PersistCatalog
            (env, STORE_PREFIX, STORE_PREFIX + "catalog", dbConfig, null,
             null, false /*rawAccess*/, null /*Store*/);
    }

    private void close()
        throws DatabaseException {

        if (catalog != null) {
            catalog.close();
            catalog = null;
        }
        if (store != null) {
            store.close();
            store = null;
        }
        if (env != null) {
            env.close();
            env = null;
        }
    }

    @Test
    public void testReadOldStringData()
        throws IOException {

        /* Copy log file resource to log file zero. */
        TestUtils.loadLog(getClass(), "je-4.0.103_StringData.jdb", envHome);

        open();
        PrimaryIndex<String, StringData> primary = 
            store.getPrimaryIndex(String.class, StringData.class);
        
        /* Read the older String data. */
        StringData entity = primary.get("pk1");
        assertNotNull(entity);
        CompositeKey compK = new CompositeKey("CompKey1_1", "CompKey1_2");
        String[] f3 = {"f3_1", "f3_2"};
        List<String> f4 = new ArrayList<String>();
        f4.add("f4_1");
        f4.add("f4_2");
        entity.validate
            (new StringData ("pk1", "sk1", compK, "f1", "f2", f3, f4));    
        close();
    }
    
    @Test
    public void testWriteReadOldStringData() 
        throws IOException {
        
        /* Copy log file resource to log file zero. */
        TestUtils.loadLog(getClass(), "je-4.0.103_StringData.jdb", envHome);

        open();

        PrimaryIndex<String, StringData> primary = 
            store.getPrimaryIndex(String.class, StringData.class);
        CompositeKey compK = 
            new CompositeKey("new_CompKey2_1", "new_CompKey2_2");
        String[] f3 = {"new_f3_1", "new_f3_2"};
        List<String> f4 = new ArrayList<String>();
        f4.add("new_f4_1");
        f4.add("new_f4_2");
        
        /* Put the String data in a new foramt.*/
        primary.put(null, new StringData("pk2", "new_sk2", compK, "new_f1", 
                                         "new_f2", f3, f4));
        
        /* Read the String data using new format. */
        StringData entity = primary.get("pk2");
        assertNotNull(entity);
        entity.validate(new StringData("pk2", "new_sk2", compK, "new_f1", 
                                       "new_f2", f3, f4));
        
        /* Read the old String data. */
        entity = primary.get("pk1");
        assertNotNull(entity);
        compK = new CompositeKey("CompKey1_1", "CompKey1_2");
        f3 = new String[]{"f3_1", "f3_2"};
        f4 = new ArrayList<String>();
        f4.add("f4_1");
        f4.add("f4_2");
        entity.validate
            (new StringData ("pk1", "sk1", compK, "f1", "f2", f3, f4));
        close();
    }
    
    @Entity (version = 1)
    static class StringData {
        @PrimaryKey
        private String pk;
        @SecondaryKey (relate = MANY_TO_ONE)
        private String sk1;
        @SecondaryKey (relate = MANY_TO_ONE)
        private CompositeKey sk2;
        private String f1;
        
        /* 
         * This filed is changed to an Object, which will be converted 
         * automatically by class widening evolution.
         */
        private Object f2;
        private String[] f3;
        private List<String> f4;
        
        StringData() { }
        
        StringData(String pk, 
                   String sk1, 
                   CompositeKey sk2, 
                   String f1, 
                   String f2, 
                   String[] f3, 
                   List<String> f4) {
            this.pk = pk;
            this.sk1 = sk1;
            this.sk2 = sk2;
            this.f1 = f1;
            this.f2 = f2;
            this.f3 = f3;
            this.f4 = f4;
        }
        
        String getPriKeyObject() {
            return pk;
        }
        
        String getSK1() {
            return sk1;
        }
        
        CompositeKey getSK2() {
            return sk2;
        }
        
        String getF1() {
            return f1;
        }
        
        String getF2() {
            return (String)f2;
        }
        
        String[] getF3() {
            return f3;
        }
        
        List<String> getF4() {
            return f4;
        }
        
        public void validate(Object other) {
            StringData o = (StringData) other;
            TestCase.assertEquals(pk, o.pk);
            TestCase.assertEquals(sk1, o.sk1);
            sk2.validate(o.sk2);
            TestCase.assertEquals(f1, o.f1);
            TestCase.assertEquals(f2, o.f2);
            for (int i = 0; i < f3.length; i++) {
                TestCase.assertEquals(f3[i], o.f3[i]);
            }
            TestCase.assertEquals(f4, o.f4);
        }
    }
    
    @Persistent
    static class CompositeKey {
        @KeyField(2)
        private String f1;
        @KeyField(1)
        private String f2;

        private CompositeKey() {}

        CompositeKey(String f1, String f2) {
            this.f1 = f1;
            this.f2 = f2;
        }

        void validate(CompositeKey o) {
            TestCase.assertEquals(f1, o.f1);
            TestCase.assertEquals(f2, o.f2);
        }
    }
}
