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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.EnumSet;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.sleepycat.je.DatabaseException;
/* <!-- begin JE only --> */
import com.sleepycat.je.DbInternal;
/* <!-- end JE only --> */
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.Transaction;
/* <!-- begin JE only --> */
import com.sleepycat.je.dbi.EnvironmentImpl;
/* <!-- end JE only --> */
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.persist.EntityCursor;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.SecondaryIndex;
import com.sleepycat.persist.StoreConfig;
import com.sleepycat.persist.impl.ComplexFormat;
import com.sleepycat.persist.impl.Store;
import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.KeyField;
import com.sleepycat.persist.model.Persistent;
import com.sleepycat.persist.model.PrimaryKey;
import com.sleepycat.persist.model.SecondaryKey;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;
import com.sleepycat.util.test.TestEnv;

/**
 * In the versions before je4.1, we failed to set the dup comparator.  This 
 * bug applies only when the primary key has a comparator.  The bug was fixed 
 * by setting the dup comparator to the primary key comparator, for all new 
 * secondary databases.  [#17252]
 */
public class SecondaryDupOrderTest extends TestBase {
    private static final String STORE_NAME = "test";

    private File envHome;
    private Environment env;
    private EntityStore store;
    private PrimaryIndex<StoredComparatorEntity.Key,
                         StoredComparatorEntity> priIndex;
    private SecondaryIndex<StoredComparatorEntity.MyEnum,
                           StoredComparatorEntity.Key,
                           StoredComparatorEntity> secIndex;
    private SecondaryIndex<Integer,
                           StoredComparatorEntity.Key,
                           StoredComparatorEntity> secIndex2;
    
    private final static String secDBName = 
        "persist#test#com.sleepycat.persist.test." +
        "SecondaryDupOrderTest$StoredComparatorEntity#secKey";
    private final static String secDBName2 = 
        "persist#test#com.sleepycat.persist.test." +
        "SecondaryDupOrderTest$StoredComparatorEntity#secKey2";
    
    private final static StoredComparatorEntity.Key[] priKeys =
        new StoredComparatorEntity.Key[] {
            new StoredComparatorEntity.Key
                (StoredComparatorEntity.MyEnum.A, 1,
                 StoredComparatorEntity.MyEnum.A),
            new StoredComparatorEntity.Key
                (StoredComparatorEntity.MyEnum.A, 1,
                 StoredComparatorEntity.MyEnum.B),
            new StoredComparatorEntity.Key
                (StoredComparatorEntity.MyEnum.A, 2,
                 StoredComparatorEntity.MyEnum.A),
            new StoredComparatorEntity.Key
                (StoredComparatorEntity.MyEnum.A, 2,
                 StoredComparatorEntity.MyEnum.B),
            new StoredComparatorEntity.Key
                (StoredComparatorEntity.MyEnum.B, 1,
                 StoredComparatorEntity.MyEnum.A),
            new StoredComparatorEntity.Key
                (StoredComparatorEntity.MyEnum.B, 1,
                 StoredComparatorEntity.MyEnum.B),
            new StoredComparatorEntity.Key
                (StoredComparatorEntity.MyEnum.C, 0,
                 StoredComparatorEntity.MyEnum.C),
        };

    private final static StoredComparatorEntity.MyEnum[] secKeys =
        new StoredComparatorEntity.MyEnum[] {
            StoredComparatorEntity.MyEnum.C,
            StoredComparatorEntity.MyEnum.B,
            StoredComparatorEntity.MyEnum.A,
            null,
            StoredComparatorEntity.MyEnum.A,
            StoredComparatorEntity.MyEnum.B,
            StoredComparatorEntity.MyEnum.C,
        };
    
    final Integer[] secKeys2 = new Integer[] { 2, 1, 0, null, 0, 1, 2, };

    @Before
    public void setUp() 
        throws Exception {
        
        envHome = SharedTestUtils.getTestDir();
        super.setUp();
    }

    @After
    public void tearDown() {
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
    }
    
    private void open()
        throws DatabaseException {
    
        EnvironmentConfig envConfig = TestEnv.BDB.getConfig();
        envConfig.setAllowCreate(true);
        envConfig.setTransactional(true);
        envConfig.setConfigParam(EnvironmentConfig.VERIFY_BTREE, "false");
        env = new Environment(envHome, envConfig);

        /* <!-- begin JE only --> */
        EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);
        TestUtils.validateNodeMemUsage(envImpl, true /*assertOnError*/);
        /* <!-- end JE only --> */
    
        StoreConfig storeConfig = new StoreConfig();
        storeConfig.setAllowCreate(true);
        storeConfig.setTransactional(true);
        store = new EntityStore(env, STORE_NAME, storeConfig);
        
        priIndex = store.getPrimaryIndex(StoredComparatorEntity.Key.class,
                                         StoredComparatorEntity.class);
        secIndex = store.getSecondaryIndex(priIndex, 
                                           StoredComparatorEntity.MyEnum.class, 
                                           "secKey");
        secIndex2 = 
            store.getSecondaryIndex(priIndex, Integer.class, "secKey2");
    }
    
    /* Open the old database and delete the sec database. */
    private void openAndDeleteSecDatabase(String secDBName)
        throws DatabaseException {
    
        EnvironmentConfig envConfig = TestEnv.BDB.getConfig();
        envConfig.setAllowCreate(true);
        envConfig.setTransactional(true);
        env = new Environment(envHome, envConfig);
        Transaction txn = env.beginTransaction(null, null);
        env.removeDatabase(txn, secDBName);
        txn.commit();
        close();
    }
    
    private void close()
        throws DatabaseException {
    
        if (store != null) {
            store.close();
            store = null;
        }
        if (env != null) {
            env.close();
            env = null;
        }
    }
    
    /*
     * [17252] Wrong order for DPL secondary index duplicates.
     * 
     * When the user define a comparator for the primary key order, the output 
     * of the secondary duplicates are in the user-define order, rather than in 
     * the natural order.
     */
    @Test
    public void testStoredComparators()
        throws DatabaseException {
        
        open();
        final int nEntities = priKeys.length;
        Transaction txn = env.beginTransaction(null, null);
        for (int i = 0; i < nEntities; i += 1) {
            priIndex.put(txn, new StoredComparatorEntity
                         (priKeys[i], secKeys[i], secKeys2[i]));
        }
        txn.commit();
        close();
        
        open();
    
        /* Check the primary database order. */
        checkPrimaryDBOrder();
        
        /* 
         * Check all of the secondary databases order. 
         * 
         * Sec DB1 and Sec DB2 :
         * The Secondary duplicates order should also be reverse order, 
         * which is the same as primary database.
         */
        checkAllSecondaryDBOrder(true, true);
        close();
    }
    
    
    /*
     * In the versions before je4.1, we failed to set the dup comparator.  This 
     * bug applies only when the primary key has a comparator.  The bug was 
     * fixed by setting the dup comparator to the primary key comparator, for 
     * all new secondary databases.  [#17252]
     * 
     * When reading old database stored by earlier je version, the dup 
     * comparator will not be set to the primary key comparator. When the user 
     * wants to correct the ordering for an incorrectly ordered secondary 
     * database, he must delete the database but does not need to increment 
     * the class version.
     */
    @Test
    public void testReadOldDatabaseWithoutComparator() 
        throws IOException {
        
        /* Copy log file resource to log file zero. */
        TestUtils.loadLog
            (getClass(), "SecDupsWithoutComparator_je_4_0.jdb", envHome);

        open();
        
        /* Check primary database order. */
        checkPrimaryDBOrder();
        
        /* 
         * Check all of the secondary databases order. 
         * 
         * Sec DB1 and Sec DB2 :
         * Because the dup comparator will not set to primary key comparator in 
         * the old secondary databases, the secondary duplicates order should 
         * be nature order.
         */
        checkAllSecondaryDBOrder(false, false);
        close();
    }
    
    /*
     * When the user wants to correct the ordering for an incorrectly ordered
     * secondary database created by older je, he must delete the secondary 
     * database but does not need to increment the class version, then 
     * re-create the database with je4.1.
     */
    @Test
    public void testCorrectOldDatabaseOrder() 
        throws IOException {
        
        /* Copy log file resource to log file zero. */
        TestUtils.loadLog
            (getClass(), "SecDupsWithoutComparator_je_4_0.jdb", envHome);
        String clsName = StoredComparatorEntity.class.getName();
        
        /* Delete the seckey secondary database. */
        openAndDeleteSecDatabase(secDBName);
        
        /* 
         * Re-open the database will create a new sec database for seckey with 
         * correct dup Order. catalog.flush will be called to update the 
         * ComplexFormat.
         */
        Store.expectFlush = true;
        try {
            open();
            
            /* 
                 * incorrectlyOrderedSecKeys is null for the database created by
             * old version je.
             */
            ComplexFormat format = 
                (ComplexFormat) store.getModel().getRawType(clsName);  
            /* 
             * incorrectlyOrderedSecKeys is initialized and added the name of 
             * secKey2, which means secKey2 secondary database need to be 
             * assigned duplicate comparator.
             */
            assertEquals(format.getIncorrectlyOrderedSecKeys().size(), 1);
            assertTrue
                (format.getIncorrectlyOrderedSecKeys().contains("secKey2"));
        } finally {
            Store.expectFlush = false;
        }

        /* Check primary database order. */
        checkPrimaryDBOrder();

        /* 
         * Check all of the secondary databases order. 
         * 
         * Sec DB1:
         * This secondary database is first deleted, and then re-created by new
         * je. The dup comparator will be set to primary key comparator, so the 
         * Secondary duplicates order should be reverse order.
         * 
         * Sec DB2 :
         * Because the dup comparator will not set to primary key comparator in 
         * the old secondary databases, the secondary duplicates order should 
         * be nature order.
         */
        checkAllSecondaryDBOrder(true, false);   
        close();

        /* Delete the seckey2 secondary database. */
        openAndDeleteSecDatabase(secDBName2);
        
        /* 
         * Re-open the database will create a new sec database for seckey2 with 
         * correct dup Order. catalog.flush will be called to update the 
         * ComplexFormat.
         */
        Store.expectFlush = true;
        try {
            open();
            
            /* 
             * incorrectlyOrderedSecKeys is empty after re-open the secKey2
             * secondary database.
             */
            ComplexFormat format = 
                (ComplexFormat) store.getModel().getRawType(clsName);  
            assertEquals(format.getIncorrectlyOrderedSecKeys().size(), 0);
        } finally {
            Store.expectFlush = false;
        }
        
        /* 
         * Check all of the secondary databases order. 
         * 
         * Sec DB1:
         * This secondary database is created by new je, so the Secondary 
         * duplicates order should be reverse order.
         * 
         * Sec DB2 :
         * This secondary database is first deleted, and then re-created by new
         * je. The dup comparator will be set to primary key comparator, so the 
         * Secondary duplicates order should be reverse order.
         */
        checkAllSecondaryDBOrder(true, true);  
        close();
    }
    
    private void checkPrimaryDBOrder() {
        Transaction txn = env.beginTransaction(null, null);
        EntityCursor<StoredComparatorEntity> entities =
            priIndex.entities(txn, null);
        final int nEntities = priKeys.length;
        for (int i = nEntities - 1; i >= 0; i -= 1) {
            StoredComparatorEntity e = entities.next();
            assertNotNull(e);
            assertEquals(priKeys[i], e.key);
            assertEquals(secKeys[i], e.secKey);
        }
        assertNull(entities.next());
        entities.close();
        txn.commit();
    }
    
    private void checkAllSecondaryDBOrder(boolean ifReverse1, 
                                          boolean ifReverse2) {
        final int nEntities = priKeys.length;
        int[] order1 = new int[nEntities];
        int[] order2 = new int[nEntities];
        if (ifReverse1) {
            /* The reverse order. */
            for (int k = 0, i = nEntities - 1; i >= 0; i -= 1, k += 1) {
                order1[k] = i;
            }
        } else {
            /* The nature order. */
            for (int k = 0, i = 0; i < nEntities; i += 1, k += 1) {
                order1[k] = i;
            }
        }
        if (ifReverse2) {
            /* The reverse order. */
            for (int k = 0, i = nEntities - 1; i >= 0; i -= 1, k += 1) {
                order2[k] = i;
            }
        } else {
            /* The nature order. */
            for (int k = 0, i = 0; i < nEntities; i += 1, k += 1) {
                order2[k] = i;
            }
        }
        
        Transaction txn = env.beginTransaction(null, null);
        EntityCursor<StoredComparatorEntity> entities = 
            secIndex.entities(txn, null);
        for (StoredComparatorEntity.MyEnum myEnum :
             EnumSet.allOf(StoredComparatorEntity.MyEnum.class)) {
            for (int i : order1) {
                if (secKeys[i] == myEnum) {
                    StoredComparatorEntity e = entities.next();
                    assertNotNull(e);
                    assertEquals(priKeys[i], e.key);
                    assertEquals(secKeys[i], e.secKey);
                }
            }
        }
        assertNull(entities.next());
        entities.close();
        txn.commit();
        
        txn = env.beginTransaction(null, null);
        entities = secIndex2.entities(txn, null);
        for (int secKey = 0; secKey < 3; secKey++) {
            for (int i : order2) {
                if (secKeys2[i] != null && secKeys2[i] == secKey) {
                    StoredComparatorEntity e = entities.next();
                    assertNotNull(e);
                    assertEquals(priKeys[i], e.key);
                    assertEquals(secKeys2[i], e.secKey2);
                }
            }
        }
        assertNull(entities.next());
        entities.close();
        txn.commit(); 
    }
    
    @Entity
    static class StoredComparatorEntity {

        enum MyEnum { A, B, C };

        @Persistent
        static class Key implements Comparable<Key> {

            @KeyField(1)
            MyEnum f1;

            @KeyField(2)
            Integer f2;

            @KeyField(3)
            MyEnum f3;

            private Key() {}

            Key(MyEnum f1, Integer f2, MyEnum f3) {
                this.f1 = f1;
                this.f2 = f2;
                this.f3 = f3;
            }

            public int compareTo(Key o) {
                /* Reverse the natural order. */
                int i = f1.compareTo(o.f1);
                if (i != 0) return -i;
                i = f2.compareTo(o.f2);
                if (i != 0) return -i;
                i = f3.compareTo(o.f3);
                if (i != 0) return -i;
                return 0;
            }

            @Override
            public boolean equals(Object other) {
                if (!(other instanceof Key)) {
                    return false;
                }
                Key o = (Key) other;
                return f1 == o.f1 &&
                       f2.equals(o.f2) &&
                       f3 == o.f3;
            }

            @Override
            public int hashCode() {
                return f1.ordinal() + f2 + f3.ordinal();
            }

            @Override
            public String toString() {
                return "[Key " + f1 + ' ' + f2 + ' ' + f3 + ']';
            }
        }

        @PrimaryKey
        Key key;

        @SecondaryKey(relate=MANY_TO_ONE)
        private MyEnum secKey;

        @SecondaryKey(relate=MANY_TO_ONE)
        private Integer secKey2;

        private StoredComparatorEntity() {}

        StoredComparatorEntity(Key key, MyEnum secKey, Integer secKey2) {
            this.key = key;
            this.secKey = secKey;
            this.secKey2 = secKey2;
        }

        @Override
        public String toString() {
            return "[pri = " + key + " sec = " + secKey + " sec2 = " + 
                   secKey2 + ']';
        }
    }
}
