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
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.persist.EntityCursor;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.SecondaryIndex;
import com.sleepycat.persist.StoreConfig;
import com.sleepycat.persist.evolve.Deleter;
import com.sleepycat.persist.evolve.Mutations;
import com.sleepycat.persist.evolve.Renamer;
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
@RunWith(Parameterized.class)
public class SecondaryDupOrderEvolveTest extends TestBase {
    private static final String STORE_NAME = "test";
    private File envHome;
    private Environment env;
    private EntityStore store;
    private enum MyEnum { A, B, C };

    private static String packageName =
        "com.sleepycat.persist.test.SecondaryDupOrderEvolveTest$";
    private final String originalClsName;
    private final String evolvedClsName;
    private final String caseLabel;
    private Class caseCls;
    private StoredComparatorEntity_Base caseObj;
    private static final String secDBName =
        "persist#test#com.sleepycat.persist.test." +
        "SecondaryDupOrderEvolveTest$StoredComparatorEntity#secKey";
    private static final String secDBName2 =
        "persist#test#com.sleepycat.persist.test." +
        "SecondaryDupOrderEvolveTest$StoredComparatorEntity#new_secKey2";

    private final static Key[] priKeys =
        new Key[] { new Key(MyEnum.A, 1, MyEnum.A),
                    new Key(MyEnum.A, 1, MyEnum.B),
                    new Key(MyEnum.A, 2, MyEnum.A),
                    new Key(MyEnum.A, 2, MyEnum.B),
                    new Key(MyEnum.B, 1, MyEnum.A),
                    new Key(MyEnum.B, 1, MyEnum.B),
                    new Key(MyEnum.C, 0, MyEnum.C),
                  };

    private final static MyEnum[]
        secKeys = new MyEnum[] { MyEnum.C, MyEnum.B, MyEnum.A,
                                 null,
                                 MyEnum.A, MyEnum.B, MyEnum.C,
                               };

    private final static Integer[] secKeys2 =
        new Integer[] { 2, 1, 0, null, 0, 1, 2, };

    private static final String[] EvolveCase = {
        "StoredComparatorEntity_RenameSecField",
        "StoredComparatorEntity_DeleteSecAnnotation",
        "StoredComparatorEntity_DeleteSecField",
        };
  
    @Parameters
    public static List<Object[]> genParams() {
       List<Object[]> list = new ArrayList<Object[]>();
       for (String evolvedClsName : EvolveCase)
           list.add(new Object[]{"StoredComparatorEntity", evolvedClsName});

       return list;
    }

    public SecondaryDupOrderEvolveTest(String originalClsName,
                                       String evolvedClsName) {
        this.originalClsName = packageName + originalClsName;
        this.evolvedClsName =packageName + evolvedClsName;
        this.caseLabel = evolvedClsName;
        customName = "-" + caseLabel;
    }

    @Before
    public void setUp()
        throws Exception {
        
        super.setUp();
        envHome = SharedTestUtils.getTestDir();
        TestUtils.removeLogFiles("Setup", envHome, false);
        /* Copy log file resource to log file zero. */
        TestUtils.loadLog
            (getClass(), "SecDupsWithoutComparatorEvolve_je_4_0.jdb", envHome);
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
        try {
            TestUtils.removeLogFiles("TearDown", envHome, false);
        } catch (Error e) {
            System.out.println("During tearDown: " + e);
        }
        envHome = null;
        env = null;
    }

    private void open(StoredComparatorEntity_Base caseObj)
        throws DatabaseException {

        EnvironmentConfig envConfig = TestEnv.BDB.getConfig();
        envConfig.setAllowCreate(true);
        envConfig.setTransactional(true);
        env = new Environment(envHome, envConfig);

        StoreConfig storeConfig = new StoreConfig();
        storeConfig.setAllowCreate(true);
        storeConfig.setTransactional(true);
        storeConfig.setMutations(caseObj.getMutations());
        store = new EntityStore(env, STORE_NAME, storeConfig);
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
     * During class evolution of ComplexFormat, we'll need to make sure that
     * the new field, incorrectlyOrderedSecKeys, is copied from the old
     * format to the new format, and key names are renamed or delete if
     * appropriate.
     */
    @Test
    public void testEvolveOldDatabaseOrder()
        throws Exception {

        /* Delete the seckey secondary database. */
        openAndDeleteSecDatabase(secDBName);
        caseCls = Class.forName(originalClsName);
        caseObj = (StoredComparatorEntity_Base) caseCls.newInstance();

        /*
         * Re-open the database will create a new sec database for seckey with
         * correct dup Order. catalog.flush will be called to update the
         * ComplexFormat.
         */
        Store.expectFlush = true;
        try {
            open(caseObj);

            /*
             * incorrectlyOrderedSecKeys is null for the database created by
             * old version je.
             */
            ComplexFormat format =
                (ComplexFormat) store.getModel().getRawType(originalClsName);
            assertEquals(format.getIncorrectlyOrderedSecKeys(), null);
        } finally {
            Store.expectFlush = false;
        }
        /* Check primary database order. */
        caseObj.checkPrimaryDBOrder(store);
        ComplexFormat format =
            (ComplexFormat) store.getModel().getRawType(originalClsName);

        /*
         * incorrectlyOrderedSecKeys is initialized and added the name of
         * new_secKey2, which means new_secKey2 secondary database need to
         * be assigned duplicate comparator.
         */
        assertEquals(format.getIncorrectlyOrderedSecKeys().size(), 1);
        assertTrue
            (format.getIncorrectlyOrderedSecKeys().contains("new_secKey2"));

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
        caseObj.checkAllSecondaryDBOrder(store, true, false);
        close();

        /* Delete the seckey2 secondary database. */
        openAndDeleteSecDatabase(secDBName2);
        caseCls = Class.forName(evolvedClsName);
        caseObj = (StoredComparatorEntity_Base) caseCls.newInstance();

        /*
         * Re-open the database will create a new sec database for seckey2 with
         * correct dup Order. catalog.flush will be called to update the
         * ComplexFormat.
         */
        Store.expectFlush = true;
        try {
            open(caseObj);
            format = (ComplexFormat)
                store.getModel().getRawType(evolvedClsName);
            if (evolvedClsName.equals(packageName + EvolveCase[0])) {

                /*
                 * new_secKey2 will be changed to new_new_secKey2 in the new
                 * format's incorrectlyOrderedSecKeys.
                 */
                assertEquals(format.getIncorrectlyOrderedSecKeys().size(), 1);
                assertTrue(format.getIncorrectlyOrderedSecKeys().
                           contains("new_new_secKey2"));
            } else {

                /*
                 * new_secKey2 will be deleted in the new format's
                 * incorrectlyOrderedSecKeys.
                 */
                assertEquals(format.getIncorrectlyOrderedSecKeys().size(), 0);
            }
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
        caseObj.checkAllSecondaryDBOrder(store, true, true);

        /*
         * incorrectlyOrderedSecKeys is empty after re-open the new_secKey2
         * secondary database.
         */
        format = (ComplexFormat) store.getModel().getRawType(evolvedClsName);
        assertEquals(format.getIncorrectlyOrderedSecKeys().size(), 0);
        close();
    }

    @Entity(version=1)
    static class StoredComparatorEntity
        extends StoredComparatorEntity_Base{

        @PrimaryKey
        Key key;

        @SecondaryKey(relate=MANY_TO_ONE)
        private MyEnum secKey;

        /* Rename secKey2 to new_secKey2. */
        @SecondaryKey(relate=MANY_TO_ONE)
        private Integer new_secKey2;

        StoredComparatorEntity() {}

        StoredComparatorEntity(Key key, MyEnum secKey, Integer secKey2) {
            this.key = key;
            this.secKey = secKey;
            this.new_secKey2 = secKey2;
        }

        @Override
        public String toString() {
            return "[pri = " + key + " sec = " + secKey + " new_sec2 = " +
                   new_secKey2 + ']';
        }

        @Override
        void checkPrimaryDBOrder(EntityStore store) {
            PrimaryIndex<Key, StoredComparatorEntity> priIndex;
            priIndex = store.getPrimaryIndex(Key.class,
                                             StoredComparatorEntity.class);
            EntityCursor<StoredComparatorEntity> entities =
                priIndex.entities(null, null);
            final int nEntities = priKeys.length;
            for (int i = nEntities - 1; i >= 0; i -= 1) {
                StoredComparatorEntity e = entities.next();
                assertNotNull(e);
                assertEquals(priKeys[i], e.key);
                assertEquals(secKeys[i], e.secKey);
            }
            assertNull(entities.next());
            entities.close();
        }

        @Override
        void checkAllSecondaryDBOrder(EntityStore store,
                                      boolean ifReverse1,
                                      boolean ifReverse2) {
            PrimaryIndex<Key, StoredComparatorEntity> priIndex;
            priIndex = store.getPrimaryIndex
                (Key.class, StoredComparatorEntity.class);
            SecondaryIndex<MyEnum, Key,
                           StoredComparatorEntity> secIndex;
            SecondaryIndex<Integer, Key,
                           StoredComparatorEntity> secIndex2;
            secIndex = store.getSecondaryIndex(priIndex, MyEnum.class,
                                               "secKey");
            secIndex2 = store.getSecondaryIndex(priIndex, Integer.class,
                                                "new_secKey2");
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

            EntityCursor<StoredComparatorEntity> entities =
                secIndex.entities(null, null);
            for (MyEnum myEnum : EnumSet.allOf(MyEnum.class)) {
                for (int i : order1) {
                    if (secKeys[i] == myEnum) {
                        StoredComparatorEntity e =
                            entities.next();
                        assertNotNull(e);
                        assertEquals(priKeys[i], e.key);
                        assertEquals(secKeys[i], e.secKey);
                    }
                }
            }
            assertNull(entities.next());
            entities.close();

            entities = secIndex2.entities(null, null);
            for (int secKey = 0; secKey < 3; secKey++) {
                for (int i : order2) {
                    if (secKeys2[i] != null && secKeys2[i] == secKey) {
                        StoredComparatorEntity e =
                            entities.next();
                        assertNotNull(e);
                        assertEquals(priKeys[i], e.key);
                        assertEquals(secKeys2[i], e.new_secKey2);
                    }
                }
            }
            assertNull(entities.next());
            entities.close();
        }

        @Override
        Mutations getMutations() {
            Mutations mutations = new Mutations();
            String clsName =
                StoredComparatorEntity.class.getName();
            mutations.addRenamer(new Renamer(clsName, 0, "secKey2",
                                             "new_secKey2"));
            return mutations;
        }
    }

    /* Rename Entity name to StoredComparatorEntity_RenameSecField. */
    @Entity(version=2)
    static class StoredComparatorEntity_RenameSecField
        extends StoredComparatorEntity_Base {

        @PrimaryKey
        Key key;

        @SecondaryKey(relate=MANY_TO_ONE)
        private MyEnum secKey;

        /* Rename secKey2 from new_secKey2 to new_new_secKey2. */
        @SecondaryKey(relate=MANY_TO_ONE)
        private Integer new_new_secKey2;

        StoredComparatorEntity_RenameSecField() {}

        StoredComparatorEntity_RenameSecField(Key key,
                                              MyEnum secKey,
                                              Integer secKey2) {
            this.key = key;
            this.secKey = secKey;
            this.new_new_secKey2 = secKey2;
        }

        @Override
        void checkPrimaryDBOrder(EntityStore store) {
            PrimaryIndex<Key, StoredComparatorEntity_RenameSecField> priIndex;
            priIndex = store.getPrimaryIndex
                (Key.class, StoredComparatorEntity_RenameSecField.class);
            EntityCursor<StoredComparatorEntity_RenameSecField> entities =
                priIndex.entities(null, null);
            final int nEntities = priKeys.length;
            for (int i = nEntities - 1; i >= 0; i -= 1) {
                StoredComparatorEntity_RenameSecField e = entities.next();
                assertNotNull(e);
                assertEquals(priKeys[i], e.key);
                assertEquals(secKeys[i], e.secKey);
            }
            assertNull(entities.next());
            entities.close();
        }

        @Override
        void checkAllSecondaryDBOrder(EntityStore store,
                                      boolean ifReverse1,
                                      boolean ifReverse2) {
            PrimaryIndex<Key, StoredComparatorEntity_RenameSecField> priIndex;
            priIndex = store.getPrimaryIndex
                (Key.class, StoredComparatorEntity_RenameSecField.class);
            SecondaryIndex<MyEnum, Key,
                           StoredComparatorEntity_RenameSecField> secIndex;
            SecondaryIndex<Integer, Key,
                           StoredComparatorEntity_RenameSecField> secIndex2;
            secIndex = store.getSecondaryIndex(priIndex, MyEnum.class,
                                               "secKey");
            secIndex2 = store.getSecondaryIndex(priIndex, Integer.class,
                                                "new_new_secKey2");
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

            EntityCursor<StoredComparatorEntity_RenameSecField> entities =
                secIndex.entities(null, null);
            for (MyEnum myEnum : EnumSet.allOf(MyEnum.class)) {
                for (int i : order1) {
                    if (secKeys[i] == myEnum) {
                        StoredComparatorEntity_RenameSecField e =
                            entities.next();
                        assertNotNull(e);
                        assertEquals(priKeys[i], e.key);
                    }
                }
            }
            assertNull(entities.next());
            entities.close();

            entities = secIndex2.entities(null, null);
            for (int secKey = 0; secKey < 3; secKey++) {
                for (int i : order2) {
                    if (secKeys2[i] != null && secKeys2[i] == secKey) {
                        StoredComparatorEntity_RenameSecField e =
                            entities.next();
                        assertNotNull(e);
                        assertEquals(priKeys[i], e.key);
                        assertEquals(secKeys2[i], e.new_new_secKey2);
                    }
                }
            }
            assertNull(entities.next());
            entities.close();
        }

        @Override
        Mutations getMutations() {
            Mutations mutations = new Mutations();
            String clsName1 =
                StoredComparatorEntity.class.getName();
            String clsName2 =
                StoredComparatorEntity_RenameSecField.class.getName();
            mutations.addRenamer(new Renamer(clsName1, 0, "secKey2",
                                             "new_secKey2"));
            mutations.addRenamer(new Renamer(clsName1, 0, "secKey2",
                                             "new_new_secKey2"));
            mutations.addRenamer(new Renamer(clsName1, 0, clsName2));
            mutations.addRenamer(new Renamer(clsName1, 1, clsName2));
            mutations.addRenamer(new Renamer(clsName1, 1, "new_secKey2",
                                             "new_new_secKey2"));
            return mutations;
        }

        @Override
        public String toString() {
            return "[pri = " + key + " sec = " + secKey + " new_new_sec2 = " +
                    new_new_secKey2 + ']';
        }
    }

    /* Rename Entity name to StoredComparatorEntity_DeleteSecAnnotation. */
    @Entity(version=2)
    static class StoredComparatorEntity_DeleteSecAnnotation
        extends StoredComparatorEntity_Base{

        @PrimaryKey
        Key key;

        @SecondaryKey(relate=MANY_TO_ONE)
        private MyEnum secKey;

        /* Delete @SecondaryKdy annotation of new_secKey2. */
        private Integer new_secKey2;

        StoredComparatorEntity_DeleteSecAnnotation() {}

        StoredComparatorEntity_DeleteSecAnnotation(Key key,
                                                   MyEnum secKey,
                                                   Integer secKey2) {
            this.key = key;
            this.secKey = secKey;
            this.new_secKey2 = secKey2;
        }

        @Override
        void checkPrimaryDBOrder(EntityStore store) {
            PrimaryIndex<Key, StoredComparatorEntity_DeleteSecAnnotation> priIndex;
            priIndex = store.getPrimaryIndex
                (Key.class, StoredComparatorEntity_DeleteSecAnnotation.class);
            EntityCursor<StoredComparatorEntity_DeleteSecAnnotation> entities =
                priIndex.entities(null, null);
            final int nEntities = priKeys.length;
            for (int i = nEntities - 1; i >= 0; i -= 1) {
                StoredComparatorEntity_DeleteSecAnnotation e = entities.next();
                assertNotNull(e);
                assertEquals(priKeys[i], e.key);
                assertEquals(secKeys[i], e.secKey);
            }
            assertNull(entities.next());
            entities.close();
        }

        @Override
        void checkAllSecondaryDBOrder(EntityStore store,
                                      boolean ifReverse1,
                                      boolean ifReverse2) {
            PrimaryIndex<Key, StoredComparatorEntity_DeleteSecAnnotation>
                priIndex;
            priIndex = store.getPrimaryIndex
                (Key.class, StoredComparatorEntity_DeleteSecAnnotation.class);
            SecondaryIndex<MyEnum, Key,
                           StoredComparatorEntity_DeleteSecAnnotation> secIndex;
            secIndex = store.getSecondaryIndex(priIndex, MyEnum.class,
                                               "secKey");
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

            EntityCursor<StoredComparatorEntity_DeleteSecAnnotation> entities =
                secIndex.entities(null, null);
            for (MyEnum myEnum : EnumSet.allOf(MyEnum.class)) {
                for (int i : order1) {
                    if (secKeys[i] == myEnum) {
                        StoredComparatorEntity_DeleteSecAnnotation e =
                            entities.next();
                        assertNotNull(e);
                        assertEquals(priKeys[i], e.key);
                    }
                }
            }
            assertNull(entities.next());
            entities.close();
        }

        @Override
        Mutations getMutations() {
            Mutations mutations = new Mutations();
            String clsName1 =
                StoredComparatorEntity.class.getName();
            String clsName2 =
                StoredComparatorEntity_DeleteSecAnnotation.class.getName();
            mutations.addRenamer(new Renamer(clsName1, 0, "secKey2",
                                             "new_secKey2"));
            mutations.addRenamer(new Renamer(clsName1, 0, clsName2));
            mutations.addRenamer(new Renamer(clsName1, 1, clsName2));
            return mutations;
        }

        @Override
        public String toString() {
            return "[pri = " + key + " sec = " + secKey + " sec2 = " +
                   new_secKey2 + ']';
        }
    }

    /* Rename Entity name to StoredComparatorEntity_DeleteSecField. */
    @Entity(version=2)
    static class StoredComparatorEntity_DeleteSecField
        extends StoredComparatorEntity_Base {

        @PrimaryKey
        Key key;

        @SecondaryKey(relate=MANY_TO_ONE)
        private MyEnum secKey;

        /* Delete secKey2. */
        //private Integer new_secKey2;

        StoredComparatorEntity_DeleteSecField() {}

        StoredComparatorEntity_DeleteSecField(Key key, MyEnum secKey) {
            this.key = key;
            this.secKey = secKey;
        }

        @Override
        void checkPrimaryDBOrder(EntityStore store) {
            PrimaryIndex<Key, StoredComparatorEntity_DeleteSecField> priIndex;
            priIndex = store.getPrimaryIndex
                (Key.class, StoredComparatorEntity_DeleteSecField.class);
            EntityCursor<StoredComparatorEntity_DeleteSecField> entities =
                priIndex.entities(null, null);
            final int nEntities = priKeys.length;
            for (int i = nEntities - 1; i >= 0; i -= 1) {
                StoredComparatorEntity_DeleteSecField e = entities.next();
                assertNotNull(e);
                assertEquals(priKeys[i], e.key);
                assertEquals(secKeys[i], e.secKey);
            }
            assertNull(entities.next());
            entities.close();
        }

        @Override
        void checkAllSecondaryDBOrder(EntityStore store,
                                      boolean ifReverse1,
                                      boolean ifReverse2) {
            PrimaryIndex<Key, StoredComparatorEntity_DeleteSecField>
                priIndex;
            priIndex = store.getPrimaryIndex
                (Key.class, StoredComparatorEntity_DeleteSecField.class);
            SecondaryIndex<MyEnum, Key,
                           StoredComparatorEntity_DeleteSecField> secIndex;
            secIndex = store.getSecondaryIndex(priIndex, MyEnum.class,
                                               "secKey");
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

            EntityCursor<StoredComparatorEntity_DeleteSecField> entities =
                secIndex.entities(null, null);
            for (MyEnum myEnum : EnumSet.allOf(MyEnum.class)) {
                for (int i : order1) {
                    if (secKeys[i] == myEnum) {
                        StoredComparatorEntity_DeleteSecField e =
                            entities.next();
                        assertNotNull(e);
                        assertEquals(priKeys[i], e.key);
                    }
                }
            }
            assertNull(entities.next());
            entities.close();
        }

        @Override
        Mutations getMutations() {
            Mutations mutations = new Mutations();
            String clsName1 =
                StoredComparatorEntity.class.getName();
            String clsName2 =
                StoredComparatorEntity_DeleteSecField.class.getName();
            mutations.addRenamer(new Renamer(clsName1, 0, clsName2));
            mutations.addDeleter(new Deleter(clsName1, 0, "secKey2"));
            mutations.addRenamer(new Renamer(clsName1, 1, clsName2));
            mutations.addDeleter(new Deleter(clsName1, 1, "new_secKey2"));
            return mutations;
        }

        @Override
        public String toString() {
            return "[pri = " + key + " sec = " + secKey +  ']';
        }
    }

    @Persistent
    static abstract class StoredComparatorEntity_Base {

        abstract Mutations getMutations();
        abstract void checkPrimaryDBOrder(EntityStore store);
        abstract void checkAllSecondaryDBOrder(EntityStore store,
                                               boolean ifReverse1,
                                               boolean ifReverse2);
    }

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
}

