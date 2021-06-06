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

import java.io.File;

import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.Transaction;
import com.sleepycat.persist.EntityCursor;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.SecondaryIndex;
import com.sleepycat.persist.StoreConfig;
import com.sleepycat.persist.evolve.Mutations;
import com.sleepycat.persist.model.AnnotationModel;
import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.EntityModel;
import com.sleepycat.persist.model.KeyField;
import com.sleepycat.persist.model.Persistent;
import com.sleepycat.persist.model.PrimaryKey;
import com.sleepycat.persist.model.SecondaryKey;

/* 
 * Create a database without a comparator for secondary duplicates with je-4.0. 
 * This database will be used in the unit test com.sleepycat.persist.test.
 * SecondaryDupOrderEvolveTest.
 */
public class CreateSecDupsWithoutComparatorEvolve {
    enum MyEnum { A, B, C };
    Environment env;
    private EntityStore store;
    private PrimaryIndex<Key, 
                         StoredComparatorEntity> priIndex;
    private SecondaryIndex<MyEnum,
                           Key,
                           StoredComparatorEntity> secIndex;
    private SecondaryIndex<Integer,
                           Key,
                           StoredComparatorEntity> secIndex2;
    
    public static void main(String args[]) {
        CreateSecDupsWithoutComparatorEvolve csd = 
            new CreateSecDupsWithoutComparatorEvolve();
        csd.open();
        csd.writeData();
        csd.readData();
        csd.close();
    }
    
    private void open() {

        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        envConfig.setTransactional(true);
        File envHome = new File("./");
        env = new Environment(envHome, envConfig);
        StoreConfig config = new StoreConfig();
        config.setAllowCreate(envConfig.getAllowCreate());
        config.setTransactional(envConfig.getTransactional());
        EntityModel model = new AnnotationModel();
        config.setModel(model);
        store = new EntityStore(env, "test", config);
        priIndex = store.getPrimaryIndex(Key.class,
                                        StoredComparatorEntity.class);
        secIndex = store.getSecondaryIndex
            (priIndex, MyEnum.class, "secKey");
        secIndex2 = store.getSecondaryIndex
            (priIndex, Integer.class, "secKey2");
    }
    
    private void close() {        
        store.close();
        store = null;
        env.close();
        env = null;
    }
    
    private void writeData() {
        final Key[] priKeys =
            new Key[] {
                new Key
                    (MyEnum.A, 1,
                     MyEnum.A),
                new Key
                    (MyEnum.A, 1,
                     MyEnum.B),
                new Key
                    (MyEnum.A, 2,
                     MyEnum.A),
                new Key
                    (MyEnum.A, 2,
                     MyEnum.B),
                new Key
                    (MyEnum.B, 1,
                     MyEnum.A),
                new Key
                    (MyEnum.B, 1,
                     MyEnum.B),
                new Key
                    (MyEnum.C, 0,
                     MyEnum.C),
            };

        final MyEnum[] secKeys =
            new MyEnum[] {
                MyEnum.C,
                MyEnum.B,
                MyEnum.A,
                null,
                MyEnum.A,
                MyEnum.B,
                MyEnum.C,
            };
        
        final Integer[] secKeys2 = new Integer[] { 2, 1, 0, null, 0, 1, 2, };
        final int nEntities = priKeys.length;
        Transaction txn = env.beginTransaction(null, null);
        for (int i = 0; i < nEntities; i += 1) {
            priIndex.put(txn, new StoredComparatorEntity
                         (priKeys[i], secKeys[i], secKeys2[i]));
        }
        txn.commit();
    }
    
    private void readData() {
        Transaction txn = env.beginTransaction(null, null);
        StoredComparatorEntity data = 
            priIndex.get(new Key
                    (MyEnum.A, 1,
                     MyEnum.A));
        System.out.println(data);
        EntityCursor<StoredComparatorEntity> entities =
            priIndex.entities(txn, null);
        System.out.println("Primary database order:");
        for (StoredComparatorEntity e : entities) {
            System.out.println(e);
        }
        entities.close();
        txn.commit();
        txn = env.beginTransaction(null, null);
        entities = secIndex.entities(txn, null);
        System.out.println("Secondary database 1 order:");
        for (StoredComparatorEntity e : entities) {
            System.out.println(e);
        }
        entities.close();
        txn.commit();
        txn = env.beginTransaction(null, null);
        entities = secIndex2.entities(txn, null);
        System.out.println("Secondary database 2 order:");
        for (StoredComparatorEntity e : entities) {
            System.out.println(e);
        }
        entities.close();
        txn.commit();
        
    }
    
    @Entity
    static class StoredComparatorEntity
        extends StoredComparatorEntity_Base {

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

        @Override
        void checkAllSecondaryDBOrder(EntityStore store, boolean ifReverse1,
                                      boolean ifReverse2) {    
        }

        @Override
        void checkPrimaryDBOrder(EntityStore store) {    
        }

        @Override
        Mutations getMutations() {
            return null;
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
