/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2014 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package com.sleepycat.je;

import static com.sleepycat.persist.model.Relationship.MANY_TO_ONE;

import java.io.Serializable;
import java.util.Comparator;

import com.sleepycat.je.tree.Key;
import com.sleepycat.persist.EntityCursor;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.SecondaryIndex;
import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.KeyField;
import com.sleepycat.persist.model.Persistent;
import com.sleepycat.persist.model.PrimaryKey;
import com.sleepycat.persist.model.SecondaryKey;

public class LoadedClassImpl
    implements ClassLoaderTest.LoadedClass {
    
    public Comparator<byte[]> getReverseComparator() {
        return new ReverseComparator();
    }
    
    public Class<? extends Comparator<byte[]>> getReverseComparatorClass() {
        return ReverseComparator.class;
    }

    public static class ReverseComparator
        implements Comparator<byte[]>, Serializable  {

        private static final long serialVersionUID = 1L;

        public int compare(byte[] o1, byte[] o2) {
            return Key.compareKeys(o2, o1, null /*comparator*/);
        }
    }

    public Object getSerializableInstance() {
        return new SerialData();
    }

    private static class SerialData implements Serializable {

        private static final long serialVersionUID = 1L;

        private int x;
        private int y;

        private SerialData() {
            x = 1;
            y = 2;
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof SerialData)) {
                return false;
            }
            final SerialData o = (SerialData) other;
            return x == o.x && y == o.y;
        }
    }

    @Persistent
    static class KeyClass implements Comparable<KeyClass> {

        @KeyField(1)
        int i1;
        @KeyField(2)
        int i2;

        KeyClass() {
        }

        KeyClass(int key, int data) {
            this.i1 = key;
            this.i2 = data;
        }

        public int compareTo(KeyClass o) {
            int cmp = i2 - o.i2;
            if (cmp != 0) {
                return cmp;
            }
            cmp = i1 - o.i1;
            if (cmp != 0) {
                return cmp;
            }
            return 0;
        }

        @Override
        public String toString() {
            return "[KeyClass i1=" + i1 + " i2=" + i2 + "]";
        }
    }

    @Entity
    static class MyEntity {

        @PrimaryKey
        int key;

        int data;

        @SecondaryKey(relate=MANY_TO_ONE)
        KeyClass skey;

        MyEntity() {
        }

        MyEntity(int key, int data) {
            this.key = key;
            this.data = data;
            this.skey = new KeyClass(key, data);
        }

        @Override
        public String toString() {
            return "[MyEntity key=" + key + " data=" + data +
                   " skey=" + skey + "]";
        }
    }

    public void writeEntities(EntityStore store) {
        final PrimaryIndex<Integer, MyEntity> index =
            store.getPrimaryIndex(Integer.class, MyEntity.class);
        index.put(new MyEntity(0, 10));
        index.put(new MyEntity(1, 11));
        index.put(new MyEntity(2, 10));
        index.put(new MyEntity(3, 11));
        index.put(new MyEntity(4, 10));
        index.put(new MyEntity(5, 11));
    }

    public void readEntities(EntityStore store) {

        final PrimaryIndex<Integer, MyEntity> priIndex =
            store.getPrimaryIndex(Integer.class, MyEntity.class);
        final SecondaryIndex<KeyClass, Integer, MyEntity> secIndex =
            store.getSecondaryIndex(priIndex, KeyClass.class, "skey");

        MyEntity e = priIndex.get(1);
        check(e != null && e.key == 1);

        e = secIndex.get(new KeyClass(1, 11));
        if (!(e != null && e.key == 1)) {
            System.out.println(e);
        }
        check(e != null && e.key == 1);

        EntityCursor<MyEntity> cursor = priIndex.entities();
        for (int i = 0; i <= 5; i += 1) {
            e = cursor.next();
            check(e.key == i);
        }
        e = cursor.next();
        check(e == null);
        cursor.close();

        cursor = secIndex.entities();
        for (int i : new int[] {0, 2, 4, 1, 3, 5}) {
            e = cursor.next();
            check(e.key == i);
        }
        e = cursor.next();
        check(e == null);
        cursor.close();
    }

    void check(boolean b) {
        if (!b) {
            throw new RuntimeException();
        }
    }
}
