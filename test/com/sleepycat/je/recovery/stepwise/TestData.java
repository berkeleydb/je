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
package com.sleepycat.je.recovery.stepwise;

import java.util.Arrays;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.je.DatabaseEntry;

/**
 * Wrapper class that encapsulates a record in a database used for recovery
 * testing.
 */
public class TestData implements Comparable<TestData> {
    private DatabaseEntry key;
    private DatabaseEntry data;

    public TestData(DatabaseEntry key, DatabaseEntry data) {
        this.key = new DatabaseEntry(key.getData());
        this.data = new DatabaseEntry(data.getData());
    }

    public boolean equals(Object o ) {
        if (this == o)
            return true;
        if (!(o instanceof TestData))
            return false;

        TestData other = (TestData) o;
        if (Arrays.equals(key.getData(), other.key.getData()) &&
            Arrays.equals(data.getData(), other.data.getData())) {
            return true;
        } else
            return false;
    }

    public String toString() {
        return  " k=" + IntegerBinding.entryToInt(key) +
                " d=" + IntegerBinding.entryToInt(data);
    }

    public int hashCode() {
        return toString().hashCode();
    }

    public DatabaseEntry getKey() {
        return key;
    }

    public DatabaseEntry getData() {
        return data;
    }

    /** TODO: do any recovery tests use a custom comparator? */
    @Override
    public int compareTo(TestData o) {
        final int key1 = IntegerBinding.entryToInt(key);
        final int key2 = IntegerBinding.entryToInt(o.key);
        final int keyCmp = Integer.compare(key1, key2);
        if (keyCmp != 0) {
            return keyCmp;
        }
        final int data1 = IntegerBinding.entryToInt(data);
        final int data2 = IntegerBinding.entryToInt(o.data);
        return Integer.compare(data1, data2);
    }
}
