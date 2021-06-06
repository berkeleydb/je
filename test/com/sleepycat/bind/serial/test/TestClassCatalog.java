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

package com.sleepycat.bind.serial.test;

import java.io.ObjectStreamClass;
import java.util.HashMap;
import java.util.Map;

import com.sleepycat.bind.serial.ClassCatalog;
import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;

/**
 * @author Mark Hayes
 */
public class TestClassCatalog implements ClassCatalog {

    private final Map<Integer, ObjectStreamClass> idToDescMap =
        new HashMap<Integer, ObjectStreamClass>();
    private final Map<String, Integer> nameToIdMap =
        new HashMap<String, Integer>();
    private int nextId = 1;

    public TestClassCatalog() {
    }

    public void close() {
    }

    public synchronized byte[] getClassID(ObjectStreamClass desc) {
        String className = desc.getName();
        Integer intId = nameToIdMap.get(className);
        if (intId == null) {
            intId = nextId;
            nextId += 1;

            idToDescMap.put(intId, desc);
            nameToIdMap.put(className, intId);
        }
        DatabaseEntry entry = new DatabaseEntry();
        IntegerBinding.intToEntry(intId, entry);
        return entry.getData();
    }

    public synchronized ObjectStreamClass getClassFormat(byte[] byteId)
        throws DatabaseException {

        DatabaseEntry entry = new DatabaseEntry();
        entry.setData(byteId);
        int intId = IntegerBinding.entryToInt(entry);

        ObjectStreamClass desc = (ObjectStreamClass) idToDescMap.get(intId);
        if (desc == null) {
            throw new RuntimeException("classID not found");
        }
        return desc;
    }

    public ClassLoader getClassLoader() {
        return null;
    }
}
