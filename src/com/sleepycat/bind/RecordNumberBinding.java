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

package com.sleepycat.bind;

import com.sleepycat.compat.DbCompat;
import com.sleepycat.je.DatabaseEntry;

/**
 * <!-- begin JE only -->
 * @hidden
 * <!-- end JE only -->
 * An <code>EntryBinding</code> that treats a record number key entry as a
 * <code>Long</code> key object.
 *
 * <p>Record numbers are returned as <code>Long</code> objects, although on
 * input any <code>Number</code> object may be used.</p>
 *
 * @author Mark Hayes
 */
public class RecordNumberBinding implements EntryBinding {

    /**
     * Creates a byte array binding.
     */
    public RecordNumberBinding() {
    }

    // javadoc is inherited
    public Long entryToObject(DatabaseEntry entry) {

        return Long.valueOf(entryToRecordNumber(entry));
    }

    // javadoc is inherited
    public void objectToEntry(Object object, DatabaseEntry entry) {

        recordNumberToEntry(((Number) object).longValue(), entry);
    }

    /**
     * Utility method for use by bindings to translate a entry buffer to an
     * record number integer.
     *
     * @param entry the entry buffer.
     *
     * @return the record number.
     */
    public static long entryToRecordNumber(DatabaseEntry entry) {

        return DbCompat.getRecordNumber(entry) & 0xFFFFFFFFL;
    }

    /**
     * Utility method for use by bindings to translate a record number integer
     * to a entry buffer.
     *
     * @param recordNumber the record number.
     *
     * @param entry the entry buffer to hold the record number.
     */
    public static void recordNumberToEntry(long recordNumber,
                                           DatabaseEntry entry) {
        entry.setData(new byte[4], 0, 4);
        DbCompat.setRecordNumber(entry, (int) recordNumber);
    }
}
