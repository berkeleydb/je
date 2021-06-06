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

import com.sleepycat.je.DatabaseEntry;

/**
 * A pass-through <code>EntryBinding</code> that uses the entry's byte array as
 * the key or data object.
 *
 * @author Mark Hayes
 */
public class ByteArrayBinding implements EntryBinding<byte[]> {

    /*
     * We can return the same byte[] for 0 length arrays.
     */
    private static byte[] ZERO_LENGTH_BYTE_ARRAY = new byte[0];

    /**
     * Creates a byte array binding.
     */
    public ByteArrayBinding() {
    }

    // javadoc is inherited
    public byte[] entryToObject(DatabaseEntry entry) {

        int len = entry.getSize();
        if (len == 0) {
            return ZERO_LENGTH_BYTE_ARRAY;
        } else {
            byte[] bytes = new byte[len];
            System.arraycopy(entry.getData(), entry.getOffset(),
                             bytes, 0, bytes.length);
            return bytes;
        }
    }

    // javadoc is inherited
    public void objectToEntry(byte[] object, DatabaseEntry entry) {

        entry.setData(object, 0, object.length);
    }
}
