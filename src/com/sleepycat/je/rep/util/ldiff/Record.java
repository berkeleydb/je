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

package com.sleepycat.je.rep.util.ldiff;

import java.util.Arrays;

import com.sleepycat.je.utilint.VLSN;

/*
 * An object used to record a key/data pair in the different area, also
 * saves the VLSN number for the record.
 */
public class Record {
    private final byte[] key;
    private final byte[] data;
    private final VLSN vlsn;
    byte[] mix;

    public Record(byte[] key, byte[] data, VLSN vlsn) {
        this.key = key;
        this.data = data;
        this.vlsn = vlsn;
    }

    /*
     * Get the byte and data array together so that we can generate
     * an unique hash code for this object.
     */
    private void generateMix() {
        mix = new byte[key.length + data.length];
        System.arraycopy(key, 0, mix, 0, key.length);
        System.arraycopy(data, 0, mix, key.length, data.length);
    }

    public byte[] getKey() {
        return key;
    }

    public byte[] getData() {
        return data;
    }

    public VLSN getVLSN() {
        return vlsn;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (!(o instanceof Record)) {
            return false;
        }

        final Record record = (Record) o;

        return Arrays.equals(record.getKey(), getKey()) &&
               Arrays.equals(record.getData(), getData());
    }

    @Override
    public int hashCode() {
        if (mix == null && key != null && data != null) {
            generateMix();
        }

        return Arrays.hashCode(mix);
    }
}
