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

package com.sleepycat.je.util;

import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.utilint.StringUtils;

public class StringDbt extends DatabaseEntry {
    public StringDbt() {
    }

    public StringDbt(String value) {
        setString(value);
    }

    public StringDbt(byte[] value) {
        setData(value);
    }

    public void setString(String value) {
        byte[] data = StringUtils.toUTF8(value);
        setData(data);
    }

    public String getString() {
        return StringUtils.fromUTF8(getData(), 0, getSize());
    }

    public String toString() {
        return getString();
    }
}
