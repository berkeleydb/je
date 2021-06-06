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

package com.sleepycat.je.utilint;

public class HexFormatter {
    static public String formatLong(long l) {
        StringBuilder sb = new StringBuilder();
        sb.append(Long.toHexString(l));
        sb.insert(0, "0000000000000000".substring(0, 16 - sb.length()));
        sb.insert(0, "0x");
        return sb.toString();
    }
}
