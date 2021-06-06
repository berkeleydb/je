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

import java.util.SortedSet;

/**
 * A home for misc formatting utilities.
 */
public class FormatUtil {

    /**
     * Utility class to convert a sorted set of long values to a compact string
     * suitable for printing. The representation is made compact by identifying
     * ranges so that the sorted set can be represented as a sequence of hex
     * ranges and singletons.
     */
    public static String asHexString(SortedSet<Long> set) {

        if (set.isEmpty()) {
            return "";
        }

        final StringBuilder sb = new StringBuilder();
        java.util.Iterator<Long> i = set.iterator();
        long rstart = i.next();
        long rend = rstart;

        while (i.hasNext()) {
            final long f= i.next();
            if (f == (rend + 1)) {
                /* Continue the existing range. */
                rend++;
                continue;
            }

            /* flush and start new range */
            flushRange(sb, rstart, rend);
            rstart = rend = f;
        };

        flushRange(sb, rstart, rend);
        return sb.toString();
    }

    private static void flushRange(final StringBuilder sb,
                                   long rstart,
                                   long rend) {
        if (rstart == -1) {
            return;
        }

        if (rstart == rend) {
            sb.append(" 0x").append(Long.toHexString(rstart));
        } else {
            sb.append(" 0x").append(Long.toHexString(rstart)).
            append("-").
            append("0x").append(Long.toHexString(rend));
        }
    }
}
