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

package com.sleepycat.je.statcap;

import java.text.DateFormat;
import java.util.Date;

import com.sleepycat.je.utilint.TracerFormatter;

class StatUtils {
    private static final DateFormat formatter =
        TracerFormatter.makeDateFormat();
    private static final Date date = new Date();
    /** Returns a string representation of the specified time. */
    public static synchronized String getDate(final long millis) {
        /* The date and formatter are not thread safe */
        date.setTime(millis);
        return formatter.format(date);
    }
}