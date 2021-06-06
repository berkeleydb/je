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

import java.io.File;
import java.io.FilenameFilter;
import java.util.StringTokenizer;

public class BadFileFilter implements FilenameFilter {

    /**
     * Accept files of this format:
     * <nnnnnnnn>.bad.<n>
     */
    public boolean accept(File dir, String name) {
        boolean ok = false;
        StringTokenizer tokenizer = new StringTokenizer(name, ".");
        /* There should be two parts. */
        if (tokenizer.countTokens() == 3) {
            String fileNumber = tokenizer.nextToken();
            String fileSuffix = tokenizer.nextToken();
            String repeat = tokenizer.nextToken();

            /* Check the length and the suffix. */
            if ((fileNumber.length() == 8) &&
                (fileSuffix.equals("bad"))) {

                /* The first and third parts should be a numbers. */
                try {
                    Integer.parseInt(fileNumber);
                    Integer.parseInt(repeat);
                    ok = true;
                } catch (NumberFormatException e) {
                    ok = false;
                }
            }
        }

        return ok;
    }
}
