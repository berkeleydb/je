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
package com.sleepycat.persist.test;

import junit.framework.TestCase;

import com.sleepycat.compat.DbCompat;
import com.sleepycat.je.Environment;

class PersistTestUtils {

    /**
     * Asserts than a database expectExists or does not exist. If keyName is
     * null, checks an entity database.  If keyName is non-null, checks a
     * secondary database.
     */
    static void assertDbExists(boolean expectExists,
                               Environment env,
                               String storeName,
                               String entityClassName,
                               String keyName) {
        String fileName;
        String dbName;
        if (DbCompat.SEPARATE_DATABASE_FILES) {
            fileName = storeName + '-' + entityClassName;
            if (keyName != null) {
                fileName += "-" + keyName;
            }
            dbName = null;
        } else {
            fileName = null;
            dbName = "persist#" + storeName + '#' + entityClassName;
            if (keyName != null) {
                dbName += "#" + keyName;
            }
        }
        boolean exists = DbCompat.databaseExists(env, fileName, dbName);
        if (expectExists != exists) {
            TestCase.fail
                ((expectExists ? "Does not exist: " : "Does exist: ") +
                 dbName);
        }
    }
}
