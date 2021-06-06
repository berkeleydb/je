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

package com.sleepycat.je.test;

import java.util.Set;

import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.SecondaryKeyCreator;
import com.sleepycat.je.SecondaryMultiKeyCreator;
import com.sleepycat.util.test.TxnTestCase;

/**
 * Permutes a TxnTestCase over a boolean property for using multiple secondary
 * keys.
 */
public abstract class MultiKeyTxnTestCase extends TxnTestCase {

    boolean useMultiKey = false;

    /**
     * Wraps a single key creator to exercise the multi-key code for tests that
     * only create a single secondary key.
     */
    static class SimpleMultiKeyCreator
        implements SecondaryMultiKeyCreator {

        private SecondaryKeyCreator keyCreator;

        SimpleMultiKeyCreator(SecondaryKeyCreator keyCreator) {
            this.keyCreator = keyCreator;
        }

        public void createSecondaryKeys(SecondaryDatabase secondary,
                                        DatabaseEntry key,
                                        DatabaseEntry data,
                                        Set results)
            throws DatabaseException {

            DatabaseEntry result = new DatabaseEntry();
            if (keyCreator.createSecondaryKey(secondary, key, data, result)) {
                results.add(result);
            }
        }
    }
}
