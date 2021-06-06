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

package com.sleepycat.je.recovery.stepwise;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.sleepycat.bind.tuple.IntegerBinding;

/*
 * A non-transactional log entry should add itself to the expected set.
 */

class NonTxnalDeletedEntry extends LogEntryInfo {
    NonTxnalDeletedEntry(long lsn,
                  int key,
                  int data) {
        super(lsn, key, data);
    }

    /* Delete this item from the expected set. */
    @Override
    public void updateExpectedSet
        (Set<TestData> useExpected, 
         Map<Long, Set<TestData>> newUncommittedRecords, 
         Map<Long, Set<TestData>>  deletedUncommittedRecords) {

        Iterator<TestData> iter = useExpected.iterator();
        while (iter.hasNext()) {
            TestData setItem = iter.next();
            int keyValInSet = IntegerBinding.entryToInt(setItem.getKey());
            if (keyValInSet == key) {
                if (data == -1) {
                    /* non-dup case, remove the matching key. */
                    iter.remove();
                    break;
                } else {
                    int dataValInSet = 
                        IntegerBinding.entryToInt(setItem.getData());
                    if (dataValInSet == data) {
                        iter.remove();
                        break;
                    }
                }
            }
        }
    }
}
