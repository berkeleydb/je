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

/*
 * A Commit entry signals that some records should be moved from the
 * not-yet-committed sets to the expected set.
 */
public class CommitEntry extends LogEntryInfo {
    private long txnId;

    CommitEntry(long lsn, long txnId) {
        super(lsn, 0, 0);
        this.txnId = txnId;
    }

    @Override
    public void updateExpectedSet
        (Set<TestData>  useExpected,
         Map<Long, Set<TestData>> newUncommittedRecords,
         Map<Long, Set<TestData>> deletedUncommittedRecords) {

        Long mapKey = new Long(txnId);

        /* Add any new records to the expected set. */
        Set<TestData> records = newUncommittedRecords.get(mapKey);
        if (records != null) {
            Iterator<TestData> iter = records.iterator();
            while (iter.hasNext()) {
                useExpected.add(iter.next());
            }
        }

        /* Remove any deleted records from expected set. */
        records = deletedUncommittedRecords.get(mapKey);
        if (records != null) {
            Iterator<TestData> iter = records.iterator();
            while (iter.hasNext()) {
                useExpected.remove(iter.next());
            }
        }
    }
}
