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

package com.sleepycat.je.rep.impl.networkRestore;

import static com.sleepycat.je.rep.impl.networkRestore.NetworkBackupStatDefinition.BACKUP_FILE_COUNT;
import static com.sleepycat.je.rep.impl.networkRestore.NetworkBackupStatDefinition.DISPOSED_COUNT;
import static com.sleepycat.je.rep.impl.networkRestore.NetworkBackupStatDefinition.EXPECTED_BYTES;
import static com.sleepycat.je.rep.impl.networkRestore.NetworkBackupStatDefinition.FETCH_COUNT;
import static com.sleepycat.je.rep.impl.networkRestore.NetworkBackupStatDefinition.SKIP_COUNT;
import static com.sleepycat.je.rep.impl.networkRestore.NetworkBackupStatDefinition.TRANSFERRED_BYTES;
import static com.sleepycat.je.rep.impl.networkRestore.NetworkBackupStatDefinition.TRANSFER_RATE;

import java.io.Serializable;

import com.sleepycat.je.utilint.LongAvgRateStat;
import com.sleepycat.je.utilint.StatGroup;

/**
 * Stores NetworkBackup statistics.
 *
 * @see NetworkBackupStatDefinition
 */
public class NetworkBackupStats implements Serializable {
    private static final long serialVersionUID = 0;

    private final StatGroup statGroup;

    NetworkBackupStats(StatGroup statGroup) {
        this.statGroup = statGroup;
    }

    public int getBackupFileCount() {
        return statGroup.getInt(BACKUP_FILE_COUNT);
    }

    public int getSkipCount() {
        return statGroup.getInt(SKIP_COUNT);
    }

    public int getFetchCount() {
        return statGroup.getInt(FETCH_COUNT);
    }

    public int getDisposedCount() {
        return statGroup.getInt(DISPOSED_COUNT);
    }

    public long getExpectedBytes() {
        return statGroup.getLong(EXPECTED_BYTES);
    }

    public long getTransferredBytes() {
        return statGroup.getLong(TRANSFERRED_BYTES);
    }

    public long getTransferRate() {
        final LongAvgRateStat stat =
            (LongAvgRateStat) statGroup.getStat(TRANSFER_RATE);
        return (stat == null) ? 0 : stat.get();
    }

    @Override
    public String toString() {
        return statGroup.toString();
    }
}
