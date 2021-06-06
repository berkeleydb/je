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

package com.sleepycat.je.cleaner;

import java.util.HashSet;
import java.util.Set;

import com.sleepycat.bind.tuple.LongBinding;
import com.sleepycat.bind.tuple.TupleBase;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.utilint.VLSN;

/**
 */
public class ReservedFileInfo {
    public final VLSN firstVLSN;
    public final VLSN lastVLSN;
    public final Set<DatabaseId> dbIds;

    ReservedFileInfo(final VLSN firstVLSN,
                     final VLSN lastVLSN,
                     final Set<DatabaseId> dbIds) {
        this.firstVLSN = firstVLSN;
        this.lastVLSN = lastVLSN;
        this.dbIds = dbIds;
    }

    public static Long entryToKey(final DatabaseEntry entry) {
        return LongBinding.entryToLong(entry);
    }

    public static void keyToEntry(Long key, final DatabaseEntry entry) {
        LongBinding.longToEntry(key, entry);
    }

    public static ReservedFileInfo entryToObject(final DatabaseEntry entry) {
        final TupleInput input = TupleBase.entryToInput(entry);
        input.readByte(); /* Future flags. */
        final VLSN firstVLSN = new VLSN(input.readPackedLong());
        final VLSN lastVLSN = new VLSN(input.readPackedLong());
        final Set<DatabaseId> dbIds = new HashSet<>();
        final int nDbs = input.readPackedInt();
        for (int i = 0; i < nDbs; i += 1) {
            dbIds.add(new DatabaseId(input.readPackedLong()));
        }
        return new ReservedFileInfo(firstVLSN, lastVLSN, dbIds);
    }

    public static void objectToEntry(final ReservedFileInfo info,
                                     final DatabaseEntry entry) {
        final TupleOutput output = new TupleOutput();
        output.writeByte(0); /* Future flags. */
        output.writePackedLong(info.firstVLSN.getSequence());
        output.writePackedLong(info.lastVLSN.getSequence());
        output.writePackedInt(info.dbIds.size());
        for (final DatabaseId id : info.dbIds) {
            output.writePackedLong(id.getId());
        }
        TupleBase.outputToEntry(output, entry);
    }
}
