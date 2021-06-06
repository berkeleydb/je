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

package com.sleepycat.je.log;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Collection;

import org.junit.Test;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.log.entry.LogEntry;
import com.sleepycat.je.log.entry.ReplicableLogEntry;
import com.sleepycat.util.test.TestBase;

public class LogEntryTest extends TestBase {

    @Test
    public void testEquality()
        throws DatabaseException {

        byte testTypeNum = LogEntryType.LOG_IN.getTypeNum();

        /* Look it up by type */
        LogEntryType foundType = LogEntryType.findType(testTypeNum);
        assertEquals(foundType, LogEntryType.LOG_IN);
        assertTrue(foundType.getSharedLogEntry() instanceof
                   com.sleepycat.je.log.entry.INLogEntry);

        /* Look it up by type */
        foundType = LogEntryType.findType(testTypeNum);
        assertEquals(foundType, LogEntryType.LOG_IN);
        assertTrue(foundType.getSharedLogEntry() instanceof
                   com.sleepycat.je.log.entry.INLogEntry);

        /* Get a new entry object */
        LogEntry sharedEntry = foundType.getSharedLogEntry();
        LogEntry newEntry = foundType.getNewLogEntry();

        assertTrue(sharedEntry != newEntry);
    }

    /**
     * See {@link ReplicableLogEntry#getEmbeddedLoggables()}.
     */
    @Test
    public void testLastFormatChange() throws Exception {
        for (final LogEntryType type : LogEntryType.getAllTypes()) {
            final LogEntry entry = type.getSharedLogEntry();
            if (!(entry instanceof ReplicableLogEntry)) {
                continue;
            }
            final ReplicableLogEntry repEntry = (ReplicableLogEntry) entry;
            verifyLastFormatChange(
                repEntry.getClass().getName(), repEntry.getLastFormatChange(),
                repEntry.getEmbeddedLoggables());
        }
    }

    private void verifyLastFormatChange(
        final String entryClassName,
        final int entryLastFormatChange,
        final Collection<VersionedWriteLoggable> embeddedLoggables)
        throws Exception {

        assertNotNull(embeddedLoggables);

        if (embeddedLoggables.size() == 0) {
            return;
        }

        for (final VersionedWriteLoggable child : embeddedLoggables) {

            final int childLastFormatChange = child.getLastFormatChange();

            if (childLastFormatChange > entryLastFormatChange) {
                fail(String.format(
                    "Embedded %s version %d is GT entry %s version %d",
                    child.getClass().getName(), childLastFormatChange,
                    entryClassName, entryLastFormatChange));
            }

            verifyLastFormatChange(
                entryClassName, entryLastFormatChange,
                child.getEmbeddedLoggables());
        }
    }
}
