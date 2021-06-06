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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Properties;

import com.sleepycat.je.log.entry.FileHeaderEntry;
import com.sleepycat.je.log.entry.LogEntry;
import com.sleepycat.je.log.entry.RestoreRequired;
import com.sleepycat.je.log.entry.SingleItemEntry;

/**
 * A RestoreMarker is a file that indicates that a normal recovery is not
 * possible, because the log files are physically or semantically inconsistent
 * in some way.
 *
 * One example is an interrupted, incomplete network restore.  The network
 * restore copies log files from a source to destination node. If it's halted,
 * while the destination node may contain files that are readable,
 * checksum-able and seem correct, the set as a whole may not have a complete
 * and coherent copy of the log. In such a case, recovery should not be run on
 * this environment's log until some action is taken to make it consistent. For
 * a network restore, this curative action is to restart the copy process, and
 * to complete the copy of a set of logs from some node to this node. The
 * network restore creates a marker file just before it does any form of change
 * to the log that would make the log inconsistent.
 *
 * The restore marker file is named <MAXINT>.jdb, and holds a normal log file
 * header and a RestoreRequired log entry. The RestoreRequired entry indicates
 * the type of error of the initial cause, and information needed to repair the
 * environment. The mechanism depends on the fact that the very first step of
 * recovery is to read backwards from the last file in the
 * environment. Recovery will start at this file, and will fail when it reads
 * the RestoreRequired log entry, throwing an exception that contains
 * prescriptive information for how the environment can be repaired.
 *
 * Note that createMarkerFile() is idempotent and can be safely called multiple
 * times. It's done that way to make it simpler for the caller.
 *
 * The handler that repairs the log must also delete the marker file so future
 * recoveries can succeed.
 */
public class RestoreMarker {

    /**
     * Internal exception used to distinguish marker file creation from other
     * IOErrors.
     */
    public static class FileCreationException extends Exception {
        FileCreationException(String msg, Throwable cause) {
            super(msg, cause);
        }
    }

    private final LogManager logManager;
    private final File lastFile;

    public RestoreMarker(FileManager fileManager,
                         LogManager logManager) {

        this.logManager = logManager;
        String lastFileName = fileManager.getFullFileName(Integer.MAX_VALUE);
        lastFile = new File(lastFileName);
    }

    public static String getMarkerFileName() {
        return FileManager.getFileName(Integer.MAX_VALUE);
    }

    /**
     * Remove the marker file. Use FileManager.delete so this file works with
     * the FileDeletionDetector.
     * @throws IOException if the file won't delete.
     */
    public void removeMarkerFile(FileManager fileManager)
        throws IOException {

        if (lastFile.exists()) {
            fileManager.deleteFile(Integer.MAX_VALUE);
        }
    }

    /**
     * Create the restore marker file.
     *
     * The method may be called repeatedly, but will not re-create the marker
     * file if there's already a non-zero length file.
     *
     * @param failureType the failure type that should be recorded in the
     * RestoreRequired log entry.
     * @param props will be serialized to store information about how to handle
     * the failure type.
     * @throws FileCreationException if the marker file can't be created.
     */
    public void createMarkerFile(RestoreRequired.FailureType failureType,
                                 Properties props)
        throws FileCreationException {

        /* Don't overwrite the file if it already exists. */
        if (lastFile.exists() && lastFile.length() > 0) {
            return;
        }

        try {
            lastFile.createNewFile();
    
            /*
             * The file will have two log entries:
             * - a manufactured file header. Note that the file header usually
             * has a previous offset that points to the previous log entry. In
             * this case, it's set to 0 because we will never scan backwards
             * from this file.
             * - a RestoreRequired log entry
             */
            FileHeader header = new FileHeader(Integer.MAX_VALUE, 0);
            LogEntry headerLogEntry =
                    new FileHeaderEntry(LogEntryType.LOG_FILE_HEADER, header);
            ByteBuffer buf1 = logManager.putIntoBuffer(headerLogEntry,
                                                       0); // prevLogEntryOffset
    
            RestoreRequired rr = new RestoreRequired(failureType, props);
    
            LogEntry marker =
                SingleItemEntry.create(LogEntryType.LOG_RESTORE_REQUIRED, rr);
            ByteBuffer buf2 = logManager.putIntoBuffer(marker, 0);

            try (FileOutputStream stream = new FileOutputStream(lastFile);
                 FileChannel channel = stream.getChannel()) {
                channel.write(buf1);
                channel.write(buf2);
            } catch (IOException e) {
                /* the stream and channel will be closed */
                throw e;
            }
        } catch (IOException ioe) {
            throw new FileCreationException(
                "Marker file creation failed for: " + failureType +
                " " + ioe.toString(), ioe);
        }
    }
}
