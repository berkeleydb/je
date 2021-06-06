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
package com.sleepycat.je.util.verify;

import java.util.Properties;

import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.dbi.EnvironmentFailureReason;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.RestoreMarker;
import com.sleepycat.je.log.entry.RestoreRequired;
import com.sleepycat.je.utilint.LoggerUtils;

public class VerifierUtils {

    private static final String EXCEPTION_KEY = "ex";

    private static final String RESTORE_REQUIRED_MSG =
        "The environment may not be opened due to persistent data " +
            "corruption that was detected earlier. The marker file " +
            "(7fffffff.jdb) may be deleted to allow recovery, but " +
            "this is normally unsafe and not recommended. " +
            "Original exception:\n";

    /**
     * Create the restore marker file from Exception origException and return
     * an EFE that can be thrown by the caller. The EFE will invalidate the
     * environment.
     *
     * @param failureType the failure type that should be recorded in the
     * RestoreRequired log entry.
     * @param origException the exception contains the properties that are
     * stored to the marker file.
     */
    public static EnvironmentFailureException createMarkerFileFromException(
        RestoreRequired.FailureType failureType,
        Throwable origException,
        EnvironmentImpl envImpl,
        EnvironmentFailureReason reason) {

        String markerFileError = "";

        /*
         * If env is read-only (for example when using the DbVerify command
         * line) we cannot create the marker file, but we should still create
         * and return an invalidating EFE indicating persistent corruption.
         */
        if (!envImpl.isReadOnly()) {
            final Properties props = new Properties();

            props.setProperty(
                EXCEPTION_KEY, origException.toString() + "\n" +
                    LoggerUtils.getStackTrace(origException));

            final RestoreMarker restoreMarker = new RestoreMarker(
                envImpl.getFileManager(), envImpl.getLogManager());

            try {
                restoreMarker.createMarkerFile(failureType, props);
            } catch (RestoreMarker.FileCreationException e) {
                markerFileError = " " + e.getMessage();
            }
        }

        return new EnvironmentFailureException(
            envImpl,
            reason,
            "Persistent corruption detected: " + origException.toString() +
                markerFileError,
            origException);
    }

    /*
     * Get a message referencing the original data corruption exception.
     */
    public static String getRestoreRequiredMessage(
        RestoreRequired restoreRequired) {

        Properties p = restoreRequired.getProperties();
        return RESTORE_REQUIRED_MSG + p.get(EXCEPTION_KEY);
    }
}
