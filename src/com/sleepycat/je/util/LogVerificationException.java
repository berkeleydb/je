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

import java.io.IOException;

/**
 * Thrown during log verification if a checksum cannot be verified or a log
 * entry is determined to be invalid by examining its contents.
 *
 * <p>This class extends {@code IOException} so that it can be thrown by the
 * {@code InputStream} methods of {@link LogVerificationInputStream}.</p>
 */
public class LogVerificationException extends IOException {
    private static final long serialVersionUID = 1L;

    public LogVerificationException(final String message) {
        super(message);
    }

    public LogVerificationException(final String message,
                                    final Throwable cause) {
        super(message);
        initCause(cause);
    }
}
