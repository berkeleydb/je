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

/**
 * Indicates that a checksum validation failed.  A checked exception is used so
 * it can be caught and handled internally in some cases.  When not handled
 * internally, it is wrapped with an EnvironmentFailureException with
 * EnvironmentFailureReason.LOG_CHECKSUM before being propagated through the
 * public API.
 */
public class ChecksumException extends Exception {

    private static final long serialVersionUID = 1;

    public ChecksumException(String message) {
        super(message);
    }

    public ChecksumException(String message, Exception e) {
        super(message, e);
    }
}
