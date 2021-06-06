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

package com.sleepycat.je.rep.net;

/**
 * Interface to support supplying of a password.
 */

public interface PasswordSource {
    /**
     * Get the password.  The use of this is context dependent.
     *
     * @return a copy of the password.  It is recommended that the caller
     * overwrite the return value with other characters when the password
     * is no longer required.
     */
    public char[] getPassword();
}
