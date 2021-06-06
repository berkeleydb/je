/*-
 *
 *  This file is part of Oracle Berkeley DB Java Edition
 *  Copyright (C) 2002, 2016 Oracle and/or its affiliates.  All rights reserved.
 *
 *  Oracle Berkeley DB Java Edition is free software: you can redistribute it
 *  and/or modify it under the terms of the GNU Affero General Public License
 *  as published by the Free Software Foundation, version 3.
 *
 *  Oracle Berkeley DB Java Edition is distributed in the hope that it will be
 *  useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Affero
 *  General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License in
 *  the LICENSE file along with Oracle Berkeley DB Java Edition.  If not, see
 *  <http://www.gnu.org/licenses/>.
 *
 *  An active Oracle commercial licensing agreement for this product
 *  supercedes this license.
 *
 *  For more information please contact:
 *
 *  Vice President Legal, Development
 *  Oracle America, Inc.
 *  5OP-10
 *  500 Oracle Parkway
 *  Redwood Shores, CA 94065
 *
 *  or
 *
 *  berkeleydb-info_us@oracle.com
 *
 *  [This line intentionally left blank.]
 *  [This line intentionally left blank.]
 *  [This line intentionally left blank.]
 *  [This line intentionally left blank.]
 *  [This line intentionally left blank.]
 *  [This line intentionally left blank.]
 *  EOF
 *
 */

package com.sleepycat.je;

import com.sleepycat.je.txn.Locker;

/**
 * Thrown when a write operation cannot be performed because a disk limit has
 * been violated. It may also be thrown by {@link Environment#checkpoint}
 * {@link Environment#sync} and {@link Environment#close} (when it performs
 * a checkpoint).
 *
 * @see EnvironmentConfig#MAX_DISK
 * @see EnvironmentConfig#FREE_DISK
 * @since 7.5
 */
public class DiskLimitException extends OperationFailureException {

    private static final long serialVersionUID = 1;

    /**
     * For internal use only.
     * @hidden
     *
     * @param locker is non-null to mark the txn abort-only, or null in cases
     * where no txn/locker is involved.
     */
    public DiskLimitException(Locker locker, String message) {
        super(
            locker /*locker*/,
            locker != null /*abortOnly*/,
            message,
            null /*cause*/);
    }

    /**
     * For internal use only.
     * @hidden
     */
    private DiskLimitException(String message, DiskLimitException cause) {
        super(message, cause);
    }

    /**
     * For internal use only.
     * @hidden
     */
    @Override
    public OperationFailureException wrapSelf(String msg) {
        return new DiskLimitException(msg, this);
    }
}
