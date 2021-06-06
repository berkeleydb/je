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

package com.sleepycat.je.rep.txn;

import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.txn.ThreadLocker;

/**
 * A MasterThreadLocker is used with a user initiated non-transactional
 * operation on a Master, for a replicated DB.  Currently there is no behavior
 * specific to this class, and it is only a placeholder for future HA
 * functionality.
 */
public class MasterThreadLocker extends ThreadLocker {

    public MasterThreadLocker(final RepImpl repImpl) {
        super(repImpl);
    }

    @Override
    public ThreadLocker newEmptyThreadLockerClone() {
        return new MasterThreadLocker((RepImpl) envImpl);
    }
}
