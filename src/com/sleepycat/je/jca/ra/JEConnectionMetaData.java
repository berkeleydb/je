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

package com.sleepycat.je.jca.ra;

import javax.resource.spi.ManagedConnectionMetaData;

public class JEConnectionMetaData
    implements ManagedConnectionMetaData {

    public JEConnectionMetaData() {
    }

    public String getEISProductName() {
        return "Berkeley DB Java Edition JCA";
    }

    public String getEISProductVersion() {
        return "2.0";
    }

    public int getMaxConnections() {
        /* Make a je.* parameter? */
        return 100;
    }

    public String getUserName() {
            return null;
    }
}
