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

package com.sleepycat.bind.serial.test;

import java.io.ObjectStreamClass;
import java.math.BigInteger;

import com.sleepycat.bind.serial.ClassCatalog;

/**
 * NullCatalog is a dummy Catalog implementation that simply
 * returns large (8 byte) class IDs so that ObjectOutput
 * can be simulated when computing a serialized size.
 *
 * @author Mark Hayes
 */
class NullClassCatalog implements ClassCatalog {

    private long id = Long.MAX_VALUE;

    public void close() {
    }

    public byte[] getClassID(ObjectStreamClass classFormat) {
        return BigInteger.valueOf(id--).toByteArray();
    }

    public ObjectStreamClass getClassFormat(byte[] classID) {
        return null; // ObjectInput not supported
    }

    public ClassLoader getClassLoader() {
        return null;
    }
}
