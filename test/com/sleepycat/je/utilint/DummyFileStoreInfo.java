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

package com.sleepycat.je.utilint;

import java.io.IOException;

/**
 * Define a FileStoreInfo implementation that can be controlled by the test,
 * both to isolate the test from current file system free space conditions, and
 * to permit testing with specific free space conditions.
 */
public class DummyFileStoreInfo extends FileStoreInfo
        implements FileStoreInfo.Factory {

    public static DummyFileStoreInfo INSTANCE = new DummyFileStoreInfo();

    protected DummyFileStoreInfo() { }

    /* Implement Factory */

    @Override
    public void factoryCheckSupported() { }

    @Override
    public FileStoreInfo factoryGetInfo(final String file)
        throws IOException {

        factoryCheckSupported();
        return this;
    }

    /* Implement FileStoreInfo */

    @Override
    public long getTotalSpace()
        throws IOException {

        return Long.MAX_VALUE;
    }

    @Override
    public long getUsableSpace()
        throws IOException {

        return Long.MAX_VALUE;
    }

    /* Object methods */

    @Override
    public boolean equals(final Object o) {
        return getClass().isInstance(o);
    }

    @Override
    public int hashCode() {
        return getClass().hashCode();
    }
}
