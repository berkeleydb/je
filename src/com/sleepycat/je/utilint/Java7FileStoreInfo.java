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
import java.nio.file.FileStore;
import java.nio.file.FileSystems;
import java.nio.file.Files;

/**
 * An implementation of {@link FileStoreInfo} that uses Java 7 facilities.
 * Until we require Java 7, this class should only be referenced via
 * reflection.
 */
class Java7FileStoreInfo extends FileStoreInfo {

    /** The underlying Java 7 file store. */
    private final FileStore fileStore;

    /** The associated Factory. */
    static class Java7Factory implements Factory {
        @Override
        public void factoryCheckSupported() { }
        @Override
        public FileStoreInfo factoryGetInfo(final String file)
            throws IOException {

            return new Java7FileStoreInfo(file);
        }
    }

    /**
     * Creates an instance for the specified file.
     *
     * @param file the file
     * @throws IllegalArgumentException if the argument is {@code null}
     * @throws IOException if there is an I/O error
     */
    Java7FileStoreInfo(final String file)
        throws IOException {

        if (file == null) {
            throw new IllegalArgumentException("The file must not be null");
        }
        fileStore = Files.getFileStore(FileSystems.getDefault().getPath(file));
    }

    @Override
    public long getTotalSpace()
        throws IOException {

        return fileStore.getTotalSpace();
    }

    @Override
    public long getUsableSpace()
        throws IOException {

        return fileStore.getUsableSpace();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (!(obj instanceof Java7FileStoreInfo)) {
            return false;
        } else {
            return fileStore.equals(((Java7FileStoreInfo) obj).fileStore);
        }
    }

    @Override
    public int hashCode() {
        return 197 + (fileStore.hashCode() ^ 199);
    }

    @Override
    public String toString() {
        return fileStore.toString();
    }
}
