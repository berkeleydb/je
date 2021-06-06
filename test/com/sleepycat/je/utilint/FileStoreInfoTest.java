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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeThat;

import static org.hamcrest.core.IsNull.nullValue;

import java.io.File;
import java.io.IOException;
import java.util.logging.Logger;

import org.junit.Test;

import com.sleepycat.je.utilint.LoggerUtils;

/** Test {@link FileStoreInfo}. */
public class FileStoreInfoTest {

    private final Logger logger =
        LoggerUtils.getLoggerFixedPrefix(getClass(), "Test");

    /** Test when running on Java 6. */
    @Test
    public void testJava6()
        throws Exception {

        try {
            Class.forName(FileStoreInfo.FILE_STORE_CLASS);
            assumeThat("Skip when running Java 7 or later", nullValue());
        } catch (ClassNotFoundException e) {
        }

        try {
            FileStoreInfo.checkSupported();
            fail("Expected UnsupportedOperationException");
        } catch (UnsupportedOperationException e) {
            logger.info("Got expected unsupported exception for Java 6: " + e);
        }

        try {
            FileStoreInfo.getInfo(System.getProperty("user.dir"));
            fail("Expected UnsupportedOperationException");
        } catch (UnsupportedOperationException e) {
            logger.info("Got expected exception for Java 6: " + e);
        }
    }

    /** Test when running on Java 7 or later. */
    @Test
    public void testJava7()
        throws Exception {

        try {
            Class.forName(FileStoreInfo.FILE_STORE_CLASS);
        } catch (ClassNotFoundException e) {
            assumeThat("Skip when running Java 6", nullValue());
        }

        FileStoreInfo.checkSupported();

        final File file1 = File.createTempFile("file1", null);
        file1.deleteOnExit();
        final FileStoreInfo info1 = FileStoreInfo.getInfo(file1.getPath());

        assertFalse(info1.equals(null));
        assertFalse(info1.equals(Boolean.TRUE));
        assertEquals(info1, info1);
        assertEquals(info1, FileStoreInfo.getInfo(file1.getPath()));

        assertTrue("Total space greater than zero",
                   info1.getTotalSpace() > 0);
        assertTrue("Usable space greater than zero",
                   info1.getUsableSpace() > 0);

        final File file2 = File.createTempFile("file2", null);
        file2.deleteOnExit();
        final FileStoreInfo info2 = FileStoreInfo.getInfo(file2.getPath());

        assertEquals("Equal file store info for files in same directory",
                     info1, info2);

        file2.delete();
        try {
            FileStoreInfo.getInfo(file2.getPath()).getTotalSpace();
            fail("Expected IOException");
        } catch (IOException e) {
            logger.info("Got expected exception for deleted file: " + e);
        }
   }
}
