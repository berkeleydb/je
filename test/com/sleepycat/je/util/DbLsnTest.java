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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.junit.Test;

import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.log.FileManager;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

public class DbLsnTest extends TestBase {
    long[] values = { 0xFF, 0xFFFF, 0xFFFFFF, 0x7FFFFFFF, 0xFFFFFFFFL };

    @Test
    public void testDbLsn() {
        for (int i = 0; i < values.length; i++) {
            long value = values[i];
            long lsn = DbLsn.makeLsn(value, value);
            assertTrue((DbLsn.getFileNumber(lsn) == value) &&
                       (DbLsn.getFileOffset(lsn) == value));
        }
    }

    @Test
    public void testComparableEquality() {
        /* Test equality */

        /* Don't bother with last values[] entry -- it makes NULL_LSN. */
        int lastValue = values.length - 1;
        for (int i = 0; i < lastValue; i++) {
            long value = values[i];
            long lsn1 = DbLsn.makeLsn(value, value);
            long lsn2 = DbLsn.makeLsn(value, value);
            assertTrue(DbLsn.compareTo(lsn1, lsn2) == 0);
        }

        /* Check NULL_LSN. */
        assertTrue(DbLsn.makeLsn(values[lastValue],
                                 values[lastValue]) ==
                   DbLsn.makeLsn(values[lastValue],
                                 values[lastValue]));
    }

    @Test
    public void testComparableException() {
        /* Check that compareTo throws EnvironmentFailureException */

        try {
            long lsn1 = DbLsn.makeLsn(0, 0);
            DbLsn.compareTo(lsn1, DbLsn.NULL_LSN);
            fail("compareTo(null) didn't throw EnvironmentFailureException");
        } catch (EnvironmentFailureException expected) {
        }

        try {
            long lsn1 = DbLsn.makeLsn(0, 0);
            DbLsn.compareTo(DbLsn.NULL_LSN, lsn1);
            fail("compareTo(null) didn't throw EnvironmentFailureException");
        } catch (EnvironmentFailureException expected) {
        }
    }

    @Test
    public void testComparableInequalityFileNumber() {
        /* Check for inequality in the file number */

        /* Don't bother with last values[] entry -- it makes NULL_LSN. */
        int lastValue = values.length - 1;
        for (int i = 0; i < lastValue; i++) {
            long value = values[i];
            long lsn1 = DbLsn.makeLsn(value, value);
            long lsn2 = DbLsn.makeLsn(0, value);
            assertTrue(DbLsn.compareTo(lsn1, lsn2) == 1);
            assertTrue(DbLsn.compareTo(lsn2, lsn1) == -1);
        }

        /* Check against NULL_LSN. */
        long lsn1 = DbLsn.makeLsn(values[lastValue], values[lastValue]);
        long lsn2 = DbLsn.makeLsn(0, values[lastValue]);
        try {
            assertTrue(DbLsn.compareTo(lsn1, lsn2) == 1);
        } catch (EnvironmentFailureException expected) {
        }

        try {
            assertTrue(DbLsn.compareTo(lsn2, lsn1) == 1);
        } catch (EnvironmentFailureException expected) {
        }
    }

    @Test
    public void testComparableInequalityFileOffset() {
        /* Check for inequality in the file offset */

        for (int i = 0; i < values.length - 1; i++) {
            long value = values[i];
            long lsn1 = DbLsn.makeLsn(value, value);
            long lsn2 = DbLsn.makeLsn(value, 0);
            /* Can't compareTo(NULL_LSN). */
            if (lsn1 != DbLsn.NULL_LSN &&
                lsn2 != DbLsn.NULL_LSN) {
                assertTrue(DbLsn.compareTo(lsn1, lsn2) == 1);
                assertTrue(DbLsn.compareTo(lsn2, lsn1) == -1);
            }
        }
    }

    @Test
    public void testNoCleaningDistance() {
        long a = DbLsn.makeLsn(1, 10);
        long b = DbLsn.makeLsn(3, 40);
        assertEquals(230, DbLsn.getNoCleaningDistance(b, a, 100));
        assertEquals(230, DbLsn.getNoCleaningDistance(a, b, 100));

        long c = DbLsn.makeLsn(1, 50);
        assertEquals(40, DbLsn.getNoCleaningDistance(a, c, 100));
        assertEquals(40, DbLsn.getNoCleaningDistance(c, a, 100));
    }

    @Test
    public void testWithCleaningDistance()
        throws Exception {

        /* Try with non-consecutive files (due to cleaning). */

        final File envHome = SharedTestUtils.getTestDir();
        TestUtils.removeLogFiles("testWithCleaningDistance", envHome, false);

        final EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setAllowCreate(true);

        Environment env = new Environment(envHome, envConfig);

        try {
            final FileManager fileManager =
                DbInternal.getNonNullEnvImpl(env).getFileManager();

            createFile(fileManager, 1L, 0);
            createFile(fileManager, 3L, 0);

            final long a = DbLsn.makeLsn(1, 10);
            final long b = DbLsn.makeLsn(3, 40);

            assertEquals(
                130,
                DbLsn.getWithCleaningDistance(b, a, 100, fileManager));

            assertEquals(
                130,
                DbLsn.getWithCleaningDistance(a, b, 100, fileManager));

            final long c = DbLsn.makeLsn(1, 50);

            assertEquals(
                40,
                DbLsn.getWithCleaningDistance(a, c, 100, fileManager));

            assertEquals(
                40,
                DbLsn.getWithCleaningDistance(c, a, 100, fileManager));

            env.close();
            env = null;
        } finally {
            if (env != null) {
                try {
                    env.close();
                } catch (Throwable e) {
                    /* Ignore this. Another exception is in flight. */
                }
            }
        }
    }

    @Test
    public void testTrueDistance()
        throws Exception {

        /* Try with non-consecutive files (due to cleaning). */

        final File envHome = SharedTestUtils.getTestDir();
        TestUtils.removeLogFiles("testTrueDistance", envHome, false);

        final EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setAllowCreate(true);

        Environment env = new Environment(envHome, envConfig);

        try {
            final FileManager fileManager =
                DbInternal.getNonNullEnvImpl(env).getFileManager();

            createFile(fileManager, 1L, 100);
            createFile(fileManager, 3L, 300);
            createFile(fileManager, 5L, 500);

            final long a = DbLsn.makeLsn(1, 10);
            final long b = DbLsn.makeLsn(1, 50);
            final long c = DbLsn.makeLsn(3, 70);
            final long d = DbLsn.makeLsn(5, 90);

            expectTrueDistance(0, a, a, fileManager);
            expectTrueDistance(0, b, b, fileManager);
            expectTrueDistance(0, c, c, fileManager);
            expectTrueDistance(0, d, d, fileManager);

            expectTrueDistance(40, a, b, fileManager);
            expectTrueDistance(40, b, a, fileManager);

            expectTrueDistance(160, a, c, fileManager);
            expectTrueDistance(480, a, d, fileManager);
            expectTrueDistance(120, b, c, fileManager);
            expectTrueDistance(440, b, d, fileManager);
            expectTrueDistance(320, c, d, fileManager);

            env.close();
            env = null;
        } finally {
            if (env != null) {
                try {
                    env.close();
                } catch (Throwable e) {
                    /* Ignore this. Another exception is in flight. */
                }
            }
        }
    }

    private void expectTrueDistance(final long expectValue,
                                    final long lsn1,
                                    final long lsn2,
                                    final FileManager fileManager) {
        assertEquals(
            expectValue,
            DbLsn.getTrueDistance(lsn1, lsn2, fileManager));
    }

    private void createFile(final FileManager fileManager,
                            final long fileNum,
                            final int length)
        throws IOException {

        final String path = fileManager.getFullFileName(fileNum);
        final File file = new File(path);
        file.createNewFile();
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            raf.setLength(length);
        }
    }
}
