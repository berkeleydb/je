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

import java.io.File;
import java.util.ArrayList;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeSet;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Durability;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentStats;
import com.sleepycat.je.Get;
import com.sleepycat.je.Put;
import com.sleepycat.je.log.FileManager;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.impl.RepTestBase;
import com.sleepycat.je.rep.utilint.RepTestUtils;
import com.sleepycat.je.rep.utilint.RepTestUtils.RepEnvInfo;
import com.sleepycat.je.utilint.Pair;

import org.junit.Before;
import org.junit.Test;

/**
 * Create an env with reserved files, then deletes them with the
 * DbDeleteReservedFiles utility. Checks that sizes and files deleted are as
 * expected.
 *
 * Does this to each node in a rep group and checks that we can open
 * the env after running the utility do writes and reads. When opening an
 * env after deleting reserved files, the VLSNIndex is truncated
 * automatically, and this is exercised by the test.
 */
public class DbDeleteReservedFilesTest extends RepTestBase {

    private static final long ONE_MB = 1L << 20;
    private static final long FILE_SIZE = 5 * ONE_MB;
    private static final int RECORDS = 100;
    private static final int RESERVED_FILES = 5;
    private static final long RESERVED_SIZE = (RESERVED_FILES + 1) * FILE_SIZE;

    public DbDeleteReservedFilesTest() {
        super();
    }

    @Override
    @Before
    public void setUp()
        throws Exception {

        groupSize = 3;
        super.setUp();

        for (int i = 0; i < groupSize; i += 1) {
            final EnvironmentConfig envConfig = repEnvInfo[i].getEnvConfig();

            envConfig.setDurability(Durability.COMMIT_NO_SYNC);

            envConfig.setConfigParam(
                EnvironmentConfig.LOG_FILE_MAX, String.valueOf(FILE_SIZE));
        }
    }

    @Test
    public void testDeleteReservedFiles()
        throws Exception {

        RepTestUtils.joinGroup(repEnvInfo);
        writeReservedFiles();

        for (final RepEnvInfo info : repEnvInfo) {

            readData(info);

            final long reservedBytes =
                info.getEnv().getStats(null).getReservedLogSize();

            final long reservedFiles =
                (reservedBytes + (FILE_SIZE / 2)) / FILE_SIZE;

            assertTrue(reservedFiles >= RESERVED_FILES);

            RepTestUtils.shutdownRepEnvs(info);

            /* Delete 1 file, first file remaining is 0x1. */
            deleteReservedFiles(info, 1, 1);

            /* Delete 2 files, first file remaining is 0x3. */
            deleteReservedFiles(info, 3, 2);

            /*
             * Delete remaining reserved files, first file remaining is the
             * first non-reserved file.
             */
            deleteReservedFiles(info, reservedFiles, reservedFiles - 3);

            /*
             * Try reading after joining group. This will truncate the
             * VLSNIndex.
             */
            RepTestUtils.joinGroup(repEnvInfo);
            readData(info);
        }

        /*
         * Make sure we can still write/read in this group.
         */
        writeReservedFiles();

        for (final RepEnvInfo info : repEnvInfo) {
            readData(info);
        }
    }

    private void deleteReservedFiles(final RepEnvInfo info,
                                     final long firstFileRemaining,
                                     final long numFilesToDelete)
        throws Exception {

        SortedSet<File> prevFiles = listDataFiles(info);

        final long deleteBytes =
            (numFilesToDelete * FILE_SIZE) - (FILE_SIZE / 2);

        final long deleteMb = deleteBytes / ONE_MB;

        final ArrayList<String> args = new ArrayList<>();
        args.add("-h");
        args.add(info.getEnvHome().getAbsolutePath());
        args.add("-s");
        args.add(String.valueOf(deleteMb));

        /*
         * First just list the files to be deleted.
         */
        args.add("-l");

        DbDeleteReservedFiles util =
            new DbDeleteReservedFiles(args.toArray(new String[0]));

        Pair<Long, SortedMap<File, Long>> result = util.execute();

        prevFiles = verifyResult(
            info, prevFiles, numFilesToDelete, deleteMb,
            result.first(), (SortedSet<File>) result.second().keySet(),
            firstFileRemaining, false);

        /*
         * Now delete the files -- remove the -l param.
         */
        args.remove(args.size() - 1);

        util = new DbDeleteReservedFiles(args.toArray(new String[0]));

        result = util.execute();

        verifyResult(
            info, prevFiles, numFilesToDelete, deleteMb,
            result.first(), (SortedSet<File>) result.second().keySet(),
            firstFileRemaining, true);
    }

    private SortedSet<File> verifyResult(
        final RepEnvInfo info,
        final SortedSet<File> prevFiles,
        final long numFilesToDelete,
        final long deleteMb,
        final long reportedDb,
        final SortedSet<File> filesToDelete,
        final long firstFileRemaining,
        final boolean expectDeleted) {

        final SortedSet<File> currFiles = listDataFiles(info);

        if (expectDeleted) {

            final File firstFile =
                new File(
                    info.getEnvHome(),
                    FileManager.getFileName(firstFileRemaining)).
                    getAbsoluteFile();

            assertEquals(firstFile, currFiles.first());
        }

        assertTrue(
            "prevFiles=" + prevFiles + " filesToDelete=" + filesToDelete,
            prevFiles.containsAll(filesToDelete));

        assertEquals(numFilesToDelete, filesToDelete.size());

        if (!expectDeleted) {
            long size = 0;
            for (final File file : filesToDelete) {
                size += file.length();
            }
            size /= ONE_MB;
            assertEquals(size, reportedDb);
            assertTrue(size >= deleteMb);
        }

        if (expectDeleted) {
            prevFiles.removeAll(filesToDelete);
            assertEquals(prevFiles, currFiles);
        } else {
            assertEquals(prevFiles, currFiles);
        }


        return currFiles;
    }

    private SortedSet<File> listDataFiles(final RepEnvInfo info) {

        final File dir = info.getEnvHome();

        final String[] names = FileManager.listFiles(
            info.getEnvHome(),
            new String[] { FileManager.JE_SUFFIX },
            false);

        final SortedSet<File> set = new TreeSet<>();

        for (final String name : names) {
            final File file = new File(dir, name).getAbsoluteFile();
            set.add(file);
        }

        return set;
    }

    private void writeReservedFiles() {

        final ReplicatedEnvironment master =
            RepTestUtils.getMaster(repEnvInfo, false);

        final DatabaseEntry key = new DatabaseEntry();
        final DatabaseEntry data = new DatabaseEntry(new byte[1024]);

        final Database db = master.openDatabase(
            null, TEST_DB_NAME,
            new DatabaseConfig().setTransactional(true).setAllowCreate(true));

        while (true) {
            for (int i = 0; i < RECORDS; i += 1) {
                IntegerBinding.intToEntry(i, key);
                db.put(null, key, data, Put.OVERWRITE, null);
            }
            final EnvironmentStats stats = master.getStats(null);
            if (stats.getReservedLogSize() >= RESERVED_SIZE) {
                break;
            }
        }

        db.close();
        RepTestUtils.syncGroup(repEnvInfo);
    }

    private void readData(final RepEnvInfo info) {

        final ReplicatedEnvironment env = info.getEnv();
        final DatabaseEntry key = new DatabaseEntry();
        final DatabaseEntry data = new DatabaseEntry();

        final Database db = env.openDatabase(
            null, TEST_DB_NAME,
            new DatabaseConfig().setTransactional(true));

        int records = 0;
        try (final Cursor cursor = db.openCursor(null, null)) {
            while (cursor.get(key, data, Get.NEXT, null) != null) {
                assertEquals(records, IntegerBinding.entryToInt(key));
                assertEquals(1024, data.getSize());
                records += 1;
            }
        }

        assertEquals(RECORDS, records);
        db.close();
    }
}
