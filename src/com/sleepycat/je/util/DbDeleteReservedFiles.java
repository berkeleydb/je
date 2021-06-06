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

import java.io.File;
import java.util.Map;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;

import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentStats;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.FileManager;
import com.sleepycat.je.utilint.CmdUtil;
import com.sleepycat.je.utilint.Pair;

/**
 * Command line utility used to delete reserved files explicitly, when
 * attempting to recover from a disk-full condition.
 *
 * <p>When using HA ({@link com.sleepycat.je.rep.ReplicatedEnvironment}),
 * cleaned files are {@link EnvironmentStats#getReservedLogSize() reserved}
 * and are not deleted until a disk limit is approached. Normally the
 * {@link com.sleepycat.je.EnvironmentConfig#MAX_DISK} and
 * {@link com.sleepycat.je.EnvironmentConfig#FREE_DISK} limits will
 * cause the reserved files to be deleted automatically to prevent
 * filling the disk. However, if these limits are both set to zero, or disk
 * space is used outside of the JE environment, it is possible for the disk
 * to become full. Manual recovery from this situation may require deleting
 * the reserved files without opening the JE Environment using the
 * application. This situation is not expected, but the {@code
 * DbDeleteReservedFiles} utility provides a safeguard.</p>
 *
 * <p>Depending on the arguments given, the utility will either delete or list
 * the oldest reserved files. The files deleted or listed are those that can
 * be deleted in order to free the amount specified. Note that size deleted
 * may be larger than the specified size, because only whole files can be
 * deleted.</p>
 *
 *<pre>
 * java { com.sleepycat.je.util.DbDeleteReservedFiles |
 *        -jar je-&lt;version&gt;.jar DbDeleteReservedFiles }
 *   -h &lt;dir&gt;            # environment home directory
 *   -s &lt;size in MB&gt;     # desired size to be freed in MB
 *  [-l]                       # list reserved files/sizes, do not delete
 *  [-V]                       # print JE version number
 *</pre>
 *
 * <p>When the application uses custom key comparators, be sure to add the
 * jars or classes to the classpath that contain the application's comparator
 * classes.</p>
 *
 * <p>This utility opens the JE Environment in read-only mode in order to
 * determine which files are reserved. To speed up this process, specify
 * a large Java heap size when running the utility; 32 GB is recommended.</p>
 */
public class DbDeleteReservedFiles {

    private static long ONE_MB = 1L << 20;

    private static final String USAGE =
        "usage: " +
            CmdUtil.getJavaCommand(DbDeleteReservedFiles.class) + "\n" +
            "       -h <dir> # environment home directory\n" +
            "       -s <mb>  # desired size to delete in MB\n" +
            "       [-l]     # list files only, do not delete\n" +
            "       [-V]     # print JE version number";

    /*
     * The instance methods in this class are currently only used for
     * testing and are not public because we do not know of a use case for
     * this class other as a command line utility. Also, the env must be
     * closed in order to delete reserved files. But we could expose an API
     * later if necessary.
     */

    public static void main(final String[] args) {
        try {
            final DbDeleteReservedFiles util = new DbDeleteReservedFiles(args);
            final Pair<Long, SortedMap<File, Long>> result = util.execute();
            util.printResult(result.first(), result.second());
            System.exit(0);
        } catch (UsageException e) {
            System.err.println(e.getMessage());
            System.exit(2);
        } catch (Throwable e) {
            e.printStackTrace(System.err);
            System.exit(1);
        }
    }

    private File envHome;
    private long deleteMb;
    private boolean list;

    DbDeleteReservedFiles(final String[] args)
        throws UsageException {

        for (int i = 0; i < args.length; i += 1) {
            final String name = args[i];
            String val = null;

            if (i < args.length - 1 && !args[i + 1].startsWith("-")) {
                i += 1;
                val = args[i];
            }

            switch (name) {

            case "-h":
                if (val == null) {
                    throw usage("No value after -h");
                }
                envHome = new File(val);
                break;

            case "-s":
                if (val == null) {
                    throw usage("No value after -s");
                }
                try {
                    deleteMb = Long.parseLong(val);
                } catch (NumberFormatException e) {
                    throw usage(val + " is not a number");
                }
                if (deleteMb <= 0) {
                    throw usage(val + " is not a positive integer");
                }
                break;

            case "-l":
                list = true;
                break;
            }
        }

        if (envHome == null) {
            throw usage("-h is required");
        }

        if (deleteMb == 0) {
            throw usage("-s is required");
        }
    }

    Pair<Long, SortedMap<File, Long>> execute() {

        final Environment env = new Environment(
            envHome,
            new EnvironmentConfig().setReadOnly(true));

        final EnvironmentImpl envImpl = DbInternal.getEnvironmentImpl(env);
        final FileManager fileManager = envImpl.getFileManager();

        final SortedSet<Long> reservedFiles =
            envImpl.getFileProtector().getReservedFileInfo().second();

        final SortedMap<File, Long> filesToDelete = new TreeMap<>();
        long deleteBytes = 0;

        for (final Long fileNum : reservedFiles) {

            final File file = new File(fileManager.getFullFileName(fileNum));
            final long len = file.length();
            filesToDelete.put(file, len);
            deleteBytes += len;

            if (deleteBytes / ONE_MB >= deleteMb) {
                break;
            }
        }

        env.close();

        if (!list) {
            for (final File file : filesToDelete.keySet()) {
                file.delete();
            }
        }

        return new Pair<>(deleteBytes / ONE_MB, filesToDelete);
    }

    private void printResult(final long size,
                             final SortedMap<File, Long> files) {

        final StringBuilder msg = new StringBuilder(
            String.format("File          Size (MB) %n"));

        for (final Map.Entry<File, Long> entry : files.entrySet()) {
            final File file = entry.getKey();
            final long len = entry.getValue();
            msg.append(String.format(
                "%s  %,d %n", file.getName(), len / ONE_MB));
        }

        msg.append(String.format("Total size (MB): %,d %n", size));

        if (list) {
            msg.append("Files were NOT deleted.");
        } else {
            msg.append("Files were deleted.");
        }

        System.out.println(msg);
    }

    private static class UsageException extends Exception {
        UsageException(final String msg) {
            super(msg);
        }
    }

    private static UsageException usage(final String msg) {

        StringBuilder builder = new StringBuilder();

        if (msg != null) {
            builder.append(msg);
            builder.append(String.format("%n"));
        }

        builder.append(USAGE);

        return new UsageException(builder.toString());
    }
}
