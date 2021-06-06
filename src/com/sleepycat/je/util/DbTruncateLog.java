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
import java.io.IOException;

import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.utilint.CmdUtil;

/**
 * DbTruncateLog is a utility that lets the user truncate JE log starting at a
 * specified file and offset to the last log file, inclusive. Generally used in
 * replication systems for handling 
 * com.sleepycat.je.rep.RollbackProhibitedException, to permit the application 
 * to interject application specific handling. Should be used with caution.
 * <p>
 * The parameters for DbTruncateLog are provided through the
 * RollbackProhibitedException instance, and the exception message. The goal is
 * to truncate the JE log after a specified file number and file offset. 
 * DbTruncateLog will automatically delete all log entries after that specified
 * log entry.
 * <p>
 * For example, suppose the JE log consists of these files:
 * <pre>
 *    00000002.jdb
 *    0000000e.jdb
 *    0000000f.jdb
 *    00000010.jdb
 *    00000012.jdb
 *    0000001d.jdb
 *    0000001e.jdb
 *    0000001f.jdb
 * </pre>
 * And the log must be truncated at file 0x1d, offset 0x34567, users should use
 * the following command:
 * <ol>
 * <li> DbTruncateLog -h &lt;envDir&gt; -f 0x1d -o 0x34567 </li>
 * </ol>
 */
public class DbTruncateLog {

    private long truncateFileNum = -1;
    private long truncateOffset = -1;
    private File envHome;

    /**
     * Usage:
     * <pre>
     *  -h environmentDirectory
     *  -f file number. If hex, prefix with "0x"
     *  -o file offset byte. If hex, prefix with "0x"
     * </pre>
     * For example, to truncate a log to file 0xa, offset 0x1223:
     * <pre>
     * DbTruncateLog -h &lt;environmentDir&gt; -f 0xa -o 0x1223
     * </pre>
     */
    public static void main(String[] argv) {
        try {
            DbTruncateLog truncator = new DbTruncateLog();
            truncator.parseArgs(argv);
            truncator.truncateLog();
        } catch (Exception e) {
            e.printStackTrace();
            usage();
            System.exit(1);
        }
    }

    public DbTruncateLog() {
    }
     
    private void parseArgs(String[] argv) {
        int whichArg = 0;
        boolean seenFile = false;
        boolean seenOffset = false;

        while (whichArg < argv.length) {
            String nextArg = argv[whichArg];

            if (nextArg.equals("-h")) {
                whichArg++;
                envHome = new File(CmdUtil.getArg(argv, whichArg));
            } else if (nextArg.equals("-f")) {
                whichArg++;
                truncateFileNum =
                    CmdUtil.readLongNumber(CmdUtil.getArg(argv, whichArg));
                seenFile = true;
            } else if (nextArg.equals("-o")) {
                whichArg++;
                truncateOffset =
                    CmdUtil.readLongNumber(CmdUtil.getArg(argv, whichArg));
                seenOffset = true;
            } else {
                throw new IllegalArgumentException
                    (nextArg + " is not a supported option.");
            }
            whichArg++;
        }

        if (envHome == null) {
            usage();
            System.exit(1);
        }

        if ((!seenFile) || (!seenOffset)) {
            usage();
            System.exit(1);
        }
    }

    private void truncateLog() 
        throws IOException {
        
        truncateLog(envHome, truncateFileNum, truncateOffset);
    }

    /**
     * @hidden
     * Truncate the JE log to the given file and offset. For unit tests.
     */
    public void truncateLog(File env, 
                            long truncFileNum, 
                            long truncOffset) 
        throws IOException {

        /* Make a read/write environment */
        EnvironmentImpl envImpl =
            CmdUtil.makeUtilityEnvironment(env, false);
        
        /* Go through the file manager to get the JE file. Truncate. */
        envImpl.getFileManager().truncateLog(truncFileNum, truncOffset);

        envImpl.close();
    }

    private static void usage() {
        System.out.println("Usage: " +
                           CmdUtil.getJavaCommand(DbTruncateLog.class));
        System.out.println("                 -h <environment home>");
        System.out.println("                 -f <file number, in hex>");
        System.out.println("                 -o <offset, in hex>");
        System.out.println("JE log will be deleted starting from the " +
                           "position presented by the file number and " + 
                           "offset to the end, inclusive.");
    }
}
