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
import java.lang.reflect.Constructor;

import com.sleepycat.je.EnvironmentLockedException;
import com.sleepycat.je.EnvironmentNotFoundException;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.DumpFileReader;
import com.sleepycat.je.log.FileManager;
import com.sleepycat.je.log.LastFileReader;
import com.sleepycat.je.log.PrintFileReader;
import com.sleepycat.je.log.StatsFileReader;
import com.sleepycat.je.log.VLSNDistributionReader;
import com.sleepycat.je.tree.Key;
import com.sleepycat.je.tree.Key.DumpType;
import com.sleepycat.je.utilint.CmdUtil;
import com.sleepycat.je.utilint.DbLsn;

/**
 * Dumps the contents of the log in XML format to System.out.
 *
 * <p>To print an environment log:</p>
 *
 * <pre>
 *      DbPrintLog.main(argv);
 * </pre>
 */
public class DbPrintLog {

    /**
     * Dump a JE log into human readable form.
     */
    public void dump(File envHome,
                     String entryTypes,
                     String txnIds,
                     long startLsn,
                     long endLsn,
                     boolean verbose,
                     boolean stats,
                     boolean repEntriesOnly,
                     boolean csvFormat,
                     boolean forwards,
                     boolean vlsnDistribution,
                     String customDumpReaderClass)
        throws EnvironmentNotFoundException,
               EnvironmentLockedException {

        dump(envHome, entryTypes, "" /*dbIds*/, txnIds, startLsn, endLsn,
            verbose, stats, repEntriesOnly, csvFormat, forwards,
            vlsnDistribution, customDumpReaderClass);
    }

    private void dump(File envHome,
                     String entryTypes,
                     String dbIds,
                     String txnIds,
                     long startLsn,
                     long endLsn,
                     boolean verbose,
                     boolean stats,
                     boolean repEntriesOnly,
                     boolean csvFormat,
                     boolean forwards,
                     boolean vlsnDistribution,
                     String customDumpReaderClass) {
        EnvironmentImpl env =
            CmdUtil.makeUtilityEnvironment(envHome, true);
        FileManager fileManager = env.getFileManager();
        fileManager.setIncludeDeletedFiles(true);
        int readBufferSize =
            env.getConfigManager().getInt
            (EnvironmentParams.LOG_ITERATOR_READ_SIZE);

        /* Configure the startLsn and endOfFileLsn if reading backwards. */
        long endOfFileLsn = DbLsn.NULL_LSN;
        if (startLsn == DbLsn.NULL_LSN && endLsn == DbLsn.NULL_LSN &&
            !forwards) {
            LastFileReader fileReader =
                new LastFileReader(env, readBufferSize);
            while (fileReader.readNextEntry()) {
            }
            startLsn = fileReader.getLastValidLsn();
            endOfFileLsn = fileReader.getEndOfLog();
        }

        try {

            /* 
             * Make a reader. First see if a custom debug class is available,
             * else use the default versions. 
             */
            DumpFileReader reader = null;
            if (customDumpReaderClass != null) {

                reader = getDebugReader(
                    customDumpReaderClass, env, readBufferSize,
                    startLsn, endLsn, endOfFileLsn, entryTypes, txnIds,
                    verbose, repEntriesOnly, forwards);

            } else {
                if (stats) {

                    reader = new StatsFileReader(
                        env, readBufferSize, startLsn, endLsn, endOfFileLsn,
                        entryTypes, dbIds, txnIds, verbose, repEntriesOnly,
                        forwards);

                } else if (vlsnDistribution) {

                    reader = new VLSNDistributionReader(
                        env, readBufferSize, startLsn, endLsn, endOfFileLsn,
                        verbose, forwards);

                } else {

                    reader = new PrintFileReader(
                        env, readBufferSize, startLsn, endLsn, endOfFileLsn,
                        entryTypes, dbIds, txnIds, verbose, repEntriesOnly,
                        forwards);
                }
            }

            /* Enclose the output in a tag to keep proper XML syntax. */
            if (!csvFormat) {
                System.out.println("<DbPrintLog>");
            }

            while (reader.readNextEntry()) {
            }

            reader.summarize(csvFormat);
            if (!csvFormat) {
                System.out.println("</DbPrintLog>");
            }
        } finally {
            env.close();
        }
    }

    /**
     * The main used by the DbPrintLog utility.
     *
     * @param argv An array of command line arguments to the DbPrintLog
     * utility.
     *
     * <pre>
     * usage: java { com.sleepycat.je.util.DbPrintLog | -jar
     * je-&lt;version&gt;.jar DbPrintLog }
     *  -h &lt;envHomeDir&gt;
     *  -s  &lt;start file number or LSN, in hex&gt;
     *  -e  &lt;end file number or LSN, in hex&gt;
     *  -k  &lt;binary|hex|text|obfuscate&gt; (format for dumping the key/data)
     *  -db &lt;targeted db ids, comma separated&gt;
     *  -tx &lt;targeted txn ids, comma separated&gt;
     *  -ty &lt;targeted entry types, comma separated&gt;
     *  -S  show summary of log entries
     *  -SC show summary of log entries in CSV format
     *  -r  only print replicated log entries
     *  -b  scan log backwards. The entire log must be scanned, cannot be used
     *      with -s or -e
     *  -q  if specified, concise version is printed,
     *      default is verbose version
     *  -c  &lt;name of custom dump reader class&gt; if specified, DbPrintLog
     *      will attempt to load a class of this name, which will be used to
     *      process log entries. Used to customize formatting and dumping when
     *      debugging files.
     * </pre>
     *
     * <p>All arguments are optional.  The current directory is used if {@code
     * -h} is not specified.</p>
     */
    public static void main(String[] argv) {
        try {
            int whichArg = 0;
            String entryTypes = null;
            String dbIds = null;
            String txnIds = null;
            long startLsn = DbLsn.NULL_LSN;
            long endLsn = DbLsn.NULL_LSN;
            boolean verbose = true;
            boolean stats = false;
            boolean csvFormat = false;
            boolean repEntriesOnly = false;
            boolean forwards = true;
            String customDumpReaderClass = null;
            boolean vlsnDistribution = false;

            /* Default to looking in current directory. */
            File envHome = new File(".");
            Key.DUMP_TYPE = DumpType.BINARY;

            while (whichArg < argv.length) {
                String nextArg = argv[whichArg];
                if (nextArg.equals("-h")) {
                    whichArg++;
                    envHome = new File(CmdUtil.getArg(argv, whichArg));
                } else if (nextArg.equals("-ty")) {
                    whichArg++;
                    entryTypes = CmdUtil.getArg(argv, whichArg);
                } else if (nextArg.equals("-db")) {
                    whichArg++;
                    dbIds = CmdUtil.getArg(argv, whichArg);
                } else if (nextArg.equals("-tx")) {
                    whichArg++;
                    txnIds = CmdUtil.getArg(argv, whichArg);
                } else if (nextArg.equals("-s")) {
                    whichArg++;
                    startLsn = CmdUtil.readLsn(CmdUtil.getArg(argv, whichArg));
                } else if (nextArg.equals("-e")) {
                    whichArg++;
                    endLsn = CmdUtil.readLsn(CmdUtil.getArg(argv, whichArg));
                } else if (nextArg.equals("-k")) {
                    whichArg++;
                    String dumpType = CmdUtil.getArg(argv, whichArg);
                    if (dumpType.equalsIgnoreCase("text")) {
                        Key.DUMP_TYPE = DumpType.TEXT;
                    } else if (dumpType.equalsIgnoreCase("hex")) {
                        Key.DUMP_TYPE = DumpType.HEX;
                    } else if (dumpType.equalsIgnoreCase("binary")) {
                        Key.DUMP_TYPE = DumpType.BINARY;
                    } else if (dumpType.equalsIgnoreCase("obfuscate")) {
                        Key.DUMP_TYPE = DumpType.OBFUSCATE;
                    } else {
                        System.err.println
                            (dumpType +
                             " is not a supported dump format type.");
                    }
                } else if (nextArg.equals("-q")) {
                    verbose = false;
                } else if (nextArg.equals("-b")) {
                    forwards = false;
                } else if (nextArg.equals("-S")) {
                    stats = true;
                } else if (nextArg.equals("-SC")) {
                    stats = true;
                    csvFormat = true;
                } else if (nextArg.equals("-r")) {
                    repEntriesOnly = true;
                } else if (nextArg.equals("-c")) {
                    whichArg++;
                    customDumpReaderClass = CmdUtil.getArg(argv, whichArg);
                } else if (nextArg.equals("-vd")) {
                    /* 
                     * An unadvertised option which displays vlsn distribution
                     * in a log, for debugging.
                     */
                    vlsnDistribution = true;
                } else {
                    System.err.println
                        (nextArg + " is not a supported option.");
                    usage();
                    System.exit(-1);
                }
                whichArg++;
            }

            /* Don't support scan backwards when -s or -e is enabled. */
            if ((startLsn != DbLsn.NULL_LSN || endLsn != DbLsn.NULL_LSN) &&
                !forwards) {
                throw new UnsupportedOperationException
                    ("Backwards scans are not supported when -s or -e are " +
                     "used. They can only be used against the entire log.");
            }

            DbPrintLog printer = new DbPrintLog();
            printer.dump(envHome, entryTypes, dbIds, txnIds, startLsn, endLsn,
                         verbose, stats, repEntriesOnly, csvFormat, forwards,
                         vlsnDistribution, customDumpReaderClass);

        } catch (Throwable e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
            usage();
            System.exit(1);
        }
    }

    private static void usage() {
        System.out.println("Usage: " +
                           CmdUtil.getJavaCommand(DbPrintLog.class));
        System.out.println(" -h  <envHomeDir>");
        System.out.println(" -s  <start file number or LSN, in hex>");
        System.out.println(" -e  <end file number or LSN, in hex>");
        System.out.println(" -k  <binary|text|hex|obfuscate> " +
                           "(format for dumping the key and data)");
        System.out.println(" -db <targeted db ids, comma separated>");
        System.out.println(" -tx <targeted txn ids, comma separated>");
        System.out.println(" -ty <targeted entry types, comma separated>");
        System.out.println(" -S  show Summary of log entries");
        System.out.println(" -SC show Summary of log entries in CSV format");
        System.out.println(" -r  only print replicated log entries");
        System.out.println(" -b  scan all the log files backwards, don't ");
        System.out.println("     support scan between two log files");
        System.out.println(" -q  if specified, concise version is printed");
        System.out.println("     Default is verbose version.)");
        System.out.println(" -c  <custom dump reader class> if specified, ");
        System.out.println("     attempt to load this class to use for the ");
        System.out.println("     formatting of dumped log entries");
        System.out.println("All arguments are optional");
    }

    /**
     * If a custom dump reader class is specified, we'll use that for 
     * DbPrintLog instead of the regular DumpFileReader. The custom reader must
     * have DumpFileReader as a superclass ancestor. Its constructor must have
     * this signature:
     *
     *  public class FooReader extends DumpFileReader {
     *
     *      public FooReader(EnvironmentImpl env,
     *                       Integer readBufferSize, 
     *                       Long startLsn,
     *                       Long finishLsn,
     *                       Long endOfFileLsn,
     *                       String entryTypes,
     *                       String txnIds,
     *                       Boolean verbose,
     *                       Boolean repEntriesOnly,
     *                       Boolean forwards) 
     *          super(env, readBufferSize, startLsn, finishLsn, endOfFileLsn,
     *                entryTypes, txnIds, verbose, repEntriesOnly, forwards);
     *
     * See com.sleepycat.je.util.TestDumper, on the test side, for an example.
     */
    private DumpFileReader getDebugReader(String customDumpReaderClass,
                                          EnvironmentImpl env,
                                          int readBufferSize,
                                          long startLsn,
                                          long finishLsn,
                                          long endOfFileLsn,
                                          String entryTypes,
                                          String txnIds,
                                          boolean verbose,
                                          boolean repEntriesOnly,
                                          boolean forwards) {
        Class<?> debugClass = null;
        try {
            debugClass = Class.forName(customDumpReaderClass);
        } catch (Exception e) {
            throw new IllegalArgumentException
                ("-c was specified, but couldn't load " +
                 customDumpReaderClass + " ", e);
        }

        Class<?> args[] = { EnvironmentImpl.class,
                            Integer.class,     // readBufferSize
                            Long.class,        // startLsn
                            Long.class,        // finishLsn
                            Long.class,        // endOfFileLsn
                            String.class,      // entryTypes
                            String.class,      // txnIds
                            Boolean.class,     // verbose
                            Boolean.class,     // repEntriesOnly
                            Boolean.class };   // forwards

        DumpFileReader debugReader = null;
        try {
            Constructor<?> con = 
                debugClass.getConstructor(args);
            debugReader = (DumpFileReader) con.newInstance(env,
                                                           readBufferSize,
                                                           startLsn,
                                                           finishLsn,
                                                           endOfFileLsn,
                                                           entryTypes,
                                                           txnIds,
                                                           verbose,
                                                           repEntriesOnly,
                                                           forwards);
        } catch (Exception e) {
            throw new IllegalStateException
                ("-c was specified, but couldn't instantiate " + 
                 customDumpReaderClass + " ", e);
        }

        return debugReader;
    }
}
