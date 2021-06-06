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
import java.io.PrintStream;
import java.text.DateFormat;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.Map;
import java.util.NavigableSet;
import java.util.SortedMap;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.JEVersion;
import com.sleepycat.je.cleaner.ExpirationProfile;
import com.sleepycat.je.cleaner.ExpirationTracker;
import com.sleepycat.je.cleaner.FileProcessor;
import com.sleepycat.je.cleaner.FileSummary;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.UtilizationFileReader;
import com.sleepycat.je.utilint.CmdUtil;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.FormatUtil;
import com.sleepycat.je.utilint.Pair;

/**
 * DbSpace displays the disk space utilization for an environment.
 * <pre>
 * usage: java { com.sleepycat.je.util.DbSpace |
 *               -jar je-&lt;version&gt;.jar DbSpace }
 *          -h &lt;dir&gt;# environment home directory
 *         [-q]     # quiet, print grand totals only
 *         [-u]     # sort by average utilization
 *         [-d]     # dump file summary details
 *         [-r]     # recalculate utilization (expensive)
 *         [-R]     # recalculate expired data (expensive)
 *         [-s]     # start file number or LSN, in hex
 *         [-e]     # end file number or LSN, in hex
 *         [-t]     # time for calculating expired data
 *                  #   format: yyyy-MM-dd'T'HHZ
 *                  #  example: 2016-03-09T22-0800
 *         [-V]     # print JE version number
 * </pre>
 */
public class DbSpace {

    private static final String DATE_FORMAT = "yyyy-MM-dd'T'HHZ";
    private static final String DATE_EXAMPLE = "2016-03-17T22-0800";

    private static final String USAGE =
        "usage: " + CmdUtil.getJavaCommand(DbSpace.class) + "\n" +
        "       -h <dir> # environment home directory\n" +
        "       [-q]     # quiet, print grand totals only\n" +
        "       [-u]     # sort by average utilization\n" +
        "       [-d]     # dump file summary details\n" +
        "       [-r]     # recalculate utilization (expensive)\n" +
        "       [-R]     # recalculate expired data (expensive)\n" +
        "       [-s]     # start file number or LSN, in hex\n" +
        "       [-e]     # end file number or LSN, in hex\n" +
        "       [-t]     # time for calculating expired data,\n" +
        "                #   format: " + DATE_FORMAT + "\n" +
        "                #  example: " + DATE_EXAMPLE + "\n" +
        "       [-V]     # print JE version number";

    public static void main(String argv[])
        throws Exception {

        DbSpace space = new DbSpace();
        space.parseArgs(argv);

        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setReadOnly(true);
        Environment env = new Environment(space.envHome, envConfig);
        space.initEnv(DbInternal.getNonNullEnvImpl(env));

        try {
            space.print(System.out);
            System.exit(0);
        } catch (Throwable e) {
            e.printStackTrace(System.err);
            System.exit(1);
        } finally {
            try {
                env.close();
            } catch (Throwable e) {
                e.printStackTrace(System.err);
                System.exit(1);
            }
        }
    }

    private File envHome = null;
    private EnvironmentImpl envImpl;
    private boolean quiet = false;
    private boolean sorted = false;
    private boolean details = false;
    private boolean doRecalcUtil = false;
    private boolean doRecalcExpired = false;
    private long startLsn = DbLsn.NULL_LSN;
    private long finishLsn = DbLsn.NULL_LSN;
    private long targetTime = System.currentTimeMillis();

    private DbSpace() {
    }

    /**
     * Creates a DbSpace object for calculating utilization using an open
     * Environment.
     */
    public DbSpace(Environment env,
                   boolean quiet,
                   boolean details,
                   boolean sorted) {
        this(DbInternal.getNonNullEnvImpl(env), quiet, details, sorted);
    }

    /**
     * For internal use only.
     * @hidden
     */
    public DbSpace(EnvironmentImpl envImpl,
                   boolean quiet,
                   boolean details,
                   boolean sorted) {
        initEnv(envImpl);
        this.quiet = quiet;
        this.details = details;
        this.sorted = sorted;
    }

    private void initEnv(EnvironmentImpl envImpl) {
        this.envImpl = envImpl;
    }

    private void printUsage(String msg) {
        if (msg != null) {
            System.err.println(msg);
        }
        System.err.println(USAGE);
        System.exit(-1);
    }

    private void parseArgs(String argv[]) {

        int argc = 0;
        int nArgs = argv.length;

        if (nArgs == 0) {
            printUsage(null);
            System.exit(0);
        }

        while (argc < nArgs) {
            String thisArg = argv[argc++];
            if (thisArg.equals("-q")) {
                quiet = true;
            } else if (thisArg.equals("-u")) {
                sorted = true;
            } else if (thisArg.equals("-d")) {
                details = true;
            } else if (thisArg.equals("-r")) {
                doRecalcUtil = true;
            } else if (thisArg.equals("-R")) {
                doRecalcExpired = true;
            } else if (thisArg.equals("-V")) {
                System.out.println(JEVersion.CURRENT_VERSION);
                System.exit(0);
            } else if (thisArg.equals("-h")) {
                if (argc < nArgs) {
                    envHome = new File(argv[argc++]);
                } else {
                    printUsage("-h requires an argument");
                }
            } else if (thisArg.equals("-s")) {
                if (argc < nArgs) {
                    startLsn = CmdUtil.readLsn(argv[argc++]);
                } else {
                    printUsage("-s requires an argument");
                }
            } else if (thisArg.equals("-e")) {
                if (argc < nArgs) {
                    finishLsn = CmdUtil.readLsn(argv[argc++]);
                } else {
                    printUsage("-e requires an argument");
                }
            } else if (thisArg.equals("-t")) {
                if (argc < nArgs) {
                    String s = argv[argc++];
                    DateFormat format = new SimpleDateFormat(DATE_FORMAT);
                    ParsePosition pp = new ParsePosition(0);
                    Date date = format.parse(s, pp);
                    if (date != null) {
                        targetTime = date.getTime();
                    } else {
                        printUsage(
                            "-t doesn't match format: " + DATE_FORMAT +
                            " example: " + DATE_EXAMPLE);
                    }
                } else {
                    printUsage("-t requires an argument");
                }
            }
        }

        if (envHome == null) {
            printUsage("-h is a required argument");
        }

        if (doRecalcUtil && doRecalcExpired) {
            printUsage("-r and -R cannot both be used");
        }
    }

    /**
     * Sets the recalculation property, which if true causes a more expensive
     * recalculation of utilization to be performed for debugging purposes.
     * This property is false by default.
     */
    public void setRecalculate(boolean recalc) {
        this.doRecalcUtil = recalc;
    }

    /**
     * Sets the start file number, which is a lower bound on the range of
     * files for which utilization is reported and (optionally) recalculated.
     * By default there is no lower bound.
     */
    public void setStartFile(long startFile) {
        this.startLsn = startFile;
    }

    /**
     * Sets the ending file number, which is an upper bound on the range of
     * files for which utilization is reported and (optionally) recalculated.
     * By default there is no upper bound.
     */
    public void setEndFile(long endFile) {
        this.finishLsn = endFile;
    }

    /**
     * Sets the time for calculating expired data.
     */
    public void setTime(long time) {
        targetTime = time;
    }

    /**
     * Calculates utilization and prints a report to the given output stream.
     */
    public void print(PrintStream out)
        throws DatabaseException {

        long startFile = (startLsn != DbLsn.NULL_LSN) ?
            DbLsn.getFileNumber(startLsn) : 0;

        long finishFile = (finishLsn != DbLsn.NULL_LSN) ?
            DbLsn.getFileNumber(finishLsn) : Long.MAX_VALUE;

        SortedMap<Long,FileSummary> map =
            envImpl.getUtilizationProfile().getFileSummaryMap(true).
            subMap(startFile, finishFile);

        map.keySet().removeAll(
            envImpl.getCleaner().getFileSelector().getInProgressFiles());

        Map<Long, FileSummary> recalcMap =
            doRecalcUtil ?
            UtilizationFileReader.calcFileSummaryMap(
                envImpl, startLsn, finishLsn) : null;

        ExpirationProfile expProfile =
            new ExpirationProfile(envImpl.getExpirationProfile());

        expProfile.refresh(targetTime);

        int fileIndex = 0;

        Summary totals = new Summary();
        Summary[] summaries = null;

        if (!quiet) {
            summaries = new Summary[map.size()];
        }

        for (Map.Entry<Long, FileSummary> entry : map.entrySet()) {

            Long fileNum = entry.getKey();
            FileSummary fs = entry.getValue();

            FileSummary recalcFs = null;

            if (recalcMap != null) {
                recalcFs = recalcMap.get(fileNum);
            }

            int expiredSize = expProfile.getExpiredBytes(fileNum);
            int recalcExpiredSize = -1;
            ExpirationTracker expTracker = null;

            if (doRecalcExpired) {

                FileProcessor fileProcessor =
                    envImpl.getCleaner().createProcessor();

                expTracker = fileProcessor.countExpiration(fileNum);
                recalcExpiredSize = expTracker.getExpiredBytes(targetTime);
            }

            Summary summary = new Summary(
                fileNum, fs, recalcFs, expiredSize, recalcExpiredSize);

            if (summaries != null) {
                summaries[fileIndex] = summary;
            }

            if (details) {

                out.println(
                    "File 0x" + Long.toHexString(fileNum) +
                    " expired: " + expiredSize +
                    " histogram: " + expProfile.toString(fileNum) +
                    " " + fs);

                if (recalcMap != null) {
                    out.println(
                        "Recalc util 0x" + Long.toHexString(fileNum) +
                        " " + recalcFs);
                }

                if (expTracker != null) {
                    out.println(
                        "Recalc expiration 0x" + Long.toHexString(fileNum) +
                        " recalcExpired: " + recalcExpiredSize +
                        " recalcHistogram: " + expTracker.toString());
                }
            }

            totals.add(summary);
            fileIndex += 1;
        }

        if (details) {
            out.println();
        }

        out.println(
            doRecalcExpired ? Summary.RECALC_EXPIRED_HEADER :
                (doRecalcUtil ? Summary.RECALC_HEADER : Summary.HEADER));

        if (summaries != null) {
            if (sorted) {
                Arrays.sort(summaries, new Comparator<Summary>() {
                    public int compare(Summary s1, Summary s2) {
                        return s1.avgUtilization() - s2.avgUtilization();
                    }
                });
            }
            for (Summary summary : summaries) {
                summary.print(out);
            }
        }

        totals.print(out);

        if (totals.expiredSize > 0) {

            DateFormat format = new SimpleDateFormat(DATE_FORMAT);

            out.format(
                "%nAs of %s, %,d kB are expired, resulting in the%n" +
                "differences between minimum and maximum utilization.%n",

            format.format(targetTime), totals.expiredSize / 1024);
        }

        Pair<Long, NavigableSet<Long>> reservedFileInfo =
            envImpl.getFileProtector().getReservedFileInfo();

        long reservedSize = reservedFileInfo.first();
        NavigableSet<Long> reservedFiles = reservedFileInfo.second();
        boolean printReservedFiles = !quiet && !reservedFiles.isEmpty();

        out.format(
            "%n%,d kB are used by additional reserved files%s%n",
            reservedSize / 1024,
            printReservedFiles ? ":" : ".");

        if (printReservedFiles) {
            out.println(FormatUtil.asHexString(reservedFiles));
        }
    }

    private class Summary {

        static final String HEADER =
            "                      % Utilized\n" +
            "  File    Size (kB)  Avg  Min  Max  \n" +
            "--------  ---------  ---- ---  ---";
          // 12345678  123456789  123  123  123
          //         12         12   12   12
          // TOTALS:

        static final String RECALC_HEADER =
            "                      % Utilized    Recalculated\n" +
            "  File    Size (kB)  Avg  Min  Max  Avg  Min  Max\n" +
            "--------  ---------  ---  ---  ---  ---  ---  ---";
          // 12345678  123456789  123  123  123  123  123  123
          //         12         12   12   12   12   12   12
          // TOTALS:

        static final String RECALC_EXPIRED_HEADER =
            "                      % Utilized    w/Expiration\n" +
            "  File    Size (kB)  Avg  Min  Max  Recalculated\n" +
            "--------  ---------  ---  ---  ---  ------------";
          // 12345678  123456789  123  123  123      123
          //         12         12   12   12   123456
          // TOTALS:

        Long fileNum;
        long totalSize;
        long obsoleteSize;
        long recalcObsoleteSize;
        long expiredSize;
        long recalcExpiredSize;

        Summary() {}

        Summary(Long fileNum,
                FileSummary summary,
                FileSummary recalcSummary,
                int expiredSize,
                int recalcExpiredSize) {
            this.fileNum = fileNum;
            totalSize = summary.totalSize;
            obsoleteSize = summary.getObsoleteSize();
            if (recalcSummary != null) {
                recalcObsoleteSize = recalcSummary.getObsoleteSize();
            }
            this.expiredSize = Math.min(expiredSize, totalSize);
            this.recalcExpiredSize = Math.min(recalcExpiredSize, totalSize);
        }

        void add(Summary o) {
            totalSize += o.totalSize;
            obsoleteSize += o.obsoleteSize;
            recalcObsoleteSize += o.recalcObsoleteSize;
            expiredSize += o.expiredSize;
            recalcExpiredSize += o.recalcExpiredSize;
        }

        void print(PrintStream out) {

            if (fileNum != null) {
                pad(out, Long.toHexString(fileNum.longValue()), 8, '0');
            } else {
                out.print(" TOTALS ");
            }

            int kb = (int) (totalSize / 1024);

            out.print("  ");
            pad(out, Integer.toString(kb), 9, ' ');
            out.print("  ");
            pad(out, Integer.toString(avgUtilization()), 3, ' ');
            out.print("  ");
            pad(out, Integer.toString(minUtilization()), 3, ' ');
            out.print("  ");
            pad(out, Integer.toString(maxUtilization()), 3, ' ');

            if (doRecalcExpired) {

                out.print("      ");
                pad(out, Integer.toString(expRecalcUtilization()), 3, ' ');

            } else if (doRecalcUtil) {

                out.print("  ");
                pad(out, Integer.toString(avgRecalcUtilization()), 3, ' ');
                out.print("  ");
                pad(out, Integer.toString(minRecalcUtilization()), 3, ' ');
                out.print("  ");
                pad(out, Integer.toString(maxRecalcUtilization()), 3, ' ');
            }

            out.println();
        }

        int avgUtilization() {
            return (minUtilization() + maxUtilization()) / 2;
        }

        int minUtilization() {
            return minUtilization(obsoleteSize, expiredSize);
        }

        int maxUtilization() {
            return maxUtilization(obsoleteSize, expiredSize);
        }

        int expRecalcUtilization() {
            return minUtilization(obsoleteSize, recalcExpiredSize);
        }

        int avgRecalcUtilization() {
            return (minRecalcUtilization() + maxRecalcUtilization()) / 2;
        }

        int minRecalcUtilization() {
            return minUtilization(recalcObsoleteSize, expiredSize);
        }

        int maxRecalcUtilization() {
            return maxUtilization(recalcObsoleteSize, expiredSize);
        }

        private int minUtilization(long obsolete, long expired) {
            return FileSummary.utilization(
                Math.min(obsolete + expired, totalSize),
                totalSize);
        }

        private int maxUtilization(long obsolete, long expired) {
            return FileSummary.utilization(
                Math.max(obsolete, expired),
                totalSize);
        }

        private void pad(PrintStream out,
                         String val,
                         int digits,
                         char  padChar) {
            int padSize = digits - val.length();
            for (int i = 0; i < padSize; i += 1) {
                out.print(padChar);
            }
            out.print(val);
        }
    }
}
