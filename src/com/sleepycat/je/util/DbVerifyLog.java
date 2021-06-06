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
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.JEVersion;
import com.sleepycat.je.ThreadInterruptedException;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.EnvironmentFailureReason;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.FileManager;
import com.sleepycat.je.log.entry.RestoreRequired;
import com.sleepycat.je.util.verify.VerifierUtils;
import com.sleepycat.je.utilint.CmdUtil;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.PropUtil;

/**
 * Verifies the checksums in one or more log files.
 *
 * <p>This class may be instantiated and used programmatically, or used as a
 * command line utility as described below.</p>
 *
 * <pre>
 * usage: java { com.sleepycat.je.util.DbVerifyLog |
 *               -jar je-&lt;version&gt;.jar DbVerifyLog }
 *  [-h &lt;dir&gt;]      # environment home directory
 *  [-s &lt;file&gt;]     # starting (minimum) file number
 *  [-e &lt;file&gt;]     # ending (one past the maximum) file number
 *  [-d &lt;millis&gt;]   # delay in ms between reads (default is zero)
 *  [-V]                  # print JE version number"
 * </pre>
 *
 * <p>All arguments are optional.  The current directory is used if {@code -h}
 * is not specified.  File numbers may be specified in hex (preceded by {@code
 * 0x}) or decimal format.  For convenience when copy/pasting from other
 * output, LSN format (&lt;file&gt;/&lt;offset&gt;) is also allowed.</p>
 */
public class DbVerifyLog {

    private static final String USAGE =
        "usage: " + CmdUtil.getJavaCommand(DbVerifyLog.class) + "\n" +
        "   [-h <dir>]       # environment home directory\n" +
        "   [-s <file>]      # starting (minimum) file number\n" +
        "   [-e <file>]      # ending (one past the maximum) file number\n" +
        "   [-d <millis>]    # delay in ms between reads (default is zero)\n" +
        "   [-V]             # print JE version number";

    private final EnvironmentImpl envImpl;
    private final int readBufferSize;
    private volatile boolean stopVerify = false;

    private long delayMs = 0;

    /**
     * Creates a utility object for verifying the checksums in log files.
     *
     * <p>The read buffer size is {@link
     * EnvironmentConfig#LOG_ITERATOR_READ_SIZE}.</p>
     *
     * @param env the {@code Environment} associated with the log.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     */
    public DbVerifyLog(final Environment env) {
        this(env, 0);
    }

    /**
     * Creates a utility object for verifying log files.
     *
     * @param env the {@code Environment} associated with the log.
     *
     * @param readBufferSize is the buffer size to use.  If a value less than
     * or equal to zero is specified, {@link
     * EnvironmentConfig#LOG_ITERATOR_READ_SIZE} is used.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     */
    public DbVerifyLog(final Environment env, final int readBufferSize) {
        this(DbInternal.getNonNullEnvImpl(env), readBufferSize);
    }

    /**
     * @hidden
     */
    public DbVerifyLog(final EnvironmentImpl envImpl,
                       final int readBufferSize) {
        this.readBufferSize = (readBufferSize > 0) ?
            readBufferSize :
            envImpl.getConfigManager().getInt
                (EnvironmentParams.LOG_ITERATOR_READ_SIZE);
        this.envImpl = envImpl;
    }

    /**
     * Verifies all log files in the environment.
     *
     * @throws LogVerificationException if a checksum cannot be verified or a
     * log entry is determined to be invalid by examining its contents.
     *
     * @throws IOException if an IOException occurs while reading a log file.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     */
    public void verifyAll()
        throws LogVerificationException, IOException {

        /* The same reason with BtreeVerifier.verifyAll. */
        if (stopVerify) {
            return;
        }

        LoggerUtils.envLogMsg(
            Level.INFO, envImpl, "Start verify of data files");

        verify(0, Long.MAX_VALUE);

        LoggerUtils.envLogMsg(
            Level.INFO, envImpl, "End verify of data files");
    }

    /**
     * Verifies the given range of log files in the environment.
     *
     * @param startFile is the lowest numbered log file to be verified.
     *
     * @param endFile is one greater than the highest numbered log file to be
     * verified.
     *
     * @throws LogVerificationException if a checksum cannot be verified or a
     * log entry is determined to be invalid by examining its contents.
     *
     * @throws IOException if an IOException occurs while reading a log file.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     */
    public void verify(final long startFile, final long endFile)
        throws LogVerificationException, IOException {

        try {
            final FileManager fileManager = envImpl.getFileManager();
            final File homeDir = envImpl.getEnvironmentHome();
            final String[] fileNames =
                fileManager.listFileNames(startFile, endFile - 1);
            final ByteBuffer buf = ByteBuffer.allocateDirect(readBufferSize);

            for (final String fileName : fileNames) {
                /*
                 * When env is closed, the current executing dataVerifier task
                 * should be canceled asap. So when env is closed,
                 * setStopVerifyFlag() is called in DataVerifier.shutdown().
                 * Here stopVerify is checked to determine whether dataVerifier
                 * task continues.
                 */
                if (stopVerify) {
                    return;
                }

                final File file = new File(homeDir, fileName);
                /*
                 * If JE enables Cleaner, then it is possible that Cleaner
                 * deletes one or more files, whose fileNum is between
                 * startFile and endFile, during the for-loop. So for each
                 * loop, the current 'file' may be deleted by the Cleaner, then
                 * 'new FileInputStream' will throw FileNotFoundException.
                 *
                 * In addition, now JE has a daemon thread to detect the
                 * unexpected log file deletion. So if FileNotFoundException is
                 * caused by unexpected log deletion, then that daemon thread
                 * will catch this abnormal situation. Here, we just ignore
                 * this exception.
                 */
                FileInputStream fis;
                try {
                    fis = new FileInputStream(file);
                } catch (FileNotFoundException fne) {
                    continue;
                }

                final FileChannel fic = fis.getChannel();
                final LogVerificationReadableByteChannel vic =
                    new LogVerificationReadableByteChannel(envImpl, fic, fileName);

                IOException ioe = null;
                try {
                    while (vic.read(buf) != -1) {
                        buf.clear();

                        /* Return as soon as possible if shutdown. */
                        if (stopVerify) {
                            return;
                        }

                        if (delayMs > 0) {
                            try {
                                Thread.sleep(delayMs);
                            } catch (InterruptedException e) {
                                throw new ThreadInterruptedException(
                                    envImpl, e);
                            }
                        }
                    }
                } catch (IOException e) {
                    ioe = e;
                    throw ioe;
                } finally {

                    try {
                    /*
                     * vic.close aims to close associated channel fic, but
                     * it may be redundant because fis.close also closes fic.
                     */
                        fis.close();
                        vic.close();
                    } catch (IOException e) {
                        if (ioe == null) {
                            throw e;
                        }
                    }
                }
            }
        } catch (LogVerificationException lve) {

            VerifierUtils.createMarkerFileFromException(
                RestoreRequired.FailureType.LOG_CHECKSUM,
                lve,
                envImpl,
                EnvironmentFailureReason.LOG_CHECKSUM);

            throw lve;
        }
    }

    public static void main(String[] argv) {
        try {

            File envHome = new File(".");
            long startFile = 0;
            long endFile = Long.MAX_VALUE;
            long delayMs = 0;

            for (int whichArg = 0; whichArg < argv.length; whichArg += 1) {
                final String nextArg = argv[whichArg];
                if (nextArg.equals("-h")) {
                    whichArg++;
                    envHome = new File(CmdUtil.getArg(argv, whichArg));
                } else if (nextArg.equals("-s")) {
                    whichArg++;
                    String arg = CmdUtil.getArg(argv, whichArg);
                    final int slashOff = arg.indexOf("/");
                    if (slashOff >= 0) {
                        arg = arg.substring(0, slashOff);
                    }
                    startFile = CmdUtil.readLongNumber(arg);
                } else if (nextArg.equals("-e")) {
                    whichArg++;
                    String arg = CmdUtil.getArg(argv, whichArg);
                    final int slashOff = arg.indexOf("/");
                    if (slashOff >= 0) {
                        arg = arg.substring(0, slashOff);
                    }
                    endFile = CmdUtil.readLongNumber(arg);
                } else if (nextArg.equals("-d")) {
                    whichArg++;
                    delayMs =
                        CmdUtil.readLongNumber(CmdUtil.getArg(argv, whichArg));
                } else if (nextArg.equals("-V")) {
                    System.out.println(JEVersion.CURRENT_VERSION);
                    System.exit(0);
                } else {
                    printUsageAndExit("Unknown argument: " + nextArg);
                }
            }

            final EnvironmentImpl envImpl =
                CmdUtil.makeUtilityEnvironment(envHome, true /*readOnly*/);
            final DbVerifyLog verifier = new DbVerifyLog(envImpl, 0);
            /* Set the delay time specified by -d flag. */
            verifier.setReadDelay(delayMs, TimeUnit.MILLISECONDS);
            verifier.verify(startFile, endFile);
            System.exit(0);
        } catch (Throwable e) {
            e.printStackTrace();
            printUsageAndExit(e.toString());
        }
    }

    private static void printUsageAndExit(String msg) {
        if (msg != null) {
            System.err.println(msg);
        }
        System.err.println(USAGE);
        System.exit(1);
    }

    /**
     * Configures the delay between file reads during verification. A delay
     * between reads is needed to allow other JE components, such as HA, to
     * make timely progress.
     *
     * <p>By default there is no read delay (it is zero).</p>
     *
     * <p>Note that when using the {@link EnvironmentConfig#ENV_RUN_VERIFIER
     * background data verifier}, the delay between reads is
     * {@link EnvironmentConfig#VERIFY_LOG_READ_DELAY}.</p>
     *
     * @param delay the delay between reads or zero for no delay.
     *
     * @param unit the {@code TimeUnit} of the delay value. May be
     * null only if delay is zero.
     */
    public void setReadDelay(long delay, TimeUnit unit) {
        delayMs = PropUtil.durationToMillis(delay, unit);
    }

    /**
     * @hidden
     * For internal use only.
     */
    public void setStopVerifyFlag(boolean val) {
        stopVerify = val;
    }
}
