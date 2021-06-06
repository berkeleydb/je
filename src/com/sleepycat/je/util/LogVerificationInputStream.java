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

import java.io.IOException;
import java.io.InputStream;

import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.utilint.LogVerifier;

/**
 * Verifies the checksums in an {@code InputStream} for a log file in a JE
 * {@code Environment}.
 *
 * <p>This {@code InputStream} reads input from some other given {@code
 * InputStream}, and verifies checksums while reading.  Its primary intended
 * use is to verify log files that are being copied as part of a programmatic
 * backup.  It is critical that invalid files are not added to a backup set,
 * since then both the live environment and the backup will be invalid.</p>
 *
 * <p>The following example verifies log files as they are being copied.  The
 * {@link DbBackup} class should normally be used to obtain the array of files
 * to be copied.</p>
 *
 * <!-- NOTE: Whenever the method below is changed, update the copy in
 * VerifyLogTest to match, so that it will be tested. -->
 *
 * <pre>
 *  void copyFiles(final Environment env,
 *                 final String[] fileNames,
 *                 final File destDir,
 *                 final int bufSize)
 *      throws IOException, DatabaseException {
 *
 *      final File srcDir = env.getHome();
 *
 *      for (final String fileName : fileNames) {
 *
 *          final File destFile = new File(destDir, fileName);
 *          final FileOutputStream fos = new FileOutputStream(destFile);
 *
 *          final File srcFile = new File(srcDir, fileName);
 *          final FileInputStream fis = new FileInputStream(srcFile);
 *          final LogVerificationInputStream vis =
 *              new LogVerificationInputStream(env, fis, fileName);
 *
 *          final byte[] buf = new byte[bufSize];
 *
 *          try {
 *              while (true) {
 *                  final int len = vis.read(buf);
 *                  if (len &lt; 0) {
 *                      break;
 *                  }
 *                  fos.write(buf, 0, len);
 *              }
 *          } finally {
 *              fos.close();
 *              vis.close();
 *          }
 *      }
 *  }
 * </pre>
 *
 * <p>It is important to read the entire underlying input stream until the
 * end-of-file is reached to detect incomplete entries at the end of the log
 * file.</p>
 *
 * <p>Note that {@code mark} and {@code reset} are not supported and {@code
 * markSupported} returns false.  The default {@link InputStream}
 * implementation of these methods is used.</p>
 *
 * @see DbBackup
 * @see DbVerifyLog
 */
public class LogVerificationInputStream extends InputStream {

    private static final int SKIP_BUF_SIZE = 2048;

    private final InputStream in;
    private final LogVerifier verifier;
    private byte[] skipBuf;

    /**
     * Creates a verification input stream.
     *
     * @param env the {@code Environment} associated with the log.
     *
     * @param in the underlying {@code InputStream} for the log to be read.
     *
     * @param fileName the file name of the input stream, for reporting in the
     * {@code LogVerificationException}.  This should be a simple file name of
     * the form {@code NNNNNNNN.jdb}, where NNNNNNNN is the file number in
     * hexadecimal format.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     */
    public LogVerificationInputStream(final Environment env,
                                      final InputStream in,
                                      final String fileName) {
        this(DbInternal.getNonNullEnvImpl(env), in, fileName, -1L);
    }

    /**
     * Internal constructor.  If fileNum is less than zero, it is derived from
     * fileName.
     */
    LogVerificationInputStream(final EnvironmentImpl envImpl,
                               final InputStream in,
                               final String fileName,
                               final long fileNum) {
        verifier = new LogVerifier(envImpl, fileName, fileNum);
        this.in = in;
    }

    /**
     * {@inheritDoc}
     *
     * <p>This method reads the underlying {@code InputStream} and verifies the
     * contents of the stream.</p>
     *
     * @throws LogVerificationException if a checksum cannot be verified or a
     * log entry is determined to be invalid by examining its contents.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     */
    @Override
    public int read()
        throws IOException {

        /*
         * This method will rarely, if ever, be called when reading a file, so
         * allocating a new byte array is not a performance issue and is the
         * simplest approach.
         */
        final byte[] b = new byte[1];
        final int lenRead = read(b, 0, 1);
        return (lenRead <= 0) ? lenRead : (b[0] & 0xff);
    }

    /**
     * {@inheritDoc}
     *
     * <p>This method reads the underlying {@code InputStream} and verifies the
     * contents of the stream.</p>
     *
     * @throws LogVerificationException if a checksum cannot be verified or a
     * log entry is determined to be invalid by examining its contents.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     */
    @Override
    public int read(final byte b[])
        throws IOException {

        return read(b, 0, b.length);
    }

    /**
     * {@inheritDoc}
     *
     * <p>This method reads the underlying {@code InputStream} and verifies the
     * contents of the stream.</p>
     *
     * @throws LogVerificationException if a checksum cannot be verified or a
     * log entry is determined to be invalid by examining its contents.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     */
    @Override
    public int read(final byte b[], final int off, final int len)
        throws IOException {

        final int lenRead = in.read(b, off, len);
        if (lenRead <= 0) {
            if (lenRead < 0) {
                verifier.verifyAtEof();
            }
            return lenRead;
        }

        verifier.verify(b, off, lenRead);

        return lenRead;
    }

    /**
     * {@inheritDoc}
     *
     * <p>This method reads the underlying {@code InputStream} in order to
     * skip the required number of bytes and verifies the contents of the
     * stream.  A temporary buffer is allocated lazily for reading.</p>
     *
     * @throws LogVerificationException if a checksum cannot be verified or a
     * log entry is determined to be invalid by examining its contents.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     */
    @Override
    public long skip(final long bytesToSkip)
        throws IOException {

        if (bytesToSkip <= 0) {
            return 0;
        }

        /*
         * Like InputStream.skip, we lazily allocate a skip buffer.  We must
         * read the data in order to validate the checksum.  Unlike the
         * InputStream.skip implementation, we cannot use a static buffer
         * because we do process the data and cannot allow multiple threads to
         * share the same buffer.
         */
        if (skipBuf == null) {
            skipBuf = new byte[SKIP_BUF_SIZE];
        }

        long remaining = bytesToSkip;
        while (remaining > 0) {
            final int lenRead = read
                (skipBuf, 0, (int) Math.min(SKIP_BUF_SIZE, remaining));
            if (lenRead < 0) {
                break;
            }
            remaining -= lenRead;
        }

        return bytesToSkip - remaining;
    }

    /**
     * {@inheritDoc}
     *
     * <p>This method simply performs <code>in.available()</code>.
     */
    @Override
    public int available()
        throws IOException {

        return in.available();
    }

    /**
     * {@inheritDoc}
     *
     * <p>This method simply performs {@code in.close()}.
     */
    @Override
    public void close()
        throws IOException {

        in.close();
    }
}
