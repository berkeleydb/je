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
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.utilint.LogVerifier;

/**
 * Verifies the checksums in a {@link ReadableByteChannel} for a log file in a
 * JE {@link Environment}.  This class is similar to the {@link
 * LogVerificationInputStream} class, but permits using NIO channels and direct
 * buffers to provide better copying performance.
 *
 * <p>This {@code ReadableByteChannel} reads input from some other given {@code
 * ReadableByteChannel}, and verifies checksums while reading.  Its primary
 * intended use is to verify log files that are being copied as part of a
 * programmatic backup.  It is critical that invalid files are not added to a
 * backup set, since then both the live environment and the backup will be
 * invalid.
 *
 * <p>The following example verifies log files as they are being copied.  The
 * {@link DbBackup} class should normally be used to obtain the array of files
 * to be copied.
 *
 * <!-- NOTE: Whenever the method below is changed, update the copy in
 * VerifyLogTest to match, so that it will be tested. -->
 *
 * <pre>
 *  void copyFilesNIO(final Environment env,
 *                    final String[] fileNames,
 *                    final File destDir,
 *                    final int bufSize)
 *      throws IOException, DatabaseException {
 *
 *      final File srcDir = env.getHome();
 *
 *      for (final String fileName : fileNames) {
 *
 *          final File destFile = new File(destDir, fileName);
 *          final FileOutputStream fos = new FileOutputStream(destFile);
 *          final FileChannel foc = fos.getChannel();
 *
 *          final File srcFile = new File(srcDir, fileName);
 *          final FileInputStream fis = new FileInputStream(srcFile);
 *          final FileChannel fic = fis.getChannel();
 *          final LogVerificationReadableByteChannel vic =
 *              new LogVerificationReadableByteChannel(env, fic, fileName);
 *
 *          final ByteBuffer buf = ByteBuffer.allocateDirect(bufSize);
 *
 *          try {
 *              while (true) {
 *                  final int len = vic.read(buf);
 *                  if (len &lt; 0) {
 *                      break;
 *                  }
 *                  buf.flip();
 *                  foc.write(buf);
 *                  buf.clear();
 *              }
 *          } finally {
 *              fos.close();
 *              vic.close();
 *          }
 *      }
 *  }
 * </pre>
 *
 * <p>It is important to read the entire underlying input stream until the
 * end-of-file is reached to detect incomplete entries at the end of the log
 * file.
 *
 * @see DbBackup
 * @see DbVerifyLog
 * @see LogVerificationInputStream
 */
public class LogVerificationReadableByteChannel
        implements ReadableByteChannel {

    private static final int TEMP_SIZE = 8192;
    private final ReadableByteChannel channel;
    private final LogVerifier verifier;
    private byte[] tempArray;

    /**
     * Creates a verification input stream.
     *
     * @param env the {@code Environment} associated with the log
     *
     * @param channel the underlying {@code ReadableByteChannel} for the log to
     * be read
     *
     * @param fileName the file name of the input stream, for reporting in the
     * {@code LogVerificationException}.  This should be a simple file name of
     * the form {@code NNNNNNNN.jdb}, where NNNNNNNN is the file number in
     * hexadecimal format.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs
     */
    public LogVerificationReadableByteChannel(
        final Environment env,
        final ReadableByteChannel channel,
        final String fileName) {

        this(DbInternal.getNonNullEnvImpl(env), channel, fileName);
    }

    /**
     * Creates a verification input stream.
     *
     * @param envImpl the {@code EnvironmentImpl} associated with the log
     *
     * @param channel the underlying {@code ReadableByteChannel} for the log to
     * be read
     *
     * @param fileName the file name of the input stream, for reporting in the
     * {@code LogVerificationException}.  This should be a simple file name of
     * the form {@code NNNNNNNN.jdb}, where NNNNNNNN is the file number in
     * hexadecimal format.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @hidden
     */
    public LogVerificationReadableByteChannel(
        final EnvironmentImpl envImpl,
        final ReadableByteChannel channel,
        final String fileName) {

        this.channel = channel;
        verifier = new LogVerifier(envImpl, fileName);
    }

    /**
     * {@inheritDoc}
     *
     * <p>This method reads the underlying {@code ReadableByteChannel} and
     * verifies the contents of the stream.
     *
     * @throws LogVerificationException if a checksum cannot be verified or a
     * log entry is determined to be invalid by examining its contents
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs
     */
    @Override
    public synchronized int read(final ByteBuffer buffer)
        throws IOException {

        final int start = buffer.position();
        final int count = channel.read(buffer);
        if (count < 0) {
            verifier.verifyAtEof();
        } else {
            if (buffer.hasArray()) {
                verifier.verify(buffer.array(), buffer.arrayOffset() + start,
                                count);
            } else {
                if (tempArray == null) {
                    tempArray = new byte[TEMP_SIZE];
                }
                buffer.position(start);
                int len = count;
                while (len > 0) {
                    final int chunk = Math.min(len, TEMP_SIZE);
                    buffer.get(tempArray, 0, chunk);
                    verifier.verify(tempArray, 0, chunk);
                    len -= chunk;
                }
            }
        }
        return count;
    }

    /**
     * {@inheritDoc}
     *
     * <p>This method calls {@code close} on the underlying channel.
     */
    @Override
    synchronized public void close()
        throws IOException {

        channel.close();
    }

    /**
     * {@inheritDoc}
     *
     * <p>This method calls {@code isOpen} on the underlying channel.
     */
    @Override
    public boolean isOpen() {
        return channel.isOpen();
    }
}
