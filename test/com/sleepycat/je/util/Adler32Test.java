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

import java.util.Random;
import java.util.zip.Checksum;

import org.junit.Test;

import com.sleepycat.util.test.TestBase;

public class Adler32Test extends TestBase {

    static private int N_ITERS = 1000;

    @Test
    public void testRandomAdler32ByteArray() {
        Checksum javaChecksum = new java.util.zip.Adler32();
        Checksum jeChecksum = new com.sleepycat.je.utilint.Adler32();
        Checksum chunkingChecksum =
            new com.sleepycat.je.utilint.Adler32.ChunkingAdler32(128);
        Random rnd = new Random();
        for (int i = 0; i < N_ITERS; i++) {
            int nBytes = rnd.nextInt(65535);
            byte[] b = new byte[nBytes];
            rnd.nextBytes(b);
            javaChecksum.reset();
            jeChecksum.reset();
            chunkingChecksum.reset();
            javaChecksum.update(b, 0, nBytes);
            jeChecksum.update(b, 0, nBytes);
            chunkingChecksum.update(b, 0, nBytes);
            assertEquals(javaChecksum.getValue(), jeChecksum.getValue());
            assertEquals(javaChecksum.getValue(), chunkingChecksum.getValue());
        }
    }

    public void xtestRandomAdler32ByteArrayPerformance() {
        Checksum javaChecksum = new java.util.zip.Adler32();
        Checksum jeChecksum = new com.sleepycat.je.utilint.Adler32();
        Random rnd = new Random();
        byte[][] baa = new byte[N_ITERS][];
        int[] lengths = new int[N_ITERS];
        long totalBytes = 0;
        for (int i = 0; i < N_ITERS; i++) {
            int nBytes = rnd.nextInt(65535);
            byte[] b = new byte[nBytes];
            baa[i] = b;
            lengths[i] = nBytes;
            totalBytes += nBytes;
            rnd.nextBytes(b);
        }
        long jeChecksumTime =
            measureChecksum(baa, lengths, jeChecksum, false);
        long javaChecksumTime =
            measureChecksum(baa, lengths, javaChecksum, false);
        long jeChecksumTimeByteAtATime =
            measureChecksum(baa, lengths, jeChecksum, true);
        long javaChecksumTimeByteAtATime =
            measureChecksum(baa, lengths, javaChecksum, true);
        System.out.println(N_ITERS + " Iterations, " +
                           totalBytes + " bytes:\n " +
                           javaChecksumTime + " millis. for java\n" +
                           jeChecksumTime + " millis. for je\n" +
                           javaChecksumTimeByteAtATime +
                           " millis. for java byte at a time\n" +
                           jeChecksumTimeByteAtATime +
                           " millis. for je byte at a time");
    }

    private long measureChecksum(byte[][] baa,
                                 int[] lengths,
                                 Checksum cksum,
                                 boolean byteAtATime) {
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < N_ITERS; i++) {
            byte[] b = baa[i];
            int len = lengths[i];
            cksum.reset();
            if (byteAtATime) {
                for (int j = 0; j < len; j++) {
                    cksum.update(b[j]);
                }
            } else {
                cksum.update(b, 0, len);
            }
        }
        long endTime = System.currentTimeMillis();
        return (endTime - startTime);
    }

    @Test
    public void testRandomAdler32SingleBytes() {
        Checksum javaChecksum = new java.util.zip.Adler32();
        Checksum jeChecksum = new com.sleepycat.je.utilint.Adler32();
        Random rnd = new Random();
        for (int i = 0; i < N_ITERS; i++) {
            int nBytes = rnd.nextInt(65535);
            javaChecksum.reset();
            jeChecksum.reset();
            for (int j = 0; j < nBytes; j++) {
                byte b = (byte) (rnd.nextInt(256) & 0xff);
                javaChecksum.update(b);
                jeChecksum.update(b);
            }
            assertEquals(javaChecksum.getValue(), jeChecksum.getValue());
        }
    }
}
