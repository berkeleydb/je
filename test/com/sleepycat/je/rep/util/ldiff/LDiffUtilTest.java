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

package com.sleepycat.je.rep.util.ldiff;

import org.junit.Test;

import com.sleepycat.util.test.TestBase;
import com.sleepycat.utilint.StringUtils;

public class LDiffUtilTest extends TestBase {
    byte[][] al = new byte[][] {
            StringUtils.toUTF8("key1|Value1"),
            StringUtils.toUTF8("key2|Value2"),
            StringUtils.toUTF8("key3|Value3"),
            StringUtils.toUTF8("key4|Value4"),
            StringUtils.toUTF8("key5|Value5"),
            StringUtils.toUTF8("key6|Value6"),
            StringUtils.toUTF8("key7|Value7"),
            StringUtils.toUTF8("key8|Value8"),
            StringUtils.toUTF8("key9|Value9"),
            StringUtils.toUTF8("key10|Value10") };

    @Test
    public void testPlaceHolder() {
        /* 
         * A Junit test will fail if there are no tests cases at all, so
         * here is a placeholder test.
         */
    }

    /* Verifies the basics of the rolling checksum computation. */
    /*
     * public void testgetRollingChksum() { List<byte[]> tlist =
     * Arrays.asList(al); int blockSize = 5; long rsum =
     * LDiffUtil.getRollingChksum(tlist.subList(0, blockSize)); for (int i = 1;
     * (i + blockSize) <= tlist.size(); i++) { int removeIndex = i - 1; int
     * addIndex = removeIndex + blockSize; List<byte[]> list =
     * tlist.subList(removeIndex + 1, addIndex + 1); // The reference value.
     * long ref = LDiffUtil.getRollingChksum(list); // The incrementally
     * computed chksum rsum = LDiffUtil.rollChecksum(rsum, blockSize,
     * LDiffUtil.getXi(al[removeIndex]), LDiffUtil.getXi(al[addIndex]));
     * assertEquals(ref, rsum); // System.err.printf("ref:%x, rsum:%x\n", ref,
     * rsum); } }
     */
}
