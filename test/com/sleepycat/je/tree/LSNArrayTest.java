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

package com.sleepycat.je.tree;

import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.util.test.TestBase;

public class LSNArrayTest extends TestBase {
    private static final int N_ELTS = 128;

    private IN theIN;

    @Before
    public void setUp() 
        throws Exception {
        
        super.setUp();
        theIN = new IN();
    }

    @Test
    public void testPutGetElement() {
        doTest(N_ELTS);
    }

    @Test
    public void testOverflow() {
        doTest(N_ELTS << 2);
    }

    @Test
    public void testFileOffsetGreaterThan3Bytes() {
        theIN.initEntryLsn(10);
        theIN.setLsnInternal(0, 0xfffffe);
        assertTrue(theIN.getLsn(0) == 0xfffffe);
        assertTrue(theIN.getEntryLsnByteArray() != null);
        assertTrue(theIN.getEntryLsnLongArray() == null);
        theIN.setLsnInternal(1, 0xffffff);
        assertTrue(theIN.getLsn(1) == 0xffffff);
        assertTrue(theIN.getEntryLsnLongArray() != null);
        assertTrue(theIN.getEntryLsnByteArray() == null);

        theIN.initEntryLsn(10);
        theIN.setLsnInternal(0, 0xfffffe);
        assertTrue(theIN.getLsn(0) == 0xfffffe);
        assertTrue(theIN.getEntryLsnByteArray() != null);
        assertTrue(theIN.getEntryLsnLongArray() == null);
        theIN.setLsnInternal(1, 0xffffff + 1);
        assertTrue(theIN.getLsn(1) == 0xffffff + 1);
        assertTrue(theIN.getEntryLsnLongArray() != null);
        assertTrue(theIN.getEntryLsnByteArray() == null);
    }

    private void doTest(int nElts) {
        theIN.initEntryLsn(nElts);
        for (int i = nElts - 1; i >= 0; i--) {
            long thisLsn = DbLsn.makeLsn(i, i);
            theIN.setLsnInternal(i, thisLsn);
            if (theIN.getLsn(i) != thisLsn) {
                System.out.println(i + " found: " +
                                   DbLsn.toString(theIN.getLsn(i)) +
                                   " expected: " +
                                   DbLsn.toString(thisLsn));
            }
            assertTrue(theIN.getLsn(i) == thisLsn);
        }

        for (int i = 0; i < nElts; i++) {
            long thisLsn = DbLsn.makeLsn(i, i);
            theIN.setLsn(i, thisLsn);
            assertTrue(theIN.getLsn(i) == thisLsn);
        }
    }
}
