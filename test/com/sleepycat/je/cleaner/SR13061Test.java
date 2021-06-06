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

package com.sleepycat.je.cleaner;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.sleepycat.bind.tuple.TupleOutput;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.tree.FileSummaryLN;
import com.sleepycat.utilint.StringUtils;

/**
 * Tests that a FileSummaryLN with an old style string key can be read.  When
 * we relied solely on log entry version to determine whether an LN had a
 * string key, we could fail when an old style LN was migrated by the cleaner.
 * In that case the key was still a string key but the log entry version was
 * updated to something greater than zero.  See FileSummaryLN.hasStringKey for
 * details of how we now guard against this.
 */
@RunWith(Parameterized.class)
public class SR13061Test extends CleanerTestBase {

    public SR13061Test(boolean multiSubDir) {
        envMultiSubDir = multiSubDir;
        customName = envMultiSubDir ? "multi-sub-dir" : null ;
    }
    
    @Parameters
    public static List<Object[]> genParams() {
        
        return getEnv(new boolean[] {false, true});
    }

    @Test
    public void testSR13061()
        throws DatabaseException {

        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        if (envMultiSubDir) {
            envConfig.setConfigParam(EnvironmentConfig.LOG_N_DATA_DIRECTORIES,
                                     DATA_DIRS + "");
        }
        env = new Environment(envHome, envConfig);

        FileSummaryLN ln = new FileSummaryLN(new FileSummary());

        /*
         * All of these tests failed before checking that the byte array must
         * be eight bytes for integer keys.
         */
        assertTrue(ln.hasStringKey(stringKey(0)));
        assertTrue(ln.hasStringKey(stringKey(1)));
        assertTrue(ln.hasStringKey(stringKey(12)));
        assertTrue(ln.hasStringKey(stringKey(123)));
        assertTrue(ln.hasStringKey(stringKey(1234)));
        assertTrue(ln.hasStringKey(stringKey(12345)));
        assertTrue(ln.hasStringKey(stringKey(123456)));
        assertTrue(ln.hasStringKey(stringKey(1234567)));
        assertTrue(ln.hasStringKey(stringKey(123456789)));
        assertTrue(ln.hasStringKey(stringKey(1234567890)));

        /*
         * These tests failed before checking that the first byte of the
         * sequence number (in an eight byte key) must not be '0' to '9' for
         * integer keys.
         */
        assertTrue(ln.hasStringKey(stringKey(12345678)));
        assertTrue(ln.hasStringKey(stringKey(12340000)));

        /* These tests are just for good measure. */
        assertTrue(!ln.hasStringKey(intKey(0, 1)));
        assertTrue(!ln.hasStringKey(intKey(1, 1)));
        assertTrue(!ln.hasStringKey(intKey(12, 1)));
        assertTrue(!ln.hasStringKey(intKey(123, 1)));
        assertTrue(!ln.hasStringKey(intKey(1234, 1)));
        assertTrue(!ln.hasStringKey(intKey(12345, 1)));
        assertTrue(!ln.hasStringKey(intKey(123456, 1)));
        assertTrue(!ln.hasStringKey(intKey(1234567, 1)));
        assertTrue(!ln.hasStringKey(intKey(12345678, 1)));
        assertTrue(!ln.hasStringKey(intKey(123456789, 1)));
        assertTrue(!ln.hasStringKey(intKey(1234567890, 1)));
    }

    private byte[] stringKey(long fileNum) {

        try {
            return StringUtils.toUTF8(String.valueOf(fileNum));
        } catch (Exception e) {
            fail(e.toString());
            return null;
        }
    }

    private byte[] intKey(long fileNum, long seqNum) {

        TupleOutput out = new TupleOutput();
        out.writeUnsignedInt(fileNum);
        out.writeUnsignedInt(seqNum);
        return out.toByteArray();
    }
}
