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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;
import com.sleepycat.utilint.StringUtils;

public class KeyTest extends TestBase {
    private File envHome;
    private Environment env;

    @Override
    @Before
    public void setUp() 
        throws Exception {

        envHome = SharedTestUtils.getTestDir();
        super.setUp();
    }

    @After
    @Override
    public void tearDown() {
        if (env != null) {
            try {
                env.close();
            } catch (DatabaseException E) {
            }
        }
    }

    @Test
    public void testKeyPrefixer() {
        assertEquals("aaa", makePrefix("aaaa", "aaab"));
        assertEquals("a", makePrefix("abaa", "aaab"));
        assertNull(makePrefix("baaa", "aaab"));
        assertEquals("aaa", makePrefix("aaa", "aaa"));
        assertEquals("aaa", makePrefix("aaa", "aaab"));
    }

    private String makePrefix(String k1, String k2) {
        byte[] ret = Key.createKeyPrefix(StringUtils.toUTF8(k1),
                                         StringUtils.toUTF8(k2));
        if (ret == null) {
            return null;
        }
        return StringUtils.fromUTF8(ret);
    }

    @Test
    public void testKeyPrefixSubsetting() {
        keyPrefixSubsetTest("aaa", "aaa", true);
        keyPrefixSubsetTest("aa", "aaa", true);
        keyPrefixSubsetTest("aaa", "aa", false);
        keyPrefixSubsetTest("", "aa", false);
        keyPrefixSubsetTest(null, "aa", false);
        keyPrefixSubsetTest("baa", "aa", false);
    }

    private void keyPrefixSubsetTest(String keyPrefix,
                                     String newKey,
                                     boolean expect) {
        try {
            EnvironmentConfig envConfig = TestUtils.initEnvConfig();
            envConfig.setAllowCreate(true);
            env = new Environment(envHome, envConfig);
            byte[] keyPrefixBytes =
                (keyPrefix == null ? null : StringUtils.toUTF8(keyPrefix));
            byte[] newKeyBytes = StringUtils.toUTF8(newKey);
            DatabaseConfig dbConf = new DatabaseConfig();
            dbConf.setKeyPrefixing(true);
            EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);
            DatabaseImpl databaseImpl =
                new DatabaseImpl(null,
                                 "dummy", new DatabaseId(10), envImpl, dbConf);
            IN in = new IN(databaseImpl, null, 10, 10);
            in.setKeyPrefix(keyPrefixBytes);
            boolean result = compareToKeyPrefix(in, newKeyBytes);
            assertTrue(result == expect);
        } catch (Exception E) {
            E.printStackTrace();
            fail("caught " + E);
        } finally {
            env.close();
            env = null;
        }
    }

    /*
     * Returns whether the current prefix (if any) is also a prefix of a
     * given newKey.
     *
     * This has default protection for the unit tests.
     */
    private boolean compareToKeyPrefix(IN in, byte[] newKey) {

        byte[] keyPrefix = in.getKeyPrefix();

        if (keyPrefix == null || keyPrefix.length == 0) {
            return false;
        }

        int newKeyLen = newKey.length;

        for (int i = 0; i < keyPrefix.length; i++) {
            if (i < newKeyLen && keyPrefix[i] == newKey[i]) {
                continue;
            } else {
                return false;
            }
        }

        return true;
    }

    @Test
    public void testKeyComparisonPerformance() {
        byte[] key1 = StringUtils.toUTF8("abcdefghijabcdefghij");
        byte[] key2 = StringUtils.toUTF8("abcdefghijabcdefghij");

        for (int i = 0; i < 1000000; i++) {
            assertTrue(Key.compareKeys(key1, key2, null) == 0);
        }
    }

    @Test
    public void testKeyComparison() {
        byte[] key1 = StringUtils.toUTF8("aaa");
        byte[] key2 = StringUtils.toUTF8("aab");
        assertTrue(Key.compareKeys(key1, key2, null) < 0);
        assertTrue(Key.compareKeys(key2, key1, null) > 0);
        assertTrue(Key.compareKeys(key1, key1, null) == 0);

        key1 = StringUtils.toUTF8("aa");
        key2 = StringUtils.toUTF8("aab");
        assertTrue(Key.compareKeys(key1, key2, null) < 0);
        assertTrue(Key.compareKeys(key2, key1, null) > 0);

        key1 = StringUtils.toUTF8("");
        key2 = StringUtils.toUTF8("aab");
        assertTrue(Key.compareKeys(key1, key2, null) < 0);
        assertTrue(Key.compareKeys(key2, key1, null) > 0);
        assertTrue(Key.compareKeys(key1, key1, null) == 0);

        key1 = StringUtils.toUTF8("");
        key2 = StringUtils.toUTF8("");
        assertTrue(Key.compareKeys(key1, key2, null) == 0);

        byte[] ba1 = { -1, -1, -1 };
        byte[] ba2 = { 0x7f, 0x7f, 0x7f };
        assertTrue(Key.compareKeys(ba1, ba2, null) > 0);

        try {
            Key.compareKeys(key1, null, null);
            fail("NullPointerException not caught");
        } catch (NullPointerException NPE) {
        }
    }
}
