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

package com.sleepycat.je;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.ArrayList;
import java.util.Properties;

import org.junit.Test;

import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.DbConfigManager;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

public class EnvironmentConfigTest extends TestBase {
    private final File envHome;

    public EnvironmentConfigTest() {
        envHome = SharedTestUtils.getTestDir();
    }

    /**
     * Try out the validation in EnvironmentConfig.
     */
    @Test
    public void testValidation() {

        /*
         * This validation should be successfull
         */
        Properties props = new Properties();
        props.setProperty(EnvironmentConfig.TXN_TIMEOUT, "10000");
        props.setProperty(EnvironmentConfig.TXN_DEADLOCK_STACK_TRACE, 
                          "true");
        new EnvironmentConfig(props); // Just instantiate a config object.

        /*
         * Should fail: we should throw because leftover.param is not
         * a valid parameter.
         */
        props.clear();
        props.setProperty("leftover.param", "foo");
        checkEnvironmentConfigValidation(props);

        /*
         * Should fail: we should throw because FileHandlerLimit
         * is less than its minimum
         */
        props.clear();
        props.setProperty(EnvironmentConfig.LOCK_N_LOCK_TABLES, "0");
        checkEnvironmentConfigValidation(props);

        /*
         * Should fail: we should throw because FileHandler.on is not
         * a valid value.
         */
        props.clear();
        props.setProperty(EnvironmentConfig.TXN_DEADLOCK_STACK_TRACE, "xxx");
        checkEnvironmentConfigValidation(props);
    }

    /**
     * Test single parameter setting.
     */
    @Test
    public void testSingleParam()
        throws Exception {

        try {
            EnvironmentConfig config = new EnvironmentConfig();
            config.setConfigParam("foo", "7");
            fail("Should fail because of invalid param name");
        } catch (IllegalArgumentException e) {
            // expected.
        }

        EnvironmentConfig config = new EnvironmentConfig();
        config.setConfigParam(EnvironmentParams.MAX_MEMORY_PERCENT.getName(),
                              "81");
        assertEquals(81, config.getCachePercent());
    }

    /* 
     * Test that replicated config param is wrongly set on a standalone 
     * Environment. 
     */
    @Test
    public void testRepParam() 
        throws Exception {

        /* The replicated property name and value. */
        final String propName = "je.rep.maxMessageSize";
        final String propValue = "1000000";

        /*
         * Should fail if set this configuration through the EnvironmentConfig
         * class.
         */
        EnvironmentConfig envConfig = new EnvironmentConfig();
        try {
            envConfig.setConfigParam(propName, propValue);
            fail("Should fail because it's a replicated parameter");
        } catch (IllegalArgumentException e) {
            /* Expected exception here. */
        } catch (Exception e) {
            fail("Unexpected exception: " + e);
        }

        /* Read the je.properties. */
        ArrayList<String> formerLines = TestUtils.readWriteJEProperties
            (envHome, propName + "=" + propValue + "\n");

        /* Check this behavior is valid. */
        Environment env = null;
        try {
            envConfig.setAllowCreate(true);
            env = new Environment(envHome, envConfig);
        } catch (Exception e) {
            fail("Unexpected exception: " + e);
        }

        /* 
         * Check that getting the value for je.rep.maxMessageSize will throw 
         * exception. 
         */
        try {
            EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);
            envImpl.getConfigManager().get(propName);
            fail("Expect to see exceptions here.");
        } catch (IllegalArgumentException e) {
            /* Expected exception here. */
        } catch (Exception e) {
            fail("Unexpected exception: " + e);
        } finally {
            if (env != null) {
                env.close();
            }
        }

        TestUtils.reWriteJEProperties(envHome, formerLines);
    }

    @Test
    public void testSerialize()
        throws Exception {

        final String nodeName = "env1";

        EnvironmentConfig envConfig = new EnvironmentConfig();
        /* Test the seriliazed fileds of EnvironmentConfig. */
        envConfig.setAllowCreate(true);
        envConfig.setNodeName(nodeName);
        /* Test the transient fields of EnvironmentConfig. */
        envConfig.setTxnReadCommitted(true);
        /* Test the serialized fields of EnvironmentMutableConfig. */
        envConfig.setTransactional(true);
        envConfig.setTxnNoSync(true);
        envConfig.setCacheSize(100000);
        envConfig.setConfigParam(EnvironmentConfig.ENV_RUN_CLEANER,
                                 "false");
        envConfig.setCacheMode(CacheMode.DEFAULT);
        /* Test the transient fields in the EnvironmentMutableConfig. */
        envConfig.setLoadPropertyFile(true);
        envConfig.setExceptionListener(new ExceptionListener() {
            public void exceptionThrown(ExceptionEvent event) {
            }
        });

        EnvironmentConfig newConfig = (EnvironmentConfig) 
            TestUtils.serializeAndReadObject(envHome, envConfig);
        
        assertTrue(newConfig != envConfig);
        /* Check the serialized fields of EnvironmentConfig. */
        assertEquals(newConfig.getAllowCreate(), envConfig.getAllowCreate());
        assertEquals(newConfig.getNodeName(), nodeName);
        /* Check the transient fields of EnvironmentConfig. */
        assertFalse(newConfig.getCreateUP() == envConfig.getCreateUP());
        assertNotSame
            (newConfig.getCheckpointUP(), envConfig.getCheckpointUP());
        assertNotSame
            (newConfig.getTxnReadCommitted(), envConfig.getTxnReadCommitted());
        /* Check the serialized fields of EnvironmentMutableConfig. */
        assertEquals
            (newConfig.getTransactional(), envConfig.getTransactional());
        assertEquals(newConfig.getTxnNoSync(), envConfig.getTxnNoSync());
        assertEquals(newConfig.getCacheSize(), envConfig.getCacheSize());
        assertEquals(new DbConfigManager(newConfig).
                     get(EnvironmentConfig.ENV_RUN_CLEANER), 
                     "false");
        assertEquals(newConfig.getCacheMode(), envConfig.getCacheMode());
        /* Check the transient fields of EnvironmentMutableConfig. */
        assertFalse(newConfig.getLoadPropertyFile() == 
                    envConfig.getLoadPropertyFile());
        assertFalse
            (newConfig.getValidateParams() == envConfig.getValidateParams());
        assertEquals(newConfig.getExceptionListener(), null);
    }

    @Test
    public void testInconsistentParams()
        throws Exception {

        try {
            EnvironmentConfig config = new EnvironmentConfig();
            config.setAllowCreate(true);
            config.setLocking(false);
            config.setTransactional(true);
            File envHome = SharedTestUtils.getTestDir();
            new Environment(envHome, config);
            fail("Should fail because of inconsistent param values");
        } catch (IllegalArgumentException e) {
            // expected.
        }
    }

    /* Helper to catch expected exceptions. */
    private void checkEnvironmentConfigValidation(Properties props) {
        try {
            new EnvironmentConfig(props);
            fail("Should fail because of a parameter validation problem");
        } catch (IllegalArgumentException e) {
            // expected.
        }
    }
}
