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

package com.sleepycat.je.jmx;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.lang.reflect.Method;

import javax.management.Attribute;
import javax.management.DynamicMBean;
import javax.management.MBeanException;

import org.junit.Test;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

/**
 * @excludeDualMode
 *
 * Instantiate and exercise the JEMonitor.
 */
public class JEMonitorTest extends TestBase {

    private static final boolean DEBUG = false;
    private File envHome;

    public JEMonitorTest() {
        envHome = SharedTestUtils.getTestDir();
    }

    /**
     * Test JEMonitor's attributes getters. 
     */
    @Test
    public void testGetters()
        throws Throwable {

        Environment env = null;
        try {
            env = openEnv(true);
            String environmentDir = env.getHome().getPath();
            DynamicMBean mbean = createMBean(env);
            MBeanTestUtils.validateGetters(mbean, 8, DEBUG); // see the change.
            env.close();

            /*
             * Replicated Environment must be transactional, so RepJEMonitor
             * can't open an Environment with non-transactional.
             */
            if (!this.getClass().getName().contains("rep")) {
                env = openEnv(false);
                mbean = createMBean(env);
                MBeanTestUtils.validateGetters(mbean, 6, DEBUG);
                env.close();
            }

            MBeanTestUtils.checkForNoOpenHandles(environmentDir);
        } catch (Throwable t) {
            t.printStackTrace();
            if (env != null) {
                env.close();
            }
            
            throw t;
        }
    }

    /* 
     * Create a DynamicMBean with the assigned standalone or replicated 
     * Environment. 
     */
    protected DynamicMBean createMBean(Environment env) {
        return new JEMonitor(env);
    }

    /**
     * Test JEMonitor's attributes setters.
     */
    @Test
    public void testSetters()
        throws Throwable {

        Environment env = null;
        try {
            /* Mimic an application by opening an environment. */
            env = openEnv(true);
            String environmentDir = env.getHome().getPath();

            /* Open an Mbean and set the Environment home. */
            DynamicMBean mbean = createMBean(env);

            /*
             * Try setting different attributes. Check against the
             * initial value, and the value after setting.
             */
            EnvironmentConfig config = env.getConfig();
            Class configClass = config.getClass();

            Method getCacheSize =
                configClass.getMethod("getCacheSize", (Class[]) null);
            checkAttribute(env,
                           mbean,
                           getCacheSize,
                           JEMBeanHelper.ATT_CACHE_SIZE,
                           new Long(100000)); // new value

            Method getCachePercent =
                configClass.getMethod("getCachePercent", (Class[]) null);
            checkAttribute(env,
                           mbean,
                           getCachePercent,
                           JEMBeanHelper.ATT_CACHE_PERCENT,
                           new Integer(10));
            env.close();

            MBeanTestUtils.checkForNoOpenHandles(environmentDir);
        } catch (Throwable t) {
            t.printStackTrace();

            if (env != null) {
                env.close();
            }

            throw t;
        }
    }

    /**
     * Test correction of JEMonitor's operations invocation.
     */
    @Test
    public void testOperations()
        throws Throwable {

        Environment env = null;
        try {
            env = openEnv(true);
            String environmentDir = env.getHome().getPath();
            DynamicMBean mbean = createMBean(env);
            int opNum = 0;
            if (!this.getClass().getName().contains("rep")) {
                opNum = 8;
            } else {
                opNum = 10;
            }
            MBeanTestUtils.validateMBeanOperations
                (mbean, opNum, true, null, null, DEBUG);

            /*
             * Getting database stats against a non-existing db ought to
             * throw an exception.
             */
            try {
                MBeanTestUtils.validateMBeanOperations
                    (mbean, opNum, true, "bozo", null, DEBUG);
                fail("Should not have run stats on a non-existent db");
            } catch (MBeanException expected) {
                // ignore
            }

            /*
             * Make sure the vanilla db open within the helper can open
             * a db created with a non-default configuration.
             */
            DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setAllowCreate(true);
            dbConfig.setTransactional(true);
            Database db = env.openDatabase(null, "bozo", dbConfig);

            /* insert a record. */
            DatabaseEntry entry = new DatabaseEntry();
            IntegerBinding.intToEntry(1, entry);
            db.put(null, entry, entry);

            MBeanTestUtils.validateMBeanOperations
                (mbean, opNum, true, "bozo", new String[] {"bozo"}, DEBUG);
            db.close();
            env.close();

            /*
             * Replicated Environment must be transactional, so can't test
             * RepJEMonitor with opening a non-transactional Environment.
             */
            if (!this.getClass().getName().contains("rep")) {
                env = openEnv(false);
                mbean = createMBean(env);
                MBeanTestUtils.validateMBeanOperations
                    (mbean, 7, true, null, null, DEBUG);
                env.close();
            }

            MBeanTestUtils.checkForNoOpenHandles(environmentDir);
        } catch (Throwable t) {
            t.printStackTrace();
            if (env != null) {
                env.close();
            }
            throw t;
        }
    }

    /* Check the correction of JEMonitor's attributes. */
    private void checkAttribute(Environment env,
                                DynamicMBean mbean,
                                Method configMethod,
                                String attributeName,
                                Object newValue)
        throws Exception {

        /* Check starting value. */
        EnvironmentConfig config = env.getConfig();
        Object result = configMethod.invoke(config, (Object[]) null);
        assertTrue(!result.toString().equals(newValue.toString()));

        /* set through mbean */
        mbean.setAttribute(new Attribute(attributeName, newValue));

        /* check present environment config. */
        config = env.getConfig();
        assertEquals(newValue.toString(),
                     configMethod.invoke(config, (Object[]) null).toString());

        /* check through mbean. */
        Object mbeanNewValue = mbean.getAttribute(attributeName);
        assertEquals(newValue.toString(), mbeanNewValue.toString());
    }

    /**
     * Checks that all parameters and return values are Serializable to
     * support JMX over RMI.
     */
    @Test
    public void testSerializable()
        throws Exception {

        /* Create and close the environment. */
        Environment env = openEnv(true);

        /* Test without an open environment. */
        DynamicMBean mbean = createMBean(env);
        MBeanTestUtils.doTestSerializable(mbean);

        env.close();
    }

    /*
     * Helper to open an environment.
     */
    protected Environment openEnv(boolean openTransactionally)
        throws Exception {

        return MBeanTestUtils.openTxnalEnv(openTransactionally, envHome);
    }
}
