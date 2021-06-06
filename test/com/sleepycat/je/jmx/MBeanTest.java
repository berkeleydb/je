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

import java.io.File;

import javax.management.DynamicMBean;

import junit.framework.TestCase;

import com.sleepycat.je.Environment;
import com.sleepycat.je.util.TestUtils;

/**
 * @excludeDualMode
 *
 * Instantiate and exercise the JEMBeanHelper.
 */
public class MBeanTest extends TestCase {

    private static final boolean DEBUG = false;
    private File envHome;
    private String environmentDir;

    public MBeanTest() {
        environmentDir = System.getProperty(TestUtils.DEST_DIR);
        envHome = new File(environmentDir);
    }

    public void setUp() {

        TestUtils.removeLogFiles("Setup", envHome, false);
    }

    public void tearDown()
        throws Exception {

        TestUtils.removeLogFiles("tearDown", envHome, true);
    }

    /**
     * MBean which can configure and open an environment.
     */
    public void testOpenableBean()
        throws Throwable {

        Environment env = null;
        try {
            /* Environment is not open, and we can open. */
            env = MBeanTestUtils.openTxnalEnv(false, envHome);
            env.close();

            DynamicMBean mbean = new JEApplicationMBean(environmentDir);
            MBeanTestUtils.validateGetters(mbean, 5, DEBUG);
            MBeanTestUtils.validateMBeanOperations
                (mbean, 1, false, null, null, DEBUG);

            /* Open the environment. */
            mbean.invoke(JEApplicationMBean.OP_OPEN, null, null);

            MBeanTestUtils.validateGetters(mbean, 7, DEBUG);
            MBeanTestUtils.validateMBeanOperations
                (mbean, 7, true, null, null, DEBUG);

            /*
             * The last call to validateOperations ended up closing the
             * environment.
             */
            MBeanTestUtils.validateGetters(mbean, 5, DEBUG);
            MBeanTestUtils.validateMBeanOperations
                (mbean, 1, false, null, null, DEBUG);

            /* Should be no open handles. */
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
     * Checks that all parameters and return values are Serializable to
     * support JMX over RMI.
     */
    public void testSerializable()
        throws Exception {

        /* Create and close the environment. */
        Environment env = MBeanTestUtils.openTxnalEnv(false, envHome);
        env.close();

        /* Test without an open environment. */
        DynamicMBean mbean = new JEApplicationMBean(environmentDir);
        MBeanTestUtils.doTestSerializable(mbean);

        /* Test with an open environment. */
        mbean.invoke(JEApplicationMBean.OP_OPEN, null, null);
        MBeanTestUtils.doTestSerializable(mbean);

        /* Close. */
        mbean.invoke(JEApplicationMBean.OP_CLOSE, null, null);
    }
}
