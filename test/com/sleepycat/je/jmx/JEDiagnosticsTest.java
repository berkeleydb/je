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

import java.io.File;
import java.lang.reflect.Method;
import java.util.logging.Handler;
import java.util.logging.Level;

import javax.management.Attribute;
import javax.management.DynamicMBean;
import javax.management.MBeanInfo;
import javax.management.MBeanOperationInfo;

import org.junit.Test;

import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

/**
 * @excludeDualMode
 *
 * Instantiate and exercise the JEDiagnostics.
 */
public class JEDiagnosticsTest extends TestBase {

    private static final boolean DEBUG = false;
    private File envHome;

    public JEDiagnosticsTest() {
        envHome = SharedTestUtils.getTestDir();
    }

    /**
     * Test JEDiagnostics' attribute getters.
     */
    @Test
    public void testGetters()
        throws Throwable {

        Environment env = null;
        try {
            if (!this.getClass().getName().contains("rep")) {
                env = openEnv();
                DynamicMBean mbean = createMBean(env);
                MBeanTestUtils.validateGetters(mbean, 2, DEBUG); 
                env.close();
            }

            env = openEnv();
            String environmentDir = env.getHome().getPath();
            DynamicMBean mbean = createMBean(env);
            MBeanTestUtils.validateGetters(mbean, 2, DEBUG);
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

    /* Create a DynamicMBean using a standalone or replicated Environment. */
    protected DynamicMBean createMBean(Environment env) {
        return new JEDiagnostics(env);
    }

    /**
     * Test JEDiagnostics' attribute setters.
     */
    @Test
    public void testSetters()
        throws Throwable {

        Environment env = null;
        try {
            env = openEnv();
            String environmentDir = env.getHome().getPath();

            DynamicMBean mbean = createMBean(env);

            EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);
            Class envImplClass = envImpl.getClass();

            /* Test setting ConsoleHandler's level. */
            Method getConsoleHandler =
                envImplClass.getMethod("getConsoleHandler", (Class[]) null);
            checkAttribute(env,
                           mbean,
                           getConsoleHandler,
                           "consoleHandlerLevel",
                           "OFF");

            /* Test setting FileHandler's level. */
            Method getFileHandler = 
                envImplClass.getMethod("getFileHandler", (Class[]) null);
            checkAttribute(env,
                           mbean,
                           getFileHandler,
                           "fileHandlerLevel",
                           "OFF");

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
     * Test JEDiagnostics' operations invocation.
     */
    @Test
    public void testOperations() 
        throws Throwable {

        Environment env = null;
        try {
            env = openEnv();
            String environmentDir = env.getHome().getPath();
            DynamicMBean mbean = createMBean(env);

            /* 
             * RepJEDiagnostics has three operations while JEDiagnostics is
             * lack of getRepStats operation.
             */
            validateOperations(mbean, env, 1);
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

    /* Verify the correction of JEDiagnostics' operations. */
    private void validateOperations(DynamicMBean mbean,
                                    Environment env,
                                    int numExpectedOperations) 
        throws Throwable {

        MBeanTestUtils.checkOpNum(mbean, numExpectedOperations, DEBUG);
        
        MBeanInfo info = mbean.getMBeanInfo();
        MBeanOperationInfo[] ops = info.getOperations();
        EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);
        for (int i = 0; i < ops.length; i++) {
            String opName = ops[i].getName();
            if (opName.equals("resetLoggerLevel")) {

                /* 
                 * If this method is invoked by RepJEDiagnostics, the logger
                 * name should contain RepImpl, not EnvironmentImpl.
                 */
                Object[] params = new Object[] {"EnvironmentImpl", "OFF"};
                if (this.getClass().getName().contains("rep")) {
                    params = new Object[] {"RepImpl", "OFF"};
                }
                Object result = mbean.invoke
                    (opName, params,
                     new String[] {"java.lang.String", "java.lang.String"});
                assertEquals(envImpl.getLogger().getLevel(), Level.OFF);
                assertTrue(result == null);
            } else {

                /* 
                 * Check the correction of the getRepStats operation that only
                 * in RepJEDiagnostics.
                 */
                if (this.getClass().getName().contains("rep")) {
                    Object result = mbean.invoke(opName, null, null);
                    assertTrue(result instanceof String);
                    MBeanTestUtils.checkObjectType
                        ("Operation", opName, ops[i].getReturnType(), result);
                }
            }
        }
    }
                                   
    /* Test this MBean's serialization. */
    @Test
    public void testSerializable() 
        throws Throwable {

        Environment env = openEnv();
        
        DynamicMBean mbean = createMBean(env);
        MBeanTestUtils.doTestSerializable(mbean);

        env.close();
    }
    
    /* Check this MBean's attributes. */ 
    private void checkAttribute(Environment env,
                                DynamicMBean mbean,
                                Method getMethod,
                                String attributeName,
                                Object newValue)
        throws Exception {

        EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);
        Object result = getMethod.invoke(envImpl, (Object[]) null);
        assertTrue(!result.toString().equals(newValue.toString()));

        mbean.setAttribute(new Attribute(attributeName, newValue));

        envImpl = DbInternal.getNonNullEnvImpl(env);
        Handler handler = (Handler) getMethod.invoke(envImpl, (Object[]) null);
        assertEquals(newValue.toString(), handler.getLevel().toString());

        Object mbeanNewValue = mbean.getAttribute(attributeName);
        assertEquals(newValue.toString(), mbeanNewValue.toString());
    }

    /*
     * Helper to open an environment.
     */
    protected Environment openEnv()
        throws Exception {

        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setAllowCreate(true);

        return new Environment(envHome, envConfig);
    }
}
