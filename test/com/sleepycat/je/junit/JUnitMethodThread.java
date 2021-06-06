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

package com.sleepycat.je.junit;

import java.lang.reflect.Method;

/**
 * A JUnitThread whose testBody calls a given TestCase method.
 */
public class JUnitMethodThread extends JUnitThread {

    private Object testCase;
    private Method method;
    private Object param;

    public JUnitMethodThread(String threadName, String methodName,
                             Object testCase)
        throws NoSuchMethodException {

        this(threadName, methodName, testCase, null);
    }

    public JUnitMethodThread(String threadName, String methodName,
                             Object testCase, Object param)
        throws NoSuchMethodException {

        super(threadName);
        this.testCase = testCase;
        this.param = param;
        method = testCase.getClass().getMethod(methodName, new Class[0]);
    }

    public void testBody()
        throws Exception {

        if (param != null) {
            method.invoke(testCase, new Object[] { param });
        } else {
            method.invoke(testCase, new Object[0]);
        }
    }
}
