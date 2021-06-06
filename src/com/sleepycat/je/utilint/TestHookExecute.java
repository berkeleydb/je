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

package com.sleepycat.je.utilint;

/**
 * Execute a test hook if set. This wrapper is used so that test hook execution
 * can be packaged into a single statement that can be done within an assert
 * statement.
 */
public class TestHookExecute {

    public static boolean doHookSetupIfSet(TestHook<?> testHook) {
        if (testHook != null) {
            testHook.hookSetup();
        }
        return true;
    }

    public static boolean doHookIfSet(TestHook<?> testHook) {
        if (testHook != null) {
            testHook.doHook();
        }
        return true;
    }

    public static <T> boolean doHookIfSet(TestHook<T> testHook, T obj) {
        if (testHook != null) {
            testHook.doHook(obj);
        }
        return true;
    }
}
