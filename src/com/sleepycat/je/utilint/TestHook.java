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

import java.io.IOException;

/**
 * TestHook is used to induce testing behavior that can't be provoked
 * externally.  For example, unit tests may use hooks to throw IOExceptions, or
 * to cause waiting behavior.
 *
 * To use this, a unit test should implement TestHook with a class that
 * overrides the desired method. The desired code will have a method that
 * allows the unit test to specify a hook, and will execute the hook if it is
 * non-null.  This should be done within an assert like so:
 *
 *    assert TestHookExecute(myTestHook);
 *
 * See Tree.java for examples.
 */
public interface TestHook<T> {

    public void hookSetup();

    public void doIOHook()
        throws IOException;

    public void doHook();

    public void doHook(T obj);

    public T getHookValue();
}
