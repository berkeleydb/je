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

package com.sleepycat.je.rep.jmx;

import com.sleepycat.je.Environment;
import com.sleepycat.je.jmx.JEDiagnostics;

/*
 * This concrete MBean is a logging monitor on a replicated JE Environment.
 *
 * It not only has the same attributes and operations as the standalone 
 * JEDiagnostics, but also has some specific replicated related operations.
 */
public class RepJEDiagnostics extends JEDiagnostics {
    protected RepJEDiagnostics(Environment env) {
        super(env);
    }

    public RepJEDiagnostics() {
        super();
    }

    @Override
    protected void initClassFields() {
        currentClass = RepJEDiagnostics.class;
        className = "RepJEDiagnostics";
        DESCRIPTION = "Logging Monitor on an open replicated Environment.";
    }

    @Override
    protected void doRegisterMBean(Environment useEnv)
        throws Exception {

        server.registerMBean(new RepJEDiagnostics(useEnv), jeName);
    }
}
