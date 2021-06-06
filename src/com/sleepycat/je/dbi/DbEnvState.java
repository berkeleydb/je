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

package com.sleepycat.je.dbi;


/**
 * DbEnvState implements a typesafe enumeration of environment states
 * and does state change validation.
 */
class DbEnvState {
    private static final boolean DEBUG = false;

    private String name;

    /* Valid environment states. */
    public static final DbEnvState INIT = new DbEnvState("initialized");
    public static final DbEnvState OPEN = new DbEnvState("open");
    public static final DbEnvState CLOSED = new DbEnvState("closed");
    public static final DbEnvState INVALID = new DbEnvState("invalid");

    /* Valid previous states, for state transition checking. */
    public static final DbEnvState[] VALID_FOR_CLOSE =  {INIT, OPEN, INVALID};
    /* Not currently used:
    public static final DbEnvState[] VALID_FOR_OPEN =   {INIT, CLOSED};
    public static final DbEnvState[] VALID_FOR_REMOVE = {INIT, CLOSED};
    */

    DbEnvState(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return name;
    }

    /* Check for valid state transitions. */
    void checkState(DbEnvState[] validPrevStates, DbEnvState newState) {
        if (DEBUG) {
            System.out.println("newState = " + newState +
                               " currentState = " + name);
        }
        boolean transitionOk = false;
        for (int i = 0; i < validPrevStates.length; i++) {
            if (this == validPrevStates[i]) {
                transitionOk = true;
                break;
            }
        }
        if (!transitionOk) {
            throw new IllegalStateException
                ("Can't go from environment state " + toString() +
                 " to " + newState.toString());
        }
    }
}
