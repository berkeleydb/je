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

import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.TransactionStats.Active;

/**
 * An array of active Txn stats.
 */
public class ActiveTxnArrayStat extends Stat<Active[]> {
    private static final long serialVersionUID = 1L;

    private Active[] array;

    public ActiveTxnArrayStat(StatGroup group, StatDefinition definition) {
        super(group, definition);
    }

    public ActiveTxnArrayStat(StatGroup group,
                              StatDefinition definition,
                              Active[] array) {
        super(group, definition);
        this.array = array;
    }

    @Override
    public Active[] get() {
        return array;
    }

    @Override
    public void set(Active[] array) {
        this.array = array;
    }

    @Override
    public void add(Stat<Active[]> other) {
        throw EnvironmentFailureException.unexpectedState
            ("ActiveTxnArrayStat doesn't support the add operation.");
    }

    @Override
    public void clear() {
        if (array != null && array.length > 0) {
            for (int i = 0; i < array.length; i++) {
                array[i] = new Active(array[i].getName(), 0, 0);
            }
        }
    }

    @Override
    public Stat<Active[]> computeInterval(Stat<Active[]> base) {
        return copy();
    }

    @Override
    public void negate() {
        throw EnvironmentFailureException.unexpectedState
            ("ActiveTxnArrayStat doesn't support the negate operation.");
    }

    @Override
    public ActiveTxnArrayStat copy() {
        try {
            ActiveTxnArrayStat ret = (ActiveTxnArrayStat) super.clone();
            if (array != null && array.length > 0) {
                Active[] newArray = new Active[array.length];
                System.arraycopy
                    (array, 0, newArray, 0, array.length);
                ret.set(newArray);
            }

            return ret;
        } catch (CloneNotSupportedException e) {
            throw EnvironmentFailureException.unexpectedException(e);
        }
    }

    @Override
    protected String getFormattedValue() {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        if (array != null && array.length > 0) {
            for (Active active : array) {
                sb.append(" txnId = " + Stat.FORMAT.format(active.getId()) +
                          " txnName = " + active.getName() + "\n");
            }
        }
        sb.append("]");

        return sb.toString();
    }

    @Override
    public boolean isNotSet() {
        if ( array == null) {
            return true;
        }

        if (array.length == 0) {
            return true;
        }

        return false;
    }
}
