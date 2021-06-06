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

package com.sleepycat.je.rep.stream;

import com.sleepycat.je.rep.impl.RepImpl;

/**
 * The FeederFilter is used by the Feeder to determine whether a record should
 * be sent to the Replica. The filter object is created at the replica and is
 * transmitted to the Feeder as part of the syncup process. The filter thus
 * represents replica code that is running inside the Feeder, that is, the
 * computation has been moved closer to the data and can be used to eliminate
 * unnecessary network communication overheads.
 */
public interface FeederFilter {

    /**
     * The execute method that invoked before a record is sent to the replica.
     * If the filter returns null, the feeder will not send the record to the
     * replica as part of the replication stream, since it's not of interest
     * to the replica. It can for example be used to filter out tables that
     * are not of interest to the replica.
     *
     * @param record the record to be filtered
     * @param repImpl repImpl of the RN where the filter is executed
     *
     * @return the original input record if it is to be sent to the replica.
     * null if it's to be skipped.
     */
    OutputWireRecord execute(final OutputWireRecord record,
                             final RepImpl repImpl);


    /**
     * Gets arrays of subscribed table ids. If null or array length is 0,
     * that means the subscriber would subscribe all tables in the store.
     *
     * @return  arrays of subscribed table ids
     */
    String[] getTableIds();
}
