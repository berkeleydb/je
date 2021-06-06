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

package com.sleepycat.je.rep.subscription;

import com.sleepycat.je.utilint.VLSN;

/**
 * Interface of subscription callback function, to be implemented by clients to
 * process each received subscription message.
 */
public interface SubscriptionCallback {

    /**
     * Process a put (insert or update) entry from stream
     *
     * @param vlsn   VLSN of the insert entry
     * @param key    key of the insert entry
     * @param value  value of the insert entry
     * @param txnId  id of txn the entry belongs to
     */
    void processPut(VLSN vlsn, byte[] key, byte[] value, long txnId);

    /**
     * Process a delete entry from stream
     *
     * @param vlsn   VLSN of the delete entry
     * @param key    key of the delete entry
     * @param txnId  id of txn the entry belongs to
     */
    void processDel(VLSN vlsn, byte[] key, long txnId);

    /**
     * Process a commit entry from stream
     *
     * @param vlsn  VLSN of commit entry
     * @param txnId id of txn to commit
     */
    void processCommit(VLSN vlsn, long txnId);

    /**
     * Process an abort entry from stream
     *
     * @param vlsn  VLSN of abort entry
     * @param txnId id of txn to abort
     */
    void processAbort(VLSN vlsn, long txnId);

    /**
     * Process the exception from stream.
     *
     * @param exp  exception raised in service and to be processed by
     *             client
     */
    void processException(final Exception exp);
}
