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
package com.sleepycat.je.trigger;

import java.io.Serializable;

import com.sleepycat.je.Environment;
import com.sleepycat.je.Transaction;

/**
 * Placeholder to be used when persistent triggers are supported in the future.
 * See warning at the top of Trigger.java
 * <p>
 * Note that all subtypes of PersistentTrigger must be serializable, because
 * they are stored persistently in the environment.
 * <p>
 * The following table captures the relationship between the database
 * granularity operations and their associated trigger methods.
 * </p>
 * <table BORDER CELLPADDING=3 CELLSPACING=1 width="50%" align="center">
 * <tr>
 * <td ALIGN=CENTER><b>Database Operation</b></td>
 * <td ALIGN=CENTER><b>Trigger Method</b></td>
 * </tr>
 * <tr>
 * <td>{@link Environment#openDatabase Environment.openDatabase} resulting in
 * the creation of a new primary database. Or the first open of a database for
 * write operations.</td>
 * <td>{@link #open open}</td>
 * </tr>
 * <tr>
 * <td>{@link Database#close Database.close} the close of a database that was
 * opened for write operations.</td>
 * <td>{@link #close close}</td>
 * </tr>
 * <tr>
 * <td>{@link Environment#removeDatabase Environment.removeDatabase}</td>
 * <td>{@link PersistentTrigger#remove remove}</td>
 * </tr>
 * <tr>
 * <td>{@link Environment#truncateDatabase Environment.truncateDatabase}</td>
 * <td>{@link PersistentTrigger#truncate truncate}</td>
 * </tr>
 * <tr>
 * <td>{@link Environment#renameDatabase Environment.renameDatabase}</td>
 * <td>{@link PersistentTrigger#rename rename}</td>
 * </tr>
 * </table>
 * </p>
 */
public interface PersistentTrigger extends Trigger, Serializable {

    /* Database operations */

    /**
     * The trigger method invoked after the open of the first {@link Database}
     * writable handle.
     *
     * A call to the open trigger always precedes any subsequent calls to the
     * {@link #put} and {@link #delete} triggers defined below, since the
     * <code>put</code> and <code>delete</code> operations can only be invoked
     * on a database handle.
     * <p>
     * If the database is replicated, the replay mechanism on a
     * <code>Replica</code> may open and close databases as it replays the
     * replication stream. The maximum number of databases that may be open at
     * any given time and the duration for which they can be left open can be
     * controlled by configuring
     * <code>ReplicationConfig.REPLAY_MAX_OPEN_DB_HANDLES</code> and
     * <code>ReplicationConfig.REPLAY_DB_HANDLE_TIMEOUT</code> respectively.
     * <p>
     * The method may be invoked when the database is first created, or
     * subsequently when a new trigger is added to an existing database. As a
     * result, a call to this trigger is always preceded by a call to the
     * {@link #addTrigger(Transaction) addTrigger} trigger method.
     *
     * @param txn the active transaction associated with the operation. The
     * argument is null if the operation is not transactional.
     *
     * @param environment a handle to the environment associated with the
     * database being opened. The trigger code must not close the environment
     * handle.
     *
     * @param isNew is true if the database was newly created as a result of
     * the call to {@link Environment#openDatabase}
     *
     * @see Environment#openDatabase
     */
    public void open(Transaction txn, Environment environment, boolean isNew);

    /**
     * The trigger method associated with the close of the last writable
     * {@link Database} handle.
     * <p>
     * If the database is replicated, the replay mechanism on a
     * <code>Replica</code> may open and close databases as it replays the
     * replication stream. The maximum number of databases that may be open at
     * any given time and the duration for which they can be left open can be
     * controlled by configuring
     * <code>ReplicationConfig.REPLAY_MAX_OPEN_DB_HANDLES</code> and
     * <code>ReplicationConfig.REPLAY_DB_HANDLE_TIMEOUT</code> respectively.
     * <p>
     * @see Database#close
     */
    public void close();

    /**
     * The trigger method invoked after the successful removal of a primary
     * {@link Database}.
     *
     * @param txn the transaction associated with the operation. The argument
     * is null if the environment is non-transactional.
     *
     * @see Environment#removeDatabase
     */
    public void remove(Transaction txn);

    /**
     * The trigger method invoked after the successful truncation of a
     * {@link Database}.
     *
     * @param txn the transaction associated with the operation. The argument
     * is null if the environment is non-transactional.
     *
     * @see Environment#truncateDatabase
     */
    public void truncate(Transaction txn);

    /**
     * The trigger method invoked after the successful renaming of a primary
     * {@link Database}.
     *
     * @param txn the transaction associated with the operation. The argument
     * is null if the environment is non-transactional.
     *
     * @param newName it's current (new) name
     *
     * @see Environment#renameDatabase
     */
    public void rename(Transaction txn, String newName);
}
