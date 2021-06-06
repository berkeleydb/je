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

import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Transaction;

/**
 * <code>Trigger</code> defines the trigger methods associated with a database.
 * They provide a mechanism to track the database definition operations used to
 * manage the lifecycle of the database itself, as well as the record
 * operations used to modify the contents of the database.
 * <p>
 * WARNING: Only transient triggers are currently supported, and the
 * documention below has not yet been updated to reflect this fact.  The bottom
 * line is that triggers are currently only useful and known to be reliable for
 * maintaining a cache of database information on a replica, where the cache is
 * initialized after opening the database (and configuring the trigger), and
 * where only the TransactionTrigger.commit method is used.  More specifically:
 *  <ul>
 *  <li>Although the {@link PersistentTrigger} interface exists, it may not
 *  currently be used reliably.</li>
 *  <li>Triggers must be configured on each node in a rep group separately,
 *  when needed.  Specifically, a trigger configured on a master will not be
 *  automatically configured and invoked on the replicas in the group.</li>
 *  <li>Because only transient triggers are currently supported, trigger
 *  methods are only called after opening a database (when configuring the
 *  trigger in the DatabaseConfig), and are not called after closing the
 *  database.</li>
 *  <li>As a result of the above point, triggers are not called during
 *  recovery, and therefore cannot be reliably used to perform write operations
 *  using the tranaction passed to the trigger method.</li>
 *  <li>Also see the warning at the top of ReplicatedDatabaseTrigger.java.</li>
 *  </ul>
 * <p>
 * The trigger methods {@link #put put} and {@link #delete delete} are used to
 * track all record operations on the database.
 * </p>
 * <p>
 * A trigger method takes a transaction as its first argument. If the
 * environment is not transactional, the argument is null. In all other cases,
 * it's a valid transaction ({@link Transaction#isValid() Transaction.isValid}
 * is true) and the trigger can use this transaction to make it's own set of
 * accompanying changes.
 * <p>
 * If the invocation of a trigger results in a runtime exception, the
 * transaction (if one was associated with the method) is invalidated and any
 * subsequent triggers associated with the operation are skipped. It's the
 * caller's responsibility to handle the exception and abort the invalidated
 * transaction. If the exception is thrown during the replay of a transaction
 * on a replica in an HA application, the environment is invalidated and a new
 * environment handle must be created.
 * </p>
 * <p>
 * A Trigger is associated with a database via
 * {@link DatabaseConfig#setTriggers DatabaseConfig.setTriggers}.
 * </p>
 */
public interface Trigger {

    /**
     * Returns the name associated with the trigger. All the triggers
     * associated with a particular database must have unique names.
     *
     * @return the Trigger's name
     */
    public String getName();

    /**
     * Sets the database name associated with this trigger. The JE trigger
     * mechanism invokes this method to ensure that the trigger knows the name
     * it's associated with across a rename of the database.
     * <p>
     * This method is also invoked each time the trigger is de-serialized, so
     * that the trigger does not need to store this information as part of it's
     * serialized representation.
     *
     * @param databaseName the name of the database associated with this
     * trigger
     *
     * @return this
     */
    public Trigger setDatabaseName(String databaseName);

    /**
     * Returns the result of the {@link #setDatabaseName(String)} operation.
     *
     * @return the name of the database associated with this trigger
     */
    public String getDatabaseName();

    /* Trigger lifecycle operations. */

    /**
     * The trigger method invoked when this trigger is added to the database.
     * This is the very first trigger method that is invoked and it's invoked
     * exactly once. If the database is replicated, it's invoked once per node
     * on each node.
     * </p>
     * @param txn the active transaction associated with the operation. The
     * argument is null if the database is not transactional.
     */
    public void addTrigger(Transaction txn);

    /**
     * The trigger method invoked when this trigger is removed from the
     * database, either as a result of opening the database with a different
     * trigger configuration, or because the database it was associated with
     * it has been removed. In the latter case, this trigger method follows
     * the invocation of the {@link Persistent#remove} remove trigger. If the
     * transaction is committed, there will be no subsequent trigger method
     * invocations for this trigger.
     *
     * @param txn the active transaction associated with the operation. The
     * argument is null if the database is not transactional.
     */
    public void removeTrigger(Transaction txn);

    /* Record operations. */

    /**
     * The trigger method invoked after a successful put, that is, one that
     * actually results in a modification to the database.
     * <p>
     * If a new entry was inserted, oldData will be null and newData will be
     * non-null. If an existing entry was updated, oldData and newData will
     * be non-null.
     * </p>
     *
     * @param txn the active transaction associated with the operation. The
     * argument is null if the database is non-transactional.
     *
     * @param key the non-null primary key
     *
     * @param oldData the data before the change, or null if the record
     * did not exist.
     *
     * @param newData the non-null data after the change
     */
    public void put(Transaction txn,
                    DatabaseEntry key,
                    DatabaseEntry oldData,
                    DatabaseEntry newData);
    // TODO: make API provisions for put triggers where we optimize it not to
    // fetch the oldData

    /**
     * The trigger method invoked after a successful delete, that is, one that
     * actually resulted in a key/value pair being removed.
     * <p>
     * Truncating a database does not invoke this trigger;
     * {@link PersistentTrigger#truncate} is invoked upon truncation.
     * </p>
     *
     * @param txn the active transaction associated with the operation. The
     * argument is null if the database is non-transactional.
     *
     * @param key the non-null primary key
     *
     * @param oldData the non-null data that was associated with the deleted
     * key
     */
    public void delete(Transaction txn,
                       DatabaseEntry key,
                       DatabaseEntry oldData);
}
