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
 * ReplicatedTrigger defines trigger methods that are invoked on a
 * replica when a replica needs to resume a transaction that's only partially
 * present in the replica's logs and needs to be resumed so that it can be
 * replayed to a conclusion, that is, to the point where the partial
 * transaction has been committed or aborted by the master.
 * <p>
 * WARNING: This interface is not currently supported.  This means that, on a
 * replica where transactions may be rolled back without a full environment
 * shutdown, the repeatXxx methods cannot be used to handle this circumstance.
 * To be safe, it is best to only use TransactionTrigger methods, namely
 * TransactionTrigger.commit.
 * <p>
 * WARNING: Only transient triggers are currently supported, and the
 * documention below has not yet been updated to reflect this fact.  See
 * details at the top of Trigger.java.
 * <p>
 * The trigger methods
 * can be invoked in one of two circumstances:
 * <ol>
 * <li>A new environment handle is opened on the replica and its logs contain a
 * partial transaction.</li>
 * <li>A new master is elected and a replica has to switch over to the new
 * master, while in the midst of replaying a transaction.</li>
 * </ol>
 * <p>
 * These trigger methods are only invoked if the partial transactions contain
 * operations associated with triggers.
 * </p>
 *
 * Consider a transaction consisting of two put operations:
 *
 * <pre class="code">
 * put k1
 * put k2
 * commit t
 * </pre>
 *
 * In the absence of a replica or master failure this would normally result in
 * the sequence of trigger calls:
 *
 * <pre class="code">
 * Trigger.put(t, k1, ...)
 * Trigger.put(t, k2,....)
 * Trigger.commit(t)
 * </pre>
 *
 * If the replica failed in the midst of the transaction replay, immediately
 * after the first put operation, the sequence of trigger invocations before
 * the replica went down would be:
 *
 * <pre class="code">
 *  Trigger.put(k1, ...)
 * </pre>
 *
 * followed by the trigger calls below when the replica handle was subsequently
 * reopened:
 *
 * <pre class="code">
 *  ReplicatedTrigger.repeat(t)
 *  Trigger.repeatPut(t, k1, ...)
 *  Trigger.put(t, k2, ...)
 *  Trigger.commit(t)
 * </pre>
 *
 * The interface defines one "repeat" trigger method for each of the trigger
 * methods defined by Trigger. The methods are distinct from those
 * defined by Trigger to highlight the fact that the trigger method is
 * being invoked a second time for the same operation and the trigger method
 * may not have completed the actions it intended to take when it was invoked
 * the first time. For example, the trigger method may have been used to update
 * a couple of local indexes and it was only finished with updating one local
 * index and persisting it before the replica crashed. As a result the method
 * may need to take special action to repair state maintained by it.
 * <p>
 * A ReplicatedTrigger is associated with a replicated database via {@link
 * DatabaseConfig#setTriggers DatabaseConfig.setTriggers}.  For a replicated
 * database, the ReplicatedTrigger interface must be implemented for all
 * triggers.  For a non-replicated database, implementing the ReplicatedTrigger
 * interface is allowed, but the ReplicatedTrigger methods will not be called.
 * </p>
 */
public interface ReplicatedDatabaseTrigger extends Trigger {

    /**
     * Used to inform the application that the trigger method calls associated
     * with the partial transaction will be repeated.
     *
     * @param txn the partial transaction
     */
    public void repeatTransaction(Transaction txn);

    /* Trigger lifecycle operations. */

    /**
     * The trigger method invoked when an addTrigger operation needs to be
     * repeated.
     *
     * @see Trigger#addTrigger
     */
    public void repeatAddTrigger(Transaction txn);

    /**
     * The trigger method invoked when a removeTrigger operation needs to be
     * repeated.
     *
     * @see Trigger#removeTrigger
     */
    public void repeatRemoveTrigger(Transaction txn);

    /* Database Granularity operations */

    /**
     * The trigger method invoked when a database create trigger needs to be
     * repeated.
     *
     * @see PersistentTrigger#open
     */
    public void repeatCreate(Transaction txn);

    /**
     * The trigger method invoked when a database remove trigger needs to be
     * repeated.
     *
     * @see PersistentTrigger#remove
     */
    public void repeatRemove(Transaction txn);

    /**
     * The trigger method invoked when a database truncate trigger needs to be
     * repeated.
     *
     * @see PersistentTrigger#truncate
     */
    public void repeatTruncate(Transaction txn);

    /**
     * The trigger method invoked when a database rename trigger needs to be
     * repeated.
     *
     * @see PersistentTrigger#rename
     */
    public void repeatRename(Transaction txn, String newName);

    /* Key/value granularity operations. */

    /**
     * The trigger method invoked when a database put trigger needs to be
     * repeated. Note that this method differs from the corresponding
     * <code>Trigger.put</code> method in that it omits the
     * <code>oldData</code> argument.
     *
     * @see Trigger#put
     */
    public void repeatPut(Transaction txn,
                          DatabaseEntry key,
                          DatabaseEntry newData);

    /**
     * The trigger method invoked when a database delete trigger needs to be
     * repeated.  Note that this method differs from the corresponding
     * <code>Trigger.delete</code> method in that it omits the
     * <code>oldData</code> argument.
     *
     * @see Trigger#remove
     */
    public void repeatDelete(Transaction txn,
                             DatabaseEntry key);
}
