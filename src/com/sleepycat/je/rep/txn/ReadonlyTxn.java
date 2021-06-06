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

package com.sleepycat.je.rep.txn;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.LockConflictException;
import com.sleepycat.je.LockNotAvailableException;
import com.sleepycat.je.ReplicaConsistencyPolicy;
import com.sleepycat.je.ThreadInterruptedException;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.ReplicationContext;
import com.sleepycat.je.rep.MasterStateException;
import com.sleepycat.je.rep.ReplicaConsistencyException;
import com.sleepycat.je.rep.ReplicaWriteException;
import com.sleepycat.je.rep.ReplicatedEnvironment.State;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.txn.LockResult;
import com.sleepycat.je.txn.LockType;
import com.sleepycat.je.txn.Txn;

/**
 * A ReadonlyTxn represents
 *  - a user initiated Txn executed on the Master node, when local-write or
 *    read-only is configured, or
 *  - a user initiated Txn executed on the Replica node, whether or not
 *    local-write is configured, or
 *  - an auto-commit Txn on a Replica node for a replicated DB.
 *
 * As its name implies it is used to implement the read-only semantics for
 * access to replicated DBs on the Replica. It is not replicated txn, i.e.,
 * it is not part of the rep stream.
 *
 * In addition, it uses the transaction hooks defined on Txn to implement the
 * ReplicaConsistencyPolicy.  This must be done for all access to replicated
 * DBs, including when local-write is configured.
 */
public class ReadonlyTxn extends Txn {

    private final boolean localWrite;

    public ReadonlyTxn(EnvironmentImpl envImpl, TransactionConfig config)
        throws DatabaseException {

        super(envImpl, config, ReplicationContext.NO_REPLICATE);

        localWrite = config.getLocalWrite();
    }

    @Override
    public boolean isLocalWrite() {
        return localWrite;
    }

    /**
     * Provides a wrapper to screen for write locks. The use of write locks is
     * used to infer that an attempt is being made to modify a replicated
     * database. Note that this technique misses "conditional" updates, for
     * example a delete operation using a non-existent key, but we are ok with
     * that since the primary intent here is to ensure the integrity of the
     * replicated stream that is being played back at that replica and these
     * checks prevent such mishaps.
     */
    @Override
    public LockResult lockInternal(long lsn,
                                   LockType lockType,
                                   boolean noWait,
                                   boolean jumpAheadOfWaiters,
                                   DatabaseImpl database)
        throws LockNotAvailableException, LockConflictException,
               DatabaseException {

        if (lockType.isWriteLock() && !database.allowReplicaWrite()) {
            disallowReplicaWrite();
        }
        return super.lockInternal
            (lsn, lockType, noWait, jumpAheadOfWaiters, database);
    }

    /**
     * If logging occurs before locking, we must screen out write locks here.
     *
     * If we allow the operation (e.g., for a NameLN), then be sure to call the
     * base class method to prepare to undo in the (very unlikely) event that
     * logging succeeds but locking fails. [#22875]
     */
    @Override
    public void preLogWithoutLock(DatabaseImpl database) {
        if (!database.allowReplicaWrite()) {
            disallowReplicaWrite();
        }
        super.preLogWithoutLock(database);
    }

    /**
     * Unconditionally throws ReplicaWriteException because this locker was
     * created on a replica.
     */
    @Override
    public void disallowReplicaWrite() {
        throw new ReplicaWriteException
            (this, ((RepImpl) envImpl).getStateChangeEvent());
    }

    /**
     * Verifies that consistency requirements are met before allowing the
     * transaction to proceed.
     */
    @Override
    protected void txnBeginHook(TransactionConfig config)
        throws ReplicaConsistencyException, DatabaseException {

        checkConsistency((RepImpl) envImpl, config.getConsistencyPolicy());
    }

    /**
     * Utility method used here and by ReplicaThreadLocker.
     */
    static void checkConsistency(final RepImpl repImpl,
                                 final ReplicaConsistencyPolicy policy) {
        if (State.DETACHED.equals(repImpl.getState()) ||
            State.MASTER.equals(repImpl.getState())) {
            /* Detached state, permit read-only access to the environment. */
            return;
        }
        assert (policy != null) : "Missing default consistency policy";
        try {
            policy.ensureConsistency(repImpl);
        } catch (InterruptedException e) {
            throw new ThreadInterruptedException(repImpl, e);
        } catch (MasterStateException e) {
            /*
             * Transitioned to master, while waiting for consistency, so the
             * txn is free to go ahead on the master.
             */
            return;
        }
    }
}
