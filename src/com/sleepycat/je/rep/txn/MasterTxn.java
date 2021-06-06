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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Logger;

import com.sleepycat.je.CommitToken;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Durability.ReplicaAckPolicy;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.LockConflictException;
import com.sleepycat.je.LockNotAvailableException;
import com.sleepycat.je.ThreadInterruptedException;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.LogItem;
import com.sleepycat.je.log.ReplicationContext;
import com.sleepycat.je.rep.InsufficientAcksException;
import com.sleepycat.je.rep.ReplicaWriteException;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.UnknownMasterException;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.impl.node.NameIdPair;
import com.sleepycat.je.rep.impl.node.Replay;
import com.sleepycat.je.rep.impl.node.Replica;
import com.sleepycat.je.txn.LockResult;
import com.sleepycat.je.txn.LockType;
import com.sleepycat.je.txn.Txn;
import com.sleepycat.je.txn.TxnManager;
import com.sleepycat.je.txn.WriteLockInfo;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.TestHook;
import com.sleepycat.je.utilint.TestHookExecute;
import com.sleepycat.je.utilint.VLSN;

/**
 * A MasterTxn represents:
 *  - a user initiated Txn executed on the Master node, when local-write and
 *    read-only are not configured, or
 *  - an auto-commit Txn on the Master node for a replicated DB.
 *
 * This class uses the hooks defined by Txn to support the durability
 * requirements of a replicated transaction on the Master.
 */
public class MasterTxn extends Txn {

    /* Holds the commit VLSN after a successful commit. */
    private VLSN commitVLSN = VLSN.NULL_VLSN;
    private final NameIdPair nameIdPair;
    private final UUID envUUID;

    /* The number of acks required by this txn commit. */
    private int requiredAckCount = -1;

    /* If this transaction requests an Arbiter ack. */
    private boolean needsArbiterAck;

    /*
     * Used to measure replicated transaction commit performance. All deltas
     * are measured relative to the start time, to minimize storage overhead.
     */

    /* The time the transaction was started. */
    private final long startMs = System.currentTimeMillis();

    /* The start relative delta time when the commit pre hook exited. */
    private int preLogCommitEndDeltaMs = 0;

    /*
     * The start relative delta time when the commit message was written to
     * the rep stream.
     */
    private int repWriteStartDeltaMs = 0;

    /**
     * Flag to keep track of whether this transaction has taken the read lock
     * that protects access to the blocking latch used by Master Transfer.
     */
    private boolean locked;

    /**
     * Flag to prevent any change to the txn's contents. Used in
     * master->replica transition. Intentionally volatile, so it can be
     * interleaved with use of the MasterTxn mutex.
     */
    private volatile boolean freeze;

    /* For unit testing */
    private TestHook<Integer> convertHook;

    /* The default factory used to create MasterTxns */
    private static final MasterTxnFactory DEFAULT_FACTORY =
        new MasterTxnFactory() {

            @Override
            public MasterTxn create(EnvironmentImpl envImpl,
                                    TransactionConfig config,
                                    NameIdPair nameIdPair) {
                return new MasterTxn(envImpl, config, nameIdPair);
            }

            @Override
            public MasterTxn createNullTxn(EnvironmentImpl envImpl,
                                           TransactionConfig config,
                                           NameIdPair nameIdPair) {

                return new MasterTxn(envImpl, config, nameIdPair) {
                    @Override
                    protected boolean updateLoggedForTxn() {
                        /*
                         * Return true so that the commit will be logged even
                         * though there are no changes associated with this txn
                         */
                        return true;
                    }
                };
            }
    };

    /* The current Txn Factory. */
    private static MasterTxnFactory factory = DEFAULT_FACTORY;

    public MasterTxn(EnvironmentImpl envImpl,
                     TransactionConfig config,
                     NameIdPair nameIdPair)
        throws DatabaseException {

        super(envImpl, config, ReplicationContext.MASTER);
        this.nameIdPair = nameIdPair;
        this.envUUID = ((RepImpl) envImpl).getUUID();
        assert !config.getLocalWrite();
    }

    @Override
    public boolean isLocalWrite() {
        return false;
    }

    /**
     * Returns the transaction commit token used to identify the transaction.
     *
     * @see com.sleepycat.je.txn.Txn#getCommitToken()
     */
    @Override
    public CommitToken getCommitToken() {
        if (commitVLSN.isNull()) {
            return null;
        }
        return new CommitToken(envUUID, commitVLSN.getSequence());
    }

    public VLSN getCommitVLSN() {
        return commitVLSN;
    }

    /**
     * MasterTxns use txn ids from a reserved negative space. So override
     * the default generation of ids.
     */
    @Override
    protected long generateId(TxnManager txnManager,
                              long ignore /* mandatedId */) {
        assert(ignore == 0);
        return txnManager.getNextReplicatedTxnId();
    }

    /**
     * Causes the transaction to wait until we have sufficient replicas to
     * acknowledge the commit.
     */
    @Override
    protected void txnBeginHook(TransactionConfig config)
        throws DatabaseException {

        RepImpl repImpl = (RepImpl) envImpl;
        try {
            repImpl.txnBeginHook(this);
        } catch (InterruptedException e) {
            throw new ThreadInterruptedException(envImpl, e);
        }
    }

    @Override
    protected void preLogCommitHook()
        throws DatabaseException {

        RepImpl repImpl = (RepImpl) envImpl;
        ReplicaAckPolicy ackPolicy = getCommitDurability().getReplicaAck();
        requiredAckCount =
            repImpl.getRepNode().getDurabilityQuorum().
            getCurrentRequiredAckCount(ackPolicy);

        /*
         * TODO: An optimization we'd like to do is to identify transactions
         * that only modify non-replicated databases, so they can avoid waiting
         * for Replica commit acks and avoid checks like the one that requires
         * that the node be a master before proceeding with the transaction.
         */
        repImpl.preLogCommitHook(this);
        preLogCommitEndDeltaMs = (int) (System.currentTimeMillis() - startMs);
    }

    @Override
    protected void postLogCommitHook(LogItem commitItem)
        throws DatabaseException {

        commitVLSN = commitItem.header.getVLSN();
        try {
            RepImpl repImpl = (RepImpl) envImpl;
            repImpl.postLogCommitHook(this, commitItem);
        } catch (InterruptedException e) {
            throw new ThreadInterruptedException(envImpl, e);
        }
    }

    @Override
    protected void preLogAbortHook()
        throws DatabaseException {

        RepImpl repImpl = (RepImpl) envImpl;
        repImpl.preLogAbortHook(this);
    }

    @Override
    protected void postLogCommitAbortHook() {

        RepImpl repImpl = (RepImpl) envImpl;
        repImpl.postLogCommitAbortHook(this);
    }

    @Override
    protected void postLogAbortHook() {
        RepImpl repImpl = (RepImpl)envImpl;
        repImpl.postLogAbortHook(this);
    }

    /**
     * Prevent this MasterTxn from taking locks if the node becomes a
     * replica. The application has a reference to this Txn, and may
     * attempt to use it well after the node has transitioned from master
     * to replica.
     */
    @Override
    public LockResult lockInternal(long lsn,
                                   LockType lockType,
                                   boolean noWait,
                                   boolean jumpAheadOfWaiters,
                                   DatabaseImpl database)
        throws LockNotAvailableException, LockConflictException,
               DatabaseException {
        ReplicatedEnvironment.State nodeState = ((RepImpl)envImpl).getState();
        if (nodeState.isMaster()) {
            return super.lockInternal
                (lsn, lockType, noWait, jumpAheadOfWaiters, database);
        }

        throwNotMaster(nodeState);
        return null; /* not reached */
    }

    private void throwNotMaster(ReplicatedEnvironment.State nodeState) {
        if (nodeState.isReplica()) {
            throw new ReplicaWriteException
                (this, ((RepImpl)envImpl).getStateChangeEvent());
        }
        throw new UnknownMasterException
            ("Transaction " + getId() +
             " cannot execute write operations because this node is" +
             " no longer a master");
    }

    /**
     * If logging occurs before locking, we must screen out write locks here.
     */
    @Override
    public synchronized void preLogWithoutLock(DatabaseImpl database) {
        ReplicatedEnvironment.State nodeState = ((RepImpl)envImpl).getState();
        if (nodeState.isMaster()) {
            super.preLogWithoutLock(database);
            return;
        }

        throwNotMaster(nodeState);
    }

    /**
     * Determines whether we should lock the block-latch lock.
     * <p>
     * We acquire the lock during pre-log hook, and release it during post-log
     * hook.  Specifically, there are the following cases:
     * <ol>
     * <li>
     * For a normal commit, we acquire it in {@code preLogCommitHook()} and
     * release it in {@code postLogCommitHook()}
     * <li>
     * For a normal abort (invoked by the application on the {@code
     * Txn.abort()} API), we acquire the lock in {@code preLogAbortHook()} and
     * release it in {@code postLogAbortHook()}.
     * <li>
     * When a commit fails in such a way as to call {@code
     * Txn.throwPreCommitException()}, we go through the abort path as well.
     * In this case:
     * <ul>
     * <li>we will of course already have called {@code preLogCommitHook()};
     * <li>the abort path calls {@code preLogAbortHook()} and {@code
     * postLogAbortHook()} as always;
     * <li>finally we call {@code postLogCommitAbortHook()}
     * </ul>
     * Fortunately we can avoid the complexity of dealing with a second
     * (recursive) lock acquisition here, because by the time either post-hook
     * is called we've done any writing of VLSNs.  Thus, when we want to
     * take the lock, we take it if it hasn't already been taken, and do
     * nothing if it has; when releasing, we release it if we have it, and do
     * nothing if we don't.
     * </ol>
     * <p>
     * See additional javadoc at {@code RepImpl.blockLatchLock}
     */
    public boolean lockOnce() {
        if (locked) {
            return false;
        }
        locked = true;
        return true;
    }

    /**
     * Determines whether we should unlock the block-latch lock.
     *
     * @see #lockOnce
     */
    public boolean unlockOnce() {
        if (locked) {
            locked = false;
            return true;
        }
        return false;
    }

    public int getRequiredAckCount() {
        return requiredAckCount;
    }

    public void resetRequiredAckCount() {
        requiredAckCount = 0;
    }

    /** A masterTxn always writes its own id into the commit or abort. */
    @Override
    protected int getReplicatorNodeId() {
        return nameIdPair.getId();
    }

    @Override
    protected long getDTVLSN() {
        /*
         * For the master transaction, it should always be null, and will
         * be corrected under the write log latch on its way to disk.
         */
        return VLSN.NULL_VLSN_SEQUENCE;
    }

    public long getStartMs() {
        return startMs;
    }

    public void stampRepWriteTime() {
        this.repWriteStartDeltaMs =
            (int)(System.currentTimeMillis() - startMs);
    }

    /**
     * Returns the amount of time it took to copy the commit record from the
     * log buffer to the rep stream. It's measured as the time interval
     * starting with the time the preCommit hook completed, to the time the
     * message write to the replication stream was initiated.
     */
    public long messageTransferMs() {
        return repWriteStartDeltaMs > 0 ?

                (repWriteStartDeltaMs - preLogCommitEndDeltaMs) :

                /*
                 * The message was invoked before the post commit hook fired.
                 */
                0;
    }

    @Override
    protected boolean
        propagatePostCommitException(DatabaseException postCommitException) {
        return (postCommitException instanceof InsufficientAcksException) ?
                true :
                super.propagatePostCommitException(postCommitException);
    }

    /* The Txn factory interface. */
    public interface MasterTxnFactory {
        MasterTxn create(EnvironmentImpl envImpl,
                         TransactionConfig config,
                         NameIdPair nameIdPair);

        /**
         * Create a special "null" txn that does not result in any changes to
         * the environment. It's sole purpose is to persist and communicate
         * DTVLSN values.
         */
        MasterTxn createNullTxn(EnvironmentImpl envImpl,
                                TransactionConfig config,
                                NameIdPair nameIdPair);
    }

    /* The method used to create user Master Txns via the factory. */
    public static MasterTxn create(EnvironmentImpl envImpl,
                                   TransactionConfig config,
                                   NameIdPair nameIdPair) {
        return factory.create(envImpl, config, nameIdPair);
    }

    public static MasterTxn createNullTxn(EnvironmentImpl envImpl,
                                   TransactionConfig config,
                                   NameIdPair nameIdPair) {
        return factory.createNullTxn(envImpl, config, nameIdPair);
    }

    /**
     * Method used for unit testing.
     *
     * Sets the factory to the one supplied. If the argument is null it
     * restores the factory to the original default value.
     */
    public static void setFactory(MasterTxnFactory factory) {
        MasterTxn.factory = (factory == null) ? DEFAULT_FACTORY : factory;
    }

    /**
     * Convert a MasterTxn that has any write locks into a ReplayTxn, and close
     * the MasterTxn after it is disemboweled. A MasterTxn that only has read
     * locks is unchanged and is still usable by the application. To be clear,
     * the application can never use a MasterTxn to obtain a lock if the node
     * is in Replica mode, but may indeed be able to use a read-lock-only
     * MasterTxn if the node cycles back into Master status.
     *
     * For converted MasterTxns, all write locks are transferred to a replay
     * transaction, read locks are released, and the txn is closed. Used when a
     * node is transitioning from master to replica mode without recovery,
     * which may happen for an explicit master transfer request, or merely for
     * a network partition/election of new
     * master.
     *
     * The newly created replay transaction will need to be in the appropriate
     * state, holding all write locks, so that the node in replica form can
     * execute the proper syncups.  Note that the resulting replay txn will
     * only be aborted, and will never be committed, because the txn originated
     * on this node, which is transitioning from master -> replica.
     *
     * We only transfer write locks. We need not transfer read locks, because
     * replays only operate on writes, and are never required to obtain read
     * locks. Read locks are released though, because (a) this txn is now only
     * abortable, and (b) although the Replay can preempt any read locks held
     * by the MasterTxn, such preemption will add delay.
     *
     * @return a ReplayTxn, if there were locks in this transaction, and
     * there's a need to create a ReplayTxn.
     */
    public ReplayTxn convertToReplayTxnAndClose(Logger logger,
                                                Replay replay) {

        /* Assertion */
        if (!freeze) {
            throw EnvironmentFailureException.unexpectedState
                (envImpl,
                 "Txn " + getId() +
                 " should be frozen when converting to replay txn");
        }

        /*
         * This is an important and relatively rare operation, and worth
         * logging.
         */
        LoggerUtils.info(logger, envImpl,
                         "Transforming txn " + getId() +
                         " from MasterTxn to ReplayTxn");

        int hookCount = 0;
        ReplayTxn replayTxn = null;
        boolean needToClose = true;
        try {
            synchronized (this) {

                if (isClosed()) {
                    LoggerUtils.info(logger, envImpl,
                                     "Txn " + getId() +
                                     " is closed, no tranform needed");
                    needToClose = false;
                    return null;
                }

                /*
                 * Get the list of write locks, and process them in lsn order,
                 * so we properly maintain the lastLoggedLsn and firstLoggedLSN
                 * fields of the newly created ReplayTxn.
                 */
                final Set<Long> lockedLSNs = getWriteLockIds();

                /*
                 * This transaction may not actually have any write locks. In
                 * that case, we permit it to live on.
                 */
                if (lockedLSNs.size() == 0) {
                    LoggerUtils.info(logger, envImpl, "Txn " + getId() +
                                     " had no write locks, didn't create" +
                                     " ReplayTxn");
                    needToClose = false;
                    return null;
                }

                /*
                 * We have write locks. Make sure that this txn can now
                 * only be aborted. Otherwise, there could be this window
                 * in this method:
                 *  t1: locks stolen, no locks left in this txn
                 *  t2: txn unfrozen, commits and aborts possible
                 *    -- at this point, another thread could sneak in and
                 *    -- try to commit. The txn would commmit successfully,
                 *    -- because a commit w/no write locks is a no-op.
                 *    -- but that would convey the false impression that the
                 *    -- txn's write operations had commmitted.
                 *  t3: txn is closed
                 */
                setOnlyAbortable(new UnknownMasterException
                                 (envImpl.getName() +
                                  " is no longer a master"));
                replayTxn = replay.getReplayTxn(getId(), false);

                /*
                 * Order the lsns, so that the locks are taken in the proper
                 * order, and the txn's firstLoggedLsn and lastLoggedLsn fields
                 * are properly set.
                 */
                List<Long> sortedLsns = new ArrayList<>(lockedLSNs);
                Collections.sort(sortedLsns);
                LoggerUtils.info(logger, envImpl,
                                 "Txn " + getId()  + " has " +
                                 lockedLSNs.size() + " locks to transform");

                /*
                 * Transfer each lock. Note that ultimately, since mastership
                 * is changing, and replicated commits will only be executed
                 * when a txn has originated on that node, the target ReplayTxn
                 * can never be committed, and will only be aborted.
                 */
                for (Long lsn: sortedLsns) {

                    LoggerUtils.info(logger, envImpl,
                                     "Txn " + getId() +
                                     " is transferring lock " + lsn);

                    /*
                     * Use a special method to steal the lock. Another approach
                     * might have been to have the replayTxn merely attempt a
                     * lock(); as an importunate txn, the replayTxn would
                     * preempt the MasterTxn's lock. However, that path doesn't
                     * work because lock() requires having a databaseImpl in
                     * hand, and that's not available here.
                     */
                    replayTxn.stealLockFromMasterTxn(lsn);

                    /*
                     * Copy all the lock's info into the Replay and remove it
                     * from the master. Normally, undo clears write locks, but
                     * this MasterTxn will not be executing undo.
                     */
                    WriteLockInfo replayWLI = replayTxn.getWriteLockInfo(lsn);
                    WriteLockInfo masterWLI = getWriteLockInfo(lsn);
                    replayWLI.copyAllInfo(masterWLI);
                    removeLock(lsn);
                }

                /*
                 * Txns have collections of undoDatabases and deletedDatabases.
                 * Undo databases are normally incrementally added to the txn
                 * as locks are obtained Unlike normal locking or recovery
                 * locking, in this case we don't have a reference to the
                 * databaseImpl that goes with this lock, so we copy the undo
                 * collection in one fell swoop.
                 */
                replayTxn.copyDatabasesForConversion(this);

                /*
                 * This txn is no longer responsible for databaseImpl
                 * cleanup, as that issue now lies with the ReplayTxn, so
                 * remove the collection.
                 */
                deletedDatabases = null;

                /*
                 * All locks have been removed from this transaction. Clear
                 * the firstLoggedLsn and lastLoggedLsn so there's no danger
                 * of attempting to undo anything; this txn is no longer
                 * responsible for any records.
                 */
                lastLoggedLsn = DbLsn.NULL_LSN;
                firstLoggedLsn = DbLsn.NULL_LSN;

                /* If this txn also had read locks, clear them */
                clearReadLocks();
            }
        } finally {

            assert TestHookExecute.doHookIfSet(convertHook, hookCount++);

            unfreeze();

            assert TestHookExecute.doHookIfSet(convertHook, hookCount++);

            /*
             * We need to abort the txn, but we can't call abort() because that
             * method checks whether we are the master! Instead, call the
             * internal method, close(), in order to end this transaction and
             * unregister it from the transactionManager. Must be called
             * outside the synchronization block.
             */
            if (needToClose) {
                LoggerUtils.info(logger, envImpl, "About to close txn " +
                                 getId() + " state=" + getState());
                close(false /*isCommit */);
                LoggerUtils.info(logger, envImpl, "Closed txn " +  getId() +
                                 " state=" + getState());
            }
            assert TestHookExecute.doHookIfSet(convertHook, hookCount++);
        }

        return replayTxn;
    }

    public void freeze() {
        freeze = true;
    }

    private void unfreeze() {
        freeze = false;
    }

    /**
     * Used to hold the transaction stable while it is being cloned as a
     * ReplayTxn, during master->replica transitions. Essentially, there
     * are two parties that now have a reference to this transaction -- the
     * originating application thread, and the RepNode thread that is
     * trying to set up internal state so it can begin to act as a replica.
     *
     * The transaction will throw UnknownMasterException or
     * ReplicaWriteException if the transaction is frozen, so that the
     * application knows that the transaction is no longer viable, but it
     * doesn't attempt to do most of the follow-on cleanup and release of locks
     * that failed aborts and commits normally attempt. One aspect of
     * transaction cleanup can't be skipped though. It is necessary to do the
     * post log hooks to free up the block txn latch lock so that the
     * transaction can be closed by the RepNode thread. For example:
     * - application thread starts transaction
     * - application takes the block txn latch lock and attempts commit or
     * abort, but is stopped because the txn is frozen by master transfer.
     * - the application must release the block txn latch lock.
     * @see Replica#replicaTransitionCleanup
     */
    @Override
    protected void checkIfFrozen(boolean isCommit) {
        if (freeze) {
            try {
                ((RepImpl) envImpl).checkIfMaster(this);
            } catch (DatabaseException e) {
                if (isCommit) {
                    postLogCommitAbortHook();
                } else {
                    postLogAbortHook();
                }
                throw e;
            }
        }
    }

    /* For unit testing */
    public void setConvertHook(TestHook<Integer> hook) {
        convertHook = hook;
    }

    @Override
    public boolean isMasterTxn() {
        return true;
    }

    public void setArbiterAck(boolean val) {
        needsArbiterAck = val;
    }

    public boolean getArbiterAck() {
        return needsArbiterAck;
    }

}
