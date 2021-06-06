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

package com.sleepycat.je;

import java.util.concurrent.TimeUnit;

import com.sleepycat.je.Durability.ReplicaAckPolicy;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.txn.Locker;
import com.sleepycat.je.txn.Txn;
import com.sleepycat.je.utilint.PropUtil;

/**
 * The Transaction object is the handle for a transaction.  Methods off the
 * transaction handle are used to configure, abort and commit the transaction.
 * Transaction handles are provided to other Berkeley DB methods in order to
 * transactionally protect those operations.
 *
 * <p>A single Transaction may be used to protect operations for any number of
 * Databases in a given environment.  However, a single Transaction may not be
 * used for operations in more than one distinct environment.</p>
 *
 * <p>Transaction handles are free-threaded; transactions handles may be used
 * concurrently by multiple threads. Once the {@link Transaction#abort
 * Transaction.abort} or {@link Transaction#commit Transaction.commit} method
 * is called, the handle may not be accessed again, regardless of the success
 * or failure of the method, with one exception:  the {@code abort} method may
 * be called any number of times to simplify error handling.</p>
 *
 * <p>To obtain a transaction with default attributes:</p>
 *
 * <blockquote><pre>
 *     Transaction txn = myEnvironment.beginTransaction(null, null);
 * </pre></blockquote>
 *
 * <p>To customize the attributes of a transaction:</p>
 *
 * <blockquote><pre>
 *     TransactionConfig config = new TransactionConfig();
 *     config.setReadUncommitted(true);
 *     Transaction txn = myEnvironment.beginTransaction(null, config);
 * </pre></blockquote>
 */
public class Transaction {

    /**
     * The current state of the transaction.
     *
     * @since 5.0.48
     */
    public enum State {

        /**
         * The transaction has not been committed or aborted, and can be used
         * for performing operations.  This state is also indicated if {@link
         * #isValid} returns true.  For all other states, {@link #isValid} will
         * return false.
         */
        OPEN,

        /**
         * An exception was thrown by the {@code commit} method due to an error
         * that occurred while attempting to make the transaction durable.  The
         * transaction may or may not be locally durable, according to the
         * {@link Durability#getLocalSync local SyncPolicy} requested.
         * <p>
         * This is an unusual situation and is normally due to a system
         * failure, storage device failure, disk full condition, thread
         * interrupt, or a bug of some kind.  When a transaction is in this
         * state, the Environment will have been {@link Environment#isValid()
         * invalidated} by the error.
         * <p>
         * In a replicated environment, a transaction in this state is not
         * transferred to replicas.  If it turns out that the transaction is
         * indeed durable, it will be transferred to replicas via normal
         * replication mechanisms when the Environment is re-opened.
         * <p>
         * When the {@code commit} method throws an exception and the
         * transaction is in the {@code POSSIBLY_COMMITTED} state, some
         * applications may wish to perform a data query to determine whether
         * the transaction is durable or not.  Note that in the event of a
         * system level failure, the reads themselves may be unreliable, e.g.
         * the data may be in the file system cache but not on disk. Other
         * applications may wish to repeat the transaction unconditionally,
         * after resolving the error condition, particularly when the set of
         * operations in the transaction is designed to be idempotent.
         */
        POSSIBLY_COMMITTED,

        /**
         * The transaction has been committed and is locally durable according
         * to the {@link Durability#getLocalSync local SyncPolicy} requested.
         * <p>
         * Note that a transaction may be in this state even when an exception
         * is thrown by the {@code commit} method.  For example, in a
         * replicated environment, an {@link
         * com.sleepycat.je.rep.InsufficientAcksException} may be thrown after
         * the transaction is committed locally.
         */
        COMMITTED,

        /**
         * The transaction has been invalidated by an exception and cannot be
         * committed.  See {@link OperationFailureException} for a description
         * of how a transaction can become invalid.  The application is
         * responsible for aborting the transaction.
         */
        MUST_ABORT,

        /**
         * The transaction has been aborted.
         */
        ABORTED,
    }

    private Txn txn;
    private final Environment env;
    private final long id;
    private String name;

    /*
     * It's set upon a successful updating replicated commit and identifies the
     * VLSN associated with the commit entry.
     */
    private CommitToken commitToken = null;

    /*
     * Is null until setTxnNull is called, and then it holds the state at the
     * time the txn was closed.
     */
    private State finalState = null;

    /*
     * Commit and abort methods are synchronized to prevent them from running
     * concurrently with operations using the transaction.  See
     * Cursor.getTxnSynchronizer.
     */

    /**
     * For internal use.
     * @hidden
     * Creates a transaction.
     */
    protected Transaction(Environment env, Txn txn) {
        this.env = env;
        this.txn = txn;
        txn.setTransaction(this);

        /*
         * Copy the id to this wrapper object so the id will be available
         * after the transaction is closed and the txn field is nulled.
         */
        this.id = txn.getId();
    }

    /**
     * Cause an abnormal termination of the transaction.
     *
     * <p>The log is played backward, and any necessary undo operations are
     * done. Before Transaction.abort returns, any locks held by the
     * transaction will have been released.</p>
     *
     * <p>In the case of nested transactions, aborting a parent transaction
     * causes all children (unresolved or not) of the parent transaction to be
     * aborted.</p>
     *
     * <p>All cursors opened within the transaction must be closed before the
     * transaction is aborted.</p>
     *
     * <p>After this method has been called, regardless of its return, the
     * {@link Transaction} handle may not be accessed again, with one
     * exception:  the {@code abort} method itself may be called any number of
     * times to simplify error handling.</p>
     *
     * <p>WARNING: To guard against memory leaks, the application should
     * discard all references to the closed handle.  While BDB makes an effort
     * to discard references from closed objects to the allocated memory for an
     * environment, this behavior is not guaranteed.  The safe course of action
     * for an application is to discard all references to closed BDB
     * objects.</p>
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if the environment has been closed, or
     * cursors associated with the transaction are still open.
     */
    public synchronized void abort()
        throws DatabaseException {

        try {

            /*
             * If the transaction is already closed, do nothing.  Do not call
             * checkOpen in order to support any number of calls to abort().
             */
            if (txn == null) {
                return;
            }

            /*
             * Check env only after checking for closed txn, to mimic close()
             * behavior for Cursors, etc, and avoid unnecessary exception
             * handling.  [#21264]
             */
            checkEnv();

            env.removeReferringHandle(this);
            txn.abort();

            /* Remove reference to internal txn, so we can reclaim memory. */
            setTxnNull();
        } catch (Error E) {
            DbInternal.getNonNullEnvImpl(env).invalidate(E);
            throw E;
        }
    }

    /**
     * Return the transaction's unique ID.
     *
     * @return The transaction's unique ID.
     */
    public long getId() {
        return id;
    }

    /**
     * This method is intended for use with a replicated environment.
     * <p>
     * It returns the commitToken associated with a successful replicated
     * commit. A null value is returned if the txn was not associated with a
     * replicated environment, or the txn did not result in any changes to the
     * environment. This method should only be called after the transaction
     * has finished.
     * <p>
     * This method is typically used in conjunction with the <code>
     * CommitPointConsistencyPolicy</code>.
     *
     * @return the token used to identify the replicated commit. Return null if
     * the transaction has aborted, or has committed without making any
     * updates.
     *
     * @throws IllegalStateException if the method is called before the
     * transaction has committed or aborted.
     *
     * @see com.sleepycat.je.rep.CommitPointConsistencyPolicy
     */
    public CommitToken getCommitToken() 
        throws IllegalStateException {

        if (txn == null) {

            /* 
             * The commit token is only legitimate after the transaction is
             * closed. A null txn field means the transaction is closed.
             */
            return commitToken; 
        }
        
        throw new IllegalStateException
           ("This transaction is still in progress and a commit token " +
            "is not available");
    }

    /**
     * End the transaction.  If the environment is configured for synchronous
     * commit, the transaction will be committed synchronously to stable
     * storage before the call returns.  This means the transaction will
     * exhibit all of the ACID (atomicity, consistency, isolation, and
     * durability) properties.
     *
     * <p>If the environment is not configured for synchronous commit, the
     * commit will not necessarily have been committed to stable storage before
     * the call returns.  This means the transaction will exhibit the ACI
     * (atomicity, consistency, and isolation) properties, but not D
     * (durability); that is, database integrity will be maintained, but it is
     * possible this transaction may be undone during recovery.</p>
     *
     * <p>All cursors opened within the transaction must be closed before the
     * transaction is committed.</p>
     *
     * <p>If the method encounters an error, the transaction <!-- and all child
     * transactions of the transaction --> will have been aborted when the call
     * returns.</p>
     *
     * <p>After this method has been called, regardless of its return, the
     * {@link Transaction} handle may not be accessed again, with one
     * exception:  the {@code abort} method may be called any number of times
     * to simplify error handling.</p>
     *
     * <p>WARNING: To guard against memory leaks, the application should
     * discard all references to the closed handle.  While BDB makes an effort
     * to discard references from closed objects to the allocated memory for an
     * environment, this behavior is not guaranteed.  The safe course of action
     * for an application is to discard all references to closed BDB
     * objects.</p>
     *
     * @throws com.sleepycat.je.rep.InsufficientReplicasException if the master
     * in a replicated environment could not contact a quorum of replicas as
     * determined by the {@link ReplicaAckPolicy}.
     *
     * @throws com.sleepycat.je.rep.InsufficientAcksException if the master in
     * a replicated environment did not receive enough replica acknowledgments,
     * although the commit succeeded locally.
     *
     * @throws com.sleepycat.je.rep.ReplicaWriteException if a write operation
     * was performed with this transaction, but this node is now a Replica.
     *
     * @throws OperationFailureException if this exception occurred earlier and
     * caused the transaction to be invalidated.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if the transaction or environment has been
     * closed, or cursors associated with the transaction are still open.
     */
    public synchronized void commit()
        throws DatabaseException {

        try {
            checkEnv();
            checkOpen();
            env.removeReferringHandle(this);
            txn.commit();
            commitToken = txn.getCommitToken();
            /* Remove reference to internal txn, so we can reclaim memory. */
            setTxnNull();
        } catch (Error E) {
            DbInternal.getNonNullEnvImpl(env).invalidate(E);
            throw E;
        }
    }

    /**
     * End the transaction using the specified durability requirements. This
     * requirement overrides any default durability requirements associated
     * with the environment. If the durability requirements cannot be satisfied,
     * an exception is thrown to describe the problem. Please see
     * {@link Durability} for specific exceptions that could result when the
     * durability requirements cannot be satisfied.
     *
     * <p>All cursors opened within the transaction must be closed before the
     * transaction is committed.</p>
     *
     * <p>If the method encounters an error, the transaction <!-- and all child
     * transactions of the transaction --> will have been aborted when the call
     * returns.</p>
     *
     * <p>After this method has been called, regardless of its return, the
     * {@link Transaction} handle may not be accessed again, with one
     * exception:  the {@code abort} method may be called any number of times
     * to simplify error handling.</p>
     *
     * <p>WARNING: To guard against memory leaks, the application should
     * discard all references to the closed handle.  While BDB makes an effort
     * to discard references from closed objects to the allocated memory for an
     * environment, this behavior is not guaranteed.  The safe course of action
     * for an application is to discard all references to closed BDB
     * objects.</p>
     *
     * @param durability the durability requirements for this transaction
     *
     * @throws com.sleepycat.je.rep.InsufficientReplicasException if the master
     * in a replicated environment could not contact enough replicas to
     * initiate the commit.
     *
     * @throws com.sleepycat.je.rep.InsufficientAcksException if the master in
     * a replicated environment did not receive enough replica acknowledgments,
     * althought the commit succeeded locally.
     *
     * @throws com.sleepycat.je.rep.ReplicaWriteException if a write operation
     * was performed with this transaction, but this node is now a Replica.
     *
     * @throws OperationFailureException if this exception occurred earlier and
     * caused the transaction to be invalidated.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if the transaction or environment has been
     * closed, or cursors associated with the transaction are still open.
     *
     * @throws IllegalArgumentException if an invalid parameter is specified.
     */
    public synchronized void commit(Durability durability)
        throws DatabaseException {

        doCommit(durability, false /* explicitSync */);
    }

    /**
     * End the transaction, writing to stable storage and committing
     * synchronously.  This means the transaction will exhibit all of the ACID
     * (atomicity, consistency, isolation, and durability) properties.
     *
     * <p>This behavior is the default for database environments unless
     * otherwise configured using the {@link
     * com.sleepycat.je.EnvironmentConfig#setTxnNoSync
     * EnvironmentConfig.setTxnNoSync} method.  This behavior may also be set
     * for a single transaction using the {@link
     * com.sleepycat.je.Environment#beginTransaction
     * Environment.beginTransaction} method.  Any value specified to this
     * method overrides both of those settings.</p>
     *
     * <p>All cursors opened within the transaction must be closed before the
     * transaction is committed.</p>
     *
     * <p>If the method encounters an error, the transaction <!-- and all child
     * transactions of the transaction --> will have been aborted when the call
     * returns.</p>
     *
     * <p>After this method has been called, regardless of its return, the
     * {@link Transaction} handle may not be accessed again, with one
     * exception:  the {@code abort} method may be called any number of times
     * to simplify error handling.</p>
     *
     * <p>WARNING: To guard against memory leaks, the application should
     * discard all references to the closed handle.  While BDB makes an effort
     * to discard references from closed objects to the allocated memory for an
     * environment, this behavior is not guaranteed.  The safe course of action
     * for an application is to discard all references to closed BDB
     * objects.</p>
     *
     * @throws com.sleepycat.je.rep.InsufficientReplicasException if the master
     * in a replicated environment could not contact enough replicas to
     * initiate the commit.
     *
     * @throws com.sleepycat.je.rep.InsufficientAcksException if the master in
     * a replicated environment did not receive enough replica acknowledgments,
     * althought the commit succeeded locally.
     *
     * @throws com.sleepycat.je.rep.ReplicaWriteException if a write operation
     * was performed with this transaction, but this node is now a Replica.
     *
     * @throws OperationFailureException if this exception occurred earlier and
     * caused the transaction to be invalidated.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if the transaction or environment has been
     * closed, or cursors associated with the transaction are still open.
     */
    public synchronized void commitSync()
        throws DatabaseException {

        doCommit(Durability.COMMIT_SYNC, true /* explicitSync */);
    }

    /**
     * End the transaction, not writing to stable storage and not committing
     * synchronously. This means the transaction will exhibit the ACI
     * (atomicity, consistency, and isolation) properties, but not D
     * (durability); that is, database integrity will be maintained, but it is
     * possible this transaction may be undone during recovery.
     *
     * <p>This behavior may be set for a database environment using the {@link
     * com.sleepycat.je.EnvironmentConfig#setTxnNoSync
     * EnvironmentConfig.setTxnNoSync} method or for a single transaction using
     * the {@link com.sleepycat.je.Environment#beginTransaction
     * Environment.beginTransaction} method.  Any value specified to this
     * method overrides both of those settings.</p>
     *
     * <p>All cursors opened within the transaction must be closed before the
     * transaction is committed.</p>
     *
     * <p>If the method encounters an error, the transaction <!-- and all child
     * transactions of the transaction --> will have been aborted when the call
     * returns.</p>
     *
     * <p>After this method has been called, regardless of its return, the
     * {@link Transaction} handle may not be accessed again, with one
     * exception:  the {@code abort} method may be called any number of times
     * to simplify error handling.</p>
     *
     * <p>WARNING: To guard against memory leaks, the application should
     * discard all references to the closed handle.  While BDB makes an effort
     * to discard references from closed objects to the allocated memory for an
     * environment, this behavior is not guaranteed.  The safe course of action
     * for an application is to discard all references to closed BDB
     * objects.</p>
     *
     * @throws com.sleepycat.je.rep.InsufficientReplicasException if the master
     * in a replicated environment could not contact enough replicas to
     * initiate the commit.
     *
     * @throws com.sleepycat.je.rep.InsufficientAcksException if the master in
     * a replicated environment did not receive enough replica acknowledgments,
     * althought the commit succeeded locally.
     *
     * @throws com.sleepycat.je.rep.ReplicaWriteException if a write operation
     * was performed with this transaction, but this node is now a Replica.
     *
     * @throws OperationFailureException if this exception occurred earlier and
     * caused the transaction to be invalidated.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if the transaction or environment has been
     * closed, or cursors associated with the transaction are still open.
     */
    public synchronized void commitNoSync()
        throws DatabaseException {

        doCommit(Durability.COMMIT_NO_SYNC, true /* explicitSync */);
    }

    /**
     * End the transaction, writing to stable storage but not committing
     * synchronously.  This means the transaction will exhibit the ACI
     * (atomicity, consistency, and isolation) properties, but not D
     * (durability); that is, database integrity will be maintained, but it is
     * possible this transaction may be undone during recovery.
     *
     * <p>This behavior is the default for database environments unless
     * otherwise configured using the {@link
     * com.sleepycat.je.EnvironmentConfig#setTxnNoSync
     * EnvironmentConfig.setTxnNoSync} method.  This behavior may also be set
     * for a single transaction using the {@link
     * com.sleepycat.je.Environment#beginTransaction
     * Environment.beginTransaction} method.  Any value specified to this
     * method overrides both of those settings.</p>
     *
     * <p>All cursors opened within the transaction must be closed before the
     * transaction is committed.</p>
     *
     * <p>If the method encounters an error, the transaction <!-- and all child
     * transactions of the transaction --> will have been aborted when the call
     * returns.</p>
     *
     * <p>After this method has been called, regardless of its return, the
     * {@link Transaction} handle may not be accessed again, with one
     * exception:  the {@code abort} method may be called any number of times
     * to simplify error handling.</p>
     *
     * <p>WARNING: To guard against memory leaks, the application should
     * discard all references to the closed handle.  While BDB makes an effort
     * to discard references from closed objects to the allocated memory for an
     * environment, this behavior is not guaranteed.  The safe course of action
     * for an application is to discard all references to closed BDB
     * objects.</p>
     *
     * @throws com.sleepycat.je.rep.InsufficientReplicasException if the master
     * in a replicated environment could not contact enough replicas to
     * initiate the commit.
     *
     * @throws com.sleepycat.je.rep.InsufficientAcksException if the master in
     * a replicated environment did not receive enough replica acknowledgments,
     * althought the commit succeeded locally.
     *
     * @throws com.sleepycat.je.rep.ReplicaWriteException if a write operation
     * was performed with this transaction, but this node is now a Replica.
     *
     * @throws OperationFailureException if this exception occurred earlier and
     * caused the transaction to be invalidated.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if the transaction or environment has been
     * closed, or cursors associated with the transaction are still open.
     */
    public synchronized void commitWriteNoSync()
        throws DatabaseException {

        doCommit(Durability.COMMIT_WRITE_NO_SYNC, true /* explicitSync */);
    }

    /**
     * For internal use.
     * @hidden
     */
    public boolean getPrepared() {
        return txn.getPrepared();
    }

    /**
     * Perform error checking and invoke the commit on Txn.
     *
     * @param durability the durability to use for the commit
     * @param explicitSync true if the method was invoked from one of the
     * sync-specific APIs, false if durability was used explicitly. This
     * parameter exists solely to support mixed mode api usage checks.
     *
     * @throws IllegalArgumentException via commit(Durability)
     */
    private void doCommit(Durability durability, boolean explicitSync) {
        try {
            checkEnv();
            checkOpen();
            env.removeReferringHandle(this);
            if (explicitSync) {
                /* A sync-specific api was invoked. */
                if (txn.getExplicitDurabilityConfigured()) {
                    throw new IllegalArgumentException
                        ("Mixed use of deprecated durability API for the " +
                         "transaction commit with the new durability API for" +
                         " TransactionConfig or MutableEnvironmentConfig");
                }
            } else if (txn.getExplicitSyncConfigured()) {
                /* Durability was explicitly configured for commit */
                throw new IllegalArgumentException
                    ("Mixed use of new durability API for the " +
                      "transaction commit with deprecated durability API for" +
                      " TransactionConfig or MutableEnvironmentConfig");
            }
            txn.commit(durability);
            commitToken = txn.getCommitToken();
            /* Remove reference to internal txn, so we can reclaim memory. */
            setTxnNull();
        } catch (Error E) {
            DbInternal.getNonNullEnvImpl(env).invalidate(E);
            throw E;
        }
    }

    /**
     * Returns the timeout value for the transaction lifetime.
     *
     * <p>If {@link #setTxnTimeout(long,TimeUnit)} has not been called to
     * configure the timeout, the environment configuration value ({@link
     * EnvironmentConfig#TXN_TIMEOUT} )is returned.</p>
     *
     * @param unit the {@code TimeUnit} of the returned value. May not be null.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if the transaction or environment has been
     * closed.
     *
     * @throws IllegalArgumentException if the unit is null.
     *
     * @since 4.0
     */
    public long getTxnTimeout(TimeUnit unit)
        throws EnvironmentFailureException,
               IllegalStateException,
               IllegalArgumentException {

        checkEnv();
        checkOpen();
        return PropUtil.millisToDuration((int) txn.getTxnTimeout(), unit);
    }

    /**
     * Configures the timeout value for the transaction lifetime.
     *
     * <p>If the transaction runs longer than this time, an operation using the
     * transaction may throw {@link TransactionTimeoutException}. The
     * transaction timeout is checked when locking a record, as part of a read
     * or write operation.</p>
     *
     * <p>A value of zero (which is the default) disables timeouts for the
     * transaction, meaning that no limit on the duration of the transaction is
     * enforced. Note that the {@link #setLockTimeout(long, TimeUnit)} lock
     * timeout} is independent of the transaction timeout, and the lock timeout
     * should not normally be set to zero.</p>
     *
     * @param timeOut The timeout value for the transaction lifetime, or zero
     * to disable transaction timeouts.
     *
     * @param unit the {@code TimeUnit} of the timeOut value. May be null only
     * if timeOut is zero.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if the transaction or environment has been
     * closed.
     *
     * @throws IllegalArgumentException if timeOut or unit is invalid.
     *
     * @since 4.0
     */
    public void setTxnTimeout(long timeOut, TimeUnit unit)
        throws IllegalArgumentException, DatabaseException {

        checkEnv();
        checkOpen();
        txn.setTxnTimeout(PropUtil.durationToMillis(timeOut, unit));
    }

    /**
     * Configures the timeout value for the transaction lifetime, with the
     * timeout value specified in microseconds.  This method is equivalent to:
     *
     * <pre>setTxnTimeout(long, TimeUnit.MICROSECONDS);</pre>
     *
     * @deprecated as of 4.0, replaced by {@link #setTxnTimeout(long,
     * TimeUnit)}.
     */
    public void setTxnTimeout(long timeOut)
        throws IllegalArgumentException, DatabaseException {

        setTxnTimeout(timeOut, TimeUnit.MICROSECONDS);
    }

    /**
     * Returns the lock request timeout value for the transaction.
     *
     * <p>If {@link #setLockTimeout(long,TimeUnit)} has not been called to
     * configure the timeout, the environment configuration value ({@link
     * EnvironmentConfig#LOCK_TIMEOUT}) is returned.</p>
     *
     * @param unit the {@code TimeUnit} of the returned value. May not be null.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if the transaction or environment has been
     * closed.
     *
     * @throws IllegalArgumentException if the unit is null.
     *
     * @since 4.0
     */
    public long getLockTimeout(TimeUnit unit)
        throws EnvironmentFailureException,
               IllegalStateException,
               IllegalArgumentException {

        checkEnv();
        checkOpen();
        return PropUtil.millisToDuration((int) txn.getLockTimeout(), unit);
    }

    /**
     * Configures the lock request timeout value for the transaction. This
     * overrides the {@link EnvironmentConfig#setLockTimeout(long, TimeUnit)
     * default lock timeout}.
     *
     * <p>A value of zero disables lock timeouts. This is not recommended, even
     * when the application expects that deadlocks will not occur or will be
     * easily resolved. A lock timeout is a fall-back that guards against
     * unexpected "live lock", unresponsive threads, or application failure to
     * close a cursor or to commit or abort a transaction.</p>
     *
     * @param timeOut The lock timeout for all transactional and
     * non-transactional operations, or zero to disable lock timeouts.
     *
     * @param unit the {@code TimeUnit} of the timeOut value. May be null only
     * if timeOut is zero.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if the transaction or environment has been
     * closed.
     *
     * @throws IllegalArgumentException if timeOut or unit is invalid.
     *
     * @since 4.0
     */
    public void setLockTimeout(long timeOut, TimeUnit unit)
        throws IllegalArgumentException, DatabaseException {

        checkEnv();
        checkOpen();
        txn.setLockTimeout(PropUtil.durationToMillis(timeOut, unit));
    }

    /**
     * Configures the lock request timeout value for the transaction, with the
     * timeout value specified in microseconds.  This method is equivalent to:
     *
     * <pre>setLockTimeout(long, TimeUnit.MICROSECONDS);</pre>
     *
     * @deprecated as of 4.0, replaced by {@link #setLockTimeout(long,
     * TimeUnit)}.
     */
    public void setLockTimeout(long timeOut)
        throws IllegalArgumentException, DatabaseException {

        setLockTimeout(timeOut, TimeUnit.MICROSECONDS);
    }

    /**
     * Set the user visible name for the transaction.
     *
     * @param name The user visible name for the transaction.
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Get the user visible name for the transaction.
     *
     * @return The user visible name for the transaction.
     */
    public String getName() {
        return name;
    }

    /**
     * For internal use.
     * @hidden
     */
    @Override
    public int hashCode() {
        return (int) id;
    }

    /**
     * For internal use.
     * @hidden
     */
    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }

        if (!(o instanceof Transaction)) {
            return false;
        }

        if (((Transaction) o).id == id) {
            return true;
        }

        return false;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("<Transaction id=\"");
        sb.append(id).append("\"");
        if (name != null) {
            sb.append(" name=\"");
            sb.append(name).append("\"");
            }
        sb.append(">");
        return sb.toString();
    }

    /**
     * This method should only be called by the LockerFactory.getReadableLocker
     * and getWritableLocker methods.  The locker returned does not enforce the
     * readCommitted isolation setting.
     *
     * @throws IllegalArgumentException via all API methods with a txn param
     */
    Locker getLocker()
        throws DatabaseException {

        if (txn == null) {
            throw new IllegalArgumentException
                ("Transaction " + id +
                 " has been closed and is no longer usable.");
        }
        return txn;
    }

    /*
     * Helpers
     */

    Txn getTxn() {
        return txn;
    }

    Environment getEnvironment() {
        return env;
    }

    /**
     * @throws EnvironmentFailureException if the underlying environment is
     * invalid, via all methods.
     *
     * @throws IllegalStateException via all methods.
     */
    private void checkEnv() {
        EnvironmentImpl envImpl =  env.getNonNullEnvImpl();
        if (envImpl == null) {
            throw new IllegalStateException
                ("The environment has been closed. " +
                 "This transaction is no longer usable.");
        }
        envImpl.checkIfInvalid();
    }

    /**
     * @throws IllegalStateException via all methods except abort.
     */
    void checkOpen() {
        if (txn == null || txn.isClosed()) {
            throw new IllegalStateException("Transaction Id " + id +
                                            " has been closed.");
        }
    }

    /**
     * Returns whether this {@code Transaction} is open, which is equivalent
     * to when {@link Transaction#getState} returns {@link
     * Transaction.State#OPEN}.  See {@link Transaction.State#OPEN} for more
     * information.
     *
     * <p>When an {@link OperationFailureException}, or one of its subclasses,
     * is caught, the {@code isValid} method may be called to determine whether
     * the {@code Transaction} can continue to be used, or should be
     * aborted.</p>
     */
    public boolean isValid() {
        return txn != null &&
               txn.isValid();
    }

    /**
     * Remove reference to internal txn, so we can reclaim memory.  Before
     * setting it null, save the final State value, so we can return it from
     * getState.
     */
    private void setTxnNull() {
        finalState = txn.getState();
        txn = null;
    }

    /**
     * Returns the current state of the transaction.
     *
     * @since 5.0.48
     */
    public State getState() {
        if (txn != null) {
            assert finalState == null;
            return txn.getState();
        } else {
            assert finalState != null;
            return finalState;
        }
    }
}
