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

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.dbi.GetMode;
import com.sleepycat.je.dbi.SearchMode;
import com.sleepycat.je.dbi.TriggerManager;
import com.sleepycat.je.evictor.OffHeapCache;
import com.sleepycat.je.tree.BIN;
import com.sleepycat.je.tree.IN;
import com.sleepycat.je.txn.HandleLocker;
import com.sleepycat.je.txn.Locker;
import com.sleepycat.je.txn.LockerFactory;
import com.sleepycat.je.utilint.DatabaseUtil;
import com.sleepycat.je.utilint.LoggerUtils;

/**
 * A database handle.
 *
 * <p>Database attributes are specified in the {@link
 * com.sleepycat.je.DatabaseConfig DatabaseConfig} class. Database handles are
 * free-threaded and may be used concurrently by multiple threads.</p>
 *
 * <p>To open an existing database with default attributes:</p>
 *
 * <blockquote><pre>
 *     Environment env = new Environment(home, null);
 *     Database myDatabase = env.openDatabase(null, "mydatabase", null);
 * </pre></blockquote>
 *
 * <p>To create a transactional database that supports duplicates:</p>
 *
 * <blockquote><pre>
 *     DatabaseConfig dbConfig = new DatabaseConfig();
 *     dbConfig.setTransactional(true);
 *     dbConfig.setAllowCreate(true);
 *     dbConfig.setSortedDuplicates(true);
 *     Database db = env.openDatabase(txn, "mydatabase", dbConfig);
 * </pre></blockquote>
 */
public class Database implements Closeable {

    static final CursorConfig DEFAULT_CURSOR_CONFIG =
        CursorConfig.DEFAULT.clone().setNonSticky(true);

    static final CursorConfig READ_COMMITTED_CURSOR_CONFIG =
        CursorConfig.READ_COMMITTED.clone().setNonSticky(true);

    /*
     * DbState embodies the Database handle state.
     */
    enum DbState {
        OPEN, CLOSED, INVALID, PREEMPTED, CORRUPTED 
    }

    /*
     * The current state of the handle. When the state is non-open, the
     * databaseImpl should not be accessed, since the databaseImpl is set to
     * null during close. This check does not guarantee that an NPE will not
     * occur, since we do not synchronize -- the state could change after
     * checking it and before accessing the databaseImpl. But the check makes
     * an NPE unlikely.
     */
    private volatile DbState state;

    /* The DatabasePreemptedException cause when state == PREEMPTED. */
    private volatile OperationFailureException preemptedCause;

    /* The SecondaryIntegrityException cause when state == CORRUPTED. */
    private volatile OperationFailureException corruptedCause;

    /*
     * The envHandle field cannot be declared as final because it is
     * initialized by methods called by the ctor. However, after construction
     * it is non-null and should be treated as final.
     */
    Environment envHandle;

    /*
     * The databaseImpl field is set to null during close to avoid OOME. It
     * should normally only be accessed via the checkOpen and getDbImpl
     * methods. It is guaranteed to be non-null if state == DbState.OPEN.
     */
    private DatabaseImpl databaseImpl;

    /*
     * Used to store per-Database handle properties: allow create,
     * exclusive create, read only and use existing config. Other Database-wide
     * properties are stored in DatabaseImpl.
     */
    DatabaseConfig configuration;

    /* True if this handle permits write operations; */
    private boolean isWritable;

    /* Record how many open cursors on this database. */
    private final AtomicInteger openCursors = new AtomicInteger();

    /*
     * Locker that owns the NameLN lock held while the Database is open.
     *
     * The handleLocker field is set to null during close to avoid OOME. It
     * is only accessed during close, while synchronized, so checking for null
     * is unnecessary.
     */
    private HandleLocker handleLocker;

    /*
     * If a user-supplied SecondaryAssociation is configured, this field
     * contains it.  Otherwise, it contains an internal SecondaryAssociation
     * that uses the simpleAssocSecondaries to store associations between a
     * single primary and its secondaries.
     */
    SecondaryAssociation secAssoc;
    Collection<SecondaryDatabase> simpleAssocSecondaries;

    /*
     * Secondaries whose keys have values contrained to the primary keys in
     * this database.
     */
    Collection<SecondaryDatabase> foreignKeySecondaries;

    final Logger logger;

    /**
     * Creates a database but does not open or fully initialize it.  Is
     * protected for use in compat package.
     */
    Database(final Environment env) {
        this.envHandle = env;
        logger = getEnv().getLogger();
    }

    /**
     * Creates a database, called by Environment.
     */
    DatabaseImpl initNew(final Environment env,
                         final Locker locker,
                         final String databaseName,
                         final DatabaseConfig dbConfig) {

        dbConfig.validateForNewDb();

        init(env, dbConfig);

        /* Make the databaseImpl. */
        databaseImpl = getEnv().getDbTree().createDb(
            locker, databaseName, dbConfig, handleLocker);
        databaseImpl.addReferringHandle(this);
        return databaseImpl;
    }

    /**
     * Opens a database, called by Environment.
     */
    void initExisting(final Environment env,
                      final Locker locker,
                      final DatabaseImpl dbImpl,
                      final String databaseName,
                      final DatabaseConfig dbConfig) {

        /*
         * Make sure the configuration used for the open is compatible with the
         * existing databaseImpl.
         */
        validateConfigAgainstExistingDb(locker, databaseName, dbConfig,
                                        dbImpl);

        init(env, dbConfig);
        this.databaseImpl = dbImpl;
        dbImpl.addReferringHandle(this);
    }

    private void init(final Environment env, final DatabaseConfig config) {
        assert handleLocker != null;
        envHandle = env;
        configuration = config.clone();
        isWritable = !configuration.getReadOnly();
        secAssoc = makeSecondaryAssociation();
        state = DbState.OPEN;
    }

    SecondaryAssociation makeSecondaryAssociation() {
        foreignKeySecondaries = new CopyOnWriteArraySet<>();

        if (configuration.getSecondaryAssociation() != null) {
            if (configuration.getSortedDuplicates()) {
                throw new IllegalArgumentException(
                    "Duplicates not allowed for a primary database");
            }
            simpleAssocSecondaries = Collections.emptySet();
            return configuration.getSecondaryAssociation();
        }

        simpleAssocSecondaries = new CopyOnWriteArraySet<>();

        return new SecondaryAssociation() {

            public boolean isEmpty() {
                return simpleAssocSecondaries.isEmpty();
            }

            public Database getPrimary(@SuppressWarnings("unused")
                                       DatabaseEntry primaryKey) {
                return Database.this;
            }

            public Collection<SecondaryDatabase>
                getSecondaries(@SuppressWarnings("unused")
                                DatabaseEntry primaryKey) {
                return simpleAssocSecondaries;
            }
        };
    }

    /**
     * Used to remove references to this database from other objects, when this
     * database is closed.  We don't remove references from cursors or
     * secondaries here, because it's an error to close a database before its
     * cursors and to close a primary before its secondaries.
     */
    void removeReferringAssociations() {
        envHandle.removeReferringHandle(this);
    }

    /**
     * Sees if this new handle's configuration is compatible with the
     * pre-existing database.
     */
    private void validateConfigAgainstExistingDb(Locker locker,
                                                 final String databaseName,
                                                 final DatabaseConfig config,
                                                 final DatabaseImpl dbImpl) {
        /*
         * The sortedDuplicates, temporary, and replicated properties are
         * persistent and immutable.  But they do not need to be specified if
         * the useExistingConfig property is set.
         */
        if (!config.getUseExistingConfig()) {
            validatePropertyMatches(
                "sortedDuplicates", dbImpl.getSortedDuplicates(),
                config.getSortedDuplicates());
            validatePropertyMatches(
                "temporary", dbImpl.isTemporary(),
                config.getTemporary());
            /* Only check replicated if the environment is replicated. */
            if (getEnv().isReplicated()) {
                validatePropertyMatches(
                    "replicated", dbImpl.isReplicated(),
                    config.getReplicated());
            }
        }

        /*
         * The transactional and deferredWrite properties are kept constant
         * while any handles are open, and set when the first handle is opened.
         * But if an existing handle is open and the useExistingConfig property
         * is set, then they do not need to be specified.
         */
        if (dbImpl.hasOpenHandles()) {
            if (!config.getUseExistingConfig()) {
                validatePropertyMatches(
                    "transactional", dbImpl.isTransactional(),
                    config.getTransactional());
                validatePropertyMatches(
                    "deferredWrite", dbImpl.isDurableDeferredWrite(),
                    config.getDeferredWrite());
            }
        } else {
            dbImpl.setTransactional(config.getTransactional());
            dbImpl.setDeferredWrite(config.getDeferredWrite());
            if (config.getDeferredWrite()) {
                mutateDeferredWriteBINDeltas(dbImpl);
            }
        }

        /*
         * If this database handle uses the existing config, we shouldn't
         * search for and write any changed attributes to the log.
         */
        if (config.getUseExistingConfig()) {
            return;
        }

        /* Write any changed, persistent attributes to the log. */
        boolean dbImplModified = false;

        /* Only re-set the comparators if the override is allowed. */
        if (config.getOverrideBtreeComparator()) {
            dbImplModified |= dbImpl.setBtreeComparator(
                config.getBtreeComparator(),
                config.getBtreeComparatorByClassName());
        }

        if (config.getOverrideDuplicateComparator()) {
            dbImplModified |= dbImpl.setDuplicateComparator(
                config.getDuplicateComparator(),
                config.getDuplicateComparatorByClassName());
        }

        dbImplModified |= dbImpl.setTriggers(locker,
                                             databaseName,
                                             config.getTriggers(),
                                             config.getOverrideTriggers());

        /* Check if KeyPrefixing property is updated. */
        boolean newKeyPrefixing = config.getKeyPrefixing();
        if (newKeyPrefixing != dbImpl.getKeyPrefixing()) {
            dbImplModified = true;
            if (newKeyPrefixing) {
                dbImpl.setKeyPrefixing();
            } else {
                dbImpl.clearKeyPrefixing();
            }
        }

        /*
         * Check if NodeMaxEntries properties are updated.
         */
        int newNodeMaxEntries = config.getNodeMaxEntries();
        if (newNodeMaxEntries != 0 &&
            newNodeMaxEntries != dbImpl.getNodeMaxTreeEntries()) {
            dbImplModified = true;
            dbImpl.setNodeMaxTreeEntries(newNodeMaxEntries);
        }

        /* Do not write LNs in a read-only environment.  Also see [#15743]. */
        EnvironmentImpl envImpl = getEnv();
        if (dbImplModified && !envImpl.isReadOnly()) {

            /* Write a new NameLN to the log. */
            try {
                envImpl.getDbTree().updateNameLN(locker, dbImpl.getName(),
                                                 null);
            } catch (LockConflictException e) {
                throw new IllegalStateException(
                    "DatabaseConfig properties may not be updated when the " +
                    "database is already open; first close other open " +
                    "handles for this database.", e);
            }

            /* Dirty the root. */
            envImpl.getDbTree().modifyDbRoot(dbImpl);
        }

        /* CacheMode is changed for all handles, but is not persistent. */
        dbImpl.setCacheMode(config.getCacheMode());
    }

    /**
     * Mutate BIN-deltas to full BINs for the given DW DB.  [#25999]
     * <p>
     * Any BIN-deltas in cache must be mutated to full BINs, since
     * BIN-deltas are not allowed in DW mode. This can be expensive, and is
     * only a workaround for the underlying problem that BIN-deltas are not
     * supported in DW mode. The mutation is necessary when a db transitions
     * from non-DW to DW mode. In that case there may be BIN-deltas in
     * cache, even if the DB has not yet been opened since BIN-deltas may
     * be placed in the Btree by recovery. This method is not optimized
     * because ultimately BIN-deltas will be supported in DW mode or DW mode
     * will be discontinued.
     * <p>
     * At the time this method is called, the DW flag on this object has
     * already been set. Therefore, this workaround doesn't guarantee that
     * internal operations on the db (e.g., compression or eviction) won't
     * operation on a BIN-delta in a DW DB. Therefore, we cannot assume/assert
     * that a cached BIN-delta does not exist in a DW DB. Such assertions were
     * removed as part of this workaround.
     * <p>
     * Setting the DW flag before mutating the BINs is necessary to ensure
     * that no new BIN-deltas appear in cache during this process. BINs loaded
     * into cache for a DW db are mutated to full BINs in IN.postFetchInit.
     *
     * @see BIN#shouldLogDelta
     */
    private void mutateDeferredWriteBINDeltas(DatabaseImpl dbImpl) {

        final OffHeapCache ohCache = getEnv().getOffHeapCache();

        for (final IN in : getEnv().getInMemoryINs()) {
            if (in.getDatabase() != dbImpl) {
                continue;
            }
            in.latchNoUpdateLRU();
            try {
                if (in.isBIN()) {
                    in.mutateToFullBIN(false);
                    continue;
                }
                if (ohCache == null ||
                    in.getNormalizedLevel() != 2) {
                    continue;
                }
                for (int i = 0; i < in.getNEntries(); i += 1) {
                    if (in.getOffHeapBINId(i) < 0) {
                        continue;
                    }
                    final IN child = in.loadIN(i, CacheMode.UNCHANGED);
                    if (child == null) {
                        continue;
                    }
                    child.latchNoUpdateLRU();
                    try {
                        child.mutateToFullBIN(false);
                    } finally {
                        child.releaseLatch();
                    }
                }
            } finally {
                in.releaseLatch();
            }
        }
    }

    /**
     * @throws IllegalArgumentException via Environment.openDatabase and
     * openSecondaryDatabase.
     */
    private void validatePropertyMatches(final String propName,
                                         final boolean existingValue,
                                         final boolean newValue) {
        if (newValue != existingValue) {
            throw new IllegalArgumentException(
                "You can't open a Database with a " + propName +
                " configuration of " + newValue +
                " if the underlying database was created with a " +
                propName + " setting of " + existingValue + '.');
        }
    }

    /**
     * Discards the database handle.
     * <p>
     * When closing the last open handle for a deferred-write database, any
     * cached database information is flushed to disk as if {@link #sync} were
     * called.
     * <p>
     * The database handle should not be closed while any other handle that
     * refers to it is not yet closed; for example, database handles should not
     * be closed while cursor handles into the database remain open, or
     * transactions that include operations on the database have not yet been
     * committed or aborted.  Specifically, this includes {@link
     * com.sleepycat.je.Cursor Cursor} and {@link com.sleepycat.je.Transaction
     * Transaction} handles.
     * <p>
     * When multiple threads are using the {@link com.sleepycat.je.Database
     * Database} handle concurrently, only a single thread may call this
     * method.
     * <p>
     * When called on a database that is the primary database for a secondary
     * index, the primary database should be closed only after all secondary
     * indices which reference it have been closed.
     * <p>
     * The database handle may not be accessed again after this method is
     * called, regardless of the method's success or failure, with one
     * exception:  the {@code close} method itself may be called any number of
     * times.</p>
     *
     * <p>WARNING: To guard against memory leaks, the application should
     * discard all references to the closed handle.  While BDB makes an effort
     * to discard references from closed objects to the allocated memory for an
     * environment, this behavior is not guaranteed.  The safe course of action
     * for an application is to discard all references to closed BDB
     * objects.</p>
     *
     * @see DatabaseConfig#setDeferredWrite DatabaseConfig.setDeferredWrite
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if cursors associated with this database
     * are still open.
     */
    public void close() {
        try {
            closeInternal(true /*doSyncDw*/, true /*deleteTempDb*/,
                          DbState.CLOSED, null /*preemptedException*/);
        } catch (Error E) {
            envHandle.invalidate(E);
            throw E;
        }
    }

    /*
     * This method is private for now because it is incomplete.  To fully
     * implement it we must clear all dirty nodes for the database that is
     * closed, since otherwise they will be flushed during the next checkpoint.
     */
    @SuppressWarnings("unused")
    private void closeNoSync() {
        try {
            closeInternal(false /*doSyncDw*/, true /*deleteTempDb*/,
                          DbState.CLOSED, null /*preemptedException*/);
        } catch (Error E) {
            envHandle.invalidate(E);
            throw E;
        }
    }

    /**
     * Marks the handle as preempted when the handle lock is stolen by the HA
     * replayer, during replay of a naming operation (remove, truncate or
     * rename).  This causes DatabasePreemptedException to be thrown on all
     * subsequent use of the handle or cursors opened on this handle.  [#17015]
     */
    synchronized void setPreempted(final String dbName, final String msg) {

        /*
         * Return silently when the DB is closed, because the calling thread is
         * performing an DbTree operation, and a "database closed" exception
         * should not be thrown there.
         */
        final DatabaseImpl dbImpl = databaseImpl;

        if (dbImpl == null) {
            return;
        }

        final OperationFailureException preemptedException =
            dbImpl.getEnv().createDatabasePreemptedException(
                msg, dbName, this);

        closeInternal(false /*doSyncDw*/, false /*deleteTempDb*/,
                      DbState.PREEMPTED, preemptedException);
    }

    synchronized void setCorrupted(SecondaryIntegrityException sie) {
        if (state != DbState.OPEN) {
            return;
        }
        corruptedCause = sie;
        state = DbState.CORRUPTED;
    }

    boolean isCorrupted() {
        return corruptedCause != null;
    }

    /**
     * Invalidates the handle when the transaction used to open the database
     * is aborted.
     *
     * Note that this method (unlike close) does not perform sync and removal
     * of DW DBs.  A DW DB cannot be transactional.
     */
    synchronized void invalidate() {
        closeInternal(false /*doSyncDw*/, false /*deleteTempDb*/,
                      DbState.INVALID, null /*preemptedException*/);
    }

    EnvironmentImpl getEnv() {
        return envHandle.getNonNullEnvImpl();
    }

    private void closeInternal(
        final boolean doSyncDw,
        final boolean deleteTempDb,
        final DbState newState,
        final OperationFailureException preemptedException) {

        /*
         * We acquire the SecondaryAssociationLatch exclusively because
         * associations are changed by removeReferringAssociations, and
         * operations using the associations should not run concurrently with
         * close.
         */
        try {
            final EnvironmentImpl envImpl = getEnv();
            try {
                envImpl.getSecondaryAssociationLock().
                    writeLock().lockInterruptibly();
            } catch (InterruptedException e) {
                throw new ThreadInterruptedException(envImpl, e);
            }
            try {
                closeInternalWork(
                    doSyncDw, deleteTempDb, newState, preemptedException);
            } finally {
                envImpl.getSecondaryAssociationLock().writeLock().unlock();
            }
        } finally {
            minimalClose(newState, preemptedException);
        }
    }

    /**
     * Nulls-out indirect references to the environment, to allow GC. It also
     * sets the state to the given non-open state, if the state is currently
     * open. We must set the state to non-open before setting references to
     * null.
     *
     * The app may hold the Database references longer than expected. In
     * particular during an Environment re-open we need to give GC a fighting
     * chance while handles from two environments are temporarily referenced.
     *
     * Note that this is needed even when the db or env is invalid.
     */
    synchronized void minimalClose(
        final DbState newState,
        final OperationFailureException preemptedException) {

        assert newState != DbState.OPEN;

        if (state == DbState.OPEN) {
            state = newState;
            preemptedCause = preemptedException;
        }

        databaseImpl = null;
        handleLocker = null;
    }

    private void closeInternalWork(
        final boolean doSyncDw,
        final boolean deleteTempDb,
        final DbState newState,
        final OperationFailureException preemptedException) {

        assert newState != DbState.OPEN;

        final StringBuilder handleRefErrors = new StringBuilder();
        RuntimeException triggerException = null;
        final DatabaseImpl dbImpl;

        synchronized (this) {

            /*
             * Do nothing if handle was previously closed.
             *
             * When the state is set to CLOSED, INVALID and PREEMPTED, the
             * database has been closed. So for these states, do not close the
             * database again.
             *
             * The CORRUPTED state is set when SecondaryIntegrityException is
             * thrown, but the database is not closed at that time.
             * This state is currently only set for a secondary database.
             * For this state, we want to let the user close the database.
             *
             * Besides, if the database was not opened, just return.
             */
            if (state == DbState.CLOSED || state == DbState.INVALID ||
                state == DbState.PREEMPTED || state == null) {
                return;
            }

            /*
             * databaseImpl and handleLocker are set to null only while
             * synchronized, at which time the state is also changed to
             * non-open. So they should not be null here.
             */
            dbImpl = databaseImpl;
            assert dbImpl != null;
            assert handleLocker != null;

            /*
             * Check env only after checking for closed db, to mimic close()
             * behavior for Cursors, etc, and avoid unnecessary exception
             * handling.  [#21264]
             */
            final EnvironmentImpl envImpl = checkEnv();

            /*
             * The state should be changed ASAP during close, so that
             * addCursor and removeCursor will see the updated state ASAP.
             */
            state = newState;
            preemptedCause = preemptedException;

            /*
             * Throw an IllegalStateException if there are open cursors or
             * associated secondaries.
             */
            if (newState == DbState.CLOSED) {
                if (openCursors.get() != 0) {
                    handleRefErrors.append(" ").
                           append(openCursors.get()).
                           append(" open cursors.");
                }
                if (simpleAssocSecondaries != null &&
                    simpleAssocSecondaries.size() > 0) {
                    handleRefErrors.append(" ").
                           append(simpleAssocSecondaries.size()).
                           append(" associated SecondaryDatabases.");
                }
                if (foreignKeySecondaries != null &&
                    foreignKeySecondaries.size() > 0) {
                    handleRefErrors.append(" ").
                           append(foreignKeySecondaries.size()).
                           append(
                           " associated foreign key SecondaryDatabases.");
                }
            }

            trace(Level.FINEST, "Database.close: ", null, null);

            removeReferringAssociations();

            dbImpl.removeReferringHandle(this);
            envImpl.getDbTree().releaseDb(dbImpl);

            /*
             * If the handle was preempted, we mark the locker as
             * only-abortable with the DatabasePreemptedException.  If
             * the handle locker is a user txn, this causes the
             * DatabasePreemptedException to be thrown if the user
             * attempts to commit, or continue to use, the txn, rather
             * than throwing a LockConflictException.  [#17015]
             */
            if (newState == DbState.PREEMPTED) {
                handleLocker.setOnlyAbortable(preemptedException);
            }

            /*
             * Tell our protecting txn that we're closing. If this type
             * of transaction doesn't live beyond the life of the
             * handle, it will release the db handle lock.
             */
            if (newState == DbState.CLOSED) {
                if (isWritable() && (dbImpl.noteWriteHandleClose() == 0)) {
                    try {
                        TriggerManager.runCloseTriggers(handleLocker, dbImpl);
                    } catch (RuntimeException e) {
                        triggerException = e;
                    }
                }
                handleLocker.operationEnd(true);
            } else {
                handleLocker.operationEnd(false);
            }
        }

        /*
         * Notify the database when a handle is closed.  This should not be
         * done while synchronized since it may perform database removal or
         * sync.  Statements above are synchronized but this section must not
         * be.
         *
         * Note that handleClosed may throw an exception, so any following code
         * may not be executed.
         */
        dbImpl.handleClosed(doSyncDw, deleteTempDb);

        /* Throw exceptions for previously encountered problems. */
        if (handleRefErrors.length() > 0) {
            throw new IllegalStateException(
                "Database closed while still referenced by other handles." +
                handleRefErrors.toString());
        }
        if (triggerException != null) {
            throw triggerException;
        }
    }

    /**
     * Flushes any cached information for this database to disk; only
     * applicable for deferred-write databases.
     * <p> Note that deferred-write databases are automatically flushed to disk
     * when the {@link #close} method is called.
     *
     * @see DatabaseConfig#setDeferredWrite DatabaseConfig.setDeferredWrite
     *
     * @throws com.sleepycat.je.rep.DatabasePreemptedException in a replicated
     * environment if the master has truncated, removed or renamed the
     * database.
     *
     * @throws OperationFailureException if this exception occurred earlier and
     * caused the transaction to be invalidated.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws UnsupportedOperationException if this is not a deferred-write
     * database, or this database is read-only.
     *
     * @throws IllegalStateException if the database has been closed.
     */
    public void sync() {

        checkEnv();
        final DatabaseImpl dbImpl = checkOpen();
        trace(Level.FINEST, "Database.sync", null, null, null, null);

        dbImpl.sync(true);
    }

    /**
     * Opens a sequence in the database.
     *
     * @param txn For a transactional database, an explicit transaction may
     * be specified, or null may be specified to use auto-commit.  For a
     * non-transactional database, null must be specified.
     *
     * @param key The key {@link DatabaseEntry} of the sequence.
     *
     * @param config The sequence attributes.  If null, default attributes are
     * used.
     *
     * @return a new Sequence handle.
     *
     * @throws SequenceExistsException if the sequence record already exists
     * and the {@code SequenceConfig ExclusiveCreate} parameter is true.
     *
     * @throws SequenceNotFoundException if the sequence record does not exist
     * and the {@code SequenceConfig AllowCreate} parameter is false.
     *
     * @throws OperationFailureException if one of the <a
     * href="../je/OperationFailureException.html#readFailures">Read Operation
     * Failures</a> occurs. If the sequence does not exist and the {@link
     * SequenceConfig#setAllowCreate AllowCreate} parameter is true, then one
     * of the <a
     * href="../je/OperationFailureException.html#writeFailures">Write
     * Operation Failures</a> may also occur.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws UnsupportedOperationException if this database is read-only, or
     * this database is configured for duplicates.
     *
     * @throws IllegalStateException if the Sequence record is deleted by
     * another thread during this method invocation, or the database has been
     * closed.
     *
     * @throws IllegalArgumentException if an invalid parameter is specified,
     * for example, an invalid {@code SequenceConfig} parameter.
     */
    public Sequence openSequence(final Transaction txn,
                                 final DatabaseEntry key,
                                 final SequenceConfig config) {
        try {
            checkEnv();
            DatabaseUtil.checkForNullDbt(key, "key", true);
            checkOpen();
            trace(Level.FINEST, "Database.openSequence", txn, key, null, null);

            return new Sequence(this, txn, key, config);
        } catch (Error E) {
            envHandle.invalidate(E);
            throw E;
        }
    }

    /**
     * Removes the sequence from the database.  This method should not be
     * called if there are open handles on this sequence.
     *
     * @param txn For a transactional database, an explicit transaction may be
     * specified, or null may be specified to use auto-commit.  For a
     * non-transactional database, null must be specified.
     *
     * @param key The key {@link com.sleepycat.je.DatabaseEntry
     * DatabaseEntry} of the sequence.
     *
     * @throws OperationFailureException if one of the <a
     * href="../je/OperationFailureException.html#writeFailures">Write
     * Operation Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws UnsupportedOperationException if this database is read-only.
     */
    public void removeSequence(final Transaction txn,
                               final DatabaseEntry key) {
        try {
            delete(txn, key);
        } catch (Error E) {
            envHandle.invalidate(E);
            throw E;
        }
    }

    /**
     * Returns a cursor into the database.
     *
     * @param txn the transaction used to protect all operations performed with
     * the cursor, or null if the operations should not be transaction
     * protected.  If the database is non-transactional, null must be
     * specified.  For a transactional database, the transaction is optional
     * for read-only access and required for read-write access.
     *
     * @param cursorConfig The cursor attributes.  If null, default attributes
     * are used.
     *
     * @return A database cursor.
     *
     * @throws com.sleepycat.je.rep.DatabasePreemptedException in a replicated
     * environment if the master has truncated, removed or renamed the
     * database.
     *
     * @throws OperationFailureException if this exception occurred earlier and
     * caused the transaction to be invalidated.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if the database has been closed.
     *
     * @throws IllegalArgumentException if an invalid parameter is specified,
     * for example, an invalid {@code CursorConfig} parameter.
     */
    public Cursor openCursor(final Transaction txn,
                             CursorConfig cursorConfig) {
        try {
            checkEnv();
            checkOpen();

            if (cursorConfig == null) {
                cursorConfig = CursorConfig.DEFAULT;
            }

            if (cursorConfig.getReadUncommitted() &&
                cursorConfig.getReadCommitted()) {
                throw new IllegalArgumentException(
                    "Only one may be specified: " +
                    "ReadCommitted or ReadUncommitted");
            }

            trace(Level.FINEST, "Database.openCursor", txn, cursorConfig);
            return newDbcInstance(txn, cursorConfig);
        } catch (Error E) {
            envHandle.invalidate(E);
            throw E;
        }
    }

    /**
     * Create a DiskOrderedCursor to iterate over the records in 'this'
     * Database.  Because the retrieval is based on Log Sequence Number (LSN)
     * order rather than key order, records are returned in unsorted order in
     * exchange for generally faster retrieval.  LSN order approximates disk
     * sector order.
     * <p>
     * See {@link DiskOrderedCursor} for more details and a description of the
     * consistency guarantees provided by the scan.
     * <p>
     * <em>WARNING:</em> After calling this method, deletion of log files by
     * the JE log cleaner will be disabled until {@link
     * DiskOrderedCursor#close()} is called.  To prevent unbounded growth of
     * disk usage, be sure to call {@link DiskOrderedCursor#close()} to
     * re-enable log file deletion.
     */
    public DiskOrderedCursor openCursor(DiskOrderedCursorConfig cursorConfig) {
        try {
            checkEnv();
            checkOpen();

            if (cursorConfig == null) {
                cursorConfig = DiskOrderedCursorConfig.DEFAULT;
            }

            trace(Level.FINEST, "Database.openForwardCursor",
                null, cursorConfig);

            Database[] dbs = new Database[1];
            dbs[0] = this;

            return new DiskOrderedCursor(dbs, cursorConfig);
        } catch (Error E) {
            envHandle.invalidate(E);
            throw E;
        }
    }

    /**
     * Is overridden by SecondaryDatabase.
     */
    Cursor newDbcInstance(final Transaction txn,
                          final CursorConfig cursorConfig) {
        return new Cursor(this, txn, cursorConfig);
    }

    /**
     * @hidden
     * For internal use only.
     *
     * @deprecated in favor of {@link #populateSecondaries(Transaction,
     * DatabaseEntry, DatabaseEntry, long, CacheMode)}.
     */
    public void populateSecondaries(final Transaction txn,
                                    final DatabaseEntry key,
                                    final DatabaseEntry data) {

        populateSecondaries(txn, key, data, 0 /*expirationTime*/, null);
    }

    /**
     * @hidden
     * For internal use only.
     *
     * Given the {@code key}, {@code data} and {@code expirationTime} for a
     * locked primary DB record, update the corresponding secondary database
     * (index) records, for secondaries enabled for incremental population.
     * <p>
     * The secondaries associated the primary record are determined by calling
     * {@link SecondaryAssociation#getSecondaries}.  For each of these
     * secondaries, {@link SecondaryDatabase#isIncrementalPopulationEnabled} is
     * called to determine whether incremental population is enabled.  If so,
     * appropriate secondary records are inserted and deleted so that the
     * index accurately reflects the current state of the primary record.
     * <p>
     * Note that for a given primary record, this method will not modify the
     * secondary database if the secondary has already been updated for the
     * primary record, due to concurrent primary write operations.  Due to this
     * behavior, certain integrity checks are not performed as documented in
     * {@link SecondaryDatabase#startIncrementalPopulation}.
     * <p>
     * The primary record must be locked (read or write locked) when this
     * method is called. Therefore, the caller should not use dirty-read to
     * read the primary record. The simplest way to ensure that the primary
     * record is locked is to use a cursor to read primary records, and call
     * this method while the cursor is still positioned on the primary record.
     * <p>
     * It is the caller's responsibility to pass all primary records to this
     * method that contain index keys for a secondary DB being incrementally
     * populated, before calling {@link
     * SecondaryDatabase#endIncrementalPopulation} on that secondary DB.
     *
     * @param txn is the transaction to be used to write secondary records. If
     * null and the secondary DBs are transactional, auto-commit will be used.

     * @param key is the key of the locked primary record.
     *
     * @param data is the data of the locked primary record.
     *
     * @param expirationTime the expiration time of the locked primary record.
     * This can be obtained from {@link OperationResult#getExpirationTime()}
     * when reading the primary record.
     *
     * @param cacheMode the CacheMode to use, or null for the Database default.
     */
    public void populateSecondaries(final Transaction txn,
                                    final DatabaseEntry key,
                                    final DatabaseEntry data,
                                    final long expirationTime,
                                    CacheMode cacheMode) {
        try {
            checkEnv();
            DatabaseUtil.checkForNullDbt(key, "key", true);
            DatabaseUtil.checkForNullDbt(data, "true", true);
            final DatabaseImpl dbImpl = checkOpen();
            trace(Level.FINEST, "populateSecondaries", null, key, data, null);

            final Collection<SecondaryDatabase> secondaries =
                secAssoc.getSecondaries(key);

            final Locker locker = LockerFactory.getWritableLocker(
                envHandle, txn, dbImpl.isInternalDb(), isTransactional(),
                dbImpl.isReplicated()); // autoTxnIsReplicated

            boolean success = false;

            if (cacheMode == null) {
                cacheMode = dbImpl.getDefaultCacheMode();
            }

            try {
                for (final SecondaryDatabase secDb : secondaries) {
                    if (secDb.isIncrementalPopulationEnabled()) {

                        secDb.updateSecondary(
                            locker, null /*cursor*/, key /*priKey*/,
                            null /*oldData*/, data /*newData*/, cacheMode,
                            expirationTime, false  /*expirationUpdated*/,
                            expirationTime);
                    }
                }
                success = true;
            } finally {
                locker.operationEnd(success);
            }
        } catch (Error E) {
            envHandle.invalidate(E);
            throw E;
        }
    }

    /**
     * Removes records with a given key from the database. In the presence of
     * duplicate keys, all records associated with the given key will be
     * removed. When the database has associated secondary databases, this
     * method also deletes the associated index records.
     *
     * @param txn For a transactional database, an explicit transaction may
     * be specified, or null may be specified to use auto-commit.  For a
     * non-transactional database, null must be specified.
     *
     * @param key the key used as
     * <a href="DatabaseEntry.html#inParam">input</a>.
     *
     * @param options the WriteOptions, or null to use default options.
     *
     * @return the OperationResult if the record is deleted, else null if the
     * given key was not found in the database.
     *
     * @throws OperationFailureException if one of the <a
     * href="../je/OperationFailureException.html#writeFailures">Write
     * Operation Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws UnsupportedOperationException if this database is read-only.
     *
     * @throws IllegalStateException if the database has been closed.
     *
     * @throws IllegalArgumentException if an invalid parameter is specified.
     * This includes passing a null input key parameter, an input key parameter
     * with a null data array, or a partial key input parameter.
     *
     * @since 7.0
     */
    public OperationResult delete(final Transaction txn,
                                  final DatabaseEntry key,
                                  final WriteOptions options) {
        try {
            checkEnv();
            final DatabaseImpl dbImpl = checkOpen();

            trace(Level.FINEST, "Database.delete", txn, key, null, null);

            final CacheMode cacheMode =
                options != null ? options.getCacheMode() : null;

            OperationResult result = null;

            final Locker locker = LockerFactory.getWritableLocker(
                envHandle, txn,
                dbImpl.isInternalDb(),
                isTransactional(),
                dbImpl.isReplicated()); // autoTxnIsReplicated

            try {
                result = deleteInternal(locker, key, cacheMode);
            } finally {
                if (locker != null) {
                    locker.operationEnd(result != null);
                }
            }

            return result;
        } catch (Error E) {
            envHandle.invalidate(E);
            throw E;
        }
    }

    /**
     * Internal version of delete() that does no parameter checking.  Notify
     * triggers, update secondaries and enforce foreign key constraints.
     * Deletes all duplicates.
     */
    OperationResult deleteInternal(final Locker locker,
                                   final DatabaseEntry key,
                                   final CacheMode cacheMode) {

        final DatabaseEntry noData = new DatabaseEntry();
        noData.setPartial(0, 0, true);

        try (final Cursor cursor = new Cursor(this, locker, null)) {
            cursor.setNonSticky(true);

            final LockMode lockMode =
                cursor.isSerializableIsolation(LockMode.RMW) ?
                    LockMode.RMW : LockMode.READ_UNCOMMITTED_ALL;

            OperationResult searchResult = cursor.search(
                key, noData, lockMode, cacheMode, SearchMode.SET, false);

            final DatabaseImpl dbImpl = getDbImpl();
            OperationResult anyResult = null;

            while (searchResult != null) {

                final OperationResult deleteResult = cursor.deleteInternal(
                    dbImpl.getRepContext(), cacheMode);

                if (deleteResult != null) {
                    anyResult = deleteResult;
                }

                if (!dbImpl.getSortedDuplicates()) {
                    break;
                }

                searchResult = cursor.retrieveNext(
                    key, noData, lockMode, cacheMode, GetMode.NEXT_DUP);
            }

            if (anyResult == null) {
                dbImpl.getEnv().incDeleteFailOps(dbImpl);
            }

            return anyResult;
        }
    }

    /**
     * Removes records with a given key from the database. In the presence of
     * duplicate keys, all records associated with the given key will be
     * removed. When the database has associated secondary databases, this
     * method also deletes the associated index records.
     *
     * <p>Calling this method is equivalent to calling {@link
     * #delete(Transaction, DatabaseEntry, WriteOptions)}.</p>
     *
     * @param txn For a transactional database, an explicit transaction may
     * be specified, or null may be specified to use auto-commit.  For a
     * non-transactional database, null must be specified.
     *
     * @param key the key used as
     * <a href="DatabaseEntry.html#inParam">input</a>.
     *
     * @return The method will return {@link
     * com.sleepycat.je.OperationStatus#NOTFOUND OperationStatus.NOTFOUND} if
     * the given key is not found in the database; otherwise {@link
     * com.sleepycat.je.OperationStatus#SUCCESS OperationStatus.SUCCESS}.
     *
     * @throws OperationFailureException if one of the <a
     * href="../je/OperationFailureException.html#writeFailures">Write
     * Operation Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws UnsupportedOperationException if this database is read-only.
     *
     * @throws IllegalStateException if the database has been closed.
     *
     * @throws IllegalArgumentException if an invalid parameter is specified.
     * This includes passing a null input key parameter, an input key parameter
     * with a null data array, or a partial key input parameter.
     */
    public OperationStatus delete(final Transaction txn,
                                  final DatabaseEntry key) {
        final OperationResult result = delete(txn, key, null);
        return result == null ?
            OperationStatus.NOTFOUND : OperationStatus.SUCCESS;
    }

    /**
     * Retrieves a record according to the specified {@link Get} type.
     *
     * <p>If the operation succeeds, the record will be locked according to the
     * {@link ReadOptions#getLockMode() lock mode} specified, the key and/or
     * data will be returned via the (non-null) DatabaseEntry parameters, and a
     * non-null OperationResult will be returned. If the operation fails
     * because the record requested is not found, null is returned.</p>
     *
     * <p>The following table lists each allowed operation and whether the key
     * and data parameters are <a href="DatabaseEntry.html#params">input or
     * output parameters</a>. See the individual {@link Get} operations for
     * more information.</p>
     *
     * <div><table border="1" summary="">
     * <tr>
     *     <th>Get operation</th>
     *     <th>Description</th>
     *     <th>'key' parameter</th>
     *     <th>'data' parameter</th>
     * </tr>
     * <tr>
     *     <td>{@link Get#SEARCH}</td>
     *     <td>Searches using an exact match by key.</td>
     *     <td><a href="DatabaseEntry.html#inParam">input</a></td>
     *     <td><a href="DatabaseEntry.html#outParam">output</a></td>
     * </tr>
     * <tr>
     *     <td>{@link Get#SEARCH_BOTH}</td>
     *     <td>Searches using an exact match by key and data.</td>
     *     <td><a href="DatabaseEntry.html#inParam">input</a></td>
     *     <td><a href="DatabaseEntry.html#inParam">input</a></td>
     * </tr>
     * </table></div>
     *
     * @param txn For a transactional database, an explicit transaction may be
     * specified to transaction-protect the operation, or null may be specified
     * to perform the operation without transaction protection.  For a
     * non-transactional database, null must be specified.
     *
     * @param key the key input parameter.
     *
     * @param data the data input or output parameter, depending on getType.
     *
     * @param getType the Get operation type. May not be null.
     *
     * @param options the ReadOptions, or null to use default options.
     *
     * @return the OperationResult if the record requested is found, else null.
     *
     * @throws OperationFailureException if one of the <a
     * href="OperationFailureException.html#readFailures">Read Operation
     * Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if the database has been closed.
     *
     * @throws IllegalArgumentException if an invalid parameter is specified.
     * This includes passing a null getType, a null input key/data parameter,
     * an input key/data parameter with a null data array, and a partial
     * key/data input parameter.
     *
     * @since 7.0
     */
    public OperationResult get(
        final Transaction txn,
        final DatabaseEntry key,
        final DatabaseEntry data,
        final Get getType,
        ReadOptions options) {

        try {
            checkEnv();
            checkOpen();

            if (options == null) {
                options = Cursor.DEFAULT_READ_OPTIONS;
            }

            LockMode lockMode = options.getLockMode();

            trace(
                Level.FINEST, "Database.get", String.valueOf(getType), txn,
                key, null, lockMode);

            checkLockModeWithoutTxn(txn, lockMode);

            final CursorConfig cursorConfig;

            if (lockMode == LockMode.READ_COMMITTED) {
                cursorConfig = READ_COMMITTED_CURSOR_CONFIG;
                lockMode = null;
            } else {
                cursorConfig = DEFAULT_CURSOR_CONFIG;
            }

            OperationResult result = null;

            final Locker locker = LockerFactory.getReadableLocker(
                this, txn, cursorConfig.getReadCommitted());

            try {
                try (final Cursor cursor =
                         new Cursor(this, locker, cursorConfig)) {

                    result = cursor.getInternal(
                        key, data, getType, options, lockMode);
                }
            } finally {
                locker.operationEnd(result != null);
            }

            return result;

        } catch (Error E) {
            envHandle.invalidate(E);
            throw E;
        }
    }

    /**
     * Retrieves the key/data pair with the given key.  If the matching key has
     * duplicate values, the first data item in the set of duplicates is
     * returned. Retrieval of duplicates requires the use of {@link Cursor}
     * operations.
     *
     * <p>Calling this method is equivalent to calling {@link
     * #get(Transaction, DatabaseEntry, DatabaseEntry, Get, ReadOptions)} with
     * {@link Get#SEARCH}.</p>
     *
     * @param txn For a transactional database, an explicit transaction may be
     * specified to transaction-protect the operation, or null may be specified
     * to perform the operation without transaction protection.  For a
     * non-transactional database, null must be specified.
     *
     * @param key the key used as
     * <a href="DatabaseEntry.html#inParam">input</a>.
     *
     * @param data the data returned as
     * <a href="DatabaseEntry.html#outParam">output</a>.
     *
     * @param lockMode the locking attributes; if null, default attributes are
     * used.
     *
     * @return {@link com.sleepycat.je.OperationStatus#NOTFOUND
     * OperationStatus.NOTFOUND} if no matching key/data pair is found;
     * otherwise, {@link com.sleepycat.je.OperationStatus#SUCCESS
     * OperationStatus.SUCCESS}.
     *
     * @throws OperationFailureException if one of the <a
     * href="OperationFailureException.html#readFailures">Read Operation
     * Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if the database has been closed.
     *
     * @throws IllegalArgumentException if an invalid parameter is specified.
     */
    public OperationStatus get(final Transaction txn,
                               final DatabaseEntry key,
                               final DatabaseEntry data,
                               LockMode lockMode) {
        final OperationResult result = get(
            txn, key, data, Get.SEARCH, DbInternal.getReadOptions(lockMode));

        return result == null ?
            OperationStatus.NOTFOUND : OperationStatus.SUCCESS;
    }

    /**
     * Retrieves the key/data pair with the given key and data value, that is,
     * both the key and data items must match.
     *
     * <p>Calling this method is equivalent to calling {@link
     * #get(Transaction, DatabaseEntry, DatabaseEntry, Get, ReadOptions)} with
     * {@link Get#SEARCH_BOTH}.</p>
     *
     * @param txn For a transactional database, an explicit transaction may be
     * specified to transaction-protect the operation, or null may be specified
     * to perform the operation without transaction protection.  For a
     * non-transactional database, null must be specified.
     *
     * @param key the key used as
     * <a href="DatabaseEntry.html#inParam">input</a>.
     *
     * @param data the data used as
     * <a href="DatabaseEntry.html#inParam">input</a>.
     *
     * @param lockMode the locking attributes; if null, default attributes are
     * used.
     *
     * @return {@link com.sleepycat.je.OperationStatus#NOTFOUND
     * OperationStatus.NOTFOUND} if no matching key/data pair is found;
     * otherwise, {@link com.sleepycat.je.OperationStatus#SUCCESS
     * OperationStatus.SUCCESS}.
     *
     * @throws OperationFailureException if one of the <a
     * href="OperationFailureException.html#readFailures">Read Operation
     * Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if the database has been closed.
     *
     * @throws IllegalArgumentException if an invalid parameter is specified.
     */
    public OperationStatus getSearchBoth(final Transaction txn,
                                         final DatabaseEntry key,
                                         final DatabaseEntry data,
                                         LockMode lockMode) {
        final OperationResult result = get(
            txn, key, data, Get.SEARCH_BOTH,
            DbInternal.getReadOptions(lockMode));

        return result == null ?
            OperationStatus.NOTFOUND : OperationStatus.SUCCESS;
    }

    /**
     * Inserts or updates a record according to the specified {@link Put}
     * type.
     *
     * <p>If the operation succeeds, the record will be locked according to the
     * {@link ReadOptions#getLockMode() lock mode} specified, the cursor will
     * be positioned on the record, and a non-null OperationResult will be
     * returned. If the operation fails because the record already exists (or
     * does not exist, depending on the putType), null is returned.</p>
     *
     * <p>When the database has associated secondary databases, this method
     * also inserts or deletes associated index records as necessary.</p>
     *
     * <p>The following table lists each allowed operation. See the individual
     * {@link Put} operations for more information.</p>
     *
     * <div><table border="1" summary="">
     * <tr>
     *     <th>Put operation</th>
     *     <th>Description</th>
     *     <th>Returns null when?</th>
     *     <th>Other special rules</th>
     * </tr>
     * <tr>
     *     <td>{@link Put#OVERWRITE}</td>
     *     <td>Inserts or updates a record depending on whether a matching
     *     record is already present.</td>
     *     <td>Never returns null.</td>
     *     <td>Without duplicates, a matching record is one with the same key;
     *     with duplicates, it is one with the same key and data.</td>
     * </tr>
     * <tr>
     *     <td>{@link Put#NO_OVERWRITE}</td>
     *     <td>Inserts a record if a record with a matching key is not already
     *     present.</td>
     *     <td>When an existing record matches.</td>
     *     <td>If the database has duplicate keys, a record is inserted only if
     *     there are no records with a matching key.</td>
     * </tr>
     * <tr>
     *     <td>{@link Put#NO_DUP_DATA}</td>
     *     <td>Inserts a record in a database with duplicate keys if a record
     *     with a matching key and data is not already present.</td>
     *     <td>When an existing record matches.</td>
     *     <td>Without duplicates, this operation is not allowed.</td>
     * </tr>
     * </table></div>
     *
     * @param txn For a transactional database, an explicit transaction may be
     * specified, or null may be specified to use auto-commit.  For a
     * non-transactional database, null must be specified.
     *
     * @param key the key used as
     * <a href="DatabaseEntry.html#inParam">input</a>.
     *
     * @param data the data used as
     * <a href="DatabaseEntry.html#inParam">input</a>.
     *
     * @param putType the Put operation type. May not be null.
     *
     * @param options the WriteOptions, or null to use default options.
     *
     * @return the OperationResult if the record is written, else null.
     *
     * @throws OperationFailureException if one of the <a
     * href="../je/OperationFailureException.html#writeFailures">Write
     * Operation Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws UnsupportedOperationException if the database is read-only, or
     * putType is Put.NO_DUP_DATA and the database is not configured for
     * duplicates.
     *
     * @throws IllegalStateException if the database has been closed.
     *
     * @throws IllegalArgumentException if an invalid parameter is specified.
     * This includes passing a null putType, a null input key/data parameter,
     * an input key/data parameter with a null data array, a partial key/data
     * input parameter, or when putType is Put.CURRENT.
     *
     * @since 7.0
     */
    public OperationResult put(
        final Transaction txn,
        final DatabaseEntry key,
        final DatabaseEntry data,
        final Put putType,
        final WriteOptions options) {

        try {
            checkEnv();
            final DatabaseImpl dbImpl = checkOpen();

            if (putType == Put.CURRENT) {
                throw new IllegalArgumentException(
                    "putType may not be Put.CURRENT");
            }

            OperationResult result = null;

            trace(
                Level.FINEST, "Database.put", String.valueOf(putType), txn,
                key, data, null);

            final Locker locker = LockerFactory.getWritableLocker(
                envHandle, txn,
                dbImpl.isInternalDb(),
                isTransactional(),
                dbImpl.isReplicated()); // autoTxnIsReplicated

            try {
                try (final Cursor cursor =
                         new Cursor(this, locker, DEFAULT_CURSOR_CONFIG)) {

                    result = cursor.putInternal(key, data, putType, options);
                }
            } finally {
                locker.operationEnd(result != null);
            }

            return result;
        } catch (Error E) {
            envHandle.invalidate(E);
            throw E;
        }
    }

    /**
     * Stores the key/data pair into the database.
     *
     * <p>Calling this method is equivalent to calling {@link
     * #put(Transaction, DatabaseEntry, DatabaseEntry, Put, WriteOptions)} with
     * {@link Put#OVERWRITE}.</p>
     *
     * <p>If the key already appears in the database and duplicates are not
     * configured, the data associated with the key will be replaced.  If the
     * key already appears in the database and sorted duplicates are
     * configured, the new data value is inserted at the correct sorted
     * location.</p>
     *
     * @param txn For a transactional database, an explicit transaction may be
     * specified, or null may be specified to use auto-commit.  For a
     * non-transactional database, null must be specified.
     *
     * @param key the key used as
     * <a href="DatabaseEntry.html#inParam">input</a>..
     *
     * @param data the data used as
     * <a href="DatabaseEntry.html#inParam">input</a>.
     *
     * @return {@link com.sleepycat.je.OperationStatus#SUCCESS
     * OperationStatus.SUCCESS}.
     *
     * @throws OperationFailureException if one of the <a
     * href="../je/OperationFailureException.html#writeFailures">Write
     * Operation Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws UnsupportedOperationException if this database is read-only.
     *
     * @throws IllegalStateException if the database has been closed.
     */
    public OperationStatus put(final Transaction txn,
                               final DatabaseEntry key,
                               final DatabaseEntry data) {
        final OperationResult result = put(
            txn, key, data, Put.OVERWRITE, null);

        EnvironmentFailureException.assertState(result != null);
        return OperationStatus.SUCCESS;
    }

    /**
     * Stores the key/data pair into the database if the key does not already
     * appear in the database.
     *
     * <p>Calling this method is equivalent to calling {@link
     * #put(Transaction, DatabaseEntry, DatabaseEntry, Put, WriteOptions)} with
     * {@link Put#NO_OVERWRITE}.</p>
     *
     * <p>This method will return {@link
     * com.sleepycat.je.OperationStatus#KEYEXIST OpeationStatus.KEYEXIST} if
     * the key already exists in the database, even if the database supports
     * duplicates.</p>
     *
     * @param txn For a transactional database, an explicit transaction may be
     * specified, or null may be specified to use auto-commit.  For a
     * non-transactional database, null must be specified.
     *
     * @param key the key used as
     * <a href="DatabaseEntry.html#inParam">input</a>..
     *
     * @param data the data used as
     * <a href="DatabaseEntry.html#inParam">input</a>.
     *
     * @return {@link com.sleepycat.je.OperationStatus#KEYEXIST
     * OperationStatus.KEYEXIST} if the key already appears in the database,
     * else {@link com.sleepycat.je.OperationStatus#SUCCESS
     * OperationStatus.SUCCESS}
     *
     * @throws OperationFailureException if one of the <a
     * href="../je/OperationFailureException.html#writeFailures">Write
     * Operation Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws UnsupportedOperationException if this database is read-only.
     *
     * @throws IllegalStateException if the database has been closed.
     */
    public OperationStatus putNoOverwrite(final Transaction txn,
                                          final DatabaseEntry key,
                                          final DatabaseEntry data) {
        final OperationResult result = put(
            txn, key, data, Put.NO_OVERWRITE, null);

        return result == null ?
            OperationStatus.KEYEXIST : OperationStatus.SUCCESS;
    }

    /**
     * Stores the key/data pair into the database if it does not already appear
     * in the database.
     *
     * <p>Calling this method is equivalent to calling {@link
     * #put(Transaction, DatabaseEntry, DatabaseEntry, Put, WriteOptions)} with
     * {@link Put#NO_DUP_DATA}.</p>
     *
     * <p>This method may only be called if the underlying database has been
     * configured to support sorted duplicates.</p>
     *
     * @param txn For a transactional database, an explicit transaction may be
     * specified, or null may be specified to use auto-commit.  For a
     * non-transactional database, null must be specified.
     *
     * @param key the key used as
     * <a href="DatabaseEntry.html#inParam">input</a>..
     *
     * @param data the data used as
     * <a href="DatabaseEntry.html#inParam">input</a>.
     *
     * @return {@link com.sleepycat.je.OperationStatus#KEYEXIST
     * OperationStatus.KEYEXIST} if the key/data pair already appears in the
     * database, else {@link com.sleepycat.je.OperationStatus#SUCCESS
     * OperationStatus.SUCCESS}
     *
     * @throws OperationFailureException if one of the <a
     * href="../je/OperationFailureException.html#writeFailures">Write
     * Operation Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws UnsupportedOperationException if this database is not configured
     * for duplicates, or this database is read-only.
     *
     * @throws IllegalStateException if the database has been closed.
     */
    public OperationStatus putNoDupData(final Transaction txn,
                                        final DatabaseEntry key,
                                        final DatabaseEntry data) {
        final OperationResult result = put(
            txn, key, data, Put.NO_DUP_DATA, null);

        return result == null ?
            OperationStatus.KEYEXIST : OperationStatus.SUCCESS;
    }

    /**
     * Creates a specialized join cursor for use in performing equality or
     * natural joins on secondary indices.
     *
     * <p>Each cursor in the <code>cursors</code> array must have been
     * initialized to refer to the key on which the underlying database should
     * be joined.  Typically, this initialization is done by calling {@link
     * Cursor#getSearchKey Cursor.getSearchKey}.</p>
     *
     * <p>Once the cursors have been passed to this method, they should not be
     * accessed or modified until the newly created join cursor has been
     * closed, or else inconsistent results may be returned.  However, the
     * position of the cursors will not be changed by this method or by the
     * methods of the join cursor.</p>
     *
     * @param cursors an array of cursors associated with this primary
     * database.
     *
     * @param config The join attributes.  If null, default attributes are
     * used.
     *
     * @return a specialized cursor that returns the results of the equality
     * join operation.
     *
     * @throws OperationFailureException if one of the <a
     * href="OperationFailureException.html#readFailures">Read Operation
     * Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if the database has been closed.
     *
     * @throws IllegalArgumentException if an invalid parameter is specified,
     * for example, an invalid {@code JoinConfig} parameter.
     *
     * @see JoinCursor
     */
    public JoinCursor join(final Cursor[] cursors, final JoinConfig config) {
        try {
            EnvironmentImpl env = checkEnv();
            checkOpen();
            DatabaseUtil.checkForNullParam(cursors, "cursors");
            if (cursors.length == 0) {
                throw new IllegalArgumentException(
                    "At least one cursor is required.");
            }

            /*
             * Check that all cursors use the same locker, if any cursor is
             * transactional.  And if non-transactional, that all databases are
             * in the same environment.
             */
            Locker locker = cursors[0].getCursorImpl().getLocker();
            if (!locker.isTransactional()) {
                for (int i = 1; i < cursors.length; i += 1) {
                    Locker locker2 = cursors[i].getCursorImpl().getLocker();
                    if (locker2.isTransactional()) {
                        throw new IllegalArgumentException(
                            "All cursors must use the same transaction.");
                    }
                    EnvironmentImpl env2 =
                        cursors[i].getDatabaseImpl().getEnv();
                    if (env != env2) {
                        throw new IllegalArgumentException(
                            "All cursors must use the same environment.");
                    }
                }
            } else {
                for (int i = 1; i < cursors.length; i += 1) {
                    Locker locker2 = cursors[i].getCursorImpl().getLocker();
                    if (locker.getTxnLocker() != locker2.getTxnLocker()) {
                        throw new IllegalArgumentException(
                            "All cursors must use the same transaction.");
                    }
                }
            }

            /* Create the join cursor. */
            return new JoinCursor(this, cursors, config);
        } catch (Error E) {
            envHandle.invalidate(E);
            throw E;
        }
    }

    /**
     * Preloads the cache.  This method should only be called when there are no
     * operations being performed on the database in other threads.  Executing
     * preload during concurrent updates may result in some or all of the tree
     * being loaded into the JE cache.  Executing preload during any other
     * types of operations may result in JE exceeding its allocated cache
     * size. preload() effectively locks the entire database and therefore will
     * lock out the checkpointer, cleaner, and compressor, as well as not allow
     * eviction to occur.
     *
     * @deprecated As of JE 2.0.83, replaced by {@link
     * Database#preload(PreloadConfig)}.</p>
     *
     * @param maxBytes The maximum number of bytes to load.  If maxBytes is 0,
     * je.evictor.maxMemory is used.
     *
     * @throws OperationFailureException if one of the <a
     * href="OperationFailureException.html#readFailures">Read Operation
     * Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if the database has been closed.
     */
    public void preload(final long maxBytes) {
        checkEnv();
        final DatabaseImpl dbImpl = checkOpen();

        PreloadConfig config = new PreloadConfig();
        config.setMaxBytes(maxBytes);

        dbImpl.preload(config);
    }

    /**
     * Preloads the cache.  This method should only be called when there are no
     * operations being performed on the database in other threads.  Executing
     * preload during concurrent updates may result in some or all of the tree
     * being loaded into the JE cache.  Executing preload during any other
     * types of operations may result in JE exceeding its allocated cache
     * size. preload() effectively locks the entire database and therefore will
     * lock out the checkpointer, cleaner, and compressor, as well as not allow
     * eviction to occur.
     *
     * @deprecated As of JE 2.0.101, replaced by {@link
     * Database#preload(PreloadConfig)}.</p>
     *
     * @param maxBytes The maximum number of bytes to load.  If maxBytes is 0,
     * je.evictor.maxMemory is used.
     *
     * @param maxMillisecs The maximum time in milliseconds to use when
     * preloading.  Preloading stops once this limit has been reached.  If
     * maxMillisecs is 0, preloading can go on indefinitely or until maxBytes
     * (if non-0) is reached.
     *
     * @throws OperationFailureException if one of the <a
     * href="OperationFailureException.html#readFailures">Read Operation
     * Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if the database has been closed.
     */
    public void preload(final long maxBytes, final long maxMillisecs) {
        checkEnv();
        final DatabaseImpl dbImpl = checkOpen();

        PreloadConfig config = new PreloadConfig();
        config.setMaxBytes(maxBytes);
        config.setMaxMillisecs(maxMillisecs);

        dbImpl.preload(config);
    }

    /**
     * Preloads the cache.  This method should only be called when there are no
     * operations being performed on the database in other threads.  Executing
     * preload during concurrent updates may result in some or all of the tree
     * being loaded into the JE cache.  Executing preload during any other
     * types of operations may result in JE exceeding its allocated cache
     * size. preload() effectively locks the entire database and therefore will
     * lock out the checkpointer, cleaner, and compressor, as well as not allow
     * eviction to occur.  If the database is replicated and the environment is
     * in the replica state, then the replica may become temporarily
     * disconnected from the master if the replica needs to replay changes
     * against the database and is locked out because the time taken by the
     * preload operation exceeds {@link
     * com.sleepycat.je.rep.ReplicationConfig#FEEDER_TIMEOUT}.
     * <p>
     * While this method preloads a single database, {@link
     * Environment#preload} lets you preload multiple databases.
     *
     * @param config The PreloadConfig object that specifies the parameters
     * of the preload.
     *
     * @return A PreloadStats object with various statistics about the
     * preload() operation.
     *
     * @throws OperationFailureException if one of the <a
     * href="OperationFailureException.html#readFailures">Read Operation
     * Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if the database has been closed.
     *
     * @throws IllegalArgumentException if {@code PreloadConfig.getMaxBytes} is
     * greater than size of the JE cache.
     */
    public PreloadStats preload(final PreloadConfig config) {
        checkEnv();
        final DatabaseImpl dbImpl = checkOpen();

        PreloadConfig useConfig =
            (config == null) ? new PreloadConfig() : config;

        return dbImpl.preload(useConfig);
    }

    /**
     * Counts the key/data pairs in the database. This operation is faster than
     * obtaining a count from a cursor based scan of the database, and will not
     * perturb the current contents of the cache. However, the count is not
     * guaranteed to be accurate if there are concurrent updates. Note that
     * this method does scan a significant portion of the database and should
     * be considered a fairly expensive operation. 
     * <p>
     * This operation uses the an internal infrastructure and algorithm that is
     * similar to the one used for the {@link DiskOrderedCursor}. Specifically,
     * it will disable deletion of log files by the JE log cleaner during its
     * execution and will consume a certain amount of memory (but without
     * affecting the memory that is available for the JE cache). To avoid
     * excessive memory consumption (and a potential {@code OutOfMemoryError})
     * this method places an internal limit on its memory consumption. If this
     * limit is reached, the method will still work properly, but its
     * performance will degrade. To specify a different memory limit than the
     * one used by this method, use the
     * {@link Database#count(long memoryLimit)} method.
     * <p>
     * Currently, the internal memory limit is calculated as 10% of the
     * difference between the max JVM memory (the value returned by
     * Runtime.getRuntime().maxMemory()) and the configured JE cache size.
     *
     * @return The count of key/data pairs in the database.
     *
     * @throws OperationFailureException if one of the <a
     * href="OperationFailureException.html#readFailures">Read Operation
     * Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if the database has been closed.
     *
     * @see <a href="EnvironmentStats.html#cacheUnexpectedSizes">Cache
     * Statistics: Unexpected Sizes</a>
     */
    public long count() {
        checkEnv();
        final DatabaseImpl dbImpl = checkOpen();

        return dbImpl.count(0);
    }

    /**
     * Counts the key/data pairs in the database. This operation is faster than
     * obtaining a count from a cursor based scan of the database, and will not
     * perturb the current contents of the cache. However, the count is not
     * guaranteed to be accurate if there are concurrent updates. Note that
     * this method does scan a significant portion of the database and should
     * be considered a fairly expensive operation. 
     * <p>
     * This operation uses the an internal infrastructure and algorithm that is
     * similar to the one used for the {@link DiskOrderedCursor}. Specifically,
     * it will disable deletion of log files by the JE log cleaner during its
     * execution and will consume a certain amount of memory (but without
     * affecting the memory that is available for the JE cache). To avoid
     * excessive memory consumption (and a potential {@code OutOfMemoryError})
     * this method takes as input an upper bound on the memory it may consume.
     * If this limit is reached, the method will still work properly, but its
     * performance will degrade.
     *
     * @param memoryLimit The maximum memory (in bytes) that may be consumed
     * by this method.
     *
     * @return The count of key/data pairs in the database.
     *
     * @throws OperationFailureException if one of the <a
     * href="OperationFailureException.html#readFailures">Read Operation
     * Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if the database has been closed.
     *
     * @see <a href="EnvironmentStats.html#cacheUnexpectedSizes">Cache
     * Statistics: Unexpected Sizes</a>
     */
    public long count(long memoryLimit) {
        checkEnv();
        final DatabaseImpl dbImpl = checkOpen();

        return dbImpl.count(memoryLimit);
    }

    /**
     * Returns database statistics.
     *
     * <p>If this method has not been configured to avoid expensive operations
     * (using the {@link com.sleepycat.je.StatsConfig#setFast
     * StatsConfig.setFast} method), it will access some of or all the pages in
     * the database, incurring a severe performance penalty as well as possibly
     * flushing the underlying cache.</p>
     *
     * <p>In the presence of multiple threads or processes accessing an active
     * database, the information returned by this method may be
     * out-of-date.</p>
     *
     * @param config The statistics returned; if null, default statistics are
     * returned.
     *
     * @return Database statistics.
     *
     * @throws OperationFailureException if one of the <a
     * href="OperationFailureException.html#readFailures">Read Operation
     * Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if the database has been closed.
     */
    public DatabaseStats getStats(StatsConfig config) {
        checkEnv();
        final DatabaseImpl dbImpl = checkOpen();

        if (config == null) {
            config = StatsConfig.DEFAULT;
        }

        return dbImpl.stat(config);
    }

    /**
     * Verifies the integrity of the database.
     *
     * <p>Verification is an expensive operation that should normally only be
     * used for troubleshooting and debugging.</p>
     *
     * @param config Configures the verify operation; if null, the default
     * operation is performed.
     *
     * @return Database statistics.
     *
     * @throws OperationFailureException if one of the <a
     * href="OperationFailureException.html#readFailures">Read Operation
     * Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if a corruption is detected, or if
     * an unexpected, internal or environment-wide failure occurs. If a
     * persistent corruption is detected,
     * {@link EnvironmentFailureException#isCorrupted()} will return true.
     *
     * @throws IllegalStateException if the database has been closed.
     *
     * @throws IllegalArgumentException if an invalid parameter is specified.
     */
    public DatabaseStats verify(VerifyConfig config) {
        try {
            checkEnv();
            final DatabaseImpl dbImpl = checkOpen();

            if (config == null) {
                config = VerifyConfig.DEFAULT;
            }

            return dbImpl.verify(config);
        } catch (Error E) {
            envHandle.invalidate(E);
            throw E;
        }
    }

    /**
     * Returns the database name.
     *
     * <p>This method may be called at any time during the life of the
     * application.</p>
     *
     * @return The database name.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if the database has been closed.
     */
    public String getDatabaseName() {
        try {
            checkEnv();
            final DatabaseImpl dbImpl = checkOpen();

            return dbImpl.getName();
        } catch (Error E) {
            envHandle.invalidate(E);
            throw E;
        }
    }

    /*
     * Non-transactional database name, safe to access when creating error
     * messages.
     */
    String getDebugName() {
        final DatabaseImpl dbImpl = databaseImpl;
        return (dbImpl == null) ? "[closed]" : dbImpl.getDebugName();
    }

    /**
     * Returns this Database object's configuration.
     *
     * <p>This may differ from the configuration used to open this object if
     * the database existed previously.</p>
     *
     * <p>Unlike most Database methods, this method may be called after the
     * database is closed.</p>
     *
     * @return This Database object's configuration.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if the database has been closed.
     */
    public DatabaseConfig getConfig() {

        checkEnv();
        final DatabaseImpl dbImpl = checkOpen();

        try {
            return DatabaseConfig.combineConfig(dbImpl, configuration);
        } catch (Error E) {
            envHandle.invalidate(E);
            throw E;
        }
    }

    /**
     * Equivalent to getConfig().getTransactional() but cheaper.
     */
    boolean isTransactional() {
        final DatabaseImpl dbImpl = checkOpen();
        return dbImpl.isTransactional();
    }

    /**
     * Returns the {@link com.sleepycat.je.Environment Environment} handle for
     * the database environment underlying the {@link
     * com.sleepycat.je.Database Database}.
     *
     * <p>This method may be called at any time during the life of the
     * application.</p>
     *
     * @return The {@link com.sleepycat.je.Environment Environment} handle
     * for the database environment underlying the {@link
     * com.sleepycat.je.Database Database}.
     */
    public Environment getEnvironment() {
        return envHandle;
    }

    /**
     * Returns a list of all {@link com.sleepycat.je.SecondaryDatabase
     * SecondaryDatabase} objects associated with a primary database.
     *
     * <p>If no secondaries are associated with this database, an empty list is
     * returned.</p>
     */
    /*
     * Replacement for above paragraph when SecondaryAssociation is published:
     * <p>If no secondaries are associated with this database, or a {@link
     * SecondaryAssociation} is {@link DatabaseConfig#setSecondaryAssociation
     * configured}, an empty list is returned.</p>
     */
    public List<SecondaryDatabase> getSecondaryDatabases() {
        return new ArrayList<>(simpleAssocSecondaries);
    }

    /**
     * Compares two keys using either the default comparator if no BTree
     * comparator has been set or the BTree comparator if one has been set.
     *
     * @return -1 if entry1 compares less than entry2,
     *          0 if entry1 compares equal to entry2,
     *          1 if entry1 compares greater than entry2
     *
     * @throws IllegalStateException if the database has been closed.
     *
     * @throws IllegalArgumentException if either entry is a partial
     * DatabaseEntry, or is null.
     */
    public int compareKeys(final DatabaseEntry entry1,
                           final DatabaseEntry entry2) {
        return doCompareKeys(entry1, entry2, false/*duplicates*/);
    }

    /**
     * Compares two data elements using either the default comparator if no
     * duplicate comparator has been set or the duplicate comparator if one has
     * been set.
     *
     * @return -1 if entry1 compares less than entry2,
     *          0 if entry1 compares equal to entry2,
     *          1 if entry1 compares greater than entry2
     *
     * @throws IllegalStateException if the database has been closed.
     *
     * @throws IllegalArgumentException if either entry is a partial
     * DatabaseEntry, or is null.
     */
    public int compareDuplicates(final DatabaseEntry entry1,
                                 final DatabaseEntry entry2) {
        return doCompareKeys(entry1, entry2, true/*duplicates*/);
    }

    private int doCompareKeys(final DatabaseEntry entry1,
                              final DatabaseEntry entry2,
                              final boolean duplicates) {
        try {
            checkEnv();
            final DatabaseImpl dbImpl = checkOpen();
            DatabaseUtil.checkForNullDbt(entry1, "entry1", true);
            DatabaseUtil.checkForNullDbt(entry2, "entry2", true);
            DatabaseUtil.checkForPartial(entry1, "entry1");
            DatabaseUtil.checkForPartial(entry2, "entry2");

            return dbImpl.compareEntries(entry1, entry2, duplicates);

        } catch (Error E) {
            envHandle.invalidate(E);
            throw E;
        }
    }

    /*
     * Helpers, not part of the public API
     */

    /**
     * Returns true if the Database was opened read/write.
     *
     * @return true if the Database was opened read/write.
     */
    boolean isWritable() {
        return isWritable;
    }

    /**
     * Returns the non-null, underlying getDbImpl.
     *
     * This method should always be called to access the databaseImpl, to guard
     * against NPE when the database has been closed after the initial checks.
     *
     * However, callers should additionally call checkOpen at API entry points
     * to reject the operation as soon as possible. Plus, if the database has
     * been closed, this method may return non-null because the databaseImpl
     * field is not volatile.
     *
     * @throws IllegalStateException if the database has been closed since
     * checkOpen was last called.
     */
    DatabaseImpl getDbImpl() {

        final DatabaseImpl dbImpl = databaseImpl;

        if (dbImpl != null) {
            return dbImpl;
        }

        checkOpen();

        /*
         * checkOpen should have thrown an exception, but we'll throw again
         * here just in case.
         */
        throw new IllegalStateException("Database is closed. State=" + state);
    }

    /**
     * Called during database open to set the handleLocker field.
     * @see HandleLocker
     */
    HandleLocker initHandleLocker(EnvironmentImpl envImpl,
                                  Locker openDbLocker) {
        handleLocker = HandleLocker.createHandleLocker(envImpl, openDbLocker);
        return handleLocker;
    }

    @SuppressWarnings("unused")
    void removeCursor(final ForwardCursor ignore)
        throws DatabaseException {

        /*
         * Do not call checkOpen if the handle was preempted or corrupted, to
         * allow closing a cursor after an operation failure. [#17015]
         */
        if (state != DbState.PREEMPTED && state != DbState.CORRUPTED) {
            checkOpen();
        }
        openCursors.getAndDecrement();
    }

    @SuppressWarnings("unused")
    void addCursor(final ForwardCursor ignore) {
        checkOpen();
        openCursors.getAndIncrement();
    }

    DatabaseImpl checkOpen() {
        switch (state) {
        case OPEN:
            return databaseImpl;
        case CLOSED:
            throw new IllegalStateException("Database was closed.");
        case INVALID:
            throw new IllegalStateException(
                "The Transaction used to open the Database was aborted.");
        case PREEMPTED:
            throw preemptedCause.wrapSelf(preemptedCause.getMessage());
        case CORRUPTED:
            throw corruptedCause.wrapSelf(corruptedCause.getMessage());
        default:
            assert false : state;
            return null;
        }
    }

    /**
     * @throws EnvironmentFailureException if the underlying environment is
     * invalid.
     * @throws IllegalStateException if the environment is not open.
     */
    EnvironmentImpl checkEnv() {
        return envHandle.checkOpen();
    }

    void checkLockModeWithoutTxn(final Transaction userTxn,
                                 final LockMode lockMode) {
        if (userTxn == null && LockMode.RMW.equals(lockMode)) {
            throw new IllegalArgumentException(
                lockMode + " is meaningless and can not be specified " +
                "when a null (autocommit) transaction is used. There " +
                "will never be a follow on operation which will promote " +
                "the lock to WRITE.");
        }
    }

    /**
     * Sends trace messages to the java.util.logger. Don't rely on the logger
     * alone to conditionalize whether we send this message, we don't even want
     * to construct the message if the level is not enabled.
     */
    void trace(final Level level,
               final String methodName,
               final String getOrPutType,
               final Transaction txn,
               final DatabaseEntry key,
               final DatabaseEntry data,
               final LockMode lockMode) {

        if (logger.isLoggable(level)) {
            StringBuilder sb = new StringBuilder();
            sb.append(methodName).append(" ");
            sb.append(getOrPutType);
            if (txn != null) {
                sb.append(" txnId=").append(txn.getId());
            }
            sb.append(" key=").append(key.dumpData());
            if (data != null) {
                sb.append(" data=").append(data.dumpData());
            }
            if (lockMode != null) {
                sb.append(" lockMode=").append(lockMode);
            }
            LoggerUtils.logMsg(
                logger, getEnv(), level, sb.toString());
        }
    }

    /**
     * Sends trace messages to the java.util.logger. Don't rely on the logger
     * alone to conditionalize whether we send this message, we don't even want
     * to construct the message if the level is not enabled.
     */
    void trace(final Level level,
               final String methodName,
               final Transaction txn,
               final DatabaseEntry key,
               final DatabaseEntry data,
               final LockMode lockMode) {

        if (logger.isLoggable(level)) {
            StringBuilder sb = new StringBuilder();
            sb.append(methodName);
            if (txn != null) {
                sb.append(" txnId=").append(txn.getId());
            }
            sb.append(" key=").append(key.dumpData());
            if (data != null) {
                sb.append(" data=").append(data.dumpData());
            }
            if (lockMode != null) {
                sb.append(" lockMode=").append(lockMode);
            }
            LoggerUtils.logMsg(
                logger, getEnv(), level, sb.toString());
        }
    }

    /**
     * Sends trace messages to the java.util.logger. Don't rely on the logger
     * alone to conditionalize whether we send this message, we don't even want
     * to construct the message if the level is not enabled.
     */
    void trace(final Level level,
               final String methodName,
               final Transaction txn,
               final Object config) {

        if (logger.isLoggable(level)) {
            StringBuilder sb = new StringBuilder();
            sb.append(methodName);
            sb.append(" name=").append(getDebugName());
            if (txn != null) {
                sb.append(" txnId=").append(txn.getId());
            }
            if (config != null) {
                sb.append(" config=").append(config);
            }
            LoggerUtils.logMsg(
                logger, getEnv(), level, sb.toString());
        }
    }

    boolean hasSecondaryOrForeignKeyAssociations() {
        return (!secAssoc.isEmpty() || !foreignKeySecondaries.isEmpty());
    }

    SecondaryAssociation getSecondaryAssociation() {
        return secAssoc;
    }

    /**
     * Creates a SecondaryIntegrityException using the information given.
     *
     * This method is in the Database class, rather than in SecondaryDatabase,
     * to support joins with plain Cursors [#21258].
     */
    SecondaryIntegrityException secondaryRefersToMissingPrimaryKey(
        final Locker locker,
        final DatabaseEntry secKey,
        final DatabaseEntry priKey,
        final long expirationTime) {

        return new SecondaryIntegrityException(
            this,
            locker,
            "Secondary refers to a missing key in the primary database",
            getDebugName(), secKey, priKey, expirationTime);
    }
}
