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
import java.io.File;
import java.io.PrintStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import javax.transaction.xa.Xid;

import com.sleepycat.je.Durability.ReplicaAckPolicy;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.DbConfigManager;
import com.sleepycat.je.dbi.DbEnvPool;
import com.sleepycat.je.dbi.DbTree;
import com.sleepycat.je.dbi.DbTree.TruncateDbResult;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.dbi.RepConfigProxy;
import com.sleepycat.je.dbi.StartupTracker.Phase;
import com.sleepycat.je.dbi.TriggerManager;
import com.sleepycat.je.txn.HandleLocker;
import com.sleepycat.je.txn.Locker;
import com.sleepycat.je.txn.LockerFactory;
import com.sleepycat.je.txn.Txn;
import com.sleepycat.je.utilint.DatabaseUtil;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.Pair;

/**
 * A database environment.  Environments include support for some or all of
 * caching, locking, logging and transactions.
 *
 * <p>To open an existing environment with default attributes the application
 * may use a default environment configuration object or null:
 * <blockquote>
 *     <pre>
 *      // Open an environment handle with default attributes.
 *     Environment env = new Environment(home, new EnvironmentConfig());
 *     </pre>
 * </blockquote>
 * or
 * <blockquote><pre>
 *     Environment env = new Environment(home, null);
 * </pre></blockquote>
 * <p>Note that many Environment objects may access a single environment.</p>
 * <p>To create an environment or customize attributes, the application should
 * customize the configuration class. For example:</p>
 * <blockquote><pre>
 *     EnvironmentConfig envConfig = new EnvironmentConfig();
 *     envConfig.setTransactional(true);
 *     envConfig.setAllowCreate(true);
 *     envConfig.setCacheSize(1000000);
 *     Environment newlyCreatedEnv = new Environment(home, envConfig);
 * </pre></blockquote>
 *
 * <p>Note that environment configuration parameters can also be set through
 * the &lt;environment home&gt;/je.properties file. This file takes precedence
 * over any programmatically specified configuration parameters so that
 * configuration changes can be made without recompiling. Environment
 * configuration follows this order of precedence:</p>
 *
 * <ol>
 * <li>Configuration parameters specified in
 * &lt;environment home&gt;/je.properties take first precedence.
 * <li> Configuration parameters set in the EnvironmentConfig object used at
 * Environment construction e   tameters not set by the application are set to
 * system defaults, described along with the parameter name String constants
 * in the EnvironmentConfig class.
 * </ol>
 *
 * <p>An <em>environment handle</em> is an Environment instance.  More than one
 * Environment instance may be created for the same physical directory, which
 * is the same as saying that more than one Environment handle may be open at
 * one time for a given environment.</p>
 *
 * The Environment handle should not be closed while any other handle remains
 * open that is using it as a reference (for example, {@link
 * com.sleepycat.je.Database Database} or {@link com.sleepycat.je.Transaction
 * Transaction}.  Once {@link com.sleepycat.je.Environment#close
 * Environment.close} is called, this object may not be accessed again.
 */
public class Environment implements Closeable {

    /**
     * envImpl is a reference to the shared underlying environment.
     *
     * The envImpl field is set to null during close to avoid OOME. It
     * should normally only be accessed via the checkOpen and
     * getNonNullEnvImpl methods. During close, while synchronized, it is safe
     * to access it directly.
     */
    private volatile EnvironmentImpl environmentImpl;

    /*
     * If the env was invalided (even if the env is now closed) this contains
     * the first EFE that invalidated it. Contains null if the env was not
     * invalidated.
     *
     * This reference is shared with EnvironmentImpl, to allow the invalidating
     * exception to be returned after close, when environmentImpl is null.
     * The EFE does not reference the EnvironmentImpl, so GC is not effected.
     *
     * This field cannot be declared as final because it is initialized by
     * methods called by the ctor. However, after construction it is non-null
     * and should be treated as final.
     */
    private AtomicReference<EnvironmentFailureException> invalidatingEFE;

    private TransactionConfig defaultTxnConfig;
    private EnvironmentMutableConfig handleConfig;
    private final EnvironmentConfig appliedFinalConfig;

    private final Map<Database, Database> referringDbs;
    private final Map<Transaction, Transaction> referringDbTxns;

    /**
     * @hidden
     * The name of the cleaner daemon thread.  This constant is passed to an
     * ExceptionEvent's threadName argument when an exception is thrown in the
     * cleaner daemon thread.
     */
    public static final String CLEANER_NAME = "Cleaner";

    /**
     * @hidden
     * The name of the IN Compressor daemon thread.  This constant is passed to
     * an ExceptionEvent's threadName argument when an exception is thrown in
     * the IN Compressor daemon thread.
     */
    public static final String INCOMP_NAME = "INCompressor";

    /**
     * @hidden
     * The name of the Checkpointer daemon thread.  This constant is passed to
     * an ExceptionEvent's threadName argument when an exception is thrown in
     * the Checkpointer daemon thread.
     */
    public static final String CHECKPOINTER_NAME = "Checkpointer";

    /**
     * @hidden
     * The name of the StatCapture daemon thread.  This constant is passed to
     * an ExceptionEvent's threadName argument when an exception is thrown in
     * the StatCapture daemon thread.
     */
    public static final String STATCAPTURE_NAME = "StatCapture";

    /**
     * @hidden
     * The name of the log flusher daemon thread.
     */
    public static final String LOG_FLUSHER_NAME = "LogFlusher";

    /**
     * @hidden
     * The name of the deletion detector daemon thread.
     */
    public static final String FILE_DELETION_DETECTOR_NAME =
        "FileDeletionDetector";

    /**
     * @hidden
     * The name of the data corruption verifier daemon thread.
     */
    public static final String DATA_CORRUPTION_VERIFIER_NAME =
        "DataCorruptionVerifier";

    /**
     * Creates a database environment handle.
     *
     * @param envHome The database environment's home directory.
     *
     * @param configuration The database environment attributes.  If null,
     * default attributes are used.
     *
     * @throws EnvironmentNotFoundException if the environment does not exist
     * (does not contain at least one log file) and the {@code
     * EnvironmentConfig AllowCreate} parameter is false.
     *
     * @throws EnvironmentLockedException when an environment cannot be opened
     * for write access because another process has the same environment open
     * for write access.  <strong>Warning:</strong> This exception should be
     * handled when an environment is opened by more than one process.
     *
     * @throws VersionMismatchException when the existing log is not compatible
     * with the version of JE that is running.  This occurs when a later
     * version of JE was used to create the log.  <strong>Warning:</strong>
     * This exception should be handled when more than one version of JE may be
     * used to access an environment.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws UnsupportedOperationException if this environment was previously
     * opened for replication and is not being opened read-only.
     *
     * @throws IllegalArgumentException if an invalid parameter is specified,
     * for example, an invalid {@code EnvironmentConfig} parameter.
     */
    public Environment(File envHome, EnvironmentConfig configuration)
        throws EnvironmentNotFoundException,
               EnvironmentLockedException,
               VersionMismatchException,
               DatabaseException,
               IllegalArgumentException {

        this(envHome, configuration, null /*repConfigProxy*/,
             null /*envImplParam*/);
    }

    /**
     * @hidden
     * Internal common constructor.
     *
     * @param envImpl is non-null only when used by EnvironmentImpl to
     * create an InternalEnvironment.
     */
    protected Environment(File envHome,
                          EnvironmentConfig envConfig,
                          RepConfigProxy repConfigProxy,
                          EnvironmentImpl envImpl) {

        referringDbs = new ConcurrentHashMap<Database, Database>();
        referringDbTxns = new ConcurrentHashMap<Transaction, Transaction>();

        DatabaseUtil.checkForNullParam(envHome, "envHome");

        appliedFinalConfig =
            setupHandleConfig(envHome, envConfig, repConfigProxy);

        if (envImpl != null) {
            /* We're creating an InternalEnvironment in EnvironmentImpl. */
            environmentImpl = envImpl;
        } else {
            /* Open a new or existing environment in the shared pool. */
            environmentImpl =
                makeEnvironmentImpl(envHome, envConfig, repConfigProxy);
        }
    }

    /**
     * @hidden
     * makeEnvironmentImpl() is called both by the Environment constructor and
     * by the ReplicatedEnvironment constructor when recreating the environment
     * for a hard recovery.
     */
    protected EnvironmentImpl makeEnvironmentImpl(
        File envHome,
        EnvironmentConfig envConfig,
        RepConfigProxy repConfigProxy) {

        environmentImpl = DbEnvPool.getInstance().getEnvironment(
            envHome,
            appliedFinalConfig,
            envConfig != null /*checkImmutableParams*/,
            setupRepConfig(envHome, repConfigProxy, envConfig));

        environmentImpl.registerMBean(this);

        invalidatingEFE = environmentImpl.getInvalidatingExceptionReference();

        return environmentImpl;
    }

    /**
     * Validate the parameters specified in the environment config.  Applies
     * the configurations specified in the je.properties file to override any
     * programmatically set configurations.  Create a copy to save in this
     * handle. The main reason to return a config instead of using the
     * handleConfig field is to return an EnvironmentConfig instead of a
     * EnvironmentMutableConfig.
     */
    private EnvironmentConfig setupHandleConfig(File envHome,
                                                EnvironmentConfig envConfig,
                                                RepConfigProxy repConfig)
        throws IllegalArgumentException {

        /* If the user specified a null object, use the default */
        EnvironmentConfig baseConfig = (envConfig == null) ?
            EnvironmentConfig.DEFAULT : envConfig;

        /* Make a copy, apply je.properties, and init the handle config. */
        EnvironmentConfig useConfig = baseConfig.clone();

        /* Apply the je.properties file. */
        if (useConfig.getLoadPropertyFile()) {
            DbConfigManager.applyFileConfig(envHome,
                                            DbInternal.getProps(useConfig),
                                            false);       // forReplication
        }
        copyToHandleConfig(useConfig, useConfig, repConfig);
        return useConfig;
    }

    /**
     * @hidden
     * Obtain a validated replication configuration. In a non-HA environment,
     * return null.
     */
    protected RepConfigProxy
        setupRepConfig(final File envHome,
                       final RepConfigProxy repConfigProxy,
                       final EnvironmentConfig envConfig) {

        return null;
    }

    /**
     * The Environment.close method closes the Berkeley DB environment.
     *
     * <p>When the last environment handle is closed, allocated resources are
     * freed, and daemon threads are stopped, even if they are performing work.
     * For example, if the cleaner is still cleaning the log, it will be
     * stopped at the next reasonable opportunity and perform no more cleaning
     * operations. After stopping background threads, a final checkpoint is
     * performed by this method, in order to reduce the time to recover the
     * next time the environment is opened.</p>
     *
     * <p>When minimizing recovery time is desired, it is often useful to stop
     * all application activity and perform an additional checkpoint prior to
     * calling {@code close}. This additional checkpoint will write most of
     * dirty Btree information, so that that the final checkpoint is very
     * small (and recovery is fast). To ensure that recovery time is minimized,
     * the log cleaner threads should also be stopped prior to the extra
     * checkpoint. This prevents log cleaning from dirtying the Btree, which
     * can make the final checkpoint larger (and recovery time longer). The
     * recommended procedure for minimizing recovery time is:</p>
     *
     * <pre>
     *     // Stop/finish all application operations that are using JE.
     *     ...
     *
     *     // Stop the cleaner daemon threads.
     *     EnvironmentMutableConfig config = env.getMutableConfig();
     *     config.setConfigParam(EnvironmentConfig.ENV_RUN_CLEANER, "false");
     *     env.setMutableConfig(config);
     *
     *     // Perform an extra checkpoint
     *     env.checkpoint(new CheckpointConfig().setForce(true));
     *
     *     // Finally, close the environment.
     *     env.close();
     * </pre>
     *
     * <p>The Environment handle should not be closed while any other handle
     * that refers to it is not yet closed; for example, database environment
     * handles must not be closed while database handles remain open, or
     * transactions in the environment have not yet committed or aborted.
     * Specifically, this includes {@link com.sleepycat.je.Database Database},
     * and {@link com.sleepycat.je.Transaction Transaction} handles.</p>
     *
     * <p>If this handle has already been closed, this method does nothing and
     * returns without throwing an exception.</p>
     *
     * <p>In multithreaded applications, only a single thread should call
     * Environment.close.</p>
     *
     * <p>The environment handle may not be used again after this method has
     * been called, regardless of the method's success or failure, with one
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
     * @throws EnvironmentWedgedException when the current process must be
     * shut down and restarted before re-opening the Environment.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws DiskLimitException if the final checkpoint cannot be performed
     * because a disk limit has been violated. The Environment will be closed,
     * but this exception will be thrown so that the application is aware that
     * a checkpoint was not performed.
     *
     * @throws IllegalStateException if any open databases or transactions
     * refer to this handle. The Environment will be closed, but this exception
     * will be thrown so that the application is aware that not all databases
     * and transactions were closed.
     */
    public synchronized void close()
        throws DatabaseException {

        if (environmentImpl == null) {
            return;
        }

        if (!environmentImpl.isValid()) {

            /*
             * We're trying to close on an environment that has seen a fatal
             * exception. Try to do the minimum, such as closing file
             * descriptors, to support re-opening the environment in the same
             * JVM.
             */
            try {
                environmentImpl.closeAfterInvalid();
            } finally {
                clearEnvImpl();

                for (Database db : referringDbs.keySet()) {
                    db.minimalClose(Database.DbState.CLOSED, null);
                }
            }
            return;
        }

        final StringBuilder errors = new StringBuilder();
        try {
            checkForCloseErrors(errors);

            try {
                environmentImpl.close();
            } catch (DatabaseException e) {
                e.addErrorMessage(errors.toString());
                throw e;
            } catch (RuntimeException e) {
                if (errors.length() > 0) {
                    throw new IllegalStateException(errors.toString(), e);
                }
                throw e;
            }

            if (errors.length() > 0) {
                throw new IllegalStateException(errors.toString());
            }
        } finally {
            clearEnvImpl();
        }
    }

    /**
     * Set environmentImpl to null during close, to allow GC when the app may
     * hold on to a reference to the Environment handle for some time period.
     */
    void clearEnvImpl() {
        environmentImpl = null;
    }

    /**
     * Close an InternalEnvironment handle.  We do not call
     * EnvironmentImpl.close here, since an InternalEnvironment is not
     * registered like a non-internal handle.  However, we must call
     * checkForCloseErrors to auto-close internal databases, as well as check
     * for errors.
     */
    synchronized void closeInternalHandle() {
        final StringBuilder errors = new StringBuilder();
        checkForCloseErrors(errors);
        if (errors.length() > 0) {
            throw new IllegalStateException(errors.toString());
        }
    }

    private void checkForCloseErrors(StringBuilder errors) {

        checkOpenDbs(errors);

        checkOpenTxns(errors);

        if (!isInternalHandle()) {

            /*
             * Only check for open XA transactions against user created
             * environment handles.
             */
            checkOpenXATransactions(errors);
        }
    }

    /**
     * Appends error messages to the errors argument if there are
     * open XA transactions associated with the underlying EnvironmentImpl.
     */
    private void checkOpenXATransactions(final StringBuilder errors) {
        Xid[] openXids = getNonNullEnvImpl().getTxnManager().XARecover();
        if (openXids != null && openXids.length > 0) {
            errors.append("There ");
            int nXATxns = openXids.length;
            if (nXATxns == 1) {
                errors.append("is 1 existing XA transaction opened");
                errors.append(" in the Environment.\n");
                errors.append("It");
            } else {
                errors.append("are ");
                errors.append(nXATxns);
                errors.append(" existing transactions opened in");
                errors.append(" the Environment.\n");
                errors.append("They");
            }
            errors.append(" will be left open ...\n");
        }
    }

    /**
     * Appends error messages to the errors argument if there are open
     * transactions associated with the environment.
     */
    private void checkOpenTxns(final StringBuilder errors) {
        int nTxns = (referringDbTxns == null) ? 0 : referringDbTxns.size();
        if (nTxns == 0) {
            return;
        }

        errors.append("There ");
        if (nTxns == 1) {
            errors.append("is 1 existing transaction opened");
            errors.append(" against the Environment.\n");
        } else {
            errors.append("are ");
            errors.append(nTxns);
            errors.append(" existing transactions opened against");
            errors.append(" the Environment.\n");
        }
        errors.append("Aborting open transactions ...\n");

       for (Transaction txn : referringDbTxns.keySet()) {
            try {
                errors.append("aborting " + txn);
                txn.abort();
            } catch (RuntimeException e) {
                if (!environmentImpl.isValid()) {
                    /* Propagate if env is invalidated. */
                    throw e;
                }
                errors.append("\nWhile aborting transaction ");
                errors.append(txn.getId());
                errors.append(" encountered exception: ");
                errors.append(e).append("\n");
            }
        }
    }

    /**
     * Appends error messages to the errors argument if there are open database
     * handles associated with the environment.
     */
    private void checkOpenDbs(final StringBuilder errors) {

        if (referringDbs.isEmpty()) {
            return;
        }

        int nOpenUserDbs = 0;

        for (Database db : referringDbs.keySet()) {
            String dbName = "";
            try {

                /*
                 * Save the db name before we attempt the close, it's
                 * unavailable after the close.
                 */
                dbName = db.getDebugName();

                if (!db.getDbImpl().isInternalDb()) {
                    nOpenUserDbs += 1;
                    errors.append("Unclosed Database: ");
                    errors.append(dbName).append("\n");
                }
                db.close();
            } catch (RuntimeException e) {
                if (!environmentImpl.isValid()) {
                    /* Propagate if env is invalidated. */
                    throw e;
                }
                errors.append("\nWhile closing Database ");
                errors.append(dbName);
                errors.append(" encountered exception: ");
                errors.append(LoggerUtils.getStackTrace(e)).append("\n");
            }
        }

        if (nOpenUserDbs > 0) {
            errors.append("Databases left open: ");
            errors.append(nOpenUserDbs).append("\n");
        }
    }

    /**
     * Opens, and optionally creates, a <code>Database</code>.
     *
     * @param txn For a transactional database, an explicit transaction may be
     * specified, or null may be specified to use auto-commit.  For a
     * non-transactional database, null must be specified.
     *
     * @param databaseName The name of the database.
     *
     * @param dbConfig The database attributes.  If null, default attributes
     * are used.
     *
     * @return Database handle.
     *
     * @throws DatabaseExistsException if the database already exists and the
     * {@code DatabaseConfig ExclusiveCreate} parameter is true.
     *
     * @throws DatabaseNotFoundException if the database does not exist and the
     * {@code DatabaseConfig AllowCreate} parameter is false.
     *
     * @throws OperationFailureException if one of the <a
     * href="../je/OperationFailureException.html#readFailures">Read Operation
     * Failures</a> occurs. If the database does not exist and the {@link
     * DatabaseConfig#setAllowCreate AllowCreate} parameter is true, then one
     * of the <a
     * href="../je/OperationFailureException.html#writeFailures">Write
     * Operation Failures</a> may also occur.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if this handle or the underlying
     * environment has been closed.
     *
     * @throws IllegalArgumentException if an invalid parameter is specified,
     * for example, an invalid {@code DatabaseConfig} property.
     *
     * @throws IllegalStateException if DatabaseConfig properties are changed
     * and there are other open handles for this database.
     */
    public synchronized Database openDatabase(Transaction txn,
                                              String databaseName,
                                              DatabaseConfig dbConfig)
        throws DatabaseNotFoundException,
               DatabaseExistsException,
               IllegalArgumentException,
               IllegalStateException {

        final EnvironmentImpl envImpl = checkOpen();

        if (dbConfig == null) {
            dbConfig = DatabaseConfig.DEFAULT;
        }

        try {
            final Database db = new Database(this);

            setupDatabase(
                envImpl, txn, db, databaseName, dbConfig,
                false  /*isInternalDb*/);

            return db;
        } catch (Error E) {
            envImpl.invalidate(E);
            throw E;
        }
    }

    /**
     * Opens and optionally creates a <code>SecondaryDatabase</code>.
     *
     * <p>Note that the associations between primary and secondary databases
     * are not stored persistently.  Whenever a primary database is opened for
     * write access by the application, the appropriate associated secondary
     * databases should also be opened by the application.  This is necessary
     * to ensure data integrity when changes are made to the primary
     * database.</p>
     *
     * @param txn For a transactional database, an explicit transaction may be
     * specified, or null may be specified to use auto-commit.  For a
     * non-transactional database, null must be specified.
     *
     * @param databaseName The name of the database.
     *
     * @param primaryDatabase the primary database with which the secondary
     * database will be associated.  The primary database must not be
     * configured for duplicates.
     *
     * @param dbConfig The secondary database attributes.  If null, default
     * attributes are used.
     *
     * @return Database handle.
     *
     * @throws DatabaseExistsException if the database already exists and the
     * {@code DatabaseConfig ExclusiveCreate} parameter is true.
     *
     * @throws DatabaseNotFoundException if the database does not exist and the
     * {@code DatabaseConfig AllowCreate} parameter is false.
     *
     * @throws OperationFailureException if one of the <a
     * href="../je/OperationFailureException.html#readFailures">Read Operation
     * Failures</a> occurs. If the database does not exist and the {@link
     * DatabaseConfig#setAllowCreate AllowCreate} parameter is true, then one
     * of the <a
     * href="../je/OperationFailureException.html#writeFailures">Write
     * Operation Failures</a> may also occur.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if this handle or the underlying
     * environment has been closed.
     *
     * @throws IllegalArgumentException if an invalid parameter is specified,
     * for example, an invalid {@code SecondaryConfig} property.
     *
     * @throws IllegalStateException if DatabaseConfig properties are changed
     * and there are other open handles for this database.
     */
    public synchronized SecondaryDatabase openSecondaryDatabase(
        Transaction txn,
        String databaseName,
        Database primaryDatabase,
        SecondaryConfig dbConfig)
        throws DatabaseNotFoundException,
               DatabaseExistsException,
               DatabaseException,
               IllegalArgumentException,
               IllegalStateException {

        final EnvironmentImpl envImpl = checkOpen();

        try {
            envImpl.getSecondaryAssociationLock().
                writeLock().lockInterruptibly();
        } catch (InterruptedException e) {
            throw new ThreadInterruptedException(envImpl, e);
        }
        try {
            if (dbConfig == null) {
                dbConfig = SecondaryConfig.DEFAULT;
            }
            final SecondaryDatabase db =
                new SecondaryDatabase(this, dbConfig, primaryDatabase);

            setupDatabase(
                envImpl, txn, db, databaseName, dbConfig,
                false  /*isInternalDb*/);

            return db;
        } catch (Error E) {
            envImpl.invalidate(E);
            throw E;
        } finally {
            envImpl.getSecondaryAssociationLock().writeLock().unlock();
        }
    }

    /**
     * The meat of open database processing.
     *
     * Currently, only external DBs are opened via this method, but we may
     * allow internal DB opens in the future.
     *
     * @param txn may be null
     * @param newDb is the Database handle which houses this database
     *
     * @throws IllegalArgumentException via openDatabase and
     * openSecondaryDatabase
     *
     * @see HandleLocker
     */
    private void setupDatabase(EnvironmentImpl envImpl,
                               Transaction txn,
                               Database newDb,
                               String databaseName,
                               DatabaseConfig dbConfig,
                               boolean isInternalDb)
        throws DatabaseNotFoundException, DatabaseExistsException {

        DatabaseUtil.checkForNullParam(databaseName, "databaseName");

        LoggerUtils.envLogMsg(Level.FINEST, envImpl,
                              "Environment.open: " + " name=" + databaseName +
                              " dbConfig=" + dbConfig);

        final boolean autoTxnIsReplicated =
            dbConfig.getReplicated() && envImpl.isReplicated();

        /*
         * Check that the open configuration is valid and doesn't conflict with
         * the envImpl configuration.
         */
        dbConfig.validateOnDbOpen(databaseName, autoTxnIsReplicated);

        validateDbConfigAgainstEnv(
            envImpl, dbConfig, databaseName, isInternalDb);

        /* Perform eviction before each operation that allocates memory. */
        envImpl.criticalEviction(false /*backgroundIO*/);

        DatabaseImpl database = null;
        boolean operationOk = false;
        HandleLocker handleLocker = null;
        final Locker locker = LockerFactory.getWritableLocker
            (this, txn, isInternalDb, dbConfig.getTransactional(),
             autoTxnIsReplicated, null);
        try {

            /*
             * Create the handle locker and lock the NameLN of an existing
             * database.  A read lock on the NameLN is acquired for both locker
             * and handleLocker.  Note: getDb may return a deleted database.
             */
            handleLocker = newDb.initHandleLocker(envImpl, locker);
            database = envImpl.getDbTree().getDb(locker, databaseName,
                                                 handleLocker, false);

            boolean dbCreated = false;
            final boolean databaseExists =
                (database != null) && !database.isDeleted();

            if (databaseExists) {
                if (dbConfig.getAllowCreate() &&
                    dbConfig.getExclusiveCreate()) {
                    throw new DatabaseExistsException
                        ("Database " + databaseName + " already exists");
                }

                newDb.initExisting(this, locker, database, databaseName,
                                   dbConfig);
            } else {
                /* Release deleted DB. [#13415] */
                envImpl.getDbTree().releaseDb(database);
                database = null;

                if (!isInternalDb &&
                    DbTree.isReservedDbName(databaseName)) {
                    throw new IllegalArgumentException
                        (databaseName + " is a reserved database name.");
                }

                if (!dbConfig.getAllowCreate()) {
                    throw new DatabaseNotFoundException("Database " +
                                                        databaseName +
                                                        " not found.");
                }

                /*
                 * Init a new DB. This calls DbTree.createDb and the new
                 * database is returned.  A write lock on the NameLN is
                 * acquired by locker and a read lock by the handleLocker.
                 */
                database = newDb.initNew(this, locker, databaseName, dbConfig);
                dbCreated = true;
            }

            /*
             * The open is successful.  We add the opened database handle to
             * this environment to track open handles in general, and to the
             * locker so that it can be invalidated by a user txn abort.
             */
            operationOk = true;
            addReferringHandle(newDb);
            locker.addOpenedDatabase(newDb);

            /* Run triggers before any subsequent auto commits. */
            final boolean firstWriteHandle =
                newDb.isWritable() &&
                (newDb.getDbImpl().noteWriteHandleOpen() == 1);

            if (dbCreated || firstWriteHandle) {
                TriggerManager.runOpenTriggers(locker, newDb, dbCreated);
            }
        } finally {

            /*
             * If the open fails, decrement the DB usage count, release
             * handle locks and remove references from other objects.  In other
             * cases this is done by Database.close() or invalidate(), the
             * latter in the case of a user txn abort.
             */
            if (!operationOk) {
                envImpl.getDbTree().releaseDb(database);
                if (handleLocker != null) {
                    handleLocker.operationEnd(false);
                }
                newDb.removeReferringAssociations();
            }

            /*
             * Tell the locker that this operation is over. Some types of
             * lockers (BasicLocker and auto Txn) will actually finish.
             */
            locker.operationEnd(operationOk);
        }
    }

    /**
     * @throws IllegalArgumentException via openDatabase and
     * openSecondaryDatabase
     */
    private void validateDbConfigAgainstEnv(EnvironmentImpl envImpl,
                                            DatabaseConfig dbConfig,
                                            String databaseName,
                                            boolean isInternalDb)
        throws IllegalArgumentException {

        /*
         * R/W database handles on a replicated database must be transactional,
         * for now. In the future we may support non-transactional database
         * handles.
         */
        if (envImpl.isReplicated() &&
            dbConfig.getReplicated() &&
            !dbConfig.getReadOnly()) {
            if (!dbConfig.getTransactional()) {
                throw new IllegalArgumentException
                ("Read/Write Database instances for replicated " +
                 "database " + databaseName + " must be transactional.");
            }
        }

        /* Check operation's transactional status against the Environment */
        if (!isInternalDb &&
            dbConfig.getTransactional() &&
            !(envImpl.isTransactional())) {
            throw new IllegalArgumentException
                ("Attempted to open Database " + databaseName +
                 " transactionally, but parent Environment is" +
                 " not transactional");
        }

        /* Check read/write status */
        if (envImpl.isReadOnly() && (!dbConfig.getReadOnly())) {
            throw new IllegalArgumentException
                ("Attempted to open Database " + databaseName +
                 " as writable but parent Environment is read only ");
        }
    }

    /**
     * Removes a database from the environment, discarding all records in the
     * database and removing the database name itself.
     *
     * <p>Compared to deleting all the records in a database individually,
     * {@code removeDatabase} is a very efficient operation.  Some internal
     * housekeeping information is updated, but the database records are not
     * read or written, and very little I/O is needed.</p>
     *
     * <p>When called on a database configured with secondary indices, the
     * application is responsible for also removing all associated secondary
     * indices.  To guarantee integrity, a primary database and all of its
     * secondary databases should be removed atomically using a single
     * transaction.</p>
     *
     * <p>Applications should not remove a database with open {@link Database
     * Database} handles.  If the database is open with the same transaction as
     * passed in the {@code txn} parameter, {@link IllegalStateException} is
     * thrown by this method.  If the database is open using a different
     * transaction, this method will block until all database handles are
     * closed, or until the conflict is resolved by throwing {@link
     * LockConflictException}.</p>
     *
     * @param txn For a transactional environment, an explicit transaction
     * may be specified or null may be specified to use auto-commit.  For a
     * non-transactional environment, null must be specified.
     *
     * @param databaseName The database to be removed.
     *
     * @throws DatabaseNotFoundException if the database does not exist.
     *
     * @throws OperationFailureException if one of the <a
     * href="../je/OperationFailureException.html#writeFailures">Write
     * Operation Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws UnsupportedOperationException if this is a read-only
     * environment.
     *
     * @throws IllegalStateException if the database is currently open using
     * the transaction passed in the {@code txn} parameter, or if this handle
     * or the underlying environment has been closed.
     *
     * @throws IllegalArgumentException if an invalid parameter is specified.
     */
    public void removeDatabase(final Transaction txn,
                               final String databaseName)
        throws DatabaseNotFoundException {

        DatabaseUtil.checkForNullParam(databaseName, "databaseName");

        new DbNameOperation<Void>(txn) {

            Pair<DatabaseImpl, Void> runWork(final Locker locker)
                throws DatabaseNotFoundException,
                       DbTree.NeedRepLockerException {

                final DatabaseImpl dbImpl =
                    dbTree.dbRemove(locker, databaseName, null /*checkId*/);

                return new Pair<>(dbImpl, null);
            }

            void runTriggers(final Locker locker, final DatabaseImpl dbImpl) {
                TriggerManager.runRemoveTriggers(locker, dbImpl);
            }
        }.run();
    }

    /**
     * Renames a database, without removing the records it contains.
     *
     * <p>Applications should not rename a database with open {@link Database
     * Database} handles.  If the database is open with the same transaction as
     * passed in the {@code txn} parameter, {@link IllegalStateException} is
     * thrown by this method.  If the database is open using a different
     * transaction, this method will block until all database handles are
     * closed, or until the conflict is resolved by throwing {@link
     * LockConflictException}.</p>
     *
     * @param txn For a transactional environment, an explicit transaction
     * may be specified or null may be specified to use auto-commit.  For a
     * non-transactional environment, null must be specified.
     *
     * @param databaseName The new name of the database.
     *
     * @throws DatabaseNotFoundException if the database does not exist.
     *
     * @throws OperationFailureException if one of the <a
     * href="../je/OperationFailureException.html#writeFailures">Write
     * Operation Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws UnsupportedOperationException if this is a read-only
     * environment.
     *
     * @throws IllegalStateException if the database is currently open using
     * the transaction passed in the {@code txn} parameter, or if this handle
     * or the underlying environment has been closed.
     *
     * @throws IllegalArgumentException if an invalid parameter is specified.
     */
    public void renameDatabase(final Transaction txn,
                               final String databaseName,
                               final String newName)
        throws DatabaseNotFoundException {

        DatabaseUtil.checkForNullParam(databaseName, "databaseName");
        DatabaseUtil.checkForNullParam(newName, "newName");

        new DbNameOperation<Void>(txn) {

            Pair<DatabaseImpl, Void> runWork(final Locker locker)
                throws DatabaseNotFoundException,
                       DbTree.NeedRepLockerException {

                final DatabaseImpl dbImpl =
                    dbTree.dbRename(locker, databaseName, newName);

                return new Pair<>(dbImpl, null);
            }

            void runTriggers(final Locker locker, final DatabaseImpl dbImpl) {
                TriggerManager.runRenameTriggers(locker, dbImpl, newName);
            }
        }.run();
    }

    /**
     * Empties the database, discarding all the records it contains, without
     * removing the database name.
     *
     * <p>Compared to deleting all the records in a database individually,
     * {@code truncateDatabase} is a very efficient operation.  Some internal
     * housekeeping information is updated, but the database records are not
     * read or written, and very little I/O is needed.</p>
     *
     * <p>When called on a database configured with secondary indices, the
     * application is responsible for also truncating all associated secondary
     * indices.  To guarantee integrity, a primary database and all of its
     * secondary databases should be truncated atomically using a single
     * transaction.</p>
     *
     * <p>Applications should not truncate a database with open {@link Database
     * Database} handles.  If the database is open with the same transaction as
     * passed in the {@code txn} parameter, {@link IllegalStateException} is
     * thrown by this method.  If the database is open using a different
     * transaction, this method will block until all database handles are
     * closed, or until the conflict is resolved by throwing {@link
     * LockConflictException}.</p>
     *
     * @param txn For a transactional environment, an explicit transaction may
     * be specified or null may be specified to use auto-commit.  For a
     * non-transactional environment, null must be specified.
     *
     * @param databaseName The database to be truncated.
     *
     * @param returnCount If true, count and return the number of records
     * discarded.
     *
     * @return The number of records discarded, or -1 if returnCount is false.
     *
     * @throws DatabaseNotFoundException if the database does not exist.
     *
     * @throws OperationFailureException if one of the <a
     * href="../je/OperationFailureException.html#writeFailures">Write
     * Operation Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws UnsupportedOperationException if this is a read-only
     * environment.
     *
     * @throws IllegalStateException if the database is currently open using
     * the transaction passed in the {@code txn} parameter, or if this handle
     * or the underlying environment has been closed.
     *
     * @throws IllegalArgumentException if an invalid parameter is specified.
     */
    public long truncateDatabase(final Transaction txn,
                                 final String databaseName,
                                 final boolean returnCount)
        throws DatabaseNotFoundException {

        DatabaseUtil.checkForNullParam(databaseName, "databaseName");

        return (new DbNameOperation<Long>(txn) {

            Pair<DatabaseImpl, Long> runWork(final Locker locker)
                throws DatabaseNotFoundException,
                       DbTree.NeedRepLockerException {

                final TruncateDbResult result =
                    dbTree.truncate(locker, databaseName, returnCount);

                return new Pair<>(result.newDb, result.recordCount);
            }

            void runTriggers(final Locker locker, final DatabaseImpl dbImpl) {
                TriggerManager.runTruncateTriggers(locker, dbImpl);
            }
        }).run();
    }

    /**
     * Runs a DB naming operation: remove, truncate or rename.  The common code
     * is factored out here. In particular this class handles non-replicated
     * DBs in a replicated environment, when auto-commit is used.
     * <p>
     * For a non-replicated DB, an auto-commit txn must be created by calling
     * LockerFactory.getWritableLocker with the autoTxnIsReplicated param set
     * to false.  If autoTxnIsReplicated is set to true in a replicated
     * environment, HA consistency checks will be made when the txn is begun
     * and acks will be enforced at commit.  For example, for an HA node in an
     * unknown state, the consistency checks would fail and prevent performing
     * the operation on the local/non-replicated DB.
     * <p>
     * Unfortunately, we need to create a txn/locker in order to query the DB
     * metadata, to determine whether it is replicated.  Therefore, we always
     * attempt the operation initially with autoTxnIsReplicated set to false.
     * The DbTree name operation methods (see DbTree.lockNameLN) will throw an
     * internal exception (NeedRepLockerException) if a non-replicated
     * auto-commit txn is used on a replicated DB.  That signals this class to
     * retry the operation with autoTxnIsReplicated set to true.
     * <p>
     * Via an unlikely series of DB renaming it is possible that on the 2nd try
     * with a replicated txn, we find that the DB is non-replicated.  However,
     * there is little harm in proceeding, since the consistency check is
     * already done.
     */
    private abstract class DbNameOperation<R> {

        private final EnvironmentImpl envImpl;
        private final Transaction txn;
        final DbTree dbTree;

        DbNameOperation(final Transaction txn) {
            this.txn = txn;
            this.envImpl = checkOpen();
            checkWritable(envImpl);

            dbTree = envImpl.getDbTree();
        }

        /** Run the DB name operation. */
        abstract Pair<DatabaseImpl, R> runWork(final Locker locker)
            throws DatabaseNotFoundException, DbTree.NeedRepLockerException;

        /** Run triggers after a successful DB name operation. */
        abstract void runTriggers(final Locker locker,
                                  final DatabaseImpl dbImpl);
        
        /**
         * Try the operation with autoTxnIsReplicated=false, and then again
         * with autoTxnIsReplicated=true if NeedRepLockerException is thrown.
         */
        R run() throws DatabaseNotFoundException {
            try {
                return runOnce(getWritableLocker(false));
            } catch (DbTree.NeedRepLockerException e) {
                try {
                    return runOnce(getWritableLocker(true));
                } catch (DbTree.NeedRepLockerException e2) {
                    /* Should never happen. */
                    throw EnvironmentFailureException.unexpectedException(
                        envImpl, e);
                }
            }
        }
        
        private R runOnce(final Locker locker)
            throws DatabaseNotFoundException, DbTree.NeedRepLockerException {

            boolean success = false;
            try {
                final Pair<DatabaseImpl, R> results = runWork(locker);
                final DatabaseImpl dbImpl = results.first();
                if (dbImpl == null) {
                    /* Should never happen. */
                    throw EnvironmentFailureException.unexpectedState(envImpl);
                }
                success = true;
                runTriggers(locker, dbImpl);
                return results.second();
            } catch (Error E) {
                envImpl.invalidate(E);
                throw E;
            } finally {
                locker.operationEnd(success);
            }
        }

        private Locker getWritableLocker(boolean autoTxnIsReplicated) {
            return LockerFactory.getWritableLocker(
                Environment.this, txn, false /*isInternalDb*/,
                envImpl.isTransactional(), autoTxnIsReplicated);
        }
    }

    /**
     * For unit testing.  Returns the current memory usage in bytes for all
     * btrees in the envImpl.
     */
    long getMemoryUsage()
        throws DatabaseException {

        final EnvironmentImpl envImpl = checkOpen();

        return envImpl.getMemoryBudget().getCacheMemoryUsage();
    }

    /**
     * Returns the database environment's home directory.
     *
     * This method may be called when the environment has been invalidated, but
     * not yet closed. In other words, {@link EnvironmentFailureException} is
     * never thrown by this method.
     *
     * @return The database environment's home directory.
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if this handle has been closed.
     */
    public File getHome()
        throws DatabaseException {

        final EnvironmentImpl envImpl = getNonNullEnvImpl();

        return envImpl.getEnvironmentHome();
    }

    /*
     * Transaction management
     */

    /**
     * Returns the default txn config for this environment handle.
     */
    TransactionConfig getDefaultTxnConfig() {
        return defaultTxnConfig;
    }

    /**
     * Copies the handle properties out of the config properties, and
     * initializes the default transaction config.
     */
    private void copyToHandleConfig(EnvironmentMutableConfig useConfig,
                                    EnvironmentConfig initStaticConfig,
                                    RepConfigProxy initRepConfig) {

        /*
         * Create the new objects, initialize them, then change the instance
         * fields. This avoids synchronization issues.
         */
        EnvironmentMutableConfig newHandleConfig =
            new EnvironmentMutableConfig();
        useConfig.copyHandlePropsTo(newHandleConfig);
        this.handleConfig = newHandleConfig;

        TransactionConfig newTxnConfig =
            TransactionConfig.DEFAULT.clone();
        newTxnConfig.setNoSync(handleConfig.getTxnNoSync());
        newTxnConfig.setWriteNoSync(handleConfig.getTxnWriteNoSync());
        newTxnConfig.setDurability(handleConfig.getDurability());

        if (initStaticConfig != null) {
            newTxnConfig.setSerializableIsolation
                (initStaticConfig.getTxnSerializableIsolation());
            newTxnConfig.setReadCommitted
                (initStaticConfig.getTxnReadCommitted());
        } else {
            newTxnConfig.setSerializableIsolation
                (defaultTxnConfig.getSerializableIsolation());
            newTxnConfig.setReadCommitted
                (defaultTxnConfig.getReadCommitted());
            newTxnConfig.setConsistencyPolicy
                (defaultTxnConfig.getConsistencyPolicy());
        }
        if (initRepConfig != null) {
            newTxnConfig.setConsistencyPolicy
                (initRepConfig.getConsistencyPolicy());
        }
        this.defaultTxnConfig = newTxnConfig;
    }

    /**
     * Creates a new transaction in the database environment.
     *
     * <p>Transaction handles are free-threaded; transactions handles may be
     * used concurrently by multiple threads.</p>
     *
     * <p>Cursors may not span transactions; that is, each cursor must be
     * opened and closed within a single transaction. The parent parameter is a
     * placeholder for nested transactions, and must currently be null.</p>
     *
     * @param txnConfig The transaction attributes.  If null, default
     * attributes are used.
     *
     * @return The newly created transaction's handle.
     *
     * @throws com.sleepycat.je.rep.InsufficientReplicasException if the Master
     * in a replicated environment could not contact a quorum of replicas as
     * determined by the {@link ReplicaAckPolicy}.
     *
     * @throws com.sleepycat.je.rep.ReplicaConsistencyException if a replica
     * in a replicated environment cannot become consistent within the timeout
     * period.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws UnsupportedOperationException if this is not a transactional
     * environment.
     *
     * @throws IllegalStateException if this handle or the underlying
     * environment has been closed.
     *
     * @throws IllegalArgumentException if an invalid parameter is specified,
     * for example, an invalid {@code TransactionConfig} parameter.
     */
    public Transaction beginTransaction(Transaction parent,
                                        TransactionConfig txnConfig)
        throws DatabaseException,
               IllegalArgumentException {

        try {
            return beginTransactionInternal(parent, txnConfig,
                                            false /*isInternalTxn*/);
        } catch (Error E) {
            invalidate(E);
            throw E;
        }
    }

    /**
     * Like beginTransaction, but does not require that the Environment is
     * transactional.
     */
    Transaction beginInternalTransaction(TransactionConfig txnConfig) {
        return beginTransactionInternal(null /*parent*/, txnConfig,
                                        true /*isInternalTxn*/);
    }

    /**
     * @throws IllegalArgumentException via beginTransaction.
     * @throws UnsupportedOperationException via beginTransaction.
     */
    private Transaction beginTransactionInternal(Transaction parent,
                                                 TransactionConfig txnConfig,
                                                 boolean isInternalTxn )
        throws DatabaseException {

        final EnvironmentImpl envImpl = checkOpen();

        if (parent != null) {
            throw new IllegalArgumentException
                ("Parent txn is non-null. " +
                 "Nested transactions are not supported.");
        }

        if (!isInternalTxn && !envImpl.isTransactional()) {
            throw new UnsupportedOperationException
                ("Transactions can not be used in a non-transactional " +
                 "environment");
        }

        checkTxnConfig(txnConfig);

        /*
         * Apply txn config defaults.  We don't need to clone unless we have to
         * apply the env default, since we don't hold onto a txn config
         * reference.
         */
        TransactionConfig useConfig = null;
        if (txnConfig == null) {
            useConfig = defaultTxnConfig;
        } else {
            if (defaultTxnConfig.getNoSync() ||
                defaultTxnConfig.getWriteNoSync()) {

                /*
                 * The environment sync settings have been set, check if any
                 * were set in the user's txn config. If none were set in the
                 * user's config, apply the environment defaults
                 */
                if (!txnConfig.getNoSync() &&
                    !txnConfig.getSync() &&
                    !txnConfig.getWriteNoSync()) {
                    useConfig = txnConfig.clone();
                    if (defaultTxnConfig.getWriteNoSync()) {
                        useConfig.setWriteNoSync(true);
                    } else {
                        useConfig.setNoSync(true);
                    }
                }
            }

            if ((defaultTxnConfig.getDurability() != null) &&
                 (txnConfig.getDurability() == null)) {

                /*
                 * Inherit transaction durability from the environment in the
                 * absence of an explicit transaction config durability.
                 */
                if (useConfig == null) {
                    useConfig = txnConfig.clone();
                }
                useConfig.setDurability(defaultTxnConfig.getDurability());
            }

            if ((defaultTxnConfig.getConsistencyPolicy() != null) &&
                (txnConfig.getConsistencyPolicy() == null)) {
                   if (useConfig == null) {
                       useConfig = txnConfig.clone();
                   }
                   useConfig.setConsistencyPolicy
                       (defaultTxnConfig.getConsistencyPolicy());
            }

            /* Apply isolation level default. */
            if (!txnConfig.getSerializableIsolation() &&
                !txnConfig.getReadCommitted() &&
                !txnConfig.getReadUncommitted()) {
                if (defaultTxnConfig.getSerializableIsolation()) {
                    if (useConfig == null) {
                        useConfig = txnConfig.clone();
                    }
                    useConfig.setSerializableIsolation(true);
                } else if (defaultTxnConfig.getReadCommitted()) {
                    if (useConfig == null) {
                        useConfig = txnConfig.clone();
                    }
                    useConfig.setReadCommitted(true);
                }
            }

            /* No environment level defaults applied. */
            if (useConfig == null) {
                useConfig = txnConfig;
            }
        }
        Txn internalTxn = envImpl.txnBegin(parent, useConfig);
        Transaction txn = new Transaction(this, internalTxn);
        addReferringHandle(txn);
        return txn;
    }

    /**
     * Checks the txnConfig object to ensure that its correctly configured and
     * is compatible with the configuration of the Environment.
     *
     * @param txnConfig the configuration being checked.
     *
     * @throws IllegalArgumentException via beginTransaction
     */
    private void checkTxnConfig(TransactionConfig txnConfig)
        throws IllegalArgumentException {

        if (txnConfig == null) {
            return;
        }
        if ((txnConfig.getSerializableIsolation() &&
             txnConfig.getReadUncommitted()) ||
            (txnConfig.getSerializableIsolation() &&
             txnConfig.getReadCommitted()) ||
            (txnConfig.getReadUncommitted() &&
             txnConfig.getReadCommitted())) {
            throw new IllegalArgumentException
                ("Only one may be specified: SerializableIsolation, " +
                "ReadCommitted or ReadUncommitted");
        }
        if ((txnConfig.getDurability() != null) &&
            ((defaultTxnConfig.getSync() ||
              defaultTxnConfig.getNoSync() ||
              defaultTxnConfig.getWriteNoSync()))) {
           throw new IllegalArgumentException
               ("Mixed use of deprecated durability API for the " +
                "Environment with the new durability API for " +
                "TransactionConfig.setDurability()");
        }
        if ((defaultTxnConfig.getDurability() != null) &&
            ((txnConfig.getSync() ||
              txnConfig.getNoSync() ||
              txnConfig.getWriteNoSync()))) {
            throw new IllegalArgumentException
                   ("Mixed use of new durability API for the " +
                    "Environment with the deprecated durability API for " +
                    "TransactionConfig.");
        }
    }

    /**
     * Synchronously checkpoint the database environment.
     * <p>
     * This is an optional action for the application since this activity
     * is, by default, handled by a database environment owned background
     * thread.
     * <p>
     * A checkpoint has the side effect of flushing all preceding
     * non-transactional write operations, as well as any preceding
     * transactions that were committed with {@link
     * Durability.SyncPolicy#NO_SYNC no-sync durability}.  However, for best
     * performance, checkpoints should be used only to bound recovery time.
     * {@link #flushLog} can be used to write buffered data for durability
     * purposes.
     *
     * @param ckptConfig The checkpoint attributes.  If null, default
     * attributes are used.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws DiskLimitException if the checkpoint cannot be performed
     * because a disk limit has been violated.
     *
     * @throws IllegalStateException if this handle or the underlying
     * environment has been closed.
     */
    public void checkpoint(CheckpointConfig ckptConfig)
        throws DatabaseException {

        final EnvironmentImpl envImpl = checkOpen();

        if (ckptConfig == null) {
            ckptConfig = CheckpointConfig.DEFAULT;
        }

        try {
            envImpl.invokeCheckpoint(ckptConfig, "api");
        } catch (Error E) {
            envImpl.invalidate(E);
            throw E;
        }
    }

    /**
     * Synchronously flushes database environment databases to stable storage.
     * Calling this method is equivalent to forcing a checkpoint and setting
     * {@link CheckpointConfig#setMinimizeRecoveryTime} to true.
     * <p>
     * A checkpoint has the side effect of flushing all preceding
     * non-transactional write operations, as well as any preceding
     * transactions that were committed with {@link
     * Durability.SyncPolicy#NO_SYNC no-sync durability}.  However, for best
     * performance, checkpoints should be used only to bound recovery time.
     * {@link #flushLog} can be used to write buffered data for durability
     * purposes.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws DiskLimitException if the sync cannot be performed
     * because a disk limit has been violated.
     *
     * @throws IllegalStateException if this handle or the underlying
     * environment has been closed.
     */
    public void sync()
        throws DatabaseException {

        final EnvironmentImpl envImpl = checkOpen();

        try {
            final CheckpointConfig config = new CheckpointConfig();
            config.setForce(true);
            config.setMinimizeRecoveryTime(true);

            envImpl.invokeCheckpoint(config, "sync");
        } catch (Error E) {
            envImpl.invalidate(E);
            throw E;
        }
    }

    /**
     * Writes buffered data to the log, and optionally performs an fsync to
     * guarantee that data is written to the physical device.
     * <p>
     * This method is used to make durable, by writing to the log, all
     * preceding non-transactional write operations, as well as any preceding
     * transactions that were committed with {@link
     * Durability.SyncPolicy#NO_SYNC no-sync durability}.  If the {@code fsync}
     * parameter is true, it can also be used to flush all logged data to the
     * physical storage device, by performing an fsync.
     * <p>
     * Note that this method <em>does not</em> flush previously unwritten data
     * in deferred-write databases; that is done by calling {@link
     * Database#sync} or performing a checkpoint.
     *
     * @param fsync is true to perform an fsync as well as a file write, or
     * false to perform only a file write.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if this handle or the underlying
     * environment has been closed.
     */
    public void flushLog(boolean fsync) {

        final EnvironmentImpl envImpl = checkOpen();

        try {
            envImpl.flushLog(fsync);
        } catch (Error E) {
            envImpl.invalidate(E);
            throw E;
        }
    }

    /**
     * Synchronously invokes log file (data file) cleaning until the target
     * disk space utilization has been reached; this method is called
     * periodically by the cleaner background threads.
     *
     * <p>Zero or more log files will be cleaned as necessary to bring the
     * current {@link EnvironmentStats#getCurrentMinUtilization disk space
     * utilization} of the environment above the configured {@link
     * EnvironmentConfig#CLEANER_MIN_UTILIZATION utilization threshold}.
     *
     * <p>Note that this method does not perform the complete task of cleaning
     * a log file. Eviction and checkpointing log Btree information that is
     * marked dirty by the cleaner, and a full checkpoint is necessary,
     * following cleaning, before cleaned files will be deleted (or renamed).
     * Checkpoints occur periodically and when the environment is closed.</p>
     *
     * <p>This is an optional action for the application since this activity
     * is, by default, handled by one or more Environment-owned background
     * threads.</p>
     *
     * <p>The intended use case for the {@code cleanLog} method is when the
     * application wishes to disable the built-in cleaner threads using the
     * {@link EnvironmentConfig#ENV_RUN_CLEANER} property. To replace the
     * functionality of the cleaner threads, the application should call
     * {@code cleanLog} periodically.</p>
     *
     * <p>Note that because this method cleans multiple files before returning,
     * in an attempt to reach the target utilization, it may not return for a
     * long time when there is a large {@link
     * EnvironmentStats#getCleanerBacklog backlog} of files to be cleaned. This
     * method cannot be aborted except by closing the environment. If the
     * application needs the ability to abort the cleaning process, the
     * {@link #cleanLogFile} method should be used instead.</p>
     *
     * <p>Note that in certain unusual situations the cleaner may not be able
     * to make forward progress and the target utilization will never be
     * reached. For example, this can occur if the target utilization is set
     * too high or checkpoints are performed too often. To guard against
     * cleaning "forever", this method will return when all files have been
     * cleaned, even when the target utilization has not been reached.</p>
     *
     * @return The number of log files that were cleaned, and that will be
     * deleted (or renamed) when a qualifying checkpoint occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws UnsupportedOperationException if this is a read-only or
     * memory-only environment.
     *
     * @throws IllegalStateException if this handle or the underlying
     * environment has been closed.
     */
    public int cleanLog()
        throws DatabaseException {

        final EnvironmentImpl envImpl = checkOpen();

        try {
            return envImpl.invokeCleaner(true /*cleanMultipleFiles*/);
        } catch (Error E) {
            envImpl.invalidate(E);
            throw E;
        }
    }

    /**
     * Synchronously invokes cleaning of a single log file (data file), if
     * the target disk space utilization has not been reached.
     *
     * <p>One log file will be cleaned if the current {@link
     * EnvironmentStats#getCurrentMinUtilization disk space utilization} of the
     * environment is below the configured {@link
     * EnvironmentConfig#CLEANER_MIN_UTILIZATION utilization threshold}. No
     * files will be cleaned if disk space utilization is currently above the
     * threshold. The lowest utilized file is selected for cleaning, since it
     * has the lowest cleaning cost.</p>
     *
     * <p>Note that this method does not perform the complete task of cleaning
     * a log file. Eviction and checkpointing log Btree information that is
     * marked dirty by the cleaner, and a full checkpoint is necessary,
     * following cleaning, before cleaned files will be deleted (or renamed).
     * Checkpoints occur periodically and when the environment is closed.</p>
     *
     * <p>The intended use case for the {@code cleanLog} method is "batch
     * cleaning". This is when the application disables the cleaner threads
     * (using the {@link EnvironmentConfig#ENV_RUN_CLEANER} property)
     * for maximum performance during active periods, and calls {@code
     * cleanLog} during periods when the application is quiescent or less
     * active than usual. Similarly, there may be times when an application
     * wishes to perform cleaning explicitly until the target utilization
     * rather than relying on the cleaner's background threads. For example,
     * some applications may wish to perform batch cleaning prior to closing
     * the environment, to reclaim as much disk space as possible at that
     * time.</p>
     *
     * <p>To clean until the target utilization threshold is reached, {@code
     * cleanLogFile} can be called in a loop until it returns {@code false}.
     * When there is a large {@link EnvironmentStats#getCleanerBacklog
     * backlog} of files to be cleaned, the application may wish to limit the
     * amount of cleaning. Batch cleaning can be aborted simply by breaking out
     * of the loop. The cleaning of a single file is not a long operation; it
     * should take several minutes at most. For example:</p>
     *
     * <pre>
     *     boolean cleaningAborted;
     *     boolean anyCleaned = false;
     *
     *     while (!cleaningAborted && env.cleanLogFile()) {
     *         anyCleaned = true;
     *     }
     * </pre>
     *
     * <p>Note that in certain unusual situations the cleaner may not be able
     * to make forward progress and the target utilization will never be
     * reached. For example, this can occur if the target utilization is set
     * too high or checkpoints are performed too often. To guard against
     * cleaning "forever", the application may wish to cancel the batch
     * cleaning (break out of the loop) when the cleaning time or number of
     * files cleaned exceeds some reasonable limit.</p>
     *
     * <p>As mentioned above, the cleaned log files will not be deleted until
     * the next full checkpoint. If the application wishes to reclaim this disk
     * space as soon as possible, an explicit checkpoint may be performed after
     * the batch cleaning operation. For example:</p>
     *
     * <pre>
     *     if (anyCleaned) {
     *         env.checkpoint(new CheckpointConfig().setForce(true));
     *     }
     * </pre>
     *
     * <p>However, even an explicit checkpoint is not guaranteed to delete the
     * cleaned log files if, at the time the file was cleaned, records in the
     * file were locked or were part of a database that was being removed, due
     * to concurrent application activity that was accessing records or
     * removing databases. In this case the files will be deleted only after
     * these operations are complete and a subsequent checkpoint is performed.
     * To guarantee that the cleaned files will be deleted, an application may
     * stop all concurrent activity (ensure all operations and transactions
     * have ended) and then perform a checkpoint.</p>
     *
     * <p>When closing the environment and minimizing recovery time is desired
     * (see {@link #close}), as well as reclaiming disk space, the recommended
     * procedure is as follows:</p>

     * <pre>
     *     // Stop/finish all application operations that are using JE.
     *     ...
     *
     *     // Stop the cleaner daemon threads.
     *     EnvironmentMutableConfig config = env.getMutableConfig();
     *     config.setConfigParam(EnvironmentConfig.ENV_RUN_CLEANER, "false");
     *     env.setMutableConfig(config);
     *
     *     // Perform batch cleaning.
     *     while (!cleaningAborted && env.cleanLogFile()) {
     *     }
     *
     *     // Perform an extra checkpoint
     *     env.checkpoint(new CheckpointConfig().setForce(true));
     *
     *     // Finally, close the environment.
     *     env.close();
     * </pre>
     *
     * @return true if one log was cleaned, or false if none were cleaned.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws UnsupportedOperationException if this is a read-only or
     * memory-only environment.
     *
     * @throws IllegalStateException if this handle or the underlying
     * environment has been closed.
     */
    public boolean cleanLogFile()
        throws DatabaseException {

        final EnvironmentImpl envImpl = checkOpen();

        try {
            return envImpl.invokeCleaner(false /*cleanMultipleFiles*/) > 0;
        } catch (Error E) {
            envImpl.invalidate(E);
            throw E;
        }
    }

    /**
     * Synchronously invokes the mechanism for keeping memory usage within the
     * cache size boundaries.
     *
     * <p>This is an optional action for the application since this activity
     * is, by default, handled by a database environment owned background
     * thread.</p>
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if this handle or the underlying
     * environment has been closed.
     */
    public void evictMemory()
        throws DatabaseException {

        final EnvironmentImpl envImpl = checkOpen();

        try {
            envImpl.invokeEvictor();
        } catch (Error E) {
            envImpl.invalidate(E);
            throw E;
        }
    }

    /**
     * Synchronously invokes the compressor mechanism which compacts in memory
     * data structures after delete operations.
     *
     * <p>This is an optional action for the application since this activity
     * is, by default, handled by a database environment owned background
     * thread.</p>
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if this handle or the underlying
     * environment has been closed.
     */
    public void compress()
        throws DatabaseException {

        final EnvironmentImpl envImpl = checkOpen();

        try {
            envImpl.invokeCompressor();
        } catch (Error E) {
            envImpl.invalidate(E);
            throw E;
        }
    }

    /**
     * Preloads the cache with multiple databases.  This method should only be
     * called when there are no operations being performed on the specified
     * databases in other threads.  Executing preload during concurrent updates
     * of the specified databases may result in some or all of the tree being
     * loaded into the JE cache.  Executing preload during any other types of
     * operations may result in JE exceeding its allocated cache
     * size. preload() effectively locks all of the specified database and
     * therefore will lock out the checkpointer, cleaner, and compressor, as
     * well as not allow eviction to occur.  If databases are replicated and
     * the environment is in the replica state, then the replica may become
     * temporarily disconnected from the master if the replica needs to replay
     * changes against the database and is locked out because the time taken by
     * the preload operation exceeds {@link
     * com.sleepycat.je.rep.ReplicationConfig#FEEDER_TIMEOUT}.
     *
     * @param config The PreloadConfig object that specifies the parameters
     * of the preload.
     *
     * @return A PreloadStats object with the result of the preload operation
     * and various statistics about the preload() operation.
     *
     * @throws OperationFailureException if one of the <a
     * href="OperationFailureException.html#readFailures">Read Operation
     * Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if any of the databases has been closed.
     *
     * @see Database#preload(PreloadConfig)
     */
    public PreloadStats preload(final Database[] databases,
                                PreloadConfig config)
        throws DatabaseException {

        final EnvironmentImpl envImpl = checkOpen();

        DatabaseUtil.checkForZeroLengthArrayParam(databases, "databases");

        if (config == null) {
            config = new PreloadConfig();
        }

        try {
            final int nDbs = databases.length;
            final DatabaseImpl[] dbImpls = new DatabaseImpl[nDbs];
            for (int i = 0; i < nDbs; i += 1) {
                dbImpls[i] = DbInternal.getDbImpl(databases[i]);
            }
            return envImpl.preload(dbImpls, config);
        } catch (Error E) {
            envImpl.invalidate(E);
            throw E;
        }
    }

    /**
     *
     * Create a DiskOrderedCursor to iterate over the records of a given set
     * of databases. Because the retrieval is based on Log Sequence Number
     * (LSN) order rather than key order, records are returned in unsorted
     * order in exchange for generally faster retrieval. LSN order
     * approximates disk sector order.
     * <p>
     * See {@link DiskOrderedCursor} for more details and a description of the
     * consistency guarantees provided by the scan.
     * <p>
     * <em>WARNING:</em> After calling this method, deletion of log files by
     * the JE log cleaner will be disabled until {@link
     * DiskOrderedCursor#close()} is called.  To prevent unbounded growth of
     * disk usage, be sure to call {@link DiskOrderedCursor#close()} to
     * re-enable log file deletion.
     *
     * @param databases An array containing the handles to the database that
     * are to be scanned. All these handles must be currently open.
     * Furthermore, all the databases must belong to this environments, and
     * they should all support duplicates or none of them should support
     * duplicates. Note: this method does not make a copy of this array,
     * and as a result, the contents of the array should not be modified
     * while the returned DiskOrderedCursor is still in use.
     *
     * @param config The DiskOrderedCursorConfig object that specifies the
     * parameters of the disk ordered scan.
     *
     * @return the new DiskOrderedCursor object.
     *
     * @throws IllegalArgumentException if (a) the databases parameter is
     * null or an empty array, or (b) any of the handles in the databases
     * parameter is null, or (c) the databases do not all belong to this
     * environment, or (d) some databases support duplicates and some don't.
     *
     * @throws IllegalStateException if any of the databases has been
     * closed or invalidated.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     */
    public DiskOrderedCursor openDiskOrderedCursor(
        final Database[] databases,
        DiskOrderedCursorConfig config)
        throws DatabaseException {

        final EnvironmentImpl envImpl = checkOpen();

        DatabaseUtil.checkForZeroLengthArrayParam(databases, "databases");

        if (config == null) {
            config = DiskOrderedCursorConfig.DEFAULT;
        }

        try {
            int nDbs = databases.length;

            for (int i = 0; i < nDbs; i += 1) {

                if (databases[i] == null) {
                    throw new IllegalArgumentException(
                        "The handle at position " + i + " of the databases " +
                        "array is null.");
                }

                if (databases[i].getEnvironment() != this) {
                    throw new IllegalArgumentException(
                        "The handle at position " + i + " of the databases " +
                        "array points to a database that does not belong " +
                        "to this environment");
                }
            }

            return new DiskOrderedCursor(databases, config);

        } catch (Error E) {
            envImpl.invalidate(E);
            throw E;
        }
    }


    /**
     * Returns this object's configuration.
     *
     * @return This object's configuration.
     *
     * <p>Unlike most Environment methods, this method may be called if the
     * environment is invalid, but not yet closed.</p>
     *
     * @throws IllegalStateException if this handle has been closed.
     */
    public EnvironmentConfig getConfig()
        throws DatabaseException {

        final EnvironmentImpl envImpl = getNonNullEnvImpl();

        try {
            final EnvironmentConfig config = envImpl.cloneConfig();
            handleConfig.copyHandlePropsTo(config);
            config.fillInEnvironmentGeneratedProps(envImpl);
            return config;
        } catch (Error E) {
            envImpl.invalidate(E);
            throw E;
        }
    }

    /**
     * Sets database environment attributes.
     *
     * <p>Attributes only apply to a specific Environment object and are not
     * necessarily shared by other Environment objects accessing this
     * database environment.</p>
     *
     * <p>Unlike most Environment methods, this method may be called if the
     * environment is invalid, but not yet closed.</p>
     *
     * @param mutableConfig The database environment attributes.  If null,
     * default attributes are used.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if this handle has been closed.
     */
    public synchronized void setMutableConfig(
        EnvironmentMutableConfig mutableConfig)
        throws DatabaseException {

        final EnvironmentImpl envImpl = checkOpen();

        DatabaseUtil.checkForNullParam(mutableConfig, "mutableConfig");

        /*
         * This method is synchronized so that we atomically call both
         * EnvironmentImpl.setMutableConfig and copyToHandleConfig. This
         * ensures that the handle and the EnvironmentImpl properties match.
         */
        try {

            /*
             * Change the mutable properties specified in the given
             * configuration.
             */
            envImpl.setMutableConfig(mutableConfig);

            /* Reset the handle config properties. */
            copyToHandleConfig(mutableConfig, null, null);
        } catch (Error E) {
            envImpl.invalidate(E);
            throw E;
        }
    }

    /**
     * Returns database environment attributes.
     *
     * <p>Unlike most Environment methods, this method may be called if the
     * environment is invalid, but not yet closed.</p>
     *
     * @return Environment attributes.
     *
     * @throws IllegalStateException if this handle has been closed.
     */
    public EnvironmentMutableConfig getMutableConfig()
        throws DatabaseException {

        final EnvironmentImpl envImpl = getNonNullEnvImpl();

        try {
            final EnvironmentMutableConfig config =
                envImpl.cloneMutableConfig();
            handleConfig.copyHandlePropsTo(config);
            config.fillInEnvironmentGeneratedProps(envImpl);
            return config;
        } catch (Error E) {
            envImpl.invalidate(E);
            throw E;
        }
    }

    /**
     * Returns the general database environment statistics.
     *
     * @param config The general statistics attributes.  If null, default
     * attributes are used.
     *
     * @return The general database environment statistics.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if this handle or the underlying
     * environment has been closed.
     */
    public EnvironmentStats getStats(StatsConfig config)
        throws DatabaseException {

        final EnvironmentImpl envImpl = checkOpen();

        if (config == null) {
            config = StatsConfig.DEFAULT;
        }

        try {
            return envImpl.loadStats(config);
        } catch (Error E) {
            envImpl.invalidate(E);
            throw E;
        }
    }

    /**
     * Returns the database environment's locking statistics.
     *
     * @param config The locking statistics attributes.  If null, default
     * attributes are used.
     *
     * @return The database environment's locking statistics.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if this handle or the underlying
     * environment has been closed.
     *
     * @deprecated as of 4.0.10, replaced by {@link
     * Environment#getStats(StatsConfig)}.</p>
     */
    public LockStats getLockStats(StatsConfig config)
        throws DatabaseException {

        final EnvironmentImpl envImpl = checkOpen();

        if (config == null) {
            config = StatsConfig.DEFAULT;
        }

        try {
            return envImpl.lockStat(config);
        } catch (Error E) {
            envImpl.invalidate(E);
            throw E;
        }
    }

    /**
     * Returns the database environment's transactional statistics.
     *
     * @param config The transactional statistics attributes.  If null,
     * default attributes are used.
     *
     * @return The database environment's transactional statistics.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if this handle or the underlying
     * environment has been closed.
     */
    public TransactionStats getTransactionStats(StatsConfig config)
        throws DatabaseException {

        final EnvironmentImpl envImpl = checkOpen();

        if (config == null) {
            config = StatsConfig.DEFAULT;
        }

        try {
            return envImpl.txnStat(config);
        } catch (Error E) {
            envImpl.invalidate(E);
            throw E;
        }
    }

    /**
     * Returns a List of database names for the database environment.
     *
     * <p>Each element in the list is a String.</p>
     *
     * @return A List of database names for the database environment.
     *
     * @throws OperationFailureException if one of the <a
     * href="OperationFailureException.html#readFailures">Read Operation
     * Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if this handle or the underlying
     * environment has been closed.
     */
    public List<String> getDatabaseNames()
        throws DatabaseException {

        final EnvironmentImpl envImpl = checkOpen();

        try {
            return envImpl.getDbTree().getDbNames();
        } catch (Error E) {
            envImpl.invalidate(E);
            throw E;
        }
    }

    /**
     * Returns if the database environment is consistent and correct.
     *
     * <p>Verification is an expensive operation that should normally only be
     * used for troubleshooting and debugging.</p>
     *
     * @param config The verification attributes.  If null, default
     * attributes are used.
     *
     * @param out is unused. To specify the output stream for verification
     * information, use {@link VerifyConfig#setShowProgressStream}.
     *
     * @return true if the database environment is consistent and correct.
     * Currently true is always returned when this method returns normally,
     * i.e., when no exception is thrown.
     *
     * @throws EnvironmentFailureException if a corruption is detected, or if
     * an unexpected, internal or environment-wide failure occurs. If a
     * persistent corruption is detected,
     * {@link EnvironmentFailureException#isCorrupted()} will return true.
     *
     * @throws IllegalStateException if this handle or the underlying
     * environment has been closed.
     */
    public boolean verify(VerifyConfig config,
                          @SuppressWarnings("unused") PrintStream out)
        throws DatabaseException {

        final EnvironmentImpl envImpl = checkOpen();

        if (config == null) {
            config = VerifyConfig.DEFAULT;
        }

        try {
            envImpl.verify(config);
        } catch (Error E) {
            envImpl.invalidate(E);
            throw E;
        }

        return true;
    }

    /**
     * Returns the transaction associated with this thread if implied
     * transactions are being used.  Implied transactions are used in an XA or
     * JCA "Local Transaction" environment.  In an XA environment the
     * XAEnvironment.start() entrypoint causes a transaction to be created and
     * become associated with the calling thread.  Subsequent API calls
     * implicitly use that transaction.  XAEnvironment.end() causes the
     * transaction to be disassociated with the thread.  In a JCA Local
     * Transaction environment, the call to JEConnectionFactory.getConnection()
     * causes a new transaction to be created and associated with the calling
     * thread.
     *
     * @throws IllegalStateException if this handle or the underlying
     * environment has been closed.
     */
    public Transaction getThreadTransaction()
        throws DatabaseException {

        final EnvironmentImpl envImpl = checkOpen();

        try {
            return envImpl.getTxnManager().getTxnForThread();
        } catch (Error E) {
            envImpl.invalidate(E);
            throw E;
        }
    }

    /**
     * Sets the transaction associated with this thread if implied transactions
     * are being used.  Implied transactions are used in an XA or JCA "Local
     * Transaction" environment.  In an XA environment the
     * XAEnvironment.start() entrypoint causes a transaction to be created and
     * become associated with the calling thread.  Subsequent API calls
     * implicitly use that transaction.  XAEnvironment.end() causes the
     * transaction to be disassociated with the thread.  In a JCA Local
     * Transaction environment, the call to JEConnectionFactory.getConnection()
     * causes a new transaction to be created and associated with the calling
     * thread.
     *
     * @throws IllegalStateException if this handle or the underlying
     * environment has been closed.
     */
    public void setThreadTransaction(Transaction txn) {

        final EnvironmentImpl envImpl = checkOpen();

        try {
            envImpl.getTxnManager().setTxnForThread(txn);
        } catch (Error E) {
            envImpl.invalidate(E);
            throw E;
        }
    }

    /**
     * Returns whether this {@code Environment} is open, valid and can be used.
     *
     * <p>When an {@link EnvironmentFailureException}, or one of its
     * subclasses, is caught, the {@code isValid} method can be called to
     * determine whether the {@code Environment} can continue to be used, or
     * should be closed. Some EnvironmentFailureExceptions invalidate the
     * environment and others do not.</p>
     *
     * <p>If this method returns false, the environment may have been closed by
     * the application, or may have been invalidated by an exception and not
     * yet closed. The {@link #isClosed()} method may be used to distinguish
     * between these two cases, and {@link #getInvalidatingException()} can be
     * used to return the exception. Note that it is safe to call {@link
     * #close} redundantly, so it is safe to always call {@link #close} when
     * this method returns false.</p>
     */
    public boolean isValid() {
        final EnvironmentImpl envImpl = environmentImpl;
        return envImpl != null && envImpl.isValid();
    }

    /**
     * Returns whether the environment has been closed by the application.
     *
     * <p>If this method returns true, {@link #close()}} has been called. If
     * the environment was previously invalidated by an exception, it will be
     * returned by {@link #getInvalidatingException()}.</p>
     *
     * <p>If this method returns false, the environment may or may not be
     * usable, since it may have been invalidated by an exception but not yet
     * closed. To determine whether it was invalidated, call {@link #isValid()}
     * or {@link #getInvalidatingException()}.</p>
     *
     * @return whether the environment has been closed by the application.
     *
     * @since 7.2
     */
    public boolean isClosed() {
        final EnvironmentImpl envImpl = environmentImpl;
        return envImpl == null || envImpl.isClosed();
    }

    /**
     * Returns the exception that caused the environment to be invalidated, or
     * null if the environment was not invalidated by an exception.
     *
     * <p>This method may be used to determine whether the environment was
     * invalidated by an exception, by checking for a non-null return value.
     * This method will return the invalidating exception, regardless of
     * whether the environment is closed. Note that {@link #isValid()} will
     * return false when the environment is closed, even when it was not
     * invalidated by an exception.</p>
     *
     * <p>This method may also be used to identify and handle the original
     * invalidating exception, when more than one exception is thrown. When an
     * environment is first invalidated by an EnvironmentFailureException, the
     * exception is saved so that it can be returned by this method. Other
     * EnvironmentFailureExceptions may be thrown later as side effects of the
     * original problem, or possibly as separate problems. It is normally the
     * first invalidating exception that is most relevant.</p>
     *
     * @return the invalidating exception or null.
     *
     * @since 7.2
     */
    public EnvironmentFailureException getInvalidatingException() {
        assert invalidatingEFE != null;
        return invalidatingEFE.get();
    }

    /**
     * Print a detailed report about the costs of different phases of
     * environment startup. This report is by default logged to the je.info
     * file if startup takes longer than je.env.startupThreshold.
     *
     * <p>Unlike most Environment methods, this method may be called if the
     * environment is invalid, but not yet closed.</p>
     *
     * @throws IllegalStateException if this handle or the underlying
     * environment has been closed.
     */
    public void printStartupInfo(PrintStream out) {
        final EnvironmentImpl envImpl = getNonNullEnvImpl();
        envImpl.getStartupTracker().displayStats(out, Phase.TOTAL_ENV_OPEN);
    }

    /*
     * Non public api -- helpers
     */

    /**
     * Let the Environment remember what's opened against it.
     */
    private void addReferringHandle(Database db) {
        referringDbs.put(db, db);
    }

    /**
     * Lets the Environment remember what's opened against it.
     */
    private void addReferringHandle(Transaction txn) {
        referringDbTxns.put(txn, txn);
    }

    /**
     * The referring db has been closed.
     */
    void removeReferringHandle(Database db) {
        referringDbs.remove(db);
    }

    /**
     * The referring Transaction has been closed.
     */
    void removeReferringHandle(Transaction txn) {
        referringDbTxns.remove(txn);
    }

    /**
     * @throws EnvironmentFailureException if the underlying environment is
     * invalid.
     * @throws IllegalStateException if the environment is not open.
     */
    EnvironmentImpl checkOpen() {
        final EnvironmentImpl envImpl = getNonNullEnvImpl();
        envImpl.checkOpen();
        return envImpl;
    }

    /**
     * Returns the non-null, underlying EnvironmentImpl.
     *
     * This method is called to access the environmentImpl field, to guard
     * against NPE when the environment has been closed.
     *
     * This method does not check whether the env is valid. For API method
     * calls, checkOpen is called at API entry points to check validity. The
     * validity of the env should also be checked before critical operations
     * (e.g., disk writes), after idle periods, and periodically during time
     * consuming operations.
     *
     * @throws IllegalStateException if the env has been closed.
     */
    EnvironmentImpl getNonNullEnvImpl() {

        final EnvironmentImpl envImpl = environmentImpl;

        if (envImpl == null) {
            throw new IllegalStateException("Environment is closed.");
        }

        return envImpl;
    }

    /**
     * Returns the underlying EnvironmentImpl, or null if the env has been
     * closed.
     *
     * WARNING: This method will be phased out over time and normally
     * getNonNullEnvImpl should be called instead.
     */
    EnvironmentImpl getMaybeNullEnvImpl() {
        return environmentImpl;
    }

    /* Returns true, if this is a handle allocated internally by JE. */
    protected boolean isInternalHandle() {
        return false;
    }

    /**
     * @throws UnsupportedOperationException via the database operation methods
     * (remove, truncate, rename) and potentially other methods that require a
     * writable environment.
     */
    private void checkWritable(final EnvironmentImpl envImpl ) {
        if (envImpl.isReadOnly()) {
            throw new UnsupportedOperationException
                ("Environment is Read-Only.");
        }
    }

    void invalidate(Error e) {
        final EnvironmentImpl envImpl = environmentImpl;
        if (envImpl == null) {
            return;
        }
        envImpl.invalidate(e);
    }
}
