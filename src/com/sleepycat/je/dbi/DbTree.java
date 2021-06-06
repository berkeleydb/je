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

package com.sleepycat.je.dbi;

import static com.sleepycat.je.log.entry.DbOperationType.CREATE;
import static com.sleepycat.je.log.entry.DbOperationType.REMOVE;
import static com.sleepycat.je.log.entry.DbOperationType.RENAME;
import static com.sleepycat.je.log.entry.DbOperationType.TRUNCATE;
import static com.sleepycat.je.log.entry.DbOperationType.UPDATE_CONFIG;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DatabaseNotFoundException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.LockConflictException;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.log.DbOpReplicationContext;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.LogUtils;
import com.sleepycat.je.log.Loggable;
import com.sleepycat.je.log.ReplicationContext;
import com.sleepycat.je.tree.ChildReference;
import com.sleepycat.je.tree.IN;
import com.sleepycat.je.tree.LN;
import com.sleepycat.je.tree.MapLN;
import com.sleepycat.je.tree.NameLN;
import com.sleepycat.je.tree.Tree;
import com.sleepycat.je.tree.TreeUtils;
import com.sleepycat.je.tree.WithRootLatched;
import com.sleepycat.je.txn.BasicLocker;
import com.sleepycat.je.txn.HandleLocker;
import com.sleepycat.je.txn.LockGrantType;
import com.sleepycat.je.txn.LockResult;
import com.sleepycat.je.txn.LockType;
import com.sleepycat.je.txn.Locker;
import com.sleepycat.je.txn.Txn;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.utilint.StringUtils;

/**
 * DbTree represents the database directory for this environment. DbTree is
 * itself implemented through two databases. The nameDatabase maps
 * databaseName-> an internal databaseId. The idDatabase maps
 * databaseId->DatabaseImpl.
 *
 * For example, suppose we have two databases, foo and bar. We have the
 * following structure:
 *
 *           nameDatabase                          idDatabase
 *               IN                                    IN
 *                |                                     |
 *               BIN                                   BIN
 *    +-------------+--------+            +---------------+--------+
 *  .               |        |            .               |        |
 * NameLNs         NameLN    NameLN      MapLNs for   MapLN        MapLN
 * for internal    key=bar   key=foo     internal dbs key=53       key=79
 * dbs             data=     data=                    data=        data=
 *                 dbId79    dbId53                   DatabaseImpl DatabaseImpl
 *                                                        |            |
 *                                                   Tree for foo  Tree for bar
 *                                                        |            |
 *                                                     root IN       root IN
 *
 * Databases, Cursors, the cleaner, compressor, and other entities have
 * references to DatabaseImpls. It's important that object identity is properly
 * maintained, and that all constituents reference the same DatabaseImpl for
 * the same db, lest they develop disparate views of the in-memory database;
 * corruption would ensue. To ensure that, all entities must obtain their
 * DatabaseImpl by going through the idDatabase.
 *
 * DDL type operations such as create, rename, remove and truncate get their
 * transactional semantics by transactionally locking the NameLN appropriately.
 * A read-lock on the NameLN, called a handle lock, is maintained for all DBs
 * opened via the public API (openDatabase).  This prevents them from being
 * renamed or removed while open.  See HandleLocker for details.
 *
 * However, for internal database operations, no handle lock on the NameLN is
 * acquired and MapLNs are locked with short-lived non-transactional Lockers.
 * An entity that is trying to get a reference to the DatabaseImpl gets a short
 * lived read lock just for the fetch of the MapLN, and a DatabaseImpl usage
 * count is incremented to prevent eviction; see getDb and releaseDb. A write
 * lock on the MapLN is taken when the database is created, deleted, or when
 * the MapLN is evicted. (see DatabaseImpl.isInUse())
 *
 * The nameDatabase operates pretty much as a regular application database in
 * terms of eviction and recovery. The idDatabase requires special treatment
 * for both eviction and recovery.
 *
 * The issues around eviction of the idDatabase center on the need to ensure
 * that there are no other current references to the DatabaseImpl other than
 * that held by the mapLN. The presence of a current reference would both make
 * the DatabaseImpl not GC'able, and more importantly, would lead to object
 * identity confusion later on. For example, if the MapLN is evicted while
 * there is a current reference to its DatabaseImpl, and then refetched, there
 * will be two in-memory versions of the DatabaseImpl. Since locks on the
 * idDatabase are short lived, DatabaseImpl.useCount acts as a reference count
 * of active current references. DatabaseImpl.useCount must be modified and
 * read in conjunction with appropriate locking on the MapLN. See
 * DatabaseImpl.isInUse() for details.
 *
 * This reference count checking is only needed when the entire MapLN is
 * evicted. It's possible to evict only the root IN of the database in
 * question, since that doesn't interfere with the DatabaseImpl object
 * identity.
 *
 * Another dependency on usage counts was introduced to prevent MapLN deletion
 * during cleaner and checkpointer operations that are processing entries for a
 * DB.  (Without usage counts, this problem would have occurred even if DB
 * eviction were never implemented.)  When the usage count is non-zero it
 * prohibits deleteMapLN from running. The deleted state of the MapLN must not
 * change during a reader operation (operation by a thread that has called
 * getDb and not yet called releaseDb).
 *
 * Why not just hold a MapLN read lock during a reader operation?
 * --------------------------------------------------------------
 * Originally this was not done because of cleaner performance.  We afraid that  * either of the following solutions would not perform well:
 *   + If we get (and release) a MapLN lock for every entry in a log file, this
 *     adds a lot of per-entry overhead.
 *   + If we hold the MapLN read lock for the duration of a log file cleaning
 *     (the assumption is that many entries are for the same DB), then we block
 *     checkpoints during that period, when they call modifyDbRoot.
 * Therefore, the usage count is incremented once per DB encountered during log
 * cleaning, and the count is decremented at the end.  This caching approach is
 * also used by the HA replayer.  In both cases, we do not want to lock the
 * MapLN every entry/operation, and we do not want to block checkpoints or
 * other callers of modifyDbRoot.  It is acceptable, however, to block DB
 * naming operations.
 *
 * In addition we allow modifyDbRoot to run when even the usage count is
 * non-zero, which would not be possible using a read-write locking strategy.
 * I'm not sure why this was done originally, perhaps to avoid blocking.  But
 * currently, it is necessary to prevent a self-deadlock.  All callers of
 * modifyDbRoot first call getDb, which increments the usage count.  So if
 * modifyDbRoot was to check the usage count and retry if non-zero (like
 * deleteMapLN), then it would loop forever.
 *
 * Why are the retry loops necessary in the DbTree methods?
 * --------------------------------------------------------
 * Three methods that access the MapLN perform retries (forever) when there is
 * a lock conflict: getDb, modifyDbRoot and deleteMapLN.  Initially the retry
 * loops were added to compensate for certain slow operations. To solve that
 * problem, perhaps there are alternative solutions (increasing the lock
 * timeout).  However, the deleteMapLN retry loop is necessary to avoid
 * deleting it when the DB is in use by reader operations.
 *
 * Tendency to livelock
 * --------------------
 * Because MapLN locks are short lived, but a reader operation may hold a
 * MapLN/DatabaseImpl for a longer period by incrementing the usage count,
 * there is the possibility of livelock.  One strategy for avoiding livelock is
 * to avoid algorithms where multiple threads continuously call getDb and
 * releaseDb, since this could prevent completion of deleteMapLN.  [#20816]
 */
public class DbTree implements Loggable {

    /* The id->DatabaseImpl tree is always id 0 */
    public static final DatabaseId ID_DB_ID = new DatabaseId(0);
    /* The name->id tree is always id 1 */
    public static final DatabaseId NAME_DB_ID = new DatabaseId(1);

    /** Map from internal DB name to type. */
    private final static Map<String, DbType> INTERNAL_TYPES_BY_NAME;
    static {
        final EnumSet<DbType> set = EnumSet.allOf(DbType.class);
        INTERNAL_TYPES_BY_NAME = new HashMap<String, DbType>(set.size());
        for (DbType t : set) {
            if (t.isInternal()) {
                INTERNAL_TYPES_BY_NAME.put(t.getInternalName(), t);
            }
        }
    }

    /**
     * Returns the DbType for a given DB name.
     *
     * Note that we allow dbName to be null, because it may be null when the
     * 'debug database name' is not yet known to DatabaseImpl.  This works
     * because the debug name is always known immediately for internal DBs.
     */
    public static DbType typeForDbName(String dbName) {
        final DbType t = INTERNAL_TYPES_BY_NAME.get(dbName);
        if (t != null) {
            return t;
        }
        return DbType.USER;
    }

    /*
     * Database Ids:
     * We need to ensure that local and replicated databases use different
     * number spaces for their ids, so there can't be any possible conflicts.
     * Local, non replicated databases use positive values, replicated
     * databases use negative values.  Values -1 thru NEG_DB_ID_START are
     * reserved for future special use.
     */
    public static final long NEG_DB_ID_START = -256L;
    private final AtomicLong lastAllocatedLocalDbId;
    private final AtomicLong lastAllocatedReplicatedDbId;

    private final DatabaseImpl idDatabase;          // map db ids -> databases
    private final DatabaseImpl nameDatabase;        // map names -> dbIds

    /*
     * The log version at the time the env was created. Is -1 if the initial
     * version is unknown, which means it is prior to version 15 because this
     * field was added in version 15. For environments created with log version
     * 15 and greater, no log entries can have a version LT this field's value.
     */
    private int initialLogVersion;

    /* The flags byte holds a variety of attributes. */
    private byte flags;

    /*
     * The replicated bit is set for environments that are opened with
     * replication. The behavior is as follows:
     *
     * Env is     Env is     Persistent          Follow-on action
     * replicated brand new  value of
     *                       DbTree.isReplicated
     *
     * 0             1         n/a               replicated bit = 0;
     * 0             0           0               none
     * 0             0           1               true for r/o, false for r/w
     * 1             1          n/a              replicated bit = 1
     * 1             0           0               require config of all dbs
     * 1             0           1               none
     */
    private static final byte REPLICATED_BIT = 0x1;

    /*
     * The rep converted bit is set when an environments was originally created
     * as a standalone (non-replicated) environment, and has been changed to a
     * replicated environment.
     *
     * The behaviors are as follows:
     *
     * Value of      Value of the    What happens      Can open     Can open
     * RepConfig.      DbTree        when we call       as r/o       as r/2
     * allowConvert  replicated bit  ReplicatedEnv()  Environment  Environment
     *                                                   later on?   later on?
     *
     *                           throw exception,   Yes, because  Yes, because
     *                            complain that the    env is not   env is not
     *  false          false         env is not        converted    converted
     *                               replicated
     *
     *                                              Yes, always ok  No, this is
     *                                                 open a         now a
     *  true           false          do conversion   replicated     replicated
     *                                               env with r/o       env
     *
     *
     *  Ignore         true or      open as a replicated
     * allowConvert   brand new      env the usual way       Yes         No
     *               Environment
     */
    private static final byte REP_CONVERTED_BIT = 0x2;

    /*
     * The dups converted bit is set when we have successfully converted all
     * dups databases after recovery, to indicate that we don't need to perform
     * this conversion again for this environment.  It is set initially for a
     * brand new environment that uses the new dup database format.
     */
    private static final byte DUPS_CONVERTED_BIT = 0x4;

    /*
     * The preserve VLSN bit is set in a replicated environment only, and may
     * never be changed after initial environment creation.  See
     * RepParams.PRESERVE_RECORD_VERSION.
     */
    private static final byte PRESERVE_VLSN_BIT = 0x8;

    /**
     * Number of LNs in the naming DB considered to be fairly small, and
     * therefore to result in fairly fast execution of getDbName.
     */
    private static final long FAST_NAME_LOOKUP_MAX_LNS = 100;

    private EnvironmentImpl envImpl;

    /**
     * Create a dbTree from the log.
     */
    public DbTree() {
        this.envImpl = null;
        idDatabase = new DatabaseImpl();
        idDatabase.setDebugDatabaseName(DbType.ID.getInternalName());

        /*
         * The default is false, but just in case we ever turn it on globally
         * for testing this forces it off.
         */
        idDatabase.clearKeyPrefixing();
        nameDatabase = new DatabaseImpl();
        nameDatabase.clearKeyPrefixing();
        nameDatabase.setDebugDatabaseName(DbType.NAME.getInternalName());

        /* These sequences are initialized by readFromLog. */
        lastAllocatedLocalDbId = new AtomicLong();
        lastAllocatedReplicatedDbId = new AtomicLong();

        initialLogVersion = -1;
    }

    /**
     * Create a new dbTree for a new environment.
     */
    public DbTree(EnvironmentImpl env,
                  boolean replicationIntended,
                  boolean preserveVLSN)
        throws DatabaseException {

        this.envImpl = env;

        /*
         * Sequences must be initialized before any databases are created.  0
         * and 1 are reserved, so we start at 2. We've put -1 to
         * NEG_DB_ID_START asided for the future.
         */
        lastAllocatedLocalDbId = new AtomicLong(1);
        lastAllocatedReplicatedDbId = new AtomicLong(NEG_DB_ID_START);

        /* The id database is local */
        DatabaseConfig idConfig = new DatabaseConfig();
        idConfig.setReplicated(false /* replicated */);

        /*
         * The default is false, but just in case we ever turn it on globally
         * for testing this forces it off.
         */
        idConfig.setKeyPrefixing(false);
        idDatabase = new DatabaseImpl(null,
                                      DbType.ID.getInternalName(),
                                      new DatabaseId(0),
                                      env,
                                      idConfig);
        /* Force a reset if enabled globally. */
        idDatabase.clearKeyPrefixing();

        DatabaseConfig nameConfig = new DatabaseConfig();
        nameConfig.setKeyPrefixing(false);
        nameDatabase = new DatabaseImpl(null,
                                        DbType.NAME.getInternalName(),
                                        new DatabaseId(1),
                                        env,
                                        nameConfig);
        /* Force a reset if enabled globally. */
        nameDatabase.clearKeyPrefixing();

        if (replicationIntended) {
            setIsReplicated();
        }

        if (preserveVLSN) {
            setPreserveVLSN();
        }

        /* New environments don't need dup conversion. */
        setDupsConverted();

        initialLogVersion = LogEntryType.LOG_VERSION;
    }

    /**
     * The last allocated local and replicated db ids are used for ckpts.
     */
    public long getLastLocalDbId() {
        return lastAllocatedLocalDbId.get();
    }

    public long getLastReplicatedDbId() {
        return lastAllocatedReplicatedDbId.get();
    }

    /**
     * We get a new database id of the appropriate kind when creating a new
     * database.
     */
    private long getNextLocalDbId() {
        return lastAllocatedLocalDbId.incrementAndGet();
    }

    private long getNextReplicatedDbId() {
        return lastAllocatedReplicatedDbId.decrementAndGet();
    }

    /**
     * Initialize the db ids, from recovery.
     */
    public void setLastDbId(long lastReplicatedDbId, long lastLocalDbId) {
        lastAllocatedReplicatedDbId.set(lastReplicatedDbId);
        lastAllocatedLocalDbId.set(lastLocalDbId);
    }

    /**
     * @return true if this id is for a replicated db.
     */
    private boolean isReplicatedId(long id) {
        return id < NEG_DB_ID_START;
    }

    /*
     * Tracks the lowest replicated database id used during a replay of the
     * replication stream, so that it's available as the starting point if this
     * replica transitions to being the master.
     */
    public void updateFromReplay(DatabaseId replayDbId) {
        assert !envImpl.isMaster();

        final long replayVal = replayDbId.getId();

        if (replayVal > 0 && !envImpl.isRepConverted()) {
            throw EnvironmentFailureException.unexpectedState
                ("replay database id is unexpectedly positive " + replayDbId);
        }

        if (replayVal < lastAllocatedReplicatedDbId.get()) {
            lastAllocatedReplicatedDbId.set(replayVal);
        }
    }

    /**
     * Initialize the db tree during recovery, after instantiating the tree
     * from the log.
     * a. set up references to the environment impl
     * b. check for replication rules.
     */
    void initExistingEnvironment(EnvironmentImpl eImpl)
        throws DatabaseException {

        eImpl.checkRulesForExistingEnv(isReplicated(), getPreserveVLSN());
        this.envImpl = eImpl;
        idDatabase.setEnvironmentImpl(eImpl);
        nameDatabase.setEnvironmentImpl(eImpl);
    }

    /**
     * Creates a new database object given a database name.
     *
     * Increments the use count of the new DB to prevent it from being evicted.
     * releaseDb should be called when the returned object is no longer used,
     * to allow it to be evicted.  See DatabaseImpl.isInUse.  [#13415]
     */
    public DatabaseImpl createDb(Locker locker,
                                 String databaseName,
                                 DatabaseConfig dbConfig,
                                 HandleLocker handleLocker)
        throws DatabaseException {

        return doCreateDb(locker,
                          databaseName,
                          dbConfig,
                          handleLocker,
                          null,  // replicatedLN
                          null); // repContext, to be decided by new db
    }

    /**
     * Create a database for internal use. It may or may not be replicated.
     * Since DatabaseConfig.replicated is true by default, be sure to
     * set it to false if this is a internal, not replicated database.
     */
    public DatabaseImpl createInternalDb(Locker locker,
                                         String databaseName,
                                         DatabaseConfig dbConfig)
        throws DatabaseException {

        /* Force all internal databases to not use key prefixing. */
        dbConfig.setKeyPrefixing(false);
        DatabaseImpl ret =
            doCreateDb(locker,
                       databaseName,
                       dbConfig,
                       null,  // handleLocker,
                       null,  // replicatedLN
                       null); // repContext, to be decided by new db
        /* Force a reset if enabled globally. */
        ret.clearKeyPrefixing();
        return ret;
    }

    /**
     * Create a replicated database on this client node.
     */
    public DatabaseImpl createReplicaDb(Locker locker,
                                        String databaseName,
                                        DatabaseConfig dbConfig,
                                        NameLN replicatedLN,
                                        ReplicationContext repContext)
        throws DatabaseException {

        return doCreateDb(locker,
                          databaseName,
                          dbConfig,
                          null, // databaseHndle
                          replicatedLN,
                          repContext);
    }

    /**
     * Create a database.
     *
     * Increments the use count of the new DB to prevent it from being evicted.
     * releaseDb should be called when the returned object is no longer used,
     * to allow it to be evicted.  See DatabaseImpl.isInUse.  [#13415]
     *
     * Do not evict (do not call CursorImpl.setAllowEviction(true)) during low
     * level DbTree operation. [#15176]
     */
    private DatabaseImpl doCreateDb(Locker nameLocker,
                                    String databaseName,
                                    DatabaseConfig dbConfig,
                                    HandleLocker handleLocker,
                                    NameLN replicatedLN,
                                    ReplicationContext repContext)
        throws DatabaseException {

        /* Create a new database object. */
        DatabaseId newId = null;
        long allocatedLocalDbId = 0;
        long allocatedRepDbId = 0;
        if (replicatedLN != null) {

            /*
             * This database was created on a master node and is being
             * propagated to this client node.
             */
            newId = replicatedLN.getId();
        } else {

            /*
             * This database has been created locally, either because this is
             * a non-replicated node or this is the replicated group master.
             */
            if (envImpl.isReplicated() &&
                dbConfig.getReplicated()) {
                newId = new DatabaseId(getNextReplicatedDbId());
                allocatedRepDbId = newId.getId();
            } else {
                newId = new DatabaseId(getNextLocalDbId());
                allocatedLocalDbId = newId.getId();
            }
        }

        DatabaseImpl newDb = null;
        CursorImpl idCursor = null;
        CursorImpl nameCursor = null;
        boolean operationOk = false;
        Locker idDbLocker = null;
        try {
            newDb = new DatabaseImpl(nameLocker,
                                     databaseName, newId, envImpl, dbConfig);

            /* Get effective rep context and check for replica write. */
            ReplicationContext useRepContext = repContext;
            if (repContext == null) {
                useRepContext = newDb.getOperationRepContext(CREATE);
            }
            checkReplicaWrite(nameLocker, useRepContext);

            /* Insert it into name -> id db. */
            nameCursor = new CursorImpl(nameDatabase, nameLocker);
            LN nameLN = null;
            if (replicatedLN != null) {
                nameLN = replicatedLN;
            } else {
                nameLN = new NameLN(newId);
            }

            nameCursor.insertRecord(
                StringUtils.toUTF8(databaseName), // key
                nameLN, false /*blindInsertion*/, useRepContext);

            /* Record handle lock. */
            if (handleLocker != null) {
                acquireHandleLock(nameCursor, handleLocker);
            }

            /* Insert it into id -> name db, in auto commit mode. */
            idDbLocker = BasicLocker.createBasicLocker(envImpl);
            idCursor = new CursorImpl(idDatabase, idDbLocker);

            idCursor.insertRecord(
                newId.getBytes() /*key*/, new MapLN(newDb) /*ln*/,
                false /*blindInsertion*/, ReplicationContext.NO_REPLICATE);

            /* Increment DB use count with lock held. */
            newDb.incrementUseCount();
            operationOk = true;
        } finally {
            if (idCursor != null) {
                idCursor.close();
            }

            if (nameCursor != null) {
                nameCursor.close();
            }

            if (idDbLocker != null) {
                idDbLocker.operationEnd(operationOk);
            }

            /*
             * Undo the allocation of the database ID if DB creation fails.  We
             * use compareAndSet so that we don't undo the assignment of the ID
             * by another concurrent operation, for example, truncation.
             *
             * Note that IDs are not conserved in doTruncateDb when a failure
             * occurs.  This inconsistency is historical and may or may not be
             * the best approach.
             *
             * [#18642]
             */
            if (!operationOk) {
                if (allocatedRepDbId != 0) {
                    lastAllocatedReplicatedDbId.compareAndSet
                        (allocatedRepDbId, allocatedRepDbId + 1);
                }
                if (allocatedLocalDbId != 0) {
                    lastAllocatedLocalDbId.compareAndSet
                        (allocatedLocalDbId, allocatedLocalDbId - 1);
                }
            }
        }

        return newDb;
    }

    /**
     * Opens (or creates if it does not exist) an internal, non-replicated DB.
     * Returns null only if the DB does not exist and the env is read-only.
     */
    public DatabaseImpl openNonRepInternalDB(final DbType dbType) {

        final String name = dbType.getInternalName();

        final Locker autoTxn = Txn.createLocalAutoTxn(
            envImpl, new TransactionConfig());

        boolean operationOk = false;
        try {
            DatabaseImpl db = getDb(autoTxn, name, null, false);

            if (db == null) {

                if (envImpl.isReadOnly()) {
                    return null;
                }

                final DatabaseConfig dbConfig = new DatabaseConfig();
                dbConfig.setReplicated(false);

                db = createInternalDb(autoTxn, name, dbConfig);
            }
            operationOk = true;
            return db;
        } finally {
            autoTxn.operationEnd(operationOk);
        }
    }

    /**
     * Called after locking a NameLN with nameCursor when opening a database.
     * The NameLN may be locked for read or write, depending on whether the
     * database existed when openDatabase was called.  Here we additionally
     * lock the NameLN for read on behalf of the handleLocker, which is kept
     * by the Database handle.
     *
     * The lock must be acquired while the BIN is latched, so the locker will
     * be updated if the LSN changes.  There is no lock contention possible
     * because the HandleLocker shares locks with the nameCursor locker, and
     * jumpAheadOfWaiters=true is passed in case another locker is waiting on a
     * write lock.
     *
     * If the lock is denied, checkPreempted is called on the nameCursor
     * locker, in case the lock is denied because the nameCursor's lock was
     * preempted. If so, DatabasePreemptedException will be thrown.
     *
     * @see CursorImpl#lockLN
     * @see HandleLocker
     */
    private void acquireHandleLock(CursorImpl nameCursor,
                                   HandleLocker handleLocker) {
        nameCursor.latchBIN();
        try {
            final long lsn = nameCursor.getCurrentLsn();

            final LockResult lockResult = handleLocker.nonBlockingLock
                (lsn, LockType.READ, true /*jumpAheadOfWaiters*/,
                 nameDatabase);

            if (lockResult.getLockGrant() == LockGrantType.DENIED) {
                nameCursor.getLocker().checkPreempted(null);
                throw EnvironmentFailureException.unexpectedState
                    ("No contention is possible with HandleLocker: " +
                     DbLsn.getNoFormatString(lsn));
            }
        } finally {
            nameCursor.releaseBIN();
        }
    }

    /**
     * Check deferred write settings before writing the MapLN.
     * @param db the database represented by this MapLN
     */
    public void optionalModifyDbRoot(DatabaseImpl db)
        throws DatabaseException {

        if (db.isDeferredWriteMode()) {
            return;
        }

        modifyDbRoot(db);
    }

    /**
     * Write the MapLN to disk.
     * @param db the database represented by this MapLN
     */
    public void modifyDbRoot(DatabaseImpl db)
        throws DatabaseException {

        modifyDbRoot(db, DbLsn.NULL_LSN /*ifBeforeLsn*/, true /*mustExist*/);
    }

    /**
     * Write a MapLN to the log in order to:
     *  - propagate a root change
     *  - save per-db utilization information
     *  - save database config information.
     * Any MapLN writes must be done through this method, in order to ensure
     * that the root latch is taken, and updates to the rootIN are properly
     * safeguarded. See MapN.java for more detail.
     *
     * @param db the database whose root is held by this MapLN
     *
     * @param ifBeforeLsn if argument is not NULL_LSN, only do the write if
     * this MapLN's current LSN is before isBeforeLSN.
     *
     * @param mustExist if true, throw DatabaseException if the DB does not
     * exist; if false, silently do nothing.
     */
    public void modifyDbRoot(
        DatabaseImpl db,
        long ifBeforeLsn,
        boolean mustExist)
        throws DatabaseException {

        /*
         * Do not write LNs in read-only env.  This method is called when
         * recovery causes a root split. [#21493]
         */
        if (envImpl.isReadOnly() && envImpl.isInInit()) {
            return;
        }

        if (db.getId().equals(ID_DB_ID) ||
            db.getId().equals(NAME_DB_ID)) {
            envImpl.logMapTreeRoot();
        } else {
            DatabaseEntry keyDbt = new DatabaseEntry(db.getId().getBytes());

            /*
             * Retry indefinitely in the face of lock timeouts since the
             * lock on the MapLN is only supposed to be held for short
             * periods.
             */
            while (true) {
                Locker idDbLocker = null;
                CursorImpl cursor = null;
                boolean operationOk = false;
                try {
                    idDbLocker = BasicLocker.createBasicLocker(envImpl);
                    cursor = new CursorImpl(idDatabase, idDbLocker);

                    boolean found = cursor.searchExact(keyDbt, LockType.WRITE);

                    if (!found) {
                        if (mustExist) {
                            throw new EnvironmentFailureException(
                                envImpl,
                                EnvironmentFailureReason.LOG_INTEGRITY,
                                "Can't find database ID: " + db.getId());
                        }
                        /* Do nothing silently. */
                        break;
                    }

                    /* Check BIN LSN while latched. */
                    if (ifBeforeLsn == DbLsn.NULL_LSN ||
                        DbLsn.compareTo(
                            cursor.getCurrentLsn(), ifBeforeLsn) < 0) {

                        MapLN mapLN = (MapLN) cursor.getCurrentLN(
                            true, /*isLatched*/ true/*unlatch*/);

                        assert mapLN != null; /* Should be locked. */

                        /* Perform rewrite. */
                        RewriteMapLN writeMapLN = new RewriteMapLN(cursor);
                        mapLN.getDatabase().getTree().withRootLatchedExclusive(
                            writeMapLN);

                        operationOk = true;
                    }
                    break;
                } catch (LockConflictException e) {
                    /* Continue loop and retry. */
                } finally {
                    if (cursor != null) {
                        cursor.releaseBIN();
                        cursor.close();
                    }
                    if (idDbLocker != null) {
                        idDbLocker.operationEnd(operationOk);
                    }
                }
            }
        }
    }

    private static class RewriteMapLN implements WithRootLatched {
        private final CursorImpl cursor;

        RewriteMapLN(CursorImpl cursor) {
            this.cursor = cursor;
        }

        public IN doWork(@SuppressWarnings("unused") ChildReference root)
            throws DatabaseException {

            DatabaseEntry dataDbt = new DatabaseEntry(new byte[0]);
            cursor.updateCurrentRecord(
                null /*replaceKey*/, dataDbt, null /*expirationInfo*/,
                null /*foundData*/, null /*returnNewData*/,
                ReplicationContext.NO_REPLICATE);
            return null;
        }
    }

    /**
     * In other places (e.g., when write locking a record in ReadOnlyTxn) we
     * allow writes to the naming DB on a replica, since we allow both
     * replicated and non-replicated DBs and therefore some NameLNs are
     * replicated and some are not.  Below is the sole check to prevent a
     * creation, removal, truncation, or configuration update of a replicated
     * DB on a replica.  It will throw ReplicaWriteException on a replica if
     * this operation would assign a new VLSN. [#20543]
     */
    private void checkReplicaWrite(Locker locker,
                                   ReplicationContext repContext) {
        if (repContext != null && repContext.mustGenerateVLSN()) {
            locker.disallowReplicaWrite();
        }
    }

    /**
     * Used by lockNameLN to get the rep context, which is needed for calling
     * checkReplicaWrite.
     */
    interface GetRepContext {
        ReplicationContext get(DatabaseImpl dbImpl);
    }

    /**
     * Thrown by lockNameLN when an incorrect locker was used via auto-commit.
     * See Environment.DbNameOperation.  A checked exception is used to ensure
     * that it is always handled internally and never propagated to the app.
     */
    public static class NeedRepLockerException extends Exception {}

    /**
     * Helper for database operations. This method positions a cursor
     * on the NameLN that represents this database and write locks it.
     *
     * Do not evict (do not call CursorImpl.setAllowEviction(true)) during low
     * level DbTree operation. [#15176]
     *
     * @throws IllegalStateException via
     * Environment.remove/rename/truncateDatabase
     */
    private NameLockResult lockNameLN(Locker locker,
                                      String databaseName,
                                      String action,
                                      GetRepContext getRepContext) 
        throws DatabaseNotFoundException, NeedRepLockerException {

        /*
         * We have to return both a cursor on the naming tree and a
         * reference to the found DatabaseImpl.
         */
        NameLockResult result = new NameLockResult();

        /* Find the existing DatabaseImpl and establish a cursor. */
        result.dbImpl = getDb(locker, databaseName, null, true);
        if (result.dbImpl == null) {
            throw new DatabaseNotFoundException
                ("Attempted to " + action + " non-existent database " +
                 databaseName);
        }

        boolean success = false;
        try {
            /* Get effective rep context and check for replica write. */
            result.repContext = getRepContext.get(result.dbImpl);
            checkReplicaWrite(locker, result.repContext);

            /*
             * Check for an incorrect locker created via auto-commit.  This
             * check is made after we have the DatabaseImpl and can check
             * whether it is replicated.  See Environment.DbNameOperation.
             */
            if (envImpl.isReplicated() &&
                result.dbImpl.isReplicated() &&
                locker.getTxnLocker() != null &&
                locker.getTxnLocker().isAutoTxn() &&
                !locker.isReplicated()) {
                throw new NeedRepLockerException();
            }

            result.nameCursor = new CursorImpl(nameDatabase, locker);

            /* Position the cursor at the specified NameLN. */
            DatabaseEntry key =
                new DatabaseEntry(StringUtils.toUTF8(databaseName));
            /* See [#16210]. */
            boolean found = result.nameCursor.searchExact(key, LockType.WRITE);

            if (!found) {
                throw new DatabaseNotFoundException(
                    "Attempted to " + action + " non-existent database " +
                    databaseName);
            }

            /* Call lockAndGetCurrentLN to write lock the nameLN. */
            result.nameLN = (NameLN) result.nameCursor.getCurrentLN(
                true, /*isLatched*/ true/*unlatch*/);
            assert result.nameLN != null; /* Should be locked. */

            /*
             * Check for open handles after we have the write lock and no other
             * transactions can open a handle.  After obtaining the write lock,
             * other handles may be open only if (1) we preempted their locks,
             * or (2) a handle was opened with the same transaction as used for
             * this operation.  For (1), we mark the handles as preempted to
             * cause a DatabasePreemptedException the next time they are
             * accessed.  For (2), we throw IllegalStateException.
             */
            if (locker.getImportunate()) {
                /* We preempted the lock of all open DB handles. [#17015] */
                final String msg =
                    "Database " + databaseName +
                    " has been forcibly closed in order to apply a" +
                    " replicated " + action + " operation.  This Database" +
                    " and all associated Cursors must be closed.  All" +
                    " associated Transactions must be aborted.";
                for (Database db : result.dbImpl.getReferringHandles()) {
                    DbInternal.setPreempted(db, databaseName, msg);
                }
            } else {
                /* Disallow open handles for the same transaction. */
                int handleCount = result.dbImpl.getReferringHandleCount();
                if (handleCount > 0) {
                    throw new IllegalStateException
                        ("Can't " + action + " database " + databaseName +
                         ", " + handleCount + " open Database handles exist");
                }
            }
            success = true;
        } finally {
            if (!success) {
                releaseDb(result.dbImpl);
                if (result.nameCursor != null) {
                    result.nameCursor.releaseBIN();
                    result.nameCursor.close();
                }
            }
        }

        return result;
    }

    private static class NameLockResult {
        CursorImpl nameCursor;
        DatabaseImpl dbImpl;
        NameLN nameLN;
        ReplicationContext repContext;
    }

    /**
     * Update the NameLN for the DatabaseImpl when the DatabaseConfig changes.
     *
     * JE MapLN actually includes the DatabaseImpl information, but it is not
     * transactional, so the DatabaseConfig information is stored in 
     * NameLNLogEntry and replicated.
     *
     * So when there is a DatabaseConfig changes, we'll update the NameLN for 
     * the database, which will log a new NameLNLogEntry so that the rep stream
     * will transfer it to the replicas and it will be replayed.
     *
     * @param locker the locker used to update the NameLN
     * @param dbName the name of the database whose corresponding NameLN needs
     * to be updated
     * @param repContext information used while replaying a NameLNLogEntry on 
     * the replicas, it's null on master 
     */
    public void updateNameLN(Locker locker, 
                             String dbName, 
                             final DbOpReplicationContext repContext) 
        throws LockConflictException {

        assert dbName != null;

        /* Find and write lock on the NameLN. */
        final NameLockResult result;
        try {
            result = lockNameLN
                (locker, dbName, "updateConfig", new GetRepContext() {
                
                public ReplicationContext get(DatabaseImpl dbImpl) {
                    return (repContext != null) ?
                        repContext : 
                        dbImpl.getOperationRepContext(UPDATE_CONFIG, null);
                }
            });
        } catch (NeedRepLockerException e) {
            /* Should never happen; db is known when locker is created. */
            throw EnvironmentFailureException.unexpectedException(envImpl, e);
        }
        
        final CursorImpl nameCursor = result.nameCursor;
        final DatabaseImpl dbImpl = result.dbImpl;
        final ReplicationContext useRepContext = result.repContext;
        try {

            /* Log a NameLN. */
            DatabaseEntry dataDbt = new DatabaseEntry(new byte[0]);
            nameCursor.updateCurrentRecord(
                null /*replaceKey*/, dataDbt, null /*expirationInfo*/,
                null /*foundData*/, null /*returnNewData*/, useRepContext);
        } finally {
            releaseDb(dbImpl);
            nameCursor.releaseBIN();
            nameCursor.close();
        }
    }

    /**
     * Rename the database by creating a new NameLN and deleting the old one.
     *
     * @return the database handle of the impacted database
     *
     * @throws DatabaseNotFoundException if the operation fails because the
     * given DB name is not found.
     */
    private DatabaseImpl doRenameDb(Locker locker,
                                    String databaseName,
                                    String newName,
                                    NameLN replicatedLN,
                                    final DbOpReplicationContext repContext)
        throws DatabaseNotFoundException, NeedRepLockerException {

        final NameLockResult result = lockNameLN
            (locker, databaseName, "rename", new GetRepContext() {
            
            public ReplicationContext get(DatabaseImpl dbImpl) {
                return (repContext != null) ?
                    repContext :
                    dbImpl.getOperationRepContext(RENAME);
            }
        });

        final CursorImpl nameCursor = result.nameCursor;
        final DatabaseImpl dbImpl = result.dbImpl;
        final ReplicationContext useRepContext = result.repContext;
        try {

            /*
             * Rename simply deletes the one entry in the naming tree and
             * replaces it with a new one. Remove the oldName->dbId entry and
             * insert newName->dbId.
             */
            nameCursor.deleteCurrentRecord(ReplicationContext.NO_REPLICATE);
            final NameLN useLN =
                (replicatedLN != null) ?
                 replicatedLN :
                 new NameLN(dbImpl.getId());
            /* 
             * Reset cursor to remove old BIN before calling insertRecord.
             * [#16280]
             */
            nameCursor.reset();

            nameCursor.insertRecord(
                StringUtils.toUTF8(newName), useLN,
                false /*blindInsertion*/, useRepContext);

            dbImpl.setDebugDatabaseName(newName);
            return dbImpl;
        } finally {
            releaseDb(dbImpl);
            nameCursor.close();
        }
    }

    /**
     * Stand alone and Master invocations.
     *
     * @see #doRenameDb
     */
    public DatabaseImpl dbRename(Locker locker,
                                 String databaseName,
                                 String newName)
        throws DatabaseNotFoundException, NeedRepLockerException {

        return doRenameDb(locker, databaseName, newName, null, null);
    }

    /**
     * Replica invocations.
     *
     * @see #doRenameDb
     */
    public DatabaseImpl renameReplicaDb(Locker locker,
                                        String databaseName,
                                        String newName,
                                        NameLN replicatedLN,
                                        DbOpReplicationContext repContext)
        throws DatabaseNotFoundException {

        try {
            return doRenameDb(locker, databaseName, newName, replicatedLN,
                              repContext);
        } catch (NeedRepLockerException e) {
            /* Should never happen; db is known when locker is created. */
            throw EnvironmentFailureException.unexpectedException(envImpl, e);
        }
    }

    /**
     * Remove the database by deleting the nameLN.
     *
     * @return a handle to the renamed database
     *
     * @throws DatabaseNotFoundException if the operation fails because the
     * given DB name is not found, or the non-null checkId argument does not
     * match the database identified by databaseName.
     */
    private DatabaseImpl doRemoveDb(Locker locker,
                                    String databaseName,
                                    DatabaseId checkId,
                                    final DbOpReplicationContext repContext)
        throws DatabaseNotFoundException, NeedRepLockerException {

        CursorImpl nameCursor = null;

        final NameLockResult result = lockNameLN
            (locker, databaseName, "remove", new GetRepContext() {
            
            public ReplicationContext get(DatabaseImpl dbImpl) {
                return (repContext != null) ?
                    repContext :
                    dbImpl.getOperationRepContext(REMOVE);
            }
        });

        final ReplicationContext useRepContext = result.repContext;
        try {
            nameCursor = result.nameCursor;
            if (checkId != null && !checkId.equals(result.nameLN.getId())) {
                throw new DatabaseNotFoundException
                    ("ID mismatch: " + databaseName);
            }

            /*
             * Delete the NameLN. There's no need to mark any Database
             * handle invalid, because the handle must be closed when we
             * take action and any further use of the handle will re-look
             * up the database.
             */
            nameCursor.deleteCurrentRecord(useRepContext);

            /*
             * Schedule database for final deletion during commit. This
             * should be the last action taken, since this will take
             * effect immediately for non-txnal lockers.
             *
             * Do not call releaseDb here on result.dbImpl, since that is
             * taken care of by markDeleteAtTxnEnd.
             */
            locker.markDeleteAtTxnEnd(result.dbImpl, true);
            return result.dbImpl;
        } finally {
            if (nameCursor != null) {
                nameCursor.close();
            }
        }
    }

    /**
     * Stand alone and Master invocations.
     *
     * @see #doRemoveDb
     */
    public DatabaseImpl dbRemove(Locker locker,
                         String databaseName,
                         DatabaseId checkId)
        throws DatabaseNotFoundException, NeedRepLockerException {

        return doRemoveDb(locker, databaseName, checkId, null);
    }

    /**
     * Replica invocations.
     *
     * @see #doRemoveDb
     */
    public void removeReplicaDb(Locker locker,
                                String databaseName,
                                DatabaseId checkId,
                                DbOpReplicationContext repContext)
        throws DatabaseNotFoundException {

        try {
            doRemoveDb(locker, databaseName, checkId, repContext);
        } catch (NeedRepLockerException e) {
            /* Should never happen; db is known when locker is created. */
            throw EnvironmentFailureException.unexpectedException(envImpl, e);
        }
    }

    /**
     * To truncate, remove the database named by databaseName and
     * create a new database in its place.
     *
     * Do not evict (do not call CursorImpl.setAllowEviction(true)) during low
     * level DbTree operation. [#15176]
     *
     * @param returnCount if true, must return the count of records in the
     * database, which can be an expensive option.
     *
     * @return the record count, oldDb and newDb packaged in a TruncateDbResult
     *
     * @throws DatabaseNotFoundException if the operation fails because the
     * given DB name is not found.
     */
    public TruncateDbResult
        doTruncateDb(Locker locker,
                     String databaseName,
                     boolean returnCount,
                     NameLN replicatedLN,
                     final DbOpReplicationContext repContext)
        throws DatabaseNotFoundException, NeedRepLockerException {

        assert((replicatedLN != null) ? (repContext != null) : true);

        final NameLockResult result = lockNameLN
            (locker, databaseName, "truncate", new GetRepContext() {
            
            public ReplicationContext get(DatabaseImpl dbImpl) {
                return (repContext != null) ?
                    repContext :
                    dbImpl.getOperationRepContext(TRUNCATE, dbImpl.getId());
            }
        });

        final CursorImpl nameCursor = result.nameCursor;
        final ReplicationContext useRepContext = result.repContext;
        try {
            /*
             * Make a new database with an empty tree. Make the nameLN refer to
             * the id of the new database. If this database is replicated, the
             * new one should also be replicated, and vice versa.
             */
            DatabaseImpl oldDb = result.dbImpl;
            final DatabaseId newId =
                (replicatedLN != null) ?
                 replicatedLN.getId() :
                 new DatabaseId(isReplicatedId(oldDb.getId().getId()) ?
                                getNextReplicatedDbId() :
                                getNextLocalDbId());

            DatabaseImpl newDb = oldDb.cloneDatabase();
            newDb.incrementUseCount();
            newDb.setId(newId);
            newDb.setTree(new Tree(newDb));

            /*
             * Insert the new MapLN into the id tree. Do not use a transaction
             * on the id database, because we can not hold long term locks on
             * the mapLN.
             */
            Locker idDbLocker = null;
            CursorImpl idCursor = null;
            boolean operationOk = false;
            try {
                idDbLocker = BasicLocker.createBasicLocker(envImpl);
                idCursor = new CursorImpl(idDatabase, idDbLocker);

                idCursor.insertRecord(
                    newId.getBytes() /*key*/, new MapLN(newDb),
                    false /*blindInsertion*/, ReplicationContext.NO_REPLICATE);

                operationOk = true;
            } finally {
                if (idCursor != null) {
                    idCursor.close();
                }

                if (idDbLocker != null) {
                    idDbLocker.operationEnd(operationOk);
                }
            }
            result.nameLN.setId(newDb.getId());

            /* If required, count the number of records in the database. */
            final long recordCount = (returnCount ? oldDb.count(0) : 0);

            /* log the nameLN. */
            DatabaseEntry dataDbt = new DatabaseEntry(new byte[0]);

            nameCursor.updateCurrentRecord(
                null /*replaceKey*/, dataDbt, null /*expirationInfo*/,
                null /*foundData*/, null /*returnNewData*/,
                useRepContext);
            /*
             * Marking the lockers should be the last action, since it
             * takes effect immediately for non-txnal lockers.
             *
             * Do not call releaseDb here on oldDb or newDb, since that is
             * taken care of by markDeleteAtTxnEnd.
             */

            /* Schedule old database for deletion if txn commits. */
            locker.markDeleteAtTxnEnd(oldDb, true);

            /* Schedule new database for deletion if txn aborts. */
            locker.markDeleteAtTxnEnd(newDb, false);

            return new TruncateDbResult(oldDb, newDb, recordCount);
        } finally {
            nameCursor.releaseBIN();
            nameCursor.close();
        }
    }

    /*
     * Effectively a struct used to return multiple values of interest.
     */
    public static class TruncateDbResult {
        public final DatabaseImpl oldDB;
        public final DatabaseImpl newDb;
        public final long recordCount;

        public TruncateDbResult(DatabaseImpl oldDB,
                                DatabaseImpl newDb,
                                long recordCount) {
            this.oldDB = oldDB;
            this.newDb = newDb;
            this.recordCount = recordCount;
        }
    }

    /**
     * @see #doTruncateDb
     */
    public TruncateDbResult truncate(Locker locker,
                                     String databaseName,
                                     boolean returnCount)
        throws DatabaseNotFoundException, NeedRepLockerException {

        return doTruncateDb(locker, databaseName, returnCount, null, null);
    }

    /**
     * @see #doTruncateDb
     */
    public TruncateDbResult truncateReplicaDb(Locker locker,
                                              String databaseName,
                                              boolean returnCount,
                                              NameLN replicatedLN,
                                              DbOpReplicationContext repContext)
        throws DatabaseNotFoundException {

        try {
            return doTruncateDb(locker, databaseName, returnCount,
                                replicatedLN, repContext);
        } catch (NeedRepLockerException e) {
            /* Should never happen; db is known when locker is created. */
            throw EnvironmentFailureException.unexpectedException(envImpl, e);
        }
    }

    /*
     * Remove the mapLN that refers to this database.
     *
     * Do not evict (do not call CursorImpl.setAllowEviction(true)) during low
     * level DbTree operation. [#15176]
     */
    void deleteMapLN(DatabaseId id)
        throws DatabaseException {

        /*
         * Retry indefinitely in the face of lock timeouts since the lock on
         * the MapLN is only supposed to be held for short periods.
         */
        boolean done = false;
        while (!done) {
            Locker idDbLocker = null;
            CursorImpl idCursor = null;
            boolean operationOk = false;
            try {
                idDbLocker = BasicLocker.createBasicLocker(envImpl);
                idCursor = new CursorImpl(idDatabase, idDbLocker);

                boolean found = idCursor.searchExact(
                    new DatabaseEntry(id.getBytes()), LockType.WRITE);

                if (found) {

                    /*
                     * If the database is in use by an internal JE operation
                     * (checkpointing, cleaning, etc), release the lock (done
                     * in the finally block) and retry.  [#15805]
                     */
                    MapLN mapLN = (MapLN) idCursor.getCurrentLN(
                        true, /*isLatched*/ true/*unlatch*/);

                    assert mapLN != null;
                    DatabaseImpl dbImpl = mapLN.getDatabase();

                    if (!dbImpl.isInUseDuringDbRemove()) {
                        idCursor.deleteCurrentRecord(
                            ReplicationContext.NO_REPLICATE);
                        done = true;
                    }
                } else {
                    /* MapLN does not exist. */
                    done = true;
                }
                operationOk = true;
            } catch (LockConflictException e) {
                /* Continue loop and retry. */
            } finally {
                if (idCursor != null) {
                    /* searchExact leaves BIN latched. */
                    idCursor.releaseBIN();
                    idCursor.close();
                }
                if (idDbLocker != null) {
                    idDbLocker.operationEnd(operationOk);
                }
            }
        }
    }

    /**
     * Get a database object given a database name.  Increments the use count
     * of the given DB to prevent it from being evicted.  releaseDb should be
     * called when the returned object is no longer used, to allow it to be
     * evicted.  See DatabaseImpl.isInUse.
     * [#13415]
     *
     * Do not evict (do not call CursorImpl.setAllowEviction(true)) during low
     * level DbTree operation. [#15176]
     *
     * @param nameLocker is used to access the NameLN. As always, a NullTxn
     *  is used to access the MapLN.
     * @param databaseName target database
     * @return null if database doesn't exist
     */
    public DatabaseImpl getDb(Locker nameLocker,
                              String databaseName,
                              HandleLocker handleLocker,
                              boolean writeLock)
        throws DatabaseException {

        /* Use count is not incremented for idDatabase and nameDatabase. */
        if (databaseName.equals(DbType.ID.getInternalName())) {
            return idDatabase;
        } else if (databaseName.equals(DbType.NAME.getInternalName())) {
            return nameDatabase;
        }

        /*
         * Search the nameDatabase tree for the NameLn for this name.
         */
        CursorImpl nameCursor = null;
        DatabaseId id = null;

        try {
            nameCursor = new CursorImpl(nameDatabase, nameLocker);
            DatabaseEntry keyDbt =
                new DatabaseEntry(StringUtils.toUTF8(databaseName));

            boolean found;
            if (writeLock) {
                found = nameCursor.searchExact(keyDbt, LockType.WRITE);
            } else {
                found = nameCursor.searchExact(keyDbt, LockType.READ);
            }

            if (found) {
                NameLN nameLN = (NameLN) nameCursor.getCurrentLN(
                    true, /*isLatched*/ true/*unlatch*/);
                assert nameLN != null; /* Should be locked. */
                id = nameLN.getId();

                /* Record handle lock. */
                if (handleLocker != null) {
                    acquireHandleLock(nameCursor, handleLocker);
                }
            }
        } finally {
            if (nameCursor != null) {
                nameCursor.releaseBIN();
                nameCursor.close();
            }
        }

        /*
         * Now search the id tree.
         */
        if (id == null) {
            return null;
        }
        return getDb(id, -1, databaseName);
    }

    /**
     * Get a database object based on an id only.  Used by recovery, cleaning
     * and other clients who have an id in hand, and don't have a resident
     * node, to find the matching database for a given log entry.
     */
    public DatabaseImpl getDb(DatabaseId dbId)
        throws DatabaseException {

        return getDb(dbId, -1);
    }

    /**
     * Get a database object based on an id only. Specify the lock timeout to
     * use, or -1 to use the default timeout.  A timeout should normally only
     * be specified by daemons with their own timeout configuration.  public
     * for unit tests.
     */
    public DatabaseImpl getDb(DatabaseId dbId, long lockTimeout)
        throws DatabaseException {

        return getDb(dbId, lockTimeout, (String) null);
    }

    /**
     * Get a database object based on an id only, caching the id-db mapping in
     * the given map.
     */
    public DatabaseImpl getDb(DatabaseId dbId,
                              long lockTimeout,
                              Map<DatabaseId, DatabaseImpl> dbCache)
        throws DatabaseException {

        if (dbCache.containsKey(dbId)) {
            return dbCache.get(dbId);
        }
        DatabaseImpl db = getDb(dbId, lockTimeout, (String) null);
        dbCache.put(dbId, db);
        return db;
    }

    /**
     * Get a database object based on an id only. Specify the lock timeout to
     * use, or -1 to use the default timeout.  A timeout should normally only
     * be specified by daemons with their own timeout configuration.  public
     * for unit tests.
     *
     * Increments the use count of the given DB to prevent it from being
     * evicted.  releaseDb should be called when the returned object is no
     * longer used, to allow it to be evicted.  See DatabaseImpl.isInUse.
     * [#13415]
     *
     * Do not evict (do not call CursorImpl.setAllowEviction(true)) during low
     * level DbTree operation. [#15176]
     */
    public DatabaseImpl getDb(DatabaseId dbId,
                              long lockTimeout,
                              String dbNameIfAvailable)
        throws DatabaseException {

        if (dbId.equals(idDatabase.getId())) {
            /* We're looking for the id database itself. */
            return idDatabase;
        } else if (dbId.equals(nameDatabase.getId())) {
            /* We're looking for the name database itself. */
            return nameDatabase;
        } else {
            /* Scan the tree for this db. */
            DatabaseImpl foundDbImpl = null;

            /*
             * Retry indefinitely in the face of lock timeouts.  Deadlocks may
             * be due to conflicts with modifyDbRoot.
             */
            while (true) {
                Locker locker = null;
                CursorImpl idCursor = null;
                boolean operationOk = false;
                try {
                    locker = BasicLocker.createBasicLocker(envImpl);
                    if (lockTimeout != -1) {
                        locker.setLockTimeout(lockTimeout);
                    }
                    idCursor = new CursorImpl(idDatabase, locker);
                    DatabaseEntry keyDbt = new DatabaseEntry(dbId.getBytes());

                    boolean found = idCursor.searchExact(keyDbt, LockType.READ);

                    if (found) {
                        MapLN mapLN = (MapLN) idCursor.getCurrentLN(
                            true, /*isLatched*/ true /*unlatch*/);
                        assert mapLN != null; /* Should be locked. */
                        foundDbImpl =  mapLN.getDatabase();
                        /* Increment DB use count with lock held. */
                        foundDbImpl.incrementUseCount();
                    }
                    operationOk = true;
                    break;
                } catch (LockConflictException e) {
                    /* Continue loop and retry. */
                } finally {
                    if (idCursor != null) {
                        idCursor.releaseBIN();
                        idCursor.close();
                    }
                    if (locker != null) {
                        locker.operationEnd(operationOk);
                    }
                }
            }

            /*
             * Set the debugging name in the databaseImpl.
             */
            setDebugNameForDatabaseImpl(foundDbImpl, dbNameIfAvailable);

            return foundDbImpl;
        }
    }

    /**
     * Decrements the use count of the given DB, allowing it to be evicted if
     * the use count reaches zero.  Must be called to release a DatabaseImpl
     * that was returned by a method in this class.  See DatabaseImpl.isInUse.
     * [#13415]
     */
    public void releaseDb(DatabaseImpl db) {
        /* Use count is not incremented for idDatabase and nameDatabase. */
        if (db != null &&
            db != idDatabase &&
            db != nameDatabase) {
            db.decrementUseCount();
        }
    }

    /**
     * Calls releaseDb for all DBs in the given map of DatabaseId to
     * DatabaseImpl.  See getDb(DatabaseId, long, Map). [#13415]
     */
    public void releaseDbs(Map<DatabaseId,DatabaseImpl> dbCache) {
        if (dbCache != null) {
            for (DatabaseImpl databaseImpl : dbCache.values()) {
                releaseDb(databaseImpl);
            }
        }
    }

    /*
     * We need to cache a database name in the dbImpl for later use in error
     * messages, when it may be unsafe to walk the mapping tree.  Finding a
     * name by id is slow, so minimize the number of times we must set the
     * debug name.  The debug name will only be uninitialized when an existing
     * databaseImpl is faulted in.
     */
    private void setDebugNameForDatabaseImpl(DatabaseImpl dbImpl,
                                             String dbName)
        throws DatabaseException {

        if (dbImpl != null) {
            if (dbName != null) {
                /* If a name was provided, use that. */
                dbImpl.setDebugDatabaseName(dbName);
            } else {

                /*
                 * Only worry about searching for a name if the name is
                 * uninitialized.  Only search after recovery had finished
                 * setting up the tree.
                 *
                 * Only do name lookup if it will be fairly fast.  Debugging
                 * info isn't important enough to cause long lookups during log
                 * cleaning, for example.  [#21015]
                 */
                if (envImpl.isValid() &&
                    !dbImpl.isDebugNameAvailable() &&
                    getFastNameLookup()) {
                    dbImpl.setDebugDatabaseName(getDbName(dbImpl.getId()));
                }
            }
        }
    }

    /**
     * Rebuild the IN list after recovery.
     */
    public void rebuildINListMapDb()
        throws DatabaseException {

        idDatabase.getTree().rebuildINList();
    }

    /**
     * Returns true if the naming DB has a fairly small number of names, and
     * therefore execution of getDbName will be fairly fast.
     */
    private boolean getFastNameLookup() {
        return nameDatabase.getTree().getMaxLNs() <= FAST_NAME_LOOKUP_MAX_LNS;
    }

    /**
     * Return the database name for a given db. Slow, must traverse. Called by
     * Database.getName.
     *
     * Do not evict (do not call CursorImpl.setAllowEviction(true)) during low
     * level DbTree operation. [#15176]
     */
    public String getDbName(final DatabaseId id)
        throws DatabaseException {

        if (id.equals(ID_DB_ID)) {
            return DbType.ID.getInternalName();
        } else if (id.equals(NAME_DB_ID)) {
            return DbType.NAME.getInternalName();
        }

        class Traversal implements CursorImpl.WithCursor {
            String name = null;

            public boolean withCursor(CursorImpl cursor,
                                      DatabaseEntry key,
                                      @SuppressWarnings("unused")
                                      DatabaseEntry data)
                throws DatabaseException {

                NameLN nameLN = (NameLN) cursor.lockAndGetCurrentLN(
                    LockType.NONE);

                if (nameLN != null && nameLN.getId().equals(id)) {
                    name = StringUtils.fromUTF8(key.getData());
                    return false;
                }
                return true;
            }
        }

        Traversal traversal = new Traversal();

        CursorImpl.traverseDbWithCursor(
            nameDatabase, LockType.NONE, false /*allowEviction*/, traversal);

        return traversal.name;
    }

    /**
     * @return a map of database ids to database names (Strings).
     */
    public Map<DatabaseId,String> getDbNamesAndIds()
        throws DatabaseException {

        final Map<DatabaseId,String> nameMap =
            new HashMap<DatabaseId,String>();

        class Traversal implements CursorImpl.WithCursor {
            public boolean withCursor(CursorImpl cursor,
                                      DatabaseEntry key,
                                      @SuppressWarnings("unused")
                                      DatabaseEntry data)
                throws DatabaseException {

                NameLN nameLN = (NameLN) cursor.lockAndGetCurrentLN(
                    LockType.NONE);
                DatabaseId id = nameLN.getId();
                nameMap.put(id, StringUtils.fromUTF8(key.getData()));
                return true;
            }
        }
        Traversal traversal = new Traversal();
        CursorImpl.traverseDbWithCursor
            (nameDatabase, LockType.NONE, false /*allowEviction*/, traversal);
        return nameMap;
    }

    /**
     * @return a list of database names held in the environment, as strings.
     */
    public List<String> getDbNames()
        throws DatabaseException {

        final List<String> nameList = new ArrayList<String>();

        CursorImpl.traverseDbWithCursor(nameDatabase,
                                        LockType.NONE,
                                        true /*allowEviction*/,
                                        new CursorImpl.WithCursor() {
            public boolean withCursor(@SuppressWarnings("unused")
                                      CursorImpl cursor,
                                      DatabaseEntry key,
                                      @SuppressWarnings("unused")
                                      DatabaseEntry data)
                throws DatabaseException {

                String name = StringUtils.fromUTF8(key.getData());
                if (!isReservedDbName(name)) {
                    nameList.add(name);
                }
                return true;
            }
        });

        return nameList;
    }

    /**
     * Returns true if the name is a reserved JE database name.
     */
    public static boolean isReservedDbName(String name) {
        return typeForDbName(name).isInternal();
    }

    /**
     * @return the higest level node for this database.
     */
    public int getHighestLevel(DatabaseImpl dbImpl)
        throws DatabaseException {

        /* The highest level in the map side */
        RootLevel getLevel = new RootLevel(dbImpl);
        dbImpl.getTree().withRootLatchedShared(getLevel);
        return getLevel.getRootLevel();
    }

    boolean isReplicated() {
        return (flags & REPLICATED_BIT) != 0;
    }

    void setIsReplicated() {
        flags |= REPLICATED_BIT;
    }

    /*
     * Return true if this environment is converted from standalone to
     * replicated.
     */
    boolean isRepConverted() {
        return (flags & REP_CONVERTED_BIT) != 0;
    }

    void setIsRepConverted() {
        flags |= REP_CONVERTED_BIT;
    }

    public DatabaseImpl getIdDatabaseImpl() {
        return idDatabase;
    }

    public DatabaseImpl getNameDatabaseImpl() {
        return nameDatabase;
    }

    boolean getDupsConverted() {
        return (flags & DUPS_CONVERTED_BIT) != 0;
    }

    void setDupsConverted() {
        flags |= DUPS_CONVERTED_BIT;
    }

    private boolean getPreserveVLSN() {
        return (flags & PRESERVE_VLSN_BIT) != 0;
    }

    private void setPreserveVLSN() {
        flags |= PRESERVE_VLSN_BIT;
    }

    /**
     * Returns the initial log version at the time the env was created, or -1
     * if the env was created prior to log version 15.
     */
    public int getInitialLogVersion() {
        return initialLogVersion;
    }

    /**
     * Release resources and update memory budget. Should only be called
     * when this dbtree is closed and will never be accessed again.
     */
    public void close() {
        idDatabase.releaseTreeAdminMemory();
        nameDatabase.releaseTreeAdminMemory();
    }

    public long getTreeAdminMemory() {
        return idDatabase.getTreeAdminMemory() +
            nameDatabase.getTreeAdminMemory();
    }

    /*
     * RootLevel lets us fetch the root IN within the root latch.
     */
    private static class RootLevel implements WithRootLatched {
        private final DatabaseImpl db;
        private int rootLevel;

        RootLevel(DatabaseImpl db) {
            this.db = db;
            rootLevel = 0;
        }

        public IN doWork(ChildReference root)
            throws DatabaseException {

            if (root == null) {
                return null;
            }
            IN rootIN = (IN) root.fetchTarget(db, null);
            rootLevel = rootIN.getLevel();
            return null;
        }

        int getRootLevel() {
            return rootLevel;
        }
    }

    /*
     * Logging support
     */

    /**
     * @see Loggable#getLogSize
     */
    public int getLogSize() {
        return
            LogUtils.getLongLogSize() + // lastAllocatedLocalDbId
            LogUtils.getLongLogSize() + // lastAllocatedReplicatedDbId
            idDatabase.getLogSize() +
            nameDatabase.getLogSize() +
            1 + // 1 byte of flags
            LogUtils.getPackedIntLogSize(initialLogVersion);
            //initialLogVersion
    }

    /**
     * This log entry type is configured to perform marshaling (getLogSize and
     * writeToLog) under the write log mutex.  Otherwise, the size could change
     * in between calls to these two methods as the result of utilizaton
     * tracking.
     *
     * @see Loggable#writeToLog
     */
    public void writeToLog(ByteBuffer logBuffer) {

        /*
         * Long format, rather than packed long format, is used for the last
         * allocated DB IDs.  The IDs, and therefore their packed length, can
         * change between the getLogSize and writeToLog calls. Since the root
         * is infrequently logged, the simplest solution is to use fixed size
         * values. [#18540]
         */
        LogUtils.writeLong(logBuffer, lastAllocatedLocalDbId.get());
        LogUtils.writeLong(logBuffer, lastAllocatedReplicatedDbId.get());

        idDatabase.writeToLog(logBuffer);
        nameDatabase.writeToLog(logBuffer);
        logBuffer.put(flags);
        LogUtils.writePackedInt(logBuffer, initialLogVersion);
    }

    /**
     * @see Loggable#readFromLog
     */
    public void readFromLog(ByteBuffer itemBuffer, int entryVersion) {

        if (entryVersion >= 8) {
            lastAllocatedLocalDbId.set(LogUtils.readLong(itemBuffer));
            lastAllocatedReplicatedDbId.set(LogUtils.readLong(itemBuffer));
        } else {
            lastAllocatedLocalDbId.set(LogUtils.readInt(itemBuffer));
            if (entryVersion >= 6) {
                lastAllocatedReplicatedDbId.set(LogUtils.readInt(itemBuffer));
            }
        }

        idDatabase.readFromLog(itemBuffer, entryVersion); // id db
        nameDatabase.readFromLog(itemBuffer, entryVersion); // name db

        if (entryVersion >= 6) {
            flags = itemBuffer.get();
        } else {
            flags = 0;
        }

        if (entryVersion >= 15) {
            initialLogVersion = LogUtils.readPackedInt(itemBuffer);
        } else {
            initialLogVersion = -1;
        }
    }

    /**
     * @see Loggable#dumpLog
     */
    public void dumpLog(StringBuilder sb, boolean verbose) {
        sb.append("<dbtree lastLocalDbId = \"");
        sb.append(lastAllocatedLocalDbId);
        sb.append("\" lastReplicatedDbId = \"");
        sb.append(lastAllocatedReplicatedDbId);
        sb.append("\">");
        sb.append("<idDb>");
        idDatabase.dumpLog(sb, verbose);
        sb.append("</idDb><nameDb>");
        nameDatabase.dumpLog(sb, verbose);
        sb.append("</nameDb>");
        sb.append("</dbtree>");
    }

    /**
     * @see Loggable#getTransactionId
     */
    public long getTransactionId() {
        return 0;
    }

    /**
     * @see Loggable#logicalEquals
     * Always return false, this item should never be compared.
     */
    public boolean logicalEquals(@SuppressWarnings("unused") Loggable other) {
        return false;
    }

    /*
     * For unit test support
     */

    String dumpString(int nSpaces) {
        StringBuilder self = new StringBuilder();
        self.append(TreeUtils.indent(nSpaces));
        self.append("<dbTree lastDbId =\"");
        self.append(lastAllocatedLocalDbId);
        self.append("\">");
        self.append('\n');
        self.append(idDatabase.dumpString(nSpaces + 1));
        self.append('\n');
        self.append(nameDatabase.dumpString(nSpaces + 1));
        self.append('\n');
        self.append("</dbtree>");
        return self.toString();
    }

    @Override
    public String toString() {
        return dumpString(0);
    }

    /**
     * For debugging.
     */
    public void dump() {
        idDatabase.getTree().dump();
        nameDatabase.getTree().dump();
    }
}
