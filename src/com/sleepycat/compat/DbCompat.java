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

package com.sleepycat.compat;

import java.util.Comparator;
import java.util.regex.Pattern;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DatabaseExistsException;
import com.sleepycat.je.DatabaseNotFoundException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationResult;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.ReadOptions;
import com.sleepycat.je.SecondaryConfig;
import com.sleepycat.je.SecondaryCursor;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.WriteOptions;

/**
 * A minimal set of BDB DB-JE compatibility constants and static methods, for
 * internal use only.
 *
 * Two versions of this class, with the same public interface but different
 * implementations, are maintained in parallel in the DB and JE source trees.
 * By the use of the constants and methods in this class, along with a script
 * that moves the source code from JE to DB, the source code in certain
 * packages is kept "portable" and is shared by the two products.  The script
 * translates the package names from com.sleepycat.je to com.sleepycat.db, and
 * perform other fix-ups as described further below.
 *
 * The JE directories that contain portable code are:
 *
 *  src/com/sleepycat/bind
 *                   /collections
 *                   /persist
 *                   /util
 *  test/com/sleepycat/bind
 *                    /collections
 *                    /persist
 *                    /util
 *
 * In DB, these sources are stored in the following locations:
 *
 *  Sources:
 *    src/java
 *  Tests:
 *    test/java/compat
 *
 * To keep this source code portable there are additional coding rules, above
 * and beyond the standard rules (such as coding style) for all JE code.
 *
 *  + In general we should try to use the JE/DB public API, since it is usually
 *    the same or similar in both products.  If we use internal APIs, they will
 *    always be different and will require special handling.
 *
 *  + When there are differences between products, the first choice for
 *    handling the difference is to use a DbCompat static method or constant.
 *    This keeps the source code the same for both products (except in this
 *    DbCompat class).
 *
 *  + When JE-only code is needed -- for example, some APIs only exist in JE,
 *    and special handling of JE exceptions is sometimes needed -- the
 *    following special comment syntax can be used to bracket the JE-only code:
 *
 *    <!-- begin JE only -->
 *       JE-only code goes here
 *    <!-- end JE only -->
 *    
 *    This syntax must be used inside of a comment: either inside a javadoc
 *    section as shown above, or inside a single-line comment (space before
 *    last slash is to prevent ending this javadoc comment):
 *
 *    /* <!-- begin JE only --> * /
 *       JE-only code goes here
 *    /* <!-- end JE only --> * /
 *
 *    All lines between the <!-- begin JE only --> and <!-- end JE only -->
 *    lines, and including these lines, will be removed by the script that
 *    transfers code from JE to DB.
 *
 *  + When DB-only code is needed, the code will exist in the JE product but
 *    will never be executed.  For DB-only APIs, we hide the API from the user
 *    with the @hidden javadoc tag.  The @hidden tag is ignored on the DB side.
 *    We do not have a way to remove DB-only code completely from the JE
 *    product, because we do not use a proprocessor for building JE.
 *
 *  + Because DatabaseException (and all subclasses) are checked exceptions in
 *    DB but runtime exceptions in JE, we cannot omit the 'throws' declaration.
 *    Another difference is that DB normally throws DatabaseException for all
 *    errors, while JE has many specific subclasses for specific errors.
 *    Therefore, any method that calls a DB API method (for example,
 *    Database.get or put) will have a "throws DatabaseException" clause.
 *
 *  + Special consideration is needed for the @throws clauses in javadoc. We do
 *    want to javadoc the JE-only exceptions that are thrown, so the @throws
 *    for these exceptions should be inside the "begin/end JE only" brackets.
 *    We also need to document the fact that DB may throw DatabaseException for
 *    almost any method, so we do that with a final @throws clause that looks
 *    like this:
 *
 *    @throws DatabaseException the base class for all BDB exceptions.
 *
 *    This is a compromise.  JE doesn't throw this exception, but we've
 *    described it in a way that still makes some sense for JE, sort of.
 *
 *  + Other special handling can be implemented in the transfer script, which
 *    uses SED.  Entire files can be excluded from the transfer, for example,
 *    the JE-only exception classes.  Name changes can also be made using SED,
 *    for example: s/LockConflictException/DeadlockException/. See the
 *    db/dist/s_je2db script for details.
 */
public class DbCompat {

    /* Capabilities */

    public static final boolean CDB = false;
    public static final boolean JOIN = true;
    public static final boolean NESTED_TRANSACTIONS = false;
    public static final boolean INSERTION_ORDERED_DUPLICATES = false;
    public static final boolean SEPARATE_DATABASE_FILES = false;
    public static final boolean MEMORY_SUBSYSTEM = false;
    public static final boolean LOCK_SUBSYSTEM = false;
    public static final boolean HASH_METHOD = false;
    public static final boolean RECNO_METHOD = false;
    public static final boolean QUEUE_METHOD = false;
    public static final boolean BTREE_RECNUM_METHOD = false;
    public static final boolean OPTIONAL_READ_UNCOMMITTED = false;
    public static final boolean SECONDARIES = true;
    public static boolean TRANSACTION_RUNNER_PRINT_STACK_TRACES = true;
    public static final boolean DATABASE_COUNT = true;
    public static final boolean NEW_JE_EXCEPTIONS = true;
    public static final boolean POPULATE_ENFORCES_CONSTRAINTS = true;

    /**
     * For read-only cursor operations on a replicated node, we must use a
     * transaction to satisfy HA requirements.  However, we use a Durability
     * that avoids consistency checks on the Master, and we use ReadCommitted
     * isolation since that gives the same behavior as a non-transactional
     * cursor: locks are released when the cursor is moved or closed.
     */
    public static final TransactionConfig READ_ONLY_TXN_CONFIG;

    /** Used on JE only, simply to avoid warnings about "if (true) ...". */
    public static final boolean IS_JE = true;

    static {
        READ_ONLY_TXN_CONFIG = new TransactionConfig();
        READ_ONLY_TXN_CONFIG.setDurability(Durability.READ_ONLY_TXN);
        READ_ONLY_TXN_CONFIG.setReadCommitted(true);
    }

    public static boolean getInitializeCache(EnvironmentConfig config) {
        return true;
    }

    public static boolean getInitializeLocking(EnvironmentConfig config) {
        return config.getLocking();
    }

    public static boolean getInitializeCDB(EnvironmentConfig config) {
        return false;
    }

    public static boolean isReplicated(Environment env) {
        return DbInternal.getNonNullEnvImpl(env).isReplicated();
    }

    public static boolean isTypeBtree(DatabaseConfig dbConfig) {
        return true;
    }

    public static boolean isTypeHash(DatabaseConfig dbConfig) {
        return false;
    }

    public static boolean isTypeQueue(DatabaseConfig dbConfig) {
        return false;
    }

    public static boolean isTypeRecno(DatabaseConfig dbConfig) {
        return false;
    }

    public static boolean getBtreeRecordNumbers(DatabaseConfig dbConfig) {
        return false;
    }

    public static boolean getReadUncommitted(DatabaseConfig dbConfig) {
        return true;
    }

    public static boolean getRenumbering(DatabaseConfig dbConfig) {
        return false;
    }

    public static boolean getSortedDuplicates(DatabaseConfig dbConfig) {
        return dbConfig.getSortedDuplicates();
    }

    public static boolean getUnsortedDuplicates(DatabaseConfig dbConfig) {
        return false;
    }

    public static boolean getDeferredWrite(DatabaseConfig dbConfig) {
        return dbConfig.getDeferredWrite();
    }

    // XXX Remove this when DB and JE support CursorConfig.cloneConfig
    public static CursorConfig cloneCursorConfig(CursorConfig config) {
        CursorConfig newConfig = new CursorConfig();
        newConfig.setReadCommitted(config.getReadCommitted());
        newConfig.setReadUncommitted(config.getReadUncommitted());
        return newConfig;
    }

    public static boolean getWriteCursor(CursorConfig config) {
        return false;
    }

    public static void setWriteCursor(CursorConfig config, boolean write) {
        if (write) {
            throw new UnsupportedOperationException();
        }
    }

    public static void setRecordNumber(DatabaseEntry entry, int recNum) {
        throw new UnsupportedOperationException();
    }

    public static int getRecordNumber(DatabaseEntry entry) {
        throw new UnsupportedOperationException();
    }

    public static String getDatabaseFile(Database db) {
        return null;
    }

    public static long getDatabaseCount(Database db)
        throws DatabaseException {

        return db.count();
    }

    public static OperationStatus getCurrentRecordNumber(Cursor cursor,
                                                         DatabaseEntry key,
                                                         LockMode lockMode)
        throws DatabaseException {

        throw new UnsupportedOperationException();
    }

    public static OperationStatus getSearchRecordNumber(Cursor cursor,
                                                        DatabaseEntry key,
                                                        DatabaseEntry data,
                                                        LockMode lockMode)
        throws DatabaseException {

        throw new UnsupportedOperationException();
    }

    public static OperationStatus getSearchRecordNumber(SecondaryCursor cursor,
                                                        DatabaseEntry key,
                                                        DatabaseEntry pKey,
                                                        DatabaseEntry data,
                                                        LockMode lockMode)
        throws DatabaseException {

        throw new UnsupportedOperationException();
    }

    public static OperationStatus putAfter(Cursor cursor,
                                           DatabaseEntry key,
                                           DatabaseEntry data)
        throws DatabaseException {

        throw new UnsupportedOperationException();
    }

    public static OperationStatus putBefore(Cursor cursor,
                                            DatabaseEntry key,
                                            DatabaseEntry data)
        throws DatabaseException {

        throw new UnsupportedOperationException();
    }

    public static OperationStatus append(Database db,
                                         Transaction txn,
                                         DatabaseEntry key,
                                         DatabaseEntry data) {
        throw new UnsupportedOperationException();
    }

    public static Transaction getThreadTransaction(Environment env)
        throws DatabaseException {

        return env.getThreadTransaction();
    }

    public static ClassLoader getClassLoader(Environment env) {
        return DbInternal.getNonNullEnvImpl(env).getClassLoader();
    }

    /* Methods used by the collections tests. */

    public static void setInitializeCache(EnvironmentConfig config,
                                          boolean val) {
        if (!val) {
            throw new UnsupportedOperationException();
        }
    }

    public static void setInitializeLocking(EnvironmentConfig config,
                                            boolean val) {
        if (!val) {
            throw new UnsupportedOperationException();
        }
    }

    public static void setInitializeCDB(EnvironmentConfig config,
                                        boolean val) {
        if (val) {
            throw new UnsupportedOperationException();
        }
    }

    public static void setLockDetectModeOldest(EnvironmentConfig config) {
        /* JE does this by default, since it uses timeouts. */
    }

    public static void setSerializableIsolation(TransactionConfig config,
                                                boolean val) {
        config.setSerializableIsolation(val);
    }

    public static boolean setImportunate(final Transaction txn,
                                         final boolean importunate) {
        final boolean oldVal = DbInternal.getTxn(txn).getImportunate();
        DbInternal.getTxn(txn).setImportunate(importunate);
        return oldVal;
    }

    public static void setBtreeComparator(DatabaseConfig dbConfig,
                                          Comparator<byte[]> comparator) {
        dbConfig.setBtreeComparator(comparator);
    }

    public static void setTypeBtree(DatabaseConfig dbConfig) {
    }

    public static void setTypeHash(DatabaseConfig dbConfig) {
        throw new UnsupportedOperationException();
    }

    public static void setTypeRecno(DatabaseConfig dbConfig) {
        throw new UnsupportedOperationException();
    }

    public static void setTypeQueue(DatabaseConfig dbConfig) {
        throw new UnsupportedOperationException();
    }

    public static void setBtreeRecordNumbers(DatabaseConfig dbConfig,
                                             boolean val) {
        throw new UnsupportedOperationException();
    }

    public static void setReadUncommitted(DatabaseConfig dbConfig,
                                          boolean val) {
    }

    public static void setRenumbering(DatabaseConfig dbConfig,
                                      boolean val) {
        throw new UnsupportedOperationException();
    }

    public static void setSortedDuplicates(DatabaseConfig dbConfig,
                                           boolean val) {
        dbConfig.setSortedDuplicates(val);
    }

    public static void setUnsortedDuplicates(DatabaseConfig dbConfig,
                                             boolean val) {
        if (val) {
            throw new UnsupportedOperationException();
        }
    }

    public static void setDeferredWrite(DatabaseConfig dbConfig, boolean val) {
        dbConfig.setDeferredWrite(val);
    }

    public static void setRecordLength(DatabaseConfig dbConfig, int val) {
        if (val != 0) {
            throw new UnsupportedOperationException();
        }
    }

    public static void setRecordPad(DatabaseConfig dbConfig, int val) {
        throw new UnsupportedOperationException();
    }

    public static boolean databaseExists(Environment env,
                                         String fileName,
                                         String dbName) {
        assert fileName == null;
        return env.getDatabaseNames().contains(dbName);
    }

    /**
     * Returns null if the database is not found (and AllowCreate is false) or
     * already exists (and ExclusiveCreate is true).
     */
    public static Database openDatabase(Environment env,
                                        Transaction txn,
                                        String fileName,
                                        String dbName,
                                        DatabaseConfig config) {
        assert fileName == null;
        try {
            return env.openDatabase(txn, dbName, config);
        } catch (DatabaseNotFoundException e) {
            return null;
        } catch (DatabaseExistsException e) {
            return null;
        }
    }

    /**
     * Returns null if the database is not found (and AllowCreate is false) or
     * already exists (and ExclusiveCreate is true).
     */
    public static SecondaryDatabase
        openSecondaryDatabase(Environment env,
                              Transaction txn,
                              String fileName,
                              String dbName,
                              Database primaryDatabase,
                              SecondaryConfig config) {
        assert fileName == null;
        try {
            return env.openSecondaryDatabase(txn, dbName, primaryDatabase,
                                             config);
        } catch (DatabaseNotFoundException e) {
            return null;
        } catch (DatabaseExistsException e) {
            return null;
        }
    }

    /**
     * Returns false if the database is not found.
     */
    public static boolean truncateDatabase(Environment env,
                                           Transaction txn,
                                           String fileName,
                                           String dbName) {
        assert fileName == null;
        try {
            env.truncateDatabase(txn, dbName, false /*returnCount*/);
            return true;
        } catch (DatabaseNotFoundException e) {
            return false;
        }
    }

    /**
     * Returns false if the database is not found.
     */
    public static boolean removeDatabase(Environment env,
                                         Transaction txn,
                                         String fileName,
                                         String dbName) {
        assert fileName == null;
        try {
            env.removeDatabase(txn, dbName);
            return true;
        } catch (DatabaseNotFoundException e) {
            return false;
        }
    }

    /**
     * Returns false if the database is not found.
     */
    public static boolean renameDatabase(Environment env,
                                         Transaction txn,
                                         String oldFileName,
                                         String oldDbName,
                                         String newFileName,
                                         String newDbName) {
        assert oldFileName == null;
        assert newFileName == null;
        try {
            env.renameDatabase(txn, oldDbName, newDbName);
            return true;
        } catch (DatabaseNotFoundException e) {
            return false;
        }
    }

    /**
     * Fires an assertion if the database is not found (and AllowCreate is
     * false) or already exists (and ExclusiveCreate is true).
     */
    public static Database testOpenDatabase(Environment env,
                                            Transaction txn,
                                            String file,
                                            String name,
                                            DatabaseConfig config) {
        try {
            return env.openDatabase(txn, makeTestDbName(file, name), config);
        } catch (DatabaseNotFoundException e) {
            assert false;
            return null;
        } catch (DatabaseExistsException e) {
            assert false;
            return null;
        }
    }

    /**
     * Fires an assertion if the database is not found (and AllowCreate is
     * false) or already exists (and ExclusiveCreate is true).
     */
    public static SecondaryDatabase
                  testOpenSecondaryDatabase(Environment env,
                                            Transaction txn,
                                            String file,
                                            String name,
                                            Database primary,
                                            SecondaryConfig config) {
        try {
            return env.openSecondaryDatabase(txn, makeTestDbName(file, name),
                                             primary, config);
        } catch (DatabaseNotFoundException e) {
            assert false;
            return null;
        } catch (DatabaseExistsException e) {
            assert false;
            return null;
        }
    }

    private static String makeTestDbName(String file, String name) {
        if (file == null) {
            return name;
        } else {
            if (name != null) {
                return file + '.' + name;
            } else {
                return file;
            }
        }
    }

    public static RuntimeException unexpectedException(Exception cause) {
        return EnvironmentFailureException.unexpectedException(cause);
    }

    public static RuntimeException unexpectedException(String msg, 
                                                       Exception cause) {
        return EnvironmentFailureException.unexpectedException(msg, cause);
    }

    public static RuntimeException unexpectedState(String msg) {
        return EnvironmentFailureException.unexpectedState(msg);
    }

    public static RuntimeException unexpectedState() {
        return EnvironmentFailureException.unexpectedState();
    }

    public static void enableDeadlockDetection(EnvironmentConfig envConfig,
                                               boolean isCDB) {
        // do nothing in JE, deadlock detection is always on
    }

    public static Object getErrorHandler(Environment env)
        throws DatabaseException {

        return null;
    }

    public static void setErrorHandler(Environment env, Object errHandler)
        throws DatabaseException {
    }

    public static void suppressError(Environment env,
                                     final Pattern errPattern)
        throws DatabaseException{
    }

    /*
     * Abstraction for a result, wrapping an OperationResult in JE, or an
     * OperationStatus in either product. The jeResult field
     * and make(OperationResult) method will not appear in DB core, and must be
     * accessed only by "JE only" code.
     */
    public static class OpResult {

        public static final OpResult SUCCESS =
            new OpResult(DbInternal.DEFAULT_RESULT);

        public static final OpResult FAILURE = new OpResult(null);

        public final OperationResult jeResult;

        private OpResult(OperationResult result) {
            jeResult = result;
        }

        public boolean isSuccess() {
            return jeResult != null;
        }

        public OperationStatus status() {
            return isSuccess() ?
                OperationStatus.SUCCESS : OperationStatus.NOTFOUND;
        }

        public static OpResult make(OperationResult result) {
            return (result != null) ? (new OpResult(result)) : FAILURE;
        }

        public static OpResult make(OperationStatus status) {
            return (status == OperationStatus.SUCCESS) ?
                SUCCESS : FAILURE;
        }
    }

    /*
     * Abstraction for read options, wrapping a ReadOptions in JE, or a
     * LockMode in either product. The jeOptions field and make(ReadOptions)
     * method will not appear in DB core, and must be accessed only by
     * "JE only" code.
     */
    public static class OpReadOptions {

        public static final OpReadOptions EMPTY =
            new OpReadOptions(null);

        public final ReadOptions jeOptions;

        private OpReadOptions(ReadOptions options) {
            jeOptions = options;
        }

        public LockMode getLockMode() {
            return (jeOptions != null) ? jeOptions.getLockMode() : null;
        }

        public static OpReadOptions make(ReadOptions options) {
            return (options != null) ?
                new OpReadOptions(options) : EMPTY;
        }

        public static OpReadOptions make(LockMode lockMode) {
            return (lockMode != null) ?
                new OpReadOptions(lockMode.toReadOptions()) : EMPTY;
        }
    }

    /*
     * Abstraction for write options, wrapping a WriteOptions in JE, but always
     * empty in DB core. The jeOptions field and make(WriteOptions) method will
     * not appear in DB core, and must be accessed only by "JE only" code.
     */
    public static class OpWriteOptions {

        public static final OpWriteOptions EMPTY =
            new OpWriteOptions(null);

        public final WriteOptions jeOptions;

        private OpWriteOptions(WriteOptions options) {
            jeOptions = options;
        }

        public static OpWriteOptions make(WriteOptions options) {
            return (options != null) ?
                new OpWriteOptions(options) : EMPTY;
        }
    }
}
