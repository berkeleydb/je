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

/**
 * Record lock modes for read operations. Lock mode parameters may be specified
 * for all operations that retrieve data.
 *
 * <p><strong>Locking Rules</strong></p>
 *
 * <p>Together with {@link CursorConfig}, {@link TransactionConfig} and {@link
 * EnvironmentConfig} settings, lock mode parameters determine how records are
 * locked during read operations.  Record locking is used to enforce the
 * isolation modes that are configured.  Record locking is summarized below for
 * read and write operations.  For more information on isolation levels and
 * transactions, see <a
 * href="{@docRoot}/../TransactionGettingStarted/index.html"
 * target="_top">Writing Transactional Applications</a>.</p>
 *
 * <p>With one exception, a record lock is always acquired when a record is
 * read or written, and a cursor will always hold the lock as long as it is
 * positioned on the record.  The exception is when {@link #READ_UNCOMMITTED}
 * is specified, which allows a record to be read without any locking.</p>
 *
 * <p>Both read (shared) and write (exclusive) locks are used.  Read locks are
 * normally acquired on read ({@code get} method) operations and write locks on
 * write ({@code put} method) operations.  The only exception is that a write
 * lock will be acquired on a read operation if {@link #RMW} is specified.</p>
 *
 * <p>Because read locks are shared, multiple accessors may read the same
 * record.  Because write locks are exclusive, if a record is written by one
 * accessor it may not be read or written by another accessor.  An accessor is
 * either a transaction or a thread (for non-transactional operations).</p>
 *
 * <p>Whether additional locking is performed and how locks are released depend
 * on whether the operation is transactional and other configuration
 * settings.</p>
 *
 * <p><strong>Transactional Locking</strong></p>
 *
 * <p>Transactional operations include all write operations for a transactional
 * database, and read operations when a non-null {@link Transaction} parameter
 * is passed.  When a null transaction parameter is passed for a write
 * operation for a transactional database, an auto-commit transaction is
 * automatically used.</p>
 *
 * <p>With transactions, read and write locks are normally held until the end
 * of the transaction (commit or abort).  Write locks are always held until the
 * end of the transaction.  However, if {@link #READ_COMMITTED} is configured,
 * then read locks for cursor operations are only held during the operation and
 * while the cursor is positioned on the record.  The read lock is released
 * when the cursor is moved to a different record or closed.  When {@link
 * #READ_COMMITTED} is used for a database (non-cursor) operation, the read
 * lock is released before the method returns.</p>
 *
 * <p>When neither {@link #READ_UNCOMMITTED} nor {@link #READ_COMMITTED} is
 * specified, read and write locking as described above provide Repeatable Read
 * isolation, which is the default transactional isolation level.  If
 * Serializable isolation is configured, additional "next key" locking is
 * performed to prevent "phantoms" -- records that are not visible at one point
 * in a transaction but that become visible at a later point after being
 * inserted by another transaction.  Serializable isolation is configured via
 * {@link TransactionConfig#setSerializableIsolation} or {@link
 * EnvironmentConfig#setTxnSerializableIsolation}.</p>
 *
 * <p><strong>Non-Transactional Locking</strong></p>
 *
 * <p>Non-transactional operations include all operations for a
 * non-transactional database (including a Deferred Write database), and read
 * operations for a transactional database when a null {@link Transaction}
 * parameter is passed.</p>
 *
 * <p>For non-transactional operations, both read and write locks are only held
 * while a cursor is positioned on the record, and are released when the cursor
 * is moved to a different record or closed.  For database (non-cursor)
 * operations, the read or write lock is released before the method
 * returns.</p>
 * 
 * <p>This behavior is similar to {@link #READ_COMMITTED}, except that both
 * read and write locks are released.  Configuring {@link #READ_COMMITTED} for
 * a non-transactional database cursor has no effect.</p>
 *
 * <p>Because the current thread is the accessor (locker) for non-transactional
 * operations, a single thread may have multiple cursors open without locking
 * conflicts.  Two non-transactional cursors in the same thread may access the
 * same record via write or read operations without conflicts, and the changes
 * make by one cursor will be visible to the other cursor.</p>
 *
 * <p>However, a non-transactional operation will conflict with a transactional
 * operation for the same record even when performed in the same thread.  When
 * using a transaction in a particular thread for a particular database, to
 * avoid conflicts you should use that transaction for all access to that
 * database in that thread.  In other words, to avoid conflicts always pass the
 * transaction parameter, not null, for all operations.  If you don't wish to
 * hold the read lock for the duration of the transaction, specify {@link
 * #READ_COMMITTED}.</p>
 *
 * <p><strong>Read Uncommitted (Dirty-Read)</strong></string></p>
 *
 * <p>When {@link #READ_UNCOMMITTED} is configured, no locking is performed
 * by a read operation.  {@code READ_UNCOMMITTED} does not apply to write
 * operations.</p>
 *
 * <p>{@code READ_UNCOMMITTED} is sometimes called dirty-read because records
 * are visible to the caller in their current state in the Btree at the time of
 * the read, even when that state is due to operations performed using a
 * transaction that has not yet committed.  In addition, because no lock is
 * acquired by the dirty read operation, the record's state may change at any
 * time, even while a cursor used to do the dirty-read is still positioned on
 * the record.</p>
 *
 * <p>To illustrate this, let's say a record is read with dirty-read
 * ({@code READ_UNCOMMITTED}) by calling {@link Cursor#getNext Cursor.getNext}
 * with a cursor C, and changes to the record are also being made in another
 * thread using transaction T. When a locking (non-dirty-read) call to {@link
 * Cursor#getCurrent Cursor.getCurrent} is subsequently made to read the same
 * record again with C at the current position, a result may be returned that
 * is different than the result returned by the earlier call to {@code
 * getNext}. For example:
 * <ul>
 *     <li>If the record is updated by T after the dirty-read {@code getNext}
 *     call, and T is committed, a subsequent call to {@code getCurrent} will
 *     return the data updated by T.</li>
 *
 *     <li>If the record is updated by T before the dirty-read {@code getNext}
 *     call, the {@code getNext} will returned the data updated by T.  But if
 *     call, the {@code getNext} will return the data updated by T.  But if
 *     T is then aborted, a subsequent call to {@code getCurrent} will return
 *     the version of the data before it was updated by T.</li>
 *
 *     <li>If the record was inserted by T before the dirty-read {@code
 *     getNext} call, the {@code getNext} call will return the inserted record.
 *     But if T is aborted, a subsequent call to {@code getCurrent} will return
 *     {@link OperationStatus#KEYEMPTY}.</li>
 *
 *     <li>If the record is deleted by T after the dirty-read {@code getNext}
 *     call, and T is committed, a subsequent call to {@code getCurrent} will
 *     return {@link OperationStatus#KEYEMPTY}.</li>
 * </ul>
 * </p>
 *
 * <p>Note that deleted records are handled specially in JE. Deleted records
 * remain in the Btree until after the deleting transaction is committed, and
 * they are removed from the Btree asynchronously (not immediately at commit
 * time). When using {@code #READ_UNCOMMITTED}, any record encountered in the
 * Btree that was previously deleted, whether or not the deleting transaction
 * has been committed, will be ignored (skipped over) by the read operation.
 * Of course, if the deleting transaction is aborted, the record will no longer
 * be deleted.  If the application is scanning records, for example, this means
 * that such records may be skipped by the scan. If this behavior is not
 * desirable, {@link #READ_UNCOMMITTED_ALL} may be used instead. This mode
 * ensures that records deleted by a transaction that is later aborted will not
 * be skipped by a read operation.  This is accomplished in two different ways
 * depending on the type of database and whether the record's data is requested
 * by the operation.
 * <ol>
 *     <li>If the DB is configured for duplicates or the record's data
 *     is not requested, then a record that has been deleted by an open
 *     transaction is returned by the read operation.</li>
 *
 *     <li>If the DB is not configured for duplicates and the record's data is
 *     requested, then the read operation must wait for the deleting
 *     transaction to close (commit or abort).  After the transaction is
 *     closed, the record will be returned if it is actually not deleted and
 *     otherwise will be skipped.</li>
 * </ol>
 *
 * <p>By "record data" we mean both the {@code data} parameter for a regular or
 * primary DB, and the {@code pKey} parameter for a secondary DB. By "record
 * data requested" we mean that all or part of the {@code DatabaseEntry} will
 * be returned by the read operation.  Unless explicitly <em>not</em>
 * requested, the complete {@code DatabaseEntry} is returned.  See
 * <a href="Cursor.html#partialEntry">Using Partial DatabaseEntry
 * Parameters</a> for more information.</p>
 *
 * <p>Because of this difference in behavior, although {@code
 * #READ_UNCOMMITTED} is fully non-blocking, {@code #READ_UNCOMMITTED_ALL} is
 * not (under the conditions described). As a result, when using {@code
 * #READ_UNCOMMITTED_ALL} under these conditions, a {@link
 * LockConflictException} will be thrown when blocking results in a deadlock or
 * lock timeout.</p>
 *
 * <p>To summarize, callers that use {@code READ_UNCOMMITTED} or {@code
 * #READ_UNCOMMITTED_ALL} should be prepared for the following behaviors.
 * <ul>
 *     <li>After a successful dirty-read operation, because no lock is acquired
 *     the record can be changed by another transaction, even when the cursor
 *     used to perform the dirty-read operation is still positioned on the
 *     record.</li>
 *
 *     <li>After a successful dirty-read operation using a cursor C, say that
 *     another transaction T deletes the record, and T is committed. In this
 *     case, {@link OperationStatus#KEYEMPTY} will be returned by the following
 *     methods if they are called while C is still positioned on the deleted
 *     record: {@link Cursor#getCurrent Cursor.getCurrent}, {@link
 *     Cursor#putCurrent Cursor.putCurrent} and {@link Cursor#delete
 *     Cursor.delete}.</li>
 *
 *     <li>When using {@code READ_UNCOMMITTED}, deleted records will be skipped
 *     even when the deleting transaction is still open. No blocking will occur
 *     and {@link LockConflictException} is never thrown when using this
 *     mode.</li>
 *
 *     <li>When using {@code #READ_UNCOMMITTED_ALL}, deleted records will not
 *     be skipped even when the deleting transaction is open. If the DB is a
 *     duplicates DB or the record's data is not requested, the deleted record
 *     will be returned.  If the DB is not a duplicates DB and the record's
 *     data is requested, blocking will occur until the deleting transaction is
 *     closed. In the latter case, {@link LockConflictException} will be thrown
 *     when this blocking results in a deadlock or a lock timeout.</li>
 * </ul>
 * </p>
 */
public enum LockMode {

    /**
     * Uses the default lock mode and is equivalent to passing {@code null} for
     * the lock mode parameter.
     *
     * <p>The default lock mode is {@link #READ_UNCOMMITTED} when this lock
     * mode is configured via {@link CursorConfig#setReadUncommitted} or {@link
     * TransactionConfig#setReadUncommitted}, or when using a {@link
     * DiskOrderedCursor}.  The Read Uncommitted mode overrides any other
     * configuration settings.</p>
     *
     * <p>Otherwise, the default lock mode is {@link #READ_COMMITTED} when this
     * lock mode is configured via {@link CursorConfig#setReadCommitted} or
     * {@link TransactionConfig#setReadCommitted}.  The Read Committed mode
     * overrides other configuration settings except for {@link
     * #READ_UNCOMMITTED}.</p>
     *
     * <p>Otherwise, the default lock mode is to acquire read locks and release
     * them according to the {@link LockMode default locking rules} for
     * transactional and non-transactional operations.</p>
     */
    DEFAULT,

    /**
     * Reads modified but not yet committed data.
     *
     * <p>The Read Uncommitted mode is used if this lock mode is explicitly
     * passed for the lock mode parameter, or if null or {@link #DEFAULT} is
     * passed and Read Uncommitted is the default -- see {@link #DEFAULT} for
     * details.</p>
     *
     * <p>Unlike {@link #READ_UNCOMMITTED_ALL}, deleted records will be skipped
     * even when the deleting transaction is still open. No blocking will occur
     * and {@link LockConflictException} is never thrown when using this
     * mode.</p>
     *
     * <p>See the {@link LockMode locking rules} for information on how Read
     * Uncommitted impacts transactional and non-transactional locking.</p>
     */
    READ_UNCOMMITTED,

    /**
     * Reads modified but not yet committed data, ensuring that records are not
     * skipped due to transaction aborts.
     *
     * <p>The Read Uncommitted mode is used only when this lock mode is
     * explicitly passed for the lock mode parameter.</p>
     *
     * <p>Unlike {@link #READ_UNCOMMITTED}, deleted records will not be skipped
     * even when the deleting transaction is open. If the DB is a duplicates DB
     * or the record's data is not requested, the deleted record will be
     * returned.  If the DB is not a duplicates DB and the record's data is
     * requested, blocking will occur until the deleting transaction is closed.
     * In the latter case, {@link LockConflictException} will be thrown when
     * this blocking results in a deadlock or a lock timeout.</p>
     *
     * <p>See the {@link LockMode locking rules} for information on how Read
     * Uncommitted impacts transactional and non-transactional locking.</p>
     */
    READ_UNCOMMITTED_ALL,

    /**
     * Read committed isolation provides for cursor stability but not
     * repeatable reads.  Data items which have been previously read by this
     * transaction may be deleted or modified by other transactions before the
     * cursor is closed or the transaction completes.
     *
     * <p>Note that this LockMode may only be passed to {@link Database} get
     * methods, not to {@link Cursor} methods.  To configure a cursor for Read
     * Committed isolation, use {@link CursorConfig#setReadCommitted}.</p>
     *
     * <p>See the {@link LockMode locking rules} for information on how Read
     * Committed impacts transactional and non-transactional locking.</p>
     *
     * @see <a href="EnvironmentStats.html#cacheUnexpectedSizes">Cache
     * Statistics: Unexpected Sizes</a>
     */
    READ_COMMITTED,

    /**
     * Acquire write locks instead of read locks when doing the retrieval.
     *
     * <p>Because it causes a write lock to be acquired, specifying this lock
     * mode as a {@link Cursor} or {@link Database} {@code get} (read) method
     * parameter will override the Read Committed or Read Uncommitted isolation
     * mode that is configured using {@link CursorConfig} or {@link
     * TransactionConfig}.  The write lock will acquired and held until the end
     * of the transaction.  For non-transactional use, the write lock will be
     * released when the cursor is moved to a new position or closed.</p>
     *
     * <p>Setting this flag can eliminate deadlock during a read-modify-write
     * cycle by acquiring the write lock during the read part of the cycle so
     * that another thread of control acquiring a read lock for the same item,
     * in its own read-modify-write cycle, will not result in deadlock.</p>
     */
    RMW;

    private final ReadOptions readOptions;

    LockMode() {
        readOptions = new ReadOptions().setLockMode(this);
    }

    /**
     * Returns a ReadOptions with this LockMode property, and default values
     * for all other properties.
     *
     * <p>WARNING: Do not modify the returned object, since it is a singleton.
     *
     * @since 7.0
     */
    public ReadOptions toReadOptions() {
        return readOptions;
    }

    public String toString() {
        return "LockMode." + name();
    }
}
