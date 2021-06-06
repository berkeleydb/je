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
 * Specifies the attributes of database cursor.  An instance created with the
 * default constructor is initialized with the system's default settings.
 */
public class CursorConfig implements Cloneable {

    /**
     * Default configuration used if null is passed to methods that create a
     * cursor.
     */
    public static final CursorConfig DEFAULT = new CursorConfig();

    /**
     * A convenience instance to configure read operations performed by the
     * cursor to return modified but not yet committed data.
     */
    public static final CursorConfig READ_UNCOMMITTED = new CursorConfig();

    /**
     * A convenience instance to configure a cursor for read committed
     * isolation.
     *
     * This ensures the stability of the current data item read by the cursor
     * but permits data read by this cursor to be modified or deleted prior to
     * the commit of the transaction.
     */
    public static final CursorConfig READ_COMMITTED = new CursorConfig();

    static {
        READ_UNCOMMITTED.setReadUncommitted(true);
        READ_COMMITTED.setReadCommitted(true);
    }

    private boolean readUncommitted = false;
    private boolean readCommitted = false;
    private boolean nonSticky = false;

    /**
     * An instance created using the default constructor is initialized with
     * the system's default settings.
     */
    public CursorConfig() {
    }

    /**
     * Configures read operations performed by the cursor to return modified
     * but not yet committed data.
     *
     * @param readUncommitted If true, configure read operations performed by
     * the cursor to return modified but not yet committed data.
     *
     * @see LockMode#READ_UNCOMMITTED
     *
     * @return this
     */
    public CursorConfig setReadUncommitted(boolean readUncommitted) {
        setReadUncommittedVoid(readUncommitted);
        return this;
    }
    
    /**
     * @hidden
     * The void return setter for use by Bean editors.
     */
    public void setReadUncommittedVoid(boolean readUncommitted) {
        this.readUncommitted = readUncommitted;
    } 

    /**
     * Returns true if read operations performed by the cursor are configured
     * to return modified but not yet committed data.
     *
     * @return true if read operations performed by the cursor are configured
     * to return modified but not yet committed data.
     *
     * @see LockMode#READ_UNCOMMITTED
     */
    public boolean getReadUncommitted() {
        return readUncommitted;
    }

    /**
     * Configures read operations performed by the cursor to obey read
     * committed isolation. Read committed isolation provides for cursor
     * stability but not repeatable reads. Data items which have been
     * previously read by this transaction may be deleted or modified by other
     * transactions before the cursor is closed or the transaction completes.
     *
     * @param readCommitted If true, configure read operations performed by
     * the cursor to obey read committed isolation.
     *
     * @see LockMode#READ_COMMITTED
     *
     * @return this
     */
    public CursorConfig setReadCommitted(boolean readCommitted) {
        setReadCommittedVoid(readCommitted);
        return this;
    }
    
    /**
     * @hidden
     * The void return setter for use by Bean editors.
     */
    public void setReadCommittedVoid(boolean readCommitted) {
        this.readCommitted = readCommitted;
    }

    /**
     * Returns true if read operations performed by the cursor are configured
     * to obey read committed isolation.
     *
     * @return true if read operations performed by the cursor are configured
     * to obey read committed isolation.
     *
     * @see LockMode#READ_COMMITTED
     */
    public boolean getReadCommitted() {
        return readCommitted;
    }

    /**
     * Configures the behavior of the cursor when a cursor movement operation
     * returns {@link OperationStatus#NOTFOUND}.
     *
     * By default, a cursor is "sticky", meaning that the prior position is
     * maintained by cursor movement operations, and the cursor stays at the
     * prior position when the operation does not succeed. For example, if
     * {@link Cursor#getFirst} is called successfully, and then
     * {@link Cursor#getNext} is called, if {@code getNext} returns
     * {@code NOTFOUND} the cursor will remain positioned on the first record.
     * <p>
     * Also, as part of maintaining the prior position, the lock on the record
     * at the current position will be held (at least) until after a cursor
     * movement operation succeeds and acquires a lock on the record at the new
     * position.  In the example above, a lock on the first record will still
     * be held after {@code getNext} returns {@code NOTFOUND}.
     * <p>
     * If the cursor is configured to be non-sticky, the prior position is
     * not maintained, and this has certain performance advantages:
     * <ul>
     *     <li>
     *     Some processing is avoided because the prior position is not
     *     maintained.
     *     </li>
     *     <li>
     *     The lock on the record at the prior position is released before
     *     acquiring the lock on the record at the new position (when the
     *     cursor movement operation succeeds.) This can help to prevent
     *     deadlocks in certain situations. Namely, if the cursor's isolation
     *     mode allows locks to be released when moving to a new position, then
     *     only one lock at a time will be held by the cursor. Holding multiple
     *     locks at a time can cause deadlocks, when locks are acquired in
     *     different orders by different threads, for example, when one cursor
     *     is scanning forward and another cursor is scanning backward. Note
     *     that this optimization does not apply to repeatable-read or
     *     serializable isolation, since these modes require that locks are
     *     not released by cursor movement operations.
     *     </li>
     * </ul>
     * <p>
     * However, when the cursor is configured as non-sticky and {@code getNext}
     * returns {@code NOTFOUND} in the example above, the cursor position will
     * be uninitialized, as if it had just been opened. Also, the lock on the
     * first record will have been released (except when repeatable-read or
     * serializable isolation is configured.) To move to the first record (and
     * lock it), {@code getFirst} must be called again.
     * <p>
     * Also note that in certain circumstances, internal algorithms require
     * that the prior position is retained, and the operation will behave as if
     * the cursor is sticky. Specifically, these are only the following
     * methods, and only when called on a database with duplicates configured:
     * <ul>
     *     <li>{@link Cursor#putNoOverwrite}</li>
     *     <li>{@link Cursor#getNextDup}}</li>
     *     <li>{@link Cursor#getPrevDup}}</li>
     *     <li>{@link Cursor#getNextNoDup}}</li>
     *     <li>{@link Cursor#getPrevNoDup}}</li>
     * </ul>
     *
     * @param nonSticky if false (the default), the prior position is
     * maintained by cursor movement operations, and the cursor stays at the
     * prior position when {@code NOTFOUND} is returned. If true, the prior
     * position is not maintained, and the cursor is reinitialized when
     * {@code NOTFOUND} is returned.
     *
     * @return this
     */
    public CursorConfig setNonSticky(boolean nonSticky) {
        setNonStickyVoid(nonSticky);
        return this;
    }

    /**
     * @hidden
     * The void return setter for use by Bean editors.
     */
    public void setNonStickyVoid(boolean nonSticky) {
        this.nonSticky = nonSticky;
    }

    /**
     * Returns the non-sticky setting.
     *
     * @see #setNonSticky
     */
    public boolean getNonSticky() {
        return nonSticky;
    }

    /**
     * Returns a copy of this configuration object.
     */
    @Override
    public CursorConfig clone() {
        try {
            return (CursorConfig) super.clone();
        } catch (CloneNotSupportedException willNeverOccur) {
            return null;
        }
    }

    /**
     * Returns the values for each configuration attribute.
     *
     * @return the values for each configuration attribute.
     */
    @Override
    public String toString() {
        return "readUncommitted=" + readUncommitted +
            "\nreadCommitted=" + readCommitted +
            "\n";
    }
}
