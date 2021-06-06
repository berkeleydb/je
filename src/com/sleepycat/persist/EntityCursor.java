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

package com.sleepycat.persist;

import java.util.Iterator;

/* <!-- begin JE only --> */
import com.sleepycat.je.CacheMode;
/* <!-- end JE only --> */
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
/* <!-- begin JE only --> */
import com.sleepycat.je.DuplicateDataException;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.Get;
/* <!-- end JE only --> */
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationFailureException;
/* <!-- begin JE only --> */
import com.sleepycat.je.OperationResult;
import com.sleepycat.je.ReadOptions;
/* <!-- end JE only --> */
import com.sleepycat.je.Transaction;
/* <!-- begin JE only --> */
import com.sleepycat.je.WriteOptions;
/* <!-- end JE only --> */
import com.sleepycat.persist.model.Relationship;
import com.sleepycat.persist.model.SecondaryKey;

/**
 * Traverses entity values or key values and allows deleting or updating the
 * entity at the current cursor position.  The value type (V) is either an
 * entity class or a key class, depending on how the cursor was opened.
 *
 * <p>{@code EntityCursor} objects are <em>not</em> thread-safe.  Cursors
 * should be opened, used and closed by a single thread.</p>
 *
 * <p>Cursors are opened using the {@link EntityIndex#keys} and {@link
 * EntityIndex#entities} family of methods.  These methods are available for
 * objects of any class that implements {@link EntityIndex}: {@link
 * PrimaryIndex}, {@link SecondaryIndex}, and the indices returned by {@link
 * SecondaryIndex#keysIndex} and {@link SecondaryIndex#subIndex}.  A {@link
 * ForwardCursor}, which implements a subset of cursor operations, is also
 * available via the {@link EntityJoin#keys} and {@link EntityJoin#entities}
 * methods.</p>
 *
 * <p>Values are always returned by a cursor in key order, where the key is
 * defined by the underlying {@link EntityIndex}.  For example, a cursor on a
 * {@link SecondaryIndex} returns values ordered by secondary key, while an
 * index on a {@link PrimaryIndex} or a {@link SecondaryIndex#subIndex} returns
 * values ordered by primary key.</p>
 *
 * <p><em>WARNING:</em> Cursors must always be closed to prevent resource leaks
 * which could lead to the index becoming unusable or cause an
 * <code>OutOfMemoryError</code>.  To ensure that a cursor is closed in the
 * face of exceptions, call {@link #close} in a finally block.  For example,
 * the following code traverses all Employee entities and closes the cursor
 * whether or not an exception occurs:</p>
 *
 * <pre class="code">
 * {@literal @Entity}
 * class Employee {
 *
 *     {@literal @PrimaryKey}
 *     long id;
 *
 *     {@literal @SecondaryKey(relate=MANY_TO_ONE)}
 *     String department;
 *
 *     String name;
 *
 *     private Employee() {}
 * }
 *
 * EntityStore store = ...
 *
 * {@code PrimaryIndex<Long, Employee>} primaryIndex =
 *     store.getPrimaryIndex(Long.class, Employee.class);
 *
 * {@code EntityCursor<Employee>} cursor = primaryIndex.entities();
 * try {
 *     for (Employee entity = cursor.first();
 *                   entity != null;
 *                   entity = cursor.next()) {
 *         // Do something with the entity...
 *     }
 * } finally {
 *     cursor.close();
 * }</pre>
 *
 * <h3>Initializing the Cursor Position</h3>
 *
 * <p>When it is opened, a cursor is not initially positioned on any value; in
 * other words, it is uninitialized.  Most methods in this interface initialize
 * the cursor position but certain methods, for example, {@link #current} and
 * {@link #delete}, throw {@link IllegalStateException} when called for an
 * uninitialized cursor.</p>
 *
 * <p>Note that the {@link #next} and {@link #prev} methods return the first or
 * last value respectively for an uninitialized cursor.  This allows the loop
 * in the example above to be rewritten as follows:</p>
 *
 * <pre class="code">
 * {@code EntityCursor<Employee>} cursor = primaryIndex.entities();
 * try {
 *     Employee entity;
 *     while ((entity = cursor.next()) != null) {
 *         // Do something with the entity...
 *     }
 * } finally {
 *     cursor.close();
 * }</pre>
 *
 * <h3>Cursors and Iterators</h3>
 *
 * <p>The {@link #iterator} method can be used to return a standard Java {@code
 * Iterator} that returns the same values that the cursor returns.  For
 * example:</p>
 *
 * <pre class="code">
 * {@code EntityCursor<Employee>} cursor = primaryIndex.entities();
 * try {
 *     {@code Iterator<Employee>} i = cursor.iterator();
 *     while (i.hasNext()) {
 *          Employee entity = i.next();
 *         // Do something with the entity...
 *     }
 * } finally {
 *     cursor.close();
 * }</pre>
 *
 * <p>The {@link Iterable} interface is also extended by {@link EntityCursor}
 * to allow using the cursor as the target of a Java "foreach" statement:</p>
 *
 * <pre class="code">
 * {@code EntityCursor<Employee>} cursor = primaryIndex.entities();
 * try {
 *     for (Employee entity : cursor) {
 *         // Do something with the entity...
 *     }
 * } finally {
 *     cursor.close();
 * }</pre>
 *
 * <p>The iterator uses the cursor directly, so any changes to the cursor
 * position impact the iterator and vice versa.  The iterator advances the
 * cursor by calling {@link #next()} when {@link Iterator#hasNext} or {@link
 * Iterator#next} is called.  Because of this interaction, to keep things
 * simple it is best not to mix the use of an {@code EntityCursor}
 * {@code Iterator} with the use of the {@code EntityCursor} traversal methods
 * such as {@link #next()}, for a single {@code EntityCursor} object.</p>
 *
 * <h3>Key Ranges</h3>
 *
 * <p>A key range may be specified when opening the cursor, to restrict the
 * key range of the cursor to a subset of the complete range of keys in the
 * index.  A {@code fromKey} and/or {@code toKey} parameter may be specified
 * when calling {@link EntityIndex#keys(Object,boolean,Object,boolean)} or
 * {@link EntityIndex#entities(Object,boolean,Object,boolean)}.  The key
 * arguments may be specified as inclusive or exclusive values.</p>
 *
 * <p>Whenever a cursor with a key range is moved, the key range bounds will be
 * checked, and the cursor will never be positioned outside the range.  The
 * {@link #first} cursor value is the first existing value in the range, and
 * the {@link #last} cursor value is the last existing value in the range.  For
 * example, the following code traverses Employee entities with keys from 100
 * (inclusive) to 200 (exclusive):</p>
 *
 * <pre class="code">
 * {@code EntityCursor<Employee>} cursor = primaryIndex.entities(100, true, 200, false);
 * try {
 *     for (Employee entity : cursor) {
 *         // Do something with the entity...
 *     }
 * } finally {
 *     cursor.close();
 * }</pre>
 *
 * <h3>Duplicate Keys</h3>
 *
 * <p>When using a cursor for a {@link SecondaryIndex}, the keys in the index
 * may be non-unique (duplicates) if {@link SecondaryKey#relate} is {@link
 * Relationship#MANY_TO_ONE MANY_TO_ONE} or {@link Relationship#MANY_TO_MANY
 * MANY_TO_MANY}.  For example, a {@code MANY_TO_ONE} {@code
 * Employee.department} secondary key is non-unique because there are multiple
 * Employee entities with the same department key value.  The {@link #nextDup},
 * {@link #prevDup}, {@link #nextNoDup} and {@link #prevNoDup} methods may be
 * used to control how non-unique keys are returned by the cursor.</p>
 *
 * <p>{@link #nextDup} and {@link #prevDup} return the next or previous value
 * only if it has the same key as the current value, and null is returned when
 * a different key is encountered.  For example, these methods can be used to
 * return all employees in a given department.</p>
 *
 * <p>{@link #nextNoDup} and {@link #prevNoDup} return the next or previous
 * value with a unique key, skipping over values that have the same key.  For
 * example, these methods can be used to return the first employee in each
 * department.</p>
 *
 * <p>For example, the following code will find the first employee in each
 * department with {@link #nextNoDup} until it finds a department name that
 * matches a particular regular expression.  For each matching department it
 * will find all employees in that department using {@link #nextDup}.</p>
 *
 * <pre class="code">
 * {@code SecondaryIndex<String, Long, Employee>} secondaryIndex =
 *     store.getSecondaryIndex(primaryIndex, String.class, "department");
 *
 * String regex = ...;
 * {@code EntityCursor<Employee>} cursor = secondaryIndex.entities();
 * try {
 *     for (Employee entity = cursor.first();
 *                   entity != null;
 *                   entity = cursor.nextNoDup()) {
 *         if (entity.department.matches(regex)) {
 *             while (entity != null) {
 *                 // Do something with the matching entities...
 *                 entity = cursor.nextDup();
 *             }
 *         }
 *     }
 * } finally {
 *     cursor.close();
 * }</pre>
 *
 * <h3>Updating and Deleting Entities with a Cursor</h3>
 *
 * <p>The {@link #update} and {@link #delete} methods operate on the entity at
 * the current cursor position.  Cursors on any type of index may be used to
 * delete entities.  For example, the following code deletes all employees in
 * departments which have names that match a particular regular expression:</p>
 *
 * <pre class="code">
 * {@code SecondaryIndex<String, Long, Employee>} secondaryIndex =
 *     store.getSecondaryIndex(primaryIndex, String.class, "department");
 *
 * String regex = ...;
 * {@code EntityCursor<Employee>} cursor = secondaryIndex.entities();
 * try {
 *     for (Employee entity = cursor.first();
 *                   entity != null;
 *                   entity = cursor.nextNoDup()) {
 *         if (entity.department.matches(regex)) {
 *             while (entity != null) {
 *                 cursor.delete();
 *                 entity = cursor.nextDup();
 *             }
 *         }
 *     }
 * } finally {
 *     cursor.close();
 * }</pre>
 *
 * <p>Note that the cursor can be moved to the next (or previous) value after
 * deleting the entity at the current position.  This is an important property
 * of cursors, since without it you would not be able to easily delete while
 * processing multiple values with a cursor.  A cursor positioned on a deleted
 * entity is in a special state.  In this state, {@link #current} will return
 * null, {@link #delete} will return false, and {@link #update} will return
 * false.</p>
 *
 * <p>The {@link #update} method is supported only if the value type is an
 * entity class (not a key class) and the underlying index is a {@link
 * PrimaryIndex}; in other words, for a cursor returned by one of the {@link
 * PrimaryIndex#entities} methods.  For example, the following code changes all
 * employee names to uppercase:</p>
 *
 * <pre class="code">
 * {@code EntityCursor<Employee>} cursor = primaryIndex.entities();
 * try {
 *     for (Employee entity = cursor.first();
 *                   entity != null;
 *                   entity = cursor.next()) {
 *         entity.name = entity.name.toUpperCase();
 *         cursor.update(entity);
 *     }
 * } finally {
 *     cursor.close();
 * }</pre>
 *
 * @author Mark Hayes
 */
public interface EntityCursor<V> extends ForwardCursor<V> {

    /**
     * Moves the cursor to the first value and returns it, or returns null if
     * the cursor range is empty.
     *
     * <p>{@link LockMode#DEFAULT} is used implicitly.</p>
     *
     * @return the first value, or null if the cursor range is empty.
     *
     * <!-- begin JE only -->
     * @throws OperationFailureException if one of the <a
     * href="../je/OperationFailureException.html#readFailures">Read Operation
     * Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     * <!-- end JE only -->
     *
     * @throws DatabaseException the base class for all BDB exceptions.
     */
    V first()
        throws DatabaseException;

    /**
     * Moves the cursor to the first value and returns it, or returns null if
     * the cursor range is empty.
     *
     * @param lockMode the lock mode to use for this operation, or null to
     * use {@link LockMode#DEFAULT}.
     *
     * @return the first value, or null if the cursor range is empty.
     *
     * <!-- begin JE only -->
     * @throws OperationFailureException if one of the <a
     * href="../je/OperationFailureException.html#readFailures">Read Operation
     * Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     * <!-- end JE only -->
     *
     * @throws DatabaseException the base class for all BDB exceptions.
     */
    V first(LockMode lockMode)
        throws DatabaseException;

    /**
     * Moves the cursor to the last value and returns it, or returns null if
     * the cursor range is empty.
     *
     * <p>{@link LockMode#DEFAULT} is used implicitly.</p>
     *
     * @return the last value, or null if the cursor range is empty.
     *
     * <!-- begin JE only -->
     * @throws OperationFailureException if one of the <a
     * href="../je/OperationFailureException.html#readFailures">Read Operation
     * Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     * <!-- end JE only -->
     *
     * @throws DatabaseException the base class for all BDB exceptions.
     */
    V last()
        throws DatabaseException;

    /**
     * Moves the cursor to the last value and returns it, or returns null if
     * the cursor range is empty.
     *
     * @param lockMode the lock mode to use for this operation, or null to
     * use {@link LockMode#DEFAULT}.
     *
     * @return the last value, or null if the cursor range is empty.
     *
     * <!-- begin JE only -->
     * @throws OperationFailureException if one of the <a
     * href="../je/OperationFailureException.html#readFailures">Read Operation
     * Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     * <!-- end JE only -->
     *
     * @throws DatabaseException the base class for all BDB exceptions.
     */
    V last(LockMode lockMode)
        throws DatabaseException;

    /**
     * Moves the cursor to the next value and returns it, or returns null
     * if there are no more values in the cursor range.  If the cursor is
     * uninitialized, this method is equivalent to {@link #first}.
     *
     * <p>{@link LockMode#DEFAULT} is used implicitly.</p>
     *
     * @return the next value, or null if there are no more values in the
     * cursor range.
     *
     * <!-- begin JE only -->
     * @throws OperationFailureException if one of the <a
     * href="../je/OperationFailureException.html#readFailures">Read Operation
     * Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     * <!-- end JE only -->
     *
     * @throws DatabaseException the base class for all BDB exceptions.
     */
    V next()
        throws DatabaseException;

    /**
     * Moves the cursor to the next value and returns it, or returns null
     * if there are no more values in the cursor range.  If the cursor is
     * uninitialized, this method is equivalent to {@link #first}.
     *
     * @param lockMode the lock mode to use for this operation, or null to
     * use {@link LockMode#DEFAULT}.
     *
     * @return the next value, or null if there are no more values in the
     * cursor range.
     *
     * <!-- begin JE only -->
     * @throws OperationFailureException if one of the <a
     * href="../je/OperationFailureException.html#readFailures">Read Operation
     * Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     * <!-- end JE only -->
     *
     * @throws DatabaseException the base class for all BDB exceptions.
     */
    V next(LockMode lockMode)
        throws DatabaseException;

    /**
     * Moves the cursor to the next value with the same key (duplicate) and
     * returns it, or returns null if no more values are present for the key at
     * the current position.
     *
     * <p>{@link LockMode#DEFAULT} is used implicitly.</p>
     *
     * @return the next value with the same key, or null if no more values are
     * present for the key at the current position.
     *
     * @throws IllegalStateException if the cursor is uninitialized.
     *
     * <!-- begin JE only -->
     * @throws OperationFailureException if one of the <a
     * href="../je/OperationFailureException.html#readFailures">Read Operation
     * Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     * <!-- end JE only -->
     *
     * @throws DatabaseException the base class for all BDB exceptions.
     */
    V nextDup()
        throws DatabaseException;

    /**
     * Moves the cursor to the next value with the same key (duplicate) and
     * returns it, or returns null if no more values are present for the key at
     * the current position.
     *
     * @param lockMode the lock mode to use for this operation, or null to
     * use {@link LockMode#DEFAULT}.
     *
     * @return the next value with the same key, or null if no more values are
     * present for the key at the current position.
     *
     * @throws IllegalStateException if the cursor is uninitialized.
     *
     * <!-- begin JE only -->
     * @throws OperationFailureException if one of the <a
     * href="../je/OperationFailureException.html#readFailures">Read Operation
     * Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     * <!-- end JE only -->
     *
     * @throws DatabaseException the base class for all BDB exceptions.
     */
    V nextDup(LockMode lockMode)
        throws DatabaseException;

    /**
     * Moves the cursor to the next value with a different key and returns it,
     * or returns null if there are no more unique keys in the cursor range.
     * If the cursor is uninitialized, this method is equivalent to {@link
     * #first}.
     *
     * <p>{@link LockMode#DEFAULT} is used implicitly.</p>
     *
     * @return the next value with a different key, or null if there are no
     * more unique keys in the cursor range.
     *
     * <!-- begin JE only -->
     * @throws OperationFailureException if one of the <a
     * href="../je/OperationFailureException.html#readFailures">Read Operation
     * Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     * <!-- end JE only -->
     *
     * @throws DatabaseException the base class for all BDB exceptions.
     */
    V nextNoDup()
        throws DatabaseException;

    /**
     * Moves the cursor to the next value with a different key and returns it,
     * or returns null if there are no more unique keys in the cursor range.
     * If the cursor is uninitialized, this method is equivalent to {@link
     * #first}.
     *
     * @param lockMode the lock mode to use for this operation, or null to
     * use {@link LockMode#DEFAULT}.
     *
     * @return the next value with a different key, or null if there are no
     * more unique keys in the cursor range.
     *
     * <!-- begin JE only -->
     * @throws OperationFailureException if one of the <a
     * href="../je/OperationFailureException.html#readFailures">Read Operation
     * Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     * <!-- end JE only -->
     *
     * @throws DatabaseException the base class for all BDB exceptions.
     */
    V nextNoDup(LockMode lockMode)
        throws DatabaseException;

    /**
     * Moves the cursor to the previous value and returns it, or returns null
     * if there are no preceding values in the cursor range.  If the cursor is
     * uninitialized, this method is equivalent to {@link #last}.
     *
     * <p>{@link LockMode#DEFAULT} is used implicitly.</p>
     *
     * @return the previous value, or null if there are no preceding values in
     * the cursor range.
     *
     * <!-- begin JE only -->
     * @throws OperationFailureException if one of the <a
     * href="../je/OperationFailureException.html#readFailures">Read Operation
     * Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     * <!-- end JE only -->
     *
     * @throws DatabaseException the base class for all BDB exceptions.
     */
    V prev()
        throws DatabaseException;

    /**
     * Moves the cursor to the previous value and returns it, or returns null
     * if there are no preceding values in the cursor range.  If the cursor is
     * uninitialized, this method is equivalent to {@link #last}.
     *
     * @param lockMode the lock mode to use for this operation, or null to
     * use {@link LockMode#DEFAULT}.
     *
     * @return the previous value, or null if there are no preceding values in
     * the cursor range.
     *
     * <!-- begin JE only -->
     * @throws OperationFailureException if one of the <a
     * href="../je/OperationFailureException.html#readFailures">Read Operation
     * Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     * <!-- end JE only -->
     *
     * @throws DatabaseException the base class for all BDB exceptions.
     */
    V prev(LockMode lockMode)
        throws DatabaseException;

    /**
     * Moves the cursor to the previous value with the same key (duplicate) and
     * returns it, or returns null if no preceding values are present for the
     * key at the current position.
     *
     * <p>{@link LockMode#DEFAULT} is used implicitly.</p>
     *
     * @return the previous value with the same key, or null if no preceding
     * values are present for the key at the current position.
     *
     * @throws IllegalStateException if the cursor is uninitialized.
     *
     * <!-- begin JE only -->
     * @throws OperationFailureException if one of the <a
     * href="../je/OperationFailureException.html#readFailures">Read Operation
     * Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     * <!-- end JE only -->
     *
     * @throws DatabaseException the base class for all BDB exceptions.
     */
    V prevDup()
        throws DatabaseException;

    /**
     * Moves the cursor to the previous value with the same key (duplicate) and
     * returns it, or returns null if no preceding values are present for the
     * key at the current position.
     *
     * @param lockMode the lock mode to use for this operation, or null to
     * use {@link LockMode#DEFAULT}.
     *
     * @return the previous value with the same key, or null if no preceding
     * values are present for the key at the current position.
     *
     * @throws IllegalStateException if the cursor is uninitialized.
     *
     * <!-- begin JE only -->
     * @throws OperationFailureException if one of the <a
     * href="../je/OperationFailureException.html#readFailures">Read Operation
     * Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     * <!-- end JE only -->
     *
     * @throws DatabaseException the base class for all BDB exceptions.
     */
    V prevDup(LockMode lockMode)
        throws DatabaseException;

    /**
     * Moves the cursor to the preceding value with a different key and returns
     * it, or returns null if there are no preceding unique keys in the cursor
     * range.  If the cursor is uninitialized, this method is equivalent to
     * {@link #last}.
     *
     * <p>{@link LockMode#DEFAULT} is used implicitly.</p>
     *
     * @return the previous value with a different key, or null if there are no
     * preceding unique keys in the cursor range.
     *
     * <!-- begin JE only -->
     * @throws OperationFailureException if one of the <a
     * href="../je/OperationFailureException.html#readFailures">Read Operation
     * Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     * <!-- end JE only -->
     *
     * @throws DatabaseException the base class for all BDB exceptions.
     */
    V prevNoDup()
        throws DatabaseException;

    /**
     * Moves the cursor to the preceding value with a different key and returns
     * it, or returns null if there are no preceding unique keys in the cursor
     * range.  If the cursor is uninitialized, this method is equivalent to
     * {@link #last}.
     *
     * @param lockMode the lock mode to use for this operation, or null to
     * use {@link LockMode#DEFAULT}.
     *
     * @return the previous value with a different key, or null if there are no
     * preceding unique keys in the cursor range.
     *
     * <!-- begin JE only -->
     * @throws OperationFailureException if one of the <a
     * href="../je/OperationFailureException.html#readFailures">Read Operation
     * Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     * <!-- end JE only -->
     *
     * @throws DatabaseException the base class for all BDB exceptions.
     */
    V prevNoDup(LockMode lockMode)
        throws DatabaseException;

    /**
     * Returns the value at the cursor position, or null if the value at the
     * cursor position has been deleted.
     *
     * <p>{@link LockMode#DEFAULT} is used implicitly.</p>
     *
     * @return the value at the cursor position, or null if it has been
     * deleted.
     *
     * @throws IllegalStateException if the cursor is uninitialized.
     *
     * <!-- begin JE only -->
     * @throws OperationFailureException if one of the <a
     * href="../je/OperationFailureException.html#readFailures">Read Operation
     * Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     * <!-- end JE only -->
     *
     * @throws DatabaseException the base class for all BDB exceptions.
     */
    V current()
        throws DatabaseException;

    /**
     * Returns the value at the cursor position, or null if the value at the
     * cursor position has been deleted.
     *
     * @param lockMode the lock mode to use for this operation, or null to
     * use {@link LockMode#DEFAULT}.
     *
     * @return the value at the cursor position, or null if it has been
     * deleted.
     *
     * @throws IllegalStateException if the cursor is uninitialized.
     *
     * <!-- begin JE only -->
     * @throws OperationFailureException if one of the <a
     * href="../je/OperationFailureException.html#readFailures">Read Operation
     * Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     * <!-- end JE only -->
     *
     * @throws DatabaseException the base class for all BDB exceptions.
     */
    V current(LockMode lockMode)
        throws DatabaseException;

    /* <!-- begin JE only --> */

    /**
     * Moves the cursor according to the specified {@link Get} type and returns
     * the value at the updated position.
     *
     * <p>The following table lists each allowed operation. Also specified is
     * whether the cursor must be initialized (positioned on a value) before
     * calling this method. See the individual {@link Get} operations for more
     * information.</p>
     *
     * <div><table border="1" summary="">
     * <tr>
     *     <th>Get operation</th>
     *     <th>Description</th>
     *     <th>Cursor position<br/>must be initialized?</th>
     * </tr>
     * <tr>
     *     <td>{@link Get#CURRENT}</td>
     *     <td>Accesses the current value.</td>
     *     <td>yes</td>
     * </tr>
     * <tr>
     *     <td>{@link Get#FIRST}</td>
     *     <td>Finds the first value in the cursor range.</td>
     *     <td>no</td>
     * </tr>
     * <tr>
     *     <td>{@link Get#LAST}</td>
     *     <td>Finds the last value in the cursor range.</td>
     *     <td>no</td>
     * </tr>
     * <tr>
     *     <td>{@link Get#NEXT}</td>
     *     <td>Moves to the next value.</td>
     *     <td>no**</td>
     * </tr>
     * <tr>
     *     <td>{@link Get#NEXT_DUP}</td>
     *     <td>Moves to the next value with the same key.</td>
     *     <td>yes</td>
     * </tr>
     * <tr>
     *     <td>{@link Get#NEXT_NO_DUP}</td>
     *     <td>Moves to the next value with a different key.</td>
     *     <td>no**</td>
     * </tr>
     * <tr>
     *     <td>{@link Get#PREV}</td>
     *     <td>Moves to the previous value.</td>
     *     <td>no**</td>
     * </tr>
     * <tr>
     *     <td>{@link Get#PREV_DUP}</td>
     *     <td>Moves to the previous value with the same key.</td>
     *     <td>yes</td>
     * </tr>
     * <tr>
     *     <td>{@link Get#PREV_NO_DUP}</td>
     *     <td>Moves to the previous value with a different key.</td>
     *     <td>no**</td>
     * </tr>
     * </table></div>
     *
     * <p>** - For these 'next' and 'previous' operations the cursor may be
     * uninitialized, in which case the cursor will be moved to the first or
     * last value in the cursor range, respectively.</p>
     *
     * @param getType the Get operation type. Must be one of the values listed
     * above.
     *
     * @param options the ReadOptions, or null to use default options.
     *
     * @return the EntityResult, including the value at the new cursor
     * position, or null if the requested value is not present in the cursor
     * range.
     *
     * @throws IllegalStateException if the cursor is uninitialized.
     *
     * @throws OperationFailureException if one of the <a
     * href="../je/OperationFailureException.html#readFailures">Read Operation
     * Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws DatabaseException the base class for all BDB exceptions.
     *
     * @since 7.0
     */
    EntityResult<V> get(Get getType, ReadOptions options)
        throws DatabaseException;
    /* <!-- end JE only --> */

    /**
     * Returns the number of values (duplicates) for the key at the cursor
     * position, or returns zero if all values for the key have been deleted.
     * Returns one or zero if the underlying index has unique keys.
     *
     * <!-- begin JE only -->
     * <p>The cost of this method is directly proportional to the number of
     * values.</p>
     * <!-- end JE only -->
     *
     * <p>{@link LockMode#DEFAULT} is used implicitly.</p>
     *
     * @return the number of duplicates, or zero if all values for the current
     * key have been deleted.
     *
     * @throws IllegalStateException if the cursor is uninitialized.
     *
     * <!-- begin JE only -->
     * @throws OperationFailureException if one of the <a
     * href="../je/OperationFailureException.html#readFailures">Read Operation
     * Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     * <!-- end JE only -->
     *
     * @throws DatabaseException the base class for all BDB exceptions.
     */
    int count()
        throws DatabaseException;
  
    /* <!-- begin JE only --> */
    /**
     * Returns a rough estimate of the number of values (duplicates) for the
     * key at the cursor position, or returns zero if all values for the key
     * have been deleted.  Returns one or zero if the underlying index has
     * unique keys.
     *
     * <p>If the underlying index has non-unique keys, a quick estimate of the
     * number of values is computed using information in the Btree.  Because
     * the Btree is unbalanced, in some cases the estimate may be off by a
     * factor of two or more.  The estimate is accurate when the number of
     * records is less than the configured {@link
     * DatabaseConfig#setNodeMaxEntries NodeMaxEntries}.</p>
     *
     * <p>The cost of this method is fixed, rather than being proportional to
     * the number of values.  Because its accuracy is variable, this method
     * should normally be used when accuracy is not required, such as for query
     * optimization, and a fixed cost operation is needed. For example, this
     * method is used internally for determining the index processing order in
     * an {@link EntityJoin}.</p>
     *
     * @return an estimate of the count of the number of data items for the key
     * to which the cursor refers.
     *
     * @throws IllegalStateException if the cursor is uninitialized.
     *
     * @throws OperationFailureException if one of the <a
     * href="../je/OperationFailureException.html#readFailures">Read Operation
     * Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws DatabaseException the base class for all BDB exceptions.
     */
    long countEstimate()
        throws DatabaseException;
    /* <!-- end JE only --> */

    /* <!-- begin JE only --> */
    /* for FUTURE use
     * Moves the cursor forward by a number of values and returns the number
     * moved, or returns zero if there are no values following the cursor
     * position.
     *
     * <p>Without regard to performance, calling this method is equivalent to
     * repeatedly calling {@link #next} with {@link LockMode#READ_UNCOMMITTED}
     * to skip over the desired number of values, and then calling {@link
     * #current} with {@link LockMode#DEFAULT} parameter to return the final
     * value.</p>
     *
     * <p>With regard to performance, this method is optimized to skip over
     * values using a smaller number of Btree operations.  When there is no
     * contention on the bottom internal nodes (BINs) and all BINs are in
     * cache, the number of Btree operations is reduced by roughly two orders
     * of magnitude, where the exact number depends on the {@link
     * com.sleepycat.je.EnvironmentConfig#NODE_MAX_ENTRIES} setting.  When
     * there is contention on BINs or fetching BINs is required, the scan is
     * broken up into smaller operations to avoid blocking other threads for
     * long time periods.</p>
     *
     * @param maxCount the maximum number of values to skip, i.e., the maximum
     * number by which the cursor should be moved; must be greater than
     * zero.
     *
     * @return the number of values skipped, i.e., the number by which the
     * cursor has moved; if zero is returned, the cursor position is
     * unchanged.
     *
     * @throws IllegalStateException if the cursor is uninitialized.
     *
     * @throws OperationFailureException if one of the <a
     * href="OperationFailureException.html#readFailures">Read Operation
     * Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws DatabaseException the base class for all BDB exceptions.
    long skipNext(long maxCount);
     */
    /* <!-- end JE only --> */

    /* <!-- begin JE only --> */
    /* for FUTURE use
     * Moves the cursor forward by a number of values and returns the number
     * moved, or returns zero if there are no values following the cursor
     * position.
     *
     * <p>Without regard to performance, calling this method is equivalent to
     * repeatedly calling {@link #next} with {@link LockMode#READ_UNCOMMITTED}
     * to skip over the desired number of values, and then calling {@link
     * #current} with the {@code lockMode} parameter to return the final
     * value.</p>
     *
     * <p>With regard to performance, this method is optimized to skip over
     * values using a smaller number of Btree operations.  When there is no
     * contention on the bottom internal nodes (BINs) and all BINs are in
     * cache, the number of Btree operations is reduced by roughly two orders
     * of magnitude, where the exact number depends on the {@link
     * com.sleepycat.je.EnvironmentConfig#NODE_MAX_ENTRIES} setting.  When
     * there is contention on BINs or fetching BINs is required, the scan is
     * broken up into smaller operations to avoid blocking other threads for
     * long time periods.</p>
     *
     * @param maxCount the maximum number of values to skip, i.e., the maximum
     * number by which the cursor should be moved; must be greater than
     * zero.
     *
     * @param lockMode the lock mode to use for this operation, or null to
     * use {@link LockMode#DEFAULT}.
     *
     * @return the number of values skipped, i.e., the number by which the
     * cursor has moved; if zero is returned, the cursor position is
     * unchanged.
     *
     * @throws IllegalStateException if the cursor is uninitialized.
     *
     * @throws OperationFailureException if one of the <a
     * href="OperationFailureException.html#readFailures">Read Operation
     * Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws DatabaseException the base class for all BDB exceptions.
    long skipNext(long maxCount, LockMode lockMode);
     */
    /* <!-- end JE only --> */

    /* <!-- begin JE only --> */
    /* for FUTURE use
     * Moves the cursor backward by a number of values and returns the number
     * moved, or returns zero if there are no values following the cursor
     * position.
     *
     * <p>Without regard to performance, calling this method is equivalent to
     * repeatedly calling {@link #prev} with {@link LockMode#READ_UNCOMMITTED}
     * to skip over the desired number of values, and then calling {@link
     * #current} with {@link LockMode#DEFAULT} parameter to return the final
     * value.</p>
     *
     * <p>With regard to performance, this method is optimized to skip over
     * values using a smaller number of Btree operations.  When there is no
     * contention on the bottom internal nodes (BINs) and all BINs are in
     * cache, the number of Btree operations is reduced by roughly two orders
     * of magnitude, where the exact number depends on the {@link
     * com.sleepycat.je.EnvironmentConfig#NODE_MAX_ENTRIES} setting.  When
     * there is contention on BINs or fetching BINs is required, the scan is
     * broken up into smaller operations to avoid blocking other threads for
     * long time periods.</p>
     *
     * @param maxCount the maximum number of values to skip, i.e., the maximum
     * number by which the cursor should be moved; must be greater than
     * zero.
     *
     * @return the number of values skipped, i.e., the number by which the
     * cursor has moved; if zero is returned, the cursor position is
     * unchanged.
     *
     * @throws IllegalStateException if the cursor is uninitialized.
     *
     * @throws OperationFailureException if one of the <a
     * href="OperationFailureException.html#readFailures">Read Operation
     * Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws DatabaseException the base class for all BDB exceptions.
    long skipPrev(long maxCount);
     */
    /* <!-- end JE only --> */

    /* <!-- begin JE only --> */
    /* for FUTURE use
     * Moves the cursor backward by a number of values and returns the number
     * moved, or returns zero if there are no values following the cursor
     * position.
     *
     * <p>Without regard to performance, calling this method is equivalent to
     * repeatedly calling {@link #prev} with {@link LockMode#READ_UNCOMMITTED}
     * to skip over the desired number of values, and then calling {@link
     * #current} with the {@code lockMode} parameter to return the final
     * value.</p>
     *
     * <p>With regard to performance, this method is optimized to skip over
     * values using a smaller number of Btree operations.  When there is no
     * contention on the bottom internal nodes (BINs) and all BINs are in
     * cache, the number of Btree operations is reduced by roughly two orders
     * of magnitude, where the exact number depends on the {@link
     * com.sleepycat.je.EnvironmentConfig#NODE_MAX_ENTRIES} setting.  When
     * there is contention on BINs or fetching BINs is required, the scan is
     * broken up into smaller operations to avoid blocking other threads for
     * long time periods.</p>
     *
     * @param maxCount the maximum number of values to skip, i.e., the maximum
     * number by which the cursor should be moved; must be greater than
     * zero.
     *
     * @param lockMode the lock mode to use for this operation, or null to
     * use {@link LockMode#DEFAULT}.
     *
     * @return the number of values skipped, i.e., the number by which the
     * cursor has moved; if zero is returned, the cursor position is
     * unchanged.
     *
     * @throws IllegalStateException if the cursor is uninitialized.
     *
     * @throws OperationFailureException if one of the <a
     * href="OperationFailureException.html#readFailures">Read Operation
     * Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws DatabaseException the base class for all BDB exceptions.
    long skipPrev(long maxCount, LockMode lockMode);
     */
    /* <!-- end JE only --> */

    /**
     * Returns an iterator over the key range, starting with the value
     * following the current position or at the first value if the cursor is
     * uninitialized.
     *
     * <p>{@link LockMode#DEFAULT} is used implicitly.</p>
     *
     * @return the iterator.
     */
    Iterator<V> iterator();

    /**
     * Returns an iterator over the key range, starting with the value
     * following the current position or at the first value if the cursor is
     * uninitialized.
     *
     * @param lockMode the lock mode to use for all operations performed
     * using the iterator, or null to use {@link LockMode#DEFAULT}.
     *
     * @return the iterator.
     */
    Iterator<V> iterator(LockMode lockMode);

    /**
     * Replaces the entity at the cursor position with the given entity.
     *
     * @param entity the entity to replace the entity at the current position.
     *
     * @return true if successful or false if the entity at the current
     * position was previously deleted.
     *
     * @throws IllegalStateException if the cursor is uninitialized.
     *
     * @throws UnsupportedOperationException if the index is read only or if
     * the value type is not an entity type.
     *
     * <!-- begin JE only -->
     * @throws DuplicateDataException if the old and new data are not equal
     * according to the configured duplicate comparator or default comparator.
     *
     * @throws OperationFailureException if one of the <a
     * href="../je/OperationFailureException.html#writeFailures">Write
     * Operation Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     * <!-- end JE only -->
     *
     * @throws DatabaseException the base class for all BDB exceptions.
     */
    boolean update(V entity)
        throws DatabaseException;

    /* <!-- begin JE only --> */
    /**
     * Replaces the entity at the cursor position with the given entity,
     * using a WriteOptions parameter and returning an OperationResult.
     *
     * @param entity the entity to replace the entity at the current position.
     *
     * @param options the WriteOptions, or null to use default options.
     *
     * @return the OperationResult if successful or null if the entity at the
     * current position was previously deleted.
     *
     * @throws IllegalStateException if the cursor is uninitialized.
     *
     * @throws UnsupportedOperationException if the index is read only or if
     * the value type is not an entity type.
     *
     * @throws DuplicateDataException if the old and new data are not equal
     * according to the configured duplicate comparator or default comparator.
     *
     * @throws OperationFailureException if one of the <a
     * href="../je/OperationFailureException.html#writeFailures">Write
     * Operation Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws DatabaseException the base class for all BDB exceptions.
     *
     * @since 7.0
     */
    OperationResult update(V entity, WriteOptions options)
        throws DatabaseException;
    /* <!-- end JE only --> */

    /**
     * Deletes the entity at the cursor position.
     *
     * @throws IllegalStateException if the cursor is uninitialized.
     *
     * @throws UnsupportedOperationException if the index is read only.
     *
     * @return true if successful or false if the entity at the current
     * position has been deleted.
     *
     * <!-- begin JE only -->
     * @throws OperationFailureException if one of the <a
     * href="../je/OperationFailureException.html#writeFailures">Write
     * Operation Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     * <!-- end JE only -->
     *
     * @throws DatabaseException the base class for all BDB exceptions.
     */
    boolean delete()
        throws DatabaseException;

    /* <!-- begin JE only --> */
    /**
     * Deletes the entity at the cursor position, using a WriteOptions
     * parameter and returning an OperationResult.
     *
     * @throws IllegalStateException if the cursor is uninitialized.
     *
     * @throws UnsupportedOperationException if the index is read only.
     *
     * @return the OperationResult if successful or null if the entity at the
     * current position was previously deleted.
     *
     * @throws OperationFailureException if one of the <a
     * href="../je/OperationFailureException.html#writeFailures">Write
     * Operation Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws DatabaseException the base class for all BDB exceptions.
     *
     * @since 7.0
     */
    OperationResult delete(WriteOptions options)
        throws DatabaseException;
    /* <!-- end JE only --> */

    /**
     * Duplicates the cursor at the cursor position.  The returned cursor will
     * be initially positioned at the same position as this current cursor, and
     * will inherit this cursor's {@link Transaction} and {@link CursorConfig}.
     *
     * @return the duplicated cursor.
     *
     * @throws DatabaseException the base class for all BDB exceptions.
     */
    EntityCursor<V> dup()
        throws DatabaseException;

    /**
     * Closes the cursor.
     *
     * @throws DatabaseException the base class for all BDB exceptions.
     */
    void close()
        throws DatabaseException;

    /* <!-- begin JE only --> */
    /**
     * Changes the {@code CacheMode} default used for subsequent operations
     * performed using this cursor. For a newly opened cursor, the default is
     * {@link CacheMode#DEFAULT}. Note that the default is always overridden by
     * a non-null cache mode that is specified via {@link ReadOptions} or
     * {@link WriteOptions}.
     *
     * @param cacheMode is the default {@code CacheMode} used for subsequent
     * operations using this cursor, or null to configure the Database or
     * Environment default.
     *
     * @see CacheMode
     */
    void setCacheMode(CacheMode cacheMode);
    /* <!-- end JE only --> */

    /* <!-- begin JE only --> */
    /**
     * Returns the default {@code CacheMode} used for subsequent operations
     * performed using this cursor. If {@link #setCacheMode} has not been
     * called with a non-null value, the configured Database or Environment
     * default is returned.
     *
     * @return the {@code CacheMode} default used for subsequent operations
     * using this cursor.
     *
     * @see CacheMode
     */
    CacheMode getCacheMode();
    /* <!-- end JE only --> */
}
