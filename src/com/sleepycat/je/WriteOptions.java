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

import static com.sleepycat.je.EnvironmentFailureException.*;

import java.util.concurrent.TimeUnit;

import com.sleepycat.je.dbi.TTL;

/**
 * Options for calling methods that write (insert, update or delete) records.
 *
 * <h3><a name="ttl">Time-To-Live</a></h3>
 *
 * <p>When performing a 'put' operation, a TTL may be specified using
 * {@link #setTTL(int, TimeUnit)} or {@link #setTTL(int)}.</p>
 *
 * <p>By default, the TTL property is zero, meaning there is no automatic
 * expiration. A non-zero TTL may be specified to cause an inserted record
 * to expire. The expiration time may also be changed for an existing
 * record by updating the record and specifying a different TTL, including
 * specifying zero to prevent the record from expiring. However, the TTL of
 * an existing record is updated only if {@link #setUpdateTTL(boolean)} is
 * explicitly set to true. When deleting a record, the TTL parameter is
 * ignored.</p>
 *
 * <p>Records expire on day or hour boundaries, depending on the {@code
 * timeUnit} parameter. At the time of the write operation, the TTL
 * parameter is used to compute the record's expiration time by first
 * converting it from days (or hours) to milliseconds, and then adding it
 * to the current system time. If the resulting expiration time is not
 * evenly divisible by the number of milliseconds in one day (or hour), it
 * is rounded up to the nearest day (or hour).</p>
 *
 * <p>Passing TimeUnit.DAYS, rather than TimeUnit.HOURS, for the timeUnit
 * parameter is recommended to minimize storage requirements (memory
 * and disk). Because the expiration time is stored in the JE Btree
 * internally, when using the TTL feature, the additional memory and disk
 * space required for storing Btree internal nodes (INs) is twice as much
 * when using TimeUnit.HOURS as when using TimeUnit.DAYS. Using
 * TimeUnit.DAYS adds about 5% to the space needed for INs, while
 * TimeUnit.HOURS adds about 10%.</p>
 *
 * <p>Note that JE stores the expiration time of the record and not the
 * original TTL value that was specified. The expiration time of a record
 * is available when reading (or writing) records via {@link
 * OperationResult#getExpirationTime()}.</p>
 *
 * <p>A summary of the behavior of expired records is as follows.</p>
 * <ul>
 *     <li>Space for expired records will be purged in the background by
 *     the JE cleaner, and expired records will be filtered out of queries
 *     even if they have not been purged.<p></li>
 *
 *     <li>Expired records are removed individually: there is no guarantee
 *     that records with the same expiration time will be removed
 *     simultaneously.<p></li>
 *
 *     <li>Records with expiration times support repeatable-read semantics
 *     in most cases, but with some exceptions (described below).<p></li>
 * </ul>
 *
 * <p>A more detailed description is below, including some information on
 * how expired records are handled internally.</p>
 * <ul>
 *     <li>Expired records will be purged in order to reclaim disk space.
 *     This happens in background over time, and there is no guarantee that
 *     the space for a record will be reclaimed at any particular time.
 *     Purging of expired records occurs during the normal JE cleaning
 *     process. The goals of the purging process are:
 *     <ol>
 *         <li>to minimize the cost of purging;</li>
 *         <li>to keep disk utilization below the {@link
 *         EnvironmentConfig#CLEANER_MIN_UTILIZATION} threshold, as usual,
 *         but taking into account expired data; and</li>
 *         <li>to reclaim expired data gradually and avoid spikes in
 *         cleaning on day and hour boundaries.</li>
 *     </ol>
 *
 *     <li>Expired records that have not been purged will be filtered out
 *     of queries and will not be returned to the application. In a
 *     replicated environment, purging and filtering occur independently on
 *     each node. For queries to return consistent results on all nodes,
 *     the system clocks on all nodes must be synchronized.<p></li>
 *
 *     <li>Repeatable-read semantics are supported for records that expire
 *     after being read. If a lock of any kind is held on a record and the
 *     record expires, when accessing it again using the same transaction
 *     or cursor, it will be accessed as if it is not expired. In other
 *     words, locking a record prevents it from expiring, from the
 *     viewpoint of that transaction or cursor. However, there are some
 *     caveats and exceptions to this rule:
 *        <ul>
 *           <li>A lock by one transaction or cursor will not prevent a
 *           record from being seen as expired when accessing it using a
 *           different transaction or cursor.<p></li>
 *
 *           <li>In the unlikely event that the system clock is changed,
 *           locking a record may not guarantee that the record's data has
 *           not been purged, if the data is not read at the time the
 *           record is locked. This is because the record's key and its
 *           data are purged independently. It is possible to lock a record
 *           without reading its data by passing null for the 'data'
 *           parameter. If a record is locked in this manner, and the data
 *           was previously purged because the system clock was changed,
 *           then one of the following may occur, even when using the same
 *           transaction or cursor that was used to lock the record:
 *           <p></li>
 *              <ul>
 *              <li>If the record is read again with a non-null data
 *              parameter, the operation may fail (return null) because the
 *              data cannot be read.<p></li>
 *
 *              <li>If a partial update is attempted (passing a {@link
 *              DatabaseEntry#setPartial(int,int,boolean) partial} 'data'
 *              parameter), the operation may fail (return null)
 *              because the pre-existing data cannot be read.<p></li>
 *              </ul>
 *        </ul>
 *
 *     <li>Even when multiple records have the same expiration time, JE
 *     does not provide a way for them to expire atomically, as could be
 *     done by explicitly deleting multiple records in a single
 *     transaction. This restriction is for performance reasons; if records
 *     could expire atomically, they could not be purged efficiently using
 *     the JE cleaning process. Instead, each record expires individually,
 *     as if each were deleted in a separate transaction. This means that
 *     even when a set of records is inserted or updated atomically, a
 *     query may return some but not not all of the records, when any of
 *     the records expire at a time very close to the time of the query.
 *     This is because the system clock is checked for each record
 *     individually at the time it is read by the query, and because
 *     expired records may be purged by other threads.<p></li>
 *
 *     <li>There are several special cases of the above rule that involve
 *     access to primary and secondary databases. Because a given primary
 *     record and its associated secondary records are normal records in
 *     most respects, this set of records does not expire atomically. For
 *     most read and write operations, JE treats the expiration of any
 *     record in this set as if all records have expired, and in these
 *     cases there is no special behavior to consider. For example:<p>
 *        <ul>
 *            <li>As long as the primary and secondary databases are
 *            transactional, JE ensures that the expiration times of a
 *            given primary record and all its associated secondary records
 *            are the same.<p></li>
 *
 *            <li>When reading a primary record via a secondary key, JE
 *            first reads the secondary record and then the primary. If
 *            either record expires during this process, both records are
 *            treated as expired.<p></li>
 *
 *            <li>When updating or deleting a primary record, JE first
 *            reads the primary record to obtain the secondary keys and
 *            then deletes/updates/inserts the secondary records as
 *            needed. If a secondary record expires during this process,
 *            this will not cause a {@link SecondaryIntegrityException}, as
 *            would normally happen when an expected associated record is
 *            missing.<p></li>
 *
 *            <li>When a primary and/or secondary record expires after
 *            being read, with few exceptions, repeatable-read semantics
 *            are supported as described above, i.e., locks prevent
 *            expiration from the viewpoint of the locking transaction or
 *            cursor. Exceptions to this rule are described below.<p></li>
 *        </ul>
 *
 *     However, there are several cases where such treatment by JE is not
 *     practical, and the user should be aware of special behavior when
 *     primary or secondary records expire. These are not common use cases,
 *     but it is important to be aware of them. In the cases described
 *     below, let us assume a primary database has two associated secondary
 *     databases, and a particular primary record with primary key X has
 *     two secondary records with keys A and B, one in each secondary
 *     database.<p>
 *        <ul>
 *            <li>After a transaction or cursor reads and locks the primary
 *            record via primary key X, reading via primary key X again
 *            with the same transaction or cursor will also be successful
 *            even if the record has expired, i.e., repeatable-read is
 *            supported. However, if the record expires and the same
 *            transaction or cursor attempts to read via key A or B, the
 *            record will not be found. This is because the secondary
 *            records for key A and B were not locked and they expire
 *            independently of the primary record.<p></li>
 *
 *            <li>Similarly, after a transaction or cursor reads and locks
 *            the primary record via secondary key A successfully, reading
 *            via key A again with the same transaction or cursor will also
 *            be successful even if the record has expired. Reading via
 *            primary key X will also be successful, even if the record has
 *            expired, because the primary record was locked. However, if
 *            the record expires and the same transaction or cursor
 *            attempts to read via key B, the record will not be found.
 *            This is because the secondary record for key B was not locked
 *            and it expires independently of the primary record and the
 *            secondary record for key A.<p></li>
 *
 *            <li>When reading via a secondary database, it is possible to
 *            read the only the secondary key and primary key (which are
 *            both contained in the secondary record), but not the primary
 *            record, by passing null for the 'data' parameter. In this
 *            case the primary record is not locked. Therefore, if the
 *            record expires and the same transaction or cursor attempts to
 *            read the primary record (via any secondary key or the primary
 *            key), the record will not be found.</li>
 *
 *            <li>When a record expires, if its database serves as
 *            a {@link SecondaryConfig#setForeignKeyDatabase foreign key
 *            database}, the {@link
 *            SecondaryConfig#setForeignKeyDeleteAction foreign key delete
 *            action} will not be enforced. Therefore, setting a TTL for a
 *            record in a foreign key database is not recommended. The same
 *            is true when using the DPL and a foreign key database is
 *            specified using {@link
 *            com.sleepycat.persist.model.SecondaryKey#relatedEntity()}.
 *            <p></li>
 *        </ul>
 *     <p></li>
 *
 *     <li>When JE detects what may be an internal integrity error, it
 *     tries to determine whether an expired record, rather than a true
 *     integrity error, is the underlying cause. To prevent internal errors
 *     when small changes in the system clock time are made, if a record
 *     has expired within {@link EnvironmentConfig#ENV_TTL_CLOCK_TOLERANCE}
 *     (two hours, by default), JE treats the record as deleted and no
 *     exception is thrown.
 *
 *     <p>When an integrity error does cause an exception to be thrown, the
 *     record's expiration time will be included in the exception message
 *     and this can help to diagnose the problem. This includes the
 *     following exceptions:
 *        <ul>
 *            <li>{@link SecondaryIntegrityException}</li>
 *            <li>{@link EnvironmentFailureException} with
 *            LOG_FILE_NOT_FOUND in the message.</li>
 *        </ul>
 *     </p>
 *
 *     <p>In cases where the clock has been changed by more than one hour
 *     and integrity exceptions occur because of this, it may be possible
 *     to avoid the exceptions by setting the {@link
 *     EnvironmentConfig#ENV_TTL_CLOCK_TOLERANCE} configuration parameter
 *     to a larger value.</p></li>
 * </ul>
 *
 * <p>In order to use the TTL feature in a ReplicatedEnvironment, all nodes
 * must be upgraded to JE 7.0 or later. If one or more nodes in a group
 * uses an earlier version, an IllegalStateException will be thrown when
 * attempting a put operation with a non-zero TTL. Also, once records with
 * a non-zero TTL have been written, a node using an earlier version of JE
 * may not join the group; if this is attempted, the node will fail during
 * open with an EnvironmentFailureException.</p>
 *
 * @since 7.0
 */
public class WriteOptions implements Cloneable {

    private CacheMode cacheMode = null;
    private int ttl = 0;
    private TimeUnit ttlUnit = TimeUnit.DAYS;
    private boolean updateTtl = false;

    /**
     * Constructs a WriteOptions object with default values for all properties.
     */
    public WriteOptions() {
    }

    @Override
    public WriteOptions clone() {
        try {
            return (WriteOptions) super.clone();
        } catch (CloneNotSupportedException e) {
            throw unexpectedException(e);
        }
    }

    /**
     * Sets the {@code CacheMode} to be used for the operation.
     * <p>
     * By default this property is null, meaning that the default specified
     * using {@link Cursor#setCacheMode},
     * {@link DatabaseConfig#setCacheMode} or
     * {@link EnvironmentConfig#setCacheMode} will be used.
     *
     * @param cacheMode is the {@code CacheMode} used for the operation, or
     * null to use the Cursor, Database or Environment default.
     *
     * @return 'this'.
     */
    public WriteOptions setCacheMode(final CacheMode cacheMode) {
        this.cacheMode = cacheMode;
        return this;
    }

    /**
     * Returns the {@code CacheMode} to be used for the operation, or null
     * if the Cursor, Database or Environment default will be used.
     *
     * @see #setCacheMode(CacheMode)
     */
    public CacheMode getCacheMode() {
        return cacheMode;
    }

    /**
     * Sets the Time-To-Live property for a 'put' operation, using
     * {@code TimeUnit.Days} as the TTL unit.
     *
     * @param ttl the number of days after the current time on which
     * the record will automatically expire, or zero for no automatic
     * expiration. May not be negative.
     *
     * @return 'this'.
     *
     * @see <a href="#ttl">Time-To-Live</a>
     */
    public WriteOptions setTTL(final int ttl) {
        this.ttl = ttl;
        this.ttlUnit = TimeUnit.DAYS;
        return this;
    }

    /**
     * Sets the Time-To-Live property for a 'put' operation, using the given
     * {@code TimeUnit}.
     *
     * @param ttl the number of days or hours after the current time on which
     * the record will automatically expire, or zero for no automatic
     * expiration. May not be negative.
     *
     * @param timeUnit is TimeUnit.DAYS or TimeUnit.HOURS. TimeUnit.DAYS is
     * recommended to minimize storage requirements (memory and disk).
     *
     * @return 'this'.
     *
     * @see <a href="#ttl">Time-To-Live</a>
     */
    public WriteOptions setTTL(final int ttl, final TimeUnit timeUnit) {
        this.ttl = ttl;
        this.ttlUnit = timeUnit;
        return this;
    }

    /**
     * Returns the Time-To-Live property for a 'put' operation.
     *
     * @see #setTTL(int)
     */
    public int getTTL() {
        return ttl;
    }

    /**
     * Returns the Time-To-Live time unit for a 'put' operation.
     *
     * @see #setTTL(int, TimeUnit)
     */
    public TimeUnit getTTLUnit() {
        return ttlUnit;
    }

    /**
     * Sets the update-TTL property for a 'put' operation.
     * <p>
     * If this property is true and the operation updates a record, the
     * specified TTL will be used to assign a new expiration time for the
     * record, or to clear the record's expiration time if the specified
     * TTL is zero.
     * <p>
     * If this parameter is false and the operation updates a record, the
     * record's expiration time will not be changed.
     * <p>
     * If the operation inserts a record, this parameter is ignored and the
     * specified TTL is always applied.
     * <p>
     * By default, this property is false.
     *
     * @param updateTtl is whether to assign (or clear) the expiration time
     * when updating an existing record.
     *
     * @return 'this'.
     *
     * @see <a href="#ttl">Time-To-Live</a>
     */
    public WriteOptions setUpdateTTL(final boolean updateTtl) {
        this.updateTtl = updateTtl;
        return this;
    }

    /**
     * Returns the update-TTL property for a 'put' operation.
     *
     * @see #setUpdateTTL(boolean)
     */
    public boolean getUpdateTTL() {
        return updateTtl;
    }

    /**
     * A convenience method to set the TTL based on a given expiration time
     * and the current system time.
     * <p>
     * Given a desired expiration time and {@link TimeUnit} (DAYS or HOURS),
     * sets the TTL to a value that will cause a record to expire at or after
     * the given time, if the record is stored at the current time. The
     * intended use case is to determine the TTL when writing a record and the
     * desired expiration time, rather than the TTL, is known.
     * <p>
     * This method determines the TTL by taking the difference between the
     * current time and the given time, converting it from milliseconds to
     * days (or hours), and rounding up if it is not evenly divisible by
     * the number of milliseconds in one day (or hour).
     * <p>
     * A special use case is when the expiration time was previously obtained
     * from {@link OperationResult#getExpirationTime()}, for example, when
     * performing an export followed by an import. To support this, null can be
     * passed for the timeUnit parameter and the time unit will be determined
     * as follows.
     * <ul>
     *     <li>This method first converts the expiration time to a TTL in
     *     hours, as described above. If the expiration time was obtained by
     *     calling {@link OperationResult#getExpirationTime}, then it will be
     *     evenly divisible by the number of milliseconds in one hour and no
     *     rounding will occur.</li>
     *
     *     <li>If the resulting TTL in hours is an even multiple of 24,
     *     {@code DAYS} is used; otherwise, {@code HOURS} is used. For example,
     *     when performing an import, if the original expiration time was
     *     specified in {@code DAYS}, and obtained by calling
     *     {@link OperationResult#getExpirationTime}, the unit derived by this
     *     method will also be {@code DAYS}.</li>
     * </ul>
     * <p>
     * Note that when a particular time unit is desired, null should not be
     * passed for the timeUnit parameter. Normally {@link TimeUnit#DAYS} is
     * recommended instead of {@link TimeUnit#HOURS}, to minimize storage
     * requirements (memory and disk). When the desired unit is known, the unit
     * should be passed explicitly.
     *
     * @param expirationTime is the desired expiration time in milliseconds
     * (UTC), or zero for no automatic expiration.
     *
     * @param timeUnit is {@link TimeUnit#DAYS} or {@link TimeUnit#HOURS}, or
     * null to derive the time unit as described above.
     *
     * @throws IllegalArgumentException if ttlUnits is not DAYS, HOURS or null.
     *
     * @see <a href="#ttl">Time-To-Live</a>
     */
    public WriteOptions setExpirationTime(final long expirationTime,
                                          TimeUnit timeUnit) {
        if (expirationTime == 0) {
            return setTTL(0, timeUnit);
        }

        final boolean hours;

        if (timeUnit == TimeUnit.DAYS) {
            hours = false;
        } else if (timeUnit == TimeUnit.HOURS) {
            hours = true;
        } else if (timeUnit == null) {
            hours = TTL.isSystemTimeInHours(expirationTime);
            timeUnit = hours ? TimeUnit.HOURS : TimeUnit.DAYS;
        } else {
            throw new IllegalArgumentException(
                "ttlUnits not allowed: " + timeUnit);
        }

        setTTL(
            TTL.systemTimeToExpiration(
                expirationTime - TTL.currentSystemTime(), hours),
            timeUnit);

        return this;
    }
}
