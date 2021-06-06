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

import java.util.Collection;

/**
 * @hidden
 * For internal use only.
 *
 * Provides a way to create an association between primary and secondary
 * databases that is not limited to a one-to-many association.
 * <p>
 * By implementing this interface, a secondary database may be associated with
 * one or more primary databases.  For example, imagine an application that
 * wishes to partition data from a single logical "table" into multiple primary
 * databases to take advantage of the performance benefits of {@link
 * Environment#removeDatabase} for a single partition, while at the same having
 * a single index database for the entire "table".  For read operations via a
 * secondary database, JE reads the primary key from the secondary database and
 * then must read the primary record from the primary database.  The mapping
 * from a primary key to its primary database is performed by the user's
 * implementation of the {@link #getPrimary} method.
 * <p>
 * In addition, a primary database may be divided into subsets of keys where
 * each subset may be efficiently indexed by zero or more secondary databases.
 * This effectively allows multiple logical "tables" per primary database.
 * When a primary record is written, the secondaries for its logical "table"
 * are returned by the user's implementation of {@link #getSecondaries}.
 * During a primary record write, because {@link #getSecondaries} is called to
 * determine the secondary databases, only the key creators/extractors for
 * those secondaries are invoked.  For example, if you have a single primary
 * database with 10 logical "tables", each of which has 5 distinct secondary
 * databases, when a record is written only 5 key creators/extractors will be
 * invoked, not 50.
 * <p>
 * <b>Configuring a SecondaryAssociation</b>
 * <p>
 * When primary and secondary databases are associated using a {@code
 * SecondaryAssociation}, the databases must all be configured with the same
 * {@code SecondaryAssociation} instance.  A common error is to forget to
 * configure the {@code SecondaryAssociation} on the primary database.
 * <p>
 * A {@code SecondaryAssociation} is configured using {@link
 * DatabaseConfig#setSecondaryAssociation} and {@link
 * SecondaryConfig#setSecondaryAssociation} for a primary and secondary
 * database, respectively. When calling {@link
 * Environment#openSecondaryDatabase}, null must be passed for the
 * primaryDatabase parameter when a {@code SecondaryAssociation} is configured.
 * <p>
 * Note that when a {@code SecondaryAssociation} is configured, {@code true}
 * may not be passed to the {@link SecondaryConfig#setAllowPopulate} method.
 * Population of new secondary databases in an existing {@code
 * SecondaryAssociation} is done differently, as described below.
 * <p>
 * <b>Adding and removing secondary associations</b>
 * <p>
 * The state information defining the association between primary and secondary
 * databases is maintained by the application, and made available to JE by
 * implementing the {@code SecondaryAssociation} interface.  The application
 * may add/remove databases to/from the association without any explicit
 * coordination with JE.  However, certain rules need to be followed to
 * maintain data integrity.
 * <p>
 * In the simplest case, there is a fixed (never changing) set of primary and
 * secondary databases and they are added to the association at the time they
 * are created, before writing or reading records.  In this case, since reads
 * and writes do not occur concurrently with changes to the association, no
 * special rules are needed.  For example:
 * <ol>
 *     <li>Open the databases.</li>
 *     <li>Add databases to the association.</li>
 *     <li>Begin writing and reading.</li>
 * </ol>
 * <p>
 * In other cases, primary and secondary databases may be added to an already
 * established association, along with concurrent reads and writes.  The rules
 * for doing so are described below.
 * <p>
 * Adding an empty (newly created) primary database is no more complex than in
 * the simple case above, assuming that writes to the primary database do not
 * proceed until after it has been added to the association.
 * <ol>
 *     <li>Open the new primary database.</li>
 *     <li>Add the primary database to the association such that {@link
 *     #getSecondaries} returns the appropriate list when passed a key in the
 *     new primary database, and {@link #getPrimary} returns the new database
 *     when passed a key it contains.</li>
 *     <li>Begin writing to the new primary database.</li>
 * </ol>
 * <p>
 * Using the procedure above for adding a primary database, records will be
 * indexed in secondary databases as they are added to the primary database,
 * and will be immediately available to secondary index queries.
 * <p>
 * Alternatively, records can be added to the primary database without making
 * them immediately available to secondary index queries by using the following
 * procedure.  Records will be indexed in secondary databases as they are added
 * but will not be returned by secondary queries until after the last step.
 * <ol>
 *     <li>Open the new primary database.</li>
 *     <li>Modify the association such that {@link #getSecondaries} returns the
 *     appropriate list when passed a key in the new primary database, but
 *     {@link #getPrimary} returns null when passed a key it contains.</li>
 *     <li>Write records in the new primary database.</li>
 *     <li>Modify the association such that {@link #getPrimary} returns the new
 *     database when passed a key it contains.</li>
 * </ol>
 * <p>
 * The following procedure should be used for removing an existing (non-empty)
 * primary database from an association.
 * <ol>
 *     <li>Remove the primary database from the association, such that {@link
 *     #getPrimary} returns null for all keys in the primary.</li>
 *     <li>Disable read and write operations on the database and ensure that
 *     all in-progress operations have completed.</li>
 *     <li>At this point the primary database may be closed and removed
 *     (e.g., with {@link Environment#removeDatabase}), if desired.</li>
 *     <li>For each secondary database associated with the removed primary
 *     database, {@link SecondaryDatabase#deleteObsoletePrimaryKeys} should be
 *     called to process all secondary records.</li>
 *     <li>At this point {@link #getPrimary} may throw an exception (rather
 *     than return null) when called for a primary key in the removed database,
 *     if desired.</li>
 * </ol>
 * <p>
 * The following procedure should be used for adding a new (empty) secondary
 * database to an association, and to populate the secondary incrementally
 * from its associated primary database(s).
 * <ol>
 *     <li>Open the secondary database and call {@link
 *     SecondaryDatabase#startIncrementalPopulation}.  The secondary may not be
 *     used (yet) for read operations.</li>
 *     <li>Add the secondary database to the association, such that the
 *     collection returned by {@link #getSecondaries} includes the new
 *     database, for all primary keys that should be indexed.</li>
 *     <li>For each primary database associated with the new secondary
 *     database, {@link Database#populateSecondaries} should be called to
 *     process all primary records.</li>
 *     <li>Call {@link SecondaryDatabase#endIncrementalPopulation}. The
 *     secondary database may now be used for read operations.</li>
 * </ol>
 * <p>
 * The following procedure should be used for removing an existing (non-empty)
 * secondary database from an association.
 * <ol>
 *     <li>Remove the secondary database from the association, such that it is
 *     not included in the collection returned by {@link #getSecondaries}.</li>
 *     <li>Disable read and write operations on the database and ensure that
 *     all in-progress operations have completed.</li>
 *     <li>The secondary database may now be closed and removed, if
 *     desired.</li>
 * </ol>
 * <p>
 * <b>Other Implementation Requirements</b>
 * <p>
 * The implementation must use data structures for storing and accessing
 * association information that provide <em>happens-before</em> semantics.  In
 * other words, when the association is changed, other threads calling the
 * methods in this interface must see the changes as soon as the change is
 * complete.
 * <p>
 * The implementation should use non-blocking data structures to hold
 * association information to avoid blocking on the methods in this interface,
 * which may be frequently called from many threads.
 * <p>
 * The simplest way to meet the above two requirements is to use concurrent
 * structures such as those provided in the java.util.concurrent package, e.g.,
 * ConcurrentHashMap.
 * <p>
 * For an example implementation, see:
 * test/com/sleepycat/je/test/SecondaryAssociationTest.java
 */
public interface SecondaryAssociation {

    /*
     * Implementation note on concurrent access and latching.
     *
     * When adding/removing primary/secondary DBs to an existing association,
     * concurrency issues arise.  This section explains how they are handled,
     * assuming the rules described in the javadoc below are followed when a
     * custom association is configured.
     *
     * There are two cases:
     *
     * 1) A custom association is NOT configured.  An internal association is
     * created that manages the one-many association between a primary and its
     * secondaries.
     *
     * 2) A custom association IS configured.  It is used internally.
     *
     * For case 1, no custom association, DB addition and removal take place in
     * the Environment.openSecondaryDatabase and SecondaryDatabase.close
     * methods, respectively.  These methods, and the changes to the internal
     * association, are protected by acquiring the secondary latch
     * (EnvironmentImpl.getSecondaryAssociationLock) exclusively.  All write
     * operations that use the association -- puts and deletes -- acquire this
     * latch shared.  Therefore, write operations and changes to the
     * association do not take place concurrently.  Read operations via a
     * secondary DB/cursor do not acquire this latch, so they can take place
     * concurrently with changes to the association; however, read operations
     * only call getPrimary and they expect it may return null at any time (the
     * operation ignores the record in that case).  It is impossible to close
     * a primary while it is being read via a secondary, because secondaries
     * must be closed before their associated primary.  (And the application is
     * responsible for finishing reads before closing any DB.)
     *
     * For case 2, a custom association, the secondary latch does not provide
     * protection because modifications to the association take place in the
     * application domain, not in the Environment.openSecondaryDatabase and
     * SecondaryDatabase.close methods.  Instead, protection is provided by the
     * rules described in the javadoc here. Namely:
     *
     *   Primary/secondary DBs are added to the association AFTER opening them
     *   (of course).  The DBs will not be be accessed until they are added to
     *   the association.  
     *
     *   Primary/secondary DBs are removed from the association BEFORE closing
     *   them.  The application is responsible for ensuring they are no longer
     *   accessed before they are closed.
     *
     *   When a primary DB is added, its secondaries will be kept in sync as
     *   soon as writing begins, since it must be added to the association
     *   before writes are allowed.
     *
     *   When a secondary DB is added, the incremental population procedure
     *   ensures that it is fully populated before being accessed.
     */

    /**
     * Returns true if there are no secondary databases in the association.
     *
     * This method is used by JE to optimize for the case where a primary
     * has no secondary databases.  This allows a {@code SecondaryAssociation}
     * to be configured on a primary database even when it has no associated
     * secondaries, with no added overhead, and to allow for the possibility
     * that secondaries will be added later.
     * <p>
     * For example, when a primary database has no secondaries, no internal
     * latching is performed by JE during writes.  JE determines that no
     * secondaries are present by calling {@code isEmpty}, prior to doing the
     * write operation.
     *
     * @throws RuntimeException if an unexpected problem occurs.  The exception
     * will be thrown to the application, which should then take appropriate
     * action.
     */
    boolean isEmpty();

    /**
     * Returns the primary database for the given primary key.  This method
     * is called during read operations on secondary databases that are
     * configured with this {@code SecondaryAssociation}.
     *
     * This method should return null when the primary database has been
     * removed from the association, or when it should not be included in the
     * results for secondary queries.  In this case, the current operation will
     * treat the secondary record as if it does not exist, i.e., it will be
     * skipped over.
     *
     * @throws RuntimeException if an unexpected problem occurs.  The exception
     * will be thrown to the application, which should then take appropriate
     * action.
     */
    Database getPrimary(DatabaseEntry primaryKey);

    /**
     * Returns the secondary databases associated with the given primary key.
     * This method is called during write operations on primary databases that
     * are configured with this {@code SecondaryAssociation}.
     *
     * @throws RuntimeException if an unexpected problem occurs.  The exception
     * will be thrown to the application, which should then take appropriate
     * action.
     */
    Collection<SecondaryDatabase> getSecondaries(DatabaseEntry primaryKey);
}
