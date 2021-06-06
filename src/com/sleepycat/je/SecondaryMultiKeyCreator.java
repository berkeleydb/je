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

import java.util.Set;

/**
 * The interface implemented for extracting multi-valued secondary keys from
 * primary records.
 *
 * <p>The key creator object is specified by calling {@link
 * SecondaryConfig#setMultiKeyCreator SecondaryConfig.setMultiKeyCreator}. The
 * secondary database configuration is specified when calling {@link
 * Environment#openSecondaryDatabase Environment.openSecondaryDatabase}.</p>
 *
 * <p>For example:</p>
 *
 * <pre>
 *     class MyMultiKeyCreator implements SecondaryMultiKeyCreator {
 *         public void createSecondaryKeys(SecondaryDatabase secondary,
 *                                         DatabaseEntry key,
 *                                         DatabaseEntry data,
 *                                         Set&lt;DatabaseEntry&gt; results) {
 *             //
 *             // DO HERE: Extract the secondary keys from the primary key and
 *             // data.  For each key extracted, create a DatabaseEntry and add
 *             // it to the results set.
 *             //
 *         }
 *     }
 *     ...
 *     SecondaryConfig secConfig = new SecondaryConfig();
 *     secConfig.setMultiKeyCreator(new MyMultiKeyCreator());
 *     // Now pass secConfig to Environment.openSecondaryDatabase
 * </pre>
 *
 * <p>Use this interface when any number of secondary keys may be present in a
 * single primary record, in other words, for many-to-many and one-to-many
 * relationships. When only zero or one secondary key is present (for
 * many-to-one and one-to-one relationships) you may use the {@link
 * SecondaryKeyCreator} interface instead. The table below summarizes how to
 * create all four variations of relationships.</p>
 * <div>
 * <table border="yes">
 *     <tr><th>Relationship</th>
 *         <th>Interface</th>
 *         <th>Duplicates</th>
 *         <th>Example</th>
 *     </tr>
 *     <tr><td>One-to-one</td>
 *         <td>{@link SecondaryKeyCreator}</td>
 *         <td>No</td>
 *         <td>A person record with a unique social security number key.</td>
 *     </tr>
 *     <tr><td>Many-to-one</td>
 *         <td>{@link SecondaryKeyCreator}</td>
 *         <td>Yes</td>
 *         <td>A person record with a non-unique employer key.</td>
 *     </tr>
 *     <tr><td>One-to-many</td>
 *         <td>{@link SecondaryMultiKeyCreator}</td>
 *         <td>No</td>
 *         <td>A person record with multiple unique email address keys.</td>
 *     </tr>
 *     <tr><td>Many-to-many</td>
 *         <td>{@link SecondaryMultiKeyCreator}</td>
 *         <td>Yes</td>
 *         <td>A person record with multiple non-unique organization keys.</td>
 *     </tr>
 * </table>
 *
 * </div>
 *
 * <p>To configure a database for duplicates. pass true to {@link
 * DatabaseConfig#setSortedDuplicates}.</p>
 *
 * <p>Note that <code>SecondaryMultiKeyCreator</code> may also be used for
 * single key secondaries (many-to-one and one-to-one); in this case, at most a
 * single key is added to the results set.
 * <code>SecondaryMultiKeyCreator</code> is only slightly less efficient than
 * {@link SecondaryKeyCreator} in that two or three temporary sets must be
 * created to hold the results. @see SecondaryConfig</p>
 *
 * <p><em>WARNING:</em> Key creator instances are shared by multiple threads
 * and key creator methods are called without any special synchronization.
 * Therefore, key creators must be thread safe.  In general no shared state
 * should be used and any caching of computed values must be done with proper
 * synchronization.</p>
 */
public interface SecondaryMultiKeyCreator {

    /**
     * Creates a secondary key entry, given a primary key and data entry.
     *
     * <p>A secondary key may be derived from the primary key, primary data, or
     * a combination of the primary key and data.  Zero or more secondary keys
     * may be derived from the primary record and returned in the results
     * parameter. To ensure the integrity of a secondary database the key
     * creator method must always return the same results for a given set of
     * input parameters.</p>
     *
     * <p>A {@code RuntimeException} may be thrown by this method if an error
     * occurs attempting to create the secondary key.  This exception will be
     * thrown by the API method currently in progress, for example, a {@link
     * Database#put put} method.  However, this will cause the write operation
     * to be incomplete.  When databases are not configured to be
     * transactional, caution should be used to avoid integrity problems.  See
     * <a href="SecondaryDatabase.html#transactions">Special considerations for
     * using Secondary Databases with and without Transactions</a>.</p>
     *
     * @param secondary the database to which the secondary key will be
     * added. This parameter is passed for informational purposes but is not
     * commonly used.  This parameter is always non-null.
     *
     * @param key the primary key entry.  This parameter must not be modified
     * by this method.  This parameter is always non-null.
     *
     * @param data the primary data entry.  This parameter must not be modified
     * by this method.  If {@link SecondaryConfig#setExtractFromPrimaryKeyOnly}
     * is configured as {@code true}, the {@code data} param may be either null
     * or non-null, and the implementation is expected to ignore it; otherwise,
     * this parameter is always non-null.
     *
     * @param results the set to contain the the secondary key DatabaseEntry
     * objects created by this method.
     */
    public void createSecondaryKeys(SecondaryDatabase secondary,
                                    DatabaseEntry key,
                                    DatabaseEntry data,
                                    Set<DatabaseEntry> results);
}
