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
 * A tag interface used to mark a B-tree or duplicate comparator class as a
 * partial comparator.
 *
 * Comparators are configured using
 * {@link DatabaseConfig#setBtreeComparator(java.util.Comparator)} or
 * {@link DatabaseConfig#setBtreeComparator(Class)}, and
 * {@link DatabaseConfig#setDuplicateComparator(java.util.Comparator)} or
 * {@link DatabaseConfig#setDuplicateComparator(Class)}.
 * <p>
 * As described in the javadoc for these methods, a partial comparator is a
 * comparator that allows for the keys of a database to be updated, but only
 * if the updates are not significant with respect to uniqueness and ordering.
 * Also described is the fact that comparators must be used with great caution,
 * since a badly behaved comparator can cause B-tree corruption.
 * <p>
 * Even greater caution is needed when using partial comparators, for several
 * reasons.  Partial comparators are normally used for performance reasons in
 * certain situations, but the performance trade-offs are very subtle and
 * difficult to understand.  In addition, as of JE 6, this tag interface must
 * be added to all partial comparator classes so that JE can correctly perform
 * transaction aborts, while maintaining the last committed key or duplicate
 * data values properly.  In addition, for a database with duplicates
 * configured, a partial comparator (implementing this tag interface) will
 * disable optimizations in JE 6 that drastically reduce cleaner costs.
 * <p>
 * For these reasons, we do not recommend using partial comparators, although
 * they are supported in order to avoid breaking applications that used them
 * prior to JE 6.  Whenever possible, please avoid using partial comparators.
 */
public interface PartialComparator {
}
