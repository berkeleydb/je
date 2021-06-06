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

package com.sleepycat.persist.impl;

import java.util.Map;

/**
 * Read-only catalog used by a PersistComparator to return simple formats plus
 * reconstituted enum formats.
 *
 * @author Mark Hayes
 */
class ComparatorCatalog extends SimpleCatalog {

    private final Map<String, Format> formatMap;

    ComparatorCatalog(final ClassLoader classLoader,
                      final Map<String, Format> formatMap) {
        super(classLoader);
        this.formatMap = formatMap;
    }

    public Format getFormat(final String className) {
        if (formatMap != null) {
            final Format f = formatMap.get(className);
            if (f != null) {
                return f;
            }
        }
        return super.getFormat(className);
    }
}
