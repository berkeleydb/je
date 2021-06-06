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

package com.sleepycat.bind.tuple;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.je.DatabaseEntry;

/**
 * A concrete <code>EntryBinding</code> that uses the <code>TupleInput</code>
 * object as the key or data object.
 *
 * A concrete tuple binding for key or data entries which are {@link
 * TupleInput} objects.  This binding is used when tuples themselves are the
 * objects, rather than using application defined objects. A {@link TupleInput}
 * must always be used.  To convert a {@link TupleOutput} to a {@link
 * TupleInput}, use the {@link TupleInput#TupleInput(TupleOutput)} constructor.
 *
 * @author Mark Hayes
 */
public class TupleInputBinding implements EntryBinding<TupleInput> {

    /**
     * Creates a tuple input binding.
     */
    public TupleInputBinding() {
    }

    // javadoc is inherited
    public TupleInput entryToObject(DatabaseEntry entry) {

        return TupleBinding.entryToInput(entry);
    }

    // javadoc is inherited
    public void objectToEntry(TupleInput object, DatabaseEntry entry) {

        TupleBinding.inputToEntry(object, entry);
    }
}
