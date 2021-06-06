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

package collections.ship.marshal;

import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;

/**
 * MarshalledEntity is implemented by entity (combined key/data) objects and
 * called by {@link SampleViews.MarshalledEntityBinding}.  In this sample,
 * MarshalledEntity is implemented by {@link Part}, {@link Supplier}, and
 * {@link Shipment}.  This interface is package-protected rather than public
 * to hide the marshalling interface from other users of the data objects.
 * Note that a MarshalledEntity must also have a no arguments constructor so
 * that it can be instantiated by the binding.
 *
 * @author Mark Hayes
 */
interface MarshalledEntity {

    /**
     * Extracts the entity's primary key and writes it to the key output.
     */
    void marshalPrimaryKey(TupleOutput keyOutput);

    /**
     * Completes construction of the entity by setting its primary key from the
     * stored primary key.
     */
    void unmarshalPrimaryKey(TupleInput keyInput);

    /**
     * Extracts the entity's index key and writes it to the key output.
     */
    boolean marshalSecondaryKey(String keyName, TupleOutput keyOutput);
}
