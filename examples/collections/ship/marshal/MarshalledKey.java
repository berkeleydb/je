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
 * MarshalledKey is implemented by key objects and called by {@link
 * SampleViews.MarshalledKeyBinding}.  In this sample, MarshalledKey is
 * implemented by {@link PartKey}, {@link SupplierKey}, and {@link
 * ShipmentKey}.  This interface is package-protected rather than public to
 * hide the marshalling interface from other users of the data objects.  Note
 * that a MarshalledKey must also have a no arguments constructor so
 * that it can be instantiated by the binding.
 *
 * @author Mark Hayes
 */
interface MarshalledKey {

    /**
     * Construct the key tuple entry from the key object.
     */
    void marshalKey(TupleOutput keyOutput);

    /**
     * Construct the key object from the key tuple entry.
     */
    void unmarshalKey(TupleInput keyInput);
}
