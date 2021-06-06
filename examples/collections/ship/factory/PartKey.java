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

package collections.ship.factory;

import com.sleepycat.bind.tuple.MarshalledTupleEntry;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;

/**
 * A PartKey serves as the key in the key/data pair for a part entity.
 *
 * <p>In this sample, PartKey is bound to the stored key tuple entry by
 * implementing the MarshalledTupleEntry interface.</p>
 *
 * @author Mark Hayes
 */
public class PartKey implements MarshalledTupleEntry {

    private String number;

    public PartKey(String number) {

        this.number = number;
    }

    public final String getNumber() {

        return number;
    }

    public String toString() {

        return "[PartKey: number=" + number + ']';
    }

    // --- MarshalledTupleEntry implementation ---

    public PartKey() {

        // A no-argument constructor is necessary only to allow the binding to
        // instantiate objects of this class.
    }

    public void marshalEntry(TupleOutput keyOutput) {

        keyOutput.writeString(this.number);
    }

    public void unmarshalEntry(TupleInput keyInput) {

        this.number = keyInput.readString();
    }
}
