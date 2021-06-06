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
 * A ShipmentKey serves as the key in the key/data pair for a shipment entity.
 *
 * <p> In this sample, ShipmentKey is bound to the stored key tuple entry by
 * implementing the MarshalledKey interface, which is called by {@link
 * SampleViews.MarshalledKeyBinding}. </p>
 *
 * @author Mark Hayes
 */
public class ShipmentKey implements MarshalledKey {

    private String partNumber;
    private String supplierNumber;

    public ShipmentKey(String partNumber, String supplierNumber) {

        this.partNumber = partNumber;
        this.supplierNumber = supplierNumber;
    }

    public final String getPartNumber() {

        return partNumber;
    }

    public final String getSupplierNumber() {

        return supplierNumber;
    }

    public String toString() {

        return "[ShipmentKey: supplier=" + supplierNumber +
                " part=" + partNumber + ']';
    }

    // --- MarshalledKey implementation ---

    ShipmentKey() {

        // A no-argument constructor is necessary only to allow the binding to
        // instantiate objects of this class.
    }

    public void unmarshalKey(TupleInput keyInput) {

        this.partNumber = keyInput.readString();
        this.supplierNumber = keyInput.readString();
    }

    public void marshalKey(TupleOutput keyOutput) {

        keyOutput.writeString(this.partNumber);
        keyOutput.writeString(this.supplierNumber);
    }
}
