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

package collections.ship.sentity;

/**
 * A SupplierKey serves as the key in the key/data pair for a supplier entity.
 *
 * <p> In this sample, SupplierKey is bound to the key's tuple storage entry
 * using a TupleBinding.  Because it is not used directly as storage data, it
 * does not need to be Serializable. </p>
 *
 * @author Mark Hayes
 */
public class SupplierKey {

    private String number;

    public SupplierKey(String number) {

        this.number = number;
    }

    public final String getNumber() {

        return number;
    }

    public String toString() {

        return "[SupplierKey: number=" + number + ']';
    }
}
