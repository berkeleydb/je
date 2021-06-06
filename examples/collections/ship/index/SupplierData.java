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

package collections.ship.index;

import java.io.Serializable;

/**
 * A SupplierData serves as the data in the key/data pair for a supplier
 * entity.
 *
 * <p> In this sample, SupplierData is used both as the storage data for the
 * data as well as the object binding to the data.  Because it is used
 * directly as storage data using serial format, it must be Serializable. </p>
 *
 * @author Mark Hayes
 */
public class SupplierData implements Serializable {

    private String name;
    private int status;
    private String city;

    public SupplierData(String name, int status, String city) {

        this.name = name;
        this.status = status;
        this.city = city;
    }

    public final String getName() {

        return name;
    }

    public final int getStatus() {

        return status;
    }

    public final String getCity() {

        return city;
    }

    public String toString() {

        return "[SupplierData: name=" + name +
            " status=" + status +
            " city=" + city + ']';
    }
}
