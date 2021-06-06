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

package collections.ship.entity;

/**
 * A Supplier represents the combined key/data pair for a supplier entity.
 *
 * <p> In this sample, Supplier is created from the stored key/data entry
 * using a SerialSerialBinding.  See {@link SampleViews.SupplierBinding} for
 * details.  Since this class is not used directly for data storage, it does
 * not need to be Serializable. </p>
 *
 * @author Mark Hayes
 */
public class Supplier {

    private String number;
    private String name;
    private int status;
    private String city;

    public Supplier(String number, String name, int status, String city) {

        this.number = number;
        this.name = name;
        this.status = status;
        this.city = city;
    }

    public final String getNumber() {

        return number;
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

        return "[Supplier: number=" + number +
               " name=" + name +
               " status=" + status +
               " city=" + city + ']';
    }
}
