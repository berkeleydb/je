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

package collections.ship.tuple;

/**
 * A Part represents the combined key/data pair for a part entity.
 *
 * <p> In this sample, Part is created from the stored key/data entry using a
 * SerialSerialBinding.  See {@link SampleViews.PartBinding} for details.
 * Since this class is not directly used for data storage, it does not need to
 * be Serializable. </p>
 *
 * @author Mark Hayes
 */
public class Part {

    private String number;
    private String name;
    private String color;
    private Weight weight;
    private String city;

    public Part(String number, String name, String color, Weight weight,
                String city) {

        this.number = number;
        this.name = name;
        this.color = color;
        this.weight = weight;
        this.city = city;
    }

    public final String getNumber() {

        return number;
    }

    public final String getName() {

        return name;
    }

    public final String getColor() {

        return color;
    }

    public final Weight getWeight() {

        return weight;
    }

    public final String getCity() {

        return city;
    }

    public String toString() {

        return "[Part: number=" + number +
               " name=" + name +
               " color=" + color +
               " weight=" + weight +
               " city=" + city + ']';
    }
}
