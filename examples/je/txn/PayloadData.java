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

package je.txn;

import java.io.Serializable;

public class PayloadData implements Serializable {
    private int oID;
    private String threadName;
    private double doubleData;

    PayloadData(int id, String name, double data) {
        oID = id;
        threadName = name;
        doubleData = data;
    }

    public double getDoubleData() { return doubleData; }
    public int getID() { return oID; }
    public String getThreadName() { return threadName; }
}
