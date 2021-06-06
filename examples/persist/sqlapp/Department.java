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

package persist.sqlapp;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;
import com.sleepycat.persist.model.SecondaryKey;
import static com.sleepycat.persist.model.Relationship.ONE_TO_ONE;

/**
 * The Department entity class.
 * 
 * @author chao
 */
@Entity
class Department {

    @PrimaryKey
    int departmentId;

    @SecondaryKey(relate = ONE_TO_ONE)
    String departmentName;

    String location;

    public Department(int departmentId,
                      String departmentName,
                      String location) {

        this.departmentId = departmentId;
        this.departmentName = departmentName;
        this.location = location;
    }

        @SuppressWarnings("unused")
    private Department() {} // Needed for deserialization.

    public int getDepartmentId() {
        return departmentId;
    }

    public String getDepartmentName() {
        return departmentName;
    }

    public String getLocation() {
        return location;
    }

    @Override
    public String toString() {
        return this.departmentId + ", " +
               this.departmentName + ", " +
               this.location;
    }
}
