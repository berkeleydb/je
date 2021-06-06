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

package com.sleepycat.je.rep.persist.test;

import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.persist.EntityStore;

public interface AppInterface {
    public void setVersion(final int label);
    public void setInitDuringOpen(final boolean doInit);
    public void open(final ReplicatedEnvironment env);
    public void close();
    public void writeData(final int key);
    public void writeDataA(final int key);
    public void writeDataB(final int key);
    public void writeDataC(final int key);
    public void writeData2(final int key);
    public void readData(final int key);
    public void readDataA(final int key);
    public void readDataB(final int key);
    public void readDataC(final int key);
    public void readData2(final int key);
    public void adopt(AppInterface other);
    public int getVersion();
    public ReplicatedEnvironment getEnv();
    public EntityStore getStore();
    /* For testRefreshBeforeWrite. */
    public void insertNullAnimal();
    public void readNullAnimal();
    public void insertDogAnimal();
    public void readDogAnimal();
    public void insertCatAnimal();
    public void readCatAnimal();
}
