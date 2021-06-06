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

package com.sleepycat.je;

import com.sleepycat.util.ConfigBeanInfoBase;

import java.beans.BeanDescriptor;
import java.beans.PropertyDescriptor;

/**
 * @hidden
 * Getter/Setters for JavaBean based tools.
 */
public class DiskOrderedCursorConfigBeanInfo extends ConfigBeanInfoBase {

    @Override
    public BeanDescriptor getBeanDescriptor() {
        return getBdescriptor(DiskOrderedCursorConfig.class);
    }

    @Override
    public PropertyDescriptor[] getPropertyDescriptors() {
        
        /* 
         * setMaxSeedTestHook is only used for unit test, and 
         * setMaxSeedTestHookVoid method is not necessary, so add 
         * "setMaxSeedTestHook" into ignoreMethods list.
         */         
        ignoreMethods.add("setMaxSeedTestHook");
        return getPdescriptor(DiskOrderedCursorConfig.class);
    }
}
