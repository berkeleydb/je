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

package com.sleepycat.je.dbi;

import java.lang.management.ManagementFactory;
import java.lang.reflect.Method;

/**
 * Uses com.sun.management (non-portable) APIs to detect whether compressed
 * oops is actually in effect.  Uses reflection so that isEnabled simply
 * returns null if the com.sun.management classes are not available, rather
 * than causing a class loading error during static initialization, which would
 * prevent the process from running.  For the IBM J9 environment, which doesn't
 * support the MBean, checks the value of a system property for a known string.
 */
class CompressedOopsDetector {
    private static final String HOTSPOT_BEAN_CLASS =
        "com.sun.management.HotSpotDiagnosticMXBean";
    private static final String HOTSPOT_BEAN_NAME =
        "com.sun.management:type=HotSpotDiagnostic";
    private static final String VMOPTION_CLASS =
        "com.sun.management.VMOption";

    /**
     * For IBM J9, it appears that the best way to tell if compressed OOPs are
     * in use is to see if the value of the java.vm.info system property
     * contains this value.
     */
    private static final String IBM_VM_INFO_COMPRESSED_OOPS_SUBSTRING =
        "Compressed References";

    /**
     * @return TRUE or FALSE if the status of compressed oops is known, or null
     * if it is unknown.
     */
    static Boolean isEnabled() {
        try {
            return isEnabledInternal();
        } catch (Throwable e) {
            final String vendor = System.getProperty("java.vendor");
            if ((vendor != null) && vendor.startsWith("IBM")) {
                final String info = System.getProperty("java.vm.info");
                if (info != null) {
                    return info.indexOf(
                        IBM_VM_INFO_COMPRESSED_OOPS_SUBSTRING) != -1;
                }
            }
            return null;
        }
    }

    /* Throws exceptions rather than returning null. */
    private static Boolean isEnabledInternal()
        throws Throwable {

        final Class<?> hotspotMBeanClass = Class.forName(HOTSPOT_BEAN_CLASS);
        final Object hotspotMBean =
            ManagementFactory.newPlatformMXBeanProxy(
                ManagementFactory.getPlatformMBeanServer(),
                HOTSPOT_BEAN_NAME, hotspotMBeanClass);

        /*
         * vmOption is an instance of com.sun.management.VMOption.
         * HotSpotDiagnosticMXBean.getVMOption(String option) returns a
         * VMOption, which has a "String getValue()" method.
         */
        final Method getVMOption =
            hotspotMBeanClass.getMethod("getVMOption", String.class);
        final Object vmOption =
            getVMOption.invoke(hotspotMBean, "UseCompressedOops");
        final Class<?> vmOptionClass = Class.forName(VMOPTION_CLASS);
        final Method getValue = vmOptionClass.getMethod("getValue");
        final String value = (String) getValue.invoke(vmOption);
        return Boolean.valueOf(value);
    }

    /* For manual testing. */
    public static void main(final String[] args) {
        try {
            System.out.println("isEnabled(): " + isEnabled());
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }
}
