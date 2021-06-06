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

package com.sleepycat.je.utilint;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.reflect.Method;
import java.util.List;

public class JVMSystemUtils {

    public static final boolean ZING_JVM;
    static {
        final String vendor = System.getProperty("java.vendor");
        final String vmName = System.getProperty("java.vm.name");
        /*
         * Check java.vm.name to distinguish Zing from Zulu, as recommended
         * by Azul.
         */
        ZING_JVM = vendor != null && vmName != null &&
            vendor.equals("Azul Systems, Inc.") && vmName.contains("Zing");
    }

    /**
     * Zing will bump the heap up to 1 GB if -Xmx is smaller.
     */
    public static final int MIN_HEAP_MB = ZING_JVM ? 1024 : 0;

    private static final String ZING_MANAGEMENT_FACTORY_CLASS =
        "com.azul.zing.management.ManagementFactory";

    private static final String ZING_ACCESS_ERROR =
        "Could not access Zing management bean." +
            " Make sure -XX:+UseZingMXBeans was specified.";

    private static OperatingSystemMXBean osBean =
        ManagementFactory.getOperatingSystemMXBean();

    private static final String MATCH_FILE_SEPARATOR =
        "\\" + File.separatorChar;

    /*
     * Get the system load average for the last minute.
     *
     * This method is no longer needed and could be removed. It was originally
     * used to perform reflection when we supported Java 5, but from Java 6
     * onward the getSystemLoadAverage method can be called directly. However,
     * it is a commonly used utility method, so we have chosen not to remove
     * it, for now at least.
     */
    public static double getSystemLoad() {
        return osBean.getSystemLoadAverage();
    }

    /**
     * Returns the max amount of memory in the heap available, using an
     * approach that depends on the JVM vendor, OS, etc.
     *
     * May return Long.MAX_VALUE if there is no inherent limit.
     */
    public static long getRuntimeMaxMemory() {

        /* Runtime.maxMemory is unreliable on MacOS Java 1.4.2. */
        if ("Mac OS X".equals(System.getProperty("os.name"))) {
            final String jvmVersion = System.getProperty("java.version");
            if (jvmVersion != null && jvmVersion.startsWith("1.4.2")) {
                return Long.MAX_VALUE; /* Undetermined heap size. */
            }
        }

        /*
         * Runtime.maxMemory is unreliable on Zing. Call
         * MemoryMXBean.getApplicationObjectHeapUsableMemory instead.
         */
        if (ZING_JVM) {
            try {
                final Class<?> factoryClass =
                    Class.forName(ZING_MANAGEMENT_FACTORY_CLASS);

                final Method getBeanMethod =
                    factoryClass.getMethod("getMemoryMXBean");

                final Object memoryBean = getBeanMethod.invoke(null);
                final Class<?> beanClass = memoryBean.getClass();

                final Method getMaxMemoryMethod = beanClass.getMethod(
                    "getApplicationObjectHeapUsableMemory");

                return (Long) getMaxMemoryMethod.invoke(memoryBean);

            } catch (Exception e) {
                throw new IllegalStateException(ZING_ACCESS_ERROR, e);
            }
        }

        /* Standard approach. */
        return Runtime.getRuntime().maxMemory();
    }

    /**
     * Returns the size of the System Zing Memory pool. This is the max memory
     * for all running Zing JVMs.
     */
    public static long getSystemZingMemorySize() {
        try {
            if (!ZING_JVM) {
                throw new IllegalStateException("Only allowed under Zing");
            }

            final Class<?> factoryClass =
                Class.forName(ZING_MANAGEMENT_FACTORY_CLASS);

            final Method getPoolsMethod =
                factoryClass.getMethod("getMemoryPoolMXBeans");

            final java.util.List<?> pools =
                (java.util.List<?>) getPoolsMethod.invoke(null);

            final Class<?> poolClass = pools.get(0).getClass();
            final Method getNameMethod = poolClass.getMethod("getName");
            final Method getSizeMethod = poolClass.getMethod("getCurrentSize");

            for (Object pool : pools) {
                if ("System Zing Memory".equals(getNameMethod.invoke(pool))) {
                    return (Long) getSizeMethod.invoke(pool);
                }
            }

            throw new IllegalStateException(
                "System Zing Memory pool not found");

        } catch (Exception e) {
            throw new IllegalStateException(ZING_ACCESS_ERROR, e);
        }
    }

    /**
     * Appends Zing-specific Java args, should be called before starting a
     * Java process.
     */
    public static void addZingJVMArgs(List<String> command) {
        insertZingJVMArgs(command, command.size());
    }

    /**
     * Insert Zing-specific Java args after the 'java' command, if 'java' is
     * the 0th element.
     */
    public static void insertZingJVMArgs(List<String> command) {
        if (!JVMSystemUtils.ZING_JVM) {
            return;
        }
        String[] prog = command.get(0).split(MATCH_FILE_SEPARATOR);
        if (prog[prog.length - 1].equals("java")) {
            insertZingJVMArgs(command, 1);
        }
    }

    /*
     * -XX:+UseZingMXBeans must be specified when running under Zing.
     */
    private static void insertZingJVMArgs(List<String> command, int insertAt) {
        if (!JVMSystemUtils.ZING_JVM) {
            return;
        }
        command.add(insertAt, "-XX:+UseZingMXBeans");
    }
}
