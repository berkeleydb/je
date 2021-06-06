/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2016 Oracle and/or its affiliates.  All rights reserved.
 *
 */

/**
 * Print the value of the "sun.boot.class.path" system property, which is the
 * boot classpath for the Oracle Java virtual machine.
 */
public class PrintBootClassPath {
    public static void main(String[] args) {
        System.out.println(System.getProperty("sun.boot.class.path"));
    }
}
