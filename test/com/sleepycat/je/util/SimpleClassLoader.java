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

package com.sleepycat.je.util;

import java.io.File;
import java.io.IOException;
import java.io.FileInputStream;

/**
 * Simple ClassLoader to load class files from a given directory.  Does not
 * support jar files or multiple directories.
 */
public class SimpleClassLoader extends ClassLoader {
    
    private final File classPath;

    public SimpleClassLoader(ClassLoader parentLoader, File classPath) {
        super(parentLoader);
        this.classPath = classPath;
    }

    @Override
    public Class findClass(String className)
        throws ClassNotFoundException {

        try {
            final String fileName = className.replace('.', '/') + ".class";
            final File file = new File(classPath, fileName);
            final byte[] data = new byte[(int) file.length()];
            final FileInputStream fis = new FileInputStream(file);
            try {
                fis.read(data);
            } finally {
                fis.close();
            }
            return defineClass(className, data, 0, data.length);
        } catch (IOException e) {
            throw new ClassNotFoundException
                ("Class: " + className + " could not be loaded from: " +
                 classPath, e);
        }
    }
}
