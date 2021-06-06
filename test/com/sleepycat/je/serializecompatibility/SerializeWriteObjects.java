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

package com.sleepycat.je.serializecompatibility;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Map;

import com.sleepycat.je.JEVersion;

public class SerializeWriteObjects {
    private File outputDir;

    public SerializeWriteObjects(String dirName) {
        outputDir = new File(dirName);
    }

    /* Delete the existed directory for serialized outputs. */
    private void deleteExistDir(File fileDir) {
        if (!fileDir.exists())
            return;
        if (fileDir.isFile()) {
            fileDir.delete();
            return;
        }

        File[] files = fileDir.listFiles();
        for (int i = 0; i < files.length; i++)
            deleteExistDir(files[i]);
        
        fileDir.delete();
    }

    /* 
     * If the directory doesn't exist, create a new one;
     * Or delete it and make a fresh one.
     */
    private void createHome() {
        if (outputDir.exists()) {
            deleteExistDir(outputDir);
        }

        outputDir.mkdirs();
    }

    /*
     * Generate a directory of .out files representing the serialized versions
     * of all serializable classes for this JE version. The directory will be 
     * named with JE version number, and each file will be named 
     * <classname>.out. These files will be used by SerializedReadObjectsTest.
     */
    public void writeObjects()
        throws IOException {

        createHome();
        ObjectOutputStream out;
        for (Map.Entry<String, Object> entry : 
             SerializeUtils.getSerializedSet().entrySet()) {
            out = new ObjectOutputStream
                (new FileOutputStream
                 (outputDir.getPath() + System.getProperty("file.separator") +
                  entry.getValue().getClass().getName() + ".out"));
            out.writeObject(entry.getValue());
            out.close();
        }
    }

    /* 
     * When do the test, it will create a sub process to run this main program
     * to call the writeObjects() method to generate serialized outputs.
     */
    public static void main(String args[]) 
        throws IOException {

        String dirName = args[0] + System.getProperty("file.separator") + 
                         JEVersion.CURRENT_VERSION.toString();
        SerializeWriteObjects writeTest = new SerializeWriteObjects(dirName);
        writeTest.writeObjects();
    }
}
