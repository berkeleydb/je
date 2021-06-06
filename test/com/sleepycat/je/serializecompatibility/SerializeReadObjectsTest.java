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

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InvalidClassException;
import java.io.ObjectInputStream;
import java.util.Map;

import org.junit.Test;

import com.sleepycat.je.JEVersion;
import com.sleepycat.util.test.TestBase;

/*
 * Test whether those serializable classes of prior versions can be read by
 * the latest one.
 *
 * This test is used in conjunction with SerializeWriteObjects, a main program
 * which is used to generate the serialized outputs of those serializable
 * classes for a JE version. When a new version is to be released,
 * run SerializedWriteObjects to generate serialized outputs, and then
 * add a test_x_y_z() method to this class.
 */
public class SerializeReadObjectsTest extends TestBase {

    /* Used to identify the two versions is compatible. */
    private boolean serializedSuccess = true;

    /* The directory where serialized files saved. */
    private File outputDir;

    /* The directory where outputDir saved. */
    private static final String parentDir =
        "test/com/sleepycat/je/serializecompatibility";

    /**
     * Test whether the latest version is compatible with 4.0.0.
     * @throws ClassNotFoundException when the test is enabled
     */
    @Test
    public void test_4_0_0() 
        throws ClassNotFoundException, IOException {

        doTest(new JEVersion("4.0.106"));
    }

    /**
     * Test whether the latest version is compatible with 4.1.0.
     * @throws ClassNotFoundException when the test is enabled
     */
    @Test
    public void test_4_1_0()
        throws ClassNotFoundException, IOException {

        doTest(new JEVersion("4.1.6"));
    }
      
    /*
     * Read these serialized files and convert it.  If it's compatible, it
     * won't throw the InvalidClassException; if not, it would throw out the
     * exception, serializedSuccess is false.
     */
    public void doTest(JEVersion version)
        throws ClassNotFoundException, IOException {

        outputDir = new File(parentDir, version.getNumericVersionString());
        if (!outputDir.exists()) {
            System.out.println("No such directory, try it again");
            System.exit(1);
        }

        try {
            ObjectInputStream in;
            for (Map.Entry<String, Object> entry : 
                 SerializeUtils.getSerializedSet().entrySet()) {

                /*
                 * Do the check when the latest version larger than the
                 * assigned version.
                 */
                if (JEVersion.CURRENT_VERSION.compareTo(version) >= 0) {
                    in = new ObjectInputStream
                        (new FileInputStream
                            (outputDir.getPath() +
                             System.getProperty("file.separator") +
                             entry.getKey() + ".out"));
                    /* Check that we can read the object successfully. */
                    in.readObject();
                    in.close();
                }
            }
        } catch (InvalidClassException e) {
            /* Reading serialized output failed.*/
            serializedSuccess = false;
        } catch (FileNotFoundException fnfe) {
            /* A class doesn't exist in the former version, do nothing. */
        }

        if (serializedSuccess) {
            System.out.println("Serialization is compatible");
        } else {
            System.out.println("Serialization is not compatible");
        }

        assertTrue(serializedSuccess);
    }
}
