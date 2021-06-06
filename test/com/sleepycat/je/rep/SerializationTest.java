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

package com.sleepycat.je.rep;

import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.NotSerializableException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.sleepycat.je.serializecompatibility.SerializeUtils;

/**
 * Verify that all classes marked as being serializable, can be serialized and
 * deserialized.
 */
public class SerializationTest {

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    /**
     * Verifies that the clases identified by SerializeUtils.getSerializedSet()
     * can be serialized and deserialized.
     *
     * The test does not currently verify that structural equality is preserved
     * across serialization/deserialization.
     */
    @Test
    public void test()
        throws IOException, ClassNotFoundException {

        for (Map.Entry<String, Object> entry :
            SerializeUtils.getSerializedSet().entrySet()) {
            final String className = entry.getKey();

            try {
                final ByteArrayOutputStream baos =
                    new ByteArrayOutputStream(1024);
                final ObjectOutputStream out = new ObjectOutputStream(baos);
                final Object o1 = entry.getValue();
                out.writeObject(o1);
                out.close();

                final ByteArrayInputStream bais =
                    new ByteArrayInputStream(baos.toByteArray());
                final ObjectInputStream in = new ObjectInputStream(bais);
                @SuppressWarnings("unused")
                Object o2 = in.readObject();
                in.close();

                // Equality checking -- a future SR
            } catch (NotSerializableException nse) {
                nse.printStackTrace(System.err);
                fail(className + "  " + nse.getMessage());
            }
        }
    }
}
