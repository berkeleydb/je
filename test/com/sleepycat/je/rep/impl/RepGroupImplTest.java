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
package com.sleepycat.je.rep.impl;

import static org.junit.Assert.assertEquals;

import java.net.UnknownHostException;

import org.junit.Test;

import com.sleepycat.je.rep.utilint.RepTestUtils;
import com.sleepycat.util.test.TestBase;

public class RepGroupImplTest extends TestBase {

    @Test
    public void testSerializeDeserialize()
        throws UnknownHostException {

        for (int formatVersion = RepGroupImpl.MIN_FORMAT_VERSION;
             formatVersion <= RepGroupImpl.MAX_FORMAT_VERSION;
             formatVersion++) {
            final int electable = 5;
            final int monitor = 1;
            final int secondary =
                (formatVersion < RepGroupImpl.FORMAT_VERSION_3) ?
                0 :
                3;
            RepGroupImpl group =
                RepTestUtils.createTestRepGroup(electable, monitor, secondary);
            String s1 = group.serializeHex(formatVersion);
            String tokens[] = s1.split(TextProtocol.SEPARATOR_REGEXP);
            assertEquals(
                1 +                              /* The Rep group itself */ +
                electable + monitor + secondary, /* the individual nodes. */
                tokens.length);
            RepGroupImpl dgroup = RepGroupImpl.deserializeHex(tokens, 0);
            assertEquals("Version", formatVersion, dgroup.getFormatVersion());
            if (formatVersion == RepGroupImpl.INITIAL_FORMAT_VERSION) {
                assertEquals("Deserialized version " + formatVersion,
                             group, dgroup);
            }
            String s2 = dgroup.serializeHex(formatVersion);
            assertEquals("Reserialized version " + formatVersion, s1, s2);
        }
    }
}
