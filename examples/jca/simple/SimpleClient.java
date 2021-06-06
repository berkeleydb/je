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

package jca.simple;

import javax.naming.InitialContext;

import java.util.Hashtable;

public class SimpleClient {

    public static void main(String args[])
        throws Exception {

        final boolean OC4J = false;

        InitialContext iniCtx = null;
        Hashtable env = new Hashtable();
        if (OC4J) {
            env.put("java.naming.factory.initial",
                    "com.evermind.server.ApplicationClientInitialContextFactory");
            env.put("java.naming.provider.url","ormi://localhost:23791/Simple");
            env.put("java.naming.security.principal","oc4jadmin");
            env.put("java.naming.security.credentials","oc4jadmin");
            iniCtx = new InitialContext(env);
        } else {
            iniCtx = new InitialContext();
        }

        Object ref = iniCtx.lookup("SimpleBean");
        SimpleHome home = (SimpleHome) ref;
        Simple simple = home.create();
        System.out.println("Created Simple");
        simple.put(args[0], args[1]);
        System.out.println("Simple.get('" + args[0] + "') = " +
                           simple.get(args[0]));
    }
}
