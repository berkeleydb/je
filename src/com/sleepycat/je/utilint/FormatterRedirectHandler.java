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

import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.LogRecord;

/**
 * Redirect the ConsoleHandler to use a specific Formatter. This is the
 * same redirect approach used in
 * com.sleepycat.je.utilint.ConsoleRedirectHandler, but in this case, an
 * environment (and its associated stored console handler) is not available.
 * In order to still have prefixed logging output, we incur the higher level
 * cost of resetting the formatter.
 */
public class FormatterRedirectHandler 
    extends java.util.logging.ConsoleHandler {
    
    /*
     * We want console logging to be determined by the level for
     * com.sleepycat.je.util.ConsoleHandler. Check that handler's level and use
     * it to set FormatterRedirectHandler explicitly.
     */
    private static final String HANDLER_LEVEL = 
        com.sleepycat.je.util.ConsoleHandler.class.getName() + ".level";

    public FormatterRedirectHandler() {
        super();
        
        String level = LoggerUtils.getLoggerProperty(HANDLER_LEVEL);
        setLevel((level == null) ? Level.OFF : Level.parse(level));
    }

    @Override
    public void publish(LogRecord record) {
        Formatter formatter = 
            LoggerUtils.formatterMap.get(Thread.currentThread());
        if (formatter != null) {
            setFormatter(formatter);
        }
        super.publish(record);
    }
}
