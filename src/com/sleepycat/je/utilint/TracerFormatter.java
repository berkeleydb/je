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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;

/**
 * Formatter for java.util.logging output.
 */
public class TracerFormatter extends Formatter {

    private static final String FORMAT = "yyyy-MM-dd HH:mm:ss.SSS z";
    private static final TimeZone TIMEZONE = TimeZone.getTimeZone("UTC");

    private final Date date;
    private final DateFormat formatter;
    private String envName;

    public TracerFormatter() {
        date = new Date();
        formatter = makeDateFormat();
    }

    public TracerFormatter(String envName) {
        this();
        this.envName = envName;
    }

    /**
     * Return a formatted date for the specified time.  Use this method for
     * thread safety, since Date and DateFormat are not thread safe.
     *
     * @param millis the time in milliseconds
     * @return the formatted date
     */
    public synchronized String getDate(long millis) {
        date.setTime(millis);

        return formatter.format(date);
    }

    /**
     * Format the log record in this form:
     *   <short date> <short time> <message level> <message>
     * @param record the log record to be formatted.
     * @return a formatted log record
     */
    @Override
    public String format(LogRecord record) {
        StringBuilder sb = new StringBuilder();

        String dateVal = getDate(record.getMillis());
        sb.append(dateVal);
        sb.append(" ");
        sb.append(record.getLevel().getLocalizedName());
        appendEnvironmentName(sb);
        sb.append(" ");
        sb.append(formatMessage(record));
        sb.append("\n");

        getThrown(record, sb);

        return sb.toString();
    }

    protected void appendEnvironmentName(StringBuilder sb) {
        if (envName != null) {
            sb.append(" [" + envName + "]");
        }
    }

    protected void getThrown(LogRecord record, StringBuilder sb) {
        if (record.getThrown() != null) {
            try {
                StringWriter sw = new StringWriter();
                PrintWriter pw = new PrintWriter(sw);
                record.getThrown().printStackTrace(pw);
                pw.close();
                sb.append(sw.toString());
            } catch (Exception ex) {
                /* Ignored. */
            }
        }
    }

    /**
     * Return a DateFormat object that uses the standard format and the UTC
     * timezone.
     */
    public static DateFormat makeDateFormat() {
        final DateFormat df = new SimpleDateFormat(FORMAT);
        df.setTimeZone(TIMEZONE);
        return df;
    }
}
