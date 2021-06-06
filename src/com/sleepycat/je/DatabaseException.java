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

package com.sleepycat.je;

import com.sleepycat.je.utilint.LoggerUtils;

/**
 * The root of all BDB JE-defined exceptions.
 *
 * <p>Exceptions thrown by BDB JE fall into three categories.</p>
 * <ol>
 * <li>When a method is used incorrectly as the result of an application
 * programming error, a standard Java runtime exception is thrown: {@link
 * IllegalArgumentException}, {@link IllegalStateException} or {@link
 * UnsupportedOperationException}.  These exceptions have the standard meaning
 * defined by their javadoc.  Note that JE throws {@link
 * IllegalArgumentException} rather than {@link NullPointerException} when a
 * required parameter is null.
 * </li>
 * <li>When an operation failure occurs, {@link OperationFailureException} or
 * one of its subclasses is thrown.  See {@link OperationFailureException} for
 * details.
 * </li>
 * <li>When an {@code Environment} failure occurs, {@link
 * EnvironmentFailureException} or one of its subclasses is thrown.  See {@link
 * EnvironmentFailureException} for details.
 * </li>
 * </ol>
 *
 * <p>{@link OperationFailureException} and {@link EnvironmentFailureException}
 * are the only two direct subclasses of {@code DatabaseException}.</p>
 *
 * <p>(Actually the above statement is not strictly correct.  {@link
 * EnvironmentFailureException} extends {@link RunRecoveryException} which
 * extends {@code DatabaseException}.  {@link RunRecoveryException} exists for
 * backward compatibility and has been deprecated. {@link
 * EnvironmentFailureException} should be used instead.)</p>
 *
 * <p>Note that in some cases, certain methods return status values without
 * issuing an exception. This occurs in situations that are not normally
 * considered an error, but when some informational status is returned.  For
 * example, {@link com.sleepycat.je.Database#get Database.get} returns {@link
 * com.sleepycat.je.OperationStatus#NOTFOUND OperationStatus.NOTFOUND} when a
 * requested key does not appear in the database.</p>
 */
@SuppressWarnings("javadoc")
public abstract class DatabaseException extends RuntimeException {

    private static final long serialVersionUID = 1535562945L;

    /* String appended to original message, see addErrorMessage. */
    private String extraInfo = null;

    /* Per-thread re-thrown stack traces, see addRethrownStackTrace. */
    private transient ThreadLocal<String> rethrownStackTraces =
        new ThreadLocal<String>();

    /**
     * For internal use only.
     * @hidden
     */
    public DatabaseException(Throwable t) {
        super(getVersionHeader() + t.toString(), t);
    }

    /**
     * For internal use only.
     * @hidden
     */
    public DatabaseException(String message) {
        super(getVersionHeader() + message);
    }

    /**
     * For internal use only.
     * @hidden
     */
    public DatabaseException(String message, Throwable t) {
        super((getVersionHeader() + message), t);
    }

    /**
     * For internal use only.
     * @hidden
     * Utility for generating the version at the start of the exception
     * message. Public for unit tests.
     */
    public static String getVersionHeader() {
        return "(JE " + JEVersion.CURRENT_VERSION + ") ";
    }

    /**
     * For internal use only.
     * @hidden
     *
     * Support the addition of extra error information. Use this approach
     * rather than wrapping exceptions whenever possible for two reasons:
     * 1) so the user can catch the original exception class and handle it
     * appropriately, and 2) because the EnvironmentFailureException hierarchy
     * does some intricate things with setting the environment as invalid.
     *
     * @param newExtraInfo the message to add, not including separator space.
     */
    public void addErrorMessage(String newExtraInfo) {

        if (extraInfo == null) {
            extraInfo = " " + newExtraInfo;
        } else {
            extraInfo = extraInfo + ' ' + newExtraInfo;
        }
    }

    /**
     * For internal use only.
     * @hidden
     *
     * Adds the current stack trace to the exception message, before it is
     * re-thrown in a different thread.  The full stack trace will then show
     * both where it was generated and where it was re-thrown.  Use this
     * approach rather than wrapping (via wrapSelf) when user code relies on
     * the getCause method to return a specific exception, and wrapping would
     * change the cause exception to something unexpected.
     */
    public void addRethrownStackTrace() {

        final Exception localEx = new Exception(
            "Stacktrace where exception below was rethrown (" +
            getClass().getName() + ")");

        rethrownStackTraces.set(LoggerUtils.getStackTrace(localEx));
    }

    @Override
    public String getMessage() {

        /*
         * If extraInfo and rethrownStackTrace are null, don't allocate memory
         * by constructing a new string. An OutOfMemoryError (or related Error)
         * may have occurred, and we'd rather not cause another one here.
         */
        final String msg = (extraInfo != null) ?
            (super.getMessage() + extraInfo) :
            super.getMessage();

        final String rethrownStackTrace = rethrownStackTraces.get();
        if (rethrownStackTrace == null) {
            return msg;
        }

        return rethrownStackTrace + "\n" + msg;
    }
}
