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

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Calendar;

import org.junit.After;
import org.junit.Test;

import com.sleepycat.util.test.TestBase;

public class CronScheduleParserTest extends TestBase {

    private static long millsOneDay = 24 * 60 * 60 * 1000;
    private static long millsOneHour = 60 * 60 * 1000;
    private static long millsOneMinute = 60 * 1000;

    private static Calendar generatedCurCal = Calendar.getInstance();

    static {
        /*
         * Set the current Calendar to be 05:01 Friday.
         */
        generatedCurCal.set(Calendar.DAY_OF_WEEK, 6);
        generatedCurCal.set(Calendar.HOUR_OF_DAY, 5);
        generatedCurCal.set(Calendar.MINUTE, 1);
        generatedCurCal.set(Calendar.SECOND, 0);
        generatedCurCal.set(Calendar.MILLISECOND, 0);
    }

    @After
    public void tearDown() 
        throws Exception {
        CronScheduleParser.setCurCalHook = null;
        super.tearDown();
    }

    @Test
    public void testCheckSame() {
        assertTrue(CronScheduleParser.checkSame(null, null));
        assertFalse(CronScheduleParser.checkSame(null, "0 0 * * *"));
        assertFalse(CronScheduleParser.checkSame("0 0 * * *", null));
        assertTrue(CronScheduleParser.checkSame("5 7 * * *", "5 7 * * *"));
        assertTrue(CronScheduleParser.checkSame("5 7 * * 5", "5 7 * * 5"));
        assertFalse(CronScheduleParser.checkSame("0 0 * * *", "5 7 * * *"));
        assertFalse(CronScheduleParser.checkSame("5 7 * * 5", "5 7 * * 6"));
    }

    @Test
    public void testValidate() {
        internalValidate(null, CronScheduleParser.nullCons);
        validateCorrect("* * * * *");

        /*
         * Constraint 1: The standard string should be "* * * * *", i.e.
         * there are 5 fields and 4 blank space.
         */
        internalValidate(" * * *", CronScheduleParser.cons1);
        internalValidate("* * * * ", CronScheduleParser.cons1);
        internalValidate("* * * * * *", CronScheduleParser.cons1);
        internalValidate("* *_* * *", CronScheduleParser.cons1);
        validateCorrect("5 6 * * 4");

        /*
         * Constraint 2: Each filed can only be an int value or *.
         */
        internalValidate("* * - * )", CronScheduleParser.cons2);
        internalValidate("* * * 3.2 *", CronScheduleParser.cons2);
        internalValidate("* ** * * *", CronScheduleParser.cons2);
        validateCorrect("0 0 * * 6");

        /*
         * Constraint 3: Can not specify dayOfMonth and dayOfWeek
         * simultaneously.
         */
        internalValidate("* * 4 * 5", CronScheduleParser.cons3);
        validateCorrect("59 23 * * 5");

        /*
         * Constraint 4: Can not specify dayOfMonth or month.
         */
        internalValidate("* * 4 * *", CronScheduleParser.cons4);
        internalValidate("* * * 4 *", CronScheduleParser.cons4);
        internalValidate("* * 4 4 *", CronScheduleParser.cons4);
        validateCorrect("59 23 * * *");

        /*
         * Constraint 5: If the field is a int value, then the value should
         * be in the correct range.
         */
        internalValidate("-1 * * * *", CronScheduleParser.cons5);
        internalValidate("60 * * * *", CronScheduleParser.cons5);
        validateCorrect("0 * * * *");
        validateCorrect("59 * * * *");
        internalValidate("1 -1 * * *", CronScheduleParser.cons5);
        internalValidate("1 24 * * *", CronScheduleParser.cons5);
        validateCorrect("1 0 * * *");
        validateCorrect("1 23 * * *");
        internalValidate("1 1 * * -1", CronScheduleParser.cons5);
        internalValidate("1 1 * * 7", CronScheduleParser.cons5);
        validateCorrect("1 1 * * 0");
        validateCorrect("1 1 * * 6");

        /*
         * Constraint 6: If dayOfWeek is a concrete value, then minute or
         * hour can not be '*'.
         */
        internalValidate("* * * * 6", CronScheduleParser.cons6);
        internalValidate("1 * * * 6", CronScheduleParser.cons6);
        internalValidate("* 1 * * 6", CronScheduleParser.cons6);
        validateCorrect("1 1 * * 6");

        /*
         * Constraint 7: If hour is a concrete value, minute can not be '*'.
         */
        internalValidate("* 23 * * *", CronScheduleParser.cons7);
        validateCorrect("1 23 * * *");
    }
    
    private void internalValidate(String cronSchedule, String mess) {
        try {
            new CronScheduleParser(cronSchedule);
            fail("Should throw IllegalArgumentException");
        } catch (IllegalArgumentException iae) {
            assertTrue(iae.getMessage().contains(mess));
        }
    }
    
    private void validateCorrect(String cronSchedule) {
        try {
            new CronScheduleParser(cronSchedule);
        } catch (Exception e) {
            fail("Should not throw Exception");
        }
    }

    @Test
    public void testParser() {
        MyHook hook = new MyHook();
        CronScheduleParser.setCurCalHook = hook;

        check("* * * * *", 0, millsOneMinute);

        /*
         * Because of the Daylight Saving Time or the Winter time, the
         * calculated delay by using 7 * millsOneDay may not be right, i.e.
         * loss or get one more hour. 
         */
        Calendar scheculedCal = (Calendar) generatedCurCal.clone();

        scheculedCal.set(Calendar.DAY_OF_WEEK, 6);
        scheculedCal.set(Calendar.HOUR_OF_DAY, 5);
        scheculedCal.set(Calendar.MINUTE, 5);
        check(
            "5 * * * *",
            scheculedCal.getTimeInMillis() - generatedCurCal.getTimeInMillis(),
            millsOneHour);

        scheculedCal.set(Calendar.DAY_OF_WEEK, 6);
        scheculedCal.set(Calendar.HOUR_OF_DAY, 6);
        scheculedCal.set(Calendar.MINUTE, 0);
        check(
            "0 * * * *",
            scheculedCal.getTimeInMillis() - generatedCurCal.getTimeInMillis(),
            millsOneHour);

        scheculedCal.set(Calendar.DAY_OF_WEEK, 6);
        scheculedCal.set(Calendar.HOUR_OF_DAY, 7);
        scheculedCal.set(Calendar.MINUTE, 59);
        check(
            "59 7 * * *",
            scheculedCal.getTimeInMillis() - generatedCurCal.getTimeInMillis(),
            millsOneDay);

        scheculedCal.set(Calendar.DAY_OF_WEEK, 7);
        scheculedCal.set(Calendar.HOUR_OF_DAY, 1);
        scheculedCal.set(Calendar.MINUTE, 30);
        check(
            "30 1 * * *",
            scheculedCal.getTimeInMillis() - generatedCurCal.getTimeInMillis(),
            millsOneDay);

        scheculedCal.set(Calendar.DAY_OF_WEEK, 7);
        scheculedCal.set(Calendar.HOUR_OF_DAY, 4);
        scheculedCal.set(Calendar.MINUTE, 10);
        check(
            "10 4 * * 6",
            scheculedCal.getTimeInMillis() - generatedCurCal.getTimeInMillis(),
            7 * millsOneDay);

        scheculedCal.set(Calendar.DAY_OF_WEEK, 4);
        scheculedCal.set(Calendar.HOUR_OF_DAY, 4);
        scheculedCal.set(Calendar.MINUTE, 10);
        scheculedCal.add(Calendar.DATE, 7);
        check(
            "10 4 * * 3",
            scheculedCal.getTimeInMillis() - generatedCurCal.getTimeInMillis(),
            7 * millsOneDay);
    }

    private void check(String cronSchedule, long delay, long interval) {
        CronScheduleParser csp = new CronScheduleParser(cronSchedule);
        assertEquals(delay, csp.getDelayTime());
        assertEquals(interval, csp.getInterval());
    }

    class MyHook implements TestHook<Void> {

        @Override
        public void doHook() {

            CronScheduleParser.curCal = generatedCurCal;
        }

        @Override
        public void doHook(Void obj) {
        }
        @Override
        public void hookSetup() {
        }
        @Override
        public void doIOHook() throws IOException {
        }
        @Override
        public Void getHookValue() {
            return null;
        }
    }
}
