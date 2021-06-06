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

package com.sleepycat.je.util.dbfilterstats;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintStream;
import java.io.StringReader;
import java.util.Map.Entry;
import java.util.SortedMap;

import org.junit.Test;

import com.sleepycat.je.statcap.StatFile;
import com.sleepycat.je.util.DbFilterStats;
import com.sleepycat.je.util.Splitter;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

/**
 * DbFilterStats tests.
 *
 */
public class DbFilterStatsTest extends TestBase {

    private static final char DELIMITER = ',';

    /* The directory where stats files are located. */
    private static final String testDir =
        "test/com/sleepycat/je/util/dbfilterstats";
    /* projection file with all columns */
    private static final String allCols =
        testDir + File.separator + "allcols.csv";
    /* projection file with some column prefixes */
    private static final String someCols =
            testDir + File.separator + "somecols.csv";
    private static final String statfile =
        testDir + File.separator + "je.stat.csv";
    private static final String statfile0 =
            testDir + File.separator + "je.stat.0.csv";
    private static final String envstatfile =
            testDir + File.separator + "je.config.csv";


    private final Splitter splitter = new Splitter(DELIMITER);

    /**
     * Test that uses projection file to project all stats.
     */
    @Test
    public void testProjectAll() throws Exception {
        String row;
        int rowcount = 0;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        String[] args = new String[] {
            "-f",
            allCols,
            statfile0
        };

        PrintStream original = System.out;
        try {
            System.setOut(new PrintStream(baos));
            if (!(new DbFilterStats()).execute(args)) {
                fail("command did not return expected value");
            }
            baos.flush();
        } finally {
            System.setOut(original);
        }
        String filOut = baos.toString();
        BufferedReader br = new BufferedReader(new StringReader(filOut));
        String header = br.readLine();
        rowcount++;
        assertTrue("header is null", header != null);
        String[] hcols = parseRow(header);
        assertEquals("number of columns not expected", 169, hcols.length);

        while((row = br.readLine()) != null) {
            String cols[] = parseRow(row);
            assertEquals(
                "number of columns not expected", 169, cols.length);
            rowcount++;
        }
        assertEquals("number of rows not expected", 7, rowcount);
        SortedMap<String, Long> fileMap = StatFile.sumItUp(new File(statfile0));
        SortedMap<String, Long> filterMap =
            StatFile.sumItUp(new BufferedReader(new StringReader(filOut)));
        for (Entry<String, Long> e : fileMap.entrySet()) {
            Long filterVal = filterMap.get(e.getKey());
            assertEquals("values not equal ", filterVal, e.getValue());
        }
    }

    /**
     * Test that uses projection from command line.
     */
    @Test
    public void testProjectCmdLine() throws Exception {
        String row;
        int rowcount = 0;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        String[] args = new String[] {
            "-p",
            "time, Environment:environmentCreationTime , Op, Node Compression",
            statfile0
        };

        PrintStream original = System.out;
        try {
            System.setOut(new PrintStream(baos));
            if (!(new DbFilterStats()).execute(args)) {
                fail("command did not return expected value");
            }
            baos.flush();
        } finally {
            System.setOut(original);
        }
        String filOut = baos.toString();
        BufferedReader br = new BufferedReader(new StringReader(filOut));
        String header = br.readLine();
        rowcount++;
        assertTrue("header is null", header != null);
        String[] hcols = parseRow(header);
        assertEquals("number of columns not expected", 43, hcols.length);
        assertEquals("first column not as expected", "time", hcols[0]);
        assertEquals("second column not as expected",
                     "Environment:environmentCreationTime",
                     hcols[1]);
        while((row = br.readLine()) != null) {
            String cols[] = parseRow(row);
            assertEquals(
                "number of columns not expected", 43, cols.length);
            rowcount++;
        }
        assertEquals("number of rows not expected", 7, rowcount);
        SortedMap<String, Long> fileMap = StatFile.sumItUp(new File(statfile0));
        SortedMap<String, Long> filterMap =
            StatFile.sumItUp(new BufferedReader(new StringReader(filOut)));
        for (Entry<String, Long> e : filterMap.entrySet()) {
            Long fileVal = fileMap.get(e.getKey());
            assertEquals("values not equal ", fileVal, e.getValue());
        }
    }

    /**
     * Test that uses projection from command line and projection file
     */
    @Test
    public void testFileAndCmdLine() throws Exception {
        String row;
        int rowcount = 0;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        String[] args = new String[] {
            "-p",
            "time, Environment:environmentCreationTime",
            "-f",
            someCols,
            statfile0
        };

        PrintStream original = System.out;
        try {
            System.setOut(new PrintStream(baos));
            if (!(new DbFilterStats()).execute(args)) {
                fail("command did not return expected value");
            }
            baos.flush();
        } finally {
            System.setOut(original);
        }
        String filOut = baos.toString();
        BufferedReader br = new BufferedReader(new StringReader(filOut));
        String header = br.readLine();
        rowcount++;
        assertTrue("header is null", header != null);
        String[] hcols = parseRow(header);
        assertEquals("number of columns not expected", 43, hcols.length);
        assertEquals("first column not as expected", "time", hcols[0]);
        assertEquals("second column not as expected",
                     "Environment:environmentCreationTime",
                     hcols[1]);
        while((row = br.readLine()) != null) {
            String cols[] = parseRow(row);
            assertEquals(
                "number of columns not expected", 43, cols.length);
            rowcount++;
        }
        assertEquals("number of rows not expected", 7, rowcount);
        SortedMap<String, Long> fileMap = StatFile.sumItUp(new File(statfile0));
        SortedMap<String, Long> filterMap =
            StatFile.sumItUp(new BufferedReader(new StringReader(filOut)));
        for (Entry<String, Long> e : filterMap.entrySet()) {
            Long fileVal = fileMap.get(e.getKey());
            assertEquals("values not equal ", fileVal, e.getValue());
        }
    }

    /**
     * Test that uses projection file to project all stats.
     */
    @Test
    public void testProjectAllMultiInputFiles() throws Exception {
        String row;
        int rowcount = 0;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        String[] args = new String[] {
            "-f",
            allCols,
            statfile0,
            statfile
        };

        PrintStream original = System.out;
        try {
            System.setOut(new PrintStream(baos));
            if (!(new DbFilterStats()).execute(args)) {
                fail("command did not return expected value");
            }
            baos.flush();
        } finally {
            System.setOut(original);
        }
        String filOut = baos.toString();
        BufferedReader br = new BufferedReader(new StringReader(filOut));
        String header = br.readLine();
        rowcount++;
        assertTrue("header is null", header != null);
        String[] hcols = parseRow(header);
        assertEquals("number of columns not expected", 169, hcols.length);

        while((row = br.readLine()) != null) {
            String cols[] = parseRow(row);
            assertEquals(
                "number of columns not expected", 169, cols.length);
            rowcount++;
        }
        assertEquals("number of rows not expected", 9, rowcount);
        SortedMap<String, Long> fileMap =
            StatFile.sumItUp(new File(testDir), "je.stat.");
        SortedMap<String, Long> filterMap =
            StatFile.sumItUp(new BufferedReader(new StringReader(filOut)));
        for (Entry<String, Long> e : fileMap.entrySet()) {
            Long filterVal = filterMap.get(e.getKey());
            assertEquals("values not equal for " + e.getKey() + " ",
                         filterVal, e.getValue());
        }
    }

    /**
     * Test that uses projection from command line on environment
     * statistics file.
     */
    @Test
    public void testEnvConfigProjectCmdLine() throws Exception {
        String row;
        int rowcount = 0;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        String[] args = new String[] {
            "-p",
            "time, java",
            envstatfile
        };

        PrintStream original = System.out;
        try {
            System.setOut(new PrintStream(baos));
            if (!(new DbFilterStats()).execute(args)) {
                fail("command did not return expected value");
            }
            baos.flush();
        } finally {
            System.setOut(original);
        }
        String filOut = baos.toString();
        BufferedReader br = new BufferedReader(new StringReader(filOut));
        String header = br.readLine();
        rowcount++;
        assertTrue("header is null", header != null);
        String[] hcols = parseRow(header);
        assertEquals("number of columns not expected", 6, hcols.length);
        assertEquals("first column not as expected", "time", hcols[0]);
        assertTrue("second column not as expected",
                     hcols[1].startsWith("java:"));
        while((row = br.readLine()) != null) {
            String cols[] = parseRow(row);
            assertEquals(
                "number of columns not expected", 6, cols.length);
            rowcount++;
        }
        assertEquals("number of rows not expected", 2, rowcount);
    }


    /**
     * Test that creates file with escapes and quoted strings and
     * projects all the data.
     */
    @Test
    public void testQuotesEscapes() throws Exception {
        final String TESTFILE = "test.csv";
        final String data[] =
            {"col1,col2,col3,col4,col5",
             " \"double quotestring\", escape\\,delimiter, ,,"};
        File envHome = SharedTestUtils.getTestDir();
        File tmpfile =
                new File(envHome, TESTFILE);
        if (tmpfile.exists()) {
            tmpfile.delete();
        }

        BufferedWriter out =
                new BufferedWriter(new FileWriter(tmpfile, true));
        for (int i = 0; i < data.length; i++) {
               out.write(data[i]);
               out.newLine();
        }
        out.flush();
        out.close();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        String[] args = new String[] {
            "-p",
            data[0],
            tmpfile.getAbsolutePath()
        };

        PrintStream original = System.out;
        try {
            System.setOut(new PrintStream(baos));
            if (!(new DbFilterStats()).execute(args)) {
                fail("command did not return expected value");
            }
            baos.flush();
        } finally {
            System.setOut(original);
        }
        String filOut = baos.toString();
        BufferedReader br = new BufferedReader(new StringReader(filOut));
        int rowcount = 0;
        String row;
        while((row = br.readLine()) != null) {
            assertEquals("row not expected", data[rowcount], row);
            rowcount++;
        }
    }

    private String[] parseRow(String row) {
        String [] vals = splitter.tokenize(row);
        for (int i = 0; i < vals.length; i++) {
            vals[i] = vals[i].trim();
        }
        return vals;
    }
}
