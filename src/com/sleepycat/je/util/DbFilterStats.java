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

package com.sleepycat.je.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.sleepycat.je.utilint.CmdUtil;

/**
 * Transform one or more je.stat.csv statistics files and
 * write the output to stdout. A set of column names is used to
 * specify the order and which columns are written to the output.
 * The utility is used to create an output file that is easier to
 * analyze by projecting and ordering only the data that is required.
 * Each user specified column name will either be a exact match of a
 * column in the file or a prefix match. In order to output the "time"
 * and all "Op" group statistics a column list "time,Op" could be used.
 * Multiple input files are processed in the order specified on the
 * command line. Duplicate column headers are suppressed in the output
 * when processing multiple input files.
 *
 */

public class DbFilterStats {

    private static final String USAGE =
        "usage: " + CmdUtil.getJavaCommand(DbFilterStats.class) + "\n" +
        "      [-f <projection file>]     # use file for projection list\n" +
        "      [-p \"<list of columns>\"]   # use specified projection list\n" +
        "      <stat file> [<stat file>]  # list of statistic file names";

    private static final String DELIMITER = ",";

    private File projectionFile = null;
    private String projectionArg = null;
    private final List<File> inputFiles = new ArrayList<File>();

    /* list of colunms/prefixes to project */
    private final List<String> projList = new ArrayList<String>();
    private String header = null;
    private String[] fileColHeader = null;
    private final StringBuffer rowBuf = new StringBuffer();
    /* used to save name/value from file */
    private final Map<String, String> valMap =
       new HashMap<String, String>();
    private final Splitter tokenizer = new Splitter(',');

    /**
     * The main used by the DbFilterStats utility.
     *
     * @param argv An array of command line arguments to the DbFilterStats
     * utility.
     *
     * <pre>
     * usage: java { com.sleepycat.je.util.DbFilterStats | -jar
     * je.jar DbFilterStats }
     *  -f  &lt;projection file&gt;
     *  -p  &lt;column projection list&gt; A comma separated list of column
     *      names to project.
     *  &lt;stat file&gt; [&lt;stat file&gt;]
     * </pre>
     *
     * <p>At least one argument must be specified.</p>
     */
    public static void main(String argv[]) {
        DbFilterStats dbf = new DbFilterStats();
        int retstatus = dbf.execute(argv) ? 0 : -1;
        System.exit(retstatus);
    }

    /**
     * Performs the processing of the DbFilterStats command.
     *
     * @param argv DbFilterStats command arguments
     * @return true if command is successful, otherwise false
     */
    public boolean execute(String argv[]) {
        boolean retcode = true;

        if (argv.length == 0) {
            System.err.println(USAGE);
            return retcode;
        }

        try {
            DbFilterStats dbf = new DbFilterStats();
            dbf.parseArgs(argv);
            dbf.validateParams();
            dbf.processFiles();
        } catch (IllegalArgumentException e) {
            retcode = false;
        }
        return retcode;
    }

    private void processFiles() {
        for (File f : inputFiles) {
            processFile(f);
        }
    }

    /**
     * processFile will form the list of columns based on the projection
     * list. The column data to stdout. If the header has not been output
     * or is different that what was previously written, the column header for
     * the projected columns is written to stdout.
     *
     * @param statFile comma delimited file with a header
     */
    private void processFile(File statFile) {
        String row;
        BufferedReader fr = null;
        List<String> outProj = null;

        try {
            fr = new BufferedReader(new FileReader(statFile));
            while ((row = fr.readLine()) != null) {
                String[] cols = parseRow(row, false);
                if (outProj == null) {
                    /* form output projection list from header */
                    outProj = new ArrayList<String>();
                    Map<String, String> colNameMap =
                        new HashMap<String, String>();
                    for (String cname : cols) {
                        colNameMap.put(cname, cname);
                    }

                    for (String projName : projList ) {
                        if (colNameMap.get(projName) != null) {
                            outProj.add(projName);
                        } else {
                            for (String colName : cols) {
                               if (colName.startsWith(projName)) {
                                   outProj.add(colName);
                               }
                            }
                        }
                    }

                    if (header == null || !header.equals(row)) {
                        /* output header row */
                        outputRow(outProj);
                        header = row;
                        fileColHeader = cols;
                    }
                } else {
                    if (cols.length != fileColHeader.length) {
                        printFatal("Invalid stat file " +
                                   statFile.getAbsolutePath() +
                                   " header/columns are not equal.");
                    }
                    /* put column name/value in map*/
                    valMap.clear();
                    for (int i = 0; i < cols.length; i++) {
                        valMap.put(fileColHeader[i], cols[i]);
                    }
                    /* form output row based on projection list */
                    rowBuf.setLength(0);
                    for (String pname : outProj) {
                        if (rowBuf.length() != 0) {
                            rowBuf.append(DELIMITER);
                        }
                        String value = valMap.get(pname);
                        if (value != null) {
                            rowBuf.append(value);
                        } else {
                            rowBuf.append(" ");
                        }
                    }
                    System.out.println(rowBuf.toString());
                }
            }
        } catch (FileNotFoundException e) {
            printFatal(
                "Error occured accessing stat file " +
                statFile.getAbsolutePath());
        } catch (IOException e) {
            printFatal(
                "IOException occured accessing stat file " +
                statFile.getAbsolutePath() + " exception " + e);
        } finally {
            if (fr != null) {
                try {
                    fr.close();
                }
                catch (IOException e) {
                    /* eat exception */
                }
            }
        }
    }

    private void outputRow(List<String> cvals) {
        rowBuf.setLength(0);
        for (String val : cvals) {
            if (rowBuf.length() != 0) {
                rowBuf.append(DELIMITER);
            }
            rowBuf.append(val);
        }
        System.out.println(rowBuf.toString());
    }

    private void parseArgs(String argv[]) {

        int argc = 0;
        int nArgs = argv.length;
        inputFiles.clear();

        while (argc < nArgs) {
            String thisArg = argv[argc++];
            if (thisArg.equals("-f")) {
                if (argc < nArgs) {
                    projectionFile = new File(argv[argc++]);
                } else {
                    printUsage("-f requires an argument");
                }
            } else if (thisArg.equals("-p")) {
                if (argc < nArgs) {
                    projectionArg = argv[argc++];
                } else {
                    printUsage("-p requires an argument");
                }
            } else {
                inputFiles.add(new File(thisArg));
            }
        }
    }

    private void validateParams() {
        projList.clear();
        if (inputFiles.size() == 0) {
            printUsage("requires statistic file argument");
        }

        for (File f : inputFiles) {
            if (!f.exists()) {
                printFatal("Specified stat file " + f.getAbsolutePath() +
                           " does not exist.");
            }
            if (f.isDirectory()) {
                printFatal("Specified stat file " + f.getAbsolutePath() +
                           " is not a file.");
            }
        }

        if (projectionFile == null && projectionArg == null) {
            printUsage("requires either -p or -f argument");
        }

        /* add command line projections */
        if (projectionArg != null) {
            addProjections(projectionArg);
        }

        /* add projection file projections */
        if (projectionFile != null) {
            if (!projectionFile.exists()) {
                printFatal("Specified projection file " +
                           projectionFile.getAbsolutePath() +
                           " does not exist.");
            }
            if (projectionFile.isDirectory()) {
                printFatal("Specified projection file " +
                           projectionFile.getAbsolutePath() +
                           " is not a file.");
            }
            formProjections(projectionFile);
        }
    }

    private void formProjections(File projFile) {
        String row;
        BufferedReader fr = null;

        try {
            fr = new BufferedReader(new FileReader(projFile));
            row = fr.readLine();
            if (row == null) {
                printFatal("Invalid projection file " +
                           projFile.getAbsolutePath());
            }
            addProjections(row);
        } catch (FileNotFoundException e) {
            printFatal(
                "Error occured accessing projection file " +
                projFile.getAbsolutePath());
        } catch (IOException e) {
            printFatal(
                "IOException occured accessing projection file " +
                projFile.getAbsolutePath() + e);
        } finally {
            if (fr != null) {
                try {
                    fr.close();
                }
                catch (IOException e) {
                    /* eat exception */
                }
            }
        }
    }

    private String[] parseRow(String row, boolean trimIt) {
        String [] vals = tokenizer.tokenize(row);
        if (trimIt) {
            for (int i = 0; i < vals.length; i++) {
                vals[i] = vals[i].trim();
            }
        }
        return vals;
    }

    private void addProjections(String collist) {
        String[] names = parseRow(collist, true);
        for (String name : names) {
            if (name.length() == 0 ) {
                printFatal("Projection list contained a empty entry.");
            }
            projList.add(name);
        }
    }

    private void printUsage(String msg) {
        if (msg != null) {
            System.err.println(msg);
        }
        System.err.println(USAGE);
        throw new IllegalArgumentException(msg);
    }

    private void printFatal(String msg) {
        System.err.println(msg);
        throw new IllegalArgumentException(msg);
    }
}
