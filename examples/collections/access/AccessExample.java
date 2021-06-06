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

package collections.access;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;

import com.sleepycat.bind.ByteArrayBinding;
import com.sleepycat.collections.StoredSortedMap;
import com.sleepycat.collections.TransactionRunner;
import com.sleepycat.collections.TransactionWorker;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

/**
 * AccesssExample mirrors the functionality of a class by the same name used to
 * demonstrate the com.sleepycat.je Java API. This version makes use of the new
 * com.sleepycat.collections.* collections style classes to make life easier.
 *
 * @author Gregory Burd <gburd@sleepycat.com>
 */
public class AccessExample
    implements Runnable {

    private static boolean create = true;
    private static final int EXIT_FAILURE = 1;

    public static void usage() {

        System.out.println("usage: java " + AccessExample.class.getName() +
            " [-r] [database]\n");
        System.exit(EXIT_FAILURE);
    }

    public static void main(String[] argv) {

        boolean removeExistingDatabase = false;
        String databaseName = "access.db";

        for (int i = 0; i < argv.length; i++) {
            if (argv[i].equals("-r")) {
                removeExistingDatabase = true;
            } else if (argv[i].equals("-?")) {
                usage();
            } else if (argv[i].startsWith("-")) {
                usage();
            } else {
                if ((argv.length - i) != 1)
                    usage();
                databaseName = argv[i];
                break;
            }
        }

        try {
            EnvironmentConfig envConfig = new EnvironmentConfig();
            envConfig.setTransactional(true);
            if (create) {
                envConfig.setAllowCreate(true);
            }
            Environment env = new Environment(new File("."), envConfig);
            if (removeExistingDatabase) {
                env.removeDatabase(null, databaseName);
            }

            // create the app and run it
            AccessExample app = new AccessExample(env, databaseName);
            app.run();
            app.close();
        } catch (DatabaseException e) {
            e.printStackTrace();
            System.exit(1);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
        System.exit(0);
    }

    private Database db;
    private SortedMap<byte[], byte[]> map;
    private Environment env;

    /**
     * Constructor for the AccessExample object
     */
    public AccessExample(Environment env, String databaseName)
        throws Exception {

        this.env = env;

        /*
         * Lets mimic the db.AccessExample 100% and use plain old byte arrays
         * to store the key and data strings.
         */
        ByteArrayBinding keyBinding = new ByteArrayBinding();
        ByteArrayBinding dataBinding = new ByteArrayBinding();

        /* Open a data store. */
        DatabaseConfig dbConfig = new DatabaseConfig();
        if (create) {
            dbConfig.setAllowCreate(true);
        }
        this.db = env.openDatabase(null, databaseName, dbConfig);

        /*
         * Now create a collection style map view of the data store so that it
         * is easy to work with the data in the database.
         */
        this.map = new StoredSortedMap<byte[], byte[]>
            (db, keyBinding, dataBinding, true);
    }

    /**
     * Close the database and environment.
     */
    void close()
        throws DatabaseException {

        db.close();
        env.close();
    }

    /**
     * Main processing method for the AccessExample object
     */
    public void run() {

        /*
         * Insert records into a Stored Sorted Map DatabaseImpl, where the key
         * is the user input and the data is the user input in reverse order.
         */
        final InputStreamReader reader = new InputStreamReader(System.in);

        for (; ; ) {
            final String line = askForLine(reader, System.out, "input> ");
            if (line == null) {
                break;
            }

            final String reversed =
                (new StringBuilder(line)).reverse().toString();

            log("adding: \"" +
                line + "\" : \"" +
                reversed + "\"");

            /* Do the work to add the key/data to the HashMap here. */
            TransactionRunner tr = new TransactionRunner(env);
            try {
                tr.run(new TransactionWorker() {
                        public void doWork() {
                            try {
                                if (!map.containsKey(line.getBytes("UTF-8")))
                                    map.put(line.getBytes("UTF-8"),
                                            reversed.getBytes("UTF-8"));
                                else
                                    System.out.println("Key " + line +
                                                       " already exists.");
                            } catch (Exception e) {
                                System.err.println("doWork: " + e);
                            }
                        }
                    });
            } catch (com.sleepycat.je.DatabaseException e) {
                System.err.println("AccessExample: " + e);
                System.exit(1);
            } catch (java.lang.Exception e) {
                System.err.println("AccessExample: " + e);
                System.exit(1);
            }
        }
        System.out.println("");

        /*
         * Do the work to traverse and print the HashMap key/data pairs here
         * get iterator over map entries.
         */
        Iterator<Map.Entry<byte[], byte[]>> iter = map.entrySet().iterator();
        System.out.println("Reading data");
        while (iter.hasNext()) {
            Map.Entry<byte[], byte[]> entry = iter.next();
            log("found \"" +
                new String(entry.getKey()) +
                "\" key with data \"" +
                new String(entry.getValue()) + "\"");
        }
    }

    /**
     * Prompts for a line, and keeps prompting until a non blank line is
     * returned. Returns null on error.
     *
     * @param  reader  stream from which to read user input
     * @param  out     stream on which to prompt for user input
     * @param  prompt  prompt to use to solicit input
     * @return         the string supplied by the user
     */
    String askForLine(InputStreamReader reader,
                      PrintStream out,
                      String prompt) {

        String result = "";
        while (result != null && result.length() == 0) {
            out.print(prompt);
            out.flush();
            result = getLine(reader);
        }
        return result;
    }

    /**
     * Read a single line. Gets the line attribute of the AccessExample object
     * Not terribly efficient, but does the job. Works for reading a line from
     * stdin or a file.
     *
     * @param reader stream from which to read the line
     *
     * @return either a String or null on EOF, if EOF appears in the middle of
     * a line, returns that line, then null on next call.
     */
    String getLine(InputStreamReader reader) {

        StringBuilder b = new StringBuilder();
        int c;
        try {
            while ((c = reader.read()) != -1 && c != '\n') {
                if (c != '\r') {
                    b.append((char) c);
                }
            }
        } catch (IOException ioe) {
            c = -1;
        }

        if (c == -1 && b.length() == 0) {
            return null;
        } else {
            return b.toString();
        }
    }

    /**
     * A simple log method.
     *
     * @param s The string to be logged.
     */
    private void log(String s) {

        System.out.println(s);
        System.out.flush();
    }
}
