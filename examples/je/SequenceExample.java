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

package je;

import java.io.File;
import java.io.IOException;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.Sequence;
import com.sleepycat.je.SequenceConfig;

public class SequenceExample {
    private static final int EXIT_SUCCESS = 0;
    private static final int EXIT_FAILURE = 1;
    private static final String DB_NAME = "sequence.db";
    private static final String KEY_NAME = "my_sequence";

    public SequenceExample() {
    }

    public static void usage() {
        System.out.println("usage: java " +
                           "je.SequenceExample " +
                           "<dbEnvHomeDirectory>");
        System.exit(EXIT_FAILURE);
    }

    public static void main(String[] argv) {

        if (argv.length != 1) {
            usage();
            return;
        }
        File envHomeDirectory = new File(argv[0]);

        try {
            SequenceExample app = new SequenceExample();
            app.run(envHomeDirectory);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(EXIT_FAILURE);
        }
        System.exit(EXIT_SUCCESS);
    }

    public void run(File envHomeDirectory)
        throws DatabaseException, IOException {

        /* Create the environment object. */
        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        Environment env = new Environment(envHomeDirectory, envConfig);

        /* Create the database object. */
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        Database db = env.openDatabase(null, DB_NAME, dbConfig);

        /* Create the sequence oject. */
        SequenceConfig config = new SequenceConfig();
        config.setAllowCreate(true);
        DatabaseEntry key =
            new DatabaseEntry(KEY_NAME.getBytes("UTF-8"));
        Sequence seq = db.openSequence(null, key, config);

        /* Allocate a few sequence numbers. */
        for (int i = 0; i < 10; i++) {
            long seqnum = seq.get(null, 1);
            System.out.println("Got sequence number: " + seqnum);
        }

        /* Close all. */
        seq.close();
        db.close();
        env.close();
    }
}
