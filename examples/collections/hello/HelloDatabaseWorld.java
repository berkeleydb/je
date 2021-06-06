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

package collections.hello;

import java.io.File;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;

import com.sleepycat.bind.serial.ClassCatalog;
import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.collections.StoredSortedMap;
import com.sleepycat.collections.TransactionRunner;
import com.sleepycat.collections.TransactionWorker;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

/**
 * @author Mark Hayes
 */
public class HelloDatabaseWorld implements TransactionWorker {

    private static final String[] INT_NAMES = {
        "Hello", "Database", "World",
    };
    private static boolean create = true;

    private Environment env;
    private ClassCatalog catalog;
    private Database db;
    private SortedMap<Integer, String> map;

    /** Creates the environment and runs a transaction */
    public static void main(String[] argv)
        throws Exception {

        String dir = "./tmp";

        // environment is transactional
        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setTransactional(true);
        if (create) {
            envConfig.setAllowCreate(true);
        }
        Environment env = new Environment(new File(dir), envConfig);

        // create the application and run a transaction
        HelloDatabaseWorld worker = new HelloDatabaseWorld(env);
        TransactionRunner runner = new TransactionRunner(env);
        try {
            // open and access the database within a transaction
            runner.run(worker);
        } finally {
            // close the database outside the transaction
            worker.close();
        }
    }

    /** Creates the database for this application */
    private HelloDatabaseWorld(Environment env)
        throws Exception {

        this.env = env;
        open();
    }

    /** Performs work within a transaction. */
    public void doWork() {
        writeAndRead();
    }

    /** Opens the database and creates the Map. */
    private void open()
        throws Exception {

        // use a generic database configuration
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(true);
        if (create) {
            dbConfig.setAllowCreate(true);
        }

        // catalog is needed for serial bindings (java serialization)
        Database catalogDb = env.openDatabase(null, "catalog", dbConfig);
        catalog = new StoredClassCatalog(catalogDb);

        // use Integer tuple binding for key entries
        TupleBinding<Integer> keyBinding =
            TupleBinding.getPrimitiveBinding(Integer.class);

        // use String serial binding for data entries
        SerialBinding<String> dataBinding =
            new SerialBinding<String>(catalog, String.class);

        this.db = env.openDatabase(null, "helloworld", dbConfig);

        // create a map view of the database
        this.map = new StoredSortedMap<Integer, String>
            (db, keyBinding, dataBinding, true);
    }

    /** Closes the database. */
    private void close()
        throws Exception {

        if (catalog != null) {
            catalog.close();
            catalog = null;
        }
        if (db != null) {
            db.close();
            db = null;
        }
        if (env != null) {
            env.close();
            env = null;
        }
    }

    /** Writes and reads the database via the Map. */
    private void writeAndRead() {

        // check for existing data
        Integer key = new Integer(0);
        String val = map.get(key);
        if (val == null) {
            System.out.println("Writing data");
            // write in reverse order to show that keys are sorted
            for (int i = INT_NAMES.length - 1; i >= 0; i -= 1) {
                map.put(new Integer(i), INT_NAMES[i]);
            }
        }
        // get iterator over map entries
        Iterator<Map.Entry<Integer, String>> iter = map.entrySet().iterator();
        System.out.println("Reading data");
        while (iter.hasNext()) {
            Map.Entry<Integer, String> entry = iter.next();
            System.out.println(entry.getKey().toString() + ' ' +
                               entry.getValue());
        }
    }
}
