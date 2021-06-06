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

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.bind.tuple.TupleOutput;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.Transaction;

/**
 * MeasureInsertSize inserts a given set of key/value pairs in order to measure
 * the disk space consumed by a given data set.
 *
 * To see how much disk space is consumed, simply add up the size of the log
 * files or for a rough estimate multiply the number of files by 10 MB.
 *
 * This program does sequential inserts.  For random inserts, more disk space
 * will be consumed, especially if the entire data set does not fit in the
 * cache.
 *
 * This program does not insert into secondary databases, but can be used to
 * measure the size of a secondary by specifying the key and data sizes of the
 * secondary records.  The data size for a secondary record should be specified
 * as the size of the primary key.
 *
 * Checkpoints are performed by this program as usual, and checkpoints will
 * added to the size of the log.  This is realistic for a typical application
 * but note that a smaller disk size can be obtained using a bulk load.
 *
 * For command line parameters see the usage() method.
 */
public class MeasureInsertSize {

    private File home;
    private int records;
    private int keySize;
    private int dataSize = -1;
    private int insertsPerTxn;
    private boolean deferredWrite;
    private Environment env;
    private Database db;

    public static void main(String args[]) {
        try {
            MeasureInsertSize example = new MeasureInsertSize(args);
            example.open();
            example.doInserts();
            example.close();
            System.exit(0);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public MeasureInsertSize(String[] args) {
        for (int i = 0; i < args.length; i += 1) {
            String name = args[i];
            String val = null;
            if (i < args.length - 1 && !args[i + 1].startsWith("-")) {
                i += 1;
                val = args[i];
            }
            if (name.equals("-h")) {
                if (val == null) {
                    usage("No value after -h");
                }
                home = new File(val);
            } else if (name.equals("-records")) {
                if (val == null) {
                    usage("No value after -records");
                }
                try {
                    records = Integer.parseInt(val);
                } catch (NumberFormatException e) {
                    usage(val + " is not a number");
                }
                if (records <= 0) {
                    usage(val + " is not a positive integer");
                }
            } else if (name.equals("-key")) {
                if (val == null) {
                    usage("No value after -key");
                }
                try {
                    keySize = Integer.parseInt(val);
                } catch (NumberFormatException e) {
                    usage(val + " is not a number");
                }
                if (keySize < 4) {
                    usage(val + " is not four or greater");
                }
            } else if (name.equals("-data")) {
                if (val == null) {
                    usage("No value after -data");
                }
                try {
                    dataSize = Integer.parseInt(val);
                } catch (NumberFormatException e) {
                    usage(val + " is not a number");
                }
                if (dataSize < 0) {
                    usage(val + " is not a positive integer");
                }
            } else if (name.equals("-txn")) {
                if (val == null) {
                    usage("No value after -txn");
                }
                try {
                    insertsPerTxn = Integer.parseInt(val);
                } catch (NumberFormatException e) {
                    usage(val + " is not a number");
                }
            } else if (name.equals("-deferredwrite")) {
                deferredWrite = true;
            } else {
                usage("Unknown arg: " + name);
            }
        }

        if (home == null) {
            usage("-h not specified");
        }

        if (records == 0) {
            usage("-records not specified");
        }

        if (keySize == -1) {
            usage("-key not specified");
        }

        if (dataSize == -1) {
            usage("-data not specified");
        }
    }

    private void usage(String msg) {

        if (msg != null) {
            System.out.println(msg);
        }

        System.out.println
            ("usage:" +
             "\njava "  + MeasureInsertSize.class.getName() +
             "\n   -h <directory>" +
             "\n      # Environment home directory; required" +
             "\n   -records <count>" +
             "\n      # Total records (key/data pairs); required" +
             "\n   -key <bytes> " +
             "\n      # Average key bytes per record; required" +
             "\n   -data <bytes>" +
             "\n      # Average data bytes per record; required" +
             "\n  [-txn <insertsPerTransaction>]" +
             "\n      # Inserts per txn; default: 0 (non-transactional)" +
             "\n  [-deferredwrite]" +
             "\n      # Use a Deferred Write database");

        System.exit(2);
    }

    private boolean isTransactional() {
        return insertsPerTxn > 0;
    }

    private void open()
        throws DatabaseException {

        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        envConfig.setTransactional(isTransactional());
        env = new Environment(home, envConfig);

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setTransactional(isTransactional());
        dbConfig.setDeferredWrite(deferredWrite);
        db = env.openDatabase(null, "foo", dbConfig);
    }

    private void close()
        throws DatabaseException {

        db.close();
        env.close();
    }

    public void doInserts()
        throws DatabaseException {

        DatabaseEntry data = new DatabaseEntry(new byte[dataSize]);
        DatabaseEntry key = new DatabaseEntry();
        byte[] keyBuffer = new byte[keySize];
        byte[] keyPadding = new byte[keySize - 4];

        Transaction txn = null;

        for (int i = 1; i <= records; i += 1) {

            TupleOutput keyOutput = new TupleOutput(keyBuffer);
            keyOutput.writeInt(i);
            keyOutput.writeFast(keyPadding);
            TupleBinding.outputToEntry(keyOutput, key);

            if (isTransactional() && txn == null) {
                txn = env.beginTransaction(null, null);
            }

            db.put(txn, key, data);

            if (txn != null && i % insertsPerTxn == 0) {
                txn.commit();
                txn = null;
            }
        }
    }
}
