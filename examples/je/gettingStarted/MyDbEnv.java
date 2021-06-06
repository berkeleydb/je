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

// file MyDbEnv.java

package je.gettingStarted;

import java.io.File;

import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.SecondaryConfig;
import com.sleepycat.je.SecondaryDatabase;

public class MyDbEnv {

    private Environment myEnv;

    // The databases that our application uses
    private Database vendorDb;
    private Database inventoryDb;
    private Database classCatalogDb;
    private SecondaryDatabase itemNameIndexDb;

    // Needed for object serialization
    private StoredClassCatalog classCatalog;

    // Our constructor does nothing
    public MyDbEnv() {}

    // The setup() method opens all our databases and the environment
    // for us.
    public void setup(File envHome, boolean readOnly)
        throws DatabaseException {

        EnvironmentConfig myEnvConfig = new EnvironmentConfig();
        DatabaseConfig myDbConfig = new DatabaseConfig();
        SecondaryConfig mySecConfig = new SecondaryConfig();

        // If the environment is read-only, then
        // make the databases read-only too.
        myEnvConfig.setReadOnly(readOnly);
        myDbConfig.setReadOnly(readOnly);
        mySecConfig.setReadOnly(readOnly);

        // If the environment is opened for write, then we want to be
        // able to create the environment and databases if
        // they do not exist.
        myEnvConfig.setAllowCreate(!readOnly);
        myDbConfig.setAllowCreate(!readOnly);
        mySecConfig.setAllowCreate(!readOnly);

        // Allow transactions if we are writing to the database
        myEnvConfig.setTransactional(!readOnly);
        myDbConfig.setTransactional(!readOnly);
        mySecConfig.setTransactional(!readOnly);

        // Open the environment
        myEnv = new Environment(envHome, myEnvConfig);

        // Now open, or create and open, our databases
        // Open the vendors and inventory databases
        vendorDb = myEnv.openDatabase(null,
                                      "VendorDB",
                                       myDbConfig);

        inventoryDb = myEnv.openDatabase(null,
                                        "InventoryDB",
                                         myDbConfig);

        // Open the class catalog db. This is used to
        // optimize class serialization.
        classCatalogDb =
            myEnv.openDatabase(null,
                               "ClassCatalogDB",
                               myDbConfig);

        // Create our class catalog
        classCatalog = new StoredClassCatalog(classCatalogDb);

        // Need a tuple binding for the Inventory class.
        // We use the InventoryBinding class
        // that we implemented for this purpose.
        TupleBinding inventoryBinding = new InventoryBinding();

        // Open the secondary database. We use this to create a
        // secondary index for the inventory database

        // We want to maintain an index for the inventory entries based
        // on the item name. So, instantiate the appropriate key creator
        // and open a secondary database.
        ItemNameKeyCreator keyCreator =
            new ItemNameKeyCreator(inventoryBinding);

        // Set up additional secondary properties
        // Need to allow duplicates for our secondary database
        mySecConfig.setSortedDuplicates(true);
        mySecConfig.setAllowPopulate(true); // Allow autopopulate
        mySecConfig.setKeyCreator(keyCreator);

        // Now open it
        itemNameIndexDb =
            myEnv.openSecondaryDatabase(
                    null,
                    "itemNameIndex", // index name
                    inventoryDb,     // the primary db that we're indexing
                    mySecConfig);    // the secondary config
    }

   // getter methods

    // Needed for things like beginning transactions
    public Environment getEnv() {
        return myEnv;
    }

    public Database getVendorDB() {
        return vendorDb;
    }

    public Database getInventoryDB() {
        return inventoryDb;
    }

    public SecondaryDatabase getNameIndexDB() {
        return itemNameIndexDb;
    }

    public StoredClassCatalog getClassCatalog() {
        return classCatalog;
    }

    //Close the environment
    public void close() {
        if (myEnv != null) {
            try {
                //Close the secondary before closing the primaries
                itemNameIndexDb.close();
                vendorDb.close();
                inventoryDb.close();
                classCatalogDb.close();

                // Finally, close the environment.
                myEnv.close();
            } catch(DatabaseException dbe) {
                System.err.println("Error closing MyDbEnv: " +
                                    dbe.toString());
               System.exit(-1);
            }
        }
    }
}
