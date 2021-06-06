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

package persist.gettingStarted;

import java.io.File;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.persist.EntityCursor;

public class ExampleInventoryRead {

    private static File myDbEnvPath =
        new File("/tmp/JEDB");

    private DataAccessor da;

    // Encapsulates the database environment.
    private static MyDbEnv myDbEnv = new MyDbEnv();

    // The item to locate if the -s switch is used
    private static String locateItem;

    private static void usage() {
        System.out.println("ExampleInventoryRead [-h <env directory>]" +
                           "[-s <item to locate>]");
        System.exit(-1);
    }

    public static void main(String args[]) {
        ExampleInventoryRead eir = new ExampleInventoryRead();
        try {
            eir.run(args);
        } catch (DatabaseException dbe) {
            System.err.println("ExampleInventoryRead: " + dbe.toString());
            dbe.printStackTrace();
        } finally {
            myDbEnv.close();
        }
        System.out.println("All done.");
    }

    private void run(String args[])
        throws DatabaseException {
        // Parse the arguments list
        parseArgs(args);

        myDbEnv.setup(myDbEnvPath, // path to the environment home
                      true);       // is this environment read-only?

        // Open the data accessor. This is used to retrieve
        // persistent objects.
        da = new DataAccessor(myDbEnv.getEntityStore());

        // If a item to locate is provided on the command line,
        // show just the inventory items using the provided name.
        // Otherwise, show everything in the inventory.
        if (locateItem != null) {
            showItem();
        } else {
            showAllInventory();
        }
    }

    // Shows all the inventory items that exist for a given
    // inventory name.
    private void showItem() throws DatabaseException {

        // Use the inventory name secondary key to retrieve
        // these objects.
        EntityCursor<Inventory> items =
            da.inventoryByName.subIndex(locateItem).entities();
        try {
            for (Inventory item : items) {
                displayInventoryRecord(item);
            }
        } finally {
            items.close();
        }
    }

    // Displays all the inventory items in the store
    private void showAllInventory()
        throws DatabaseException {

        // Get a cursor that will walk every
        // inventory object in the store.
        EntityCursor<Inventory> items =
            da.inventoryBySku.entities();

        try {
            for (Inventory item : items) {
                displayInventoryRecord(item);
            }
        } finally {
            items.close();
        }
    }

    private void displayInventoryRecord(Inventory theInventory)
            throws DatabaseException {

            System.out.println(theInventory.getSku() + ":");
            System.out.println("\t " + theInventory.getItemName());
            System.out.println("\t " + theInventory.getCategory());
            System.out.println("\t " + theInventory.getVendor());
            System.out.println("\t\tNumber in stock: " +
                theInventory.getVendorInventory());
            System.out.println("\t\tPrice per unit:  " +
                theInventory.getVendorPrice());
            System.out.println("\t\tContact: ");

            Vendor theVendor =
                    da.vendorByName.get(theInventory.getVendor());
            assert theVendor != null;

            System.out.println("\t\t " + theVendor.getAddress());
            System.out.println("\t\t " + theVendor.getCity() + ", " +
                theVendor.getState() + " " + theVendor.getZipcode());
            System.out.println("\t\t Business Phone: " +
                theVendor.getBusinessPhoneNumber());
            System.out.println("\t\t Sales Rep: " +
                                theVendor.getRepName());
            System.out.println("\t\t            " +
                theVendor.getRepPhoneNumber());
    }

    protected ExampleInventoryRead() {}

    private static void parseArgs(String args[]) {
        for(int i = 0; i < args.length; ++i) {
            if (args[i].startsWith("-")) {
                switch(args[i].charAt(1)) {
                    case 'h':
                        myDbEnvPath = new File(args[++i]);
                        break;
                    case 's':
                        locateItem = args[++i];
                        break;
                    default:
                        usage();
                }
            }
        }
    }
}
