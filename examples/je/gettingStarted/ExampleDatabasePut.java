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

// file: ExampleDatabasePut.java

package je.gettingStarted;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Transaction;

public class ExampleDatabasePut {

    private static File myDbEnvPath = new File("/tmp/JEDB");
    private static File inventoryFile = new File("./inventory.txt");
    private static File vendorsFile = new File("./vendors.txt");

    // DatabaseEntries used for loading records
    private static DatabaseEntry theKey = new DatabaseEntry();
    private static DatabaseEntry theData = new DatabaseEntry();

    // Encapsulates the environment and databases.
    private static MyDbEnv myDbEnv = new MyDbEnv();

    private static void usage() {
        System.out.println("ExampleDatabasePut [-h <env directory>]");
        System.out.println("      [-s <selections file>] [-v <vendors file>]");
        System.exit(-1);
    }

    public static void main(String args[]) {
        ExampleDatabasePut edp = new ExampleDatabasePut();
        try {
            edp.run(args);
        } catch (DatabaseException dbe) {
            System.err.println("ExampleDatabasePut: " + dbe.toString());
            dbe.printStackTrace();
            dbe.printStackTrace();
        } catch (Exception e) {
            System.out.println("Exception: " + e.toString());
            e.printStackTrace();
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
                      false);      // is this environment read-only?

        System.out.println("loading vendors db....");
        loadVendorsDb();

        System.out.println("loading inventory db....");
        loadInventoryDb();
    }

    private void loadVendorsDb()
            throws DatabaseException {

        // loadFile opens a flat-text file that contains our data
        // and loads it into a list for us to work with. The integer
        // parameter represents the number of fields expected in the
        // file.
        List<String[]> vendors = loadFile(vendorsFile, 8);

        // Now load the data into the database. The vendor's name is the
        // key, and the data is a Vendor class object.

        // Need a serial binding for the data
        EntryBinding dataBinding =
            new SerialBinding(myDbEnv.getClassCatalog(), Vendor.class);

        for (int i = 0; i < vendors.size(); i++) {
            String[] sArray = vendors.get(i);
            Vendor theVendor = new Vendor();
            theVendor.setVendorName(sArray[0]);
            theVendor.setAddress(sArray[1]);
            theVendor.setCity(sArray[2]);
            theVendor.setState(sArray[3]);
            theVendor.setZipcode(sArray[4]);
            theVendor.setBusinessPhoneNumber(sArray[5]);
            theVendor.setRepName(sArray[6]);
            theVendor.setRepPhoneNumber(sArray[7]);

            // The key is the vendor's name.
            // ASSUMES THE VENDOR'S NAME IS UNIQUE!
            String vendorName = theVendor.getVendorName();
            try {
                theKey = new DatabaseEntry(vendorName.getBytes("UTF-8"));
            } catch (IOException willNeverOccur) {}

            // Convert the Vendor object to a DatabaseEntry object
            // using our SerialBinding
            dataBinding.objectToEntry(theVendor, theData);

            // Put it in the database. These puts are transactionally protected
            // (we're using autocommit).
            myDbEnv.getVendorDB().put(null, theKey, theData);
        }
    }

    private void loadInventoryDb()
        throws DatabaseException {

        // loadFile opens a flat-text file that contains our data
        // and loads it into a list for us to work with. The integer
        // parameter represents the number of fields expected in the
        // file.
        List<String[]> inventoryArray = loadFile(inventoryFile, 6);

        // Now load the data into the database. The item's sku is the
        // key, and the data is an Inventory class object.

        // Need a tuple binding for the Inventory class.
        TupleBinding inventoryBinding = new InventoryBinding();

        // Start a transaction. All inventory items get loaded using a
        // single transaction.
        Transaction txn = myDbEnv.getEnv().beginTransaction(null, null);

        for (int i = 0; i < inventoryArray.size(); i++) {
            String[] sArray = inventoryArray.get(i);
            String sku = sArray[1];
            try {
                theKey = new DatabaseEntry(sku.getBytes("UTF-8"));
            } catch (IOException willNeverOccur) {}

            Inventory theInventory = new Inventory();
            theInventory.setItemName(sArray[0]);
            theInventory.setSku(sArray[1]);
            theInventory.setVendorPrice((new Float(sArray[2])).floatValue());
            theInventory.setVendorInventory((new Integer(sArray[3])).intValue());
            theInventory.setCategory(sArray[4]);
            theInventory.setVendor(sArray[5]);

            // Place the Vendor object on the DatabaseEntry object using our
            // the tuple binding we implemented in InventoryBinding.java
            inventoryBinding.objectToEntry(theInventory, theData);

            // Put it in the database. Note that this causes our secondary database
            // to be automatically updated for us.
            try {
                myDbEnv.getInventoryDB().put(txn, theKey, theData);
            } catch (DatabaseException dbe) {
                try {
                System.out.println("Error putting entry " +
                                sku.getBytes("UTF-8"));
                } catch (IOException willNeverOccur) {}
                txn.abort();
                throw dbe;
            }
        }
        // Commit the transaction. The data is now safely written to the
        // inventory database.
        txn.commit();
    }

    private static void parseArgs(String args[]) {
        for(int i = 0; i < args.length; ++i) {
            if (args[i].startsWith("-")) {
                switch(args[i].charAt(1)) {
                  case 'h':
                    myDbEnvPath = new File(args[++i]);
                    break;
                  case 'i':
                    inventoryFile = new File(args[++i]);
                    break;
                  case 'v':
                    vendorsFile = new File(args[++i]);
                    break;
                  default:
                    usage();
                }
            }
        }
    }

    private List<String[]> loadFile(File theFile, int numFields) {
        List<String[]> records = new ArrayList<String[]>();
        try {
            String theLine = null;
            FileInputStream fis = new FileInputStream(theFile);
            BufferedReader br = new BufferedReader(new InputStreamReader(fis));
            while((theLine=br.readLine()) != null) {
                String[] theLineArray = theLine.split("#");
                if (theLineArray.length != numFields) {
                    System.out.println("Malformed line found in " + theFile.getPath());
                    System.out.println("Line was: '" + theLine);
                    System.out.println("length found was: " + theLineArray.length);
                    System.exit(-1);
                }
                records.add(theLineArray);
            }
            // Close the input stream handle
            fis.close();
        } catch (FileNotFoundException e) {
            System.err.println(theFile.getPath() + " does not exist.");
            e.printStackTrace();
            usage();
        } catch (IOException e)  {
            System.err.println("IO Exception: " + e.toString());
            e.printStackTrace();
            System.exit(-1);
        }
        return records;
    }

    protected ExampleDatabasePut() {}
}
