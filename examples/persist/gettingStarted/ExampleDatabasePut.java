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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import com.sleepycat.je.DatabaseException;

public class ExampleDatabasePut {

    private static File myDbEnvPath = new File("/tmp/JEDB");
    private static File inventoryFile = new File("./inventory.txt");
    private static File vendorsFile = new File("./vendors.txt");

    private DataAccessor da;
    
    // Encapsulates the environment and data store.
    private static MyDbEnv myDbEnv = new MyDbEnv();
    
    private static void usage() {
        System.out.println("ExampleDatabasePut [-h <env directory>]");
        System.out.println("      [-i <inventory file>] [-v <vendors file>]");
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

        myDbEnv.setup(myDbEnvPath,  // Path to the environment home 
                      false);       // Environment read-only?
        
        // Open the data accessor. This is used to store
        // persistent objects.
        da = new DataAccessor(myDbEnv.getEntityStore());
       
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
        List vendors = loadFile(vendorsFile, 8);
        
        // Now load the data into the store.
        for (int i = 0; i < vendors.size(); i++) {
            String[] sArray = (String[])vendors.get(i);
            Vendor theVendor = new Vendor();
            theVendor.setVendorName(sArray[0]);
            theVendor.setAddress(sArray[1]);
            theVendor.setCity(sArray[2]);
            theVendor.setState(sArray[3]);
            theVendor.setZipcode(sArray[4]);
            theVendor.setBusinessPhoneNumber(sArray[5]);
            theVendor.setRepName(sArray[6]);
            theVendor.setRepPhoneNumber(sArray[7]);
            
            // Put it in the store. Because we do not explicitly set
            // a transaction here, and because the store was opened
            // with transactional support, auto commit is used for each
            // write to the store.
            da.vendorByName.put(theVendor);
        }
    }
    
    private void loadInventoryDb() 
        throws DatabaseException {
        
        // loadFile opens a flat-text file that contains our data
        // and loads it into a list for us to work with. The integer
        // parameter represents the number of fields expected in the
        // file.
        List inventoryArray = loadFile(inventoryFile, 6);
        
        // Now load the data into the store. The item's sku is the
        // key, and the data is an Inventory class object.
        
        for (int i = 0; i < inventoryArray.size(); i++) {
            String[] sArray = (String[])inventoryArray.get(i);
            String sku = sArray[1];
            
            Inventory theInventory = new Inventory();
            theInventory.setItemName(sArray[0]);
            theInventory.setSku(sArray[1]);
            theInventory.setVendorPrice((new Float(sArray[2])).floatValue());
            theInventory.setVendorInventory((new Integer(sArray[3])).intValue());
            theInventory.setCategory(sArray[4]);
            theInventory.setVendor(sArray[5]);

            // Put it in the store. Note that this causes our secondary key
            // to be automatically updated for us.
            da.inventoryBySku.put(theInventory);
        }
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

    private List loadFile(File theFile, int numFields) {
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
