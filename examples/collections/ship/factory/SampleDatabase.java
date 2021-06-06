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

package collections.ship.factory;

import java.io.File;

import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.collections.TupleSerialFactory;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.ForeignKeyDeleteAction;
import com.sleepycat.je.SecondaryConfig;
import com.sleepycat.je.SecondaryDatabase;

/**
 * SampleDatabase defines the storage containers, indices and foreign keys
 * for the sample database.
 *
 * @author Mark Hayes
 */
public class SampleDatabase {

    private static final String CLASS_CATALOG = "java_class_catalog";
    private static final String SUPPLIER_STORE = "supplier_store";
    private static final String PART_STORE = "part_store";
    private static final String SHIPMENT_STORE = "shipment_store";
    private static final String SHIPMENT_PART_INDEX = "shipment_part_index";
    private static final String SHIPMENT_SUPPLIER_INDEX =
                                    "shipment_supplier_index";
    private static final String SUPPLIER_CITY_INDEX = "supplier_city_index";

    private Environment env;
    private Database partDb;
    private Database supplierDb;
    private Database shipmentDb;
    private SecondaryDatabase supplierByCityDb;
    private SecondaryDatabase shipmentByPartDb;
    private SecondaryDatabase shipmentBySupplierDb;
    private StoredClassCatalog javaCatalog;
    private TupleSerialFactory factory;

    /**
     * Open all storage containers, indices, and catalogs.
     */
    public SampleDatabase(String homeDirectory)
        throws DatabaseException {

        // Open the Berkeley DB environment in transactional mode.
        //
        System.out.println("Opening environment in: " + homeDirectory);
        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setTransactional(true);
        envConfig.setAllowCreate(true);
        env = new Environment(new File(homeDirectory), envConfig);

        // Set the Berkeley DB config for opening all stores.
        //
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(true);
        dbConfig.setAllowCreate(true);

        // Create the Serial class catalog.  This holds the serialized class
        // format for all database records of serial format.
        //
        Database catalogDb = env.openDatabase(null, CLASS_CATALOG, dbConfig);
        javaCatalog = new StoredClassCatalog(catalogDb);

        // Use the TupleSerialDbFactory for a Serial/Tuple-based database
        // where marshalling interfaces are used.
        //
        factory = new TupleSerialFactory(javaCatalog);

        // Open the Berkeley DB database for the part, supplier and shipment
        // stores.  The stores are opened with no duplicate keys allowed.
        //
        partDb = env.openDatabase(null, PART_STORE, dbConfig);

        supplierDb = env.openDatabase(null, SUPPLIER_STORE, dbConfig);

        shipmentDb = env.openDatabase(null, SHIPMENT_STORE, dbConfig);

        // Open the SecondaryDatabase for the city index of the supplier store,
        // and for the part and supplier indices of the shipment store.
        // Duplicate keys are allowed since more than one supplier may be in
        // the same city, and more than one shipment may exist for the same
        // supplier or part.  A foreign key constraint is defined for the
        // supplier and part indices to ensure that a shipment only refers to
        // existing part and supplier keys.  The CASCADE delete action means
        // that shipments will be deleted if their associated part or supplier
        // is deleted.
        //
        SecondaryConfig secConfig = new SecondaryConfig();
        secConfig.setTransactional(true);
        secConfig.setAllowCreate(true);
        secConfig.setSortedDuplicates(true);

        secConfig.setKeyCreator(factory.getKeyCreator(Supplier.class,
                                                      Supplier.CITY_KEY));
        supplierByCityDb = env.openSecondaryDatabase(null, SUPPLIER_CITY_INDEX,
                                                     supplierDb, secConfig);

        secConfig.setForeignKeyDatabase(partDb);
        secConfig.setForeignKeyDeleteAction(ForeignKeyDeleteAction.CASCADE);
        secConfig.setKeyCreator(factory.getKeyCreator(Shipment.class,
                                                      Shipment.PART_KEY));
        shipmentByPartDb = env.openSecondaryDatabase(null, SHIPMENT_PART_INDEX,
                                                     shipmentDb, secConfig);

        secConfig.setForeignKeyDatabase(supplierDb);
        secConfig.setForeignKeyDeleteAction(ForeignKeyDeleteAction.CASCADE);
        secConfig.setKeyCreator(factory.getKeyCreator(Shipment.class,
                                                      Shipment.SUPPLIER_KEY));
        shipmentBySupplierDb = env.openSecondaryDatabase(null,
                                                     SHIPMENT_SUPPLIER_INDEX,
                                                     shipmentDb, secConfig);
    }

    /**
     * Return the tuple-serial factory.
     */
    public final TupleSerialFactory getFactory() {

        return factory;
    }

    /**
     * Return the storage environment for the database.
     */
    public final Environment getEnvironment() {

        return env;
    }

    /**
     * Return the class catalog.
     */
    public final StoredClassCatalog getClassCatalog() {

        return javaCatalog;
    }

    /**
     * Return the part storage container.
     */
    public final Database getPartDatabase() {

        return partDb;
    }

    /**
     * Return the supplier storage container.
     */
    public final Database getSupplierDatabase() {

        return supplierDb;
    }

    /**
     * Return the shipment storage container.
     */
    public final Database getShipmentDatabase() {

        return shipmentDb;
    }

    /**
     * Return the shipment-by-part index.
     */
    public final SecondaryDatabase getShipmentByPartDatabase() {

        return shipmentByPartDb;
    }

    /**
     * Return the shipment-by-supplier index.
     */
    public final SecondaryDatabase getShipmentBySupplierDatabase() {

        return shipmentBySupplierDb;
    }

    /**
     * Return the supplier-by-city index.
     */
    public final SecondaryDatabase getSupplierByCityDatabase() {

        return supplierByCityDb;
    }

    /**
     * Close all databases and the environment.
     */
    public void close()
        throws DatabaseException {

        // Close secondary databases, then primary databases.
        supplierByCityDb.close();
        shipmentByPartDb.close();
        shipmentBySupplierDb.close();
        partDb.close();
        supplierDb.close();
        shipmentDb.close();
        // And don't forget to close the catalog and the environment.
        javaCatalog.close();
        env.close();
    }
}
