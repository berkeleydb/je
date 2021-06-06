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

package persist.sqlapp;

import java.io.File;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.ForwardCursor;
import com.sleepycat.persist.StoreConfig;

/**
 * An example shows how some common SQL queries are implemented using DPL.
 *
 * @see #usage
 *
 * @author chao
 */
public class SQLApp {

    private static String envDir = "./tmp";
    private static boolean cleanEnvOnExit = false;
    private static Environment env = null;
    private static EntityStore store = null;
    private static DataAccessor dao = null;

    /**
     * Setup a Berkeley DB engine environment, and preload some example records.
     *
     * @throws com.sleepycat.je.DatabaseException
     */
    public void setup()
        throws DatabaseException {

        /* Open a transactional Berkeley DB engine environment. */
        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        envConfig.setTransactional(true);
        env = new Environment(new File(envDir), envConfig);

        /* Open a transactional entity store. */
        StoreConfig storeConfig = new StoreConfig();
        storeConfig.setAllowCreate(true);
        storeConfig.setTransactional(true);
        store = new EntityStore(env, "SQLAppStore", storeConfig);

        /* Initialize the data access object. */
        dao = new DataAccessor(store);

        /* Preload some example records. */
        loadDepartmentDb();
        loadEmployeeDb();
    }

    /* Load the department database. */
    private void loadDepartmentDb()
        throws DatabaseException {

        dao.departmentById.put
                (new Department(1, "CEO Office", "North America"));
        dao.departmentById.put
                (new Department(2, "Sales", "EURO"));
        dao.departmentById.put
                (new Department(3, "HR", "MEA"));
        dao.departmentById.put
                (new Department(4, "Engineering", "APAC"));
        dao.departmentById.put
                (new Department(5, "Support", "LATAM"));
    }

    /* Load the employee database. */
    private void loadEmployeeDb()
        throws DatabaseException {

        /* Add the corporate's CEO using the Employee primary index. */
        dao.employeeById.put(new Employee(1, // employeeId
                                          "Abraham Lincoln", //employeeName
                                          10000.0f, //salary
                                          null, //managerId
                                          1, //departmentId
                                          "Washington D.C., USA")); //address

        /* Add 4 managers responsible for 4 departments. */
        dao.employeeById.put(new Employee(2,
                                          "Augustus",
                                          9000.0f,
                                          1,
                                          2,
                                          "Rome, Italy"));
        dao.employeeById.put(new Employee(3,
                                          "Cleopatra",
                                          7000.0f,
                                          1,
                                          3,
                                          "Cairo, Egypt"));
        dao.employeeById.put(new Employee(4,
                                          "Confucius",
                                          7500.0f,
                                          1,
                                          4,
                                          "Beijing, China"));
        dao.employeeById.put(new Employee(5,
                                          "Toussaint Louverture",
                                          6800.0f,
                                          1,
                                          5,
                                          "Port-au-Prince, Haiti"));

        /* Add 2 employees per department. */
        dao.employeeById.put(new Employee(6,
                                          "William Shakespeare",
                                          7300.0f,
                                          2,
                                          2,
                                          "London, England"));
        dao.employeeById.put(new Employee(7,
                                          "Victor Hugo",
                                          7000.0f,
                                          2,
                                          2,
                                          "Paris, France"));
        dao.employeeById.put(new Employee(8,
                                          "Yitzhak Rabin",
                                          6500.0f,
                                          3,
                                          3,
                                          "Jerusalem, Israel"));
        dao.employeeById.put(new Employee(9,
                                          "Nelson Rolihlahla Mandela",
                                          6400.0f,
                                          3,
                                          3,
                                          "Cape Town, South Africa"));
        dao.employeeById.put(new Employee(10,
                                          "Meiji Emperor",
                                          6600.0f,
                                          4,
                                          4,
                                          "Tokyo, Japan"));
        dao.employeeById.put(new Employee(11,
                                          "Mohandas Karamchand Gandhi",
                                          7600.0f,
                                          4,
                                          4,
                                          "New Delhi, India"));
        dao.employeeById.put(new Employee(12,
                                          "Ayrton Senna da Silva",
                                          5600.0f,
                                          5,
                                          5,
                                          "Brasilia, Brasil"));
        dao.employeeById.put(new Employee(13,
                                          "Ronahlinho De Assis Moreira",
                                          6100.0f,
                                          5,
                                          5,
                                          "Brasilia, Brasil"));
    }

    /** Run the SQL examples. */
    public void runApp()
        throws DatabaseException {

        /* Print departmemt database contents order by departmentId. */
        System.out.println("SELECT * FROM department ORDER BY departmentId;");
        printQueryResults(dao.departmentById.entities());

        /* Print departmemt database contents order by departmentName. */
        System.out.println("SELECT * FROM department " +
                           "ORDER BY departmentName;");
        printQueryResults(dao.departmentByName.entities());

        /* Print employee database contents order by employeeId. */
        System.out.println("SELECT * FROM employee ORDER BY employeeId;");
        printQueryResults(dao.employeeById.entities());

        /* Do a prefix query. */
        System.out.println("SELECT * FROM employee WHERE employeeName " +
                           "LIKE 'M%';");
        printQueryResults(dao.doPrefixQuery(dao.employeeByName, "M"));

        /* Do a range query. */
        System.out.println("SELECT * FROM employee WHERE salary >= 6000 AND " +
                           "salary <= 8000;");
        printQueryResults(dao.doRangeQuery(dao.employeeBySalary,
                                           new Float(6000), //fromKey
                                           true, //fromInclusive
                                           new Float(8000), //toKey
                                           true)); //toInclusive

        /* Two conditions join on a single primary database. */
        System.out.println("SELECT * FROM employee " +
                           "WHERE employeeName = 'Victor Hugo' " +
                           "AND departmentId = 2;");
        printQueryResults(dao.doTwoConditionsJoin(dao.employeeById,
                                                  dao.employeeByName,
                                                  new String("Victor Hugo"),
                                                  dao.employeeByDepartmentId,
                                                  new Integer(2)));

        /*
         * Two conditions join on two primary databases combined with a
         * secondary key search.
         */
        System.out.println("SELECT * FROM employee e, department d " +
                           "WHERE e.departmentId = d.departmentId " +
                           "AND d.departmentName = 'Engineering';");
        dao.getEmployeeByDeptName("Engineering");
        
        /*
         * Two conditions join on two primary databases combined with a
         * filtering on the non-secondary-key.
         */
        System.out.println("SELECT * FROM employee e, department d " +
                           "WHERE e.departmentId = d.departmentId " +
                           "AND d.location = 'North America';");
        dao.getEmployeeByDeptLocation("North America");
    }

    /** Print query results. */
    public <V> void printQueryResults(ForwardCursor<V> cursor)
        throws DatabaseException {

        try {
            for (Object entity : cursor) {
                System.out.println(entity);
            }
            System.out.println();
        } finally {
            cursor.close();
        }
    }

    /**
     * Close the store and environment.
     */
    public void close() {

        if (store != null) {
            try {
                store.close();
            } catch (DatabaseException dbe) {
                System.err.println("Error closing store: " +
                        dbe.toString());
                System.exit(-1);
            }
        }

        if (env != null) {
            try {
                // Finally, close environment.
                env.close();
            } catch (DatabaseException dbe) {
                System.err.println("Error closing env: " +
                        dbe.toString());
                System.exit(-1);
            }
        }

        if (cleanEnvOnExit) {
            removeDbFiles();
        }
    }

    private void removeDbFiles() {
        File file = new File(envDir);
        for (File f : file.listFiles()) {
            f.delete();
        }
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        
        /* Parse the arguments list. */
        parseArgs(args);

        try {
            SQLApp s = new SQLApp();
            s.setup();
            s.runApp();
            s.close();
        } catch (DatabaseException e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    /* Parse input arguments. */
    private static void parseArgs(String args[]) {
        for(int i = 0; i < args.length; ++i) {
            if (args[i].startsWith("-")) {
                switch(args[i].charAt(1)) {
                  case 'h':
                    envDir = args[++i];
                    break;
                  case 'd':
                    cleanEnvOnExit = true;
                    break;
                  default:
                    usage();
                }
            }
        }
    }

    private static void usage() {
        System.out.println("Usage: java SQLApp" +
                           "\n [-h <env directory>] " +
                           "# environment home directory" +
                           "\n [-d] # clean environment after program exits");
        System.exit(-1);
    }
}
