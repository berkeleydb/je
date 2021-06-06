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

package jca.simple;

import javax.ejb.SessionBean;
import javax.ejb.SessionContext;
import javax.naming.Context;
import javax.naming.InitialContext;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.SecondaryConfig;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.SecondaryKeyCreator;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.jca.ra.JEConnection;
import com.sleepycat.je.jca.ra.JEConnectionFactory;

public class SimpleBean implements SessionBean {

    /*
     * Set this to something appropriate for your environment.  Make sure it
     * matches the ra.xml.
     */
    private final String JE_ENV = "/export/home/cwl/work-jca/je_store";
    private final boolean TRANSACTIONAL = true;

    private SessionContext sessionCtx;

    public void ejbCreate() {
    }

    public void ejbRemove() {
    }

    public void setSessionContext(SessionContext context) {
        sessionCtx = context;
    }

    public void unsetSessionContext() {
        sessionCtx = null;
    }

    public void ejbActivate() {
    }

    public void ejbPassivate() {
    }

    public void put(String key, String data) {
        try {
            @SuppressWarnings("unused")
            Environment env = null;
            @SuppressWarnings("unused")
            Transaction txn = null;
            Database db = null;
            @SuppressWarnings("unused")
            SecondaryDatabase secDb = null;
            Cursor cursor = null;
            JEConnection dc = null;
            try {
                dc = getConnection(JE_ENV);

                env = dc.getEnvironment();
                DatabaseConfig dbConfig = new DatabaseConfig();
                SecondaryConfig secDbConfig = new SecondaryConfig();
                dbConfig.setAllowCreate(true);
                dbConfig.setTransactional(TRANSACTIONAL);
                secDbConfig.setAllowCreate(true);
                secDbConfig.setTransactional(TRANSACTIONAL);
                secDbConfig.setKeyCreator(new MyKeyCreator());

                /*
                 * Use JEConnection.openDatabase() to obtain a cached Database
                 * handle.  Do not call close() on Database handles obtained
                 * using this method.
                 */
                db = dc.openDatabase("db", dbConfig);
                secDb = dc.openSecondaryDatabase("secDb", db, secDbConfig);
                cursor = db.openCursor(null, null);
                cursor.put(new DatabaseEntry(key.getBytes("UTF-8")),
                           new DatabaseEntry(data.getBytes("UTF-8")));
            } finally {
                if (cursor != null) {
                    cursor.close();
                }
                if (dc != null) {
                    dc.close();
                }
            }
        } catch (Exception e) {
            System.err.println("Failure in put" + e);
        }
    }

    public void removeDatabase() {
        try {
            JEConnection dc = null;
            try {
                dc = getConnection(JE_ENV);

                DatabaseConfig dbConfig = new DatabaseConfig();
                dbConfig.setAllowCreate(true);
                dbConfig.setTransactional(TRANSACTIONAL);

                /*
                 * Once you have removed a database from the environment,
                 * do not try to open it anymore.
                 */
                dc.removeDatabase("db");
            } finally {
                if (dc != null) {
                    dc.close();
                }
            }
        } catch (Exception e) {
            System.err.println("Failure in remove " + e);
            e.printStackTrace();
        }
    }

    public String get(String key) {
        try {
            @SuppressWarnings("unused")
            Environment env = null;
            @SuppressWarnings("unused")
            Transaction txn = null;
            Database db = null;
            Cursor cursor = null;
            JEConnection dc = null;
            try {
                dc = getConnection(JE_ENV);

                env = dc.getEnvironment();
                DatabaseConfig dbConfig = new DatabaseConfig();
                dbConfig.setAllowCreate(true);
                dbConfig.setTransactional(TRANSACTIONAL);

                /*
                 * Use JEConnection.openDatabase() to obtain a cached Database
                 * handle.  Do not call close() on Database handles obtained
                 * using this method.
                 */
                db = dc.openDatabase("db", dbConfig);
                cursor = db.openCursor(null, null);
                DatabaseEntry data = new DatabaseEntry();
                cursor.getSearchKey(new DatabaseEntry(key.getBytes("UTF-8")),
                                    data,
                                    null);
                return new String(data.getData(), "UTF-8");
            } finally {
                if (cursor != null) {
                    cursor.close();
                }
                if (dc != null) {
                    dc.close();
                }
            }
        } catch (Exception e) {
            System.err.println("Failure in get" + e);
            e.printStackTrace();
        }
        return null;
    }

    private JEConnection getConnection(String envDir) {
        try {
            EnvironmentConfig envConfig = new EnvironmentConfig();
            envConfig.setTransactional(true);
            envConfig.setAllowCreate(true);
            InitialContext iniCtx = new InitialContext();
            Context enc = (Context) iniCtx.lookup("java:comp/env");
            Object ref = enc.lookup("ra/JEConnectionFactory");
            JEConnectionFactory dcf = (JEConnectionFactory) ref;
            JEConnection dc = dcf.getConnection(envDir, envConfig);
            return dc;
        } catch(Exception e) {
            System.err.println("Failure in getConnection " + e);
        }
        return null;
    }

    private static class MyKeyCreator implements SecondaryKeyCreator {

        MyKeyCreator() {
        }

        public boolean createSecondaryKey(SecondaryDatabase secondaryDb,
                                          DatabaseEntry keyEntry,
                                          DatabaseEntry dataEntry,
                                          DatabaseEntry resultEntry) {
            return false;
        }
    }
}
