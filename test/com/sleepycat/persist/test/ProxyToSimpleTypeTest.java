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

package com.sleepycat.persist.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.StoreConfig;
import com.sleepycat.persist.model.AnnotationModel;
import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.EntityModel;
import com.sleepycat.persist.model.Persistent;
import com.sleepycat.persist.model.PersistentProxy;
import com.sleepycat.persist.model.PrimaryKey;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;
import com.sleepycat.util.test.TestEnv;

/**
 * BigDecimal is not a built-in SimpleType before je-4.1 version. The 
 * application using previous je versions will use BigDecimalProxy to store 
 * BigDecimal data. Therefore, we need to test if the BigDecimal data stored by
 * BigDecimal proxy can be correctly read and updated by built-in BigDecimal 
 * format in je-4.1 version.
 *
 * We generate a database using je-4.0.103, which will contain a record. The
 * record has BigDecimal data, which is stored by BigDecimalProxy:
 *
 *      @Entity
 *      static class BigDecimalData {
 *          @PrimaryKey
 *          private int id;
 *          private BigDecimal f1;
 *        
 *          BigDecimalData() { }
 *        
 *          BigDecimalData(int id, BigDecimal f1) {
 *              this.id = id;
 *              this.f1 = f1;
 *          }
 *        
 *          int getId() {
 *              return id;
 *          }
 *        
 *          BigDecimal getF1() {
 *              return f1;
 *          }
 *      }
 *    
 *      @Persistent(proxyFor=BigDecimal.class)
 *      static class BigDecimalProxy
 *      implements PersistentProxy<BigDecimal> {
 *
 *          private String rep;
 *          private BigDecimalProxy() {}
 *          public BigDecimal convertProxy() {
 *              return new BigDecimal(rep);
 *          }
 *
 *          public void initializeProxy(BigDecimal o) {
 *              rep = o.toString();
 *          }
 *      }
 *
 * The record stored is {1, new BigDecimal("123.1234000")}.
 *
 * This test should be excluded from the BDB build because it uses a stored JE
 * log file.
 */
public class ProxyToSimpleTypeTest extends TestBase {

    private static final String STORE_NAME = "test";

    private File envHome;
    private Environment env;
    private EntityStore store;

    @Before
    public void setUp() 
        throws Exception {
        
        envHome = SharedTestUtils.getTestDir();
        super.setUp();
    }

    @After
    public void tearDown() {
        if (store != null) {
            try {
                store.close();
            } catch (DatabaseException e) {
                System.out.println("During tearDown: " + e);
            }
        }
        if (env != null) {
            try {
                env.close();
            } catch (DatabaseException e) {
                System.out.println("During tearDown: " + e);
            }
        }
        try {
            TestUtils.removeLogFiles("TearDown", envHome, false);
        } catch (Error e) {
            System.out.println("During tearDown: " + e);
        }
        envHome = null;
        store = null;
        env = null;
    }

    private void open(boolean registerProxy)
        throws DatabaseException {

        EnvironmentConfig envConfig = TestEnv.BDB.getConfig();
        envConfig.setAllowCreate(true);
        env = new Environment(envHome, envConfig);

        StoreConfig storeConfig = new StoreConfig();
        storeConfig.setAllowCreate(true);
        if (registerProxy) {       
            EntityModel model = new AnnotationModel();
            model.registerClass(BigDecimalProxy.class);
            storeConfig.setModel(model);
        }
        store = new EntityStore(env, STORE_NAME, storeConfig);
    }

    private void close()
        throws DatabaseException {

        if (store != null) {
            store.close();
            store = null;
        }
        if (env != null) {
            env.close();
            env = null;
        }
    }

    @Test
    public void testReadOldVersionBigDecimalByProxy()
        throws IOException {

        /* Copy log file resource to log file zero. */
        TestUtils.loadLog(getClass(), "je-4.0.103_BigDecimal.jdb", envHome);

        /* We do not register BigDecimalProxy. */
        open(false /* registerProxy */);
        PrimaryIndex<Integer, BigDecimalData> primary = 
            store.getPrimaryIndex(Integer.class, BigDecimalData.class);
        BigDecimalData entity = primary.get(1);
        assertNotNull(entity);
        
        /* The precision will be preserved in the old version BigDecimal. */
        assertEquals(new BigDecimal("123.1234000"), entity.getF1());
        close();
    }
    
    /* 
     * SimpleForamt (FBigDec) will be used to update the data. Then new data
     * will be read also by SimpleForamt (FBigDec).
     */
    @Test
    public void testWriteReadSortedBigDecimal() 
        throws IOException {
        
        /* Copy log file resource to log file zero. */
        TestUtils.loadLog(getClass(), "je-4.0.103_BigDecimal.jdb", envHome);

        open(false /* registerProxy */);

        PrimaryIndex<Integer, BigDecimalData> primary = 
            store.getPrimaryIndex(Integer.class, BigDecimalData.class);
        
        /* 
         * DPL will use FBigDec format to write the BigDecimal in sorted 
         * BigDecimal.
         */
        primary.put(null, 
                    new BigDecimalData (1, new BigDecimal("1234.1234000")));
        
        /* 
         * DPL will use FBigDec format to read the BigDecimal in sorted 
         * BigDecimal.
         */
        BigDecimalData entity = primary.get(1);
        assertNotNull(entity);
        
        /* Sorted BigDecimal cannot preserve precision. */
        assertEquals(new BigDecimal("1234.1234"), entity.getF1());
        close();
        
        /* Re-open and read the data again. */
        open(false /*registerProxy*/);
        primary = store.getPrimaryIndex(Integer.class, BigDecimalData.class);
        
        /* 
         * In the future, DPL will use FBigDec format to read the BigDecimal in 
         * sorted BigDecimal.
         */
        entity = primary.get(1);
        assertNotNull(entity);
        
        /* Sorted BigDecimal cannot preserve precision. */
        assertEquals(new BigDecimal("1234.1234"), entity.getF1());
        close();
    }
    
    /* 
     * If register proxy for SimpleType, IllegalArgumentException will be 
     * thrown.
     */
    @Test
    public void testRegisterProxyForSimpleType() 
        throws IOException {
        
        /* Copy log file resource to log file zero. */
        TestUtils.loadLog(getClass(), "je-4.0.103_BigDecimal.jdb", envHome);
        try {
            open(true /* registerProxy */);
            fail();
        } catch (IllegalArgumentException e) {
            /* We expect the exception. */
        }
        close();
    }
    
    @Entity
    static class BigDecimalData {
		@PrimaryKey
		private int id;
		private BigDecimal f1;
		
		BigDecimalData() { }
		
		BigDecimalData(int id, BigDecimal f1) {
			this.id = id;
			this.f1 = f1;
		}
		
		int getId() {
			return id;
		}
		
		BigDecimal getF1() {
			return f1;
		}
	}
    
    @Persistent(proxyFor=BigDecimal.class)
    static class BigDecimalProxy
        implements PersistentProxy<BigDecimal> {

        private String rep;
        private BigDecimalProxy() {}
        
        public BigDecimal convertProxy() {
            return new BigDecimal(rep);
        }

        public void initializeProxy(BigDecimal o) {
            rep = o.toString();
        }
    }
}
