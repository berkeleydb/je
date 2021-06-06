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

import java.io.File;

import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.StoreConfig;
import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.Persistent;
import com.sleepycat.persist.model.PrimaryKey;

/* 
 * Create a database which stores abstract entity classes. This database will be 
 * used in the unit test c.s.persist.test.AddNewSecKeyToAbstractClassTest.
 */
public class CreateAbstractClassData {
    private Environment env;
    private EntityStore store;
    private PrimaryIndex<Long, AbstractEntity1> primary1;
    private PrimaryIndex<Long, AbstractEntity2> primary2;
    
    public static void main(String args[]) {
        CreateAbstractClassData epc = new CreateAbstractClassData();
        epc.open();
        epc.writeData();
        epc.close();
    }
    
    private void writeData() {
        primary1.put(null, new EntityData1(1));
        primary2.put(null, new EntityData2(1));
    }
    
    private void close() {
        store.close();
        store = null;

        env.close();
        env = null;
    }

    private void open() {

        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        File envHome = new File("./");
        env = new Environment(envHome, envConfig);
        StoreConfig config = new StoreConfig();
        config.setAllowCreate(envConfig.getAllowCreate());
        config.setTransactional(envConfig.getTransactional());
        store = new EntityStore(env, "test", config);
        primary1 = store.getPrimaryIndex(Long.class, AbstractEntity1.class);
        primary2 = store.getPrimaryIndex(Long.class, AbstractEntity2.class);
    }
    
    @Entity
    static abstract class AbstractEntity1 {
        AbstractEntity1(Long i) {
            this.id = i;
        }
        
        private AbstractEntity1(){}
        
        @PrimaryKey
        private Long id;
    }
    
    @Persistent
    static class EntityData1 extends AbstractEntity1{
        private int f1;
        
        private EntityData1(){}
        
        EntityData1(int i) {
            super(Long.valueOf(i));
            this.f1 = i;
        }
    }
    
    @Entity
    static abstract class AbstractEntity2 {
        AbstractEntity2(Long i) {
            this.id = i;
        }
        
        private AbstractEntity2(){}
        
        @PrimaryKey
        private Long id;
    }
    
    @Persistent
    static class EntityData2 extends AbstractEntity2{
        private int f1;
        
        private EntityData2(){}
        
        EntityData2(int i) {
            super(Long.valueOf(i));
            this.f1 = i;
        }
    }
}
