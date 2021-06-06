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

import static com.sleepycat.persist.model.Relationship.MANY_TO_ONE;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.StoreConfig;
import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.KeyField;
import com.sleepycat.persist.model.Persistent;
import com.sleepycat.persist.model.PrimaryKey;
import com.sleepycat.persist.model.SecondaryKey;

/* 
 * Create a database which stores StringData classes. This database will be 
 * used in the unit test c.s.persist.test.StringFormatCompatibilityTest.
 */
public class CreateStringDataDB {
    Environment env;
    private EntityStore store;
    private PrimaryIndex<String, StringData> primary;
    
    public static void main(String args[]) {
    	CreateStringDataDB csd = new CreateStringDataDB();
        csd.open();
        csd.writeData();
        csd.close();
    }
    
    private void writeData() {
        CompositeKey compK = new CompositeKey("CompKey1_1", "CompKey1_2");
        CompositeKey compK2 = new CompositeKey("CompKey2_1", "CompKey2_2");
        String[] f3 = {"f3_1", "f3_2"};
        List<String> f4 = new ArrayList<String>();
        f4.add("f4_1");
        f4.add("f4_2");
        primary.put
            (null, new StringData ("pk1", "sk1", compK, "f1", "f2", f3, f4));
        f4.clear();
        f4.add("f4_1_2");
        f4.add("f4_2_2");
        primary.put
            (null, new StringData ("pk2", "sk2", compK2, "f1", "f2", f3, f4));
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
        primary = store.getPrimaryIndex(String.class, StringData.class);
    }

    @Entity
    static class StringData {
        @PrimaryKey
        private String pk;
        @SecondaryKey (relate = MANY_TO_ONE)
        private String sk1;
        @SecondaryKey (relate = MANY_TO_ONE)
        private CompositeKey sk2;
        private String f1;
        private String f2;
        private String[] f3;
        private List<String> f4;
        
        StringData() { }

        StringData(String pk, String sk1, CompositeKey sk2, String f1, 
                   String f2, String[] f3, List<String> f4) {
            this.pk = pk;
            this.sk1 = sk1;
            this.sk2 = sk2;
            this.f1 = f1;
            this.f2 = f2;
            this.f3 = f3;
            this.f4 = f4;
        }
        
        String getPK() {
            return pk;
        }
        
        String getSK1() {
            return sk1;
        }
        
        CompositeKey getSK2() {
            return sk2;
        }
        
        String getF1() {
            return f1;
        }
        
        String getF2() {
            return f2;
        }
        
        String[] getF3() {
            return f3;
        }
        
        List<String> getF4() {
            return f4;
        }
    }
    
    @Persistent
    static class CompositeKey {
        @KeyField(2)
        private String f1;
        @KeyField(1)
        private String f2;

        private CompositeKey() {}

        CompositeKey(String f1, String f2) {
            this.f1 = f1;
            this.f2 = f2;
        }
        
        String getF1() {
            return f1;
        }
        
        String getF2() {
            return f2;
        }
    }
}
