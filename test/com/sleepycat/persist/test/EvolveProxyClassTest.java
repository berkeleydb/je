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

import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import junit.framework.TestCase;

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
import com.sleepycat.persist.evolve.Conversion;
import com.sleepycat.persist.evolve.Converter;
import com.sleepycat.persist.evolve.Deleter;
import com.sleepycat.persist.evolve.Mutations;
import com.sleepycat.persist.evolve.Renamer;
import com.sleepycat.persist.model.AnnotationModel;
import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.EntityModel;
import com.sleepycat.persist.model.Persistent;
import com.sleepycat.persist.model.PersistentProxy;
import com.sleepycat.persist.model.PrimaryKey;
import com.sleepycat.persist.raw.RawObject;
import com.sleepycat.persist.raw.RawType;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;
import com.sleepycat.util.test.TestEnv;

/**
 * If make changes to a PersistentProxy class, a class converter is needed to 
 * provide backwards compatibility [#19312].
 *
 * We generate a database which stores proxied data using je-4.0.103. Then we
 * make change to the proxy class, and provide mutations for such change. 
 * This unit test serves test all kinds of mutations for the proxy class.
 *
 * The old version proxy class:
 * 
 *    @Persistent(proxyFor=Locale.class)
 *    static class LocaleProxy implements PersistentProxy<Locale> {
 *        
 *        String language;
 *        String country;
 *        String variant;
 *
 *        private LocaleProxy() {}
 *
 *        public void initializeProxy(Locale object) {
 *            language = object.getLanguage();
 *            country = object.getCountry();
 *            variant = object.getVariant();
 *        }
 *
 *        public Locale convertProxy() {
 *            return new Locale(language, country, variant);
 *        }
 *    }
 *
 */
public class EvolveProxyClassTest extends TestBase {

    private static final String STORE_NAME = "test";

    private File envHome;
    private Environment env;
    private EntityStore store;
    private PrimaryIndex<Integer, LocaleData> primary;
    private enum mutationTypes { FIELD_CONVERSION, DELETE_FIELD, 
                                 CLASS_CONVERSION, HIERARCHY_CONVERSION, };

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
        envHome = null;
        store = null;
        env = null;
    }

    private void open(mutationTypes muType)
        throws DatabaseException {
        
        EnvironmentConfig envConfig = TestEnv.BDB.getConfig();
        envConfig.setAllowCreate(true);
        env = new Environment(envHome, envConfig);
        EntityModel model = new AnnotationModel();
        Mutations mutations = new Mutations();
        Renamer classRenamer;
        switch (muType) {
            case DELETE_FIELD :
                model.registerClass(LocaleProxy_DeleteField.class);
                classRenamer = new Renamer
                    ("com.sleepycat.persist.test.EvolveProxyClassTest$" + 
                     "LocaleProxy", 0,
                     LocaleProxy_DeleteField.class.getName());
                Deleter deleteField = new Deleter
                    ("com.sleepycat.persist.test.EvolveProxyClassTest$" + 
                     "LocaleProxy", 0, "variant");
                mutations.addRenamer(classRenamer);
                mutations.addDeleter(deleteField);
                break;
            case CLASS_CONVERSION :
                model.registerClass(LocaleProxy_ClassConversion.class);
                Converter classConverter = new Converter
                    ("com.sleepycat.persist.test.EvolveProxyClassTest$" +
                     "LocaleProxy", 0, new MyConversion_ClassConversion());
                classRenamer = new Renamer
                    ("com.sleepycat.persist.test.EvolveProxyClassTest$" + 
                     "LocaleProxy", 0, 
                     LocaleProxy_ClassConversion.class.getName());
                mutations.addRenamer(classRenamer);
                mutations.addConverter(classConverter);
                break;
            case FIELD_CONVERSION :
                model.registerClass(LocaleProxy_FieldConversion.class);
                classRenamer = new Renamer
                    ("com.sleepycat.persist.test.EvolveProxyClassTest$" + 
                     "LocaleProxy", 0,
                     LocaleProxy_FieldConversion.class.getName());
                Converter fieldConverter = new Converter
                    ("com.sleepycat.persist.test.EvolveProxyClassTest$" +
                     "LocaleProxy", 0, "language",
                     new MyConversion_FieldConversion());
                mutations.addRenamer(classRenamer);
                mutations.addConverter(fieldConverter);
                break;
            case HIERARCHY_CONVERSION :
                model.registerClass(LocaleProxy_HierarchyConversion.class);
                Converter hierarchyConverter = new Converter
                    ("com.sleepycat.persist.test.EvolveProxyClassTest$" +
                     "LocaleProxy", 0, new MyConversion_HierarchyConversion());
                classRenamer = new Renamer
                    ("com.sleepycat.persist.test.EvolveProxyClassTest$" + 
                     "LocaleProxy", 0, 
                     LocaleProxy_HierarchyConversion.class.getName());
                mutations.addRenamer(classRenamer);
                mutations.addConverter(hierarchyConverter);
                break;
            default: break;
        }
        StoreConfig storeConfig = new StoreConfig();
        storeConfig.setAllowCreate(envConfig.getAllowCreate());
        storeConfig.setTransactional(envConfig.getTransactional());
        storeConfig.setModel(model);
        storeConfig.setMutations(mutations);
        store = new EntityStore(env, STORE_NAME, storeConfig);
        primary = store.getPrimaryIndex(Integer.class, LocaleData.class);
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
    public void testDeleteFieldForProxyClass()
        throws IOException {
    
        evolveProxyClassTest(mutationTypes.DELETE_FIELD);
    }

    @Test
    public void testClassConversionForProxyClass()
        throws IOException {

        evolveProxyClassTest(mutationTypes.CLASS_CONVERSION);
    }
    
    @Test
    public void testFieldConversionForProxyClass()
        throws IOException {
    
        evolveProxyClassTest(mutationTypes.FIELD_CONVERSION);
    }
    
    @Test
    public void testHierarchyConversionForProxyClass()
        throws IOException {
    
        evolveProxyClassTest(mutationTypes.HIERARCHY_CONVERSION);
    }
    
    private void evolveProxyClassTest(mutationTypes muType) 
        throws IOException {
        /* Copy log file resource to log file zero. */
        TestUtils.loadLog(getClass(), "je-4.0.103_EvolveProxyClass.jdb", 
                          envHome);

        open(muType);
        LocaleData entity = primary.get(1);
        assertNotNull(entity);
        String variant;
        if (muType == mutationTypes.DELETE_FIELD) {
            variant = "";
        } else {
            variant = "A";
        }
        entity.validate
            (new LocaleData (1, new Locale("English", "America", variant)));
        close();
    }
    
    @Entity
    static class LocaleData {
        @PrimaryKey
        private int id;
        private Locale f1;
        
        LocaleData() { }
        
        LocaleData(int id, Locale f1) {
            this.id = id;
            this.f1 = f1;
        }
        
        int getId() {
            return id;
        }
        
        Locale getF1() {
            return f1;
        }
        
        public void validate(Object other) {
            LocaleData o = (LocaleData) other;
            TestCase.assertEquals(f1.getCountry(), o.f1.getCountry());
            TestCase.assertEquals(f1.getLanguage(), o.f1.getLanguage());
            TestCase.assertEquals(f1.getVariant(), o.f1.getVariant());
        }
        
    }
    
    /* 
     * New version proxy class:
     * Reneame the class, and convert the class.
     */
    @Persistent(proxyFor=Locale.class, version=1)
    static class LocaleProxy_ClassConversion
        implements PersistentProxy<Locale> {

        MyLocale locale;

        private LocaleProxy_ClassConversion() {}

        public void initializeProxy(Locale object) {
            locale = new MyLocale();
            locale.language = object.getLanguage();
            locale.country = object.getCountry();
            locale.variant = object.getVariant();
        }

        public Locale convertProxy() {
            return new Locale(locale.language, locale.country, locale.variant);
        }
    }
    
    @Persistent
    static class MyLocale {
        String language;
        String country;
        String variant;
        private MyLocale() {}
    }
    
    static class MyConversion_ClassConversion implements Conversion {
        private static final long serialVersionUID = 1L;
        private transient RawType newLocaleProxyType;
        private transient RawType myLocaleType;

        public void initialize(EntityModel model) {
            newLocaleProxyType = 
                model.getRawType(LocaleProxy_ClassConversion.class.getName());
            myLocaleType = model.getRawType(MyLocale.class.getName());
        }

        public Object convert(Object fromValue) {

            RawObject localeProxy = (RawObject) fromValue;
            Map<String, Object> localeProxyValues = localeProxy.getValues();
            Map<String, Object> myLocaleValues = new HashMap<String, Object>();

            myLocaleValues.put("language", 
                               localeProxyValues.remove("language"));
            myLocaleValues.put("country", localeProxyValues.remove("country"));
            myLocaleValues.put("variant", localeProxyValues.remove("variant"));
            RawObject myLocale = 
                new RawObject(myLocaleType, myLocaleValues, null);
            localeProxyValues.put("locale", myLocale);

            return new RawObject(newLocaleProxyType, localeProxyValues, 
                                 localeProxy.getSuper());
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof MyConversion_ClassConversion;
        }
    }
    
    /* 
     * New version proxy class:
     * Reneame the class, and convert one of the fields.
     */
    @Persistent(proxyFor=Locale.class, version=1)
    static class LocaleProxy_FieldConversion
        implements PersistentProxy<Locale> {

        MyLanguage language;
        String country;
        String variant;

        private LocaleProxy_FieldConversion() {}

        public void initializeProxy(Locale object) {
            language = new MyLanguage();
            language.language = object.getLanguage();
            country = object.getCountry();
            variant = object.getVariant();
        }

        public Locale convertProxy() {
            return new Locale(language.language, country, variant);
        }
    }
    
    @Persistent
    static class MyLanguage {
        String language;
        private MyLanguage() {}
    }
    
    static class MyConversion_FieldConversion implements Conversion {
        private static final long serialVersionUID = 1L;
        private transient RawType myLanguageType;

        public void initialize(EntityModel model) {
            myLanguageType = model.getRawType(MyLanguage.class.getName());
        }

        public Object convert(Object fromValue) {

            String oldLanguage = (String) fromValue;
            Map<String, Object> myLanguageValues = 
                new HashMap<String, Object>();

            myLanguageValues.put("language", fromValue);
            return new RawObject(myLanguageType, myLanguageValues, null);
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof MyConversion_FieldConversion;
        }
    }
    
    /* 
     * New version proxy class:
     * Reneame the class, and delete one of the fields.
     */
    @Persistent(proxyFor=Locale.class, version=1)
    static class LocaleProxy_DeleteField
        implements PersistentProxy<Locale> {

        String language;
        String country;
        //Delete one field.
        //String variant;

        private LocaleProxy_DeleteField() {}

        public void initializeProxy(Locale object) {
            language = object.getLanguage();
            country = object.getCountry();
            //variant = object.getVariant();
        }

        public Locale convertProxy() {
            return new Locale(language, country);
        }
    }
    
    /* 
     * New version proxy class:
     * Reneame the class, and change the class hierarchy.
     */
    @Persistent(proxyFor=Locale.class, version=1)
    static class LocaleProxy_HierarchyConversion
        extends LocaleProxy_Base implements PersistentProxy<Locale> {


        private LocaleProxy_HierarchyConversion() {}

        public void initializeProxy(Locale object) {
            language = object.getLanguage();
            country = object.getCountry();
            variant = object.getVariant();
        }

        public Locale convertProxy() {
            return new Locale(language, country, variant);
        }
    }
    
    @Persistent
    abstract static class LocaleProxy_Base {
        String language;
        String country;
        String variant;
    }
    
    static class MyConversion_HierarchyConversion implements Conversion {
        private static final long serialVersionUID = 1L;
        private transient RawType newLocaleProxyType;
        private transient RawType localeProxyBaseType;

        public void initialize(EntityModel model) {
            newLocaleProxyType = model.getRawType
                (LocaleProxy_HierarchyConversion.class.getName());
            localeProxyBaseType = 
                model.getRawType(LocaleProxy_Base.class.getName());
        }

        public Object convert(Object fromValue) {

            RawObject oldLocaleProxy = (RawObject) fromValue;
            Map<String, Object> localeProxyValues = oldLocaleProxy.getValues();
            Map<String, Object> localeProxyBaseValues = 
                new HashMap<String, Object>();

            localeProxyBaseValues.put("language", 
                                      localeProxyValues.remove("language"));
            localeProxyBaseValues.put("country", 
                                      localeProxyValues.remove("country"));
            localeProxyBaseValues.put("variant", 
                                      localeProxyValues.remove("variant"));
            RawObject localeProxyBase = new RawObject
                (localeProxyBaseType, localeProxyBaseValues, null);
            RawObject newLocaleProxy = new RawObject 
                (newLocaleProxyType, localeProxyValues, localeProxyBase);

            return newLocaleProxy;
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof MyConversion_HierarchyConversion;
        }
    }
}
