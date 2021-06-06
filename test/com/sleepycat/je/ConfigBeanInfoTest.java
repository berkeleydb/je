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

package com.sleepycat.je;

import static org.junit.Assert.fail;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Method;

import org.junit.Test;

import com.sleepycat.util.test.TestBase;

/*
 * Test if the config classes follow the rules described in com.sleepycat.util.
 * ConfigBeanInfoBase.
 */
public class ConfigBeanInfoTest extends TestBase {

    /*
     * The list of all the config classes. If you create a new config class,
     * you will need to add it into this list.
     */
    private static String[] configClasses = {
        "com.sleepycat.je.CheckpointConfig",
        "com.sleepycat.je.CursorConfig",
        "com.sleepycat.je.DatabaseConfig",
        "com.sleepycat.je.DiskOrderedCursorConfig",
        "com.sleepycat.je.EnvironmentConfig",
        "com.sleepycat.je.EnvironmentMutableConfig",
        "com.sleepycat.je.JoinConfig",
        "com.sleepycat.je.PreloadConfig",
        "com.sleepycat.je.SecondaryConfig",
        "com.sleepycat.je.SequenceConfig",
        "com.sleepycat.je.StatsConfig",
        "com.sleepycat.je.TransactionConfig",
        "com.sleepycat.je.VerifyConfig",
        "com.sleepycat.je.dbi.ReplicatedDatabaseConfig",
        "com.sleepycat.je.rep.NetworkRestoreConfig",
        "com.sleepycat.je.rep.ReplicationBasicConfig",
        "com.sleepycat.je.rep.ReplicationConfig",
        "com.sleepycat.je.rep.ReplicationMutableConfig",
        "com.sleepycat.je.rep.ReplicationNetworkConfig",
        "com.sleepycat.je.rep.ReplicationSSLConfig",
        "com.sleepycat.je.rep.util.ldiff.LDiffConfig",
        "com.sleepycat.persist.evolve.EvolveConfig",
        "com.sleepycat.persist.StoreConfig",
        "com.sleepycat.je.rep.monitor.MonitorConfig",
    };
    
    /* The methods which are not included in the tests. */
    private static String[] ignoreMethods = {
        "com.sleepycat.je.DiskOrderedCursorConfig.setMaxSeedTestHook",
    };

    /*
     * Test if a FooConfig.java has a related FooCongigBeanInfo.java, which
     * records the setter/getter methods for all properties.
     */
    @Test
    public void testBeanInfoExist() {
        for (int i = 0; i < configClasses.length; i++) {
            try {
                Class configClass = Class.forName(configClasses[i]);

                /* Get the public methods. */
                Method[] methods = configClass.getMethods();
                for (int j = 0; j < methods.length; j++) {
                    String name = methods[j].getName();
                    String subName = name.substring(0, 3);
                    String propertyName = name.substring(3);

                    /* If it is a setter method. */
                    if (subName.equals("set")) {
                        if (isIgnoreMethods(configClasses[i] + "." + name)) {
                            continue;
                        }
                        String getterName = "get" + propertyName;
                        try {

                            /*
                             * Check if there is a corresponding getter method.
                             * This getter method cannot contain any
                             * parameters, which is consistent to the design
                             * patterns for properties in javabeans.If not,
                             * NoSuchMethodException will be thrown and caught.
                             */
                            Method getter = configClass.getMethod(getterName);

                            /*
                             * According to Design Patterns for Properties
                             * (refer to sun javabeans spec), the setter method
                             * should only contain one parameter, and the type
                             * of the parameter is the same as the return type
                             * of the getter method.
                             */
                            if (methods[j].getParameterTypes().length != 1 ||
                                !methods[j].getParameterTypes()[0].
                                    equals(getter.getReturnType())) {
                                break;
                            }

                            /*
                             * If the property has setter and getter methods,
                             * then we need to add a FooConfigBeanInfo class
                             *for this FooConfig class. If the return type of
                             * the setter method is not a void, we will need to
                             * add a setter method called setPropertyNameVoid
                             * method with a void return type.
                             */
                            try {
                                Class beanInfoClass = Class.forName(
                                        configClasses[i] + "BeanInfo");
                            } catch (ClassNotFoundException e) {

                                /* If no FooConfigBeanInfo class exists. */
                                fail("Have not defined beanInfo class: " +
                                     configClasses[i] + "BeanInfo");
                            } catch (SecurityException e) {
                                e.printStackTrace();
                            }
                            try {
                                BeanInfo info = Introspector.
                                    getBeanInfo(configClass);
                                PropertyDescriptor[] descriptors =
                                    info.getPropertyDescriptors();

                                boolean ifFound = false;

                                /*
                                 * Check if the setter method of the property
                                 * has been defined in the beaninfo class.
                                 */
                                for (int k = 0; k < descriptors.length; ++k) {
                                    String methodName = descriptors[k].
                                        getWriteMethod().getName();

                                    /*
                                     * The name of the setter method can be
                                     * setPropertyName or setPropertyNameVoid.
                                     */
                                    if (methodName.equals(name) ||
                                        methodName.equals(name + "Void")) {
                                        ifFound = true;
                                        break;
                                    }
                                }

                                if (!ifFound) {
                                    fail("No setter method " + name +
                                         "[Void] for " + configClasses[i] +
                                         "BeanInfo");
                                }
                            } catch (IntrospectionException e) {
                                e.printStackTrace();
                            }
                        } catch (NoSuchMethodException e) {

                            /*
                             * There is no corresponding getter method for this
                             * property. So we do nothing.
                             */
                        }
                    }
                }
            } catch (SecurityException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
    }

    /*
     * Test that, for a property, there must be two setter methods, one is
     * setFoo which will return "this", one is setFooVoid, which will return
     * void.
     */
    @Test
    public void testSetterReturnThisAndVoid() {
        for (int i = 0; i < configClasses.length; i++) {
            try {
                Class configClass = Class.forName(configClasses[i]);
                /* Get the public methods. */
                Method[] methods = configClass.getMethods();
                for (int j = 0; j < methods.length; j++) {
                    String methodName = methods[j].getName();
                    String subName = methodName.substring(0, 3);
                    String propertyName = methodName.substring(3);
                    Class<?> returnType = methods[j].getReturnType();

                    /* If it is a setter method. */
                    if (subName.equals("set")) {
                        if (isIgnoreMethods(configClasses[i] + "." + 
                                            methodName)) {
                            continue;
                        }
                        String getterName = "get" + propertyName;
                        try {

                            /*
                             * Check if there is a corresponding getter method.
                             * This getter method cannot contain any
                             * parameters, which is consistent to the design
                             * patterns for properties in javabeans If not,
                             * NoSuchMethodException will be thrown and caught.
                             */
                            Method getter = configClass.getMethod(getterName);

                            /*
                             * According to Design Patterns for Properties
                             * (refer to sun javabeans spec), the setter method
                             * should only contain one parameter, and the type
                             * of the parameter is the same as the return type
                             * of the getter method.
                             */
                            if (methods[j].getParameterTypes().length != 1 ||
                                !methods[j].getParameterTypes()[0].
                                    equals(getter.getReturnType())) {
                                break;
                            }

                            /*
                             * The setter method setPropertyName should return
                             * "this".
                             */
                            assert returnType.isAssignableFrom(configClass) :
                                   "The setter method" +
                                   configClass.getName() + "." + methodName +
                                   " should return " + configClass.getName();

                            /*
                             * Check if there is a corresponding setter method,
                             * called setFooVoid, which return void,
                             */
                            String setterVoidName = methodName + "Void";
                            try {
                                Method setterVoid =
                                    configClass.getMethod(setterVoidName,
                                        methods[j].getParameterTypes());
                                returnType = setterVoid.getReturnType();
                                assert returnType.toString().equals("void") :
                                       "The setter method" +
                                       configClass.getName() + "." +
                                       setterVoidName + " should return " +
                                       "void";
                            } catch (NoSuchMethodException e) {
                                fail("There should be a setter method " +
                                     setterVoidName +
                                     ", which should return void");
                            }
                        } catch (NoSuchMethodException e) {

                            /*
                             * If there is no corresponding getter method for
                             * this property, then we do not regard this
                             * property as a valid property, which is
                             * consistent to javabeans' design patterns. So we
                             * do nothing.
                             */
                        }
                    }
                }
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
    }
    
    private boolean isIgnoreMethods(String methodName) {
        for (int i = 0; i < ignoreMethods.length; i++) {
            if (ignoreMethods[i].equals(methodName)) {
                return true;
            }
        }
        return false;
    }
}
