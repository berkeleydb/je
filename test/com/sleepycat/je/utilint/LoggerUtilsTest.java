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

package com.sleepycat.je.utilint;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import org.junit.Test;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentMutableConfig;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.junit.JUnitProcessThread;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

/**
 * A unit test for testing JE logging programatically. 
 */
public class LoggerUtilsTest extends TestBase {

    /**
     * If a logging config file is specified, this test cannot expect the
     * logging properties to have default settings.
     */
    private static final boolean DEFAULT_LOGGING_PROPERTIES =
        System.getProperty("java.util.logging.config.file", "").equals("");

    private final File envHome;
    private static final String loggerPrefix = "com.sleepycat.je.";
    /* Logging configure properties file name. */
    private static final String fileName = "logging.properties";
    /* Logging settings in the properties file. */
    private static final String consoleLevel =
        "com.sleepycat.je.util.ConsoleHandler.level=INFO";
    private static final String fileLevel =
        "com.sleepycat.je.util.FileHandler.level=WARNING";

    public LoggerUtilsTest() {
        envHome = SharedTestUtils.getTestDir();
    }

    /* 
     * Remove the named files from the environment directory. 
     */
    private void removeFiles(File envDir, String name) {
        File[] files = envDir.listFiles();
        for (File file : files) {
            if (file.getName().contains(name)) {
                assertTrue("couldn't delete " + name + "for " + envDir,
                           file.delete());
            }
        }
    }

    /* 
     * Test whether a JE logger's level can be set programmatically. 
     */
    @Test
    public void testLoggerLevelsRWEnv()
        throws Exception {
        
        changeLoggerLevels(false /* readOnly */);
    }

    /* 
     * Test whether a JE logger's level can be set programmatically, and that
     * logging works in a read only environment. 
     */
    @Test
    public void testLoggerLevelsROEnv()
        throws Exception {

        changeLoggerLevels(true /* readOnly */);
    }

    private void changeLoggerLevels(boolean readOnlyEnv)
        throws Exception {

        /* 
         * Set the parent's level to OFF, so all logging messages shouldn't be 
         * written to je.info files. 
         */
        Logger parent = Logger.getLogger("com.sleepycat.je");
        parent.setLevel(Level.OFF);

        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        envConfig.setConfigParam(EnvironmentConfig.FILE_LOGGING_LEVEL, "ALL");
        envConfig.setReadOnly(readOnlyEnv);
        Environment env = new Environment(envHome, envConfig);

        /* Init a test messages list. */
        ArrayList<String> messages = new ArrayList<String>();
        messages.add("Hello, Linda!");
        messages.add("Hello, Sam!");
        messages.add("Hello, Charlie!");
        messages.add("Hello, Mark!");
        messages.add("Hello, Tao!");
        messages.add("Hello, Eric!");

        /* Check the logger level before reset. */
        checkLoggerLevel();

        /* Log the test messages. */
        logMsg(DbInternal.getNonNullEnvImpl(env), messages);

        /* 
         * The loggers were turned off with a level setting of OFF, so there is
         * should be nothing in the je.info files.
         */
        ArrayList<String> readMsgs = readFromInfoFile(readOnlyEnv);
        assertTrue(readMsgs.size() == 0);

        /* 
         * Reset the parent level to ALL, so that all logging messages should 
         * be logged. 
         */
        parent.setLevel(Level.ALL);

        /* Log the test messages. */
        logMsg(DbInternal.getNonNullEnvImpl(env), messages);

        /* Check that each test message is in the je.info.0 file. */
        readMsgs = readFromInfoFile(readOnlyEnv);

        /*
         * Since JE logger's level has been set to ALL, additional JE logging
         * messages from normal operations may also be written to je.info
         * files. We should check that the actual messages in je.info should be
         * equal to or larger than the size we directly log.
         */
        if (readOnlyEnv) {
            /* Read only Environment won't log anything to JE FileHandler. */
            assertTrue(readMsgs.size() == 0);
        } else {

            /*
             * Since JE logger's level has been set to ALL, additional JE 
             * logging messages from normal operations may also be written to
             * je.info files. We should check that the actual messages in 
             * je.info files should be equal to or larger than the size we 
             * directly log.
             */
            assertTrue(readMsgs.size() >= messages.size());
            for (int i = 0; i < messages.size(); i++) {
                boolean contained = false;
                for (int j = 0; j < readMsgs.size(); j++) {
                    if (readMsgs.get(j).contains(messages.get(i))) {
                        contained = true;
                        break;
                    }
                }
                assertTrue(contained);
            }
        }

        /* Check to see that all JE loggers' level are not changed. */
        checkLoggerLevel();

        env.close();
    }

    /* Check the level for all JE loggers. */
    private void checkLoggerLevel() {
        Enumeration<String> loggerNames =
            LogManager.getLogManager().getLoggerNames();
        while (loggerNames.hasMoreElements()) {
            String loggerName = loggerNames.nextElement();
            if (loggerName.startsWith(loggerPrefix)) {
                Logger logger = Logger.getLogger(loggerName);
                assertNull(logger.getLevel());
            }
        }
    }

    /* Log some messages. */
    private void logMsg(EnvironmentImpl envImpl, ArrayList<String> messages) {
        Logger envLogger = envImpl.getLogger();
        for (String message : messages) {
            LoggerUtils.info(envLogger, envImpl, message);
        }
    }

    /* Read the contents in the je.info files. */
    private ArrayList<String> readFromInfoFile(boolean readOnlyEnv) 
        throws Exception {

        /* Get the file for je.info.0. */
        File[] files = envHome.listFiles();
        File infoFile = null;
        for (File file : files) {
            if (("je.info.0").equals(file.getName())) {
                infoFile = file;
                break;
            }
        }

        /* Make sure the file exists. */
        ArrayList<String> messages = new ArrayList<String>();
        if (readOnlyEnv) {
            return messages;
        }

        assertTrue(infoFile != null);

        /* Read the messages from the file. */
        BufferedReader in = new BufferedReader(new FileReader(infoFile));
        String message = new String();
        while ((message = in.readLine()) != null) {
            messages.add(message);
        }
        in.close();

        return messages;
    }

    /* 
     * Test FileHandler and ConsoleHandler level setting. 
     */
    @Test
    public void testHandlerLevels()
        throws Exception {

        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        Environment env = new Environment(envHome, envConfig);

        /* Check the initial handler level settings. */
        EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);
        Level consoleHandlerLevel = envImpl.getConsoleHandler().getLevel();
        Level fileHandlerLevel = envImpl.getFileHandler().getLevel();
        if (DEFAULT_LOGGING_PROPERTIES) {
            assertEquals(Level.OFF, consoleHandlerLevel);
            assertEquals(Level.INFO, fileHandlerLevel);
        }

        env.close();
        
        /* Reopen the Environment with params setting. */
        envConfig.setConfigParam(EnvironmentConfig.CONSOLE_LOGGING_LEVEL, 
                                 "WARNING");
        envConfig.setConfigParam(EnvironmentConfig.FILE_LOGGING_LEVEL, 
                                 "SEVERE");
        env = new Environment(envHome, envConfig);
        envImpl = DbInternal.getNonNullEnvImpl(env);
        Level newConsoleHandlerLevel = envImpl.getConsoleHandler().getLevel();
        Level newFileHandlerLevel = envImpl.getFileHandler().getLevel();
        /* Check that the new level are the same as param setting. */
        assertEquals(Level.WARNING, newConsoleHandlerLevel);
        assertEquals(Level.SEVERE, newFileHandlerLevel);

        /* Make sure the levels are different before and after param setting. */
        if (DEFAULT_LOGGING_PROPERTIES) {
            assertFalse(consoleHandlerLevel.equals(newConsoleHandlerLevel));
            assertFalse(fileHandlerLevel.equals(newFileHandlerLevel));
        }

        env.close();
    }

    /* 
     * Test whether the configurations inside the properties file are set 
     * correctly in JE Environment. 
     */
    @Test
    public void testPropertiesSetting() 
        throws Exception {

        invokeProcess(false);
    }

    /**
     *  Start the process and check the exit value. 
     *
     *  @param bothSetting presents whether the logging configuration and JE
     *                     params are both set on a same Environment.
     */
    private void invokeProcess(boolean bothSetting) 
        throws Exception {

        /* Create a property file and write configurations into the file. */
        String propertiesFile = createPropertiesFile();

        /* 
         * If bothSetting is true, which means we need to set JE params, so 
         * obviously we need to add another two arguments.
         */
        String[] envCommand = bothSetting ? new String[8] : new String[6];
        envCommand[0] = "-Djava.util.logging.config.file=" + propertiesFile;
        envCommand[1] = "com.sleepycat.je.utilint.LoggerUtilsTest$" +
                        "PropertiesSettingProcess";
        envCommand[2] = envHome.getAbsolutePath();
        envCommand[3] = "INFO";
        envCommand[4] = "WARNING";
        envCommand[5] = bothSetting ? "true" : "false";
        /* JE Param setting. */
        if (bothSetting) {
            envCommand[6] = "WARNING";
            envCommand[7] = "SEVERE";
        }

        /* Start a process. */
        JUnitProcessThread thread =
            new JUnitProcessThread("PropertiesSettingProcess", envCommand);
        thread.start();

        try {
            thread.finishTest();
        } catch (Throwable t) {
            System.err.println(t.toString());
        }

        /* We expect that the process exited normally. */
        assertEquals(0, thread.getExitVal());

        /* Remove the created property file. */
        removeFiles(envHome, fileName);
    }

    /* Create a properties file for use. */
    private String createPropertiesFile() 
        throws Exception {

        String name = envHome.getAbsolutePath() + 
                      System.getProperty("file.separator") + fileName;
        File file = new File(name);
        PrintWriter out = 
            new PrintWriter(new BufferedWriter(new FileWriter(file)));
        out.println(consoleLevel);
        out.println(fileLevel);
        out.close();

        return name;
    }

    /*
     * Test that JE ConsoleHandler and FileHandler get the correct level when
     * their levels are set both by the java.util.logging properties file and 
     * JE params.
     *
     * We want JE params to override the levels set by the standard 
     * properties file.
     */
    @Test
    public void testPropertiesAndParamSetting()
        throws Exception {

        invokeProcess(true);
    }

    /*
     * Test that handler levels are mutable.
     */
    @Test
    public void testMutableConfig()
        throws Exception {

        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        envConfig.setConfigParam(EnvironmentConfig.CONSOLE_LOGGING_LEVEL,
                                 "WARNING");
        envConfig.setConfigParam(EnvironmentConfig.FILE_LOGGING_LEVEL,
                                 "SEVERE");
        Environment env = new Environment(envHome, envConfig);

        /* Check the init handlers' level setting. */
        EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);
        Level consoleHandlerLevel = envImpl.getConsoleHandler().getLevel();
        Level fileHandlerLevel = envImpl.getFileHandler().getLevel();
        assertEquals(Level.WARNING, consoleHandlerLevel);
        assertEquals(Level.SEVERE, fileHandlerLevel);

        /* Change the handler param setting for an open Environment. */
        EnvironmentMutableConfig mutableConfig = env.getMutableConfig();
        mutableConfig.setConfigParam(EnvironmentConfig.CONSOLE_LOGGING_LEVEL,
                                     "SEVERE");
        mutableConfig.setConfigParam(EnvironmentConfig.FILE_LOGGING_LEVEL,
                                     "WARNING");
        env.setMutableConfig(mutableConfig);

        /* Check the handler's level has changed. */
        Level newConsoleHandlerLevel = envImpl.getConsoleHandler().getLevel();
        Level newFileHandlerLevel = envImpl.getFileHandler().getLevel();
        assertEquals(Level.SEVERE, newConsoleHandlerLevel);
        assertEquals(Level.WARNING, newFileHandlerLevel);
        assertTrue(newConsoleHandlerLevel != consoleHandlerLevel);
        assertTrue(newFileHandlerLevel != fileHandlerLevel);

        /* Check levels again. */
        mutableConfig = env.getMutableConfig();
        env.setMutableConfig(mutableConfig);
        consoleHandlerLevel = envImpl.getConsoleHandler().getLevel();
        fileHandlerLevel = envImpl.getFileHandler().getLevel();
        assertEquals(Level.SEVERE, consoleHandlerLevel);
        assertEquals(Level.WARNING, fileHandlerLevel);
        assertTrue(newConsoleHandlerLevel == consoleHandlerLevel);
        assertTrue(newFileHandlerLevel == fileHandlerLevel);

        env.close();
    }

    /* 
     * A process for starting a JE Environment with properties file or 
     * configured JE params. 
     */
    static class PropertiesSettingProcess {
        private final File envHome;
        /* Handler levels set through properties configuration file. */
        private final Level propertyConsole;
        private final Level propertyFile;
        /* Handler levels set through JE params. */
        private final Level paramConsole;
        private final Level paramFile;
        /* Indicates whether property file and params are both set. */
        private final boolean bothSetting;
        private Environment env;

        public PropertiesSettingProcess(File envHome, 
                                        Level propertyConsole, 
                                        Level propertyFile,
                                        boolean bothSetting,
                                        Level paramConsole,
                                        Level paramFile) {
            this.envHome = envHome;
            this.propertyConsole = propertyConsole;
            this.propertyFile = propertyFile;
            this.bothSetting = bothSetting;
            this.paramConsole = paramConsole;
            this.paramFile = paramFile;
        }

        /* Open a JE Environment. */
        public void openEnv() {
            try {
                EnvironmentConfig envConfig = new EnvironmentConfig();
                envConfig.setAllowCreate(true);

                /* If bothSetting is true, set the JE params. */
                if (bothSetting) {
                    envConfig.setConfigParam
                        (EnvironmentConfig.CONSOLE_LOGGING_LEVEL, 
                         paramConsole.toString());
                    envConfig.setConfigParam
                        (EnvironmentConfig.FILE_LOGGING_LEVEL,
                         paramFile.toString());
                }

                env = new Environment(envHome, envConfig);
            } catch (DatabaseException e) {
                e.printStackTrace();
                System.exit(1);
            }
        }

        /* Check the configured levels. */
        public void check() {
            if (bothSetting) {
                doCheck(paramConsole, paramFile);
            } else {
                doCheck(propertyConsole, propertyFile);
            }
        }

        private void doCheck(Level cLevel, Level fLevel) {
            try {
                EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);
                assertTrue
                    (envImpl.getConsoleHandler().getLevel() == cLevel);
                assertTrue(envImpl.getFileHandler().getLevel() == fLevel);
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(2);
            } finally {
                env.close();
            }
        }

        public static void main(String args[]) {
            PropertiesSettingProcess process = null;
            try {
                Level paramConsole = null;
                Level paramFile = null;
                if (args.length == 6) {
                    paramConsole = Level.parse(args[4]);
                    paramFile = Level.parse(args[5]);
                }

                process = new PropertiesSettingProcess(new File(args[0]), 
                                                       Level.parse(args[1]), 
                                                       Level.parse(args[2]),
                                                       new Boolean(args[3]),
                                                       paramConsole,
                                                       paramFile);
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(3);
            }
            
            if (process == null) {
        	throw new RuntimeException("Process should have been created");
            }
            process.openEnv();
            process.check();
        }
    }

    /**
     * Set up two environments with configured handlers, and make sure
     * that they only log records from the appropriate environment.
     */
    @Test
    public void testConfiguredHandler() {
        TestInfo infoA = setupMultipleEnvs("A");
        TestInfo infoB = setupMultipleEnvs("B");
        int numTestMsgs = 10;
        try {
            Logger loggerA = LoggerUtils.getLogger
                (com.sleepycat.je.utilint.LoggerUtils.class);
            Logger loggerB = LoggerUtils.getLogger
                (com.sleepycat.je.utilint.LoggerUtils.class);

            for (int i = 0; i < numTestMsgs; i++) {
                LoggerUtils.logMsg(loggerA, 
                                   infoA.envImpl,
                                   Level.SEVERE, infoA.prefix + i);
                LoggerUtils.logMsg(loggerB, 
                                   infoB.envImpl,
                                   Level.SEVERE, infoB.prefix + i);
            }

            infoA.handler.verify(numTestMsgs);
            infoB.handler.verify(numTestMsgs);
        } finally {
            cleanup(infoA.env, infoA.dir);
            cleanup(infoB.env, infoB.dir);
        }
    }

    private void cleanup(Environment env, File envDir) {
        env.close();
        TestUtils.removeLogFiles("2 envs", envDir, false);
        removeFiles(envDir, "je.info");
    }

    private TestInfo setupMultipleEnvs(String name) {
        String testPrefix  = "TEXT" + name;
        File dir = new File(envHome, name);
        dir.mkdirs();

        EnvironmentConfig config = new EnvironmentConfig();
        config.setAllowCreate(true);
        TestHandler handler = new TestHandler(testPrefix);
        handler.setLevel(Level.SEVERE);
        config.setLoggingHandler(handler);
        Environment env = new Environment(dir, config);

        return new TestInfo(env, handler, testPrefix, dir);
    }

    private class TestInfo {
        EnvironmentImpl envImpl;
        Environment env;
        String prefix;
        TestHandler handler;
        File dir;

        TestInfo(Environment env, TestHandler handler, 
                 String testPrefix, File dir) {
            this.env = env;
            this.envImpl = DbInternal.getNonNullEnvImpl(env);
            this.handler = handler;
            this.prefix = testPrefix;
            this.dir = dir;
        }
    }

    private class TestHandler extends Handler {
        
        private final String prefix;
        private final List<String> logged;

        TestHandler(String prefix) {
            this.prefix = prefix;
            logged = new ArrayList<String>();
        }

        void verify(int numExpected) {
            assertEquals(numExpected, logged.size());
            for (int i = 0; i < numExpected; i++) {
                assertEquals(logged.get(i), (prefix + i));
            }
        }

        @Override
        public void publish(LogRecord record) {
            logged.add(record.getMessage());
        }

        @Override
        public void flush() {
        }

        @Override
        public void close() throws SecurityException {
        }
    }
}
