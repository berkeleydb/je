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

package com.sleepycat.je.rep.impl.networkRestore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.bind.tuple.LongBinding;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.EnvironmentLockedException;
import com.sleepycat.je.VerifyConfig;
import com.sleepycat.je.dbi.EnvironmentFailureReason;
import com.sleepycat.je.log.FileManager;
import com.sleepycat.je.log.LogManager;
import com.sleepycat.je.log.RestoreMarker;
import com.sleepycat.je.rep.ReplicationNetworkConfig;
import com.sleepycat.je.rep.impl.node.NameIdPair;
import com.sleepycat.je.rep.net.DataChannelFactory;
import com.sleepycat.je.rep.utilint.BinaryProtocol.ProtocolException;
import com.sleepycat.je.rep.utilint.RepTestUtils;
import com.sleepycat.je.rep.utilint.ServiceDispatcher;
import com.sleepycat.je.rep.utilint.net.DataChannelFactoryBuilder;
import com.sleepycat.je.util.DbBackup;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class NetworkBackupTest extends TestBase {

    /* The port being handled by the dispatcher. */
    private static final int TEST_PORT = 5000;

    /* The source environment */
    private File envHome;
    private EnvironmentConfig envConfig;
    private Environment env;
    private Database db;

    /*
     * The destination environment, and filemanager and logmanager for that
     * destination.
     */
    File backupDir;
    private Environment backupEnv;
    private FileManager backupFileManager;
    private LogManager backupLogManager;

    private final InetSocketAddress serverAddress =
        new InetSocketAddress("localhost", TEST_PORT);

    private DataChannelFactory channelFactory;
    private ServiceDispatcher dispatcher;
    private FeederManager fm;

    protected DatabaseConfig dbconfig;
    protected final DatabaseEntry key = new DatabaseEntry(new byte[] { 1 });
    protected final DatabaseEntry data = new DatabaseEntry(new byte[] { 100 });
    protected static final String TEST_DB_NAME = "TestDB";

    protected static final VerifyConfig vconfig = new VerifyConfig();

    private static final int DB_ENTRIES = 100;

    static {
        vconfig.setAggressive(false);
        vconfig.setPropagateExceptions(true);
    }

    /* True if the Feeder enables multiple sub directories. */
    private boolean envMultiDirs;

    /*
     * True if the nodes need to copy log files enables multiple sub
     * directories.
     */
    private boolean backupMultiDirs;
    private final int DATA_DIRS = 3;

    /*
     * Experiences four cases:
     * 1. Feeder doesn't enable sub directories, nor replicas.
     * 2. Feeder doesn't enable sub directories, but replicas do.
     * 3. Feeder enables sub directories, but replicas don't.
     * 4. Feeder enables sub directories, so do replicas.
     */
    @Parameters
    public static List<Object[]> genParams() {

        return Arrays.asList(new Object[][] {{false, false}, {false, true},
            {true, false}, {true, true}});
    }

    public NetworkBackupTest(boolean envMultiDirs, boolean backupMultiDirs) {
        this.envMultiDirs = envMultiDirs;
        this.backupMultiDirs = backupMultiDirs;
        customName = (envMultiDirs ? ":env-multi-sub-dirs" : "") +
                (backupMultiDirs ? ":backup-multi-sub-dirs" : "");
    }

    @Before
    public void setUp()
        throws Exception {

        super.setUp();
        envHome = SharedTestUtils.getTestDir();
        envConfig = TestUtils.initEnvConfig();
        DbInternal.disableParameterValidation(envConfig);
        envConfig.setConfigParam(EnvironmentConfig.LOG_FILE_MAX, "1000");
        envConfig.setConfigParam(EnvironmentConfig.ENV_RUN_CLEANER, "false");

        /* If multiple sub directories property is enabled. */
        if (envMultiDirs) {
            envConfig.setConfigParam(EnvironmentConfig.LOG_N_DATA_DIRECTORIES,
                                     DATA_DIRS + "");
            createSubDir(envHome, false);
        }
        envConfig.setAllowCreate(true);
        envConfig.setTransactional(true);

        env = new Environment(envHome, envConfig);

        dbconfig = new DatabaseConfig();
        dbconfig.setAllowCreate(true);
        dbconfig.setTransactional(true);
        dbconfig.setSortedDuplicates(false);
        db = env.openDatabase(null, TEST_DB_NAME, dbconfig);

        for (int i = 0; i < DB_ENTRIES; i++) {
            IntegerBinding.intToEntry(i, key);
            LongBinding.longToEntry(i, data);
            db.put(null, key, data);
        }
        /* Create cleaner fodder. */
        for (int i = 0; i < (DB_ENTRIES / 2); i++) {
            IntegerBinding.intToEntry(i, key);
            LongBinding.longToEntry(i, data);
            db.put(null, key, data);
        }
        env.cleanLog();
        env.verify(vconfig, System.err);

        /* Create the backup environment. */
        backupDir = new File(envHome.getCanonicalPath() + ".backup");
        /* Clear the log files in the backup directory. */
        cleanEnvHome(backupDir, true);
        /* Create the Environment home for replicas. */
        if (backupMultiDirs) {
            envConfig.setConfigParam(EnvironmentConfig.LOG_N_DATA_DIRECTORIES,
                                     DATA_DIRS + "");
            createSubDir(backupDir, true);
        } else {
            envConfig.setConfigParam
                (EnvironmentConfig.LOG_N_DATA_DIRECTORIES, "0");
            backupDir.mkdir();
        }
        assertTrue(backupDir.exists());

        backupEnv = new Environment(backupDir, envConfig);
        backupFileManager =
                DbInternal.getNonNullEnvImpl(backupEnv).getFileManager();
        backupLogManager =
                DbInternal.getNonNullEnvImpl(backupEnv).getLogManager();

        final ReplicationNetworkConfig repNetConfig =
            ReplicationNetworkConfig.create(RepTestUtils.readNetProps());
        channelFactory = DataChannelFactoryBuilder.construct(repNetConfig);

        dispatcher = new ServiceDispatcher(serverAddress, channelFactory);
        dispatcher.start();
        fm = new FeederManager(dispatcher,
                               DbInternal.getNonNullEnvImpl(env),
                               new NameIdPair("n1", (short) 1));
        fm.start();
    }

    private void createSubDir(File home, boolean isBackupDir)
        throws Exception {

        if (isBackupDir) {
            if (!home.exists()) {
                home.mkdir();
            }
        }

        if ((envMultiDirs && !isBackupDir) ||
            (backupMultiDirs && isBackupDir)) {
            for (int i = 1; i <= DATA_DIRS; i++) {
                File subDir = new File(home, TestUtils.getSubDirName(i));
                assertTrue(!subDir.exists());
                assertTrue(subDir.mkdir());
            }
        }
    }

    @After
    public void tearDown()
        throws Exception {

        try {
            db.close();
            env.close();
            fm.shutdown();
            dispatcher.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    private void cleanEnvHome(File home, boolean isBackupDir)
        throws Exception {

        if (home == null) {
            return;
        }

        File[] files = home.listFiles();
        if (files == null || files.length == 0) {
            return;
        }

        /* Delete the sub directories if any. */
        for (File file : files) {
            if (file.isDirectory() && file.getName().startsWith("data")) {
                File[] subFiles = file.listFiles();
                for (File subFile : subFiles) {
                    assertTrue(subFile.delete());
                }
                assertTrue(file.delete());
            }

            if (isBackupDir && file.isFile()) {
                assertTrue(file.delete());
            }
        }

        TestUtils.removeLogFiles("tearDown", home, false);

        if (isBackupDir) {
            assertTrue(home.delete());
        }
    }

    @Test
    public void testBackupFiles()
        throws Exception {

        /* The client side */
        NetworkBackup backup1 =
            new NetworkBackup(serverAddress,
                              backupEnv.getHome(),
                              new NameIdPair("n1", (short) 1),
                              false,
                              backupFileManager,
                              backupLogManager,
                              channelFactory);
        String files1[] = backup1.execute();
        NetworkBackupStats stats1 = backup1.getStats();
        assertEquals(0, stats1.getSkipCount());

        verify(envHome, backupDir, files1);

        /*
         * Check byte transfer stats.  Not testing getTransferRate -- transfer
         * is too quick to get any results for that stat.
         */
        File[] jdbFiles1 = backupFileManager.listJDBFiles();
        long jdbFilesBytes1 = 0;
        for (File f : jdbFiles1) {
            jdbFilesBytes1 += f.length();
        }
        assertEquals(jdbFilesBytes1, stats1.getExpectedBytes());
        assertEquals(jdbFilesBytes1, stats1.getTransferredBytes());

        /* Corrupt the currently backed up log files. */
        for (File f : jdbFiles1) {
            FileOutputStream os = new FileOutputStream(f);
            os.write(1);
            os.close();
        }

        int count = backupFileManager.listJDBFiles().length;
        NetworkBackup backup2 =
            new NetworkBackup(serverAddress,
                              backupEnv.getHome(),
                              new NameIdPair("n1", (short) 1),
                              false,
                              backupFileManager,
                              backupLogManager,
                              channelFactory);
        String files2[] = backup2.execute();
        verify(envHome, backupDir, files2);
        assertEquals(count, backup2.getStats().getDisposedCount());

        verifyAsEnv(backupDir);

        /*
         * Close the database to avoid problems later when we corrupt the files
         * on the server
         */
        db.close();

        /* Corrupt files on the server, and make sure no files are copied */
        for (final File f :
                 DbInternal.getNonNullEnvImpl(env).getFileManager().
                 listJDBFiles()) {
            final FileOutputStream os = new FileOutputStream(f);
            os.write(1);
            os.close();
        }
        final NetworkBackup backup3 =
            new NetworkBackup(serverAddress,
                              backupEnv.getHome(),
                              new NameIdPair("n1", (short) 1),
                              true,
                              backupFileManager,
                              backupLogManager,
                              channelFactory);
        try {
            backup3.execute();
            fail("Expected IOException");
        } catch (IOException e) {
        }

        assertEquals("No files should have been fetched",
                     0,
                     backup3.getStats().getFetchCount());

        /* The environment is corrupted -- invalidate it */
        new EnvironmentFailureException(
            DbInternal.getNonNullEnvImpl(env),
            EnvironmentFailureReason.TEST_INVALIDATE);
    }

    /**
     * Performs a backup while the database is growing actively
     *
     * @throws InterruptedException
     * @throws IOException
     * @throws DatabaseException
     */
    @Test
    public void testConcurrentBackup()
        throws InterruptedException, IOException, DatabaseException {

        LogFileGeneratingThread lfThread = new LogFileGeneratingThread();
        BackupThread backupThread = new BackupThread();
        lfThread.start();

        backupThread.start();
        backupThread.join(60*1000);
        lfThread.quit = true;
        lfThread.join(60*1000);

        DbBackup dbBackup = new DbBackup(env);
        dbBackup.startBackup();
        int newCount = dbBackup.getLogFilesInBackupSet().length;

        assertNull(backupThread.error);
        assertNull(lfThread.error);

        /*
         * Verify that the count did increase while the backup was in progress.
         */
        assertTrue(newCount > backupThread.files.length);
        /* Verify that the backup was correct. */
        verify(envHome, backupDir, backupThread.files);

        verifyAsEnv(backupDir);
        dbBackup.endBackup();
    }

    class BackupThread extends Thread {
        Exception error = null;
        String files[] = null;

        BackupThread() {
            setDaemon(true);
        }

        @Override
        public void run() {
            try {
                NetworkBackup backup1 =
                    new NetworkBackup(serverAddress,
                                      backupEnv.getHome(),
                                      new NameIdPair("n1", (short) 1),
                                      true,
                                      backupFileManager,
                                      backupLogManager,
                                      channelFactory);
                files = backup1.execute();
            } catch (Exception e) {
                error = e;
                error.printStackTrace();
            }
        }
    }

    class LogFileGeneratingThread extends Thread {
        Exception error = null;
        volatile boolean quit = false;

        LogFileGeneratingThread() {
            setDaemon(true);
        }

        @Override
        public void run() {
            try {
                for (int i = 0; i < 100000; i++) {
                    IntegerBinding.intToEntry(i, key);
                    LongBinding.longToEntry(i, data);
                    db.put(null, key, data);
                    if (quit) {
                        return;
                    }
                }
            } catch (Exception e) {
                error = e;
                error.printStackTrace();
            }
            fail("Backup did not finish in time");
        }
    }

    @Test
    public void testBasicWithRetainLog()
        throws Exception {

        doBasicTest(true);
    }

    @Test
    public void testBasicWithoutRetainLog()
        throws Exception {

        doBasicTest(false);
    }

    private void doBasicTest(boolean retainLog)
        throws Exception {

        /* The client side */
        NetworkBackup backup1 =
            new NetworkBackup(serverAddress,
                              backupEnv.getHome(),
                              new NameIdPair("n1", (short) 1),
                              retainLog,
                              backupFileManager,
                              backupLogManager,
                              channelFactory);
        backup1.execute();
        assertEquals(0, backup1.getStats().getSkipCount());

        /*
         * repeat, should find mostly cached files. Invoking backup causes
         * a checkpoint to be written to the log.
         */
        NetworkBackup backup2 =
            new NetworkBackup(serverAddress,
                              backupEnv.getHome(),
                              new NameIdPair("n1", (short) 1),
                              retainLog,
                              backupFileManager,
                              backupLogManager,
                              channelFactory);
        String files2[] = backup2.execute();
        verify(envHome, backupDir, files2);

        assertTrue((backup1.getStats().getFetchCount() -
                     backup2.getStats().getSkipCount())  <= 1);

        verifyAsEnv(backupDir);
    }

    @Test
    public void testLeaseBasic()
        throws Exception {

        int errorFileNum = 2;
        NetworkBackup backup1 =
            new TestNetworkBackup(serverAddress,
                                  backupEnv,
                                  (short) 1,
                                  true,
                                  errorFileNum);
        try {
            backup1.execute();
            fail("Exception expected");
        } catch (IOException e) {
            /* Expected. */
        }
        /* Wait for server to detect a broken connection. */
        Thread.sleep(500);
        /* Verify that the lease was created. */
        assertEquals(1, fm.getLeaseCount());
        NetworkBackup backup2 =
            new NetworkBackup(serverAddress,
                              backupEnv.getHome(),
                              new NameIdPair("n1", (short) 1),
                              true,
                              backupFileManager,
                              backupLogManager,
                              channelFactory);
        /* Verify that the lease was renewed. */
        String[] files2 = backup2.execute();
        assertEquals(2, backup2.getStats().getSkipCount());
        assertEquals(1, fm.getLeaseRenewalCount());

        /* Verify that the copy resumed correctly. */
        verify(envHome, backupDir, files2);

        verifyAsEnv(backupDir);
    }

    @Test
    public void testLeaseExpiration()
        throws Exception {

        int errorFileNum = 2;

        /*
         * Verify that leases are created and expire as expected.
         */
        NetworkBackup backup1 = new TestNetworkBackup(serverAddress,
                                                      backupEnv,
                                                      (short) 1,
                                                      true,
                                                      errorFileNum);
        /* Shorten the lease duration for test purposes. */
        long leaseDuration = 1*1000;
        try {
            fm.setLeaseDuration(leaseDuration);
            backup1.execute();
            fail("Exception expected");
        } catch (IOException e) {
            /* Expected. */
        }
        /* Wait for server to detect broken connection. */
        Thread.sleep(500);
        /* Verify that the lease was created. */
        assertEquals(1, fm.getLeaseCount());
        Thread.sleep(leaseDuration);
        /* Verify that the lease has expired after its duration. */
        assertEquals(0, fm.getLeaseCount());

        /* Resume after lease expiration. */
        NetworkBackup backup2 =
            new NetworkBackup(serverAddress,
                              backupEnv.getHome(),
                              new NameIdPair("n1", (short) 1),
                              true,
                              backupFileManager,
                              backupLogManager,
                              channelFactory);
        /* Verify that the lease was renewed. */
        String[] files2 = backup2.execute();
        /* Verify that the copy resumed correctly. */
        verify(envHome, backupDir, files2);

        verifyAsEnv(backupDir);
    }

    private void verify(File envDir,
                        File envBackupDir,
                        String backupEnvFiles[])
       throws IOException {

       for (String backupFile : backupEnvFiles) {
           File envFile = null;

           /*
            * The file names returned by NetworkBackup only apply on the
            * replicas (if they have sub directories enabled while Feeder
            * doesn't), so need to calculate the real path on the Feeder
            * according the file name.
            */
           if (envMultiDirs) {
               if (backupMultiDirs) {
                   envFile = new File(envDir, backupFile);
               } else {
                   envFile = new File(DbInternal.getNonNullEnvImpl(env).
                                      getFileManager().
                                      getFullFileName(backupFile));
               }
           } else {
               if (backupMultiDirs) {
                   int start = backupFile.indexOf(File.separator);
                   envFile = new File
                       (envDir,
                        backupFile.substring(start, backupFile.length()));
               } else {
                   envFile = new File(envDir, backupFile);
               }
           }
           FileInputStream envStream = new FileInputStream(envFile);
           FileInputStream envBackupStream =
               new FileInputStream(new File(envBackupDir, backupFile));
           int ib1, ib2;
           do {
               ib1 = envStream.read();
               ib2 = envBackupStream.read();
           } while ((ib1 == ib2) && (ib1 != -1));
           assertEquals(ib1, ib2);
           envStream.close();
           envBackupStream.close();
       }
    }

    void verifyAsEnv(File dir)
        throws EnvironmentLockedException, DatabaseException {

        /* Close the backupEnv abnormally. */
        DbInternal.getNonNullEnvImpl(backupEnv).abnormalClose();

        Environment benv = new Environment(dir, envConfig);
        /* Note that verify modifies log files. */
        benv.verify(vconfig, System.err);
        benv.close();
    }

    /**
     * Class to provoke a client failure when requesting a specific file.
     */
    private class TestNetworkBackup extends NetworkBackup {
        int errorFileNum = 0;

        public TestNetworkBackup(InetSocketAddress serverSocket,
                                 Environment backupEnv,
                                 short clientId,
                                 boolean retainLogfiles,
                                 int errorFileNum)
            throws DatabaseException {

            super(serverSocket,
                  backupEnv.getHome(),
                  new NameIdPair("node"+clientId, clientId),
                  retainLogfiles,
                  DbInternal.getNonNullEnvImpl(backupEnv).getFileManager(),
                  DbInternal.getNonNullEnvImpl(backupEnv).getLogManager(),
                  channelFactory);
            this.errorFileNum = errorFileNum;
        }

        @Override
        protected void getFile(File file)
            throws IOException, ProtocolException, DigestException,
                    RestoreMarker.FileCreationException {
            if (errorFileNum-- == 0) {
                throw new IOException("test exception");
            }
            super.getFile(file);
        }
    }
}
