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

package com.sleepycat.je.log;

import static java.nio.file.StandardWatchEventKinds.ENTRY_DELETE;
import static java.nio.file.StandardWatchEventKinds.OVERFLOW;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.DbConfigManager;
import com.sleepycat.je.dbi.EnvironmentFailureReason;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.utilint.StoppableThread;
import com.sun.nio.file.SensitivityWatchEventModifier;

class FileDeletionDetector {

    private final EnvironmentImpl envImpl;

    /*
     * Store the files deleted by JE. It is used to detect unexpected file
     * deletion, e.g. log file wrongly deleted by user.
     */
    private final Set<String> filesDeletedByJE;

    /* WatchService to monitor directory change. */
    private final WatchService fileDeletionWatcher;

    /* Timer to periodically detect unexpected log file deletion. */
    private final Timer fileDeletionTimer;

    /* TimerTask to be executed by Timer to detect log file deletion. */
    private final FileDeleteDetectTask fileDeletionTask;

    /*
     * Used to check whether "mv" action happens.
     * 
     * If we register "folder" to WatchService, and if we execute "mv folder
     * folder.new", the WatchService can not detect this action. So we check
     * whether the corresponding dirs still exist to determine whether "mv"
     * action happens.
     */
    private Map<WatchKey, File> fileDeletionWatchKeys;

    FileDeletionDetector(final File dbEnvHome,
                         final File[] dbEnvDataDirs,
                         final EnvironmentImpl envImpl) {
        this.envImpl = envImpl;

        filesDeletedByJE = new HashSet<>();
        fileDeletionWatchKeys = new HashMap<>();

        /*
         * Create the WatchService which monitors the root env
         * home or the sub-directories, such as data00N.
         */
        try {
            fileDeletionWatcher =
                FileSystems.getDefault().newWatchService();

            /* 
             * Register the root env home or the sub-directories.
             * 
             * If there exist sub-directories, then only sub-
             * directories contain .jdb files, so we only register
             * sub-directories. Otherwise, we only register the
             * root env home.
             *
             * Here, we do not use Files.walkFileTree(
             * Path, FileVisitor). This is because this method will
             * check every entry(file or directory) under the root
             * path. The following scenario may happen(
             * com.sleepycat.je.MultiProcessWriteTest.
             * testMultiEnvWrite):
             * 1. One thread is closing env
             * 2. The second thread wants to register the env home dir
             * 3. In the second thread, Files.walkFileTree() finds
             *    that je.info.0.lck, which is actually used by
             *    the first thread
             * 4. When Files.walkFileTree() wants to further check
             *    the attribute of je.info.0.lck, je.info.0.lck can
             *    not be found because the first thread have closed
             *    the env.
             *
             * In a word, Files.walkFileTree may check more un-related
             * files to cause unexpected Exception.
             * 
             * On AIX, the deletion detect has an about 4
             * seconds delay, i.e. there is a 4 seconds delay from
             * file deletion to deletion detect. We use
             * SensitivityWatchEventModifier.HIGH to resolve this.
             * 
             * In addition, when directly deleting a directory which
             * contains some files on AIX, WatchService can only
             * detect directory-deletion event, but ignore the
             * file-deletion event. So for AIX, if a directory is
             * unexpected deleted, the current method can not handle
             * it.
             */
            if (dbEnvDataDirs != null) {
                for (File f : dbEnvDataDirs) {
                    final WatchKey key = f.toPath().register(
                        fileDeletionWatcher,
                        new WatchEvent.Kind[]{ENTRY_DELETE},
                        SensitivityWatchEventModifier.HIGH);
                    fileDeletionWatchKeys.put(key, f);
                }
            } else {
                final WatchKey key = dbEnvHome.toPath().register(
                    fileDeletionWatcher,
                    new WatchEvent.Kind[]{ENTRY_DELETE},
                    SensitivityWatchEventModifier.HIGH);
                fileDeletionWatchKeys.put(key, dbEnvHome);
            }
        } catch (IOException ie) {
            throw new EnvironmentFailureException(
                envImpl,
                EnvironmentFailureReason.UNEXPECTED_EXCEPTION,
                "Can not register " +  dbEnvHome.toString() +
                " or its sub-directories to WatchService.",
                ie);
        }

        DbConfigManager configManager = envImpl.getConfigManager();
        final int interval = configManager.getDuration(
            EnvironmentParams.LOG_DETECT_FILE_DELETE_INTERVAL);
        /* Periodically detect unexpected log file deletion. */
        fileDeletionTimer = new Timer(
            envImpl.makeDaemonThreadName(
                Environment.FILE_DELETION_DETECTOR_NAME),
            true);
        fileDeletionTask = new FileDeleteDetectTask();
        fileDeletionTimer.schedule(fileDeletionTask, 0, interval);
    }

    private class FileDeleteDetectTask extends TimerTask {
        public void run() {
            boolean success = false;
            try {
                processLogFileDeleteEvents();
                success = true;
            } catch (EnvironmentFailureException e) {
                /*
                 * Log file is deleted unexpectedly. We have already
                 * invalidated the environment. Here we just close
                 * the Timer, TimerTask and the WatchService. Do this
                 * in finally block.
                 */
            } catch (Exception e) {
                /*
                 * It is possible that processLogFileDeleteEvents is doing
                 * something on WatchService when FileManager.close call
                 * WatchService.close. Then processLogFileDeleteEvents may
                 * throw Exception, such as IOException or
                 * java.nio.file.ClosedWatchServiceException. For this
                 * situation, we just ignore the Exception and close
                 * the Timer, TimerTask and the WatchService in finally
                 * block. The close may be redundant, but for these close
                 * method, the second and subsequent calls have no effect.
                 *
                 * If the Exception is not caused by the WatchService.close
                 * in FileManager.close, then some ugly things happen. We
                 * should handle the Exception by method which is similar
                 * to StoppableThread.handleUncaughtException.
                 *
                 * !envImpl.isClosing represents that FileManager is not
                 * closing. requestShutdownDaemons sets envImpl.isClosing
                 * to be true and requestShutdownDaemons happens before
                 * FileManager.close.
                 *
                 * envImpl.isValid represents that the current env is valid.
                 * If the evn is invalid, then some other place has already
                 * invalidate the env, so here we do not need to invalidate
                 * the env again. 
                 */
                if (envImpl.isValid() && !envImpl.isClosing()) {
                    handleUnexpectedThrowable(Thread.currentThread(), e);
                }
            } catch (Error e) {
                /*
                 * Some ugly things happen.
                 */
                handleUnexpectedThrowable(Thread.currentThread(), e);
            } finally {
                if (!success) {
                    try {
                        close();
                    } catch (IOException ie) {
                        handleUnexpectedThrowable(Thread.currentThread(), ie);
                    }
                }
            }
        }
    }

    private void handleUnexpectedThrowable(Thread t, Throwable e) {
        StoppableThread.handleUncaughtException(
            envImpl.getLogger(), envImpl, t, e);
    }

    /*
     * Detect unexpected log file deletion.
     */
    private void processLogFileDeleteEvents() throws Exception{
        /*
         * We may register multi-directories to the WatchService,
         * there may be multi WatchKey for this WatchService. It is
         * possible that file deletion happen in these multi directories
         * simultaneously, i.e. multi WatchKey may be non-null.
         * We should handle them all.
         */
        while (true) {
            final WatchKey key = fileDeletionWatcher.poll();
            if (key == null) {
                /*
                 * If no event is detected, we check whether the directories
                 * corresponding to the WatchKeys still exist.
                 */
                for (final File file : fileDeletionWatchKeys.values()) {
                    if (!file.exists()) {
                        final String dir = file.getCanonicalPath();
                        throw new IOException(
                            "Directory " + dir + " does not exist now. " +
                            "Something abnormal may happen.");
                    }
                }
                break;
            }
 
            for (final WatchEvent<?> event: key.pollEvents()) {
                final WatchEvent.Kind kind = event.kind();
                if (kind == OVERFLOW) {
                    continue;
                }

                /* Get the file name from the context. */
                final WatchEvent<Path> ev = cast(event);
                final String fileName = ev.context().toString();
                if (fileName.endsWith(FileManager.JE_SUFFIX)) {
                    synchronized (filesDeletedByJE) {
                        if (!filesDeletedByJE.contains(fileName)) {
                            /* TimerTask.run will handle this exception. */
                            throw new EnvironmentFailureException(
                                envImpl,
                                EnvironmentFailureReason.
                                    LOG_UNEXPECTED_FILE_DELETION,
                                "Log file " + fileName +
                                " was deleted unexpectedly.");
                        }
                        filesDeletedByJE.remove(fileName);
                    }
                }
            }

            if (!key.reset()) {
                /*
                 * If key.reset returns false, it indicates that the key
                 * is no longer valid. Invalid state happens when one of
                 * the following events occurs:
                 * 1. The process explicitly cancels the key by using the
                 *    cancel method. --- The JE code never does this.
                 * 2. The directory becomes inaccessible.  -- This indicates
                 *    an abnormal situation.
                 * 3. The watch service is closed. -- The close may be
                 *    expected, e.g. caused by WatchService.close. The
                 *    exception caused by this can be handled in
                 *    FileDeleteDetectTask.run.
                 */
                final String dir =
                    fileDeletionWatchKeys.get(key).getCanonicalPath();
                throw new IOException(
                    "Watch Key corresponding to " + dir + " return false " +
                    "when reset. Something abnormal may happen.");
            }
        }
    }

    void addDeletedFile(String fileName) {
        synchronized (filesDeletedByJE) {
            filesDeletedByJE.add(fileName);
        }
    }

    public void close() throws IOException {
        fileDeletionTask.cancel();
        fileDeletionTimer.cancel();
        synchronized(fileDeletionWatcher) {
            fileDeletionWatcher.close();
        }
    }
    
    @SuppressWarnings("unchecked")
    <T> WatchEvent<T> cast(WatchEvent<?> event) {
        return (WatchEvent<T>)event;
    }
}
