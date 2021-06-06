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

package com.sleepycat.je.junit;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.sleepycat.je.utilint.JVMSystemUtils;

/**
 * [#16348] JE file handle leak when multi-process writing a same environment.
 *
 * Write this thread for creating multi-process test, you can generate 
 * a JUnitProcessThread, each thread would create a process for you, just need
 * to assign the command line parameters to the thread.
 */
public class JUnitProcessThread extends JUnitThread {
    private final List<String> cmd;

    /*
     * Variable record the return value of the process. If 0, it means the 
     * process finishes successfully, if not, the process fails.
     */
    private int exitVal = 0;

    /* If true, don't print out the standard output in this process. */
    private final boolean suppressOutput;

    /**
     * Pass the process name and command line to the constructor.
     */
    public JUnitProcessThread(String threadName, String[] parameters) {
        this(threadName, 0, null, parameters, false);
    }

    public JUnitProcessThread(String threadName, 
                              int heapMB,
                              String[] jvmParams,
                              String[] parameters) {
        this(threadName, heapMB, jvmParams, parameters, false);
    }

    public JUnitProcessThread(String threadName,
                              int heapMB,
                              String[] jvmParams,
                              String[] parameters, 
                              boolean suppressOutput) {
        super(threadName);

        this.suppressOutput = suppressOutput;
        
        if (jvmParams == null) {
            jvmParams = new String[0];
        }

        cmd = new ArrayList<>();

        cmd.add(System.getProperty("java.home") +
            System.getProperty("file.separator") + "bin" + 
            System.getProperty("file.separator") + "java" + 
            (System.getProperty("path.separator").equals(":") ? "" : "w.exe"));

        if (heapMB != 0) {
            heapMB = Math.max(heapMB, JVMSystemUtils.MIN_HEAP_MB);
            cmd.add("-Xmx" + heapMB + "m");
        }

        JVMSystemUtils.addZingJVMArgs(cmd);

        cmd.addAll(Arrays.asList(jvmParams));

        cmd.add("-cp");
        cmd.add(
            "." + System.getProperty("path.separator") + 
            System.getProperty("java.class.path"));

        cmd.addAll(Arrays.asList(parameters));
    }

    /** Generate a process for this thread.*/
    public void testBody() {
        try {
            Process proc = new ProcessBuilder(cmd).start();

            InputStream error = proc.getErrorStream();
            InputStream output = proc.getInputStream();

            Thread err = new Thread(new OutErrReader(error));
            err.start();

            if (!suppressOutput) {
                Thread out = new Thread(new OutErrReader(output));
                out.start();
            }

            exitVal = proc.waitFor();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /** Return the return value of the created process to main thread. */
    public int getExitVal() {
        return exitVal;
    }

    /** 
     * A class prints out the output information of writing serialized files.
     */
    public static class OutErrReader implements Runnable {
        final InputStream is;
        final boolean ignoreOutput;

        OutErrReader(InputStream is) {
            this.is = is;
            this.ignoreOutput = false;
        }

        public OutErrReader(InputStream is, boolean ignoreOutput) {
            this.is = is;
            this.ignoreOutput = ignoreOutput;
        }

        public void run() {
            try {
                BufferedReader in =
                    new BufferedReader(new InputStreamReader(is));
                String temp;
                while((temp = in.readLine()) != null) {
                    if (!ignoreOutput) {
                        System.err.println(temp);
                    }
                }
                is.close();
            } catch (Exception e) {
                if (!ignoreOutput) {
                    e.printStackTrace();
                }
            }
        }
    }
}
