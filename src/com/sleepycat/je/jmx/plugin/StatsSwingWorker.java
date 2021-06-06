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

package com.sleepycat.je.jmx.plugin;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import javax.swing.SwingWorker;

/* 
 * The class takes the responsibility for updating the tabs in JConsole plugin.
 */
public class StatsSwingWorker extends 
    SwingWorker<List<List<Map.Entry<String, String>>>, Object> {

    private final ArrayList<Stats> list;

    public StatsSwingWorker(ArrayList<Stats> list) {
        this.list = list;
    }

    @Override
    public List<List<Map.Entry<String, String>>> doInBackground() {
        ArrayList<List<Map.Entry<String, String>>> statsList= 
            new ArrayList<List<Map.Entry<String, String>>>();
        for (Stats status: list) {
            statsList.add(status.getResultsList());
        }

        return statsList;
    }

    @Override
    protected void done() {
        try {
            if (get() != null) {
                for (int i = 0; i < list.size(); i++) {
                    list.get(i).getTModel().setList(get().get(i));
                    list.get(i).getTModel().fireTableDataChanged();
                }
            }
        } catch (InterruptedException e) {
        } catch (ExecutionException e) {
        }
    }
}
