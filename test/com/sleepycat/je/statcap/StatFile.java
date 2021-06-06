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

package com.sleepycat.je.statcap;

import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;

import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.rep.utilint.StatCaptureRepDefinitions;
import com.sleepycat.je.utilint.Stat;
import com.sleepycat.je.utilint.StatDefinition;
import com.sleepycat.je.utilint.StatDefinition.StatType;
import com.sleepycat.je.utilint.StatGroup;

public class StatFile {

    static final StatCaptureDefinitions csd = new StatCaptureDefinitions();
    static final StatCaptureRepDefinitions rcsd =
                                               new StatCaptureRepDefinitions();

    public static SortedMap<String, Long> sumItUp(File dir, String fileprefix)
        throws IOException {

        Map<String, Boolean> isIncMap = setIncrementalMap();
        FindFile ff = new FindFile(fileprefix);
        File[] statfiles = dir.listFiles(ff);
        SortedMap<String, File> sortedFiles = new TreeMap<String, File>();
        for (File statfile : statfiles) {
            sortedFiles.put(statfile.getName(), statfile);
        }

        SortedMap<String, Long> retmap = new TreeMap<String, Long>();
        for (Entry<String, File> fe : sortedFiles.entrySet()) {
            Map<String, Long> datamap = sumItUp(fe.getValue());
            for (Entry<String, Long> e : datamap.entrySet()) {
                Long oldval = retmap.get(e.getKey());
                if (oldval != null && getValue(isIncMap.get(e.getKey()))) {
                    retmap.put(e.getKey(),
                               new Long(oldval.longValue() +
                               e.getValue().longValue()));
                }
                else {
                    retmap.put(e.getKey(), e.getValue());
                }
            }
        }
        return retmap;
    }

    public static SortedMap<String, Long> sumItUp(BufferedReader input)
        throws IOException {

        try {
            SortedMap<String, Long> retmap = new TreeMap<String, Long>();
            String[] header = input.readLine().split(",");
            boolean[] isIncremental = new boolean[header.length];
            for (int i = 0; i < header.length; i++) {
                retmap.put(header[i], Long.valueOf(0));
                StatDefinition sd = csd.getDefinition(header[i]);
                if (sd == null) {
                    sd = rcsd.getDefinition(header[i]);
                }

                if (sd == null) {
                    /* could be custom or java stats */
                    isIncremental[i] = false;
                } else {
                    isIncremental[i] =
                        (sd.getType() == StatType.INCREMENTAL) ? true : false;
                }
            }

            String row = null;
            while ( (row = input.readLine()) != null) {
                String[] val = row.split(",");
                assertTrue("header and row mismatch columns header " +
                    header.length + " body columns " + val.length,
                    val.length == header.length);
                for (int i = 0; i<header.length; i++) {
                    try {

                       long rv = Long.valueOf(val[i]);
                       if (isIncremental[i]) {
                           long cval = retmap.get(header[i]);
                           assertTrue("value should be greater than zero." +
                                       header[i] + "value " + cval,
                                       cval >= 0);
                           rv += retmap.get(header[i]);
                       }
                       retmap.put(header[i], rv);
                    } catch (NumberFormatException e) {
                        /* ignore this row may not be numeric */
                    }
                }
            }
            return retmap;
        } finally {
            if (input != null) {
                    input.close();
            }
        }
    }

   public static SortedMap<String, Long> sumItUp(File sf) throws IOException {

       try {
           if (!sf.exists()) {
               return new TreeMap<String, Long>();
           }
           return sumItUp(new BufferedReader(new FileReader(sf)));
       } catch (FileNotFoundException e) {
           throw EnvironmentFailureException.unexpectedState(
               "Unexpected Exception accessing file " +
                sf.getAbsolutePath() + e.getMessage());
       }
   }

   public static SortedMap<String, Long> getMap(Collection<StatGroup> csg) {

       TreeMap<String, Long> statsMap = new TreeMap<String, Long>();
       for (StatGroup sg : csg) {
           for (Entry<StatDefinition, Stat<?>> e :
                sg.getStats().entrySet()) {
               String mapName =
                       (sg.getName() + ":" +
                               e.getKey().getName()).intern();
               Object val = e.getValue().get();
               /* get stats back as strings. */
               if (val instanceof Number) {
                   statsMap.put(mapName,
                                ((Number) val).longValue());
               }
           }
       }
       return statsMap;
   }

   public static SortedMap<String, Stat<?>>
       getNameValueMap(Collection<StatGroup> csg) {

       TreeMap<String, Stat<?>> statsMap = new TreeMap<String, Stat<?>>();
       for (StatGroup sg : csg) {
           for (Entry<StatDefinition, Stat<?>> e : sg.getStats().entrySet()) {
               String mapName =
                   (sg.getName() + ":" + e.getKey().getName()).intern();
               statsMap.put(mapName, e.getValue());
           }
       }
       return statsMap;
   }

   private static Map<String, Boolean> setIncrementalMap() {
       SortedSet<String> projections = rcsd.getStatisticProjections();
       Map<String, Boolean> retmap = new HashMap<String, Boolean>();
       for (String name : projections) {
           StatDefinition sd = csd.getDefinition(name);
           if (sd == null) {
               sd = rcsd.getDefinition(name);
           }
           boolean isIncremental = false;
           if (sd != null &&
               sd.getType() == StatType.INCREMENTAL) {
               isIncremental = true;
           }
           retmap.put(name, isIncremental);
       }
       return retmap;
   }

   private static boolean getValue(Boolean b) {
       return (b == null) ? false : b;
   }

  static class FindFile implements FileFilter {

       String fileprefix;
       FindFile(String fileprefix) {
           this.fileprefix = fileprefix;
       }

       @Override
       public boolean accept(File f) {
           return f.getName().startsWith(fileprefix);
       }
   }
}
