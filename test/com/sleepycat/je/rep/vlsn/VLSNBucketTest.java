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

package com.sleepycat.je.rep.vlsn;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.VLSN;
import com.sleepycat.util.test.TestBase;

/**
 * Low level test for basic VLSNBucket functionality
 * TODO: add more tests for buckets with non-populated offsets.
 */
public class VLSNBucketTest extends TestBase {

    private final boolean verbose = Boolean.getBoolean("verbose");

    @Test
    public void testBasic() {
        int stride = 3;
        int maxMappings = 2;
        int maxDistance = 50;

        /*
         * Make a list of vlsn->lsns mappings for test data:
         * vlsn=1,lsn=3/10, 
         * vlsn=2,lsn=3/20, 
         *  ... etc ..
         */
        List<VLPair> vals = initData();
        VLSNBucket bucket = new VLSNBucket(3, // fileNumber, 
                                           stride, 
                                           maxMappings,
                                           maxDistance,
                                           vals.get(0).vlsn);

        /* Insert vlsn 1, 2 */
        assertTrue(bucket.empty());
        assertTrue(bucket.put(vals.get(0).vlsn, vals.get(0).lsn));
        assertFalse(bucket.empty());
        assertTrue(bucket.put(vals.get(1).vlsn, vals.get(1).lsn));
        
        /* 
         * Do some error checking - Make sure we can't put in a lsn for another
         * file. 
         */
        assertFalse(bucket.put(vals.get(2).vlsn, DbLsn.makeLsn(4, 20)));

        /* Make sure we can't put in a lsn that's too far away. */
        assertFalse(bucket.put(vals.get(2).vlsn, DbLsn.makeLsn(3, 100)));

        assertTrue(bucket.owns(vals.get(0).vlsn));
        assertTrue(bucket.owns(vals.get(1).vlsn));
        assertFalse(bucket.owns(vals.get(2).vlsn));

        assertTrue(bucket.put(vals.get(2).vlsn, vals.get(2).lsn));

        /* 
         * Check the mappings. There are three that were put in, and only
         * one that is stored. (1/10, (stored) 2/20, 3/30), plus the last lsn
         */
        assertEquals(vals.get(0).lsn,
                     bucket.getGTELsn(vals.get(0).vlsn));
        assertEquals(vals.get(2).lsn,
                     bucket.getGTELsn(vals.get(1).vlsn));
        assertEquals(vals.get(2).lsn,
                     bucket.getGTELsn(vals.get(2).vlsn));

        assertEquals(1, bucket.getNumOffsets());

        /* Fill the bucket up so there's more mappings. Add 4/40, 5/50, 6/60 */
        assertTrue(bucket.put(vals.get(3).vlsn, vals.get(3).lsn));
        assertTrue(bucket.put(vals.get(4).vlsn, vals.get(4).lsn));
        assertTrue(bucket.put(vals.get(5).vlsn, vals.get(5).lsn));

        /* 
         * Check that we reached the max mappings limit, and that this put is
         * refused.
         */
        assertFalse(bucket.put(new VLSN(7), DbLsn.makeLsn(3,70)));

        checkAccess(bucket, stride, vals);
    }

    /*
     * This bucket holds vlsn 1-6, just check all the access methods. */
    private void checkAccess(VLSNBucket bucket, 
                             int stride, 
                             List<VLPair> vals) {

        /* 
         * All the mappings should be there, and we should be able to retrieve
         * them.
         */
        for (int i = 0; i < vals.size(); i += stride) {
            VLPair pair = vals.get(i);
            assertTrue(bucket.owns(pair.vlsn));
            assertEquals(pair.lsn, bucket.getLsn(pair.vlsn));
        }

        /*
         *  With the strides, it's more work to use a loop to check GTE and LTE
         * than to just hard code the checks. If the expected array is grown,
         * add to these checks!
         */
        assertEquals(vals.get(0).lsn, 
                     bucket.getLTELsn(vals.get(0).vlsn));
        assertEquals(vals.get(0).lsn, 
                     bucket.getLTELsn(vals.get(1).vlsn));
        assertEquals(vals.get(0).lsn, 
                     bucket.getLTELsn(vals.get(2).vlsn));
        assertEquals(vals.get(3).lsn, 
                     bucket.getLTELsn(vals.get(3).vlsn));
        assertEquals(vals.get(3).lsn,
                     bucket.getLTELsn(vals.get(4).vlsn));
        assertEquals(vals.get(5).lsn, 
                     bucket.getLTELsn(vals.get(5).vlsn));

        assertEquals(vals.get(0).lsn,
                     bucket.getGTELsn(vals.get(0).vlsn));
        assertEquals(vals.get(3).lsn,
                     bucket.getGTELsn(vals.get(1).vlsn));
        assertEquals(vals.get(3).lsn, 
                     bucket.getGTELsn(vals.get(2).vlsn));
        assertEquals(vals.get(3).lsn,
                     bucket.getGTELsn(vals.get(3).vlsn));
        assertEquals(vals.get(5).lsn, 
                     bucket.getGTELsn(vals.get(4).vlsn));
        assertEquals(vals.get(5).lsn,
                     bucket.getGTELsn(vals.get(5).vlsn));
    }

    /**
     * Make a list of vlsn->lsns mappings for test data:
     * vlsn=1,lsn=3/10, 
     * vlsn=2,lsn=3/20, 
     *  ... etc ..
     */
    private List<VLPair> initData() {

        List<VLPair> vals = new ArrayList<VLPair>();
        for (int i = 1; i <= 6; i++) {
            vals.add(new VLPair(i, 3,  10 * i));
        }
        return vals;
    }

    @Test
    public void testOutOfOrderPuts() {
        int stride = 3;
        int maxMappings = 2;
        int maxDistance = 50;

        List<VLPair> vals = initData();
        VLSNBucket bucket = new VLSNBucket(3, // fileNumber, 
                                           stride, 
                                           maxMappings,
                                           maxDistance,
                                           vals.get(0).vlsn);

        /* Insert vlsn 2, 1 */
        assertTrue(bucket.empty());
        assertTrue(bucket.put(vals.get(1).vlsn, vals.get(1).lsn));
        assertFalse(bucket.empty());

        assertTrue(bucket.owns(vals.get(1).vlsn));
        assertTrue(bucket.owns(vals.get(0).vlsn));
        assertFalse(bucket.owns(vals.get(2).vlsn));

        assertTrue(bucket.put(vals.get(0).vlsn, vals.get(0).lsn));

        /* 
         * Do some error checking - Make sure we can't put in a lsn for another
         * file. 
         */
        assertFalse(bucket.put(vals.get(2).vlsn, DbLsn.makeLsn(4, 20)));

        /* Make sure we can't put in a lsn that's too far away. */
        assertFalse(bucket.put(vals.get(2).vlsn, DbLsn.makeLsn(3, 100)));

        assertFalse(bucket.owns(vals.get(2).vlsn));

        /* 
         * Check the mappings. There are three that were put in, and only
         * one that is stored. (1/10, (stored) 2/20, 3/30)
         */
        assertEquals(1, bucket.getNumOffsets());

        /* 
         * Fill the bucket up so there's more mappings. Add 4/40, 5/50, 6/60 
         * out of order.
         */
        assertTrue(bucket.put(vals.get(4).vlsn, vals.get(4).lsn));
        assertTrue(bucket.put(vals.get(5).vlsn, vals.get(5).lsn));
        assertTrue(bucket.put(vals.get(2).vlsn, vals.get(2).lsn));
        assertTrue(bucket.put(vals.get(3).vlsn, vals.get(3).lsn));

        /* 
         * Check that we reached the max mappings limit, and that this put is
         * refused.
         */
        assertFalse(bucket.put(new VLSN(7), DbLsn.makeLsn(3,70)));

        checkAccess(bucket, stride, vals);
    }

    /*
     * Create a bucket with some out of order puts, so that there are empty
     * offsets, and make sure that the non-null gets succeed.
     */
    @Test
    public void testGetNonNullWithHoles() {

        VLSNBucket bucket = new VLSNBucket(0,      // fileNumber, 
                                           2,      // stride, 
                                           20,     // maxMappings
                                           10000,  // maxDist
                                           new VLSN(1));
        assertTrue(bucket.put(new VLSN(1), 10));
        assertTrue(bucket.put(new VLSN(3), 30));
        /* 
         * Note that when we put in VLSN 6, the bucet's file offset array
         * will be smaller than it would normally be. It will only be
         * size=2. Do this to test the edge case of getNonNullLTELsn on 
         * a too-small array.
         */
        assertTrue(bucket.put(new VLSN(6), 60));

        assertEquals(10, bucket.getLTELsn(new VLSN(1)));
        assertEquals(10, bucket.getLTELsn(new VLSN(2)));
        assertEquals(30, bucket.getLTELsn(new VLSN(3)));
        assertEquals(30, bucket.getLTELsn(new VLSN(4)));
        assertEquals(30, bucket.getLTELsn(new VLSN(5)));
        assertEquals(60, bucket.getLTELsn(new VLSN(6)));

        assertEquals(10, bucket.getGTELsn(new VLSN(1)));
        assertEquals(30, bucket.getGTELsn(new VLSN(2)));
        assertEquals(30, bucket.getGTELsn(new VLSN(3)));
        assertEquals(60, bucket.getGTELsn(new VLSN(4)));
        assertEquals(60, bucket.getGTELsn(new VLSN(5)));
        assertEquals(60, bucket.getGTELsn(new VLSN(6)));

        assertEquals(10, bucket.getGTELsn(new VLSN(1)));
        assertEquals(30, bucket.getGTELsn(new VLSN(2)));
        assertEquals(30, bucket.getGTELsn(new VLSN(3)));
        assertEquals(60, bucket.getGTELsn(new VLSN(4)));
        assertEquals(60, bucket.getGTELsn(new VLSN(5)));
        assertEquals(60, bucket.getGTELsn(new VLSN(6)));
    }

    @Test
    public void testRemoveFromTail() {
        int stride = 3;

        /* Create a set of test mappings. */
        List<VLPair> expected = new ArrayList<VLPair>();
        int start = 10;
        int end = 20;
        for (int i = start; i < end; i++) {
            expected.add(new VLPair( i, 0, i*10));
        }

        /*
         * Load a bucket with the expected mappings. Call removeFromTail()
         * at different points, and then check that all expected values remain.
         */
        for (int startDeleteVal = start-1; 
             startDeleteVal < end + 1; 
             startDeleteVal++) {

            VLSNBucket bucket = loadBucket(expected, stride);

            VLSN startDeleteVLSN = new VLSN(startDeleteVal);  
            if (verbose) {
                System.out.println("startDelete=" + startDeleteVal);
            }
            bucket.removeFromTail(startDeleteVLSN, 
                                  (startDeleteVal - 1) * 10); // prevLsn
            
            if (verbose) {
                System.out.println("bucket=" + bucket);
            }

            for (VLPair p : expected) {
                long lsn = DbLsn.NULL_LSN;
                if (bucket.owns(p.vlsn)) {
                    lsn = bucket.getLsn(p.vlsn);
                }

                if (p.vlsn.compareTo(startDeleteVLSN) >= 0) {
                    /* Anything >= startDeleteVLSN should be truncated. */
                    assertEquals("startDelete = " + startDeleteVLSN + 
                                 " p=" + p + " bucket=" + bucket, 
                                 DbLsn.NULL_LSN, lsn);
                } else {

                    if (((p.vlsn.getSequence() - start) % stride) == 0) {
                        /* 
                         * If is on a stride boundary, there should be a 
                         * mapping.
                         */
                        assertEquals("bucket=" + bucket +  " p= " + p,
                                     p.lsn, lsn);
                    } else if (p.vlsn.compareTo
                               (startDeleteVLSN.getPrev()) == 0) {
                        /* It's the last mapping. */
                        assertEquals(p.lsn, lsn);
                    } else {
                        assertEquals(DbLsn.NULL_LSN, lsn);
                    }
                }
            }
        }
    }

    /**
     * [#20796]
     * Truncate a bucket when the truncation point is between the last file
     * offset and the last vlsn.
     */
    @Test
    public void testTruncateAfterFileOffset() {
        int stride = 3;

        /* Create a set of test mappings. */
        List<VLPair> testMappings = new ArrayList<VLPair>();
        testMappings.add(new VLPair( 10, 0, 10));
        testMappings.add(new VLPair( 15, 0, 20));
        testMappings.add(new VLPair( 20, 0, 30));
        /* Skip the 25 stride offset -- assume that vlsn 28 came in first. */
        testMappings.add(new VLPair( 28, 0, 40));

        VLSNBucket bucket = loadBucket(testMappings, 5);
        bucket.removeFromTail(new VLSN(26), DbLsn.NULL_LSN);
        assertEquals(bucket.getLast(), new VLSN(20));
        assertEquals(bucket.getLastLsn(), DbLsn.makeLsn(0, 30));

        bucket = loadBucket(testMappings, 5);
        bucket.removeFromTail(new VLSN(26), DbLsn.makeLsn(0, 33));
        assertEquals(bucket.getLast(), new VLSN(25));
        assertEquals(bucket.getLastLsn(), DbLsn.makeLsn(0, 33));
    }

    private VLSNBucket loadBucket(List<VLPair> expected, int stride) {
        int maxMappings = 5;
        int maxDistance = 50;
        
        VLSNBucket bucket = new VLSNBucket(0, // fileNumber, 
                                           stride, 
                                           maxMappings,
                                           maxDistance,
                                           new VLSN(10));
        for (VLPair pair : expected) {
            assertTrue("pair = " + pair,
                       bucket.put(pair.vlsn, pair.lsn));
        }
        return bucket;
    }
}
