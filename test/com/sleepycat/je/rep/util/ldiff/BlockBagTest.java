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

package com.sleepycat.je.rep.util.ldiff;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;
import java.util.List;

import org.junit.Test;

import com.sleepycat.util.test.TestBase;

public class BlockBagTest extends TestBase {

    /**
     * A get() following a remove() shouldn't return any removed blocks.
     */
    @Test
    public void testGetAfterRemove() {
        Block b;
        BlockBag bb;
        byte[] beginKey = { 0, 0, 0, 0 };
        byte[] beginData =
            { (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff };
        byte[] md5Hash =
            { (byte) 0xdb, (byte) 0xdb, (byte) 0xdb, (byte) 0xdb };
        int i;
        int count = 10;
        int numKeys = 10000;
        long rollingChksum = 7654321L;

        b = null;
        bb = new BlockBag();

        /*
         * Add count with the same checksum, one unique, then another count
         * with the same checksum.
         */
        for (i = 0; i < count; i++) {
            b = new Block(i);
            b.setBeginKey(beginKey);
            b.setBeginData(beginData);
            b.setMd5Hash(md5Hash);
            b.setNumRecords(numKeys);
            b.setRollingChksum(rollingChksum);
            bb.add(b);
        }
        b = new Block(i++);
        b.setBeginKey(beginKey);
        b.setBeginData(beginData);
        b.setMd5Hash(md5Hash);
        b.setNumRecords(numKeys);
        b.setRollingChksum(0L);
        bb.add(b);
        for (; i < 2 * count + 1; i++) {
            b = new Block(i);
            b.setBeginKey(beginKey);
            b.setBeginData(beginData);
            b.setMd5Hash(md5Hash);
            b.setNumRecords(numKeys);
            b.setRollingChksum(rollingChksum);
            bb.add(b);
        }

        List<Block> blocks = bb.get(rollingChksum);
        assertTrue(blocks != null);
        if (blocks == null)
            return;
        assertEquals(2 * count, blocks.size());

        List<Block> toRemove = bb.get(0L);
        assertTrue(toRemove != null);
        if (toRemove == null)
            return;
        assertEquals(1, toRemove.size());
        List<Block> removed = bb.remove(toRemove.get(0));
        assertTrue(removed != null);
        if (removed == null)
            return;
        assertEquals(count, removed.size());
        assertEquals(count, bb.size());

        blocks = bb.get(rollingChksum);
        assertTrue(blocks != null);
        if (blocks == null)
            return;
        assertEquals(count, blocks.size());
    }

    /**
     * Insert blocks with identical checksums, make sure get() returns them all
     * in insertion order.
     */
    @Test
    public void testGetMultiple() {
        BlockBag bb;
        byte[] beginKey = { 0, 0, 0, 0 };
        byte[] beginData =
            { (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff };
        byte[] md5Hash =
            { (byte) 0xdb, (byte) 0xdb, (byte) 0xdb, (byte) 0xdb };
        int numKeys = 10000;
        int count = 10;
        long rollingChksum = 7654321L;

        bb = new BlockBag();

        for (int i = 0; i < count; i++) {
            Block b = new Block(i);
            b.setBeginKey(beginKey);
            b.setBeginData(beginData);
            b.setMd5Hash(md5Hash);
            b.setNumRecords(numKeys);
            b.setRollingChksum(rollingChksum);
            bb.add(b);
        }

        List<Block> blocks = bb.get(rollingChksum);
        assertTrue(blocks != null);
        if (blocks == null)
            return;
        assertEquals(count, blocks.size());
        int id1, id2;
        for (int i = 1; i < blocks.size(); i++) {
            // Block id indicates insertion order for this test
            id1 = blocks.get(i - 1).getBlockId();
            id2 = blocks.get(i).getBlockId();
            assertTrue(id1 < id2);
        }
    }

    /**
     * If a checksum does not exist in the bag, a null should be returned
     */
    @Test
    public void testGetNonexistent() {
        BlockBag bb;
        byte[] beginKey = { 0, 0, 0, 0 };
        byte[] beginData =
            { (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff };
        byte[] md5Hash =
            { (byte) 0xdb, (byte) 0xdb, (byte) 0xdb, (byte) 0xdb };
        int numKeys = 10000;
        int count = 10;
        long rollingChksum = 7654321L;

        bb = new BlockBag();

        for (int i = 0; i < count; i++) {
            Block b = new Block(i);
            b.setBeginKey(beginKey);
            b.setBeginData(beginData);
            b.setMd5Hash(md5Hash);
            b.setNumRecords(numKeys);
            b.setRollingChksum(rollingChksum);
            bb.add(b);
        }

        List<Block> blocks = bb.get(0L);
        assertTrue(blocks == null);
    }

    /**
     * The for ( : ) construct should iterate over blocks in insertion order
     */
    @Test
    public void testIterable() {
        Block b;
        BlockBag bb;
        byte[] beginKey = { 0, 0, 0, 0 };
        byte[] beginData =
            { (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff };
        byte[] md5Hash =
            { (byte) 0xdb, (byte) 0xdb, (byte) 0xdb, (byte) 0xdb };
        int i;
        int count = 10;
        int numKeys = 10000;
        long rollingChksum = 0L;

        b = null;
        bb = new BlockBag();

        /*
         * Add count with the same checksum, one unique, then another count
         * with the same checksum.
         */
        for (i = 0; i < count; i++) {
            b = new Block(i);
            b.setBeginKey(beginKey);
            b.setBeginData(beginData);
            b.setMd5Hash(md5Hash);
            b.setNumRecords(numKeys);
            b.setRollingChksum(rollingChksum++);
            bb.add(b);
        }

        /*
         * Iterate through the records, there should be count and their ids
         * should be increasing.
         */
        i = 0;
        Block oldBlk = null;
        for (Block blk : bb) {
            if (oldBlk != null)
                assertTrue(oldBlk.getBlockId() < blk.getBlockId());
            oldBlk = blk;
            i++;
        }
        assertEquals(count, i);
    }

    /**
     * The for ( : ) construct shouldn't return deleted items.
     */
    @Test
    public void testIterableAfterDelete() {
        Block b;
        BlockBag bb;
        byte[] beginKey = { 0, 0, 0, 0 };
        byte[] beginData =
            { (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff };
        byte[] md5Hash =
            { (byte) 0xdb, (byte) 0xdb, (byte) 0xdb, (byte) 0xdb };
        int i;
        int count = 10;
        int numKeys = 10000;
        long rollingChksum = 7654321L;

        b = null;
        bb = new BlockBag();

        /*
         * Add count with the same checksum, one unique, then another count
         * with the same checksum.
         */
        for (i = 0; i < count; i++) {
            b = new Block(i);
            b.setBeginKey(beginKey);
            b.setBeginData(beginData);
            b.setMd5Hash(md5Hash);
            b.setNumRecords(numKeys);
            b.setRollingChksum(rollingChksum);
            bb.add(b);
        }
        b = new Block(i++);
        b.setBeginKey(beginKey);
        b.setBeginData(beginData);
        b.setMd5Hash(md5Hash);
        b.setNumRecords(numKeys);
        b.setRollingChksum(0L);
        bb.add(b);
        for (; i < 2 * count + 1; i++) {
            b = new Block(i);
            b.setBeginKey(beginKey);
            b.setBeginData(beginData);
            b.setMd5Hash(md5Hash);
            b.setNumRecords(numKeys);
            b.setRollingChksum(rollingChksum);
            bb.add(b);
        }

        List<Block> toRemove = bb.get(0L);
        assertTrue(toRemove != null);
        if (toRemove == null)
            return;
        assertEquals(1, toRemove.size());
        List<Block> removed = bb.remove(toRemove.get(0));
        assertTrue(removed != null);
        if (removed == null)
            return;

        /* Iterate through the records, there should be count. */
        assertEquals(count, bb.getBlockIndex() - 1);
    }

    /**
     * The typical iterator usage should return items in insertion order.
     */
    @Test
    public void testIterator() {
        Block b;
        BlockBag bb;
        byte[] beginKey = { 0, 0, 0, 0 };
        byte[] beginData =
            { (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff };
        byte[] md5Hash =
            { (byte) 0xdb, (byte) 0xdb, (byte) 0xdb, (byte) 0xdb };
        int i;
        int count = 10;
        int numKeys = 10000;
        long rollingChksum = 0L;

        b = null;
        bb = new BlockBag();

        /*
         * Add count with the same checksum, one unique, then another count
         * with the same checksum.
         */
        for (i = 0; i < count; i++) {
            b = new Block(i);
            b.setBeginKey(beginKey);
            b.setBeginData(beginData);
            b.setMd5Hash(md5Hash);
            b.setNumRecords(numKeys);
            b.setRollingChksum(rollingChksum++);
            bb.add(b);
        }

        /*
         * Iterate through the records, there should be count and their ids
         * should be increasing.
         */
        Iterator<Block> iter = bb.iterator();
        i = 0;
        Block oldBlk = null;
        while (iter.hasNext()) {
            Block blk = iter.next();
            if (oldBlk != null)
                assertTrue(oldBlk.getBlockId() < blk.getBlockId());
            oldBlk = blk;
            i++;
        }
        assertEquals(count, i);
    }

    /**
     * The typical iterator usage should not return deleted items
     */
    @Test
    public void testIteratorAfterDelete() {
        Block b;
        BlockBag bb;
        byte[] beginKey = { 0, 0, 0, 0 };
        byte[] beginData =
            { (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff };
        byte[] md5Hash =
            { (byte) 0xdb, (byte) 0xdb, (byte) 0xdb, (byte) 0xdb };
        int i;
        int count = 10;
        int numKeys = 10000;
        long rollingChksum = 7654321L;

        b = null;
        bb = new BlockBag();

        /*
         * Add count with the same checksum, one unique, then another count
         * with the same checksum.
         */
        for (i = 0; i < count; i++) {
            b = new Block(i);
            b.setBeginKey(beginKey);
            b.setBeginData(beginData);
            b.setMd5Hash(md5Hash);
            b.setNumRecords(numKeys);
            b.setRollingChksum(rollingChksum);
            bb.add(b);
        }
        b = new Block(i++);
        b.setBeginKey(beginKey);
        b.setBeginData(beginData);
        b.setMd5Hash(md5Hash);
        b.setNumRecords(numKeys);
        b.setRollingChksum(0L);
        bb.add(b);
        for (; i < 2 * count + 1; i++) {
            b = new Block(i);
            b.setBeginKey(beginKey);
            b.setBeginData(beginData);
            b.setMd5Hash(md5Hash);
            b.setNumRecords(numKeys);
            b.setRollingChksum(rollingChksum);
            bb.add(b);
        }

        List<Block> toRemove = bb.get(0L);
        assertTrue(toRemove != null);
        if (toRemove == null)
            return;
        assertEquals(1, toRemove.size());
        List<Block> removed = bb.remove(toRemove.get(0));
        assertTrue(removed != null);
        if (removed == null)
            return;

        /* Iterate through the records, there should be count. */
        Iterator<Block> iter = bb.iterator();
        i = 0;
        while (iter.hasNext()) {
            b = iter.next();
            i++;
        }
        assertEquals(count, i);
    }

    /**
     * Populate a bag and then immediately remove everything. The bag should be
     * empty.
     */
    @Test
    public void testRemoveAll() {
        Block b;
        BlockBag bb;
        byte[] beginKey = { 0, 0, 0, 0 };
        byte[] beginData =
            { (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff };
        byte[] md5Hash =
            { (byte) 0xdb, (byte) 0xdb, (byte) 0xdb, (byte) 0xdb };
        int numKeys = 10000;
        int count = 10;
        long rollingChksum = 0L;

        b = null;
        bb = new BlockBag();

        for (int i = 0; i < count; i++) {
            b = new Block(i);
            b.setBeginKey(beginKey);
            b.setBeginData(beginData);
            b.setMd5Hash(md5Hash);
            b.setNumRecords(numKeys);
            b.setRollingChksum(rollingChksum++);
            bb.add(b);
        }

        /*
         * Remove the last block. Unmatched should be the rest of the blocks.
         */
        List<Block> unmatched = bb.removeAll();
        assertTrue(unmatched != null);
        if (unmatched == null)
            return;
        assertEquals(count, unmatched.size());
        int id1, id2;
        for (int i = 1; i < unmatched.size(); i++) {
            /* Block id indicates insertion order for this test. */
            id1 = unmatched.get(i - 1).getBlockId();
            id2 = unmatched.get(i).getBlockId();
            assertTrue(id1 < id2);
        }

        assertEquals(0, bb.size());
        List<Block> retrieve = bb.get(2L);
        assertTrue(retrieve == null);
    }

    /**
     * Test removeAll() after some items have been deleted.
     */
    @Test
    public void testRemoveSomeThenAll() {
        Block b;
        BlockBag bb;
        byte[] beginKey = { 0, 0, 0, 0 };
        byte[] beginData =
            { (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff };
        byte[] md5Hash =
            { (byte) 0xdb, (byte) 0xdb, (byte) 0xdb, (byte) 0xdb };
        int i;
        int count = 10;
        int numKeys = 10000;
        long rollingChksum = 7654321L;

        b = null;
        bb = new BlockBag();

        /*
         * Add count with the same checksum, one unique, then another count
         * with the same checksum.
         */
        for (i = 0; i < count; i++) {
            b = new Block(i);
            b.setBeginKey(beginKey);
            b.setBeginData(beginData);
            b.setMd5Hash(md5Hash);
            b.setNumRecords(numKeys);
            b.setRollingChksum(rollingChksum);
            bb.add(b);
        }
        b = new Block(i++);
        b.setBeginKey(beginKey);
        b.setBeginData(beginData);
        b.setMd5Hash(md5Hash);
        b.setNumRecords(numKeys);
        b.setRollingChksum(0L);
        bb.add(b);
        for (; i < 2 * count + 1; i++) {
            b = new Block(i);
            b.setBeginKey(beginKey);
            b.setBeginData(beginData);
            b.setMd5Hash(md5Hash);
            b.setNumRecords(numKeys);
            b.setRollingChksum(rollingChksum);
            bb.add(b);
        }

        List<Block> blocks = bb.get(rollingChksum);
        assertTrue(blocks != null);
        if (blocks == null)
            return;
        assertEquals(2 * count, blocks.size());

        List<Block> toRemove = bb.get(0L);
        assertTrue(toRemove != null);
        if (toRemove == null)
            return;
        assertEquals(1, toRemove.size());
        List<Block> removed = bb.remove(toRemove.get(0));
        assertTrue(removed != null);
        if (removed == null)
            return;
        assertEquals(count, removed.size());

        blocks = bb.get(rollingChksum);
        assertTrue(blocks != null);
        if (blocks == null)
            return;
        assertEquals(count, blocks.size());

        // Remove the remaining blocks.
        List<Block> unmatched = bb.removeAll();
        assertTrue(unmatched != null);
        if (unmatched == null)
            return;
        assertEquals(count, unmatched.size());
        int id1, id2;
        for (i = 1; i < unmatched.size(); i++) {
            // Block id indicates insertion order for this test
            id1 = unmatched.get(i - 1).getBlockId();
            id2 = unmatched.get(i).getBlockId();
            assertTrue(id1 < id2);
        }

        assertEquals(0, bb.size());
        List<Block> retrieve = bb.get(rollingChksum);
        assertTrue(retrieve == null);
    }

    /**
     * Removing a block removes all blocks inserted before it as well.
     */
    @Test
    public void testRemoveUnmatched() {
        Block b;
        BlockBag bb;
        byte[] beginKey = { 0, 0, 0, 0 };
        byte[] beginData =
            { (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff };
        byte[] md5Hash =
            { (byte) 0xdb, (byte) 0xdb, (byte) 0xdb, (byte) 0xdb };
        int numKeys = 10000;
        int count = 10;
        long rollingChksum = 0L;

        b = null;
        bb = new BlockBag();

        for (int i = 0; i < count; i++) {
            b = new Block(i);
            b.setBeginKey(beginKey);
            b.setBeginData(beginData);
            b.setMd5Hash(md5Hash);
            b.setNumRecords(numKeys);
            b.setRollingChksum(rollingChksum++);
            bb.add(b);
        }

        assertTrue(b != null);

        /*
         * Remove the last block. Unmatched should be the rest of the blocks.
         */
        List<Block> unmatched = bb.remove(b);
        assertTrue(unmatched != null);
        if (unmatched == null)
            return;
        assertEquals(count - 1, unmatched.size());
        int id1, id2;
        for (int i = 1; i < unmatched.size(); i++) {
            /* Block id indicates insertion order for this test. */
            id1 = unmatched.get(i - 1).getBlockId();
            id2 = unmatched.get(i).getBlockId();
            assertTrue(id1 < id2);
        }
    }
}
