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

package com.sleepycat.je.evictor;

import static com.sleepycat.je.evictor.EvictorStatDefinition.BIN_DELTA_BLIND_OPS;
import static com.sleepycat.je.evictor.EvictorStatDefinition.BIN_DELTA_FETCH_MISS;
import static com.sleepycat.je.evictor.EvictorStatDefinition.BIN_FETCH;
import static com.sleepycat.je.evictor.EvictorStatDefinition.BIN_FETCH_MISS;
import static com.sleepycat.je.evictor.EvictorStatDefinition.BIN_FETCH_MISS_RATIO;
import static com.sleepycat.je.evictor.EvictorStatDefinition.CACHED_IN_COMPACT_KEY;
import static com.sleepycat.je.evictor.EvictorStatDefinition.CACHED_IN_NO_TARGET;
import static com.sleepycat.je.evictor.EvictorStatDefinition.CACHED_IN_SPARSE_TARGET;
import static com.sleepycat.je.evictor.EvictorStatDefinition.EVICTOR_DIRTY_NODES_EVICTED;
import static com.sleepycat.je.evictor.EvictorStatDefinition.EVICTOR_EVICTION_RUNS;
import static com.sleepycat.je.evictor.EvictorStatDefinition.EVICTOR_LNS_EVICTED;
import static com.sleepycat.je.evictor.EvictorStatDefinition.EVICTOR_NODES_EVICTED;
import static com.sleepycat.je.evictor.EvictorStatDefinition.EVICTOR_NODES_MOVED_TO_PRI2_LRU;
import static com.sleepycat.je.evictor.EvictorStatDefinition.EVICTOR_NODES_MUTATED;
import static com.sleepycat.je.evictor.EvictorStatDefinition.EVICTOR_NODES_PUT_BACK;
import static com.sleepycat.je.evictor.EvictorStatDefinition.EVICTOR_NODES_SKIPPED;
import static com.sleepycat.je.evictor.EvictorStatDefinition.EVICTOR_NODES_STRIPPED;
import static com.sleepycat.je.evictor.EvictorStatDefinition.EVICTOR_NODES_TARGETED;
import static com.sleepycat.je.evictor.EvictorStatDefinition.EVICTOR_ROOT_NODES_EVICTED;
import static com.sleepycat.je.evictor.EvictorStatDefinition.EVICTOR_SHARED_CACHE_ENVS;
import static com.sleepycat.je.evictor.EvictorStatDefinition.FULL_BIN_MISS;
import static com.sleepycat.je.evictor.EvictorStatDefinition.GROUP_DESC;
import static com.sleepycat.je.evictor.EvictorStatDefinition.GROUP_NAME;
import static com.sleepycat.je.evictor.EvictorStatDefinition.LN_FETCH;
import static com.sleepycat.je.evictor.EvictorStatDefinition.LN_FETCH_MISS;
import static com.sleepycat.je.evictor.EvictorStatDefinition.N_BYTES_EVICTED_CACHEMODE_DESC;
import static com.sleepycat.je.evictor.EvictorStatDefinition.N_BYTES_EVICTED_CACHEMODE_NAME;
import static com.sleepycat.je.evictor.EvictorStatDefinition.N_BYTES_EVICTED_CRITICAL_DESC;
import static com.sleepycat.je.evictor.EvictorStatDefinition.N_BYTES_EVICTED_CRITICAL_NAME;
import static com.sleepycat.je.evictor.EvictorStatDefinition.N_BYTES_EVICTED_DAEMON_DESC;
import static com.sleepycat.je.evictor.EvictorStatDefinition.N_BYTES_EVICTED_DAEMON_NAME;
import static com.sleepycat.je.evictor.EvictorStatDefinition.N_BYTES_EVICTED_EVICTORTHREAD_DESC;
import static com.sleepycat.je.evictor.EvictorStatDefinition.N_BYTES_EVICTED_EVICTORTHREAD_NAME;
import static com.sleepycat.je.evictor.EvictorStatDefinition.N_BYTES_EVICTED_MANUAL_DESC;
import static com.sleepycat.je.evictor.EvictorStatDefinition.N_BYTES_EVICTED_MANUAL_NAME;
import static com.sleepycat.je.evictor.EvictorStatDefinition.PRI1_LRU_SIZE;
import static com.sleepycat.je.evictor.EvictorStatDefinition.PRI2_LRU_SIZE;
import static com.sleepycat.je.evictor.EvictorStatDefinition.THREAD_UNAVAILABLE;
import static com.sleepycat.je.evictor.EvictorStatDefinition.UPPER_IN_FETCH;
import static com.sleepycat.je.evictor.EvictorStatDefinition.UPPER_IN_FETCH_MISS;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.sleepycat.je.CacheMode;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.EnvironmentMutableConfig;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.DbConfigManager;
import com.sleepycat.je.dbi.DbTree;
import com.sleepycat.je.dbi.EnvConfigObserver;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.dbi.INList;
import com.sleepycat.je.dbi.MemoryBudget;
import com.sleepycat.je.log.Provisional;
import com.sleepycat.je.tree.BIN;
import com.sleepycat.je.tree.ChildReference;
import com.sleepycat.je.tree.IN;
import com.sleepycat.je.tree.SearchResult;
import com.sleepycat.je.tree.Tree;
import com.sleepycat.je.tree.WithRootLatched;
import com.sleepycat.je.utilint.AtomicLongStat;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.FloatStat;
import com.sleepycat.je.utilint.IntStat;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.LongStat;
import com.sleepycat.je.utilint.StatDefinition;
import com.sleepycat.je.utilint.StatGroup;
import com.sleepycat.je.utilint.StoppableThreadFactory;
import com.sleepycat.je.utilint.TestHook;
import com.sleepycat.je.utilint.TestHookExecute;

/**
 *
 * Overview
 * --------
 *
 * The Evictor is responsible for managing the JE cache. The cache is
 * actually a collection of in-memory btree nodes, implemented by the
 * com.sleepycat.je.dbi.INList class. A subset of the nodes in te INList
 * are candidates for eviction. This subset is tracked in one or more
 * LRULists, which are maintained by the Evictor. When a node is evicted,
 * it is detached from its containing BTree and then removed from the INList
 * and from its containing LRUList. Once all references to an evicted node
 * are removed, it can be GC'd by the JVM.
 *
 * The Evictor owns a pool of threads that are available to handle eviction
 * tasks. The eviction pool is a standard java.util.concurrent thread pool,
 * and can be mutably configured in terms of core threads, max threads, and
 * keepalive times.
 *
 * Eviction is carried out by three types of threads:
 * 1. An application thread, in the course of doing critical eviction.
 * 2. Daemon threads, such as the cleaner or INCompressor, in the course of
 *    doing their respective duties.
 * 3. Eviction pool threads.
 *
 * Memory consumption is tracked by the MemoryBudget. The Arbiter, which is
 * also owned by the Evictor, is used to query the MemoryBudget and determine
 * whether eviction is actually needed, and if so, how many bytes should be
 * evicted by an evicting thread.
 *
 * Multiple threads can do eviction concurrently. As a result, it's important
 * that eviction is both thread safe and as parallel as possible.  Memory
 * thresholds are generally accounted for in an unsynchronized fashion, and are
 * seen as advisory. The only point of true synchronization is around the
 * selection of a node for eviction. The act of eviction itself can be done
 * concurrently.
 *
 * The eviction method is not reentrant, and a simple concurrent hash map
 * of threads is used to prevent recursive calls.
 *
 * Details on the implementation of the LRU-based eviction policy
 * --------------------------------------------------------------
 *
 * ------------------
 * Data structures
 * ------------------
 *
 * An LRU eviction policy is approximated by one or more LRULists. An LRUList
 * is a doubly linked list consisting of BTree nodes. If a node participates
 * in an LRUList, then whenever it is accessed, it moves to the "back" of the
 * list. When eviction is needed, the evictor evicts the nodes at the "front"
 * of the LRULists.
 *
 * An LRUList is implemented as 2 IN references: a "front" ref pointing to the
 * IN at the front of the list and a "back" ref, pointing to the IN at the back
 * of the list. In addition, each IN has "nextLRUNode" and "prevLRUNode" refs
 * for participating in an LRUList. This implementation works because an IN can
 * belong to at most 1 LRUList at a time. Furthermore, it is the responsibility
 * of the Evictor to know which LRUList a node belongs to at any given time
 * (more on this below). As a result, each LRUList can assume that a node will
 * either not be in any list at all, or will belong to "this" list. This way,
 * membership of a node to an LRUList can be tested by just checking that
 * either the nextLRUNode or prevLRUNode field of the node is non-null.
 *
 * The operations on an LRUList are:
 *
 * - addBack(IN) : 
 * Insert an IN at the back of the list. Assert that the node does not belong
 * to an LRUList already.
 *
 * - addFront(IN) : 
 * Insert an IN at the front of the list.  Assert that the node does not belong
 * to an LRUList already.
 *
 * - moveBack(IN) :
 * Move an IN to the back of the list, if it is in the list already. Noop
 * if the node is not in the list.
 *
 * - moveFront(IN) : 
 * Move an IN to the front of the list, if it is in the list already. Noop
 * if the node is not in the list.
 *
 * - removeFront() : 
 * Remove the IN at the front of the list and return it to the caller. 
 * Return null if the list is empty.
 *
 * - remove(IN) : 
 * Remove the IN from the list, if it is there. Return true if the node was
 * in the list, false otherwise.
 *
 * - contains(IN):
 * Return true if the node is contained in the list, false otherwise.
 *
 * All of the above methods are synchronized on the LRUList object. This may
 * create a synchronization bottleneck. To alleviate this, the Evictor uses
 * multiple LRULists, which taken together comprise a logical LRU list, called
 * an LRUSet. The number of LRULists per LRUSet (numLRULists) is fixed and
 * determined by a config parameter (max of 64). The LRULists are stored in
 * an array whose length is numLRULists.
 *
 * The Evictor actually maintains 2 LRUSets: priority-1 and priority-2.
 * Within an LRUSet, the nodeId is used to place a node to an LRUList: a
 * node with id N goes to the (N % numLRULists)-th list. In addition, each
 * node has a flag (isInPri2LRU) to identify which LRUSet it belongs to.
 * This way, the Evictor knows which LRUList a node should belong to, and 
 * accesses the appropriate LRUList instance when it needs to add/remove/move
 * a node within the LRU.
 *
 * Access to the isInPri2LRU flag is synchronized via the SH/EX node latch.
 *
 * When there is no off-heap cache configured, the priority-1 LRU is the
 * "mixed" one and the priority-2 LRU is the "dirty" one. When there is an
 * off-heap cache configured, the priority-1 LRU is the "normal" one and the
 * priority-2 LRU is the "level-2" one.
 *
 * Justification for the mixed and dirty LRUSets: We would like to keep dirty
 * INs in memory as much as possible to achieve "write absorption". Ideally,
 * dirty INs should be logged by the checkpointer only. So, we would like to
 * have the option in the Evictor to chose a clean IN to evict over a dirty
 * IN, even if the dirty IN is colder than the clean IN. In this mode, having
 * a single LRUSet will not perform very well in the situation when most (or
 * a lot) or the INs are dirty (because each time we get a dirty IN from an
 * LRUList, we would have to put it back to the list and try another IN until
 * we find a clean one, thus spending a lot of CPU time trying to select an
 * eviction target).
 *
 * Justification for the normal and level-2 LRUSets: With an off-heap cache,
 * if level-2 INs were not treated specially, the main cache evictor may run
 * out of space and (according to LRU) evict a level 2 IN, even though the IN
 * references off-heap BINs (which will also be evicted). The problem is that
 * we really don't want to evict the off-heap BINs (or their LNs) when the
 * off-heap cache is not full. Therefore we only evict level-2 INs with
 * off-heap children when there are no other nodes that can be evicted. A
 * level-2 IN is moved to the priority-2 LRUSet when it is encountered by the
 * evictor in the priority-1 LRUSet.
 *
 * Within each LRUSet, picking an LRUList to evict from is done in a round-
 * robin fashion. To this end, the Evictor maintains 2 int counters:
 * nextPri1LRUList and nextPri2LRUList. To evict from the priority-1 LRUSet, an
 * evicting thread picks the (nextPri1LRUList % numLRULists)-th list, and
 * then increments nextPri1LRUList. Similarly, to evict from the priority-2
 * LRUSet, an evicting thread picks the (nextPri2LRUList % numLRULists)-th
 * list, and then increments nextPri2LRUList. This does not have to be done in
 * a synchronized way.
 *
 * A new flag (called hasCachedChildren) is added to each IN to indicate
 * whether the IN has cached children or not. This flag is used and maintained
 * for upper INs (UINs) only. The need for this flag is explained below.
 * Access to this flag is synchronized via the SH/EX node latch.
 *
 * ---------------------------------------------------------------------------
 * LRUSet management: adding/removing/moving INs in/out of/within the LRUSets
 * ---------------------------------------------------------------------------
 *
 * We don't want to track upper IN (UIN) nodes that have cached children.
 * There are 2 reasons for this: (a) we cannot evict UINs with cached children
 * (the children must be evicted first) and (b) UINs will normally have high
 * access rate, and would add a lot of CPU overhead if they were tracked. 
 *
 * The hasCachedChildren flag is used as a quick way to determine whether a
 * UIN has cached children or not.
 *
 * Adding a node to the LRU.
 * -------------------------
 *
 * A IN N is added in an LRUSet via one of the following Evictor methods:
 * addBack(IN), addFront(IN), pri2AddBack(IN), or pri2AddFront(IN). The
 * first 2 add the node to the priority-1 LRUSet and set its isInPri2LRU flag
 * to false. The last 2 add the node to the priority-2 LRUSet and set its
 * isInPri2LRU flag to true.
 *
 * Note: DINs and DBINs are never added to the LRU.
 *
 * A node N is added to the LRU in the following situations:
 *
 * 1. N is fetched into memory from the log. Evictor.addBack(N) is called
 *    inside IN.postfetchInit() (just before N is connected to its parent).
 *
 * 2. N is a brand new node created during a split, and either N is a BIN or
 *    N does not get any cached children from its split sibling.
 *    Evictor.addFront(N) is called if N is a BIN and the cachemode is
 *    MAKE_COLD or EVICT_BIN. Otherwise, Evictor.addBack(child) is called.
 *
 * 3. N is a UIN that is being split, and before the split it had cached
 *    children, but all its cached children have now moved to its newly
 *    created sibling. Evictor.addBack(N) is called in this case.
 *
 * 4. N is a UIN that looses its last cached child (either because the child is
 *    evicted or it is deleted). Evictor.addBack(N) is called inside
 *    IN.setTarget(), if the target is null, N is a UIN, N's hasCachedChildren
 *    flag is true, and N after setting the target to null, N has no remaining
 *    cached children.
 *
 * 5. N is the 1st BIN in a brand new tree. In this case, Evictor.addBack(N)
 *    is called inside Tree.findBinForInsert().
 *
 * 6. N is a node visited during IN.rebuildINList() and N is either a BIN or
 *    a UIN with no cached children.
 *
 * 7. An evicting thread T removes N from the LRU, but after T EX-latches N,
 *    it determines that N is not evictable or should not be evicted, and
 *    should be put back in the LRU. T puts N back to the LRU using one of
 *    the above 4 methods (for details, read about the eviction processing
 *    below), but ONLY IF (a) N is still in the INList, and (b) N is not in
 *    the LRU already.
 *
 *    Case (b) can happen if N is a UIN and after T removed N from the LRU
 *    but before T could latch N, another thread T1 added a child to N and
 *    removed that child. Thus, by item 4 above, T1 adds N back to the LRU.
 *    Furthermore, since N is now back in the LRU, case (a) can now happen
 *    as well if another thread can evict N before T latches it.
 *
 * 8. When the checkpointer (or any other thread/operation) cleans a dirty IN,
 *    it must move it from the priority-2 LRUSet (if there) to the priority-1
 *    one. This is done via the Evictor.moveToPri1LRU(N) method: If the
 *    isInPri2LRU flag of N is true, LRUList.remove(N) is called to remove
 *    the node from the priority-2 LRUSet. If N was indeed in the priority-2
 *    LRUSet (i.e., LRUList.remove() returns true), addBack(N) is called to
 *    put it in the priority-1 LRUSet.
 *
 *    By moving N to the priority-1 LRUSet only after atomically removing it
 *    from the priority-2 LRUSet and checking that it was indeed there, we
 *    prevent N from being added into the LRU if N has been or would be removed
 *    from the LRU by a concurrently running evicting thread.
 *    
 * In cases 2, 3, 4, 5, 7, and 8 N is EX-latched. In case 1, the node is not
 * latched, but it is inaccessible by any other threads because it is not
 * connected to its parent yet and the parent is EX-latched (but N has already
 * been inserted in the INList; can this create any problems ?????). In case
 * 6 there is only one thread running. So, in all cases it's ok to set the
 * isInPri2LRU flag of the node.
 *
 * Question: can a thread T try to add a node N, seen as a Java obj instance,
 * into the LRU, while N is already there? I believe not, and LRUList addBack()
 * and addFront() methods assert that this cannot happen. In cases 1, 2, and 5
 * above N is newly created node, so it cannot be in the LRU already. In cases
 * 3 and 4, N is a UIN that has cached children, so it cannot be in the LRU.
 * In case 6 there is only 1 thread. Finally, in cases 7 and 8, T checks that
 * N is not in the LRU before attempting to add it (and the situation cannot
 * change between tis check and the insertion into the LRU because N is EX-
 * latched).
 *
 * Question: can a thread T try to add a node N, seen as a logical entity
 * represented by its nodeId, into the LRU, while N is already there?
 * Specifically, (a) can two Java instances, N1 and N2, of the same node
 * N exist in memory at the same time, and (b) while N1 is in the LRU, can
 * a thread T try to add N2 in the LRU? The answer to (a) is "yes", and as
 * far as I can think, the answer to (b) is "no", but there is no explicit
 * check in the code for this. Consider the following sequence of events:
 * Initially only N1 is in memory and in the LRU. An evicting thread T1
 * removes N1 from the LRU, thread T2 adds N1 in the LRU, thread T3 removes
 * N1 from the LRU and actually evicts it, thread T4 fetches N from the log,
 * thus creating instance N2 and adding N2 to the LRU, thread T1 finally
 * EX-latches N1 and has to decide what to do with it. The check in case
 * 7a above makes sure that N1 will not go back to the LRU. In fact the
 * same check makes sure that N1 will not be evicted (i.e., logged, if
 * dirty). T1 will just skip N1, thus allowing it to be GCed.
 *
 * Removing a node from the LRU
 * ----------------------------
 *
 * A node is removed from the LRU when it is selected as an eviction target
 * by an evicting thread. The thread chooses an LRUList list to evict from
 * and calls removeFront() on it. The node is not latched when it is removed
 * from the LRU in this case. The evicting thread is going to EX-latch the
 * node shortly after the removal. But as explain already earlier, between
 * the removal and the latching, another thread may put the node back to the
 * LRU, and as a result, another thread may also choose the same node for
 * eviction. The node may also be detached from the BTree, or its database
 * closed, or deleted.
 * 
 * A node may also be removing from the LRU by a non-evicting thread. This
 * is done via the Evictor.remove(IN) method. The method checks the node's
 * isInDrtryLRU flag to determine which LRUSet the node belongs to (if any)
 * and then calls LRUList.remove(N). The node must be at least SH latched
 * when the method is called. The method is a noop if the node is not in the
 * LRU. The node may not belong to any LRUList, because it has been selected
 * for eviction by another thread (and thus removed from LRU), but the
 * evicting thread has not yet latched the node. There are 3 cases (listed
 * below) where Evictor.remove(N) is called. In the first two cases
 * Evictor.remove(N) is invoked from INList.removeInternal(N). This makes
 * sure that N is removed from the LRU whenever it it removed from the
 * INList (to guarantee that the nodes in the LRU are always a subset of
 * the nodes in the INList).
 *
 * 1. When a tree branch containing N gets detached from its tree. In this
 *    case, INList.remove(N) is invoked inside accountForSubtreeRemoval() or
 *    accountForDeferredWriteSubtreeRemoval().
 * 
 * 2. When the database containing N gets deleted or truncated. In this case,
 *    INList.iter.remove() is called in DatabaseImpl.finishDeleteProcessing().
 *
 * 3. N is a UIN with no cached children (hasCachedChildren flag is false)
 *    and a new child for N is fetched. The call to Evictor.remove(N) is
 *    done inside IN.setTarget().
 *
 * Moving a node within the LRU
 * ----------------------------
 *
 * A node N is moved within its containing LRUList (if any) via the Evictor
 * moveBack(IN) and moveFront(IN) methods. The methods check the isInPri2LRU
 * flag of the node to determine the LRUSet the node belongs to and then move
 * the node to the back or to the front of the LRUList. The node will be at
 * least SH latched when these methods are called. Normally, the IN will be
 * in an LRUList. However, it may not belong to any LRUList, because it has
 * been selected for eviction by another thread (and thus removed from LRU),
 * but the evicting thread has not yet EX-latched the node. In this case,
 * these methods are is a noop. The methods are called in the following
 * situations:
 *
 * 1. N is latched with cachemode DEFAULT, KEEP_HOT, or EVICT_LN and N is a
 *    BIN or a UIN with no cached children (the hasCachedChildren flag is
 *    used to check if the UIN has cached children, so we don't need to
 *    iterate over all of the node's child entries). In this case,
 *    Evictor.moveBack(N) .
 *
 * 2. N is latched with cachemode MAKE_COLD or EVICT_BIN and N is a BIN.
 *    In this case, Evictor.moveFront(N) is called.
 * 
 * -------------------
 * Eviction Processing
 * -------------------
 *
 * A thread can initiate eviction by invoking the Evictor.doEviction() method.
 * This method implements an "eviction run". An eviction run consists of a
 * number of "eviction passes", where each pass is given as input a maximum
 * number of bytes to evict. An eviction pass is implemented by the
 * Evictor.evictBatch() method. 
 *
 * Inside Evictor.evictBatch(), an evicting thread T:
 * 
 * 1. Picks the priority-1 LRUset initially as the "current" LRUSet to be
 *    processed,
 *
 * 2. Initializes the max number of nodes to be processed per LRUSet to the
 *    current size of the priority-1 LRUSet,
 *
 * 3. Executes the following loop:
 *
 * 3.1. Picks a non-empty LRUList from the current LRUSet in a round-robin
 *      fashion, as explained earlier, and invokes LRUList.removeFront() to
 *      remove the node N at the front of the list. N becomes the current
 *      eviction target.
 *
 * 3.2. If the DB node N belongs to has been deleted or closed, skips this node,
 *      i.e., leaves N outside the LRU and goes to 3.4.
 *
 * 3.3. Calls ProcessTarget(N) (see below)
 *
 * 3.4. If the current LRUset is the priority-1 one and the number of target nodes
 *      processed reaches the max number allowed, the priority-2 LRUSet becomes
 *      the current one, the max number of nodes to be processed per LRUSet is
 *      set to the current size of the priority-2 LRUSet, and the number of
 *      nodes processed is reset to 0.
 *
 * 3.5. Breaks the loop if the max number of bytes to evict during this pass
 *      has been reached, or memConsumption is less than (maxMemory - M) (where
 *      M is a config param), or the number of nodes that have been processed
 *      in the current LRUSet reaches the max allowed.
 *
 * --------------------------
 * The processTarget() method
 * --------------------------
 *
 * This method is called after a node N has been selected for eviction (and as
 * result, removed from the LRU). The method EX-latches N and determines
 * whether it can/should really be evicted, and if not what is the appropriate
 * action to be taken by the evicting thread. Before returning, the method
 * unlatches N. Finally, it returns the number of bytes evicted (if any).
 *
 * If a decision is taken to evict N or mutate it to a BINDelta, N must first
 * be unlatched and its parent must be searched within the tree. During this
 * search, many things can happen to the unlatched N, and as a result, after
 * the parent is found and the N is relatched, processTarget() calls itself
 * recursively to re-consider all the possible actions for N.
 *
 * Let T be an evicting thread running processTarget() to determine what to do
 * with a target node N. The following is the list of possible outcomes:
 *
 * 1. SKIP - Do nothing with N if:
 *    (a) N is in the LRU. This can happen if N is a UIN and while it is
 *        unlatched by T, other threads fetch one or more of N's children,
 *        but then all of N's children are removed again, thus causing N to
 *        be put back to the LRU.
 *    (b) N is not in the INList. Given than N can be put back to the LRU while
 *        it is unlatched by T, it can also be selected as an eviction target
 *        by another thread and actually be evicted.
 *    (c) N is a UIN with cached children. N could have acquired children
 *        after the evicting thread removed it from the LRU, but before the
 *        evicting thread could EX-latch it.
 *    (d) N is the root of the DB naming tree or the DBmapping tree. 
 *    (e) N is dirty, but the DB is read-only.
 *    (f) N's environment used a shared cache and the environment has been
 *        closed or invalidated.
 *    (g) If a decision was taken to evict od mutate N, but the tree search
 *        (using N's keyId) to find N's parent, failed to find the parent, or
 *        N itself. This can happen if during the search, N was evicted by
 *        another thread, or a branch containing N was completely removed
 *        from the tree.
 *
 * 2. PUT BACK - Put N to the back of the LRUSet it last belonged to, if:
 *    (a) It is a BIN that was last accessed with KEEP_HOT cache mode.
 *    (b) N has an entry with a NULL LSN and a null target.
 *
 * 3. PARTIAL EVICT - perform partial eviction on N, if none of the cases
 *    listed above is true. Currently, partial eviction applies to BINs only
 *    and involves the eviction (stripping) of evictable LNs. If a cached LN
 *    is not  evictable, the whole BIN is not evictable as well. Currently,
 *    only MapLNs may be non-evictable (see MapLN.isEvictable()).
 *
 *    After partial eviction is performed the following outcomes are possible:
 *
 * 4. STRIPPED PUT BACK - Put N to the back of the LRUSet it last belonged to,
 *    if partial eviction did evict any bytes, and N is not a BIN in EVICT_BIN
 *    or MAKE_COLD cache mode.
 *
 * 5. PUT BACK - Put N to the back of the LRUSet it last belonged to, if
 *    no bytes were stripped, but partial eviction determined that N is not
 *    evictable.
 *
 * 6. MUTATE - Mutate N to a BINDelta, if none of the above apply and N is a
 *    BIN that can be mutated.
 *
 * 7. MOVE DIRTY TO PRI-2 LRU - Move N to the front of the priority-2 LRUSet,
 *    if none of the above apply and N is a dirty node that last belonged to
 *    the priority-1 LRUSet, and a dirty LRUSet is used (meaning that no
 *    off-heap cache is configured).
 *
 * 8. MOVE LEVEL-2 TO PRI-2 LRU - Move N to the front of the priority-2 LRUSet,
 *    if none of the above apply and N is a level-2 node with off-heap BINs
 *    that last belonged to the priority-1 LRUSet.
 *
 * 9. EVICT - Evict N is none of the above apply.
 *
 * -------
 * TODO:
 * -------
 *
 * 1. Decide what to do about assertions (keep, remove, convert to JE
 *    exceptions, convert to DEBUG-only expensive checks).
 *
 */
public class Evictor implements EnvConfigObserver {

    /*
     * If new eviction source enums are added, a new stat is created, and
     * EnvironmentStats must be updated to add a getter method.
     *
     * CRITICAL eviction is called by operations executed app or daemon
     * threads which detect that the cache has reached its limits
     * CACHE_MODE eviction is called by operations that use a specific
     * Cursor.
     * EVICTORThread is the eviction pool
     * MANUAL is the call to Environment.evictMemory, called by recovery or
     *   application code.
     */
    public enum EvictionSource {
        /* Using ordinal for array values! */
        EVICTORTHREAD {
            String getName() {
                return N_BYTES_EVICTED_EVICTORTHREAD_NAME;
            }
            String getDesc() {
                return N_BYTES_EVICTED_EVICTORTHREAD_DESC;
            }
        },
        MANUAL {
            String getName() {
                return N_BYTES_EVICTED_MANUAL_NAME;
            }
            String getDesc() {
                return N_BYTES_EVICTED_MANUAL_DESC;
            }
        },
        CRITICAL {
            String getName() {
                return N_BYTES_EVICTED_CRITICAL_NAME;
            }
            String getDesc() {
                return N_BYTES_EVICTED_CRITICAL_DESC;
            }
        },
        CACHEMODE {
            String getName() {
                return N_BYTES_EVICTED_CACHEMODE_NAME;
            }
            String getDesc() {
                return N_BYTES_EVICTED_CACHEMODE_DESC;
            }
        },
        DAEMON {
            String getName() {
                return N_BYTES_EVICTED_DAEMON_NAME;
            }
            String getDesc() {
                return N_BYTES_EVICTED_DAEMON_DESC;
            }
        };

        abstract String getName();

        abstract String getDesc();

        public StatDefinition getNumBytesEvictedStatDef() {
            return new StatDefinition(getName(), getDesc());
        }
    }

    /*
     * The purpose of EvictionDebugStats is to capture the stats of a single
     * eviction run (i.e., an execution of the Evictor.doEviction() method by
     * a single thread). An instance of EvictionDebugStats is created at the
     * start of doEviction() and is passed around to the methods called from
     * doEviction(). At the end of doEviction(), the EvictionDebugStats
     * instance can be printed out (for debugging), or (TODO) the captured
     * stats can be loaded to the global Evictor.stats.
     */
    static class EvictionDebugStats {
        boolean inPri1LRU;
        boolean withParent;

        long pri1Size;
        long pri2Size;

        int numSelectedPri1;
        int numSelectedPri2;

        int numPutBackPri1;
        int numPutBackPri2;

        int numBINsStripped1Pri1;
        int numBINsStripped2Pri1;
        int numBINsStripped1Pri2;
        int numBINsStripped2Pri2;

        int numBINsMutatedPri1;
        int numBINsMutatedPri2;

        int numUINsMoved1;
        int numUINsMoved2;
        int numBINsMoved1;
        int numBINsMoved2;

        int numUINsEvictedPri1;
        int numUINsEvictedPri2;
        int numBINsEvictedPri1;
        int numBINsEvictedPri2;

        void reset() {
            inPri1LRU = true;
            withParent = false;

            pri1Size = 0;
            pri2Size = 0;

            numSelectedPri1 = 0;
            numSelectedPri2 = 0;

            numPutBackPri1 = 0;
            numPutBackPri2 = 0;

            numBINsStripped1Pri1 = 0;
            numBINsStripped2Pri1 = 0;
            numBINsStripped1Pri2 = 0;
            numBINsStripped2Pri2 = 0;

            numBINsMutatedPri1 = 0;
            numBINsMutatedPri2 = 0;

            numUINsMoved1 = 0;
            numUINsMoved2 = 0;
            numBINsMoved1 = 0;
            numBINsMoved2 = 0;

            numUINsEvictedPri1 = 0;
            numUINsEvictedPri2 = 0;
            numBINsEvictedPri1 = 0;
            numBINsEvictedPri2 = 0;
        }

        void incNumSelected() {
            if (inPri1LRU) {
                numSelectedPri1++;
            } else {
                numSelectedPri2++;
            }
        }

        void incNumPutBack() {
            if (inPri1LRU) {
                numPutBackPri1++;
            } else {
                numPutBackPri2++;
            }
        }

        void incNumStripped() {
            if (inPri1LRU) {
                if (withParent) {
                    numBINsStripped2Pri1++;
                } else {
                    numBINsStripped1Pri1++;
                }
            } else {
                if (withParent) {
                    numBINsStripped2Pri2++;
                } else {
                    numBINsStripped1Pri2++;
                }
            }
        }

        void incNumMutated() {
            if (inPri1LRU) {
                numBINsMutatedPri1++;
            } else {
                numBINsMutatedPri2++;
            }
        }

        void incNumMoved(boolean isBIN) {
            if (withParent) {
                if (isBIN) {
                    numBINsMoved2++;
                } else {
                    numUINsMoved2++;
                }
            } else {
                if (isBIN) {
                    numBINsMoved1++;
                } else {
                    numUINsMoved1++;
                }
            }
        }

        void incNumEvicted(boolean isBIN) {
            if (inPri1LRU) {
                if (isBIN) {
                    numBINsEvictedPri1++;
                } else {
                    numUINsEvictedPri1++;
                }
            } else {
                if (isBIN) {
                    numBINsEvictedPri2++;
                } else {
                    numUINsEvictedPri2++;
                }
            }
        }

        public String toString() {
            StringBuilder sb = new StringBuilder();

            sb.append("Eviction stats PRI1: size = ");
            sb.append(pri1Size);
            sb.append("\n");
            sb.append("selected = ");
            sb.append(numSelectedPri1);
            sb.append(" | ");
            sb.append("put back = ");
            sb.append(numPutBackPri1);
            sb.append(" | ");
            sb.append("stripped = ");
            sb.append(numBINsStripped1Pri1);
            sb.append("/");
            sb.append(numBINsStripped2Pri1);
            sb.append(" | ");
            sb.append("mutated = ");
            sb.append(numBINsMutatedPri1);
            sb.append(" | ");
            sb.append("moved = ");
            sb.append(numBINsMoved1);
            sb.append("/");
            sb.append(numBINsMoved2);
            sb.append(" - ");
            sb.append(numUINsMoved1);
            sb.append("/");
            sb.append(numUINsMoved2);
            sb.append(" | ");
            sb.append("evicted = ");
            sb.append(numBINsEvictedPri1);
            sb.append(" - ");
            sb.append(numUINsEvictedPri1);
            sb.append("\n");

            sb.append("Eviction stats PRI2: size = ");
            sb.append(pri2Size);
            sb.append("\n");
            sb.append("selected = ");
            sb.append(numSelectedPri2);
            sb.append(" | ");
            sb.append("put back = ");
            sb.append(numPutBackPri2);
            sb.append(" | ");
            sb.append("stripped = ");
            sb.append(numBINsStripped1Pri2);
            sb.append("/");
            sb.append(numBINsStripped2Pri2);
            sb.append(" | ");
            sb.append("mutated = ");
            sb.append(numBINsMutatedPri2);
            sb.append(" | ");
            sb.append("evicted = ");
            sb.append(numBINsEvictedPri2);
            sb.append(" - ");
            sb.append(numUINsEvictedPri2);
            sb.append("\n");

            return sb.toString();
        }
    }

    /*
     * The purpose of LRUDebugStats is to capture stats on the current state
     * of an LRUSet. This is done via a call to LRUEvictor.getPri1LRUStats(),
     * or LRUEvictor.getPri2LRUStats(). For now at least, these methods are
     * meant to be used for debugging and unit testing only.
     */
    static class LRUDebugStats {
        int size;
        int dirtySize;

        int numBINs;
        int numDirtyBINs;

        int numStrippedBINs;
        int numDirtyStrippedBINs;

        void reset() {
            size = 0;
            dirtySize = 0;
            numBINs = 0;
            numDirtyBINs = 0;
            numStrippedBINs = 0;
            numDirtyStrippedBINs = 0;
        }

        public String toString() {
            StringBuilder sb = new StringBuilder();

            sb.append("Clean/Dirty INs = ");
            sb.append(size - dirtySize);
            sb.append("/");
            sb.append(dirtySize);

            sb.append(" BINs = ");
            sb.append(numBINs - numDirtyBINs);
            sb.append("/");
            sb.append(numDirtyBINs);

            sb.append(" Stripped BINs = ");
            sb.append(numStrippedBINs - numDirtyStrippedBINs);
            sb.append("/");
            sb.append(numDirtyStrippedBINs);

            return sb.toString();
        }
    }

    /*
     * LRUList implementation
     */
    static class LRUList {

        private static final boolean doExpensiveCheck = false;

        private final int id;

        private int size = 0;

        private IN front = null;
        private IN back = null;

        LRUList(int id) {
            this.id = id;
        }

        synchronized void addBack(IN node) {

            /* Make sure node is not in any LRUlist already */
            if (node.getNextLRUNode() != null ||
                node.getPrevLRUNode() != null) {
                
                throw EnvironmentFailureException.unexpectedState(
                   node.getEnv(),
                   Thread.currentThread().getId() + "-" +
                   Thread.currentThread().getName() +
                   "-" + node.getEnv().getName() + 
                   "Attempting to add node " + node.getNodeId() +
                   " in the LRU, but node is already in the LRU.");
            }
            assert(!node.isDIN() && !node.isDBIN());

            node.setNextLRUNode(node);

            if (back != null) {
                node.setPrevLRUNode(back);
                back.setNextLRUNode(node);
            } else {
                assert(front == null);
                node.setPrevLRUNode(node);
            }

            back = node;

            if (front == null) {
                front = back;
            }

            ++size;
        }
        
        synchronized void addFront(IN node) {

            /* Make sure node is not in any LRUlist already */
            if (node.getNextLRUNode() != null ||
                node.getPrevLRUNode() != null) {
                
                throw EnvironmentFailureException.unexpectedState(
                   node.getEnv(),
                   Thread.currentThread().getId() + "-" +
                   Thread.currentThread().getName() +
                   "-" + node.getEnv().getName() + 
                   "Attempting to add node " + node.getNodeId() +
                   " in the LRU, but node is already in the LRU.");
            }
            assert(!node.isDIN() && !node.isDBIN());

            node.setPrevLRUNode(node);

            if (front != null) {
                node.setNextLRUNode(front);
                front.setPrevLRUNode(node);
            } else {
                assert(back == null);
                node.setNextLRUNode(node);
            }

            front = node;

            if (back == null) {
                back = front;
            }

            ++size;
        }

        synchronized void moveBack(IN node) {

            /* If the node is not in the list, don't do anything */
            if (node.getNextLRUNode() == null) {
                assert(node.getPrevLRUNode() == null);
                return;
            }

            if (doExpensiveCheck && !contains2(node)) {
                System.out.println("LRUList.moveBack(): list " + id +
                                   "does not contain node " +
                                   node.getNodeId() +
                                   " Thread: " +
                                   Thread.currentThread().getId() + "-" +
                                   Thread.currentThread().getName() +
                                   " isBIN: " + node.isBIN() +
                                   " inPri2LRU: " + node.isInPri2LRU());
                assert(false);
            }

            if (node.getNextLRUNode() == node) {
                /* The node is aready at the back */
                assert(back == node);
                assert(node.getPrevLRUNode().getNextLRUNode() == node);

            } else {
                assert(front != back);
                assert(size > 1);

                if (node.getPrevLRUNode() == node) {
                    /* the node is at the front  */
                    assert(front == node);
                    assert(node.getNextLRUNode().getPrevLRUNode() == node);

                    front = node.getNextLRUNode();
                    front.setPrevLRUNode(front);
                } else {
                    /* the node is in the "middle" */
                    assert(front != node && back != node);
                    assert(node.getPrevLRUNode().getNextLRUNode() == node);
                    assert(node.getNextLRUNode().getPrevLRUNode() == node);

                    node.getPrevLRUNode().setNextLRUNode(node.getNextLRUNode());
                    node.getNextLRUNode().setPrevLRUNode(node.getPrevLRUNode());
                }
                
                node.setNextLRUNode(node);
                node.setPrevLRUNode(back);
                
                back.setNextLRUNode(node);
                back = node;
            }
        }
        
        synchronized void moveFront(IN node) {
            
            /* If the node is not in the list, don't do anything */
            if (node.getNextLRUNode() == null) {
                assert(node.getPrevLRUNode() == null);
                return;
            }

            if (doExpensiveCheck && !contains2(node)) {
                System.out.println("LRUList.moveFront(): list " + id +
                                   "does not contain node " +
                                   node.getNodeId() +
                                   " Thread: " +
                                   Thread.currentThread().getId() + "-" +
                                   Thread.currentThread().getName() +
                                   " isBIN: " + node.isBIN() +
                                   " inPri2LRU: " + node.isInPri2LRU());
                assert(false);
            }

            if (node.getPrevLRUNode() == node) {
                /* the node is aready at the front */
                assert(front == node);
                assert(node.getNextLRUNode().getPrevLRUNode() == node);

            } else {
                assert(front != back);
                assert(size > 1);

                if (node.getNextLRUNode() == node) {
                    /* the node is at the back */
                    assert(back == node);
                    assert(node.getPrevLRUNode().getNextLRUNode() == node);

                    back = node.getPrevLRUNode();
                    back.setNextLRUNode(back);
                } else {
                    /* the node is in the "middle" */
                    assert(front != node && back != node);
                    assert(node.getPrevLRUNode().getNextLRUNode() == node);
                    assert(node.getNextLRUNode().getPrevLRUNode() == node);

                    node.getPrevLRUNode().setNextLRUNode(node.getNextLRUNode());
                    node.getNextLRUNode().setPrevLRUNode(node.getPrevLRUNode());
                }
                
                node.setPrevLRUNode(node);
                node.setNextLRUNode(front);
                
                front.setPrevLRUNode(node);
                front = node;
            }
        }
        
        synchronized IN removeFront() {
            if (front == null) {
                assert(back == null);
                return null;
            }

            IN res = front;

            if (front == back) {
                assert(front.getNextLRUNode() == front);
                assert(front.getPrevLRUNode() == front);
                assert(size == 1);

                front = null;
                back = null;

            } else {
                assert(size > 1);

                front = front.getNextLRUNode();
                front.setPrevLRUNode(front);
            }

            res.setNextLRUNode(null);
            res.setPrevLRUNode(null);
            --size;

            return res;
        }
        
        synchronized boolean remove(IN node) {

            /* If the node is not in the list, don't do anything */
            if (node.getNextLRUNode() == null) {
                assert(node.getPrevLRUNode() == null);
                return false;
            }

            assert(node.getPrevLRUNode() != null);

            if (doExpensiveCheck && !contains2(node)) {
                System.out.println("LRUList.remove(): list " + id +
                                   "does not contain node " +
                                   node.getNodeId() +
                                   " Thread: " +
                                   Thread.currentThread().getId() + "-" +
                                   Thread.currentThread().getName() +
                                   " isBIN: " + node.isBIN() +
                                   " inPri2LRU: " + node.isInPri2LRU());
                assert(false);
            }

            if (front == back) {
                assert(size == 1);
                assert(front == node);
                assert(front.getNextLRUNode() == front);
                assert(front.getPrevLRUNode() == front);

                front = null;
                back = null;

            } else if (node.getPrevLRUNode() == node) {
                /* node is at the front */
                assert(front == node);
                assert(node.getNextLRUNode().getPrevLRUNode() == node);

                front = node.getNextLRUNode();
                front.setPrevLRUNode(front);

            } else if (node.getNextLRUNode() == node) {
                /* the node is at the back */
                assert(back == node);
                assert(node.getPrevLRUNode().getNextLRUNode() == node);
                
                back = node.getPrevLRUNode();
                back.setNextLRUNode(back);
            } else {
                /* the node is in the "middle" */
                assert(size > 2);
                assert(front != back);
                assert(front != node && back != node);
                assert(node.getPrevLRUNode().getNextLRUNode() == node);
                assert(node.getNextLRUNode().getPrevLRUNode() == node);
                
                node.getPrevLRUNode().setNextLRUNode(node.getNextLRUNode());
                node.getNextLRUNode().setPrevLRUNode(node.getPrevLRUNode());
            }

            node.setNextLRUNode(null);
            node.setPrevLRUNode(null);
            --size;

            return true;
        }

        synchronized void removeINsForEnv(EnvironmentImpl env) {

            if (front == null) {
                assert(back == null);
                return;
            }

            IN node = front;

            while (true) {

                IN nextNode = node.getNextLRUNode();
                IN prevNode = node.getPrevLRUNode();

                if (node.getDatabase().getEnv() == env) {

                    node.setNextLRUNode(null);
                    node.setPrevLRUNode(null);

                    if (front == back) {
                        assert(size == 1);
                        assert(front == node);
                        assert(nextNode == front);
                        assert(prevNode == front);
                        
                        front = null;
                        back = null;
                        --size;
                        break;

                    } else if (prevNode == node) {
                        /* node is at the front */
                        assert(size > 1);
                        assert(front == node);
                        assert(nextNode.getPrevLRUNode() == node);

                        front = nextNode;
                        front.setPrevLRUNode(front);
                        node = front;
                        --size;

                    } else if (nextNode == node) {
                        /* the node is at the back */
                        assert(size > 1);
                        assert(back == node);
                        assert(prevNode.getNextLRUNode() == node);
                        
                        back = prevNode;
                        back.setNextLRUNode(back);
                        --size;
                        break;
                    } else {
                        /* the node is in the "middle" */
                        assert(size > 2);
                        assert(front != back);
                        assert(front != node && back != node);
                        assert(prevNode.getNextLRUNode() == node);
                        assert(nextNode.getPrevLRUNode() == node);
                
                        prevNode.setNextLRUNode(nextNode);
                        nextNode.setPrevLRUNode(prevNode);
                        node = nextNode;
                        --size;
                    }
                } else if (nextNode == node) {
                    break;
                } else {
                    node = nextNode;
                }
            }
        }

        synchronized boolean contains(IN node) {
            return (node.getNextLRUNode() != null);
        }

        private boolean contains2(IN node) {

            if (front == null) {
                assert(back == null);
                return false;
            }

            IN curr = front;

            while (true) {
                if (curr == node) {
                    return true;
                }

                if (curr.getNextLRUNode() == curr) {
                    break;
                }

                curr = curr.getNextLRUNode();
            }

            return false;
        }

        synchronized List<IN> copyList() {

            if (front == null) {
                assert(back == null);
                return Collections.emptyList();
            }

            List<IN> list = new ArrayList<>();

            IN curr = front;

            while (true) {
                list.add(curr);

                if (curr.getNextLRUNode() == curr) {
                    break;
                }

                curr = curr.getNextLRUNode();
            }

            return list;
        }

        int getSize() {
            return size;
        }

        synchronized void getStats(EnvironmentImpl env, LRUDebugStats stats) {

            if (front == null) {
                assert(back == null);
                return;
            }

            IN curr = front;

            while (true) {
                if (env == null || curr.getEnv() == env) {
                    stats.size++;

                    if (curr.getDirty()) {
                        stats.dirtySize++;
                    }

                    if (curr.isBIN()) {
                        stats.numBINs++;

                        if (curr.getDirty()) {
                            stats.numDirtyBINs++;
                        }

                        if (!curr.hasCachedChildren()) {
                            stats.numStrippedBINs++;

                            if (curr.getDirty()) {
                                stats.numDirtyStrippedBINs++;
                            }
                        }
                    }
                }

                if (curr.getNextLRUNode() == curr) {
                    break;
                }

                curr = curr.getNextLRUNode();
            }
        }
    }

    /**
     * EnvInfo stores info related to the environments that share this evictor.
     */
    private static class EnvInfo {
        EnvironmentImpl env;
        INList ins;
    }

    /* Prevent endless eviction loops under extreme resource constraints. */
    private static final int MAX_BATCHES_PER_RUN = 100;

    private static final boolean traceUINs = false;
    private static final boolean traceBINs = false;
    private static final Level traceLevel = Level.INFO;

    /* LRU-TODO: remove */
    private static final boolean collectEvictionDebugStats = false;

    /**
     * Number of LRULists per LRUSet. This is a configuration parameter.
     * 
     * In general, using only one LRUList may create a synchronization
     * bottleneck, because all LRUList methods are synchronized and are
     * invoked with high frequency from multiple thread. To alleviate
     * this bottleneck, we need the option to break a single LRUList
     * into multiple ones comprising an "LRUSet" (even though this
     * reduces the quality of the LRU approximation).
     */
    private final int numLRULists;

    /*
     * This is true when an off-heap cache is in use. If true, then the
     * priority-2 LRUSet is always used for level 2 INs, and useDirtyLRUSet
     * and mutateBins are both set to false.
     */
    private final boolean useOffHeapCache;

    /**
     * Whether to use the priority-2 LRUSet for dirty nodes or not.
     *
     * When useOffHeapCache is true, useDirtyLRUSet is always false. When
     * useOffHeapCache is false, useDirtyLRUSet is set via a configuration
     * parameter.
     */
    private final boolean useDirtyLRUSet;

    /*
     * Whether to allow deltas when logging a dirty BIN that is being evicted.
     * This is a configuration parameter.
     */
    private final boolean allowBinDeltas;

    /*
     * Whether to mutate BINs to BIN deltas rather than evicting the full node.
     *
     * When useOffHeapCache is true, mutateBins is always false. When
     * useOffHeapCache is false, mutateBins is set via a configuration
     * parameter.
     */
    private final boolean mutateBins;

    /*
     * Access count after which we clear the DatabaseImpl cache.
     * This is a configuration parameter.
     */
    private int dbCacheClearCount;

    /*
     * This is a configuration parameter. If true, eviction is done by a pool
     * of evictor threads, as well as being done inline by application threads.
     * Note: runEvictorThreads is needed as a distinct flag, rather than
     * setting maxThreads to 0, because the ThreadPoolExecutor does not permit
     * maxThreads to be 0.
     */
    private boolean runEvictorThreads;

    /* This is a configuration parameter. */
    private int terminateMillis;

    /* The thread pool used to manage the background evictor threads. */
    private final ThreadPoolExecutor evictionPool;

    /* Flag to help shutdown launched eviction tasks. */
    private final AtomicBoolean shutdownRequested = new AtomicBoolean(false);

    private int maxPoolThreads;
    private final AtomicInteger activePoolThreads = new AtomicInteger(0);

    /*
     * Whether this evictor (and the memory cache) is shared by multiple
     * environments
     */
    private final boolean isShared;

    /*
     * In case of multiple environments sharing a cache (and this Evictor),
     * firstEnvImpl references the 1st EnvironmentImpl to be created with
     * the shared cache.
     */
    private final EnvironmentImpl firstEnvImpl;

    private final List<EnvInfo> envInfos;

    /**
     * This is used only when this evictor is shared by multiple envs. It
     * "points" to the next env to perform "special eviction" in. 
     */
    private int specialEvictionIndex = 0;

    /*
     *
     */
    private final Arbiter arbiter;

    /**
     * With an off-heap cache configured:
     * pri1LRUSet contains nodes of any type and level. A freshly cached node
     * goes into this LRUSet. A level-2 node will go to the pri2LRUSet if it is
     * selected for eviction from the pri1LRUSet and it contains off-heap BINs.
     * A node will move from the pri2LRUSet to the pri1LRUSet when its last
     * off-heap BIN is evicted from the off-heap cache.
     *
     * Without an off-heap cache configured:
     * pri1LRUSet contains both clean and dirty nodes. A freshly cached node
     * goes into this LRUSet. A dirty node will go to the pri2LRUSet if it is
     * selected for eviction from the pri1LRUSet. A node will move from the
     * pri2LRUSet to the pri1LRUSet when it gets logged (i.e., cleaned) by
     * the checkpointer.
     */
    private final LRUList[] pri1LRUSet;
    private final LRUList[] pri2LRUSet;
    
    /**
     * nextPri1LRUList is used to implement the traversal of the lists in
     * the pri1LRUSet by one or more evicting threads. Such a thread will
     * select for eviction the front node from the (nextPri1LRUList %
     * numLRULists)-th list, and then increment nextPri1LRUList.
     * nextPri2LRUList plays the same role for the priority-2 LRUSet.
     */
    private int nextPri1LRUList = 0;
    private int nextPri2LRUList = 0;

    /*
     * The evictor is disabled during the 1st phase of recovery. The
     * RecoveryManager enables the evictor after it finishes its 1st
     * phase.
     */
    private boolean isEnabled = false;

    /* Eviction calls cannot be recursive. */
    private ReentrancyGuard reentrancyGuard;

    private final Logger logger;

    /*
     * Stats
     */
    private final StatGroup stats;

    /*
     * Number of eviction tasks that were submitted to the background evictor
     * pool, but were refused because all eviction threads were busy.
     */
    private final AtomicLongStat nThreadUnavailable;

    /* Number of evictBatch() invocations. */
    private final LongStat nEvictionRuns;

    /*
     * Number of nodes selected as eviction targets. An eviction target may
     * actually be evicted, or skipped, or put back to the LRU, potentially
     * after partial eviction or BIN-delta mutation is done on it.
     */
    private final LongStat nNodesTargeted;

    /* Number of nodes evicted. */
    private final LongStat nNodesEvicted;

    /* Number of closed database root nodes evicted. */
    private final LongStat nRootNodesEvicted;

    /* Number of dirty nodes logged and evicted. */
    private final LongStat nDirtyNodesEvicted;

    /* Number of LNs evicted. */
    private final LongStat nLNsEvicted;

    /* Number of BINs stripped. */
    private final LongStat nNodesStripped;

    /* Number of BINs mutated to deltas. */
    private final LongStat nNodesMutated;

    /* Number of target nodes put back to the LRU w/o any other action taken */
    private final LongStat nNodesPutBack;

    /* Number of target nodes skipped. */
    private final LongStat nNodesSkipped;

    /* Number of target nodes moved to the priority-2 LRU */
    private final LongStat nNodesMovedToPri2LRU;

    /* Number of bytes evicted per eviction source. */
    private final AtomicLongStat[] numBytesEvicted;

    /*
     * Tree related cache hit/miss stats. A subset of the cache misses recorded
     * by the log manager, in that these only record tree node hits and misses.
     * Recorded by IN.fetchIN and IN.fetchLN, but grouped with evictor stats.
     * Use AtomicLongStat for multithreading safety.
     */
    private final AtomicLongStat nLNFetch;

    private final AtomicLongStat nLNFetchMiss;

    /* 
     * Number of times IN.fetchIN() or IN.fetchINWithNoLatch() was called
     * to fetch a UIN.
     */
    private final AtomicLongStat nUpperINFetch;

    /* 
     * Number of times IN.fetchIN() or IN.fetchINWithNoLatch() was called
     * to fetch a UIN and that UIN was not already cached.
     */
    private final AtomicLongStat nUpperINFetchMiss;

    /* 
     * Number of times IN.fetchIN() or IN.fetchINWithNoLatch() was called
     * to fetch a BIN.
     */
    private final AtomicLongStat nBINFetch;

    /* 
     * Number of times IN.fetchIN() or IN.fetchINWithNoLatch() was called
     * to fetch a BIN and that BIN was not already cached.
     */
    private final AtomicLongStat nBINFetchMiss;

    /* 
     * Number of times IN.fetchIN() or IN.fetchINWithNoLatch() was called
     * to fetch a BIN, that BIN was not already cached, and a BIN-delta was
     * fetched from disk.
     */
    private final AtomicLongStat nBINDeltaFetchMiss;

    private final FloatStat binFetchMissRatio;

    /*
     * Number of calls to BIN.mutateToFullBIN()
     */
    private final AtomicLongStat nFullBINMiss;

    /*
     * Number of blind operations on BIN deltas
     */
    private final AtomicLongStat nBinDeltaBlindOps;

    /* Stats for IN compact array representations currently in cache. */
    private final AtomicLong nINSparseTarget;
    private final AtomicLong nINNoTarget;
    private final AtomicLong nINCompactKey;

    /* Number of envs sharing the cache. */
    private final IntStat sharedCacheEnvs;

    /* Debugging and unit test support. */

    /*
     * Number of consecutive "no-eviction" events (i.e. when evictBatch()
     * returns 0). It is incremented at each "no-eviction" event and reset
     * to 0 when eviction does occur. It is used to determine whether to
     * log a WARNING for a "no-eviction" event: only 1 warning is logged
     * per sequence of consecutive "no-eviction" events (to avoid flooding
     * the logger files).
     */
    private int numNoEvictionEvents = 0;

    private TestHook<Object> preEvictINHook;
    private TestHook<IN> evictProfile;

    public Evictor(EnvironmentImpl envImpl)
        throws DatabaseException {

        isShared = envImpl.getSharedCache();

        firstEnvImpl = envImpl;

        /* Do the stats definitions. */
        stats = new StatGroup(GROUP_NAME, GROUP_DESC);

        nEvictionRuns = new LongStat(stats, EVICTOR_EVICTION_RUNS);

        nNodesTargeted = new LongStat(stats, EVICTOR_NODES_TARGETED);
        nNodesEvicted = new LongStat(stats, EVICTOR_NODES_EVICTED);
        nRootNodesEvicted = new LongStat(stats, EVICTOR_ROOT_NODES_EVICTED);
        nDirtyNodesEvicted = new LongStat(stats, EVICTOR_DIRTY_NODES_EVICTED);
        nLNsEvicted = new LongStat(stats, EVICTOR_LNS_EVICTED);
        nNodesStripped = new LongStat(stats, EVICTOR_NODES_STRIPPED);
        nNodesMutated = new LongStat(stats, EVICTOR_NODES_MUTATED);
        nNodesPutBack = new LongStat(stats, EVICTOR_NODES_PUT_BACK);
        nNodesSkipped = new LongStat(stats, EVICTOR_NODES_SKIPPED);
        nNodesMovedToPri2LRU = new LongStat(
            stats, EVICTOR_NODES_MOVED_TO_PRI2_LRU);

        nLNFetch = new AtomicLongStat(stats, LN_FETCH);
        nBINFetch = new AtomicLongStat(stats, BIN_FETCH);
        nUpperINFetch = new AtomicLongStat(stats, UPPER_IN_FETCH);
        nLNFetchMiss = new AtomicLongStat(stats, LN_FETCH_MISS);
        nBINFetchMiss = new AtomicLongStat(stats, BIN_FETCH_MISS);
        nBINDeltaFetchMiss = new AtomicLongStat(stats, BIN_DELTA_FETCH_MISS);
        nUpperINFetchMiss = new AtomicLongStat(stats, UPPER_IN_FETCH_MISS);
        nFullBINMiss = new AtomicLongStat(stats, FULL_BIN_MISS);
        nBinDeltaBlindOps = new AtomicLongStat(stats, BIN_DELTA_BLIND_OPS);
        binFetchMissRatio = new FloatStat(stats, BIN_FETCH_MISS_RATIO);

        nThreadUnavailable = new AtomicLongStat(stats, THREAD_UNAVAILABLE);

        nINSparseTarget = new AtomicLong(0);
        nINNoTarget = new AtomicLong(0);
        nINCompactKey = new AtomicLong(0);

        sharedCacheEnvs = new IntStat(stats, EVICTOR_SHARED_CACHE_ENVS);

        EnumSet<EvictionSource> allSources =
            EnumSet.allOf(EvictionSource.class);

        int numSources = allSources.size();

        numBytesEvicted = new AtomicLongStat[numSources];

        for (EvictionSource source : allSources) {

            int index = source.ordinal();

            numBytesEvicted[index] = new AtomicLongStat(
                stats, source.getNumBytesEvictedStatDef());
        }

        arbiter = new Arbiter(firstEnvImpl);

        logger = LoggerUtils.getLogger(getClass());
        reentrancyGuard = new ReentrancyGuard(firstEnvImpl, logger);

        DbConfigManager configManager = firstEnvImpl.getConfigManager();

        int corePoolSize = configManager.getInt(
            EnvironmentParams.EVICTOR_CORE_THREADS);
        maxPoolThreads = configManager.getInt(
            EnvironmentParams.EVICTOR_MAX_THREADS);
        long keepAliveTime = configManager.getDuration(
            EnvironmentParams.EVICTOR_KEEP_ALIVE);
        terminateMillis = configManager.getDuration(
            EnvironmentParams.EVICTOR_TERMINATE_TIMEOUT);
        dbCacheClearCount = configManager.getInt(
            EnvironmentParams.ENV_DB_CACHE_CLEAR_COUNT);
        numLRULists = configManager.getInt(
            EnvironmentParams.EVICTOR_N_LRU_LISTS);

        pri1LRUSet = new LRUList[numLRULists];
        pri2LRUSet = new LRUList[numLRULists];
        
        for (int i = 0; i < numLRULists; ++i) {
            pri1LRUSet[i] = new LRUList(i);
            pri2LRUSet[i] = new LRUList(numLRULists + i);
        }

        if (isShared) {
            envInfos = new ArrayList<EnvInfo>();
        } else {
            envInfos = null;
        }

        if (configManager.getLong(EnvironmentParams.MAX_OFF_HEAP_MEMORY) > 0) {
            mutateBins = false;
            useDirtyLRUSet = false;
            useOffHeapCache = true;

        } else {
            mutateBins = configManager.getBoolean(
                EnvironmentParams.EVICTOR_MUTATE_BINS);

            useDirtyLRUSet = configManager.getBoolean(
                EnvironmentParams.EVICTOR_USE_DIRTY_LRU);

            useOffHeapCache = false;
        }

        RejectedExecutionHandler rejectHandler = new RejectEvictHandler(
            nThreadUnavailable);

        evictionPool = new ThreadPoolExecutor(
            corePoolSize, maxPoolThreads, keepAliveTime,
            TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<Runnable>(1),
            new StoppableThreadFactory(
                isShared ? null : envImpl, "JEEvictor", logger),
            rejectHandler);

        allowBinDeltas = configManager.getBoolean(
            EnvironmentParams.EVICTOR_ALLOW_BIN_DELTAS);

        runEvictorThreads = configManager.getBoolean(
            EnvironmentParams.ENV_RUN_EVICTOR);

        /*
         * Request notification of mutable property changes. Do this after all
         * fields in the evictor have been initialized, in case this is called
         * quite soon.
         */
        firstEnvImpl.addConfigObserver(this);
    }

    /**
     * Respond to config updates.
     */
    @Override
    public void envConfigUpdate(
        DbConfigManager configManager,
        EnvironmentMutableConfig ignore)
        throws DatabaseException {

        int corePoolSize = configManager.getInt(
            EnvironmentParams.EVICTOR_CORE_THREADS);
        maxPoolThreads = configManager.getInt(
            EnvironmentParams.EVICTOR_MAX_THREADS);
        long keepAliveTime = configManager.getDuration(
            EnvironmentParams.EVICTOR_KEEP_ALIVE);
        terminateMillis = configManager.getDuration(
            EnvironmentParams.EVICTOR_TERMINATE_TIMEOUT);
        dbCacheClearCount = configManager.getInt(
            EnvironmentParams.ENV_DB_CACHE_CLEAR_COUNT);

        evictionPool.setCorePoolSize(corePoolSize);
        evictionPool.setMaximumPoolSize(maxPoolThreads);
        evictionPool.setKeepAliveTime(keepAliveTime, TimeUnit.MILLISECONDS);

        runEvictorThreads = configManager.getBoolean(
            EnvironmentParams.ENV_RUN_EVICTOR);
    }

    public void setEnabled(boolean v) {
        isEnabled = v;
    }

    public ThreadPoolExecutor getThreadPool() {
        return evictionPool;
    }

    /**
     * Request and wait for a shutdown of all running eviction tasks.
     */
    public void shutdown() {

        /*
         * Set the shutdown flag so that outstanding eviction tasks end
         * early. The call to evictionPool.shutdown is a ThreadPoolExecutor
         * call, and is an orderly shutdown that waits for and in flight tasks
         * to end.
         */
        shutdownRequested.set(true);
        evictionPool.shutdown();

        /*
         * AwaitTermination will wait for the timeout period, or will be
         * interrupted, but we don't really care which it is. The evictor
         * shouldn't be interrupted, but if it is, something urgent is
         * happening.
         */
        boolean shutdownFinished = false;
        try {
            shutdownFinished =
                evictionPool.awaitTermination(terminateMillis,
                                              TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            /* We've been interrupted, just give up and end. */
        } finally {
            if (!shutdownFinished) {
                evictionPool.shutdownNow();
            }
        }
    }

    public void requestShutdown() {
        shutdownRequested.set(true);
        evictionPool.shutdown();
    }

    public synchronized void addEnvironment(EnvironmentImpl env) {

        if (isShared) {
            int numEnvs = envInfos.size();
            for (int i = 0; i < numEnvs; i += 1) {
                EnvInfo info = envInfos.get(i);
                if (info.env == env) {
                    return;
                }
            }

            EnvInfo info = new EnvInfo();
            info.env = env;
            info.ins = env.getInMemoryINs();
            envInfos.add(info);
        } else {
            throw EnvironmentFailureException.unexpectedState();
        }
    }
    
    public synchronized void removeSharedCacheEnv(EnvironmentImpl env) {
        if (!isShared) {
            throw EnvironmentFailureException.unexpectedState();
        }

        int numEnvs = envInfos.size();
        for (int i = 0; i < numEnvs; i += 1) {
            EnvInfo info = envInfos.get(i);

            if (info.env == env) {

                try {
                    for (int j = 0; j < numLRULists; ++j) {
                        pri1LRUSet[j].removeINsForEnv(env);
                        pri2LRUSet[j].removeINsForEnv(env);
                    }
                } catch (AssertionError e) {
                    System.out.println("YYYYYYYYYY " + e);
                    e.printStackTrace(System.out);
                    throw e;
                }

                envInfos.remove(i);
                return;
            }
        }
    }

    public synchronized boolean checkEnv(EnvironmentImpl env) {
        if (isShared) {
            int numEnvs = envInfos.size();
            for (int i = 0; i < numEnvs; i += 1) {
                EnvInfo info = envInfos.get(i);
                if (env == info.env) {
                    return true;
                }
            }

            return false;

        } else {
            throw EnvironmentFailureException.unexpectedState();
        }
    }

    /**
     * Add the node to the back of the priority-1 LRUSet. The node is either
     * EX-latched already or is inaccessible from other threads.
     */
    public void addBack(IN node) {

        if (isEnabled && node.getEnv().getInMemoryINs().isEnabled()) {

            assert(node.getInListResident());

            node.setInPri2LRU(false);
            pri1LRUSet[(int)(node.getNodeId() % numLRULists)].addBack(node);
        }
    }

    /**
     * Add the node to the front of the priority-1 LRUSet. The node is either
     * EX-latched already or is inaccessible from other threads.
     */
    public void addFront(IN node) {

        if (isEnabled && node.getEnv().getInMemoryINs().isEnabled()) {

            assert(node.getInListResident());

            node.setInPri2LRU(false);
            pri1LRUSet[(int)(node.getNodeId() % numLRULists)].addFront(node);
        }
    }

    /*
     * Add the node to the back of the priority-2 LRUSet.
     */
    private void pri2AddBack(IN node) {

        assert(node.isLatchExclusiveOwner());
        assert(node.getInListResident());

        node.setInPri2LRU(true);
        pri2LRUSet[(int)(node.getNodeId() % numLRULists)].addBack(node);
    }
    
    /*
     * Add the node to the front of the priority-2 LRUSet.
     */
    private void pri2AddFront(IN node) {

        assert(node.isLatchExclusiveOwner());
        assert(node.getInListResident());

        node.setInPri2LRU(true);
        pri2LRUSet[(int)(node.getNodeId() % numLRULists)].addFront(node);
    }

    /**
     * Move the node to the back of its containing LRUList, if any.
     */
    public void moveBack(IN node) {

        assert(node.isLatchOwner());

        if (node.isInPri2LRU()) {
            pri2LRUSet[(int)(node.getNodeId() % numLRULists)].moveBack(node);
        } else {
            pri1LRUSet[(int)(node.getNodeId() % numLRULists)].moveBack(node);
        }
    }
    
    /**
     * Move the node to the front of its containing LRUList, if any.
     */
    public void moveFront(IN node) {

        assert(node.isLatchOwner());

        if (node.isInPri2LRU()) {
            pri2LRUSet[(int)(node.getNodeId() % numLRULists)].moveFront(node);
        } else {
            pri1LRUSet[(int)(node.getNodeId() % numLRULists)].moveFront(node);
        }
    }

    /**
     * Remove a node from its current LRUList, if any.
     */
    public void remove(IN node) {

        assert(node.isLatchOwner());

        int listId = (int)(node.getNodeId() % numLRULists);

        if (node.isInPri2LRU()) {
            pri2LRUSet[listId].remove(node);
        } else {
            pri1LRUSet[listId].remove(node);
        }
    }

    /**
     * Move the node from the priority-2 LRUSet to the priority-1 LRUSet, if
     * the node is indeed in the priority-2 LRUSet.
     */
    public void moveToPri1LRU(IN node) {

        assert(node.isLatchExclusiveOwner());

        if (!node.isInPri2LRU()) {
            return;
        }

        int listId = (int)(node.getNodeId() % numLRULists);

        if (pri2LRUSet[listId].remove(node)) {
            assert(node.getInListResident());
            node.setInPri2LRU(false);
            pri1LRUSet[listId].addBack(node);
        }
    }

    public boolean contains(IN node) {

        assert(node.isLatchOwner());

        int listId = (int)(node.getNodeId() % numLRULists);

        if (node.isInPri2LRU()) {
            return pri2LRUSet[listId].contains(node);
        }
        return pri1LRUSet[listId].contains(node);
    }

    public boolean getUseDirtyLRUSet() {
        return useDirtyLRUSet;
    }

    long getPri1LRUSize() {
        long size = 0;
        for (int i = 0; i < numLRULists; ++i) {
            size += pri1LRUSet[i].getSize();
        }

        return size;
    }

    long getPri2LRUSize() {
        long size = 0;
        for (int i = 0; i < numLRULists; ++i) {
            size += pri2LRUSet[i].getSize();
        }

        return size;
    }

    void getPri1LRUStats(EnvironmentImpl env, LRUDebugStats stats) {
        stats.reset();
        for (int i = 0; i < numLRULists; ++i) {
            pri1LRUSet[i].getStats(env, stats);
        }
    }

    void getPri2LRUStats(EnvironmentImpl env, LRUDebugStats stats) {
        stats.reset();
        for (int i = 0; i < numLRULists; ++i) {
            pri2LRUSet[i].getStats(env, stats);
        }
    }

    /**
     * This method is called from application threads for every cursor
     * operation.
     */
    public void doCriticalEviction(boolean backgroundIO) {

        if (arbiter.isOverBudget()) {

            /*
             * Any time there's excessive cache usage, let the thread pool know
             * there's work to do.
             */
            alert();

            /*
             * Only do eviction if the memory budget overage fulfills the
             * critical eviction requirements. We want to avoid having
             * application thread do eviction.
             */
            if (arbiter.needCriticalEviction()) {
                doEvict(EvictionSource.CRITICAL, backgroundIO);
            }
        }
    }

    /**
     * This method is called from daemon threads for every operation.
     */
    public void doDaemonEviction(boolean backgroundIO) {

        if (arbiter.isOverBudget()) {

            /*
             * Any time there's excessive cache usage, let the thread pool know
             * there's work to do.
             */
            alert();

            /*
             * Only do eviction if the memory budget overage fulfills the
             * critical eviction requirements. This allows evictor threads to
             * take the burden of eviction whenever possible, rather than
             * slowing other threads and risking a growing cleaner or
             * compressor backlog.
             */
            if (arbiter.needCriticalEviction()) {
                doEvict(EvictionSource.DAEMON, backgroundIO);
            }
        }
    }

    /*
     * Eviction invoked by the API
     */
    public void doManualEvict()
        throws DatabaseException {

        doEvict(EvictionSource.MANUAL, true/*backgroundIO*/);
    }

    /**
     * Evict a specific IN, used by tests.
     */
    public long doTestEvict(IN target, EvictionSource source) {
        return doEvictOneIN(
            target,
            source == EvictionSource.CACHEMODE ? CacheMode.EVICT_BIN : null,
            source);
    }

    /**
     * Evict a specific IN, used by cache modes.
     */
    public long doCacheModeEvict(IN target, CacheMode cacheMode) {
        return doEvictOneIN(target, cacheMode, EvictionSource.CACHEMODE);
    }

    private long doEvictOneIN(IN target,
                              CacheMode cacheMode,
                              EvictionSource source) {
        assert(target.isBIN());
        assert(target.isLatchOwner());

        /*
         * If a dirty BIN is being evicted via a cache mode and an off-heap
         * cache is not used, do not evict the node since it would be
         * logged. When an off-heap cache is used, we can evict dirty nodes
         * without logging them.
         */
        if (source == EvictionSource.CACHEMODE &&
            target.getDirty() &&
            !useOffHeapCache) {

            try {
                long evictedBytes = 0;
                if (cacheMode == CacheMode.EVICT_BIN) {
                    evictedBytes = target.partialEviction();
                    evictedBytes &= ~IN.NON_EVICTABLE_IN;
                    if (evictedBytes > 0) {
                        nNodesStripped.increment();
                        numBytesEvicted[source.ordinal()].add(evictedBytes);
                    }
                }
                return evictedBytes;
            } finally {
                target.releaseLatch();
            }
        }

        if (!reentrancyGuard.enter()) {
            return 0;
        }

        try {
            remove(target);

            target.releaseLatch();

            final long evictedBytes = processTarget(
                null /* rootEvictor */, target, null /* parent */,
                -1 /* entry index within parent */,
                false /* backgroundIO */, source, null /* debug stats */);

            numBytesEvicted[source.ordinal()].add(evictedBytes);

            return evictedBytes;

        } finally {
            reentrancyGuard.leave();
        }
    }

    /**
     * Let the eviction pool know there's work to do.
     */
    public void alert() {

        if (!runEvictorThreads) {
            return;
        }

        /*
         * For a private evictor/cache, we can prevent background eviction
         * during recovery here. For a shared cache, we must do it on a
         * per-target basis, in evictBatch().
         */
        if (!isShared && firstEnvImpl.isInInit()) {
            return;
        }

        /*
         * This check is meant to avoid the lock taken by
         * ArrayBlockingQueue.offer() when this is futile. The lock reduces
         * concurrency because this method is called so frequently.
         */
        if (activePoolThreads.get() >= maxPoolThreads) {
            return;
        }

        evictionPool.execute(new BackgroundEvictTask(this));
    }

    /**
     * This is where the real work is done.
     * Can execute concurrently, called by app threads or by background evictor
     */
    void doEvict(EvictionSource source, boolean backgroundIO)
        throws DatabaseException {
        
        if (!isEnabled) {
            return;
        }

        if (!reentrancyGuard.enter()) {
            return;
        }

        nEvictionRuns.increment();

        try {

            /*
             * Repeat as necessary to keep up with allocations.  Stop if no
             * progress is made, to prevent an infinite loop.
             */
            boolean progress = true;
            int nBatches = 0;
            long bytesEvicted = 0;

            EvictionDebugStats evictionStats = null;
            if (collectEvictionDebugStats) {
                evictionStats = new EvictionDebugStats();
                evictionStats.reset();
                evictionStats.pri1Size = getPri1LRUSize();
                evictionStats.pri2Size = getPri2LRUSize();
            }

            while (progress &&
                   nBatches < MAX_BATCHES_PER_RUN &&
                   !shutdownRequested.get()) {

                /*
                 * Do eviction only if memory consumption is over budget.
                 * If so, try to evict (memoryConsumption + M - maxMemory)
                 * bytes, where M is a config param.
                 */
                long maxEvictBytes = arbiter.getEvictionPledge();

                if (maxEvictBytes == 0) {
                    break;
                }

                bytesEvicted = evictBatch(
                    source, backgroundIO, maxEvictBytes, evictionStats);

                numBytesEvicted[source.ordinal()].add(bytesEvicted);

                if (bytesEvicted == 0) {

                    if (arbiter.stillNeedsEviction() &&
                        numNoEvictionEvents == 0 &&
                        logger.isLoggable(Level.FINE)) {
                        ++numNoEvictionEvents;
                        LoggerUtils.fine(
                            logger, firstEnvImpl,
                            "Eviction pass failed to evict any bytes");
                    } else {
                        ++numNoEvictionEvents;
                    }

                    progress = false;
                } else {
                    numNoEvictionEvents = 0;
                }

                nBatches += 1;
            }

            if (evictionStats != null) {
                System.out.println(evictionStats.toString());
            }

            /* For debugging. */
            if (source == EvictionSource.EVICTORTHREAD) {
                if (logger.isLoggable(Level.FINEST)) {
                    LoggerUtils.finest(logger, firstEnvImpl,
                                       "Thread evicted " + bytesEvicted +
                                       " bytes in " + nBatches + " batches");
                }
            }
        } finally {
            reentrancyGuard.leave();
        }
    }

    /**
     * Not private because it is used in unit test.
     */
    long evictBatch(
        Evictor.EvictionSource source,
        boolean bgIO,
        long maxEvictBytes,
        EvictionDebugStats evictionStats)
        throws DatabaseException {

        long totalEvictedBytes = 0;
        boolean inPri1LRUSet = true;
        int numNodesScannedThisBatch = 0;
        long maxNodesScannedThisBatch = getPri1LRUSize();
        maxNodesScannedThisBatch += numLRULists;

        assert TestHookExecute.doHookSetupIfSet(evictProfile);

        /*
         * Perform special eviction,i.e., evict non-tree memory.
         *
         * TODO: special eviction is done serially. We may want to absolve
         * application threads of that responsibility, to avoid blocking, and
         * only have evictor threads do special eviction.
         */
        synchronized (this) {
            if (isShared) {
                int numEnvs = envInfos.size();
                if (numEnvs > 0) {
                    if (specialEvictionIndex >= numEnvs) {
                        specialEvictionIndex = 0;
                    }
                    EnvInfo info = envInfos.get(specialEvictionIndex);
                    specialEvictionIndex++;

                   totalEvictedBytes = info.env.specialEviction();
                }
            } else {
                totalEvictedBytes = firstEnvImpl.specialEviction();
            }
        }

        /* Use local caching to reduce DbTree.getDb overhead. [#21330] */
        final DbCache dbCache = new DbCache(isShared, dbCacheClearCount);
        final MemoryBudget memBudget = firstEnvImpl.getMemoryBudget();

        try {
            while (totalEvictedBytes < maxEvictBytes &&
                   numNodesScannedThisBatch < maxNodesScannedThisBatch &&
                   arbiter.stillNeedsEviction()) {

                if (!isShared && !memBudget.isTreeUsageAboveMinimum()) {
                    break;
                }

                final IN target = getNextTarget(inPri1LRUSet);

                numNodesScannedThisBatch++;

                if (target != null) {

                    nNodesTargeted.increment();

                    if (evictionStats != null) {
                        evictionStats.incNumSelected();
                    }

                    assert TestHookExecute.doHookIfSet(evictProfile, target);

                    final DatabaseImpl targetDb = target.getDatabase();
                    final EnvironmentImpl dbEnv = targetDb.getEnv();

                    /*
                     * Check to make sure the target's DB was not deleted or
                     * truncated after selecting the target. Furthermore,
                     * prevent the DB from being deleted while we're working
                     * with it (this is done by the dbCache.getDb() call).
                     *
                     * Also check that the refreshedDb is the same instance
                     * as the targetDb. If not, then the MapLN associated with
                     * targetDb was recently evicted (which can happen after
                     * all handles to the DB are closed). In this case,
                     * targetDb and its INs are orphaned and cannot be
                     * processed; they should simply be removed from the
                     * LRU [#21686]
                     */
                    final DatabaseImpl refreshedDb =
                        dbCache.getDb(dbEnv, targetDb.getId());

                    if (refreshedDb != null &&
                        !refreshedDb.isDeleted() &&
                        refreshedDb == targetDb) {

                        long evictedBytes = 0;

                        if (target.isRoot()) {
                            RootEvictor rootEvictor = new RootEvictor();
                            rootEvictor.target = target;
                            rootEvictor.backgroundIO = bgIO;
                            rootEvictor.source = source;
                            rootEvictor.stats = evictionStats;

                            /* try to evict the root */
                            targetDb.getTree().withRootLatchedExclusive(
                                rootEvictor);

                            /*
                             * If the root IN was flushed, write the dirtied
                             *  MapLN.
                             */
                            if (rootEvictor.flushed) {
                                dbEnv.getDbTree().modifyDbRoot(targetDb);
                            }

                            evictedBytes = rootEvictor.evictedBytes;

                        } else {
                            evictedBytes = processTarget(
                                null,  /* rootEvictor */
                                target, null, /* parent */
                                -1, /* parent entry index */
                                bgIO, source, evictionStats);
                        }
                        
                        totalEvictedBytes += evictedBytes;

                    } else {
                        /*
                         * We don't expect to find in the INList an IN whose
                         * database that has finished delete processing,
                         * because it should have been removed from the
                         * INList during post-delete cleanup. 
                         */
                        if (targetDb.isDeleteFinished() &&
                            target.getInListResident()) {
                            final String inInfo =
                                " IN type=" + target.getLogType() + " id=" +
                                target.getNodeId() + " not expected on INList";
                            final String errMsg = (refreshedDb == null) ?
                                inInfo :
                                ("Database " + refreshedDb.getDebugName() +
                                 " id=" + refreshedDb.getId() + " rootLsn=" +
                                 DbLsn.getNoFormatString
                                 (refreshedDb.getTree().getRootLsn()) +
                                 ' ' + inInfo);
                            
                            throw EnvironmentFailureException.
                                unexpectedState(errMsg);
                        }
                    }
                }

                /*
                 * Move to the priority-2 LRUSet, if we are done processing the
                 * priority-1 LRUSet.
                 */
                if (numNodesScannedThisBatch >= maxNodesScannedThisBatch &&
                    totalEvictedBytes < maxEvictBytes &&
                    inPri1LRUSet) {

                    numNodesScannedThisBatch = 0;
                    maxNodesScannedThisBatch = getPri2LRUSize();
                    maxNodesScannedThisBatch += numLRULists;
                    inPri1LRUSet = false;

                    if (evictionStats != null) {
                        evictionStats.inPri1LRU = false;
                    }
                }
            }
        } finally {
            dbCache.releaseDbs(firstEnvImpl);
        }

        return totalEvictedBytes;
    }

    /**
     * Returns a copy of the LRU list, for tightly controlled testing.
     * Requires that there is exactly one LRU list configured.
     */
    public List<IN> getPri1LRUList() {
        assert pri1LRUSet.length == 1;
        return pri1LRUSet[0].copyList();
    }

    private IN getNextTarget(boolean inPri1LRUSet) {

        if (inPri1LRUSet) {
            int listId = Math.abs(nextPri1LRUList++) % numLRULists;
            IN target = pri1LRUSet[listId].removeFront();

            if (target != null &&
                ((traceUINs && target.isUpperIN()) ||
                 (traceBINs && target.isBIN()))) {
                LoggerUtils.envLogMsg(
                    traceLevel, target.getEnv(),
                    Thread.currentThread().getId() + "-" +
                    Thread.currentThread().getName() +
                    "-" + target.getEnv().getName() + 
                    " XXXX priority-1 Eviction target: " +
                    target.getNodeId());
            }

            return target;
        }

        int listId = Math.abs(nextPri2LRUList++) % numLRULists;
        IN target = pri2LRUSet[listId].removeFront();

        if (target != null && 
            ((traceUINs && target.isUpperIN()) ||
             (traceBINs && target.isBIN()))) {
            LoggerUtils.envLogMsg(
                traceLevel, target.getEnv(),
                Thread.currentThread().getId() + "-" +
                Thread.currentThread().getName() +
                "-" + target.getEnv().getName() + 
                " XXXX Pri2 Eviction target: " + target.getNodeId());
        }

        return target;
    }

    class RootEvictor implements WithRootLatched {

        IN target;
        boolean backgroundIO;
        EvictionSource source;
        EvictionDebugStats stats = null;

        ChildReference rootRef;
        boolean flushed = false;
        long evictedBytes = 0;

        public IN doWork(ChildReference root)
            throws DatabaseException {

            /*
             * Do not call fetchTarget since this root or DB should be
             * resident already if it is to be the target of eviction. If
             * it is not present, it has been evicted by another thread and
             * should not be fetched for two reasons: 1) this would be
             * counterproductive, 2) to guard against bringing in a root
             * for an evicted DB.
             */
            IN rootIN = (IN) root.getTarget();
            if (rootIN == null) {
                return null;
            }
            
            rootRef = root;

            /*
             * Latch the target and re-check that all conditions still hold.
             * The latch on the target will be released by processTarget().
             */
            rootIN.latchNoUpdateLRU();

            if (rootIN == target && rootIN.isRoot()) {
                evictedBytes = processTarget(
                    this, null, /* target */
                    null, /* parent */
                    -1, /* entry index within parent */
                    backgroundIO, source, stats);
            } else {
                rootIN.releaseLatch();
            }
            
            return null;
        }
    }

    /**
     * Decide what to do with an eviction target and carry out the decision.
     * Return the number of bytes evicted (if any).
     *
     * This method is called from evictBatch() after an IN has been selected
     * for eviction. It EX-latches the IN and determines whether it can/should
     * really be evicted, and if not what is the appropriate action to be
     * taken by the evicting thread.
     *
     * If a decision is taken to evict the target or mutate it to a BINDelta,
     * the target must first be unlatched and its parent must be searched
     * within the tree. During this search, many things can happen to the
     * unlatched target, and as a result, after the parent is found and the
     * target is relatched, processTarget() calls itself recursively to
     * re-consider all the possible actions on the target.
     */
    private long processTarget(
        RootEvictor rootEvictor,
        IN target,
        IN parent,
        int index,
        boolean bgIO,
        EvictionSource source,
        EvictionDebugStats stats)
        throws DatabaseException {

        boolean targetIsLatched = false;
        boolean parentIsLatched = false;
        long evictedBytes = 0;

        if (stats != null) {
            stats.withParent = (parent != null || rootEvictor != null);
        }

        try {
            if (parent != null) {
                assert(parent.isLatchExclusiveOwner());
                parentIsLatched = true;

                if (target != parent.getTarget(index)) {
                    skip(target, stats);
                    return 0;
                }

                target.latchNoUpdateLRU();

            } else if (rootEvictor != null) {
                target = rootEvictor.target;

            } else {
                target.latchNoUpdateLRU();
            }

            targetIsLatched = true;

            DatabaseImpl db = target.getDatabase();
            EnvironmentImpl dbEnv = db.getEnv();

            if (!target.getInListResident() || contains(target)) {
                /*
                 * The node was put back to the LRU, and then possibly evicted
                 * by other threads before this thread could latch it.
                 */
                skip(target, stats);
                return 0;
            }

            /*
             * Normally, UINs that have cached children are not in the LRU,
             * and as a result, cannot be selected for eviction. However, a
             * childless UIN may be selected for eviction and then acquire
             * cached children in the time after its removal from its LRUSet
             * and before it is EX-latched by the evicting thread.
             */
            if (target.isUpperIN() && target.hasCachedChildrenFlag()) {
                assert(target.hasCachedChildren());
                skip(target, stats);
                return 0;
            }

            /*
             * Disallow eviction of the mapping and naming DB roots, because
             * their eviction and re-fetching is a special case that is not
             * worth supporting.  [#13415]
             */
            if (target.isRoot()) {
                DatabaseId dbId = db.getId();
                if (dbId.equals(DbTree.ID_DB_ID) ||
                    dbId.equals(DbTree.NAME_DB_ID)) {
                    skip(target, stats);
                    return 0;
                }
            }

            /*
             * For a shared cache, we must prevent background eviction during
             * recovery here, on a per-target basis. For a private
             * evictor/cache, we can do it in alert().
             */
            if (isShared && dbEnv.isInInit() &&
                source == EvictionSource.EVICTORTHREAD) {
                putBack(target, stats, 0);
                return 0;
            }

            assert !(dbEnv.isInInit() &&
                source == EvictionSource.EVICTORTHREAD);

            if (isShared) {

                if (dbEnv.isClosed() || dbEnv.wasInvalidated()) {
                    skip(target, stats);
                    return 0;
                }

                if (!dbEnv.getMemoryBudget().isTreeUsageAboveMinimum()) {
                    putBack(target, stats, 1);
                    return 0;
                }
            }

            if (target.isPinned()) {
                putBack(target, stats, 2);
                return 0;
            }

            /*
             * Attempt partial eviction. The partialEviction() method also
             * determines whether the IN in evictable or not. For now,
             * partialEviction() will consider a node to be non-evictable if
             * it is a BIN that (a) has cursors registered on it, or (b) has
             * a resident non-evictable LN, which can happen only for MapLNs
             * (see MapLN.isEvictable()).
             */
            evictedBytes = target.partialEviction();

            boolean isEvictable = (evictedBytes & IN.NON_EVICTABLE_IN) == 0;
            evictedBytes &= ~IN.NON_EVICTABLE_IN;

            /*
             * If we could evict some bytes from this node, put it back in
             * the LRU, unless it is a BIN being explicitly evicted via a cache
             * mode, in which case we should evict it, if possible.
             */
            if (evictedBytes > 0 && 
                (target.isUpperIN() || source != EvictionSource.CACHEMODE)) {
                strippedPutBack(target, stats);
                return evictedBytes;
            }

            /*
             * If the node is not evictable, put it back.
             *
             * TODO: Logically this check should come after BIN mutation, not
             * before, but currently this would have little or no impact.
             */
            if (!isEvictable) {
                putBack(target, stats, 5);
                return evictedBytes;
            }

            /*
             * Give the node a second chance, if it is a full BIN that can be
             * mutated to a BINDelta and it is not a BIN being explicitly
             * evicted via a cache mode.
             */
            if (target.isBIN() &&
                source != EvictionSource.CACHEMODE &&
                mutateBins &&
                ((BIN)target).canMutateToBINDelta()) {
                
                BIN bin = (BIN)target;
                evictedBytes += bin.mutateToBINDelta();
                assert(evictedBytes > 0);
                binDeltaPutBack(target, stats);

                return evictedBytes;
            }

            /*
             * Give the node a second chance, if it is dirty and is not in the
             * priority-2 LRUSet already.
             */
            if (useDirtyLRUSet &&
                target.getDirty() &&
                !target.isInPri2LRU()) {

                moveToPri2LRU(target, stats);
                return evictedBytes;
            }

            /*
             * Give the node a second chance, if it has off-heap BIN children
             * and is not in the priority-2 LRUSet already.
             */
            if (useOffHeapCache &&
                target.hasOffHeapBINIds() &&
                !target.isInPri2LRU()) {

                moveToPri2LRU(target, stats);
                return evictedBytes;
            }

            /*
             * Evict the node. To do so, we must find and latch the
             * parent IN first, if we have not done this already.
             */
            if (rootEvictor != null) {
                evictedBytes += evictRoot(rootEvictor, bgIO, source, stats);

            } else if (parent != null) {
                evictedBytes += evict(target, parent, index, bgIO, stats);

            } else {
                assert TestHookExecute.doHookIfSet(preEvictINHook);
                targetIsLatched = false;
                evictedBytes += findParentAndRetry(
                    target, bgIO, source, stats);
            }

            return evictedBytes;

        } finally {
            if (targetIsLatched) {
                target.releaseLatch();
            }
            
            if (parentIsLatched) {
                parent.releaseLatch();
            }
        }
    }

    private void skip(IN target, EvictionDebugStats stats) {

        if ((traceUINs && target.isUpperIN()) ||
            (traceBINs && target.isBIN())) {
            LoggerUtils.envLogMsg(
                traceLevel, target.getEnv(),
                Thread.currentThread().getId() + "-" +
                    Thread.currentThread().getName() +
                    "-" + target.getEnv().getName() +
                    " XXXX SKIPPED Eviction Target: " +
                    target.getNodeId());
        }

        nNodesSkipped.increment();
    }

    private void putBack(IN target, EvictionDebugStats stats, int caller) {

        if ((traceUINs && target.isUpperIN()) ||
            (traceBINs && target.isBIN())) {
            LoggerUtils.envLogMsg(
                traceLevel, target.getEnv(),
                Thread.currentThread().getId() + "-" +
                Thread.currentThread().getName() +
                "-" + target.getEnv().getName() + 
                " XXXX PUT-BACK-" + caller + " Eviction Target: " +
                target.getNodeId());
        }

        if (target.isInPri2LRU()) {
            pri2AddBack(target);
        } else {
            addBack(target);
        }

        if (stats != null) {
            stats.incNumPutBack();
        }

        nNodesPutBack.increment();
    }

    private void strippedPutBack(IN target, EvictionDebugStats stats) {

        if ((traceUINs && target.isUpperIN()) ||
            (traceBINs && target.isBIN())) {
            LoggerUtils.envLogMsg(
                traceLevel, target.getEnv(),
                Thread.currentThread().getId() + "-" +
                Thread.currentThread().getName() +
                "-" + target.getEnv().getName() + 
                " XXXX STRIPPED Eviction Target: " +
                target.getNodeId());
        }

        if (target.isInPri2LRU()) {
            pri2AddBack(target);
        } else {
            addBack(target);
        }
                    
        if (stats != null) {
            stats.incNumStripped();
        }

        nNodesStripped.increment();
    }

    private void binDeltaPutBack(IN target, EvictionDebugStats stats) {

        if ((traceUINs && target.isUpperIN()) ||
            (traceBINs && target.isBIN())) {
            LoggerUtils.envLogMsg(
                traceLevel, target.getEnv(),
                Thread.currentThread().getId() + "-" +
                Thread.currentThread().getName() +
                "-" + target.getEnv().getName() + 
                " XXXX MUTATED Eviction Target: " +
                target.getNodeId());
        }

        if (target.isInPri2LRU()) {
            pri2AddBack(target);
        } else {
            addBack(target);
        }

        if (stats != null) {
            stats.incNumMutated();
        }

        nNodesMutated.increment();
    }

    private void moveToPri2LRU(IN target, EvictionDebugStats stats) {

        if ((traceUINs && target.isUpperIN()) ||
            (traceBINs && target.isBIN())) {
            LoggerUtils.envLogMsg(
                traceLevel, target.getEnv(),
                Thread.currentThread().getId() + "-" +
                    Thread.currentThread().getName() +
                "-" + target.getEnv().getName() + 
                " XXXX MOVED-TO_PRI2 Eviction Target: " +
                target.getNodeId());
        }

        if (stats != null) {
            stats.incNumMoved(target.isBIN());
        }

        pri2AddFront(target);

        nNodesMovedToPri2LRU.increment();
    }

    private long findParentAndRetry(
        IN target,
        boolean backgroundIO,
        EvictionSource source,
        EvictionDebugStats stats) {
        
        Tree tree = target.getDatabase().getTree();

        /*
         * Pass false for doFetch to avoid fetching a full BIN when a
         * delta is in cache. This also avoids a fetch when the node
         * was evicted while unlatched, but that should be very rare.
         */
        SearchResult result = tree.getParentINForChildIN(
            target, false, /*useTargetLevel*/
            false, /*doFetch*/ CacheMode.UNCHANGED);

        if (result.exactParentFound) {
            return processTarget(null, /* rootEvictor */
                                 target,
                                 result.parent,
                                 result.index,
                                 backgroundIO,
                                 source,
                                 stats);
        }

        /*
         * The target has been detached from the tree and it should stay
         * out of the LRU. It should not be in the INList, because whenever
         * we detach a node we remove it from the INList, but in case we
         * forgot to do this somewhere, we can just remove it here. 
         */
        assert(result.parent == null);

        target.latchNoUpdateLRU();

        try {
            if (target.getInListResident()) {

                firstEnvImpl.getInMemoryINs().remove(target);

                throw EnvironmentFailureException.unexpectedState(
                    "Node " + target.getNodeId() +
                    " has been detached from the in-memory tree," +
                    " but it is still in the INList. lastLogged=" +
                    DbLsn.getNoFormatString(target.getLastLoggedLsn()));
            }
        } finally {
            target.releaseLatch();
        }

        return 0;
    }

    private long evict(
        IN target,
        IN parent,
        int index,
        boolean backgroundIO,
        EvictionDebugStats stats) {
        
        final DatabaseImpl db = target.getDatabase();
        final EnvironmentImpl dbEnv = db.getEnv();
        final OffHeapCache ohCache = dbEnv.getOffHeapCache();

        //System.out.println("Evicting BIN " + target.getNodeId());

        boolean storedOffHeap = false;

        if (useOffHeapCache && target.isBIN()) {
            storedOffHeap = ohCache.storeEvictedBIN(
                (BIN) target, parent, index);
        }

        if (target.getNormalizedLevel() == 2) {
            if (!ohCache.flushAndDiscardBINChildren(target, backgroundIO)) {
                /* Could not log a dirty BIN. See below. */
                skip(target, stats);
                return 0;
            }
        }

        boolean logged = false;
        long loggedLsn = DbLsn.NULL_LSN;

        if (target.getDirty() && !storedOffHeap) {
            /*
             * Cannot evict dirty nodes in a read-only environment, or when a
             * disk limit has been exceeded. We can assume that the cache will
             * not overflow with dirty nodes because writes are prohibited.
             */
            if (dbEnv.isReadOnly() || dbEnv.getDiskLimitViolation() != null) {
                skip(target, stats);
                return 0;
            }

            Provisional provisional = dbEnv.coordinateWithCheckpoint(
                db, target.getLevel(), parent);

            loggedLsn = target.log(
                allowBinDeltas, provisional, backgroundIO, parent);

            logged = true;
        }

        long evictedBytes = target.getBudgetedMemorySize();

        parent.detachNode(index, logged /*updateLsn*/, loggedLsn);

        nNodesEvicted.increment();

        if (logged) {
            nDirtyNodesEvicted.increment();
        }

        if (stats != null) {
            stats.incNumEvicted(target.isBIN());
        }

        return evictedBytes;
    }

    private long evictRoot(
        RootEvictor rootEvictor,
        boolean backgroundIO,
        EvictionSource source,
        EvictionDebugStats stats) {

        final ChildReference rootRef = rootEvictor.rootRef;
        final IN target = (IN) rootRef.getTarget();
        final DatabaseImpl db = target.getDatabase();
        final EnvironmentImpl dbEnv = db.getEnv();
        final INList inList = dbEnv.getInMemoryINs();

        if (target.getNormalizedLevel() == 2) {
            if (!dbEnv.getOffHeapCache().flushAndDiscardBINChildren(
                target, backgroundIO)) {
                /* Could not log a dirty BIN. See below. */
                skip(target, stats);
                return 0;
            }
        }

        if (target.getDirty()) {
            /*
             * Cannot evict dirty nodes in a read-only environment, or when a
             * disk limit has been exceeded. We can assume that the cache will
             * not overflow with dirty nodes because writes are prohibited.
             */
            if (dbEnv.isReadOnly() || dbEnv.getDiskLimitViolation() != null) {
                skip(target, stats);
                return 0;
            }

            Provisional provisional = dbEnv.coordinateWithCheckpoint(
                db, target.getLevel(), null /*parent*/);

            long newLsn = target.log(
                false /*allowDeltas*/, provisional,
                backgroundIO, null  /*parent*/);
            
            rootRef.setLsn(newLsn);
            rootEvictor.flushed = true;
        }
        
        inList.remove(target);

        long evictBytes = target.getBudgetedMemorySize();
        
        rootRef.clearTarget();

        nNodesEvicted.increment();
        nRootNodesEvicted.increment();

        if (rootEvictor.flushed) {
            nDirtyNodesEvicted.increment();
        }

        if (stats != null) {
            stats.incNumEvicted(false);
        }

        return evictBytes;
    }

    /* For unit testing only. */
    public void setRunnableHook(TestHook<Boolean> hook) {
        arbiter.setRunnableHook(hook);
    }

    /* For unit testing only. */
    public void setPreEvictINHook(TestHook<Object> hook) {
        preEvictINHook = hook;
    }

    /* For unit testing only. */
    public void setEvictProfileHook(TestHook<IN> hook) {
        evictProfile = hook;
    }

    public StatGroup getStatsGroup() {
        return stats;
    }

    /**
     * Load stats.
     */
    public StatGroup loadStats(StatsConfig config) {

        if (isShared) {
            sharedCacheEnvs.set(envInfos.size());
        }

        float binFetchMisses = (float)nBINFetchMiss.get();
        float binFetches = (float)nBINFetch.get();

        binFetchMissRatio.set(
            (binFetches > 0 ? (binFetchMisses / binFetches) : 0));

        StatGroup copy = stats.cloneGroup(config.getClear());

        /*
         * These stats are not cleared. They represent the current state of
         * the cache.
         */
        new LongStat(copy, CACHED_IN_SPARSE_TARGET, nINSparseTarget.get());
        new LongStat(copy, CACHED_IN_NO_TARGET, nINNoTarget.get());
        new LongStat(copy, CACHED_IN_COMPACT_KEY, nINCompactKey.get());

        new LongStat(copy, PRI1_LRU_SIZE, getPri1LRUSize());
        new LongStat(copy, PRI2_LRU_SIZE, getPri2LRUSize());

        copy.addAll(getINListStats(config));

        return copy;
    }

    private StatGroup getINListStats(StatsConfig config) {

        if (isShared) {

            StatGroup totalINListStats = new StatGroup("temp", "temp");

            if (config.getFast()) {

                /* 
                 * This is a slow stat for shared envs, because of the need to
                 * synchronize.
                 */
                return totalINListStats;
            }

            List<EnvInfo> copy = null;
            synchronized(this) {
                copy = new ArrayList<EnvInfo>(envInfos);
            }
            
            for (EnvInfo ei: copy) {
                totalINListStats.addAll(ei.env.getInMemoryINs().loadStats());
            }

            return totalINListStats;
        } else {
            return firstEnvImpl.getInMemoryINs().loadStats();
        }
    }

    public void incNumLNsEvicted(long inc) {
        nLNsEvicted.add(inc);
    }

    /**
     * Update the appropriate fetch stat, based on node type.
     */
    public void incLNFetchStats(boolean isMiss) {
        nLNFetch.increment();
        if (isMiss) {
            nLNFetchMiss.increment();
        }
    }

    public void incUINFetchStats(boolean isMiss) {
        nUpperINFetch.increment();
        if (isMiss) {
            nUpperINFetchMiss.increment();
        }
    }

    public void incBINFetchStats(boolean isMiss, boolean isDelta) {
        nBINFetch.increment();
        if (isMiss) {
            nBINFetchMiss.increment();
            if (isDelta) {
                nBINDeltaFetchMiss.increment();
            }
        }
    }

    public void incFullBINMissStats() {
        nFullBINMiss.increment();
    }

    public void incBinDeltaBlindOps() {
        nBinDeltaBlindOps.increment();
    }

    public AtomicLong getNINSparseTarget() {
        return nINSparseTarget;
    }

    public AtomicLong getNINNoTarget() {
        return nINNoTarget;
    }

    public AtomicLong getNINCompactKey() {
        return nINCompactKey;
    }


    static class ReentrancyGuard {
        private final ConcurrentHashMap<Thread, Thread> activeThreads;
        private final EnvironmentImpl envImpl;
        private final Logger logger;

        ReentrancyGuard(EnvironmentImpl envImpl, Logger logger) {
            this.envImpl = envImpl;
            this.logger = logger;
            activeThreads = new ConcurrentHashMap<Thread, Thread>();
        }

        boolean enter() {
            Thread thisThread = Thread.currentThread();
            if (activeThreads.containsKey(thisThread)) {
                /* We don't really expect a reentrant call. */
                LoggerUtils.severe(logger, envImpl,
                                   "reentrant call to eviction from " +
                                   LoggerUtils.getStackTrace());

                /* If running w/assertions, in testing mode, assert here. */
                assert false: "reentrant call to eviction from " +
                    LoggerUtils.getStackTrace();
                return false;
            }

            activeThreads.put(thisThread, thisThread);
            return true;
        }

        void leave() {
            assert activeThreads.contains(Thread.currentThread());
            activeThreads.remove(Thread.currentThread());
        }
    }

    static class BackgroundEvictTask implements Runnable {

        private final Evictor evictor;
        private final boolean backgroundIO;

        BackgroundEvictTask(Evictor evictor) {
            this.evictor = evictor;
            this.backgroundIO = true;
        }

        public void run() {
            evictor.activePoolThreads.incrementAndGet();
            try {
                evictor.doEvict(EvictionSource.EVICTORTHREAD, backgroundIO);
            } finally {
                evictor.activePoolThreads.decrementAndGet();
            }
        }
    }

    static class RejectEvictHandler implements RejectedExecutionHandler {

        private final AtomicLongStat threadUnavailableStat;

        RejectEvictHandler(AtomicLongStat threadUnavailableStat) {
            this.threadUnavailableStat = threadUnavailableStat;
        }

        public void rejectedExecution(Runnable r,
                                      ThreadPoolExecutor executor) {
            threadUnavailableStat.increment();
        }
    }

    /**
     * Caches DatabaseImpls to reduce DbTree.getDb overhead.
     *
     * SharedEvictor, unlike PrivateEvictor, must maintain a cache map for each
     * EnvironmentImpl, since each cache map is logically associated with a
     * single DbTree instance.
     */
    static class DbCache {

        boolean shared = false;

        int nOperations = 0;

        int dbCacheClearCount = 0;

        final Map<EnvironmentImpl, Map<DatabaseId, DatabaseImpl>> envMap;

        final Map<DatabaseId, DatabaseImpl> dbMap;

        DbCache(boolean shared, int dbCacheClearCount) {

            this.shared = shared;
            this.dbCacheClearCount = dbCacheClearCount;

            if (shared) {
                envMap = 
                new HashMap<EnvironmentImpl, Map<DatabaseId, DatabaseImpl>>();

                dbMap = null;
            } else {
                dbMap = new HashMap<DatabaseId, DatabaseImpl>();
                envMap = null;
            }
        }

        /**
         * Calls DbTree.getDb for the given environment and database ID, and
         * caches the result to optimize multiple calls for the same DB.
         *
         * @param env identifies which environment the dbId parameter
         * belongs to.  For PrivateEvictor, it is the same as the
         * Evictor.firstEnvImpl field.
         *
         * @param dbId is the DB to get.
         */
        DatabaseImpl getDb(EnvironmentImpl env, DatabaseId dbId) {

            Map<DatabaseId, DatabaseImpl> map;

            if (shared) {
                map = envMap.get(env);
                if (map == null) {
                    map = new HashMap<DatabaseId, DatabaseImpl>();
                    envMap.put(env, map);
                }
            } else {
                map = dbMap;
            }

            /*
             * Clear DB cache after dbCacheClearCount operations, to
             * prevent starving other threads that need exclusive access to
             * the MapLN (for example, DbTree.deleteMapLN).  [#21015]
             *
             * Note that we clear the caches for all environments after
             * dbCacheClearCount total operations, rather than after
             * dbCacheClearCount operations for a single environment,
             * because the total is a more accurate representation of
             * elapsed time, during which other threads may be waiting for
             * exclusive access to the MapLN.
             */
            nOperations += 1;
            if ((nOperations % dbCacheClearCount) == 0) {
                releaseDbs(env);
            }

            return env.getDbTree().getDb(dbId, -1, map);
        }

        /**
         * Calls DbTree.releaseDb for cached DBs, and clears the cache.
         */
        void releaseDbs(EnvironmentImpl env) {
            if (shared) {
                for (Map.Entry<EnvironmentImpl, Map<DatabaseId, DatabaseImpl>>
                     entry : envMap.entrySet()) {

                    final EnvironmentImpl sharingEnv = entry.getKey();
                    final Map<DatabaseId, DatabaseImpl> map = entry.getValue();

                    sharingEnv.getDbTree().releaseDbs(map);
                    map.clear();
                }
            } else {
                env.getDbTree().releaseDbs(dbMap);
                dbMap.clear();
            }
        }
    }
}
