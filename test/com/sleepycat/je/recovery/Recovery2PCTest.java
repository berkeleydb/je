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

package com.sleepycat.je.recovery;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.junit.Assume;
import org.junit.experimental.theories.DataPoint;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

import com.sleepycat.je.Transaction;
import com.sleepycat.je.XAEnvironment;
import com.sleepycat.je.log.LogUtils.XidImpl;
import com.sleepycat.utilint.StringUtils;

@RunWith(Theories.class)
public class Recovery2PCTest extends RecoveryTestBase {
    private boolean explicitTxn;
    private boolean commit;
    private boolean recover;
    
    /* We only need to test XARecoveryAPI for implicit and explicit. */
    @DataPoint
    public static boolean enable = true;
    
    @DataPoint
    public static boolean disable = false;
    
    private String opName() {
        StringBuilder sb = new StringBuilder();

        if (explicitTxn) {
            sb.append("Exp");
        } else {
            sb.append("Imp");
        }

        sb.append("-");

        if (commit) {
            sb.append("C");
        } else {
            sb.append("A");
        }

        sb.append("-");

        if (recover) {
            sb.append("Rec");
        } else {
            sb.append("No Rec");
        }

        return sb.toString();
    }

    @Theory
    public void testDetectUnfinishedXATxns(boolean implicit, 
                                           boolean commitFlag,
                                           boolean recoverFlag)
        throws Throwable {

        Assume.assumeTrue(implicit);
        Assume.assumeTrue(commitFlag);
        Assume.assumeTrue(recoverFlag);
        
        explicitTxn = implicit;
        commit = commitFlag;
        recover = recoverFlag;
        
        customName = opName();
        
        System.out.println("TestCase: Recovery2PCTest-" +
        		"testDetectUnfinishedXATxns-" + customName);

        createXAEnvAndDbs(1 << 20, false/*runCheckpointerDaemon*/, 1, false);
        XAEnvironment xaEnv = (XAEnvironment) env;
        int numRecs = 2;

        try {
            Map<TestData, Set<TestData>> expectedData =
                new HashMap<TestData, Set<TestData>>();

            /* Insert all the data. */
            XidImpl xid =
                new XidImpl(1, StringUtils.toUTF8("TwoPCTest1"), null);
            Transaction txn = env.beginTransaction(null, null);
            xaEnv.setXATransaction(xid, txn);
            insertData(txn, 0, numRecs, expectedData, 1, commit, 1);
            xaEnv.prepare(xid);
            txn.commit();
            dbs[0].close();
            /* Close with a still-open XA Txn. */
            try {
                xaEnv.close();
                fail("expected IllegalStateException");
            } catch (IllegalStateException DE) {
                if (!DE.getMessage().contains("There is")) {
                    DE.printStackTrace(System.out);
                    fail("expected open XA message, but got " +
                         DE.getMessage());
                }
            }
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }

    @Theory
    public void testBasic(boolean implicit, 
                          boolean commitFlag,          
                          boolean recoverFlag)
        throws Throwable {
        
        explicitTxn = implicit;
        commit = commitFlag;
        recover = recoverFlag;
        
        customName = opName();
        
        System.out.println("TestCase: Recovery2PCTest-testBasic-" + customName);

        createXAEnvAndDbs(1 << 20, false/*runCheckpointerDaemon*/,
                          NUM_DBS, !recover);
        XAEnvironment xaEnv = (XAEnvironment) env;
        int numRecs = NUM_RECS * 3;

        try {
            /* Set up an repository of expected data. */
            Map<TestData, Set<TestData>> expectedData =
                new HashMap<TestData, Set<TestData>>();

            /* Insert all the data. */
            XidImpl xid =
                new XidImpl(1, StringUtils.toUTF8("TwoPCTest1"), null);
            Transaction txn = null;
            if (explicitTxn) {
                txn = env.beginTransaction(null, null);
                xaEnv.setXATransaction(xid, txn);
            } else {
                xaEnv.start(xid, XAResource.TMNOFLAGS);
            }
            insertData(txn, 0, numRecs - 1, expectedData, 1, commit, NUM_DBS);
            if (!explicitTxn) {
                xaEnv.end(xid, XAResource.TMSUCCESS);
            }

            xaEnv.prepare(xid);

            if (recover) {
                closeEnv();
                xaRecoverOnly();
                xaEnv = (XAEnvironment) env;
            }

            if (commit) {
                xaEnv.commit(xid, false);
            } else {
                xaEnv.rollback(xid);
            }

            if (recover) {
                verifyData(expectedData, commit, NUM_DBS);
                forceCloseEnvOnly();
            } else {
                closeEnv();
            }
            xaRecoverAndVerify(expectedData, NUM_DBS);
        } catch (Throwable t) {
            /* Print stacktrace before trying to clean up files. */
            t.printStackTrace();
            throw t;
        }
    }

    @Theory
    public void testXARecoverAPI(boolean implicit, 
                                 boolean commitFlag,          
                                 boolean recoverFlag)
         throws Throwable {
        
        Assume.assumeTrue(commitFlag);
        Assume.assumeTrue(recoverFlag);
        explicitTxn = implicit;
        commit = commitFlag;
        recover = recoverFlag;
        
        customName = opName();
        
        System.out.println("TestCase: Recovery2PCTest-testXARecoverAPI-" + 
                           customName);
        
        createXAEnvAndDbs(1 << 20, false/*runCheckpointerDaemon*/,
                          NUM_DBS << 1, false);
        final XAEnvironment xaEnv = (XAEnvironment) env;
        final int numRecs = NUM_RECS * 3;

        try {
            /* Set up an repository of expected data. */
            final Map<TestData, Set<TestData>> expectedData1 =
                new HashMap<TestData, Set<TestData>>();

            final Map<TestData, Set<TestData>> expectedData2 =
                new HashMap<TestData, Set<TestData>>();

            /* Insert all the data. */
            final Transaction txn1 =
                (explicitTxn ?
                 env.beginTransaction(null, null) :
                 null);
            final Transaction txn2 =
                (explicitTxn ?
                 env.beginTransaction(null, null) :
                 null);
            final XidImpl xid1 =
                new XidImpl(1, StringUtils.toUTF8("TwoPCTest1"), null);
            final XidImpl xid2 =
                new XidImpl(1, StringUtils.toUTF8("TwoPCTest2"), null);

            Thread thread1 = new Thread() {
                    @Override
                    public void run() {
                        try {
                            if (explicitTxn) {
                                xaEnv.setXATransaction(xid1, txn1);
                            } else {
                                xaEnv.start(xid1, XAResource.TMNOFLAGS);
                            }
                            Thread.yield();
                            insertData(txn1, 0, numRecs - 1, expectedData1, 1,
                                       true, 0, NUM_DBS);
                            Thread.yield();
                            if (!explicitTxn) {
                                xaEnv.end(xid1, XAResource.TMSUCCESS);
                            }
                            Thread.yield();
                        } catch (Exception E) {
                            fail("unexpected: " + E);
                        }
                    }
                };

            Thread thread2 = new Thread() {
                    @Override
                    public void run() {
                        try {
                            if (explicitTxn) {
                                xaEnv.setXATransaction(xid2, txn2);
                            } else {
                                xaEnv.start(xid2, XAResource.TMNOFLAGS);
                            }
                            Thread.yield();
                            insertData(txn2, numRecs, numRecs << 1,
                                       expectedData2, 1, false, NUM_DBS,
                                       NUM_DBS << 1);
                            Thread.yield();
                            if (!explicitTxn) {
                                xaEnv.end(xid2, XAResource.TMSUCCESS);
                            }
                            Thread.yield();
                        } catch (Exception E) {
                            fail("unexpected: " + E);
                        }
                    }
                };

            thread1.start();
            thread2.start();
            thread1.join();
            thread2.join();

            xaEnv.prepare(xid1);
            try {
                xaEnv.prepare(xid1);
                fail("should have thrown XID has already been registered");
            } catch (XAException XAE) {
                // xid1 has already been registered.
            }
            xaEnv.prepare(xid2);

            XAEnvironment xaEnv2 = xaEnv;
            Xid[] unfinishedXAXids = xaEnv2.recover(0);
            assertTrue(unfinishedXAXids.length == 2);
            boolean sawXid1 = false;
            boolean sawXid2 = false;
            for (int i = 0; i < 2; i++) {
                if (unfinishedXAXids[i].equals(xid1)) {
                    if (sawXid1) {
                        fail("saw Xid1 twice");
                    }
                    sawXid1 = true;
                }
                if (unfinishedXAXids[i].equals(xid2)) {
                    if (sawXid2) {
                        fail("saw Xid2 twice");
                    }
                    sawXid2 = true;
                }
            }
            assertTrue(sawXid1 && sawXid2);

            for (int ii = 0; ii < 4; ii++) {
                forceCloseEnvOnly();
                xaEnv2 = (XAEnvironment) env;
                xaRecoverOnly();
                xaEnv2 = (XAEnvironment) env;

                unfinishedXAXids = xaEnv2.recover(0);
                assertTrue(unfinishedXAXids.length == 2);
                sawXid1 = false;
                sawXid2 = false;
                for (int i = 0; i < 2; i++) {
                    if (unfinishedXAXids[i].equals(xid1)) {
                        if (sawXid1) {
                            fail("saw Xid1 twice");
                        }
                        sawXid1 = true;
                    }
                    if (unfinishedXAXids[i].equals(xid2)) {
                        if (sawXid2) {
                            fail("saw Xid2 twice");
                        }
                        sawXid2 = true;
                    }
                }
                assertTrue(sawXid1 && sawXid2);
            }

            xaEnv2 = (XAEnvironment) env;
            xaEnv2.getXATransaction(xid1);
            xaEnv2.getXATransaction(xid2);
            xaEnv2.commit(xid1, false);
            xaEnv2.rollback(xid2);
            verifyData(expectedData1, false, 0, NUM_DBS);
            verifyData(expectedData2, false, NUM_DBS, NUM_DBS << 1);
            forceCloseEnvOnly();
            xaRecoverOnly();
            verifyData(expectedData1, false, 0, NUM_DBS);
            verifyData(expectedData2, false, NUM_DBS, NUM_DBS << 1);
        } catch (Throwable t) {
            /* Print stacktrace before trying to clean up files. */
            t.printStackTrace();
            throw t;
        }
    }

    @Theory
    public void testXARecoverArgCheck(boolean implicit, 
                                      boolean commitFlag,          
                                      boolean recoverFlag)
         throws Throwable {

        Assume.assumeTrue(implicit);
        Assume.assumeTrue(commitFlag);
        Assume.assumeTrue(recoverFlag);
        
        explicitTxn = implicit;
        commit = commitFlag;
        recover = recoverFlag;
        
        customName = opName();
        
        System.out.println("TestCase: Recovery2PCTest-testXARecoverArgCheck-" 
                            + customName);
        
        createXAEnvAndDbs(1 << 20, false/*runCheckpointerDaemon*/,
                          NUM_DBS, false);
        XAEnvironment xaEnv = (XAEnvironment) env;

        try {
            XidImpl xid =
                new XidImpl(1, StringUtils.toUTF8("TwoPCTest1"), null);

            /* Check that only one of TMJOIN and TMRESUME can be set. */
            try {
                xaEnv.start(xid, XAResource.TMJOIN | XAResource.TMRESUME);
                fail("Expected XAException(XAException.XAER_INVAL)");
            } catch (XAException XAE) {
                /* Expect this. */
                assertTrue(XAE.errorCode == XAException.XAER_INVAL);
            }

            /*
             * Check that only one of TMJOIN and TMRESUME can be set by passing
             * a bogus flag value (TMSUSPEND).
             */
            try {
                xaEnv.start(xid, XAResource.TMSUSPEND);
                fail("Expected XAException(XAException.XAER_INVAL)");
            } catch (XAException XAE) {
                /* Expect this. */
                assertTrue(XAE.errorCode == XAException.XAER_INVAL);
            }

            xaEnv.start(xid, XAResource.TMNOFLAGS);
            try {
                xaEnv.start(xid, XAResource.TMNOFLAGS);
                fail("Expected XAER_DUPID");
            } catch (XAException XAE) {
                /* Expect this. */
                assertTrue(XAE.errorCode == XAException.XAER_DUPID);
            }
            xaEnv.end(xid, XAResource.TMSUCCESS);

            /*
             * Check that JOIN with a non-existant association throws NOTA.
             */
            try {
                xid = new XidImpl(1, StringUtils.toUTF8("TwoPCTest2"), null);
                xaEnv.start(xid, XAResource.TMJOIN);
                fail("Expected XAER_NOTA");
            } catch (XAException XAE) {
                /* Expect this. */
                assertTrue(XAE.errorCode == XAException.XAER_NOTA);
            }

            /*
             * Check that RESUME with a non-existant association throws NOTA.
             */
            try {
                xaEnv.start(xid, XAResource.TMRESUME);
                fail("Expected XAER_NOTA");
            } catch (XAException XAE) {
                /* Expect this. */
                assertTrue(XAE.errorCode == XAException.XAER_NOTA);
            }

            /*
             * Check that start(JOIN) from a thread that is already associated
             * throws XAER_PROTO.
             */
            Xid xid2 = new XidImpl(1, StringUtils.toUTF8("TwoPCTest3"), null);
            xaEnv.start(xid2, XAResource.TMNOFLAGS);
            xaEnv.end(xid2, XAResource.TMSUCCESS);
            xid = new XidImpl(1, StringUtils.toUTF8("TwoPCTest2"), null);
            xaEnv.start(xid, XAResource.TMNOFLAGS);
            try {
                xaEnv.start(xid2, XAResource.TMJOIN);
                fail("Expected XAER_PROTO");
            } catch (XAException XAE) {
                /* Expect this. */
                assertTrue(XAE.errorCode == XAException.XAER_PROTO);
            }

            /*
             * Check that start(RESUME) for an xid that is not suspended throws
             * XAER_PROTO.
             */
            try {
                xid = new XidImpl(1, StringUtils.toUTF8("TwoPCTest2"), null);
                xaEnv.start(xid, XAResource.TMRESUME);
                fail("Expected XAER_PROTO");
            } catch (XAException XAE) {
                /* Expect this. */
                assertTrue(XAE.errorCode == XAException.XAER_PROTO);
            }

            /*
             * Check that end(TMFAIL | TMSUCCESS) throws XAER_INVAL.
             */
            try {
                xid = new XidImpl(1, StringUtils.toUTF8("TwoPCTest2"), null);
                xaEnv.end(xid, XAResource.TMFAIL | XAResource.TMSUCCESS);
                fail("Expected XAER_INVAL");
            } catch (XAException XAE) {
                /* Expect this. */
                assertTrue(XAE.errorCode == XAException.XAER_INVAL);
            }

            /*
             * Check that end(TMFAIL | TMSUSPEND) throws XAER_INVAL.
             */
            try {
                xid = new XidImpl(1, StringUtils.toUTF8("TwoPCTest2"), null);
                xaEnv.end(xid, XAResource.TMFAIL | XAResource.TMSUSPEND);
                fail("Expected XAER_INVAL");
            } catch (XAException XAE) {
                /* Expect this. */
                assertTrue(XAE.errorCode == XAException.XAER_INVAL);
            }

            /*
             * Check that end(TMSUCCESS | TMSUSPEND) throws XAER_INVAL.
             */
            try {
                xid = new XidImpl(1, StringUtils.toUTF8("TwoPCTest2"), null);
                xaEnv.end(xid, XAResource.TMSUCCESS | XAResource.TMSUSPEND);
                fail("Expected XAER_INVAL");
            } catch (XAException XAE) {
                /* Expect this. */
                assertTrue(XAE.errorCode == XAException.XAER_INVAL);
            }

            /*
             * Check that end(TMSUSPEND) actually works.
             */
            Xid xid4 = new XidImpl(1, StringUtils.toUTF8("TwoPCTest4"), null);
            xaEnv.start(xid4, XAResource.TMNOFLAGS);
            Transaction txn4 = xaEnv.getThreadTransaction();
            assertTrue(txn4 != null);
            xaEnv.end(xid4, XAResource.TMSUSPEND);
            assertTrue(xaEnv.getThreadTransaction() == null);
            Xid xid5 = new XidImpl(1, StringUtils.toUTF8("TwoPCTest5"), null);
            xaEnv.start(xid5, XAResource.TMNOFLAGS);
            Transaction txn5 = xaEnv.getThreadTransaction();
            xaEnv.end(xid5, XAResource.TMSUSPEND);
            assertTrue(xaEnv.getThreadTransaction() == null);
            xaEnv.start(xid4, XAResource.TMRESUME);
            assertTrue(xaEnv.getThreadTransaction().equals(txn4));
            xaEnv.end(xid4, XAResource.TMSUCCESS);
            xaEnv.start(xid5, XAResource.TMRESUME);
            assertTrue(xaEnv.getThreadTransaction().equals(txn5));
            xaEnv.end(xid5, XAResource.TMSUCCESS);

            /*
             * Check TMFAIL.
             */
            try {
                xid = new XidImpl(1, StringUtils.toUTF8("TwoPCTest6"), null);
                xaEnv.start(xid, XAResource.TMNOFLAGS);
                xaEnv.end(xid, XAResource.TMFAIL);
                xaEnv.commit(xid, false);
                fail("Expected XA_RBROLLBACK");
            } catch (XAException XAE) {
                /* Expect this. */
                assertTrue(XAE.errorCode == XAException.XA_RBROLLBACK);
            }
            xaEnv.rollback(xid);

            /*
             * Check TMSUCCESS.
             */
            xid = new XidImpl(1, StringUtils.toUTF8("TwoPCTest6"), null);
            xaEnv.start(xid, XAResource.TMNOFLAGS);
            xaEnv.end(xid, XAResource.TMSUCCESS);
            xaEnv.commit(xid, false);

            /*
             * Check start(); end(SUSPEND); end(SUCCESS).  This is a case that
             * JBoss causes to happen.  It should succeed.
             */
            xid = new XidImpl(1, StringUtils.toUTF8("TwoPCTest7"), null);
            xaEnv.start(xid, XAResource.TMNOFLAGS);
            xaEnv.end(xid, XAResource.TMSUSPEND);
            xaEnv.end(xid, XAResource.TMSUCCESS);
            xaEnv.commit(xid, false);

            /*
             * Check end(SUSPEND, SUCCESS, FAIL) with no start() call.
             * This should fail.
             */
            try {
                xid = new XidImpl(1, StringUtils.toUTF8("TwoPCTest8"), null);
                xaEnv.end(xid, XAResource.TMSUSPEND);
                fail("Expected XAER_NOTA");
            } catch (XAException XAE) {
                assertTrue(XAE.errorCode == XAException.XAER_NOTA);
            }
            try {
                xid = new XidImpl(1, StringUtils.toUTF8("TwoPCTest9"), null);
                xaEnv.end(xid, XAResource.TMSUCCESS);
                fail("Expected XAER_NOTA");
            } catch (XAException XAE) {
                assertTrue(XAE.errorCode == XAException.XAER_NOTA);
            }
            try {
                xid = new XidImpl(1, StringUtils.toUTF8("TwoPCTest10"), null);
                xaEnv.end(xid, XAResource.TMFAIL);
                fail("Expected XAER_NOTA");
            } catch (XAException XAE) {
                assertTrue(XAE.errorCode == XAException.XAER_NOTA);
            }

            /* Check end(NOFLAGS), should fail. */
            try {
                xid = new XidImpl(1, StringUtils.toUTF8("TwoPCTest11"), null);
                xaEnv.start(xid, XAResource.TMNOFLAGS);
                xaEnv.end(xid, XAResource.TMNOFLAGS);
                fail("Expected XAER_INVAL");
            } catch (XAException XAE) {
                assertTrue(XAE.errorCode == XAException.XAER_INVAL);
            }

            /* Check end(SUSPEND), end(SUSPEND), should fail. */
            try {
                xid = new XidImpl(1, StringUtils.toUTF8("TwoPCTest12"), null);
                xaEnv.start(xid, XAResource.TMNOFLAGS);
                xaEnv.end(xid, XAResource.TMSUSPEND);
                xaEnv.end(xid, XAResource.TMSUSPEND);
                fail("Expected XAER_PROTO");
            } catch (XAException XAE) {
                assertTrue(XAE.errorCode == XAException.XAER_PROTO);
            }

            forceCloseEnvOnly();
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }
}
