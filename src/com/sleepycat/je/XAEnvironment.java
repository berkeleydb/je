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

package com.sleepycat.je;

import java.io.File;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.txn.PreparedTxn;
import com.sleepycat.je.txn.Txn;
import com.sleepycat.je.txn.TxnManager;

/**
 * An Environment that implements XAResource.  If JE is used in an XA
 * environment, this class should be used instead of Environment so that
 * appropriate XA functions are available.
 */
public class XAEnvironment extends Environment implements XAResource {

    private static final boolean DEBUG = false;

    /**
     * Create a database environment handle.
     *
     * @param envHome The database environment's home directory.
     *
     * @param configuration The database environment attributes.  If null,
     * default attributes are used.
     */
    public XAEnvironment(File envHome, EnvironmentConfig configuration)
        throws EnvironmentNotFoundException, EnvironmentLockedException {

        super(envHome, configuration);
    }

    /**
     * Used to get the Transaction object given an XA Xid.
     * @hidden
     * Internal use only.
     */
    public Transaction getXATransaction(Xid xid) {
        Txn ret = getXATransactionInternal(xid);
        if (ret == null ||
            ret instanceof PreparedTxn) {
            return null;
        }

        /* Do we guarantee object identity for Transaction objects? */
        return new Transaction(this, ret);
    }

    private Txn getXATransactionInternal(Xid xid) {
        return getNonNullEnvImpl().getTxnManager().getTxnFromXid(xid);
    }

    /**
     * Used to set the Transaction object for an XA Xid.  Public for tests.
     *
     * @hidden
     * Internal use only.
     */
    public void setXATransaction(Xid xid, Transaction txn) {
        getNonNullEnvImpl().getTxnManager().registerXATxn(
            xid, txn.getTxn(), false);
    }

    /*
     * XAResource methods.
     */

    @Override
    public void commit(Xid xid, boolean ignore /*onePhase*/)
        throws XAException {

        if (DEBUG) {
            System.out.println("*** commit called " + xid + "/" + ignore);
        }

        if (xid == null) {
            return;
        }

        try {
            checkOpen();
            Txn txn = getXATransactionInternal(xid);
            if (txn == null) {
                throw new XAException
                    ("No transaction found for " + xid + " during commit.");
            }
            removeReferringHandle(new Transaction(this, txn));
            if (txn.isOnlyAbortable()) {
                throw new XAException(XAException.XA_RBROLLBACK);
            }
            txn.commit(xid);
        } catch (DatabaseException DE) {
            throwNewXAException(DE);
        }
        if (DEBUG) {
            System.out.println("*** commit finished");
        }
    }

    @Override
    public void end(Xid xid, int flags)
        throws XAException {

        if (DEBUG) {
            System.out.println("*** end called " + xid + "/" + flags);
        }

        /* flags - One of TMSUCCESS, TMFAIL, or TMSUSPEND. */

        boolean tmFail = (flags & XAResource.TMFAIL) != 0;
        boolean tmSuccess = (flags & XAResource.TMSUCCESS) != 0;
        boolean tmSuspend = (flags & XAResource.TMSUSPEND) != 0;
        if ((!tmFail && !tmSuccess && !tmSuspend) ||
            (tmFail && tmSuccess) ||
            ((tmFail || tmSuccess) && tmSuspend)) {
            throw new XAException(XAException.XAER_INVAL);
        }

        final EnvironmentImpl envImpl = getNonNullEnvImpl();

        if (DEBUG) {
            System.out.println
                ("Transaction for " + Thread.currentThread() + " is " +
                 envImpl.getTxnManager().getTxnForThread());
        }

        Transaction transaction = envImpl.getTxnManager().unsetTxnForThread();
        if (transaction == null) {
            transaction = getXATransaction(xid);
        }
        Txn txn = (transaction != null) ? transaction.getTxn() : null;
        if (txn == null) {
            throw new XAException(XAException.XAER_NOTA);
        }

        if (tmFail) {

            /*
             * Creating the XAFailureException will set the txn to abort-only.
             * This exception stack trace will provide more "cause" information
             * when it is wrapped in an exception that is thrown later, which
             * occurs when an attempt is made to use the txn.
             */
            new XAFailureException(txn);
        }

        if (tmSuspend) {
            if (txn.isSuspended()) {
                throw new XAException(XAException.XAER_PROTO);
            }
            txn.setSuspended(true);
        }

        /* For tmSuccess, do nothing further. */
    }

    @Override
    public void forget(Xid xid)
        throws XAException {

        if (DEBUG) {
            System.out.println("*** forget called");
        }

        throw new XAException(XAException.XAER_NOTA);
    }

    @Override
    public boolean isSameRM(XAResource rm)
        throws XAException {

        if (DEBUG) {
            System.out.println("*** isSameRM called");
        }

        EnvironmentImpl envImpl = null;
        try {
            envImpl = checkOpen();
        } catch (DatabaseException DE) {
            throwNewXAException(DE);
        }

        if (rm == null) {
            return false;
        }

        if (!(rm instanceof XAEnvironment)) {
            return false;
        }

        return envImpl ==
            DbInternal.getNonNullEnvImpl((XAEnvironment) rm);
    }

    @Override
    public int prepare(Xid xid)
        throws XAException {

        if (DEBUG) {
            System.out.println("*** prepare called");
        }

        try {
            checkOpen();
            Transaction txn = getXATransaction(xid);
            if (txn == null) {
                throw new XAException
                    ("No transaction found for " + xid + " during prepare.");
            }
            int ret = txn.getTxn().prepare(xid);

            if (DEBUG) {
                System.out.println("*** prepare returning " + ret);
            }

            /*
             * If this transaction was R/O, then there were no writes.  We'll
             * commit it here because the user doesn't need to (and isn't
             * allowed to either).
             */
            if (ret == XAResource.XA_RDONLY) {
                commit(xid, true);
            }

            return ret;
        } catch (RuntimeException e) {
            throwNewXAException(e);
        }
        return XAResource.XA_OK;        // for compiler
    }

    @Override
    public Xid[] recover(int flags)
        throws XAException {

        if (DEBUG) {
            System.out.println("*** recover called");
        }

        /* flags - One of TMSTARTRSCAN, TMENDRSCAN, TMNOFLAGS. */

        boolean tmStartRScan = (flags & XAResource.TMSTARTRSCAN) != 0;
        boolean tmEndRScan = (flags & XAResource.TMENDRSCAN) != 0;
        if ((tmStartRScan && tmEndRScan) ||
            (!tmStartRScan && !tmEndRScan && flags != TMNOFLAGS)) {
            throw new XAException(XAException.XAER_INVAL);
        }

        /*
         * We don't have to actually do anything with STARTRSCAN or ENDRSCAN
         * since we return the whole set of Xid's to be recovered on each call.
         */
        try {
            final EnvironmentImpl envImpl = checkOpen();

            if (DEBUG) {
                System.out.println("*** recover returning1");
            }

            return envImpl.getTxnManager().XARecover();
        } catch (DatabaseException DE) {
            throwNewXAException(DE);
        }
        return null;                // for compiler
    }

    @Override
    public void rollback(Xid xid)
        throws XAException {

        if (DEBUG) {
            System.out.println("*** rollback called");
        }

        try {
            checkOpen();
            Txn txn = getXATransactionInternal(xid);
            if (txn == null) {
                throw new XAException
                    ("No transaction found for " + xid + " during rollback.");
            }
            removeReferringHandle(new Transaction(this, txn));
            txn.abort(xid);
        } catch (DatabaseException DE) {
            throwNewXAException(DE);
        }

        if (DEBUG) {
            System.out.println("*** rollback returning");
        }
    }

    @Override
    public int getTransactionTimeout()
        throws XAException {

        try {
            return (int) ((getConfig().getTxnTimeout() + 999999L) / 1000000L);
        } catch (Exception DE) {
            throwNewXAException(DE);
        }
        return 0;                // for compiler
    }

    @Override
    public boolean setTransactionTimeout(int timeout) {
        return false;
    }

    @Override
    public void start(Xid xid, int flags)
        throws XAException {

        if (DEBUG) {
            System.out.println("*** start called " + xid + "/" + flags);
        }

        boolean tmJoin = (flags & XAResource.TMJOIN) != 0;
        boolean tmResume = (flags & XAResource.TMRESUME) != 0;

        /* Check flags - only one of TMNOFLAGS, TMJOIN, or TMRESUME. */
        if (xid == null ||
            (tmJoin && tmResume) ||
            (!tmJoin &&
             !tmResume &&
             flags != XAResource.TMNOFLAGS)) {
            throw new XAException(XAException.XAER_INVAL);
        }

        try {
            Transaction txn = getXATransaction(xid);
            TxnManager txnMgr = getNonNullEnvImpl().getTxnManager();

            if (flags == XAResource.TMNOFLAGS) {

                /*
                 * If neither RESUME nor JOIN was set, make sure xid doesn't
                 * exist in allXATxns.  Throw XAER_DUPID if it does.
                 */
                if (txn == null) {
                    if (DEBUG) {
                        System.out.println
                            ("Transaction for XID " + xid + " being created");
                    }

                    txn = beginTransaction(null, null);
                    setXATransaction(xid, txn);

                } else {
                    throw new XAException(XAException.XAER_DUPID);
                }
            } else if (tmJoin) {
                if (txn == null) {
                    throw new XAException(XAException.XAER_NOTA);
                }

                if (txnMgr.getTxnForThread() != null ||
                    txn.getPrepared()) {
                    throw new XAException(XAException.XAER_PROTO);
                }
            } else if (tmResume) {
                if (txn == null) {
                    throw new XAException(XAException.XAER_NOTA);
                }

                if (!txn.getTxn().isSuspended()) {
                    throw new XAException(XAException.XAER_PROTO);
                }
                txn.getTxn().setSuspended(false);
            }

            if (DEBUG) {
                System.out.println
                    ("Setting Transaction for " + Thread.currentThread());
            }
            txnMgr.setTxnForThread(txn);
        } catch (DatabaseException DE) {
            if (DEBUG) {
                System.out.println("*** start exception");
            }
            throwNewXAException(DE);
        }

        if (DEBUG) {
            System.out.println("*** start finished");
        }
    }

    private void throwNewXAException(Exception E)
        throws XAException {

        XAException ret = new XAException(E.toString());
        ret.initCause(E);
        throw ret;
    }
}
