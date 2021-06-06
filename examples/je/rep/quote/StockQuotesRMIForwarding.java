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

package je.rep.quote;

import java.io.PrintStream;
import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.Date;

import com.sleepycat.je.Transaction;
import com.sleepycat.je.rep.ReplicaWriteException;
import com.sleepycat.je.rep.ReplicationNode;
import com.sleepycat.je.rep.StateChangeEvent;
import com.sleepycat.je.rep.StateChangeListener;

/**
 * This example is a small variation on the basic {@link StockQuotes} example.
 * Instead of rejecting update requests made at a Replica's console, it
 * illustrates how RMI could be used to forward write requests to a Master. The
 * example is otherwise identical to <code>StockQuotes</code> and you should
 * read the javadoc associated with it before proceeding with this example. The
 * discussion that follows thus focusses entirely on the RMI based
 * write-forwarding aspects of this example.
 * <p>
 * Each node in this example is an RMI server and hosts an RMI registry. The
 * registry contains exactly one binding associated with the name:
 * {@link StockQuotesRMIForwarding#RMI_NAME RMI_NAME}. The object associated
 * with the RMI binding (an instance of {@link WriteServicesImpl}) makes
 * available all the high level database write operations that are part of the
 * application. When this node is the <code>Master</code>,
 * <code>Replicas</code> will use the remote methods to invoke write operations
 * on it. All nodes are RMI servers, but only the current <code>Master</code>
 * is actually used to serve write requests while it is in the
 * <code>Master</code> state. The Replicas play the role of RMI clients making
 * remote method calls to the Master to foward their write requests.
 *
 * <p>
 * Please review the javadoc in {@link StockQuotes} for a detailed description
 * of the arguments that must be supplied at startup. The only difference is
 * that you must use the name of this class when invoking the Java VM.
 * <p>
 * For example, the first node can be started as follows:
 *
 * <pre>
 * java je.rep.quote.StockQuotesWriteForwarding -env /tmp/stockQuotes1 \
 *                                               -nodeName n1 \
 *                                               -nodeHost node.acme.com:5001 \
 *                                               -helperHost node.acme.com:5001
 * </pre>
 * <p>
 * This instance of the application will therefore use port 5001 for HA, and,
 * by convention, port 5101 (5001 + <code>RMI_PORT_DISPLACEMENT</code>) for
 * the RMI registry. If you are running on multiple machines you may (depending
 * upon your DNS setup) need to specify the
 * <code>java.rmi.server.hostname</code> property to ensure that RMI does not
 * associate loopback addresses with entries in its registry.
 */
public class StockQuotesRMIForwarding extends StockQuotes {

    /* This name is bound to the WriteServices reference in the registry. */
    public static final String RMI_NAME = "StockQuotes";

    /*
     * The displacement from the node's port to the port on which the
     * registry is established. It's just a simple way to find the registry
     * port associated with a node based upon it's port.
     */
    private static final int RMI_PORT_DISPLACEMENT = 100;

    /* The RMI registry associated with this node. */
    private Registry nodeRegistry = null;

    /* The implementation of the write services made avaialbela this node. */
    private final WriteServices writeServices;

    /* The current master as maintained by the Listener. */
    private volatile MasterInfo masterInfo = new MasterInfo();

    /**
     * The Listener used to react to StateChangeEvents. It maintains the name
     * of the current master.
     */
    private class Listener implements StateChangeListener {

        /**
         * The protocol method used to service StateChangeEvent notifications.
         */
        public void stateChange(StateChangeEvent stateChangeEvent)
            throws RuntimeException {


            switch (stateChangeEvent.getState()) {

                case MASTER:
                case REPLICA:
                    String masterName = stateChangeEvent.getMasterNodeName();
                    masterInfo = new MasterInfo(masterName);

                    System.err.println("Master: " + masterName +
                                       " at " + new Date());
                    break;

                default:
                    masterInfo = new MasterInfo();
                    System.err.println("Unknown master. " +
                                       " Node state: " +
                                       stateChangeEvent.getState());
                    break;
            }
        }
    }

    /**
     * Updates the stock price. Forward the write request, if the node is not
     * currently the master, or if the node changes status while a transaction
     * is in progress.
     *
     * @param line the validated command line
     *
     * @param printStream the output stream
     *
     * @throws InterruptedException
     */
    @Override
    void updateStock(final String line, final PrintStream printStream)
        throws InterruptedException {

        final Quote quote = QuoteUtil.parseQuote(line);

        if (repEnv.getState().isReplica()) {
            forwardStockUpdate(quote);
            return;
        }

        new RunTransaction(repEnv, printStream) {

            @Override
            public void doTransactionWork(Transaction txn) {
                dao.quoteById.put(txn, quote);
                /* Output local indication of processing. */
                System.out.println(repEnv.getNodeName() +
                                   " processed update request: " + line);
            }

            @Override
            public void onReplicaWrite(ReplicaWriteException replicaWrite) {
                /* Forward to the current master */
                forwardStockUpdate(quote);
            }
        }.run(false /*readOnly*/);
    }

    /**
     * An update request on the replica. Forward it to the current master, if
     * we have one that's reachable.
     *
     * @param stockUpdateLine the command to forward
     *
     * @param printStream the stream used to capture the output from the
     * forwarded request
     */
    private void forwardStockUpdate(Quote quote) {
        try {

            if (masterInfo.name == null) {
                System.out.println("Could not update:" +  quote.stockSymbol +
                                   " Master is unknown. ");
                return;
            }

            masterInfo.reference.update(quote);
            System.out.println(repEnv.getNodeName() + " forwarded " +
                               quote.stockSymbol + " update to " +
                               masterInfo.name);

        } catch (RemoteException e) {

            if (e.getCause() instanceof ReplicaWriteException) {
                forwardStockUpdate(quote);
                return;
            }
            System.out.println("Could not connect to master: " +
                               masterInfo.name + " Exception: " + e);
        }
    }

    /**
     * The constructor. It sets up the RMI registry and binds the remote
     * reference.
     *
     * @see StockQuotes#StockQuotes(String[])
     */
    private StockQuotesRMIForwarding(String[] params)
        throws Exception {

        super(params);

        nodeRegistry = LocateRegistry.
            createRegistry(repConfig.getNodePort() + RMI_PORT_DISPLACEMENT);
        writeServices = new WriteServicesImpl(System.out);
        UnicastRemoteObject.exportObject(writeServices, 0);
        nodeRegistry.rebind(RMI_NAME, writeServices);
    }

    /**
     * Sets up the state change listener.
     *
     * @throws InterruptedException
     */
    @Override
    void initialize()
        throws InterruptedException {

        super.initialize();
        /* Track Master state changes, so we can forward appropriately. */
        repEnv.setStateChangeListener(new Listener());
    }

    /**
     * Performs the RMI associated cleanup so that the RMI serve can be
     * shutdown cleanly.
     */
    @Override
    public void quit(PrintStream out) {
        super.quit(out);
        try {
            UnicastRemoteObject.unexportObject(writeServices, true);
            nodeRegistry.unbind(RMI_NAME);
            UnicastRemoteObject.unexportObject(nodeRegistry,true);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public static void main(String[] argv)
        throws Exception {

        StockQuotes stockQuotes = new StockQuotesRMIForwarding(argv);
        stockQuotes.runExample();
    }

    /**
     * The class supplies the RMI implementation of the write methods.
     */
    public class WriteServicesImpl implements WriteServices {
        final PrintStream printStream;

        public WriteServicesImpl(PrintStream printStream) {

            super();
            this.printStream = printStream;
        }

        private static final long serialVersionUID = 1L;

        /**
         * The update operation invoked by a Replica on this Master.
         *
         * <p> Note that this method is executed in an RMI thread and does not
         * handle the environment failure level exceptions:
         * <code>InsufficientLogException</code> and
         * <code>RollbackException</code> exception in order to keep the
         * example simple. Production code would handle the exception here and
         * coordinate with the main thread of control and other RMI threads to
         * take corrective actions and re-estabblish the environment and
         * database handles.
         */
        public void update(final Quote quote)
            throws RemoteException {

            try {
                new RunTransaction(repEnv, printStream) {

                    @Override
                    public void doTransactionWork(Transaction txn) {
                        dao.quoteById.put(txn, quote);
                        /* Output local indication of processing. */
                        System.out.println
                        (repEnv.getNodeName() +
                         " processed remote update request. " +
                         " Stock:" + quote.stockSymbol +
                         " Price:" + quote.lastTrade);
                    }

                    @Override
                    public void onReplicaWrite(ReplicaWriteException rwe) {
                        /* Attempted modification while in the replica state. */
                        throw rwe;
                    }
                }.run(false /*readOnly*/);
            } catch (InterruptedException e) {
                throw new RemoteException("Update for stock:" +
                                          quote.stockSymbol +
                                          " interrupted.", e);
            } catch (ReplicaWriteException e) {
                String errorMessage = repEnv.getNodeName() +
                " is not currently the master. Perform the update at" +
                " the node that's currently the master:" + masterInfo.name;
                throw new RemoteException("Update for stock:" +
                                          quote.stockSymbol +
                                          " failed. " + errorMessage, e);
            }
        }
    }

    /**
     * Internal class used to treat the master name and remote reference as
     * a fixed pair.
     */
    private class MasterInfo {
        /* The node name of the master*/
        final String name;

        /* The remote reference to the Master with the above name */
        final WriteServices reference;

        public MasterInfo(String masterName) {
            this.name = masterName;
            ReplicationNode masterNode = repEnv.getGroup().getMember(name);
            Registry currentMasterRegistry;
            try {
                currentMasterRegistry = LocateRegistry.
                    getRegistry(masterNode.getHostName(),
                                masterNode.getPort() + RMI_PORT_DISPLACEMENT);
                reference = (WriteServices)currentMasterRegistry.
                    lookup(RMI_NAME);
            } catch (RemoteException e) {
                throw new RuntimeException(e);
            } catch (NotBoundException e) {
                throw new RuntimeException(e);
            }
        }

        public MasterInfo() {
            name = null;
            reference = null;
        }
    }

    /* Define the remote interface. */
    public interface WriteServices extends Remote {

        /**
         * The "write" operation which will update the price associated with
         * the Stock.
         */
        void update(Quote quote) throws RemoteException;
    }
}
