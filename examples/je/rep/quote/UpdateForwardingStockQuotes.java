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

import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.util.Date;

import com.sleepycat.je.Transaction;
import com.sleepycat.je.rep.ReplicaWriteException;
import com.sleepycat.je.rep.ReplicationGroup;
import com.sleepycat.je.rep.StateChangeEvent;
import com.sleepycat.je.rep.StateChangeListener;

/**
 * This class is based on {@link RouterDrivenStockQuotes} and illustrates use
 * of an HA unaware router (implemented by {@link SimpleRouter}), that load
 * balances requests (both read and write) across all the nodes in a
 * replication group.  This example is meant to illustrate how a load balancer
 * appliance might fit into the JE HA architecture, where {@code SimpleRouter}
 * plays the role of the load balancer appliance for purposes of the example.
 * <p>
 * Be sure to read the {@link je.rep.quote Example Overview} first to put this
 * example into context.
 * <p>
 * The router is unaware of the state (Master or Replica) of each node, or the
 * type (read or write) of the request.  Nodes use the {@link
 * com.sleepycat.je.rep.StateChangeListener StateChangeListener} to track the
 * node that is currently the master and redirect write requests to it. That
 * is, unlike the {@link RouterDrivenStockQuotes} example, it's the nodes and
 * not the router that keeps track of the current master.
 * <p>
 * In this example, unlike <code>StockQuotes</code>, only the
 * {@link SimpleRouter} has a console associated with it. It accepts commands
 * typed into its console and forwards them as appropriate to the nodes in the
 * group. The logic for tracking the Master resides in each node, and is
 * supplied by the {@link com.sleepycat.je.rep.StateChangeListener
 * StateChangeListener}.
 * <p>
 * Each node, which in this example is an instance of
 * <code>UpdateForwardingStockQuotes</code>, establishes a server socket on
 * which it can listen for requests from <code>SimpleRouter</code>. Read
 * requests are processed directly by the node. Write requests are redirected
 * to the current master and the result is communicated back to
 * <code>SimpleRouter</code>.
 * <p>
 * The request flow between nodes in this example is shown below.
 * <pre>
 * ----------------       Read and Write requests
 * | SimpleRouter |------------------------------------||
 * | Instance     |---------------------||             ||
 * ----------------      ||             ||             ||
 *                       ||             ||             ||
 *                       \/             ||             ||
 * -------------------------------      ||             ||
 * | UpdateForwardingStockQuotes |      ||             ||
 * | Instance 1: Master          |      ||             ||
 * -------------------------------      \/             ||
 *   /\           -------------------------------      ||
 *   ||           | UpdateForwardingStockQuotes |      ||
 *   ||---------- | Instance 2: Replica         |      ||
 *   || Write     -------------------------------      \/
 *   || requests                 -------------------------------
 *   ||                          | UpdateForwardingStockQuotes |
 *   ||--------------------------| Instance 3: Replica         |
 *                               -------------------------------
 *
 *                                       ...more Replica instances...
 * </pre>
 * <p>
 * This example is intended to be illustrative. It forwards requests as text,
 * and receives responses in text form. Actual applications may for example,
 * forward HTTP requests, or use some other application level network protocol
 * to forward such requests.
 * <p>
 * Please review the javadoc in {@link StockQuotes} for a detailed description
 * of the arguments that must be supplied at startup. The only difference is
 * that you must use the name of this class when invoking the java vm.
 * <p>
 * For example, the first node can be started as follows:
 *
 * <pre>
 * java je.rep.quote.UpdateForwardingStockQuotes -env /tmp/stockQuotes1 \
 *                                               -nodeName n1 \
 *                                               -nodeHost node.acme.com:5001 \
 *                                               -helperHost node.acme.com:5001
 * </pre>
 * <p>
 * This instance of the application will therefore use port 5001 for HA, and,
 * by convention, port 5101 (5001 + <code>HARouter.APP_PORT_DISPLACEMENT</code>)
 * for application messages sent to it.
 * <p>
 * In addition to starting the nodes, you will also need to start the
 * {@link SimpleRouter} as described in its javadoc.
 *
 * @see SimpleRouter
 */
public class UpdateForwardingStockQuotes extends RouterDrivenStockQuotes {

    /* The current master as maintained by the Listener. */
    private volatile String currentmasterName = null;

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
                    currentmasterName = stateChangeEvent.getMasterNodeName();
                    System.err.println("Master: " + currentmasterName +
                                       " at " + new Date());
                    break;

                default:
                    currentmasterName = null;
                    System.err.println("Unknown master. " +
                                       " Node state: " +
                                       stateChangeEvent.getState());
                    break;
            }
        }
    }

    /**
     * Returns a socket to the UpdateRequestProcessor associated with the
     * current master.
     *
     * @return the socket to the UpdateRequestProcessor associated with the
     * master, or null if the node does not know if a current master.
     */
    private InetSocketAddress getUpdateRequestProcessorSocket() {
        final String master = currentmasterName;

        if (master == null) {
            return null;
        }

        ReplicationGroup group = repEnv.getGroup();

        InetSocketAddress nodeSocketAddress =
            group.getMember(master).getSocketAddress();

        return HARouter.getAppSocketAddress(nodeSocketAddress);
    }

    /**
     * Updates the stock price. Forward the write request, if the node is not
     * currently the master, or if the node changes status while a transaction
     * is in progress.
     *
     * @param line the command line
     *
     * @param printStream the output stream for message results
     *
     * @throws InterruptedException
     */
    @Override
    void updateStock(final String line, final PrintStream printStream)
        throws InterruptedException {

        if (repEnv.getState().isReplica()) {
            forwardStockUpdate(line, printStream);
            return;
        }

        final Quote quote = QuoteUtil.parseQuote(line);
        if (quote == null) {
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
                forwardStockUpdate(line, printStream);
            }
        }.run(false /*readOnly*/);
    }

    /**
     *
     * An update request on the replica. Forward it to the current master, if
     * we have one that's reachable.
     *
     * @param stockUpdateLine the command to forward
     *
     * @param printStream the stream used to capture the output from the
     * forwarded request
     */
    void forwardStockUpdate(String stockUpdateLine, PrintStream printStream) {
        try {
            QuoteUtil.forwardRequest(getUpdateRequestProcessorSocket(),
                                     stockUpdateLine,
                                     printStream);
            System.out.println(repEnv.getNodeName() + " forwarded " +
                               stockUpdateLine + " to " + currentmasterName);

        } catch (IOException e) {
            printStream.println("Could not connect to master: " +
                                currentmasterName + " Exception: " + e);
        }
    }

    /**
     * The constructor. It forwards to the base StockQuotes for initialization
     * and sets up the Listener.
     *
     * @see StockQuotes#StockQuotes(String[])
     */
    private UpdateForwardingStockQuotes(String[] params)
        throws Exception {

        super(params);
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

    public static void main(String[] argv)
        throws Exception {

        StockQuotes stockQuotes = new UpdateForwardingStockQuotes(argv);
        stockQuotes.runExample();
    }
}
