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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * This class is based on {@link StockQuotes} and illustrates use of an
 * HA-aware router (implemented by {@link HARouter}), in conjunction with the
 * {@link com.sleepycat.je.rep.monitor.Monitor Monitor} class, to direct
 * application requests, based upon the type of request (read or write) and the
 * state (Master or Replica) of a node in the replication group.  This example
 * is meant to illustrate how a software load balancer might be integrated with
 * JE HA, where {@code HARouter} plays the role of the load balancer for
 * purposes of the example.
 * <p>
 * Be sure to read the {@link je.rep.quote Example Overview} first to put this
 * example into context.
 * <p>
 * In this example, unlike <code>StockQuotes</code>, only the HARouter has a
 * console associated with it. It accepts commands typed into its console and
 * forwards them as appropriate to the Master and Replicas in the group. The
 * logic for tracking the Master resides in <code>HARouter</code>, and
 * information about the state of the replication group is supplied by the
 * {@link com.sleepycat.je.rep.monitor.Monitor Monitor}. While this example
 * uses just one HARouter instance for the entire group, production
 * applications could use multiple router instances to avoid single points of
 * failure.
 * <p>
 * Each node, which in this example is an instance of
 * <code>RouterDrivenStockQuotes</code>, establishes a server socket on which
 * it can listen for requests from HARouter. The node that is currently the
 * Master will expect both write and read requests from HARouter, while nodes
 * that are Replicas will only expect read requests from the router.
 * <p>
 * The request flow between nodes in this example is shown below.
 * <pre>
 * ------------               Read requests
 * | HARouter |------------------------------------||
 * | Instance |---------------------||             ||
 * ------------                     ||             ||
 *  ||                              ||             ||
 *  || Write requests               ||             ||
 *  \/                              ||             ||
 * ---------------------------      ||             ||
 * | RouterDrivenStockQuotes |      ||             ||
 * | Instance 1: Master      |      ||             ||
 * ---------------------------      \/             ||
 *                ---------------------------      ||
 *                | RouterDrivenStockQuotes |      ||
 *                | Instance 2: Replica     |      ||
 *                ---------------------------      \/
 *                               ---------------------------
 *                               | RouterDrivenStockQuotes |
 *                               | Instance 3: Replica     |
 *                               ---------------------------
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
 * that you must use the name of this class when invoking the JVM. For example,
 * the first node can be started as follows:
 *
 * <pre>
 * java je.rep.quote.RouterDrivenStockQuotes -env /tmp/stockQuotes1 \
 *                                           -nodeName n1 \
 *                                           -nodeHost node.acme.com:5001 \
 *                                           -helperHost node.acme.com:5001
 * </pre>
 *
 * In addition to starting the nodes, you will also need to start the
 * {@link HARouter} as described in its javadoc.
 *
 * @see HARouter
 */
public class RouterDrivenStockQuotes extends StockQuotes {

    RouterDrivenStockQuotes(String[] params) throws Exception {
        super(params);
    }

    /**
     * Overrides the method in the base class to receive requests using the
     * socket connection established by the router instead of prompting the
     * user for console input.
     */
    @Override
    void doWork()
        throws IOException, InterruptedException {

        /*
         * Get socket address on which it can be contacted with requests by
         * the Router.
         */
        InetSocketAddress appSocketAddress =
            HARouter.getAppSocketAddress(repEnv.getRepConfig().
                                         getNodeSocketAddress());
        final ServerSocket serverSocket =
            new ServerSocket(appSocketAddress.getPort());
        System.err.println("Node: " + repEnv.getNodeName() +
                           " joined replication group: " +
                           repEnv.getGroup().getName() +
                           " in state: " + repEnv.getState() + ".\n" +
                           this.getClass().getSimpleName() +
                           " ready to service Router requests at: " +
                           serverSocket + "\n");
        try {
            for (boolean done = false; !done;) {
 
                Socket socket = null;
                BufferedReader in = null;
                PrintStream out = null;

                try {
                    socket = serverSocket.accept();

                    in = new BufferedReader
                        (new InputStreamReader(socket.getInputStream()));

                    String line = getCommandLine(null,
                                                 socket.getInputStream());
                    out = new PrintStream(socket.getOutputStream(), true);
                    done = doCommand(line, out);
                } finally {
                    QuoteUtil.closeSocketAndStreams(socket, in, out);
                }
            }
        } finally {
            serverSocket.close();
        }
    }
 
    public static void main(String[] argv) throws Exception {
        StockQuotes stockQuotes = new RouterDrivenStockQuotes(argv);
        stockQuotes.runExample();
    }
}
