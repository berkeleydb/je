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
import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.List;

import je.rep.quote.Command.InvalidCommandException;

/**
 * This example illustrates the use of a simple HA-unaware router that is used
 * in conjunction with {@link UpdateForwardingStockQuotes}. The router is
 * unaware of the state (<code>Master</code> or <code>Replica</code>) of each
 * node and simply forwards requests entered at the router's console to each
 * node in the group in Round Robin fashion.
 * <p>
 * The <code>UpdateForwardingStockQuotes</code> instance will in turn, if
 * necessary, forward any write requests to the current master and return the
 * results back to SimpleRouter. <code>UpdateForwardingStockQuotes</code>
 * instances do not have their own consoles, they only service requests
 * delivered over the network by this router.
 * <p>
 * SimpleRouter takes <code>host:port</code> pairs as arguments, one pair for
 * each instance of the <code>UpdateForwardingStockQuotes</code> application.
 * The port numbers in this case are application, not HA, port numbers on which
 * the <code>UpdateForwardingStockQuotes</code> application listens for
 * application messages forwarded by <code>SimpleRouter</code>. They must
 * therefore be different from the ports used internally by HA, that is, from
 * the HA port numbers specified as arguments to
 * <code>UpdateForwardingStockQuotes</code>. The application port number is
 * computed in this example by adding
 * <code>HARouter.APP_PORT_DISPLACEMENT</code> (default value 100) to the HA
 * port number associated with the node. So, if node "n1" uses port 5001 for
 * HA, it must (based upon the conventions used in these examples) use port
 * 5101, for application level communication.
 * <p>
 * SimpleRouter can thus be invoked as follows:
 *
 * <pre>
 * java je.rep.quote.SimpleRouter node.acme.com:5101 node.acme.com:5102 node.acme.com:5103
 * </pre>
 *
 * for a three node group. In this case, the applications will use ports 5101,
 * through 5103 for application messages, while HA will use ports 5001 through
 * 5003.
 *
 * <p>
 * SimpleRouter and UpdateForwardingStockQuotes can be started in any order.
 *
 * @see UpdateForwardingStockQuotes
 */
public class SimpleRouter {

    /*
     * List of addresses on which the application is listening for routed
     * requests. All access to this list must be synchronized via groupLock.
     */
    private static final List<InetSocketAddress> appAddresses=
        new LinkedList<InetSocketAddress>();

    /*
     * Tracks the position in the appAddresses list to which a read request was
     * last dispatched.
     */
    private int lastReadAddress = 0;

    /**
     * Round robins through the list of App dispatch addresses returning each
     * address in sequence.
     *
     * @return the next dispatch address
     */
    private InetSocketAddress getNextDispatchAddress() {
        lastReadAddress++;
        if (lastReadAddress == appAddresses.size()) {
            lastReadAddress = 0;
        }
        return appAddresses.get(lastReadAddress);
    }

    /**
     * Routes a quit request to all the nodes in the replication group, thus
     * shutting them down.
     */
    private void routeQuit() {
        for (InetSocketAddress appAddress : appAddresses) {
            try {
                QuoteUtil.forwardRequest(appAddress, "quit", System.err);
            } catch (IOException e) {
                // Ignore exceptions during shutdown
            }
        }
    }

    /**
     * Routes a request in round-robin fashion to each of the nodes in the
     * replication group. It gives up if none of the nodes are available.
     *
     * @param line the text of the read request
     */
    private void routeCommand(String line) {
        IOException lastException = null;
        int retries = appAddresses.size();
        while (retries-- > 0) {
            try {
                QuoteUtil.forwardRequest(getNextDispatchAddress(),
                                         line,
                                         System.err);
                return;
            } catch (IOException e) {
                lastException = e;

                /*
                 * A more realistic implementation would remove the node
                 * from the list and arrange for it to be returned when it
                 * was up again. But this code is only intended to be a demo.
                 */
                continue;
            }
        }
        System.err.println("None of the nodes at:" + appAddresses.toString() +
                           " were available to service the request." +
                           " Exception: " + lastException);
    }

    /**
     * The central method used to sort out and dispatch individual requests.
     *
     * @throws Exception
     */
    void doWork() throws Exception {
        BufferedReader stdin =
            new BufferedReader(new InputStreamReader(System.in));
        while (true) {
            String line = QuoteUtil.promptAndRead
                ("SRouter", null, false, System.out, stdin);

            if (line == null) {
                return;
            }

            try {
                switch (Command.getCommand(line)) {

                case NONE:
                    break;

                case PRINT:
                case UPDATE:
                    routeCommand(line);
                    break;

                case QUIT:
                    /* Broadcast it to all the nodes. */
                    routeQuit();
                    return;

                default:
                    StockQuotes.usage();
                    break;
                }
            } catch (InvalidCommandException e) {
                System.err.println(e.getMessage());
                continue;
            }
        }
    }

    /**
     * Creates a Router instance and initializes it using the command line
     * arguments passed into it.
     *
     * @param argv command line arguments.
     */
    SimpleRouter(String[] argv) {
        parseParams(argv);
    }

    /**
     * Parse the command line parameters and initialize the router with
     * configuration information about the replication group and the
     * <code>Monitor</code> that is running as part of this Router.
     */
    private void parseParams(String[] argv)
        throws IllegalArgumentException {

        if (argv.length == 0) {
            usage("Insufficient arguments");
        }

        for (String hostPort : argv) {

            int portStart = hostPort.indexOf(':');
            if (portStart < 0) {
                usage("Bad argument:" + hostPort);
            }
            String hostname = hostPort.substring(0,portStart);
            int port;
            port = Integer.parseInt(hostPort.substring(portStart+1));

            appAddresses.add(new InetSocketAddress(hostname, port));
        }
    }

     /**
      * Provides invocation usage help information in response to an error
      * condition.
      *
      * @param message an explanation of the condition that provoked the
      *                display of usage information.
      */
     void usage(String message){
         System.out.println();
         System.out.println(message);
         System.out.println();
         System.out.print("usage: " + getClass().getName());
         System.out.println(" <host:port> [,<host:port>]+");
         System.out.
         println("\t <host:port> the hostname and port number pair, e.g. " +
                 "foo.bar.com:6000, on which the application is listening " +
                 "for forwarded requests.");
         System.exit(0);
     }

     public static void main(String[] argv) throws Exception {
         final SimpleRouter router = new SimpleRouter(argv);
         router.doWork();
     }
}
