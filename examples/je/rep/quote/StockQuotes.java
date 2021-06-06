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
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.concurrent.TimeUnit;

import je.rep.quote.Command.InvalidCommandException;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Durability;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.OperationFailureException;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.rep.InsufficientLogException;
import com.sleepycat.je.rep.NetworkRestore;
import com.sleepycat.je.rep.NetworkRestoreConfig;
import com.sleepycat.je.rep.ReplicaWriteException;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicationConfig;
import com.sleepycat.je.rep.RollbackException;
import com.sleepycat.je.rep.TimeConsistencyPolicy;
import com.sleepycat.je.rep.UnknownMasterException;
import com.sleepycat.persist.EntityCursor;
import com.sleepycat.persist.EntityStore;

/**
 * The most basic demonstration of a replicated application. It's intended to
 * help gain an understanding of basic HA concepts and demonstrate use of the
 * HA APIs to create a replicated environment and issue read and write
 * transactions.
 * <p>
 * Be sure to read the {@link je.rep.quote Example Overview} first to put this
 * example into context.
 * <p>
 * The program can be used to start up multiple stock quote servers supplying
 * the following arguments:
 *
 * <pre>
 * java je.rep.quote.StockQuotes -env &lt;environment home&gt; \
 *                               -nodeName &lt;nodeName&gt; \
 *                               -nodeHost &lt;hostname:port&gt; \
 *                               -helperHost &lt;hostname:port&gt;
 * </pre>
 *
 * The argument names resemble the {@link ReplicationConfig} names to draw
 * attention to the connection between the program argument names and
 * ReplicationConfig APIs.
 *
 * <pre>
 *  -env        a pre-existing directory for the replicated JE environment
 *  -nodeName   the name used to uniquely identify this node in the replication
 *  -nodeHost   the unique hostname, port pair for this node
 *  -helperHost the hostname, port pair combination for the helper node. It's
 *              the same as the nodeHost only if this node is intended to
 *              become the initial Master, during the formation of the
 *              replication group.
 * </pre>
 *
 * A typical demo session begins with a set of commands such as the following
 * to start each node. The first node can be started as below:
 *
 * <pre>
 * java je.rep.quote.StockQuotes -env dir1 -nodeName n1 \
 *                               -nodeHost node.acme.com:5001 \
 *                               -helperHost node.acme.com:5001
 * </pre>
 *
 * Note that the <code>helperHost</code> and the <code>nodeHost</code> are the
 * same, since it's the first node in the group. HA uses this fact to start a
 * brand new replication group of size one, with this node as the master if
 * there is no existing environment in the environment directory
 * <code>dir1</code>.
 * <p>
 * Nodes can be added to the group by using a variation of the above. The
 * second and third node can be started as follows:
 *
 * <pre>
 * java je.rep.quote.StockQuotes -env dir2 -nodeName n2 \
 *                               -nodeHost node.acme.com:5002 \
 *                               -helperHost node.acme.com:5001
 *
 * java je.rep.quote.StockQuotes -env dir3 -nodeName n3 \
 *                               -nodeHost node.acme.com:5003 \
 *                               -helperHost node.acme.com:5002
 * </pre>
 *
 * Note that each node has its own unique node name, and a distinct directory
 * for its replicated environment. This and any subsequent nodes can use the
 * first node as a helper to get itself going. In fact, you can pick any node
 * already in the group to serve as a helper. So, for example when adding the
 * third node, node 2 or node 1, could serve as helper nodes. The helper nodes
 * simply provide a mechanism to help a new node get itself admitted into the
 * group. The helper node is not needed once a node becomes part of the group.
 * <p>
 * When initially running the example, please use a group of at least three
 * nodes.  A two node group is a special case, and it is best to learn how to
 * run larger groups first.  For more information, see
 * <a href="{@docRoot}/../ReplicationGuide/lifecycle.html#twonode">
 * Two-Node Replication Groups</a>.  When initially creating the nodes, it is
 * also important to start the master first.
 * <p>
 * But once the nodes have been created, the order in which the nodes are
 * started up does not matter. It minimizes the initial overall group startup
 * time to have the master (the one where the <code>helperHost</code> and the
 * <code>nodeHost</code> are the same) node started first, since the master
 * initializes the replicated environment and is ready to start accepting and
 * processing commands even as the other nodes concurrently join the group.
 * <p>
 * The above commands start up a group with three nodes all running locally on
 * the same machine. You can start up nodes on different machines connected by
 * a TCP/IP network by executing the above commands on the respective machines.
 * It's important in this case that the clocks on these machines, be reasonably
 * synchronized, that is, they should be within a couple of seconds of each
 * other. You can do this manually, but it's best to use a protocol like <a
 * href="http://www.ntp.org/">NTP</a> for this purpose.
 * <p>
 * Upon subsequent restarts the nodes will automatically hold an election and
 * select one of the nodes in the group to be the master. The choice of master
 * is made visible by the master/replica prompt that the application uses to
 * make the distinction clear. Note that at least a simple majority of nodes
 * must be started before the application will respond with a prompt because
 * it's only after a simple majority of nodes is available that an election can
 * be held and a master elected. For a two node group, both nodes must be
 * started before an election can be held.
 * <p>
 * Commands are submitted directly at the command prompt in the console
 * established by the application at each node. Update commands are only
 * accepted at the console associated with the current master, identified by
 * the <i>master</i> prompt as below:
 *
 * <pre>StockQuotes-2 (master)&gt;</pre>
 *
 * After issuing a few commands, you may want to experiment with shutting down
 * or killing some number of the replicated environments and bringing them back
 * up to see how the application behaves.
 * <p>
 * If you type stock updates at an application that is currently running as a
 * replica node, the update is refused and you must manually re-enter the
 * updates on the console associated with the master. This is of course quite
 * cumbersome and serves as motivation for the subsequent examples.
 * <p>
 * As shown below, there is no routing of requests between nodes in this
 * example, which is why write requests fail when they are issued on a Replica
 * node.
 * <pre>
 * -----------------------
 * | StockQuotes         | Read and Write requests both succeed,
 * | Instance 1: Master  | because this is the Master.
 * -----------------------
 *
 *      -----------------------
 *      | StockQuotes         | Read requests succeed,
 *      | Instance 2: Replica | but Write requests fail on a Replica.
 *      -----------------------
 *
 *           -----------------------
 *           | StockQuotes         | Read requests succeed,
 *           | Instance 3: Replica | but Write requests fail on a Replica.
 *           -----------------------
 *
 *               ...more Replica instances...
 * </pre>
 * <p>
 * See {@link UpdateForwardingStockQuotes} for an example that uses
 * {@link SimpleRouter}, along with application supplied inter-node request
 * routing to direct write requests to the master.
 * <p>
 * See {@link RouterDrivenStockQuotes} along with {@link HARouter}for an
 * example that uses an external router built using the
 * {@link com.sleepycat.je.rep.monitor.Monitor Monitor} to route write
 * requests externally to the master and provide primitive load balancing
 * across the nodes in the replication group.
 */
public class StockQuotes {
    private static final String STORE_NAME = "Quotes";

    /* The name of the replication group used by this application. */
    static final String REP_GROUP_NAME = "StockQuotesRepGroup";

    static DataAccessor dao = null;
    static EntityStore store = null;
    private File envHome;
    EnvironmentConfig envConfig;

    final ReplicationConfig repConfig;
    ReplicatedEnvironment repEnv;

    /* The maximum number of times to retry handle creation. */
    private static int REP_HANDLE_RETRY_MAX = 5;

    /**
     * Updates the stock price. This command can only be accomplished on the
     * master. The method handles all transaction related exceptions and
     * retries the update if appropriate. All environment invalidating
     * exceptions are propagated up to the caller.
     *
     * @param line the command line
     *
     * @param printStream the output stream for messages
     *
     * @throws InterruptedException
     */
    void updateStock(final String line, final PrintStream printStream)
        throws InterruptedException {

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
                /* Attempted a modification while in the replica state. */
                printStream.println
                    (repEnv.getNodeName() +
                     " is not currently the master.  Perform the update at" +
                     " the node that's currently the master.");
            }
        }.run(false /*readOnly*/);
    }

    /**
     * Parses the command and dispatches it to the method that implements the
     * command.
     *
     * @param commandLine the command to be executed
     *
     * @param out the stream to which the results are written
     *
     * @return true if we input has been exhausted, or a Quit was encountered
     *
     * @throws InterruptedException
     */
    final boolean doCommand(String commandLine, PrintStream out)
        throws InterruptedException {

        if (commandLine == null) {
            return true;
        }
        try {
            Command command = Command.getCommand(commandLine);
            switch (command) {
            case NONE:
                break;
            case PRINT:
                printStocks(out);
                break;
            case QUIT:
                quit(out);
                return true;
            case UPDATE:
                updateStock(commandLine, out);
                break;
            }
        } catch (InvalidCommandException e) {
            out.println(e.getMessage());
        }
        return false;
    }

    /**
     * Prints all stocks in the database. Retries the operation a fixed number
     * of times, before giving up. Note that this method can be invoked on
     * either the master or the replica.
     *
     * @throws InterruptedException
     */
    private void printStocks(final PrintStream out)
        throws InterruptedException {

        new RunTransaction(repEnv, out) {

            @Override
            public void doTransactionWork(Transaction txn) {

                final EntityCursor<Quote> quotes =
                    dao.quoteById.entities(txn, null);
                try {
                    out.println("\tSymbol\tPrice");
                    out.println("\t======\t=====");

                    int count = 0;
                    for (Quote quote : quotes) {
                        out.println("\t" +  quote.stockSymbol +
                                    "\t" + quote.lastTrade);
                        count++;
                    }
                    out.println("\n\t" + count + " stock"
                                + ((count == 1) ? "" : "s") +
                                " listed.\n");
                } finally {
                    quotes.close();
                }
            }

        }.run(true /*readOnly*/);

        /* Output local indication of processing. */
        System.out.println(repEnv.getNodeName() + " processed print request");
    }

    /**
     * Runs the example. It handles environment invalidating exceptions,
     * re-initializing the environment handle and the dao when such an
     * exception is encountered.
     *
     * @throws Exception to propagate any IO or Interrupt exceptions
     */
    final void runExample()
        throws Exception {

        while (true) {

            try {
                initialize();
                doWork();
                shutdown();
                return;
                /* Exit the application. */
            } catch (InsufficientLogException insufficientLog) {
                /* Restore the log files from another node in the group. */
                NetworkRestore networkRestore = new NetworkRestore();
                networkRestore.execute(insufficientLog,
                                       new NetworkRestoreConfig());
                continue;
            } catch (RollbackException rollback) {

                /*
                 * Any transient state that is dependent on the environment
                 * must be re-synchronized to account for transactions that
                 * may have been rolled back.
                 */
                continue;
            } finally {
                if (repEnv != null) {
                    repEnv.close();
                    repEnv = null;
                }
            }
        }
    }

    /**
     * The command execution loop.
     *
     * @throws Exception
     */
    void doWork()
        throws Exception {

        boolean done = false;

        while (!done) {
            String line = getCommandLine(System.out, System.in);
            done = doCommand(line, System.out);
        }
    }

    /**
     * Generate prompt, read input. Valid inputs are:
     *
     *  1) quit (tells this local node to quit)
     *  2) print
     *  3) stockSymbol stockValue
     *
     * @param inputStream
     */
    String getCommandLine(PrintStream promptStream, InputStream inputStream)
        throws IOException {
        BufferedReader stdin =
            new BufferedReader(new InputStreamReader(inputStream));
        return QuoteUtil.promptAndRead("StockQuotes",
                                        repEnv.getNodeName(),
                                        repEnv.getState().isMaster(),
                                        promptStream,
                                        stdin);
    }

    /**
     * Shuts down the application. If this node was the master, then some other
     * node will be elected in its place, if a simple majority of nodes
     * survives this shutdown.
     */
    void shutdown() {

        store.close();
        store = null;

        repEnv.close();
        repEnv = null;
    }

    /**
     * The StockQuotes example.
     */
    StockQuotes(String[] params)
        throws Exception  {

        /*
         * Set envHome and generate a ReplicationConfig. Note that
         * ReplicationConfig and EnvironmentConfig values could all be
         * specified in the je.properties file, as is shown in the properties
         * file included in the example.
         */
        repConfig = new ReplicationConfig();

        /* Set consistency policy for replica. */
        TimeConsistencyPolicy consistencyPolicy = new TimeConsistencyPolicy
            (1, TimeUnit.SECONDS, /* 1 sec of lag */
             3, TimeUnit.SECONDS  /* Wait up to 3 sec */);
        repConfig.setConsistencyPolicy(consistencyPolicy);

        /* Wait up to two seconds for commit acknowledgments. */
        repConfig.setReplicaAckTimeout(2, TimeUnit.SECONDS);
        parseParams(params);

        /*
         * A replicated environment must be opened with transactions enabled.
         * Environments on a master must be read/write, while environments
         * on a client can be read/write or read/only. Since the master's
         * identity may change, it's most convenient to open the environment
         * in the default read/write mode. All write operations will be
         * refused on the client though.
         */
        envConfig = new EnvironmentConfig();
        envConfig.setTransactional(true);
        Durability durability =
            new Durability(Durability.SyncPolicy.WRITE_NO_SYNC,
                           Durability.SyncPolicy.WRITE_NO_SYNC,
                           Durability.ReplicaAckPolicy.SIMPLE_MAJORITY);
        envConfig.setDurability(durability);
        envConfig.setAllowCreate(true);
    }

    /**
     * Initializes the Environment, entity store and data accessor used by the
     * example. It's invoked when the application is first started up, and
     * subsequently, if the environment needs to be re-established due to an
     * exception.
     *
     * @throws InterruptedException
     */
    void initialize()
        throws InterruptedException {

        /* Initialize the replication handle. */
        repEnv = getEnvironment();

        /*
         * The following two operations -- opening the EntityStore and
         * initializing it by calling EntityStore.getPrimaryIndex -- don't
         * require an explicit transaction because they use auto-commit
         * internally.  A specialized RunTransaction class for auto-commit
         * could be defined, but for simplicity the RunTransaction class is
         * used here and the txn parameter is simply ignored.
         */
        new RunTransaction(repEnv, System.out) {

            @Override
            public void doTransactionWork(Transaction txn) {
                /* Initialize the entity store. */
                store = QuoteUtil.openEntityStore(repEnv, STORE_NAME);
            }

            @Override
            public void onRetryFailure
                (OperationFailureException lastException) {

                /* Restart the initialization process. */
                throw lastException;
            }
        }.run(false /*readOnly*/);

        new RunTransaction(repEnv, System.out) {

            @Override
            public void doTransactionWork(Transaction txn) {

                /* Initialize the data access object. */
                dao = new DataAccessor(store);
            }

            @Override
            public void onRetryFailure
                (OperationFailureException lastException) {

                /* Restart the initialization process. */
                throw lastException;
            }
        }.run(false /*readOnly*/);
    }

    /**
     * Implements the "quit" command. Subclasses can override to take
     * additional cleanup measures.
     */
    public void quit(PrintStream out) {
        out.println(repEnv.getNodeName() + " exited.");
    }

    /**
     * Creates the replicated environment handle and returns it. It will retry
     * indefinitely if a master could not be established because a sufficient
     * number of nodes were not available, or there were networking issues,
     * etc.
     *
     * @return the newly created replicated environment handle
     *
     * @throws InterruptedException if the operation was interrupted
     */
    ReplicatedEnvironment getEnvironment()
        throws InterruptedException {

        DatabaseException exception = null;

        /*
         * In this example we retry REP_HANDLE_RETRY_MAX times, but a
         * production HA application may retry indefinitely.
         */
        for (int i = 0; i < REP_HANDLE_RETRY_MAX; i++) {
            try {
                return new ReplicatedEnvironment(envHome,
                                                 repConfig,
                                                 envConfig);

            } catch (UnknownMasterException unknownMaster) {
                exception = unknownMaster;

                /*
                 * Indicates there is a group level problem: insufficient nodes
                 * for an election, network connectivity issues, etc. Wait and
                 * retry to allow the problem to be resolved.
                 */
                System.err.println("Master could not be established. " +
                                   "Exception message:" +
                                   unknownMaster.getMessage() +
                                   " Will retry after 5 seconds.");
                Thread.sleep(5 * 1000);

                continue;
            }
        }
        /* Failed despite retries. */
        if (exception != null) {
            throw exception;
        }
        /* Don't expect to get here. */
        throw new IllegalStateException("Failed despite retries");
    }

    /**
     * Parse the command line parameters for a replication node and set up
     * any configuration parameters.
     */
    void parseParams(String[] argv)
        throws IllegalArgumentException {

        int argc = 0;
        int nArgs = argv.length;

        if (nArgs == 0) {
            usage("-env, -nodeName, -nodeHost, and -helperHost are required.");
        }
        String nodeName = null;
        String nodeHost = null;
        while (argc < nArgs) {
            String thisArg = argv[argc++];

            if (thisArg.equals("-env")) {
                if (argc < nArgs) {
                    envHome = new File(argv[argc++]);
                } else {
                    usage("-env requires an argument");
                }
            } else if (thisArg.equals("-nodeName"))  {
                /* the node name */
                if (argc < nArgs) {
                    nodeName = argv[argc++];
                    repConfig.setNodeName(nodeName);
                } else {
                    usage("-nodeName requires an argument");
                }
            }  else if (thisArg.equals("-nodeHost")) {
                /* The node hostname, port pair. */
                nodeHost = argv[argc++];
                if (argc <= nArgs) {
                    repConfig.setNodeHostPort(nodeHost);
                } else {
                    usage("-nodeHost requires an argument");
                }
            } else if (thisArg.equals("-helperHost")) {
                /* The helper node hostname, port pair. */
                if (argc < nArgs) {
                    repConfig.setHelperHosts(argv[argc++]);
                } else {
                    usage("-helperHost requires an argument");
                }
            } else {
                usage("Unknown argument; " + thisArg);
            }
        }
        if (envHome == null) {
            usage("-env is a required parameter");
        }

        if (nodeName == null) {
            usage("-nodeName is a required parameter");
        }

        if (nodeHost == null) {
            usage("-nodeHost is a required parameter");
        }

        /* Helper host can be skipped once a anode has joined a group. */
        repConfig.setGroupName(REP_GROUP_NAME);
    }

    /**
     * Provides command level usage information.
     */
    static String usage() {
        StringBuilder builder = new StringBuilder();
        builder.append("Valid commands are:\n");
        builder.append("\tprint - displays stocks in database\n");
        builder.append("\tquit - exit the application\n");
        builder.append("\t<ticker symbol> <value> - "+
                       "inserts this stock/value pair into the database");

        return builder.toString();
    }

    /**
     * Provides invocation usage help information in response to an error
     * condition.
     *
     * @param message an explanation of the condition that provoked the
     *                display of usage information.
     */
    void usage(String message) {
        System.out.println();
        System.out.println(message);
        System.out.println();
        System.out.print("usage: " + getClass().getName());

        System.out.println
            (" -env <environment dir> -nodeName <nodeName> " +
             "-nodeHost <host:port> -helperHost <host:port> ");

        System.out.println
            ("\t -env the replicated environment directory\n" +
             "\t -nodeName the unique name associated with this node\n" +
             "\t -nodeHost the hostname and port pair associated with " +
             " this node\n" +
             "\t -helperHost the hostname and port pair associated with " +
             " the helper node\n");

        System.out.println("All parameters may also be expressed as " +
                           "properties in a je.properties file.");
        System.exit(0);
    }

    public static void main(String[] argv) throws Exception {
        StockQuotes stockQuotes = new StockQuotes(argv);
        stockQuotes.runExample();
    }
}
