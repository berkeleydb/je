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
package com.sleepycat.je.dbi;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.trigger.PersistentTrigger;
import com.sleepycat.je.trigger.TransactionTrigger;
import com.sleepycat.je.trigger.Trigger;
import com.sleepycat.je.txn.Locker;
import com.sleepycat.je.txn.Txn;

/**
 * Class that invokes the triggers associated with a database. It encapsulates
 * the mechanics associated with actually invoking a trigger.
 */
public class TriggerManager {

    /**
     * Invokes the trigger methods associated with the opening of a database.
     */
    public static void runOpenTriggers(Locker locker,
                                       Database db,
                                       boolean isNew) {

        runOpenTriggers(locker, DbInternal.getDbImpl(db), isNew);
    }

    /**
     * Invokes the trigger methods associated with the opening of a database.
     */
    public static void runOpenTriggers(Locker locker,
                                       DatabaseImpl dbImpl,
                                       final boolean isNew) {

        runTriggers(dbImpl, locker, new TriggerInvoker(isNew) {

            @Override
            public void run(Transaction triggerTransaction, Trigger dbt) {
                if (dbt instanceof PersistentTrigger) {
                    Environment env =
                        getOpenTriggerEnvironment(triggerTransaction);
                    ((PersistentTrigger)dbt).open(triggerTransaction, env,
                                                  isNew);
                }
            }
        });
    }

    /**
     * Returns the environment handle that will be passed in as an argument to
     * a database open trigger.
     *
     * To ensure that an environment handle is always available, an internal
     * handle is created and stored in the EnvironmentImpl. The lifetime of the
     * internal handle (Environment or ReplicatedEnvironment) roughly aligns
     * with the lifetime of the underlying EnvironmentImpl, or it's subtype
     * RepImpl.
     *
     * For standalone environments, using explicit transactions, the
     * environment handle that's passed as the argument is the one used to
     * initiate the transaction. When using AutoTransactions to open a
     * database, the environment argument to the trigger is the internal
     * environment handle.
     *
     * For replicated environments, the argument to the trigger is the internal
     * environment handle in all cases. This is done to make the behavior of
     * the parameter deterministic and independent of the interaction of the
     * application level database open operations with those initiated from the
     * "replay" stream.
     *
     * @param transaction the transaction associated with the trigger
     *
     * @return the environment or null (if the environment is
     * non-transactional)
     */
    private static Environment
        getOpenTriggerEnvironment(Transaction transaction) {

        if (transaction == null) {
            return null;
        }

        final EnvironmentImpl envImpl =
            DbInternal.getTxn(transaction).getEnvironmentImpl();

        /*
         * Always return the same internal environment handle for replicated
         * environments.
         */
        if (envImpl.isReplicated()) {
            return envImpl.getInternalEnvHandle();
        }

        /*
         * Returns the environment handle associated with the transaction. It's
         * the internal handle for auto transactions, and the application
         * supplied handle used during transaction creation in all other cases.
         */
        return DbInternal.getEnvironment(transaction);
    }

    /**
     * Invokes the trigger methods associated with the closing of a database.
     * Note that this also results in the invocation of removeTrigger methods,
     * for transient triggers.
     */
    public static void runCloseTriggers(Locker locker, DatabaseImpl dbImpl) {

        runTriggers(dbImpl, locker, new TriggerInvoker(false) {

            @Override
            public void run(@SuppressWarnings("unused")
                            Transaction triggerTransaction, Trigger dbt) {
                if (dbt instanceof PersistentTrigger) {
                    ((PersistentTrigger)dbt).close();
                }
            }
        });
    }

    /**
     * Invokes the trigger methods associated with the removal of a database.
     * Note that this also results in the invocation of removeTrigger methods.
     */
    public static void runRemoveTriggers(Locker locker,
                                         DatabaseImpl dbImpl) {

        runTriggers(dbImpl, locker, new TriggerInvoker(true) {

            @Override
            public void run(Transaction triggerTransaction, Trigger dbt) {
                if (dbt instanceof PersistentTrigger) {
                    ((PersistentTrigger)dbt).remove(triggerTransaction);
                }
            }
        });

        runTriggers(dbImpl, locker, new TriggerInvoker(true) {

            @Override
            public void run(Transaction triggerTransaction, Trigger dbt) {
                if (dbt instanceof PersistentTrigger) {
                    ((PersistentTrigger)dbt).removeTrigger(triggerTransaction);
                }
            }
        });
    }

    /**
     * Invokes the trigger methods associated with the truncation of a
     * database.
     */
    public static void runTruncateTriggers(Locker locker,
                                           final DatabaseImpl newDb) {

        runTriggers(newDb, locker, new TriggerInvoker(true) {

            @Override
            public void run(Transaction triggerTransaction,
                            Trigger dbt) {
                if (dbt instanceof PersistentTrigger) {
                    ((PersistentTrigger)dbt).truncate(triggerTransaction);
                    dbt.setDatabaseName(newDb.getName());
                }
            }
        });
    }

    /**
     * Invokes the trigger methods associated with the renaming of a database.
     */
    public static void runRenameTriggers(Locker locker,
                                         DatabaseImpl dbImpl,
                                         final String newName) {

        runTriggers(dbImpl, locker, new TriggerInvoker(true) {

            @Override
            public void run(Transaction triggerTransaction,
                            Trigger dbt) {

                if (dbt instanceof PersistentTrigger) {
                    ((PersistentTrigger)dbt).rename(triggerTransaction,
                                                    newName);
                    dbt.setDatabaseName(newName);
                }
            }
        });
    }

    /* Transaction level triggers. */

    /**
     * Invokes the trigger methods associated with the commit of a transaction.
     * Trigger methods are only invoked if the txn was associated with a
     * trigger invocation.
     */
    public static void runCommitTriggers(Txn txn) {

        assert txn != null;

        final Set<DatabaseImpl> triggerDbs = txn.getTriggerDbs();

        if (triggerDbs == null) {
            return;
        }

        for (DatabaseImpl dbImpl : triggerDbs) {

            runTriggers(dbImpl, txn, new TriggerInvoker(false) {

                @Override
                public void run(Transaction triggerTransaction,
                                Trigger dbt) {
                    if (dbt instanceof TransactionTrigger) {
                        ((TransactionTrigger)dbt).commit(triggerTransaction);
                    }
                }
            });
        }
    }

    /**
     * Invokes the trigger methods associated with the abort of a transaction.
     * Trigger methods are only invoked if the txn was associated with a
     * trigger invocation.
     */
    public static void runAbortTriggers(Txn txn) {

       assert txn != null;

        final Set<DatabaseImpl> triggerDbs = txn.getTriggerDbs();

        if (triggerDbs == null) {
            return;
        }

        for (final DatabaseImpl dbImpl : triggerDbs) {

            runTriggers(dbImpl, txn, new TriggerInvoker(false) {

                @Override
                public void run(Transaction triggerTransaction, Trigger dbt) {

                    if (dbt instanceof TransactionTrigger) {
                        ((TransactionTrigger)dbt).abort(triggerTransaction);
                        if (!dbImpl.getName().equals(dbt.getDatabaseName())) {
                            dbt.setDatabaseName(dbImpl.getName());
                        }
                    }
                }
            });
        }
    }

    /**
     * Invokes the trigger methods associated with a put operation.
     */
    public static void runPutTriggers(Locker locker,
                                      DatabaseImpl dbImpl,
                                      final DatabaseEntry key,
                                      final DatabaseEntry oldData,
                                      final DatabaseEntry newData) {
        assert key != null;
        assert newData != null;

        runTriggers(dbImpl, locker, new TriggerInvoker(true) {

            @Override
            public void run(Transaction triggerTransaction, Trigger dbt) {

                dbt.put(triggerTransaction, key, oldData, newData);
            }
        });
    }

    /**
     * Invokes the trigger methods associated with a delete operation.
     */
    public static void runDeleteTriggers(Locker locker,
                                         DatabaseImpl dbImpl,
                                         final DatabaseEntry key,
                                         final DatabaseEntry oldData) {
        assert key != null;

        runTriggers(dbImpl, locker, new TriggerInvoker(true) {

            @Override
            public void run(Transaction triggerTransaction, Trigger dbt) {

                dbt.delete(triggerTransaction, key, oldData);
            }
        });
    }

    /**
     * Generic method for invoking any trigger operation. It iterates over all
     * the triggers associated with the database and if the trigger fails
     * invalidates the environment.
     *
     * @param dbImpl the database associated with potential triggers
     *
     * @param locker provides the transaction associated with the operation
     *
     * @param invoker encapsulates the trigger invoker
     */
    private static void runTriggers(final DatabaseImpl dbImpl,
                                    final Locker       locker,
                                    TriggerInvoker     invoker) {

        final List<Trigger> triggers = dbImpl.getTriggers();

        if (triggers == null) {
            return;
        }

        Transaction triggerTransaction =
            (locker instanceof Txn) ? ((Txn)locker).getTransaction() : null;

        try {
            for (Trigger trigger : triggers) {
                Trigger dbt = trigger;
                invoker.run(triggerTransaction, dbt);
            }
        } catch (Exception e) {
            final EnvironmentImpl env = dbImpl.getEnv();
            throw EnvironmentFailureException.unexpectedException(env, e);
        }

        /*
         * Note the use of a trigger for the database so that the appropriate
         * commit/abort triggers can be run.
         */
        if (invoker.invokeTransactionTrigger()) {
            DbInternal.getTxn(triggerTransaction).noteTriggerDb(dbImpl);
        }
    }

    /**
     * Utility class used to faciliatte the dispatch to a trigger method.
     */
    private static abstract class TriggerInvoker {
        /*
         * Determines whether a subsequent transaction trigger should be
         * invoked.
         */
        final boolean invokeTransactionTrigger;

        public TriggerInvoker(boolean invokeTransactionTrigger) {
            super();
            this.invokeTransactionTrigger = invokeTransactionTrigger;
        }

        /* Runs the specific trigger method. */
        abstract void run(Transaction triggerTransaction, Trigger dbt);

        /*
         * Determines whether the subsequent commit/abort trigger should be
         * invoked.
         */
        boolean invokeTransactionTrigger() {
            return invokeTransactionTrigger;
        }
    }

    /**
     * Invoke the triggers associated with the addition or removal of the
     * trigger itself. They are typically invoked upon database open, or
     * database removal.
     *
     * @param locker the locker associated with the trigger update operation
     * @param oldTriggers the current list of triggers
     * @param newTriggers the new list of triggers
     */
    public static void invokeAddRemoveTriggers(Locker locker,
                                               List<Trigger> oldTriggers,
                                               List<Trigger> newTriggers) {

        Set<String> oldNames = new MapOver<String, Trigger>(oldTriggers) {
            @Override
            protected String fun(Trigger e) {
                return e.getName();
            }
        }.run(new HashSet<String>());

        Set<String> newNames = new MapOver<String, Trigger>(newTriggers) {
            @Override
            protected String fun(Trigger e) {
                return e.getName();
            }
        }.run(new HashSet<String>());

        Transaction txn = (locker instanceof Txn) ?
                ((Txn)locker).getTransaction() : null;

        /* First invoke removeTrigger */
        if (oldTriggers != null) {
            for (Trigger trigger : oldTriggers) {
                if (!newNames.contains(trigger.getName())) {
                    trigger.removeTrigger(txn);
                }
            }
        }

        /* Now invoke addTrigger */
        if (newTriggers != null) {
            for (Trigger trigger : newTriggers) {
                if (!oldNames.contains(trigger.getName())) {
                    trigger.addTrigger(txn);
                }
            }
        }
    }

    /**
     * Lisp inspired Map function.
     *
     * @param <R> The result element type for the list being returned.
     * @param <E> The type of the element being mapped over.
     */
    public static abstract class MapOver<R,E> {
        final Collection<E> c;

        public MapOver(Collection<E> c) {
            this.c = c;
        }

        @SuppressWarnings("unchecked")
        public <S extends Collection<R>> S run() {
            Collection<R> l = new LinkedList<R>();
            return (S) run(l);
        }

        public <S extends Collection<R>> S run(S l) {
            if (c == null) {
                return l;
            }
            for (E e : c) {
                l.add(fun(e));
            }
            return l;
        }

        /* The function invoked for each element in the collection. */
        protected abstract R fun(E e);
    }
}
