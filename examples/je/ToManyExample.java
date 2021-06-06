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

package je;

import java.io.File;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.bind.tuple.StringBinding;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.ForeignKeyDeleteAction;
import com.sleepycat.je.ForeignMultiKeyNullifier;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.SecondaryConfig;
import com.sleepycat.je.SecondaryCursor;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.SecondaryMultiKeyCreator;
import com.sleepycat.je.Transaction;

/**
 * An example of using many-to-many and one-to-many secondary indices.
 */
public class ToManyExample {

    private final Environment env;
    private Database catalogDb;
    private Database animalDb;
    private Database personDb;
    private SecondaryDatabase personByEmail;
    private SecondaryDatabase personByAnimal;
    private EntryBinding<String> keyBinding;
    private EntryBinding<Person> personBinding;
    private EntryBinding<Animal> animalBinding;

    /**
     * Runs the example program, given a single "-h HOME" argument.
     */
    public static void main(String[] args) {

        if (args.length != 2 || !"-h".equals(args[0])) {
            System.out.println("Usage: java " +
                               ToManyExample.class.getName() +
                               " -h ENV_HOME");
            System.exit(1);
        }
        String homeDir = args[1];

        try {
            ToManyExample example = new ToManyExample(homeDir);
            example.exec();
            example.close();
        } catch (DatabaseException e) {
            e.printStackTrace();
        }
    }

    /**
     * Opens the environment and all databases.
     */
    private ToManyExample(String homeDir) throws DatabaseException {

        /* Open the environment. */
        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        envConfig.setTransactional(true);
        env = new Environment(new File(homeDir), envConfig);

        /* Open/create all databases in a transaction. */
        Transaction txn = env.beginTransaction(null, null);
        try {
            /* A standard (no duplicates) database config. */
            DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setAllowCreate(true);
            dbConfig.setTransactional(true);

            /* The catalog is used for the serial binding. */
            catalogDb = env.openDatabase(txn, "catalog", dbConfig);
            StoredClassCatalog catalog = new StoredClassCatalog(catalogDb);
            personBinding = new SerialBinding(catalog, null);
            animalBinding = new SerialBinding(catalog, null);
            keyBinding = new StringBinding();

            /* Open the person and animal primary DBs. */
            animalDb = env.openDatabase(txn, "animal", dbConfig);
            personDb = env.openDatabase(txn, "person", dbConfig);

            /*
             * A standard secondary config; duplicates, key creators and key
             * nullifiers are specified below.
             */
            SecondaryConfig secConfig = new SecondaryConfig();
            secConfig.setAllowCreate(true);
            secConfig.setTransactional(true);

            /*
             * Open the secondary database for personByEmail.  This is a
             * one-to-many index because duplicates are not configured.
             */
            secConfig.setSortedDuplicates(false);
            secConfig.setMultiKeyCreator(new EmailKeyCreator());
            personByEmail = env.openSecondaryDatabase(txn, "personByEmail",
                                                      personDb, secConfig);

            /*
             * Open the secondary database for personByAnimal.  This is a
             * many-to-many index because duplicates are configured.  Foreign
             * key constraints are specified to ensure that all animal keys
             * exist in the animal database.
             */
            secConfig.setSortedDuplicates(true);
            secConfig.setMultiKeyCreator(new AnimalKeyCreator());
            secConfig.setForeignMultiKeyNullifier(new AnimalKeyNullifier());
            secConfig.setForeignKeyDatabase(animalDb);
            secConfig.setForeignKeyDeleteAction(ForeignKeyDeleteAction.NULLIFY);
            personByAnimal = env.openSecondaryDatabase(txn, "personByAnimal",
                                                       personDb, secConfig);

            txn.commit();
        } catch (DatabaseException e) {
            txn.abort();
            throw e;
        } catch (RuntimeException e) {
            txn.abort();
            throw e;
        }
    }

    /**
     * Closes all databases and the environment.
     */
    private void close() throws DatabaseException {

        if (personByEmail != null) {
            personByEmail.close();
        }
        if (personByAnimal != null) {
            personByAnimal.close();
        }
        if (catalogDb != null) {
            catalogDb.close();
        }
        if (personDb != null) {
            personDb.close();
        }
        if (animalDb != null) {
            animalDb.close();
        }
        if (env != null) {
            env.close();
        }
    }

    /**
     * Adds, updates, prints and deletes Person records with many-to-many and
     * one-to-many secondary indices.
     */
    private void exec()
        throws DatabaseException {

        System.out.println
            ("\nInsert some animals.");
        Animal dogs = insertAndPrintAnimal("dogs", true);
        Animal fish = insertAndPrintAnimal("fish", false);
        Animal horses = insertAndPrintAnimal("horses", true);
        Animal donkeys = insertAndPrintAnimal("donkeys", true);

        System.out.println
            ("\nInsert a new empty person.");
        Person kathy = new Person();
        kathy.name = "Kathy";
        putPerson(kathy);
        printPerson("Kathy");

        System.out.println
            ("\nAdd favorites/addresses and update the record.");
        kathy.favoriteAnimals.add(horses.name);
        kathy.favoriteAnimals.add(dogs.name);
        kathy.favoriteAnimals.add(fish.name);
        kathy.emailAddresses.add("kathy@kathy.com");
        kathy.emailAddresses.add("kathy@yahoo.com");
        putPerson(kathy);
        printPerson("Kathy");

        System.out.println
            ("\nChange favorites and addresses and update the person record.");
        kathy.favoriteAnimals.remove(fish.name);
        kathy.favoriteAnimals.add(donkeys.name);
        kathy.emailAddresses.add("kathy@gmail.com");
        kathy.emailAddresses.remove("kathy@yahoo.com");
        putPerson(kathy);
        printPerson("Kathy");

        System.out.println
            ("\nInsert another person with some of the same favorites.");
        Person mark = new Person();
        mark.favoriteAnimals.add(dogs.name);
        mark.favoriteAnimals.add(horses.name);
        mark.name = "Mark";
        putPerson(mark);
        printPerson("Mark");

        System.out.println
            ("\nPrint by favorite animal index.");
        printByIndex(personByAnimal);

        System.out.println
            ("\nPrint by email address index.");
        printByIndex(personByEmail);

        System.out.println
            ("\nDelete 'dogs' and print again by favorite animal index.");
        deleteAnimal(dogs.name);
        printPerson("Kathy");
        printPerson("Mark");
        printByIndex(personByAnimal);

        System.out.println
            ("\nDelete both records and print again (should print nothing).");
        deletePerson("Kathy");
        deletePerson("Mark");
        printPerson("Kathy");
        printPerson("Mark");
        printByIndex(personByAnimal);
        printByIndex(personByEmail);
    }

    /**
     * Inserts an animal record and prints it.  Uses auto-commit.
     */
    private Animal insertAndPrintAnimal(String name, boolean furry)
        throws DatabaseException {

        Animal animal = new Animal();
        animal.name = name;
        animal.furry = furry;

        DatabaseEntry key = new DatabaseEntry();
        keyBinding.objectToEntry(name, key);

        DatabaseEntry data = new DatabaseEntry();
        animalBinding.objectToEntry(animal, data);

        OperationStatus status = animalDb.putNoOverwrite(null, key, data);
        if (status == OperationStatus.SUCCESS) {
            System.out.println(animal);
        } else {
            System.out.println("Animal was not inserted: " + name +
                               " (" + status + ')');
        }

        return animal;
    }

    /**
     * Deletes an animal.  Uses auto-commit.
     */
    private boolean deleteAnimal(String name)
        throws DatabaseException {

        DatabaseEntry key = new DatabaseEntry();
        keyBinding.objectToEntry(name, key);

        OperationStatus status = animalDb.delete(null, key);
        return status == OperationStatus.SUCCESS;
    }

    /**
     * Gets a person by name and prints it.
     */
    private void printPerson(String name)
        throws DatabaseException {

        DatabaseEntry key = new DatabaseEntry();
        keyBinding.objectToEntry(name, key);

        DatabaseEntry data = new DatabaseEntry();

        OperationStatus status = personDb.get(null, key, data, null);
        if (status == OperationStatus.SUCCESS) {
            Person person = personBinding.entryToObject(data);
            person.name = keyBinding.entryToObject(key);
            System.out.println(person);
        } else {
            System.out.println("Person not found: " + name);
        }
    }

    /**
     * Prints all person records by a given secondary index.
     */
    private void printByIndex(SecondaryDatabase secDb)
        throws DatabaseException {

        DatabaseEntry secKey = new DatabaseEntry();
        DatabaseEntry priKey = new DatabaseEntry();
        DatabaseEntry priData = new DatabaseEntry();

        SecondaryCursor cursor = secDb.openSecondaryCursor(null, null);
        try {
            while (cursor.getNext(secKey, priKey, priData, null) ==
                   OperationStatus.SUCCESS) {
                Person person = personBinding.entryToObject(priData);
                person.name = keyBinding.entryToObject(priKey);
                System.out.println("Index key [" +
                                   keyBinding.entryToObject(secKey) +
                                   "] maps to primary key [" +
                                   person.name + ']');
            }
        } finally {
            cursor.close();
        }
    }

    /**
     * Inserts or updates a person.  Uses auto-commit.
     */
    private void putPerson(Person person)
        throws DatabaseException {

        DatabaseEntry key = new DatabaseEntry();
        keyBinding.objectToEntry(person.name, key);

        DatabaseEntry data = new DatabaseEntry();
        personBinding.objectToEntry(person, data);

        personDb.put(null, key, data);
    }

    /**
     * Deletes a person.  Uses auto-commit.
     */
    private boolean deletePerson(String name)
        throws DatabaseException {

        DatabaseEntry key = new DatabaseEntry();
        keyBinding.objectToEntry(name, key);

        OperationStatus status = personDb.delete(null, key);
        return status == OperationStatus.SUCCESS;
    }

    /**
     * A person object.
     */
    @SuppressWarnings("serial")
    private static class Person implements Serializable {

        /** The primary key. */
        private transient String name;

        /** A many-to-many set of keys. */
        private final Set<String> favoriteAnimals = new HashSet<String>();

        /** A one-to-many set of keys. */
        private final Set<String> emailAddresses = new HashSet<String>();

        @Override
        public String toString() {
            return "Person {" +
                   "\n  Name: " + name +
                   "\n  FavoriteAnimals: " + favoriteAnimals +
                   "\n  EmailAddresses: " + emailAddresses +
                   "\n}";
        }
    }

    /**
     * An animal object.
     */
    @SuppressWarnings("serial")
    private static class Animal implements Serializable {

        /** The primary key. */
        private transient String name;

        /** A non-indexed property. */
        private boolean furry;

        @Override
        public String toString() {
            return "Animal {" +
                   "\n  Name: " + name +
                   "\n  Furry: " + furry +
                   "\n}";
        }
    }

    /**
     * Returns the set of email addresses for a person.  This is an example
     * of a multi-key creator for a to-many index.
     */
    private class EmailKeyCreator implements SecondaryMultiKeyCreator {

        public void createSecondaryKeys(SecondaryDatabase secondary,
                                        DatabaseEntry primaryKey,
                                        DatabaseEntry primaryData,
                                        Set<DatabaseEntry> results) {
            Person person = personBinding.entryToObject(primaryData);
            copyKeysToEntries(person.emailAddresses, results);
        }
    }

    /**
     * Returns the set of favorite animals for a person.  This is an example
     * of a multi-key creator for a to-many index.
     */
    private class AnimalKeyCreator implements SecondaryMultiKeyCreator {

        public void createSecondaryKeys(SecondaryDatabase secondary,
                                        DatabaseEntry primaryKey,
                                        DatabaseEntry primaryData,
                                        Set<DatabaseEntry> results) {
            Person person = personBinding.entryToObject(primaryData);
            copyKeysToEntries(person.favoriteAnimals, results);
        }
    }

    /**
     * A utility method to copy a set of keys (Strings) into a set of
     * DatabaseEntry objects.
     */
    private void copyKeysToEntries(Set<String> keys,
                                   Set<DatabaseEntry> entries) {

        for (Iterator<String> i = keys.iterator(); i.hasNext();) {
            DatabaseEntry entry = new DatabaseEntry();
            keyBinding.objectToEntry(i.next(), entry);
            entries.add(entry);
        }
    }

    /**
     * Removes a given key from the set of favorite animals for a person.  This
     * is an example of a nullifier for a to-many index.  The nullifier is
     * called when an animal record is deleted because we configured this
     * secondary with ForeignKeyDeleteAction.NULLIFY.
     */
    private class AnimalKeyNullifier implements ForeignMultiKeyNullifier {

        public boolean nullifyForeignKey(SecondaryDatabase secondary,
                                         DatabaseEntry primaryKey,
                                         DatabaseEntry primaryData,
                                         DatabaseEntry secKey) {
            Person person = personBinding.entryToObject(primaryData);
            String key = keyBinding.entryToObject(secKey);
            if (person.favoriteAnimals.remove(key)) {
                personBinding.objectToEntry(person, primaryData);
                return true;
            } else {
                return false;
            }
        }
    }
}
