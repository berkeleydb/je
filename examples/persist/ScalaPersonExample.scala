/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2014 Oracle and/or its affiliates.  All rights reserved.
 *
 */

import java.io.File

import com.sleepycat.je.{Environment, EnvironmentConfig}
import com.sleepycat.persist.{EntityCursor,EntityStore,StoreConfig}
import com.sleepycat.persist.model.{Entity,PrimaryKey,SecondaryKey}
import com.sleepycat.persist.model.Relationship.ONE_TO_ONE

/**
 * Simple example of using Berkeley DB Java Edition (JE) with Scala.  The JE
 * Direct Persistence Layer (DPL) is used in this example, which requires Java
 * 1.5, so the scalac -target:jvm-1.5 option is required when compiling.  The
 * -Ygenerics option must also be used because DPL generics are used in this
 * example.
 *
 *  scalac -Ygenerics -target:jvm-1.5 -cp je-x.y.z.jar ScalaPersonExample.scala
 *
 * To run the example:
 *
 *  mkdir ./tmp
 *  scala -cp ".;je-x.y.z.jar" ScalaPersonExample
 *
 * Note that classOf[java.lang.String] and classOf[java.lang.Long] are used
 * rather than classOf[String] and classOf[Long].  The latter use the Scala
 * types rather than the Java types and cause run-time errors.
 *
 * This example was tested with Scala 2.6.1-RC1 and JE 3.2.30.
 *
 * See:
 *  http://www.scala-lang.org/
 *  http://www.oracle.com/technology/products/berkeley-db/je
 */
object ScalaPersonExample extends Application {

    /**
     * A persistent Entity is defined using DPL annotations.
     */
    @Entity
    class Person(nameParam: String, addressParam: String) {

        @PrimaryKey{val sequence="ID"}
        var id: long = 0

        @SecondaryKey{val relate=ONE_TO_ONE}
        var name: String = nameParam

        var address: String = addressParam

        private def this() = this(null, null) // default ctor for unmarshalling

        override def toString = "Person: " + id + ' ' + name + ' ' + address
    }

    /* Open the JE Environment. */
    val envConfig = new EnvironmentConfig()
    envConfig.setAllowCreate(true)
    envConfig.setTransactional(true)
    val env = new Environment(new File("./tmp"), envConfig)

    /* Open the DPL Store. */
    val storeConfig = new StoreConfig()
    storeConfig.setAllowCreate(true)
    storeConfig.setTransactional(true)
    val store = new EntityStore(env, "ScalaPersonExample", storeConfig)

    /* The PrimaryIndex maps the Long primary key to Person. */
    val priIndex =
        store.getPrimaryIndex(classOf[java.lang.Long], classOf[Person])

    /* The SecondaryIndex maps the String secondary key to Person. */
    val secIndex =
        store.getSecondaryIndex(priIndex, classOf[java.lang.String], "name")

    /* Insert some entities if the primary index is empty. */
    val txn = env.beginTransaction(null, null)
    if (priIndex.get(txn, 1L, null) == null) {
        val person1 = new Person("Zola", "#1 Zola Street")
        val person2 = new Person("Abby", "#1 Abby Street")
        priIndex.put(txn, person1)
        priIndex.put(txn, person2)
        assert(person1.id == 1) // assigned from the ID sequence
        assert(person2.id == 2) // assigned from the ID sequence
        txn.commit()
        println("--- Entities were inserted ---")
    } else {
        txn.abort()
        println("--- Entities already exist ---")
    }

    /* Get entities by primary and secondary keys. */
    println("--- Get by primary key ---")
    println(priIndex.get(1L))
    println(priIndex.get(2L))
    assert(priIndex.get(3L) == null)
    println("--- Get by secondary key ---")
    println(secIndex.get("Zola"))
    println(secIndex.get("Abby"))
    assert(secIndex.get("xxx") == null)

    /* Iterate entities in primary and secondary key order. */
    def printAll[T](cursor: EntityCursor[T]) {
        val person = cursor.next()
        if (person == null) {
            cursor.close()
        } else {
            println(person)
            printAll(cursor) // tail recursion
        }
    }
    println("--- Iterate by primary key ---")
    printAll(priIndex.entities())
    println("--- Iterate by secondary key ---")
    printAll(secIndex.entities())

    store.close()
    env.close()
}
