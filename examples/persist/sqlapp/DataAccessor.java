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

package persist.sqlapp;

import java.util.Collection;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.persist.EntityCursor;
import com.sleepycat.persist.EntityIndex;
import com.sleepycat.persist.EntityJoin;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.ForwardCursor;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.SecondaryIndex;

/**
 * The data accessor class for the entity model.
 * 
 * @author chao
 */
class DataAccessor {
    /* Employee Accessors */
    PrimaryIndex<Integer, Employee> employeeById;
    SecondaryIndex<String, Integer, Employee> employeeByName;
    SecondaryIndex<Float, Integer, Employee> employeeBySalary;
    SecondaryIndex<Integer, Integer, Employee> employeeByManagerId;
    SecondaryIndex<Integer, Integer, Employee> employeeByDepartmentId;

    /* Department Accessors */
    PrimaryIndex<Integer, Department> departmentById;
    SecondaryIndex<String, Integer, Department> departmentByName;

    /** Opens all primary and secondary indices. */
    public DataAccessor(EntityStore store)
        throws DatabaseException {

        /* Primary index of the Employee database. */
        employeeById =
                store.getPrimaryIndex(Integer.class, Employee.class);

        /* Secondary index of the Employee database. */
        employeeByName = store.getSecondaryIndex(employeeById,
                                                 String.class,
                                                 "employeeName");
        employeeBySalary = store.getSecondaryIndex(employeeById,
                                                   Float.class,
                                                   "salary");
        employeeByManagerId = store.getSecondaryIndex(employeeById,
                                                      Integer.class,
                                                      "managerId");
        employeeByDepartmentId = store.getSecondaryIndex(employeeById,
                                                         Integer.class,
                                                         "departmentId");

        /* Primary index of the Department database. */
        departmentById =
                store.getPrimaryIndex(Integer.class, Department.class);
        /* Secondary index of the Department database. */
        departmentByName = store.getSecondaryIndex(departmentById,
                                                   String.class,
                                                   "departmentName");
    }

    /**
     * Do prefix query, similar to the SQL statement:
     * <blockquote><pre>
     * SELECT * FROM table WHERE col LIKE 'prefix%';
     * </pre></blockquote>
     *
     * @param index
     * @param prefix
     * @return
     * @throws DatabaseException
     */
    public <V> EntityCursor<V> doPrefixQuery(EntityIndex<String, V> index,
                                             String prefix)
        throws DatabaseException {

        assert (index != null);
        assert (prefix.length() > 0);

        /* Opens a cursor for traversing entities in a key range. */
        char[] ca = prefix.toCharArray();
        final int lastCharIndex = ca.length - 1;
        ca[lastCharIndex]++;
        return doRangeQuery(index, prefix, true, String.valueOf(ca), false);
    }

    /**
     * Do range query, similar to the SQL statement:
     * <blockquote><pre>
     * SELECT * FROM table WHERE col >= fromKey AND col <= toKey;
     * </pre></blockquote>
     *
     * @param index
     * @param fromKey
     * @param fromInclusive
     * @param toKey
     * @param toInclusive
     * @return
     * @throws DatabaseException
     */
    public <K, V> EntityCursor<V> doRangeQuery(EntityIndex<K, V> index,
                                               K fromKey,
                                               boolean fromInclusive,
                                               K toKey,
                                               boolean toInclusive)
        throws DatabaseException {

        assert (index != null);

        /* Opens a cursor for traversing entities in a key range. */
        return index.entities(fromKey,
                              fromInclusive,
                              toKey,
                              toInclusive);
    }

    /**
     * Do a "AND" join on a single primary database, similar to the SQL:
     * <blockquote><pre>
     * SELECT * FROM table WHERE col1 = key1 AND col2 = key2;
     * </pre></blockquote>
     *
     * @param pk
     * @param sk1
     * @param key1
     * @param sk2
     * @param key2
     * @return
     * @throws DatabaseException
     */
    public <PK, SK1, SK2, E> ForwardCursor<E>
        doTwoConditionsJoin(PrimaryIndex<PK, E> pk,
                            SecondaryIndex<SK1, PK, E> sk1,
                            SK1 key1,
                            SecondaryIndex<SK2, PK, E> sk2,
                            SK2 key2)
        throws DatabaseException {

        assert (pk != null);
        assert (sk1 != null);
        assert (sk2 != null);

        EntityJoin<PK, E> join = new EntityJoin<PK, E>(pk);
        join.addCondition(sk1, key1);
        join.addCondition(sk2, key2);

        return join.entities();
    }

    /**
     * Query the Employee database by Department's secondary-key: deptName. 
     * <blockquote><pre>
     * SELECT * FROM employee e, department d
     * WHERE e.departmentId = d.departmentId
     * AND d.departmentName = 'deptName';
     * </pre></blockquote>
     *
     * @param deptName
     * @throws DatabaseException
     */
    public void getEmployeeByDeptName(String deptName)
        throws DatabaseException {

        Department dept = departmentByName.get(deptName);
        /* Do an inner join on Department and Employee. */
        EntityCursor<Employee> empCursor =
            employeeByDepartmentId.
            subIndex(dept.getDepartmentId()).entities();
        try {
            for (Employee emp : empCursor) {
                System.out.println(emp);
            }
            System.out.println();
        } finally {
            empCursor.close();
        }
    }
    
    /**
     * Query the Employee database by adding a filter on Department's
     * non-secondary-key: deptLocation.
     * <blockquote><pre>
     * SELECT * FROM employee e, department d
     * WHERE e.departmentId = d.departmentId
     * AND d.location = 'deptLocation';
     * </pre></blockquote>
     * 
     * @param deptLocation
     * @throws DatabaseException
     */
    public void getEmployeeByDeptLocation(String deptLocation)
        throws DatabaseException {

        /* Iterate over Department database. */
        Collection<Department> departments =
            departmentById.sortedMap().values();
        for (Department dept : departments) {
            if (dept.getLocation().equals(deptLocation)) {
                /* A nested loop to do an equi-join. */
                EntityCursor<Employee> empCursor =
                    employeeByDepartmentId.
                    subIndex(dept.getDepartmentId()).
                    entities();
                try {
                    /* Iterate over all employees in dept. */
                    for (Employee emp : empCursor) {
                        System.out.println(emp);
                    }
                } finally {
                    empCursor.close();
                }
            }
        }
    }
}
